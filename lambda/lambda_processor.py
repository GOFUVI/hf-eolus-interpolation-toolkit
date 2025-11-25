import os
import re
import shutil
import socket
import tempfile
import time
import urllib.error
import urllib.request

import boto3

# Environment variables provided in the Lambda configuration
DEST_BUCKET = os.environ.get("DEST_BUCKET", "")
DEST_PREFIX = os.environ.get("DEST_PREFIX", "").rstrip("/")  # remove trailing '/' if present
NETCDF_DOWNLOAD_RETRIES = max(1, int(os.environ.get("NETCDF_DOWNLOAD_RETRIES", "4")))
NETCDF_DOWNLOAD_TIMEOUT = int(os.environ.get("NETCDF_DOWNLOAD_TIMEOUT", "60"))
NETCDF_DOWNLOAD_BACKOFF = float(os.environ.get("NETCDF_DOWNLOAD_BACKOFF", "5"))

s3_client = boto3.client('s3')


def _download_with_retry(url, destination, timeout_seconds, retries, backoff_seconds):
    """Download URL to destination with retry/backoff to survive transient network hiccups."""
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=timeout_seconds) as response, open(destination, "wb") as out_file:
                shutil.copyfileobj(response, out_file)
            return
        except (urllib.error.URLError, socket.timeout) as exc:
            last_error = exc
            print(f"WARNING: Download attempt {attempt}/{retries} failed for {url}: {exc}")
        except Exception as exc:
            last_error = exc
            print(f"WARNING: Unexpected error downloading {url} (attempt {attempt}/{retries}): {exc}")

        if attempt < retries:
            sleep_time = backoff_seconds * attempt
            print(f"INFO: Retrying download in {sleep_time} seconds...")
            time.sleep(sleep_time)
    if last_error:
        raise last_error

def _lambda_handler(event, context):
    """
    Lambda handler that processes a NetCDF file:
      - Downloads the file from the provided URL (or constructs it from a date).
      - Converts the variables lat, lon, dir (wind direction) and mod (wind speed) to a tabular DataFrame.
      - Writes the DataFrame to a Parquet file.
      - Uploads the Parquet to the destination S3 bucket (partitioned by year, month, day).
    """
    # Import heavy dependencies inside handler for faster cold starts
    import xarray as xr
    import pandas as pd
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq
    import geopandas as gpd
    import json

    # Get NetCDF URL from the event
    url = None
    date_str = None
    source_model = event.get("source_model")
    if "url" in event:
        url = event["url"]
        # Try to extract the YYYYMMDD date from the filename in the URL (assuming known pattern)
        m = re.search(r"_([0-9]{8})_0000.nc4$", url)
        if m:
            date_str = m.group(1)  # Ejemplo: "20250101"
        else:
            # If extraction fails, date_str remains None (naming may vary)
            date_str = None
    elif "date" in event:
        # If 'date' is provided instead of URL, construct the URL (same logic as generate_urls.py)
        date_input = str(event["date"])
        try:
            # Expected format 'YYYY-MM-DD'
            dt = pd.to_datetime(date_input)
            date_str = dt.strftime("%Y%m%d")
            year = dt.strftime("%Y")
            month = dt.strftime("%m")
            filename = f"wrf_arw_det_history_d03_{date_str}_0000.nc4"
            url = f"https://mandeo.meteogalicia.es/thredds/fileServer/modelos/WRF_HIST/d03/{year}/{month}/{filename}"
        except Exception as e:
            raise ValueError(f"Invalid date format: {date_input}") from e
    else:
        raise ValueError("Event does not contain 'url' or 'date' to process.")

    # Define temporary path to download NetCDF
    local_netcdf = os.path.join(tempfile.gettempdir(), "data.nc")
    try:
        _download_with_retry(
            url,
            local_netcdf,
            timeout_seconds=NETCDF_DOWNLOAD_TIMEOUT,
            retries=NETCDF_DOWNLOAD_RETRIES,
            backoff_seconds=NETCDF_DOWNLOAD_BACKOFF,
        )
    except Exception as e:
        print(f"ERROR: Failed to download file {url} after retries. Detail: {e}")
        raise

    # Open the NetCDF file with xarray
    try:
        ds = xr.open_dataset(local_netcdf)
    except Exception as e:
        print(f"ERROR: No se pudo abrir el archivo NetCDF descargado. Detalle: {e}")
        raise

    # Verify that required variables exist in the dataset, including grid coordinates x and y
    for var in ["lat", "lon", "dir", "mod", "x", "y", "topo"]:
        if var not in ds.variables:
            print(f"ERROR: Variable '{var}' not found in dataset")
            raise KeyError(f"Variable {var} missing in data")

    # Extract projection (CRS) information from CF grid mapping variable, if available
    proj_var = None
    for var_name in ds.variables:
        if 'grid_mapping_name' in ds[var_name].attrs:
            proj_var = var_name
            proj_attrs = ds[var_name].attrs
            break
    if proj_var:
        proj_parts = [f"+proj={proj_attrs.get('grid_mapping_name')}" ]
        sp = proj_attrs.get('standard_parallel')
        if sp is not None:
            if isinstance(sp, (list, tuple, np.ndarray)):
                if len(sp) >= 1:
                    proj_parts.append(f"+lat_1={sp[0]}")
                if len(sp) >= 2:
                    proj_parts.append(f"+lat_2={sp[1]}")
            else:
                proj_parts.append(f"+lat_1={sp}")
        for cf_key, proj_key in [
            ('longitude_of_central_meridian', 'lon_0'),
            ('latitude_of_projection_origin', 'lat_0'),
            ('false_easting', 'x_0'),
            ('false_northing', 'y_0')
        ]:
            val = proj_attrs.get(cf_key)
            if val is not None:
                proj_parts.append(f"+{proj_key}={val}")
        units = proj_attrs.get('units')
        if units:
            proj_parts.append(f"+units={units}")
        else:
            # Default to kilometers if units not specified in NetCDF
            proj_parts.append("+units=km")
        proj_string = ' '.join(proj_parts)
        # Remove any unsupported +type attribute (e.g., +type=crs) for pyproj compatibility
        proj_string = ' '.join([tok for tok in proj_string.split() if not tok.startswith('+type=')])
    else:
        proj_string = None

    # Extract necessary variables and prepare streaming Parquet output

    lat = ds["lat"].values   # shape (ny, nx)
    lon = ds["lon"].values   # shape (ny, nx)
    # DEBUG: bounding box of input grid
    min_lat, max_lat = float(lat.min()), float(lat.max())
    min_lon, max_lon = float(lon.min()), float(lon.max())
    print(f"DEBUG: input grid bbox lat=[{min_lat},{max_lat}], lon=[{min_lon},{max_lon}]")
    x_arr = ds["x"].values   # shape (nx,)
    y_arr = ds["y"].values   # shape (ny,)
    times = ds["time"].values  # shape (time,)

    ny, nx = lat.shape
    num_points = ny * nx
    num_times = times.shape[0]

    lat_flat = lat.flatten()
    lon_flat = lon.flatten()

    # Generate 1D arrays for grid coordinates corresponding to each point
    # x_arr is 1D along x dimension, y_arr is 1D along y dimension
    x_grid, y_grid = np.meshgrid(x_arr, y_arr)
    x_flat = x_grid.flatten()
    y_flat = y_grid.flatten()

    # Capture optional region definition from event (no spatial filtering applied)
    regions = event.get('regions')
    polygon = event.get('polygon')
    region_name = event.get('region_name')
    if not source_model:
        source_model = event.get('model') or event.get('model_choice') or "unknown"
    # Default region name to filename if not provided
    if not region_name:
        region_name = os.path.splitext(os.path.basename(url))[0]
    # Define Parquet schema including x and y grid coordinates
    schema = pa.schema([
        pa.field("time", pa.timestamp("ns")),
        pa.field("lat", pa.float64()),
        pa.field("lon", pa.float64()),
        pa.field("x", pa.float64()),
        pa.field("y", pa.float64()),
        pa.field("topo", pa.float32()),
        pa.field("wind_dir", pa.float32()),
        pa.field("wind_speed", pa.float32()),
        pa.field("source_model", pa.string())
    ])

    # Stream each time slice to separate Parquet files partitioned by hour
    # and upload each to S3 under year/month/day/hour folder.
    # Derive date_str from first timestamp if not provided
    if date_str is None:
        date_str = pd.to_datetime(times[0]).strftime("%Y%m%d")

    year = date_str[0:4]
    month = date_str[4:6]
    day = date_str[6:8]
    prefix = DEST_PREFIX.rstrip("/") + "/" if DEST_PREFIX else ""
    if DEST_PREFIX:
        prefix_parts = DEST_PREFIX.rstrip("/").split("/")
        metadata_parts = prefix_parts[:-1] + ["metadata", prefix_parts[-1]]
        metadata_prefix = "/".join(metadata_parts) + "/"
    else:
        metadata_prefix = "metadata/"

    uploaded = []

    def _normalize_meta_value(value):
        """Recursively convert numpy/scalar values to plain Python types for JSON serialization."""
        import numpy as np

        if isinstance(value, dict):
            return {str(k): _normalize_meta_value(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [_normalize_meta_value(item) for item in value]
        if isinstance(value, (np.generic,)):
            return value.item()
        if isinstance(value, (bytes, bytearray)):
            return value.decode("utf-8", errors="ignore")
        if isinstance(value, np.ndarray):
            return [_normalize_meta_value(item) for item in value.tolist()]
        return value

    def _collect_variable_attributes(dataset):
        return {
            name: {k: _normalize_meta_value(v) for k, v in var.attrs.items()}
            for name, var in dataset.variables.items()
        }

    sidecar_common = {
        "netcdf_attributes": {
            "global": {k: _normalize_meta_value(v) for k, v in ds.attrs.items()},
            "variables": _collect_variable_attributes(ds),
            "encoding": {k: _normalize_meta_value(v) for k, v in ds.encoding.items()},
            "dimensions": {k: int(v) for k, v in ds.dims.items()}
        }
    }

    if not sidecar_common["netcdf_attributes"]["global"]:
        sidecar_common["netcdf_attributes"]["global"] = {}

    test_points_payload = event.get('test_points')
    if isinstance(test_points_payload, str):
        try:
            test_points_payload = json.loads(test_points_payload)
        except json.JSONDecodeError:
            test_points_payload = None

    regions_payload = None
    if regions:
        if isinstance(regions, str):
            try:
                regions_payload = json.loads(regions)
            except json.JSONDecodeError:
                regions_payload = None
        else:
            regions_payload = regions
    elif polygon or region_name:
        region_entry = {}
        if region_name:
            region_entry["region_name"] = region_name
        if polygon:
            region_entry["polygon"] = polygon
        regions_payload = [region_entry]
    for idx in range(num_times):
        dir_slice = ds["dir"].isel(time=idx).values.flatten().astype(np.float32)
        spd_slice = ds["mod"].isel(time=idx).values.flatten().astype(np.float32)
        # Extract topography slice (elevation) for this time index
        topo_slice = ds["topo"].isel(time=idx).values.flatten().astype(np.float32)
        time_val = pd.to_datetime(times[idx])
        hour_str = time_val.strftime("%H")
        time_slice = np.full(
            num_points,
            times[idx], dtype="datetime64[ns]"
        )

        # Build GeoDataFrame slice and write GeoParquet with embedded CRS and NetCDF metadata
        df_slice = pd.DataFrame({
            'time': time_slice,
            'lat': lat_flat,
            'lon': lon_flat,
            'x': x_flat,
            'y': y_flat,
            'topo': topo_slice,
            'wind_dir': dir_slice,
            'wind_speed': spd_slice,
            'source_model': source_model
        })
        # Create GeoDataFrame in geographic WGS84; embed model grid mapping in metadata
        gdf = gpd.GeoDataFrame(
            df_slice,
            geometry=gpd.points_from_xy(df_slice.lon, df_slice.lat),
            crs="EPSG:4326"
        )
        # Serialize geometry to WKB and attach NetCDF metadata (including original projection)
        gdf['geom_wkb'] = gdf.geometry.apply(lambda geom: geom.wkb)
        # Build table: drop lat/lon/geometry and rename WKB column to 'geometry'
        df_for_table = gdf.drop(columns=['lat', 'lon', 'geometry']).rename(columns={'geom_wkb': 'geometry'})
        df_for_table["date"] = time_val.strftime("%Y-%m-%d")
        df_for_table["hour"] = hour_str
        df_for_table["timestamp"] = time_val.strftime("%Y-%m-%dT%H:%M:%SZ")
        table = pa.Table.from_pandas(df_for_table, preserve_index=False)
        hour_parquet = os.path.join(tempfile.gettempdir(), f"data_{hour_str}.parquet")
        pq.write_table(table, hour_parquet, compression="snappy")

        key = f"{prefix}year={year}/month={month}/day={day}/hour={hour_str}/data.parquet"
        try:
            s3_client.upload_file(hour_parquet, DEST_BUCKET, key)
            print(f"GeoParquet for hour {hour_str} uploaded to s3://{DEST_BUCKET}/{key}")
            uploaded.append(key)
        except Exception as e:
            print(f"ERROR: Failed to upload GeoParquet for hour {hour_str}. Detail: {e}")
            raise

        # Persist lineage metadata to sidecar JSON outside the data tree
        sidecar_payload = {
            "crs": proj_string,
            "nc_proj_string": proj_string,
            **sidecar_common
        }
        if regions_payload:
            sidecar_payload["regions"] = regions_payload
        if region_name:
            sidecar_payload["region_name"] = region_name
        if test_points_payload:
            sidecar_payload["test_points"] = test_points_payload
        if url:
            sidecar_payload["source_url"] = url
        if source_model:
            sidecar_payload["source_model"] = source_model
        sidecar_local = os.path.join(tempfile.gettempdir(), f"metadata_{hour_str}.json")
        with open(sidecar_local, "w", encoding="utf-8") as sidecar_file:
            json.dump(sidecar_payload, sidecar_file, ensure_ascii=False, indent=2)

        sidecar_key = (
            f"{metadata_prefix}year={year}/month={month}/day={day}/hour={hour_str}/metadata.json"
        )
        try:
            s3_client.upload_file(sidecar_local, DEST_BUCKET, sidecar_key)
            print(f"Metadata sidecar uploaded to s3://{DEST_BUCKET}/{sidecar_key}")
            uploaded.append(sidecar_key)
        except Exception as e:
            print(f"ERROR: Failed to upload metadata sidecar for hour {hour_str}. Detail: {e}")
            raise

    # Return summary of uploaded files
    return {
        "status": "OK",
        "bucket": DEST_BUCKET,
        "uploaded_keys": uploaded
    }
    # End of internal handler

"""
Entry point for AWS Lambda. Delegate to internal handler and let exceptions propagate.
"""
lambda_handler = _lambda_handler
