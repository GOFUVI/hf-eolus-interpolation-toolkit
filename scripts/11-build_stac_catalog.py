#!/usr/bin/env python3
"""
build_stac_catalog.py - Generate a STAC Collection for GeoParquet interpolation outputs.

This CLI inspects partitioned GeoParquet files produced by the HF-EOLUS interpolation
pipeline and emits a STAC Collection with one Item per partition (typically per hour).

Highlights:
* Supports local folders or remote locations understood by pyarrow (e.g. s3:// URIs).
* Reuses GeoParquet metadata (geo -> bbox, primary geometry column) when available.
* Falls back to scanning lon/lat columns to derive spatial extent.
* Computes temporal metadata from timestamp columns or from year/month/day/hour partitions.
* Leaves asset HREFs pointing to the original files by default, with an option to copy
  assets inside the output catalog root to obtain a self-contained package.

Usage:
    scripts/11-build_stac_catalog.py \\
        --input-root local_out \\
        --output-dir catalogs/interpolated_galicia_2025 \\
        --collection-id interpolated-galicia-2025 \\
        --collection-title \"Interpolated Winds - Galicia 2025\" \\
        --region \"Galicia\" \\
        --temporal-start 2025-01-01T00:00:00Z \\
        --temporal-end 2025-01-31T23:00:00Z

Requirements:
* pyarrow >= 10.0.0 (for parquet & filesystem access)
* pystac >= 1.8.0
* shapely is NOT required; geometries are emitted as bbox polygons.

The script exits with a descriptive error if dependencies are missing.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Set
import re

try:
    import pyarrow as pa  # noqa: F401  (used indirectly via parquet/fs)
    import pyarrow.parquet as pq
    import pyarrow.fs as pafs
except ImportError as exc:  # pragma: no cover - dependency guard
    sys.exit(
        "Missing dependency: pyarrow. Install it with `pip install pyarrow` before "
        "running this script."
    )

try:
    import pystac
    from pystac import Asset, Catalog, CatalogType, Collection, Extent, Item, Link
    from pystac.extensions.table import Column, TableExtension
except ImportError as exc:  # pragma: no cover - dependency guard
    sys.exit(
        "Missing dependency: pystac. Install it with `pip install pystac` before "
        "running this script."
    )


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def resolve_fs_and_base(prefix: str) -> Tuple[pafs.FileSystem, str]:
    """Return a filesystem and base path for the provided prefix."""
    try:
        fs, base = pafs.FileSystem.from_uri(prefix)
        return fs, base
    except ValueError:
        fs = pafs.LocalFileSystem()
        return fs, os.fspath(Path(prefix).resolve())


def build_href(prefix: Optional[str], relative: PurePosixPath) -> Optional[str]:
    """Compose an href from a prefix and a relative path."""
    if prefix is None:
        return None
    if "://" in prefix:
        return prefix.rstrip("/") + "/" + str(relative)
    return str(Path(prefix) / Path(str(relative)))


def relative_from_base(full_path: str, base: str) -> PurePosixPath:
    """Compute the relative path of full_path with respect to base."""
    full_norm = PurePosixPath(full_path)
    base_norm = PurePosixPath(base)
    try:
        return full_norm.relative_to(base_norm)
    except ValueError:
        prefix = base.rstrip("/") + "/"
        if full_path.startswith(prefix):
            return PurePosixPath(full_path[len(prefix):])
    return PurePosixPath(full_norm.name)


def copy_from_fs(fs: pafs.FileSystem, source: str, destination: Path) -> bool:
    """Copy a file from a filesystem into the local catalog assets directory."""
    try:
        ensure_dir(str(destination.parent))
        with fs.open_input_file(source) as src, open(destination, "wb") as dst:
            while True:
                chunk = src.read(8 * 1024 * 1024)
                if not chunk:
                    break
                dst.write(chunk)
        return True
    except FileNotFoundError:
        return False
    except Exception:
        return False


def parse_datetime(value: str) -> datetime:
    """Parse ISO-8601 strings (with optional Z suffix) into UTC-aware datetimes."""
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def ensure_utc(dt: Optional[datetime]) -> Optional[datetime]:
    """Normalise datetimes to UTC (or keep None)."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def merge_bbox(a: Optional[Tuple[float, float, float, float]],
               b: Optional[Tuple[float, float, float, float]]) -> Optional[Tuple[float, float, float, float]]:
    """Expand bbox `a` to include bbox `b`."""
    if a is None:
        return b
    if b is None:
        return a
    return (min(a[0], b[0]), min(a[1], b[1]), max(a[2], b[2]), max(a[3], b[3]))


def bbox_to_polygon(bbox: Tuple[float, float, float, float]) -> Dict[str, Sequence[Sequence[Sequence[float]]]]:
    """Convert [minx, miny, maxx, maxy] to a GeoJSON polygon covering the box."""
    minx, miny, maxx, maxy = bbox
    return {
        "type": "Polygon",
        "coordinates": [
            [
                [minx, miny],
                [maxx, miny],
                [maxx, maxy],
                [minx, maxy],
                [minx, miny],
            ]
        ],
    }


def is_partition_segment(segment: str) -> bool:
    """Return True if the path segment looks like a Hive partition entry `name=value`."""
    return "=" in segment and len(segment.split("=", 1)[0]) > 0


COLUMN_DESCRIPTIONS: Dict[str, str] = {
    "node_id": "String identifier assigned in grid_module.R; original WRF nodes retain their native ID while newly generated mesh points receive sequential IDs or test point names.",
    "x_local": "Local X offset (metres) from the south-west corner of the projected grid, computed in build_interpolation_grid() after subtracting the minimum native X coordinate.",
    "y_local": "Local Y offset (metres) from the south-west corner of the projected grid, computed in build_interpolation_grid() after subtracting the minimum native Y coordinate.",
    "is_orig": "Boolean flag emitted by build_interpolation_grid(): TRUE marks nodes that coincide with the native WRF grid, FALSE marks nodes created for interpolation (including optional test points).",
    "x": "Longitude in decimal degrees extracted in prepare_output_sf() after reprojecting the grid to WGS 84.",
    "y": "Latitude in decimal degrees extracted in prepare_output_sf() after reprojecting the grid to WGS 84.",
    "timestamp": "ISO-8601 timestamp (UTC) built in prepare_output_sf() representing the hour being processed (centre of the interpolation interval).",
    "date": "Processing date (YYYY-MM-DD) copied into every row in prepare_output_sf().",
    "hour": "Processing hour (00-23) copied into every row in prepare_output_sf().",
    "u": "Interpolated zonal wind component (m/s) oriented west-to-east from perform_interpolation_suite().",
    "v": "Interpolated meridional wind component (m/s) oriented south-to-north from perform_interpolation_suite().",
    "u_rkt": "Regression-kriging estimate of the U component incorporating topography as external drift (regression_kriging() in interpolation_module.R).",
    "v_rkt": "Regression-kriging estimate of the V component incorporating topography as external drift (regression_kriging() in interpolation_module.R).",
    "wind_speed": "Resultant wind speed magnitude (m/s) computed in prepare_output_sf() as sqrt(u^2 + v^2).",
    "wind_dir": "Wind direction in degrees (0° = north, increasing clockwise) derived via .calc_dir() in export_module.R using meteorological convention.",
    "kriging_var_u": "Ordinary kriging variance for the U component returned by predict_component() (same projection units as the working grid squared).",
    "kriging_var_v": "Ordinary kriging variance for the V component returned by predict_component() (same projection units as the working grid squared).",
    "input_count": "Total number of original observations available in the partition, injected by prepare_output_sf() from metadata$input_count.",
    "interpolated_count": "Number of mesh nodes filled via interpolation (i.e. FALSE values in is_orig), computed in prepare_output_sf().",
    "cv_model_u": "Name of the variogram/IDW option that minimised cross-validation error for the U component (select_variogram_model()).",
    "cv_model_v": "Name of the variogram/IDW option that minimised cross-validation error for the V component (select_variogram_model()).",
    "cv_rsr_u": "Root square error ratio for the selected U variogram model measured during n-fold cross validation.",
    "cv_rsr_v": "Root square error ratio for the selected V variogram model measured during n-fold cross validation.",
    "cv_bias_u": "Bias metric for the selected U variogram model measured during n-fold cross validation.",
    "cv_bias_v": "Bias metric for the selected V variogram model measured during n-fold cross validation.",
    "test_model_u": "Model identifier evaluated on the hold-out set for the U component (mirrors cv_model_u when available).",
    "test_model_v": "Model identifier evaluated on the hold-out set for the V component (mirrors cv_model_v when available).",
    "test_rsr_u": "Root square error ratio for the U component computed on the hold-out samples in evaluate_holdout().",
    "test_rsr_v": "Root square error ratio for the V component computed on the hold-out samples in evaluate_holdout().",
    "test_bias_u": "Bias for the U component measured on the hold-out set in evaluate_holdout().",
    "test_bias_v": "Bias for the V component measured on the hold-out set in evaluate_holdout().",
    "nearest_distance_km": "Distance from each mesh node to the closest original observation (kilometres, derived from compute_knn() results in interpolation_module.R).",
    "neighbors_used": "Number of neighbouring observations within the cutoff distance employed during kriging/IDW for that node.",
    "interpolation_source": "Text flag indicating whether the value comes from the original measurements, interpolation outputs, or user-supplied test points (propagate_predictions()).",
    "vgm_model_u": "Name of the variogram model selected for U (e.g. Exp, Gau, Sph, IDW or Universal) returned by extract_variogram_params().",
    "vgm_model_v": "Name of the variogram model selected for V (e.g. Exp, Gau, Sph, IDW or Universal) returned by extract_variogram_params().",
    "vgm_range_u": "Range parameter (in native projection units) of the selected U variogram, via extract_variogram_params().",
    "vgm_range_v": "Range parameter (in native projection units) of the selected V variogram, via extract_variogram_params().",
    "vgm_sill_u": "Sill (total variance) of the selected U variogram, computed in extract_variogram_params().",
    "vgm_sill_v": "Sill (total variance) of the selected V variogram, computed in extract_variogram_params().",
    "vgm_nugget_u": "Nugget effect of the selected U variogram, computed in extract_variogram_params().",
    "vgm_nugget_v": "Nugget effect of the selected V variogram, computed in extract_variogram_params().",
    "geometry": "EWKB point geometry stored in the Parquet dataset (longitude/latitude in WGS 84) produced in export_module.R.",
    "time": "Original timestamp column from the MeteoGalicia input when present in the source Parquet partition.",
    "sci:doi": "Digital Object Identifier referencing the Zenodo record for the HF-EOLUS interpolated winds product.",
    "sci:citation": "Full citation string for the HF-EOLUS interpolated winds dataset.",
    "providers": "List of organisations responsible for producing or processing the dataset, injected via overrides.",
    "description": "Human-readable summary of the MeteoGalicia interpolation product merged from the item overrides."
}


PLOT_VARIOGRAM_RE = re.compile(r"^variogram_(?P<component>[uv])(?P<variant>_rkt)?_empirical_")


def describe_plot_asset(filename: str) -> Tuple[Optional[str], Dict[str, str]]:
    """Return a human readable description and optional properties for plot assets."""
    name = filename.lower()
    properties: Dict[str, str] = {}

    if name.startswith("grid_points"):
        description = (
            "Scatter map generated by wind_interpolation.R that shows the interpolation mesh in "
            "WGS 84: interpolated nodes, original MeteoGalicia observations (drawn on top), and "
            "optional test points created in grid_module.R."
        )
        properties["plot_type"] = "grid-mesh"
        return description, properties

    match = PLOT_VARIOGRAM_RE.match(name)
    if match:
        component = match.group("component")
        variant = match.group("variant")
        component_label = {
            "u": "zonal (u) wind component",
            "v": "meridional (v) wind component",
        }.get(component, component)
        properties["plot_type"] = "variogram"
        properties["plot_component"] = component
        if variant:
            description = (
                f"Empirical semivariogram of the regression-kriging residuals for the {component_label}, "
                "generated in regression_kriging() and saved by save_variogram_plot() "
                "inside scripts/modules/interpolation_module.R using terrain (topo) as external drift."
            )
            properties["plot_variant"] = "regression-kriging"
        else:
            description = (
                f"Empirical semivariogram for the {component_label} produced during ordinary kriging calibration; "
                "save_variogram_plot() (scripts/modules/interpolation_module.R) overlays the model selected by cross-validation."
            )
            properties["plot_variant"] = "ordinary-kriging"
        return description, properties

    return None, properties




def describe_column(name: str, stac_type: str) -> str:
    """Return a human readable description for a column."""
    if name in COLUMN_DESCRIPTIONS:
        return COLUMN_DESCRIPTIONS[name]
    return f"{name} column with values of type {stac_type}."


def deep_merge_dict(base: Dict[str, Any], extra: Dict[str, Any]) -> Dict[str, Any]:
    """Deep-merge two dictionaries without mutating the originals."""
    result: Dict[str, Any] = dict(base)
    for key, value in extra.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge_dict(result[key], value)
        else:
            result[key] = value
    return result


def parse_partitions(path: PurePosixPath) -> Dict[str, str]:
    """Extract partition key/value pairs from a relative path."""
    parts: Dict[str, str] = {}
    for segment in path.parts:
        if is_partition_segment(segment):
            key, value = segment.split("=", 1)
            parts[key] = value
    return parts


def load_geo_metadata(parquet_file: pq.ParquetFile) -> Tuple[Optional[str], Optional[Tuple[float, float, float, float]]]:
    """Extract primary geometry column and bbox from GeoParquet metadata."""
    schema = parquet_file.schema_arrow
    metadata = schema.metadata or {}
    raw_geo = metadata.get(b"geo")
    if not raw_geo:
        return None, None

    try:
        geo_json = json.loads(raw_geo.decode("utf-8"))
    except Exception:
        return None, None

    primary = geo_json.get("primary_column")
    bbox = None
    if primary:
        col_meta = geo_json.get("columns", {}).get(primary, {})
        col_bbox = col_meta.get("bbox")
        if isinstance(col_bbox, (list, tuple)) and len(col_bbox) == 4:
            bbox = tuple(float(x) for x in col_bbox)  # type: ignore[assignment]
    if bbox is None:
        top_bbox = geo_json.get("bbox")
        if isinstance(top_bbox, (list, tuple)) and len(top_bbox) == 4:
            bbox = tuple(float(x) for x in top_bbox)  # type: ignore[assignment]
    return primary, bbox


def scan_lon_lat_bbox(parquet_file: pq.ParquetFile) -> Optional[Tuple[float, float, float, float]]:
    """Compute bounding box by scanning lon/lat columns when geo metadata is absent."""
    candidate_lon_cols = ["lon", "longitude", "x"]
    candidate_lat_cols = ["lat", "latitude", "y"]
    schema_names = parquet_file.schema_arrow.names

    lon_col = next((col for col in candidate_lon_cols if col in schema_names), None)
    lat_col = next((col for col in candidate_lat_cols if col in schema_names), None)
    if not lon_col or not lat_col:
        return None

    min_lon = min_lat = float("inf")
    max_lon = max_lat = float("-inf")

    for rg in range(parquet_file.num_row_groups):
        batch = parquet_file.read_row_group(rg, columns=[lon_col, lat_col])
        lon_arr = batch.column(lon_col).to_numpy()
        lat_arr = batch.column(lat_col).to_numpy()
        if lon_arr.size == 0 or lat_arr.size == 0:
            continue
        min_lon = min(min_lon, float(lon_arr.min()))
        max_lon = max(max_lon, float(lon_arr.max()))
        min_lat = min(min_lat, float(lat_arr.min()))
        max_lat = max(max_lat, float(lat_arr.max()))

    if min_lon == float("inf") or min_lat == float("inf"):
        return None
    return (min_lon, min_lat, max_lon, max_lat)


def read_timestamp_bounds(parquet_file: pq.ParquetFile) -> Tuple[Optional[datetime], Optional[datetime]]:
    """Try to infer min/max datetime from timestamp/date columns in a Parquet file."""
    schema = parquet_file.schema_arrow
    col_candidates = [
        ("timestamp",),
        ("datetime",),
        ("time",),
        ("date",),
        ("start_datetime", "end_datetime"),
    ]

    present: List[Tuple[str, ...]] = []
    for candidate in col_candidates:
        if all(name in schema.names for name in candidate):
            present.append(candidate)
    if not present:
        return None, None

    def parse_array(arr) -> List[datetime]:
        parsed: List[datetime] = []
        for value in arr:
            if value is None:
                continue
            if isinstance(value, datetime):
                parsed.append(ensure_utc(value))
            elif isinstance(value, str):
                try:
                    parsed.append(parse_datetime(value))
                except Exception:
                    continue
        return parsed

    for names in present:
        if len(names) == 2:
            first_col, last_col = names
            data = parquet_file.read_row_groups(
                list(range(parquet_file.num_row_groups)), columns=list(names)
            )
            first_values = parse_array(data.column(first_col).to_pylist())
            last_values = parse_array(data.column(last_col).to_pylist())
            if first_values and last_values:
                return min(first_values), max(last_values)
        else:
            col = names[0]
            data = parquet_file.read_row_groups(
                list(range(parquet_file.num_row_groups)), columns=[col]
            )
            values = parse_array(data.column(col).to_pylist())
            if values:
                return min(values), max(values)
    return None, None


def partition_datetime(partitions: Dict[str, str]) -> Tuple[Optional[datetime], Optional[datetime]]:
    """Infer datetime from Hive-style partitions like year=2025/month=01/day=01/hour=00."""
    try:
        year = int(partitions["year"])
        month = int(partitions.get("month", "1"))
        day = int(partitions.get("day", "1"))
        hour = int(partitions.get("hour", "0"))
    except (KeyError, ValueError):
        return None, None
    start = datetime(year, month, day, hour, tzinfo=timezone.utc)
    end = start + timedelta(hours=1)
    return start, end


def ensure_dir(path: str) -> None:
    """Create output directory if needed."""
    os.makedirs(path, exist_ok=True)


def format_datetime(dt: Optional[datetime]) -> Optional[str]:
    """Return ISO string with Z suffix, or None."""
    if dt is None:
        return None
    return ensure_utc(dt).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class AssetRecord:
    relative_path: PurePosixPath
    partitions: Dict[str, str]
    bbox: Optional[Tuple[float, float, float, float]]
    start: Optional[datetime]
    end: Optional[datetime]
    row_count: int
    primary_geom: Optional[str]


def discover_assets(fs: pafs.FileSystem, base_path: str, extension_filter: Iterable[str]) -> List[AssetRecord]:
    """Walk the filesystem and collect asset metadata."""
    try:
        selector = pafs.FileSelector(base_dir=base_path, recursive=True)
    except TypeError:
        # PyArrow < 13 uses positional arguments (base_dir, recursive, allow_not_found)
        selector = pafs.FileSelector(base_path, True)
    records: List[AssetRecord] = []

    for info in fs.get_file_info(selector):
        if info.type != pafs.FileType.File:
            continue
        if not any(info.path.endswith(ext) for ext in extension_filter):
            continue

        rel_path = PurePosixPath(os.path.relpath(info.path, base_path))
        partitions = parse_partitions(rel_path)
        pf = pq.ParquetFile(info.path, filesystem=fs)
        primary_geom, bbox = load_geo_metadata(pf)
        if bbox is None:
            bbox = scan_lon_lat_bbox(pf)
        start, end = read_timestamp_bounds(pf)
        if start is None or end is None:
            start_part, end_part = partition_datetime(partitions)
            start = start or start_part
            end = end or end_part
        records.append(
            AssetRecord(
                relative_path=rel_path,
                partitions=partitions,
                bbox=bbox,
                start=ensure_utc(start),
                end=ensure_utc(end),
                row_count=pf.metadata.num_rows if pf.metadata else 0,
                primary_geom=primary_geom,
            )
        )
    records.sort(key=lambda r: str(r.relative_path))
    return records


def load_existing_items(catalog_dir: Path) -> List[Item]:
    """Load previously generated STAC items from a catalog directory."""
    catalog_path = catalog_dir / "catalog.json"
    if not catalog_path.exists():
        return []
    try:
        catalog = Catalog.from_file(str(catalog_path))
    except Exception as exc:
        sys.stderr.write(
            f"Warning: unable to load existing catalog at {catalog_path}: {exc}\n"
        )
        return []
    return list(catalog.get_items())


# ---------------------------------------------------------------------------
# Catalog builder
# ---------------------------------------------------------------------------

def make_item_id(record: AssetRecord) -> str:
    """Generate a stable item identifier from partitions or filename."""
    keys = ["year", "month", "day", "hour"]
    if all(k in record.partitions for k in keys):
        return f"{record.partitions['year']}{record.partitions['month']}{record.partitions['day']}T{record.partitions['hour']}"
    stem = record.relative_path.stem.replace(".", "-")
    return stem.lower()


def create_collection(args: argparse.Namespace) -> Collection:
    """Build pystac Collection from parsed CLI arguments."""
    spatial_extent = pystac.SpatialExtent([[float("nan")] * 4])  # placeholder bbox
    start_override = parse_datetime(args.temporal_start) if args.temporal_start else None
    end_override = parse_datetime(args.temporal_end) if args.temporal_end else None
    temporal_extent = pystac.TemporalExtent([[start_override, end_override]])
    extent = Extent(spatial=spatial_extent, temporal=temporal_extent)
    collection = Collection(
        id=args.collection_id,
        description=args.collection_description or args.collection_title,
        extent=extent,
        title=args.collection_title,
        license=args.license,
    )
    if args.region:
        collection.extra_fields["region"] = args.region
    if args.temporal_start or args.temporal_end:
        collection.extra_fields["period"] = {
            "start": format_datetime(start_override),
            "end": format_datetime(end_override),
        }
    return collection


def update_collection_extent(collection: Collection,
                             bbox: Optional[Tuple[float, float, float, float]],
                             start: Optional[datetime],
                             end: Optional[datetime]) -> None:
    """Update the collection extent accumulators from an individual item."""
    extent = collection.extent
    if bbox:
        existing = extent.spatial.bboxes[0]
        if existing[0] != existing[0]:  # NaN check
            extent.spatial.bboxes[0] = list(bbox)
        else:
            extent.spatial.bboxes[0] = list(merge_bbox(tuple(existing), bbox))  # type: ignore[arg-type]
    start = ensure_utc(start)
    end = ensure_utc(end)
    temporal = extent.temporal.intervals[0]
    if start:
        if temporal[0] is None or start < temporal[0]:
            temporal[0] = start
    if end:
        if temporal[1] is None or end > temporal[1]:
            temporal[1] = end


def attach_table_metadata(item: Item, parquet_file: pq.ParquetFile) -> None:
    """Populate the STAC table extension with column metadata."""
    schema = parquet_file.schema_arrow
    columns = []
    for field in schema:
        pa_type = field.type
        if pa.types.is_integer(pa_type):
            stac_type = "integer"
        elif pa.types.is_floating(pa_type):
            stac_type = "number"
        elif pa.types.is_boolean(pa_type):
            stac_type = "boolean"
        elif pa.types.is_timestamp(pa_type):
            stac_type = "datetime"
        elif pa.types.is_date(pa_type):
            stac_type = "date"
        elif pa.types.is_string(pa_type) or pa.types.is_large_string(pa_type):
            stac_type = "string"
        else:
            stac_type = "string"
        description = describe_column(field.name, stac_type)
        try:
            columns.append(Column(field.name, stac_type, description=description))
        except TypeError:
            # Column signature may vary; attempt alternate positional call or fallback to dict.
            try:
                columns.append(Column(field.name, stac_type, description))
            except TypeError:
                columns.append(
                    {
                        "name": field.name,
                        "type": stac_type,
                        "description": description,
                    }
                )
    table_ext = TableExtension.ext(item, add_if_missing=True)
    table_ext.columns = columns


def read_constant_column(parquet_file: pq.ParquetFile, column_name: str) -> Optional[Any]:
    """Return the first value of a Parquet column without loading the full dataset."""
    try:
        schema = parquet_file.schema_arrow
        if schema.get_field_index(column_name) == -1:
            return None
    except AttributeError:
        return None

    try:
        row_group = parquet_file.read_row_group(0, columns=[column_name])
    except (IndexError, OSError, ValueError):
        return None
    if row_group.num_rows == 0:
        return None
    column = row_group.column(0)
    if column.num_chunks > 0:
        for chunk in column.chunks:
            if len(chunk):
                return chunk[0].as_py()
        return None
    try:
        return column[0].as_py()
    except IndexError:
        return None


def build_items_and_collection(
    args: argparse.Namespace,
    item_overrides: Optional[Dict[str, Any]] = None,
    collection_overrides: Optional[Dict[str, Any]] = None,
) -> Collection:
    """Main builder coordinating discovery, item creation, and collection save."""
    try:
        fs, base_path = pafs.FileSystem.from_uri(args.input_root)
    except ValueError:
        fs = pafs.LocalFileSystem()
        base_path = args.input_root

    metadata_fs: Optional[pafs.FileSystem] = None
    metadata_base: Optional[str] = None
    if args.metadata_prefix:
        metadata_fs, metadata_base = resolve_fs_and_base(args.metadata_prefix)

    plots_fs: Optional[pafs.FileSystem] = None
    plots_base: Optional[str] = None
    if args.plots_prefix:
        plots_fs, plots_base = resolve_fs_and_base(args.plots_prefix)

    info = fs.get_file_info(base_path)
    if info.type != pafs.FileType.Directory:
        sys.exit(f"Input root {args.input_root} is not a directory or prefix.")

    records = discover_assets(fs, base_path, extension_filter=(".parquet", ".geoparquet"))
    if not records:
        sys.exit(f"No GeoParquet assets found under {args.input_root}.")

    collection = create_collection(args)

    if collection_overrides:
        try:
            base_dict = collection.to_dict(include_self_link=False)
        except TypeError:
            base_dict = collection.to_dict()
        merged_collection = deep_merge_dict(base_dict, collection_overrides)
        collection = Collection.from_dict(merged_collection)
    assets_root = Path(args.output_dir) / "assets"
    parquet_assets_root = assets_root / "parquet"
    metadata_assets_root = assets_root / "metadata"
    plots_assets_root = assets_root / "plots"

    parquet_catalog = Catalog(
        id=f"{args.collection_id}-parquet",
        description="GeoParquet assets produced by the HF-EOLUS interpolation pipeline.",
        title="Parquet Assets",
    )
    metadata_catalog = Catalog(
        id=f"{args.collection_id}-metadata",
        description="Metadata sidecars associated with each interpolated partition.",
        title="Metadata Assets",
    )
    plots_catalog = Catalog(
        id=f"{args.collection_id}-plots",
        description="Diagnostic plots generated during the HF-EOLUS interpolation pipeline.",
        title="Plot Assets",
    )

    has_parquet = False
    has_metadata = False
    has_plots = False
    discovered_models: Set[str] = set()
    if getattr(args, "incremental", False):
        existing_collection_path = Path(args.output_dir) / "collection.json"
        if existing_collection_path.exists():
            try:
                existing_coll = json.loads(existing_collection_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                existing_coll = None
            if existing_coll:
                existing_models = existing_coll.get("source_models")
                if isinstance(existing_models, list):
                    discovered_models.update(str(model) for model in existing_models)

    parquet_items_root = Path(args.output_dir) / "items" / "parquet"
    metadata_items_root = Path(args.output_dir) / "items" / "metadata"
    plots_items_root = Path(args.output_dir) / "items" / "plots"
    parquet_catalog_root = Path(args.output_dir) / f"{args.collection_id}-parquet"
    metadata_catalog_root = Path(args.output_dir) / f"{args.collection_id}-metadata"
    plots_catalog_root = Path(args.output_dir) / f"{args.collection_id}-plots"

    existing_parquet_ids: Set[str] = set()
    existing_metadata_ids: Set[str] = set()
    existing_plot_ids: Set[str] = set()

    if getattr(args, "incremental", False):
        for existing_item in load_existing_items(parquet_items_root):
            parquet_catalog.add_item(existing_item)
            has_parquet = True
            existing_parquet_ids.add(existing_item.id)
            bbox_tuple = tuple(existing_item.bbox) if existing_item.bbox else None
            cm = existing_item.common_metadata
            update_collection_extent(collection, bbox_tuple, cm.start_datetime, cm.end_datetime)
            if isinstance(existing_item.properties, dict):
                source_model = existing_item.properties.get("source_model")
                if source_model:
                    discovered_models.add(str(source_model))
        for existing_item in load_existing_items(metadata_items_root):
            metadata_catalog.add_item(existing_item)
            has_metadata = True
            existing_metadata_ids.add(existing_item.id)
        for existing_item in load_existing_items(plots_items_root):
            plots_catalog.add_item(existing_item)
            has_plots = True
            existing_plot_ids.add(existing_item.id)

    for record in records:
        item_id = make_item_id(record)
        bbox = record.bbox
        if bbox is None:
            sys.stderr.write(
                f"Warning: asset {record.relative_path} does not provide a bounding box; "
                "item geometry will be omitted.\n"
            )
        geom = bbox_to_polygon(bbox) if bbox else None
        dt = record.start or record.end

        if getattr(args, "incremental", False) and item_id in existing_parquet_ids:
            # Asset already cataloged; skip heavy copy and rebuild.
            continue

        source_parquet_path = os.path.join(base_path, str(record.relative_path))
        local_parquet_path = parquet_assets_root / record.relative_path
        if not copy_from_fs(fs, source_parquet_path, local_parquet_path):
            sys.exit(f"Failed to copy {source_parquet_path} into assets directory.")

        pf = pq.ParquetFile(source_parquet_path, filesystem=fs)
        source_model_value = read_constant_column(pf, "source_model")
        parquet_item = Item(
            id=item_id,
            geometry=geom,
            bbox=list(bbox) if bbox else None,
            datetime=dt,
            properties={},
        )
        if source_model_value:
            parquet_item.properties["source_model"] = source_model_value
            discovered_models.add(str(source_model_value))
        if record.start:
            parquet_item.common_metadata.start_datetime = record.start
        if record.end:
            parquet_item.common_metadata.end_datetime = record.end

        attach_table_metadata(parquet_item, pf)

        if record.row_count is not None:
            parquet_item.properties["row_count"] = record.row_count
        if record.primary_geom:
            parquet_item.properties["primary_geometry_column"] = record.primary_geom

        parquet_item_dir = Path(parquet_items_root) / item_id
        asset_href = os.path.relpath(local_parquet_path, parquet_item_dir).replace(os.sep, "/")
        parquet_item.add_asset(
            "data",
            Asset(
                href=asset_href,
                media_type="application/vnd.apache.parquet",
                roles=["data"],
                title="Interpolated winds GeoParquet",
            ),
        )

        if item_overrides:
            base_dict = parquet_item.to_dict(include_self_link=False)
            original_asset_hrefs = {
                key: asset.get("href") for key, asset in base_dict.get("assets", {}).items()
            }
            merged_dict = deep_merge_dict(base_dict, item_overrides)
            for key, href in original_asset_hrefs.items():
                if key in merged_dict.get("assets", {}) and "href" not in merged_dict["assets"][key] and href is not None:
                    merged_dict["assets"][key]["href"] = href
            parquet_item = Item.from_dict(merged_dict)

        update_collection_extent(
            collection,
            tuple(parquet_item.bbox) if parquet_item.bbox else bbox,
            parquet_item.common_metadata.start_datetime or record.start,
            parquet_item.common_metadata.end_datetime or record.end,
        )
        parquet_catalog.add_item(parquet_item)
        has_parquet = True
        existing_parquet_ids.add(item_id)

        if metadata_fs is not None and metadata_base is not None:
            metadata_rel = record.relative_path.parent / "metadata.json"
            metadata_item_id = f"{item_id}-metadata"
            if getattr(args, "incremental", False) and metadata_item_id in existing_metadata_ids:
                pass
            else:
                metadata_source = os.path.join(metadata_base, str(metadata_rel))
                local_metadata_path = metadata_assets_root / metadata_rel
                if copy_from_fs(metadata_fs, metadata_source, local_metadata_path):
                    metadata_item = Item(
                        id=metadata_item_id,
                        geometry=geom,
                        bbox=list(bbox) if bbox else None,
                        datetime=dt,
                        properties={"asset_type": "metadata"},
                    )
                    if record.start:
                        metadata_item.common_metadata.start_datetime = record.start
                    if record.end:
                        metadata_item.common_metadata.end_datetime = record.end

                    metadata_item_dir = Path(metadata_items_root) / metadata_item_id
                    metadata_href = os.path.relpath(local_metadata_path, metadata_item_dir).replace(os.sep, "/")
                    metadata_item.add_asset(
                        "metadata",
                        Asset(
                            href=metadata_href,
                            media_type="application/json",
                            roles=["metadata"],
                            title="Interpolation metadata",
                        ),
                    )
                    if item_overrides:
                        base_md = metadata_item.to_dict(include_self_link=False)
                        md_asset_hrefs = {
                            key: asset.get("href") for key, asset in base_md.get("assets", {}).items()
                        }
                        merged_md = deep_merge_dict(base_md, item_overrides)
                        for key, href in md_asset_hrefs.items():
                            if (
                                key in merged_md.get("assets", {})
                                and "href" not in merged_md["assets"][key]
                                and href is not None
                            ):
                                merged_md["assets"][key]["href"] = href
                        metadata_item = Item.from_dict(merged_md)
                    metadata_item.add_link(
                        Link(
                            rel="describes",
                            target=parquet_item,
                            media_type="application/geo+json",
                            title=parquet_item.id,
                        )
                    )
                    parquet_item.add_link(
                        Link(
                            rel="describedby",
                            target=metadata_item,
                            media_type="application/geo+json",
                            title=metadata_item.id,
                        )
                    )
                    metadata_catalog.add_item(metadata_item)
                    existing_metadata_ids.add(metadata_item.id)
                    has_metadata = True

        if plots_fs is not None and plots_base is not None:
            plot_dir = PurePosixPath(plots_base) / record.relative_path.parent
            try:
                selector = pafs.FileSelector(base_dir=str(plot_dir), recursive=False)
                plot_infos = plots_fs.get_file_info(selector)
            except Exception:
                plot_infos = []
            for plot_info in plot_infos:
                if plot_info.type != pafs.FileType.File:
                    continue
                if not plot_info.path.lower().endswith(".png"):
                    continue
                rel_plot = relative_from_base(plot_info.path, plots_base)
                local_plot_path = plots_assets_root / rel_plot
                if not copy_from_fs(plots_fs, plot_info.path, local_plot_path):
                    continue
                plot_item_id = f"{item_id}-plot-{PurePosixPath(rel_plot).stem}"
                if getattr(args, "incremental", False) and plot_item_id in existing_plot_ids:
                    continue
                plot_filename = PurePosixPath(rel_plot).name
                plot_description, plot_props = describe_plot_asset(plot_filename)
                plot_item = Item(
                    id=plot_item_id,
                    geometry=geom,
                    bbox=list(bbox) if bbox else None,
                    datetime=dt,
                    properties={"plot_name": plot_filename},
                )
                if plot_description:
                    plot_item.properties["plot_description"] = plot_description
                for key, value in plot_props.items():
                    plot_item.properties[key] = value
                if record.start:
                    plot_item.common_metadata.start_datetime = record.start
                if record.end:
                    plot_item.common_metadata.end_datetime = record.end

                plot_item_dir = Path(plots_items_root) / plot_item_id
                plot_href = os.path.relpath(local_plot_path, plot_item_dir).replace(os.sep, "/")
                plot_item.add_asset(
                    "plot",
                    Asset(
                        href=plot_href,
                        media_type="image/png",
                        roles=["overview"],
                        title=plot_filename,
                        description=plot_description,
                    ),
                )
                if item_overrides:
                    base_plot = plot_item.to_dict(include_self_link=False)
                    plot_asset_hrefs = {
                        key: asset.get("href") for key, asset in base_plot.get("assets", {}).items()
                    }
                    merged_plot = deep_merge_dict(base_plot, item_overrides)
                    for key, href in plot_asset_hrefs.items():
                        if (
                            key in merged_plot.get("assets", {})
                            and "href" not in merged_plot["assets"][key]
                            and href is not None
                        ):
                            merged_plot["assets"][key]["href"] = href
                    plot_item = Item.from_dict(merged_plot)
                if plot_description:
                    plot_item.properties.setdefault("plot_description", plot_description)
                for key, value in plot_props.items():
                    plot_item.properties.setdefault(key, value)
                if plot_description and "plot" in plot_item.assets:
                    plot_item.assets["plot"].description = plot_description
                plot_item.add_link(
                    Link(
                        rel="related",
                        target=parquet_item,
                        media_type="application/geo+json",
                        title=parquet_item.id,
                    )
                )
                parquet_item.add_link(
                    Link(
                        rel="related",
                        target=plot_item,
                        media_type="application/geo+json",
                        title=plot_item.id,
                    )
                )
                plots_catalog.add_item(plot_item)
                existing_plot_ids.add(plot_item.id)
                has_plots = True

    ensure_dir(args.output_dir)

    collection_href = Path(args.output_dir) / "collection.json"
    ensure_dir(str(collection_href.parent))
    collection.set_self_href(str(collection_href))

    if has_parquet:
        ensure_dir(str(parquet_assets_root))
        ensure_dir(str(parquet_items_root))
        if parquet_catalog_root.exists():
            shutil.rmtree(parquet_catalog_root)
        parquet_catalog.normalize_hrefs(str(parquet_catalog_root))
        parquet_catalog.set_self_href(str(parquet_catalog_root / "catalog.json"))
        collection.add_child(parquet_catalog)
    if has_metadata:
        ensure_dir(str(metadata_assets_root))
        ensure_dir(str(metadata_items_root))
        if metadata_catalog_root.exists():
            shutil.rmtree(metadata_catalog_root)
        metadata_catalog.normalize_hrefs(str(metadata_catalog_root))
        metadata_catalog.set_self_href(str(metadata_catalog_root / "catalog.json"))
        collection.add_child(metadata_catalog)
    if has_plots:
        ensure_dir(str(plots_assets_root))
        ensure_dir(str(plots_items_root))
        if plots_catalog_root.exists():
            shutil.rmtree(plots_catalog_root)
        plots_catalog.normalize_hrefs(str(plots_catalog_root))
        plots_catalog.set_self_href(str(plots_catalog_root / "catalog.json"))
        collection.add_child(plots_catalog)

    # Fill collection.period extras when not provided
    start, end = collection.extent.temporal.intervals[0]
    period = collection.extra_fields.get("period", {})
    if not period:
        period = {}
    if start and not period.get("start"):
        period["start"] = format_datetime(start)
    if end and not period.get("end"):
        period["end"] = format_datetime(end)
    if period:
        collection.extra_fields["period"] = period

    if discovered_models:
        existing_models = collection.extra_fields.get("source_models")
        if isinstance(existing_models, list):
            discovered_models.update(str(v) for v in existing_models)
        collection.extra_fields["source_models"] = sorted(discovered_models)

    collection.save(catalog_type=CatalogType.RELATIVE_PUBLISHED)
    collection_json_path = Path(args.output_dir) / "collection.json"
    try:
        coll_data_cleanup = json.loads(collection_json_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        coll_data_cleanup = None
    if coll_data_cleanup and "links" in coll_data_cleanup:
        filtered = [link for link in coll_data_cleanup["links"] if link.get("rel") != "self"]
        if len(filtered) != len(coll_data_cleanup["links"]):
            coll_data_cleanup["links"] = filtered
            collection_json_path.write_text(json.dumps(coll_data_cleanup, indent=2), encoding="utf-8")

    def relocate_subcatalog(child_id: str, target_subdir: str) -> None:
        src_dir = Path(args.output_dir) / child_id
        if not src_dir.exists():
            return
        dest_dir = Path(args.output_dir) / target_subdir
        if dest_dir.exists():
            shutil.rmtree(dest_dir)
        ensure_dir(str(dest_dir.parent))
        shutil.move(str(src_dir), str(dest_dir))

        catalog_path = dest_dir / "catalog.json"
        if catalog_path.exists():
            data = json.loads(catalog_path.read_text(encoding="utf-8"))
            for link in data.get("links", []):
                rel = link.get("rel")
                if rel == "self":
                    link["href"] = "catalog.json"
                elif rel == "parent":
                    link["href"] = "../../collection.json"
                elif rel == "root":
                    link["href"] = "../../collection.json"
            catalog_path.write_text(json.dumps(data, indent=2), encoding="utf-8")

        rel_href = os.path.relpath(catalog_path, Path(args.output_dir))
        try:
            coll_data = json.loads((Path(args.output_dir) / "collection.json").read_text(encoding="utf-8"))
        except FileNotFoundError:
            return

        updated = False
        for link in coll_data.get("links", []):
            if link.get("rel") == "child" and link.get("href") == f"./{child_id}/catalog.json":
                link["href"] = "./" + rel_href.replace(os.sep, "/")
                updated = True
        if updated:
            (Path(args.output_dir) / "collection.json").write_text(json.dumps(coll_data, indent=2), encoding="utf-8")

    relocate_subcatalog(f"{args.collection_id}-parquet", "items/parquet")
    relocate_subcatalog(f"{args.collection_id}-metadata", "items/metadata")
    relocate_subcatalog(f"{args.collection_id}-plots", "items/plots")

    return collection


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a STAC Collection for GeoParquet interpolation outputs."
    )
    parser.add_argument(
        "--input-root",
        required=True,
        help="Directory or URI where partitioned GeoParquet files are stored (local path or s3:// prefix).",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Destination directory for the STAC catalog (collection.json, items, assets).",
    )
    parser.add_argument(
        "--collection-id",
        required=True,
        help="Identifier for the STAC Collection.",
    )
    parser.add_argument(
        "--collection-title",
        required=True,
        help="Human-friendly title for the Collection.",
    )
    parser.add_argument(
        "--collection-description",
        default="",
        help="Long-form description for the Collection (defaults to title).",
    )
    parser.add_argument(
        "--region",
        default=None,
        help="Optional region name to embed in Collection extra fields.",
    )
    parser.add_argument(
        "--temporal-start",
        default=None,
        help="Override collection start datetime (ISO-8601).",
    )
    parser.add_argument(
        "--temporal-end",
        default=None,
        help="Override collection end datetime (ISO-8601).",
    )
    parser.add_argument(
        "--asset-href-prefix",
        default=None,
        help="Optional prefix to prepend to asset HREFs (e.g. s3://bucket/interpolation).",
    )
    parser.add_argument(
        "--copy-assets",
        action="store_true",
        help="Copy GeoParquet files into output_dir/assets for a self-contained catalog.",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Skip assets that already exist under output_dir and only append newly discovered partitions.",
    )
    parser.add_argument(
        "--item-overrides",
        default=None,
        help="Path to a JSON file merged into every Item (useful to override properties/assets).",
    )
    parser.add_argument(
        "--collection-overrides",
        default=None,
        help="Path to a JSON file merged into the Collection metadata (keywords, providers, extra fields, etc.).",
    )
    parser.add_argument(
        "--metadata-prefix",
        default=None,
        help="Optional prefix containing metadata sidecars (e.g. s3://bucket/meteogalicia/metadata/interpolation).",
    )
    parser.add_argument(
        "--plots-prefix",
        default=None,
        help="Optional prefix containing diagnostic plots (PNG) grouped by the same partitions.",
    )
    parser.add_argument(
        "--license",
        default="proprietary",
        help="Collection license string (defaults to 'proprietary').",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:  # pragma: no cover - CLI
    args = parse_args(argv)
    if args.temporal_start:
        parse_datetime(args.temporal_start)  # validate format
    if args.temporal_end:
        parse_datetime(args.temporal_end)

    overrides_dict: Optional[Dict[str, Any]] = None
    if args.item_overrides:
        overrides_path = Path(args.item_overrides)
        if not overrides_path.exists():
            sys.exit(f"Item overrides file not found: {args.item_overrides}")
        try:
            overrides_dict = json.loads(overrides_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            sys.exit(f"Failed to parse item overrides JSON ({args.item_overrides}): {exc}")

    collection_overrides_dict: Optional[Dict[str, Any]] = None
    if args.collection_overrides:
        collection_path = Path(args.collection_overrides)
        if not collection_path.exists():
            sys.exit(f"Collection overrides file not found: {args.collection_overrides}")
        try:
            collection_overrides_dict = json.loads(collection_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            sys.exit(
                f"Failed to parse collection overrides JSON ({args.collection_overrides}): {exc}"
            )

    collection = build_items_and_collection(args, overrides_dict, collection_overrides_dict)

    # Report summary to user
    total_items = len(list(collection.get_items()))
    bbox = collection.extent.spatial.bboxes[0]
    start, end = collection.extent.temporal.intervals[0]
    print(f"STAC collection saved to {args.output_dir}")
    print(f"- Items: {total_items}")
    if bbox[0] == bbox[0]:  # check for NaNs
        print(f"- Spatial extent: {bbox}")
    if start or end:
        start_str = format_datetime(start)
        end_str = format_datetime(end)
        print(f"- Temporal extent: {start_str} -> {end_str}")


if __name__ == "__main__":  # pragma: no cover
    main()
