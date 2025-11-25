# Wind Interpolation Scripts

This directory contains scripts to perform spatial interpolation of wind data using R.

## Contents
- `wind_interpolation.R`: Main R script to read Parquet input, convert wind speed/direction to U/V components, perform IDW and universal kriging interpolation using a Gaussian variogram model, compute validation metrics (RSR and bias), transform UTM to lat/long, save and upload empirical variogram plots for U and V components as PNG files to the specified output path, generate and upload a side-by-side wind map PNG comparing original and interpolated wind vectors, and write results as partitioned Parquet files to S3.
  Plots are also organized into folders partitioned by year, month, day, and hour under a separate S3 root path specified via the `--plots-root` argument (defaults to the main output path).
- `08-run_interpolation.sh`: Bash script to execute the R script with command-line arguments, setting AWS credentials and region for S3 access.
- `07-setup_aws_resources.sh`: Bash script to create an S3 bucket (if it doesn't exist) and an IAM role with limited access to that bucket.
- `10-update_geoparquet_regions.py`: Python script to add or remove region definitions in a GeoParquet file by updating the `metadata/.../metadata.json` sidecar (and legacy `regions` columns when present); supports `--add` to append a region JSON object and `--remove` to delete a region by name.
- `11-build_stac_catalog.py`: Python CLI that inspects the interpolation GeoParquet outputs (local folders or S3 prefixes) and generates a STAC Collection with one Item per partition. The script guarantees the mandatory STAC fields (`bbox`, `datetime`, `assets`) and can optionally copy the GeoParquet payloads into the catalog for offline distribution.
- `12-subset_stac_nodes.py`: Python CLI that, given a STAC catalog of interpolation outputs, filters rows by `node_id` and/or a Polygon/MultiPolygon geometry using DuckDB (Docker `duckdb/duckdb:latest` or local fallback), writes a single Parquet/GeoParquet with the subset, and emits a derived STAC collection that inherits the original metadata and registers itself in the parent `catalog.json` when present.
- `run_build_stac_catalog.sh`: Shell wrapper that builds (on first use) a Docker image with the Python dependencies (`pyarrow`, `pystac`, `shapely`), synchronizes (by default) the S3 prefixes for data, metadata, and plots to local disk with `aws s3 sync`, and finally invokes `11-build_stac_catalog.py` inside the container.
- `run_subset_stac_nodes.sh`: Lightweight wrapper that reuses the STAC catalog Docker image (or builds it if missing) and runs `12-subset_stac_nodes.py` inside the container, avoiding any dependence on host Python libraries.

## Prerequisites
- R 4.0 or higher *or* Docker.
- AWS CLI configured with a profile (default profile `default` is used by default).
- Wind data in Parquet format stored in S3, with columns: `x`, `y` (UTM coordinates in kilometers), `velocidad` (wind speed), `direccion` (wind direction in degrees from north).
- (Optional) AWS IAM permissions to create S3 buckets and roles if using `setup_aws_resources.sh`.

## Installation

### Local Environment
1. Install system dependencies:
   ```bash
   sudo apt-get update
   sudo apt-get install -y libgdal-dev libgeos-dev libproj-dev libudunits2-dev libcurl4-openssl-dev libssl-dev
   ```
2. Install R packages in R console:
   ```r
   install.packages(c('phylin','gstat','Metrics','arrow','sf','sp'))
   ```
3. Install and configure AWS CLI (`aws configure`).

### Docker Environment
1. Build the Docker image from the project root (now includes AWS CLI for S3 uploads):
   ```bash
   docker build -t wind-interpolation .
   ```
2. (Optional) Verify installation:
   ```bash
   docker run --rm -it wind-interpolation R --version
   ```

## AWS Setup
Use `07-setup_aws_resources.sh` to create (if needed) the S3 bucket and IAM role:
```bash
scripts/07-setup_aws_resources.sh -p default -r eu-west-3 -b <bucket_name> -n <role_name>
```

## Running Interpolation
Execute `08-run_interpolation.sh` with the desired parameters:
```bash
scripts/08-run_interpolation.sh -p default -r <res_factor> -c <cutoff_km> -w <width_km> -n <subsample_pct> -F <n_fold> -m <nmax_model> -H <hour> [--region-name <name>] [--plots-root <plots_s3_path>] [--swap-latlon] [-v|--verbose] <date> <input_s3_path> <output_s3_path>
```
Example:
```bash
scripts/08-run_interpolation.sh -p default -r 2 -c 5 -w 0.5 -n 10 -H 14 \
  --plots-root s3://my-bucket/plots_interpolated/ \
  2025-05-14 s3://my-bucket/data_parquet/ s3://my-bucket/results_interpolated/
```

### Reproducible pipeline

For the design workflow discussed in the project, configure the `PIPELINE_*` environment variables (see `README.md`) and run from the repository root:

```bash
./run_pipeline.sh
```

The script sequentially provisions IAM roles, deploys the Lambda ingestion image, creates the Step Functions state machine, executes ingestion for the requested range, configures AWS Batch, runs the interpolation batch, and finishes by generating the STAC catalog for the published outputs (schema registration in Athena/Glue is now handled manually outside the pipeline).

# Note: a fixed 10% hold-out test set is removed from the data before variogram estimation and CV, then interpolated using data excluding the test set to compute final RSR/bias metrics;
# the `-n <subsample_pct>` flag controls the percentage of the remaining data
# used for variogram estimation and cross-validation.

### Buoy comparison helper

`scripts/compare_pde_buoy.R` can be invoked on a STAC-described buoy GeoParquet and a prediction dataset (folder or file) to emit aligned pairs, metrics CSVs, plots and a Markdown summary. It now supports harmonising the buoy winds to 10 m using a neutral logarithmic profile via `--buoy-height-correction` plus `--source-height` (default 3), `--target-height` (default 10) and `--roughness` (default 0.0002). When enabled, the report and CSVs annotate the observation height used; flags can also be set per-entry when using `--buoy-config` for multi-buoy runs.

## Docker Execution
Alternatively, run within Docker:
```bash
docker run --rm -e AWS_PROFILE=default -e AWS_DEFAULT_REGION=eu-west-3 wind-interpolation \
  bash -c "scripts/08-run_interpolation.sh -r 2 -c 5 -w 0.5 -n 5000 -F <n_fold> -m <nmax_model> -H 14 [--region-name <name>] [--swap-latlon] [-v|--verbose] \
  2025-05-14 s3://my-bucket/data_parquet/ s3://my-bucket/results_interpolated/"
```

## Output
Results are saved as GeoParquet files partitioned by year/month/day/hour, with each partition containing a file named `data.parquet`. Besides the interpolated fields (`u`, `v`, `u_rkt`, `v_rkt`, `wind_speed`, `wind_dir`, geolocation, etc.), the dataset now exposes promoted metadata columns: `date`, `hour`, `timestamp`, `input_count`, `interpolated_count`, `cv_model_u/v`, `cv_rsr_u/v`, `cv_bias_u/v`, `test_model_u/v`, `test_rsr_u/v`, `test_bias_u/v`, `kriging_var_u/v`, `nearest_distance_km`, `neighbors_used`, `interpolation_source`, and variogram diagnostics (`vgm_model_u/v`, `vgm_range_u/v`, `vgm_sill_u/v`, `vgm_nugget_u/v`). Projection, region definitions, test points, and lineage (`source_url`) are published exclusively in the sidecar JSON under `metadata/<dataset>/.../metadata.json`.
- A wind map image `wind_map_<date>.png` is generated, showing original and interpolated wind vectors side by side, and uploaded to the specified output path.

## STAC Catalog Generation

Use either the Docker wrapper or the Python CLI to convert the partitioned GeoParquet outputs into a STAC Collection.

### Docker wrapper (recommended to sync S3 and generate the catalog locally)

```bash
scripts/run_build_stac_catalog.sh \
  --input-root local_out \
  --output-dir catalogs/interpolated_galicia_2025 \
  --collection-id interpolated-galicia-2025 \
  --collection-title "Interpolated Winds - Galicia 2025" \
  --region Galicia \
  --temporal-start 2025-01-01T00:00:00Z \
  --temporal-end 2025-01-31T23:00:00Z
```

The wrapper:
- Builds (if missing) the `wind-interpolation-stac:latest` image with `pyarrow`, `pystac`, and `shapely`.
- If `--input-root` or `--sync-prefix` points to `s3://`, runs `aws s3 sync` to `local_sync/` inside the repository (or the directory passed with `--sync-target`). You can pick the profile via `--aws-profile hf_eolus`; the region is inferred from that profile (or forced with `--aws-region eu-west-3`).
- When `--metadata-prefix` or `--plots-prefix` point to S3 and `--skip-sync` is not used, it mirrors those prefixes into `<sync-target>_metadata/` and `<sync-target>_plots/` (or `local_sync_metadata/` and `local_sync_plots/` by default) and passes the local paths to the container.
- Invokes the Python script inside the container using the locally synced paths.

Additional options:
- `--sync-prefix s3://bucket/prefix`: explicitly sets the prefix to sync (if omitted and `--input-root` is an S3 URI, it is inferred automatically).
- `--sync-target <path>`: directory where the prefix is mirrored locally (default `local_sync/`).
- `--skip-sync`: disables synchronization (useful if you already downloaded the data); ensure `--input-root` then points to the local copy.
- `--aws-profile <profile>`: runs `aws s3 sync` with that profile and derives the default region from its config.
- `--aws-region <region>`: forces the region used during sync (overrides the one from the profile or env variables).
- `--metadata-prefix <prefix>`: path (local or S3) to interpolation metadata JSON sidecars, added as `metadata` assets. If S3 (and `--skip-sync` is not set), they are copied automatically to `<sync-target>_metadata/`.
- `--plots-prefix <prefix>`: path to diagnostic PNGs (grid, variograms, etc.) to add as `overview` assets. S3 prefixes are mirrored to `<sync-target>_plots/` when sync is enabled.
- `--item-overrides <path.json>`: deep-merges the provided JSON into every Item (data, metadata, plots) before saving (keeping the generated asset `href` values).
- `--collection-overrides <path.json>`: deep-merges the provided JSON into the resulting Collection (title, keywords, providers, `extra_fields`, etc.).
- `--incremental`: skips partitions already cataloged under `output-dir`, keeps existing assets untouched, and only copies/emits Items for new GeoParquet files.
- `--by-year` (optionally `--years 2018,2019,...`): iterates one `year=YYYY` partition at a time, syncing only that year's assets/metadata/plots into a temporary staging folder (`<output-dir>/.stac_year_build`). Year detection scans local `year=*` folders or, for S3 inputs, runs `aws s3 ls` on the prefix. The wrapper auto-enables `--incremental`, keeps asset `href`s anchored to the original prefix via `--asset-href-prefix`, and rejects `--skip-sync` when any input prefix is on S3.
  If the target catalog already contains all Items for a given year (the Item count matches the source parquet count), that year is skipped automatically.

Example override JSON files live in `case_study/stac_overrides/`.

## Subsetting STAC outputs

`scripts/12-subset_stac_nodes.py` filters an interpolation catalog by `node_id` and/or geometry and emits:
- a single Parquet/GeoParquet with the subset of rows, and
- a derived STAC collection/item set registered under the parent `catalog.json` when present.

Recommended usage via Docker (keeps dependencies isolated):

```bash
scripts/run_subset_stac_nodes.sh \
  --catalog case_study/catalogs/meteogalicia_interpolation/catalog.json \
  --output-dir case_study/catalogs/meteogalicia_interpolation_subsets/vilano \
  --node-id Vilano_buoy \
  --polygon aoi.geojson
```

Key flags (also available directly in the Python CLI):
- `--catalog <path_or_url>`: root catalog or collection to read.
- `--output-dir <path>`: where the derived catalog and Parquet/GeoParquet will be written.
- `--node-id <name>[,<name>...]`: include only these `node_id` values.
- `--polygon <path>`: GeoJSON/JSON with Polygon or MultiPolygon to spatially clip rows.
- `--output-format parquet|geoparquet`: choose Parquet or GeoParquet output (defaults to GeoParquet if geometry is kept).
- `--aws-profile`, `--aws-region`: forwarded to the wrapper when reading from S3.

When running locally without Docker, install `pyarrow`, `pystac`, `shapely`, and `duckdb` and invoke the Python script with the same flags. The Docker wrapper mounts the repository to run entirely in-container and avoids host Python setup.

### Direct Python invocation

```bash
pip install pyarrow pystac

python scripts/11-build_stac_catalog.py \
  --input-root local_out \
  --output-dir catalogs/interpolated_galicia_2025 \
  --collection-id interpolated-galicia-2025 \
  --collection-title "Interpolated Winds - Galicia 2025" \
  --region Galicia \
  --temporal-start 2025-01-01T00:00:00Z \
  --temporal-end 2025-01-31T23:00:00Z

# Add `--incremental` when you only need to append new partitions without touching the existing Items.
```

To inject custom STAC metadata, provide JSON files with the structures to merge (for example `case_study/stac_overrides/item_override_example.json` for Items or `case_study/stac_overrides/collection_override_example.json` for the Collection) and pass them with `--item-overrides` / `--collection-overrides` (the merge is deep and respects standard STAC fields).

Key behaviour:
- `bbox` is populated from GeoParquet metadata when present, or by scanning `lon`/`lat` columns.
- `datetime`, `start_datetime`, and `end_datetime` are derived from `timestamp` columns or from `year=/month=/day=/hour=` partitions.
- Data, metadata JSON and diagnostic plots are copied into `assets/` (parquet/metadata/plots) so the catalog is self-contained; corresponding Items are grouped under `items/parquet`, `items/metadata`, and `items/plots`, each with its own subcatalog.
- `--asset-href-prefix` rewrites Item asset URLs when you prefer remote HREFs instead of copying assets.
- `--metadata-prefix`/`--plots-prefix` copy the matching sidecars into `assets/metadata/` and `assets/plots/` and expose them as overview catalogs.
- `--incremental` skips GeoParquet assets that already live under `output-dir`, so the generator only needs to copy and index the newly produced partitions (existing Items remain untouched on disk while the Collection metadata is refreshed).

After creating a new collection, add a new child link to `case_study/catalogs/catalog.json` so the top-level catalog advertises it without modifying the existing `case_study/catalogs/pde_vilano_buoy` structure.

### Publishing in STAC Browser

1. Generate the catalog as shown above and serve it locally:
   ```bash
   cd catalogs/interpolated_galicia_2025
   python3 -m http.server 8000
   ```
2. Launch STAC Browser pointing to the collection (Docker image works offline if already cached):
   ```bash
   docker run --rm -p 8080:8080 \
     -e CATALOG_URL=http://host.docker.internal:8000/collection.json \
     ghcr.io/stac-utils/stac-browser:v3.3.0
   ```
3. Open http://localhost:8080 and verify that the mandatory fields (`bbox`, `datetime`, `assets`) display correctly for each Item.
