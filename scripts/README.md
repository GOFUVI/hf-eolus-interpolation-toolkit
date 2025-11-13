# Wind Interpolation Scripts

This directory contains scripts to perform spatial interpolation of wind data using R.

## Contents
- `wind_interpolation.R`: Main R script to read Parquet input, convert wind speed/direction to U/V components, perform IDW and universal kriging interpolation using a Gaussian variogram model, compute validation metrics (RSR and bias), transform UTM to lat/long, save and upload empirical variogram plots for U and V components as PNG files to the specified output path, generate and upload a side-by-side wind map PNG comparing original and interpolated wind vectors, and write results as partitioned Parquet files to S3.
  Plots are also organized into folders partitioned by year, month, day, and hour under a separate S3 root path specified via the `--plots-root` argument (defaults to the main output path).
- `08-run_interpolation.sh`: Bash script to execute the R script with command-line arguments, setting AWS credentials and region for S3 access.
- `07-setup_aws_resources.sh`: Bash script to create an S3 bucket (if it doesn't exist) and an IAM role with limited access to that bucket.
- `10-update_geoparquet_regions.py`: Python script to add or remove region definitions in a GeoParquet file by updating the `metadata/.../metadata.json` sidecar (and legacy `regions` columns when present); supports `--add` to append a region JSON object and `--remove` to delete a region by name.
- `11-build_stac_catalog.py`: Python CLI that inspects the interpolation GeoParquet outputs (local folders or S3 prefixes) and generates a STAC Collection with one Item per partition. The script guarantees the mandatory STAC fields (`bbox`, `datetime`, `assets`) and can optionally copy the GeoParquet payloads into the catalog for offline distribution.
- `run_build_stac_catalog.sh`: Shell wrapper that builds (on first use) a Docker image with the Python dependencies (`pyarrow`, `pystac`, `shapely`), sincroniza (por defecto) los prefijos S3 de datos, metadatos y plots hacia disco local con `aws s3 sync`, y finalmente invoca `11-build_stac_catalog.py` dentro del contenedor.

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

### Docker wrapper (recommended para sincronizar S3 y generar el catálogo localmente)

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

El wrapper:
- Construye (si no existe) la imagen `wind-interpolation-stac:latest` con `pyarrow`, `pystac` y `shapely`.
- Si `--input-root` o `--sync-prefix` apuntan a `s3://`, ejecuta `aws s3 sync` hacia `local_sync/` dentro del repositorio (o el directorio pasado con `--sync-target`). Puedes indicar el perfil con `--aws-profile hf_eolus`; la región se obtiene automáticamente de la configuración de ese perfil (puedes forzarla con `--aws-region eu-west-3`).
- Cuando `--metadata-prefix` o `--plots-prefix` señalan a S3 y no se usa `--skip-sync`, replica automáticamente esos prefijos en `<sync-target>_metadata/` y `<sync-target>_plots/` (o `local_sync_metadata/` y `local_sync_plots/` por defecto) y pasa las rutas locales al contenedor.
- Lanza el script Python dentro del contenedor utilizando la ruta local sincronizada.

Opciones adicionales:
- `--sync-prefix s3://bucket/prefix`: define explícitamente el prefijo a sincronizar (si se omite y `--input-root` es un S3 URI, se usa automáticamente).
- `--sync-target <ruta>`: cambia el directorio local donde se replica el prefijo (por defecto `local_sync/`).
- `--skip-sync`: desactiva la sincronización (útil si ya tienes los datos descargados); asegúrate entonces de que `--input-root` apunta a la copia local.
- `--aws-profile <perfil>`: ejecuta el `aws s3 sync` con ese perfil y deriva la región por defecto de su configuración.
- `--aws-region <región>`: fuerza la región utilizada durante la sincronización (sobrescribe la obtenida del perfil o de variables de entorno).
- `--metadata-prefix <prefijo>`: ruta (local o S3) con los metadata.json generados por la interpolación, se añaden como assets con rol `metadata`. Si el prefijo es S3 (y no se usa `--skip-sync`), se copia automáticamente a `<sync-target>_metadata/`.
- `--plots-prefix <prefijo>`: ruta con los PNG de diagnósticos (grid, variogramas, etc.) para añadirlos como assets `overview`. Con prefijos S3 se replica en `<sync-target>_plots/` cuando la sincronización está activa.
- `--item-overrides <ruta.json>`: fusiona el JSON indicado sobre todos los Items (datos, metadatos y plots) antes de guardarlos (se preservan los `href` de los assets generados por el script).
- `--collection-overrides <ruta.json>`: fusiona el JSON indicado en la Collection resultante (título, keywords, providers, extra_fields, etc.).

Los JSON de ejemplo para overrides se encuentran en `case_study/stac_overrides/`.

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
```

To inject custom STAC metadata, provide JSON files with the structures to merge (for example `case_study/stac_overrides/item_override_example.json` for Items or `case_study/stac_overrides/collection_override_example.json` for the Collection) and pass them with `--item-overrides` / `--collection-overrides` (the merge is deep and respects standard STAC fields).

Key behaviour:
- `bbox` is populated from GeoParquet metadata when present, or by scanning `lon`/`lat` columns.
- `datetime`, `start_datetime`, and `end_datetime` are derived from `timestamp` columns or from `year=/month=/day=/hour=` partitions.
- Data, metadata JSON and diagnostic plots are copied into `assets/` (parquet/metadata/plots) so the catalog is self-contained; corresponding Items are grouped under `items/parquet`, `items/metadata`, and `items/plots`, each with its own subcatalog.
- `--asset-href-prefix` rewrites Item asset URLs when you prefer remote HREFs instead of copying assets.
- `--metadata-prefix`/`--plots-prefix` copy the matching sidecars into `assets/metadata/` and `assets/plots/` and expose them as overview catalogs.

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
