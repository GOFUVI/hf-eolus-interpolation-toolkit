# Wind Interpolation Toolkit

[![CI](https://github.com/GOFUVI/hf-eolus-interpolation-toolkit/actions/workflows/ci.yml/badge.svg)](https://github.com/GOFUVI/hf-eolus-interpolation-toolkit/actions/workflows/ci.yml)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.17131227.svg)](https://doi.org/10.5281/zenodo.17131227)

This toolkit scales the wind-interpolation methodology originally prototyped for the Ría de Vigo (Fernández-Baladrón et al., 2020) so that it can run reproducibly on AWS. It ingests MeteoGalicia WRF model outputs, applies spatial interpolation (IDW, regression kriging, universal kriging), evaluates them against in situ observations, and publishes the resulting GeoParquet products using STAC catalogs.

## AWS Architecture (high level)

- **Ingestion** – AWS Step Functions triggers a Lambda function that downloads MeteoGalicia NetCDF files, applies optional polygon filters, converts wind variables to hourly Parquet partitions and uploads them to S3. The same job publishes per-hour metadata sidecars.
- **Interpolation** – AWS Batch launches containerised R jobs (scripts `07`/`08`) that build refinement grids, train kriging/IDW models, compute diagnostics (RSR, bias, LOOCV) and generate quadrant plots. Results are written back to S3 as GeoParquet plus metadata and diagnostic graphics.
- **Publication** – `scripts/run_build_stac_catalog.sh` wraps the STAC builder inside Docker, optionally syncing input/output prefixes from S3, and emits a collection plus hourly items linking data, metadata and plots. Overrides allow you to inject additional JSON fragments without editing the generator.

## Quick start

### Prerequisites

- AWS CLI configured with a profile that can manage IAM, Step Functions, Lambda, Batch and S3.
- Docker (used for the STAC builder and, optionally, for local interpolation runs).
- R 4.3+ with `renv` (CLI scripts rely on the locked package set).
- Python 3.11+ for helper scripts.

### Setup

```bash
# 1) Clone and restore dependencies
git clone https://github.com/GOFUVI/hf-eolus-interpolation-toolkit.git
cd hf-eolus-interpolation-toolkit
Rscript -e "renv::restore()"

# 2) (Optional) run tests locally
bash scripts/00-verify_tests.sh

# 3) Configure pipeline variables (replace values as needed)
export PIPELINE_PROFILE=default
export PIPELINE_BUCKET_NAME=hf-eolus
export PIPELINE_INGEST_START=2025-01-01
export PIPELINE_INGEST_END=2025-01-02
export PIPELINE_BOUNDARY_FILE=/path/to/case-study/area_boundary.geojson
export PIPELINE_TEST_POINTS_FILE=/path/to/case-study/test_points.csv
```

### Launch the end-to-end workflow

```bash
./run_pipeline.sh
```

The script reads `PIPELINE_*` variables (see below) and executes the entire sequence: tests, IAM roles, Lambda deployment, Step Functions ingestion, Batch interpolation and STAC publishing.

## Configuring `run_pipeline.sh`

`run_pipeline.sh` requires a handful of environment variables. The most frequently used ones are listed below; every option also supports an equivalent `PIPELINE_*` variable.

| Variable | Purpose | Default |
| --- | --- | --- |
| `PIPELINE_PROFILE` | AWS CLI profile used by every command | `default`
| `PIPELINE_AWS_REGION` | Region for AWS resources | profile default or `eu-west-3`
| `PIPELINE_BUCKET_NAME` | Bucket that stores ingestion outputs, interpolation results and metadata | **required**
| `PIPELINE_INGEST_START`, `PIPELINE_INGEST_END` | Date range (`YYYY-MM-DD`) for MeteoGalicia downloads | **required**
| `PIPELINE_MODEL` | MeteoGalicia grid identifier (`wrf4km`, `wrf1km`, etc.) | `wrf4km`
| `PIPELINE_BOUNDARY_FILE` | Optional GeoJSON polygon to clip ingestion | unset
| `PIPELINE_TEST_POINTS_FILE` | Optional CSV of validation points used downstream | unset
| `PIPELINE_INPUT_PREFIX` | S3 prefix containing hourly Parquet (ingestion output) | `s3://${PIPELINE_BUCKET_NAME}/meteogalicia/data/`
| `PIPELINE_OUTPUT_PREFIX` | Destination for interpolation GeoParquet | `s3://${PIPELINE_BUCKET_NAME}/meteogalicia/interpolation`
| `PIPELINE_PLOTS_PREFIX` | Destination for plots (quadrants, diagnostics) | `s3://${PIPELINE_BUCKET_NAME}/meteogalicia/interp_plots`
| `PIPELINE_METADATA_PREFIX` | Destination for metadata sidecars | `s3://${PIPELINE_BUCKET_NAME}/meteogalicia/metadata/interpolation`
| `PIPELINE_RES_FACTOR`, `PIPELINE_CUTOFF_KM`, `PIPELINE_WIDTH_KM`, `PIPELINE_SUBSAMPLE_PCT`, `PIPELINE_NMAX_MODEL`, `PIPELINE_NFOLD` | Interpolation hyperparameters | tuned per run
| `PIPELINE_BUOY_CONFIG` | Path to a JSON plan describing one or more buoys to compare against (enables the optional report step) | unset (disabled)
| `PIPELINE_BUOY_REPORTS_DIR` | Directory (relative to the repository) where reports are written as `reports/<buoy>/` | `reports/buoys`
| `PIPELINE_BUOY_PREDICTION_PATH` | Local dataset root/file used by the comparison script (defaults to the synced GeoParquet under `local_sync/`) | `local_sync`

Unset values fall back to sensible defaults; missing required values trigger an immediate error.

## Workflow breakdown

1. **Tests (`scripts/00-verify_tests.sh`)** – runs the R unit tests and publishes the coverage summary under `reports/pipeline/`.
2. **IAM Roles (`scripts/01-create_roles.sh`)** – creates/updates the Lambda and Batch IAM roles using the selected profile and bucket.
3. **Lambda Deployment (`scripts/02-deploy_lambda.sh`)** – builds/pushes the ingestion container to ECR and updates the Lambda function. The script waits until it becomes active.
4. **State Machine (`scripts/03-create_state_machine.sh`)** – deploys the Step Functions state machine referencing the Lambda function and bucket paths.
5. **Ingestion (`scripts/04-run_pipeline.sh`)** – launches the Step Functions execution for the requested date range, polygon and model.
6. **Batch Setup (`scripts/07-setup_aws_resources.sh`)** – ensures the AWS Batch compute environment, job queue and job definition exist.
7. **Interpolation (`scripts/08-run_interpolation.sh`)** – submits the Batch job(s) that execute `scripts/wind_interpolation.R` with the configured hyperparameters.
8. **Catalog Publishing (`scripts/run_build_stac_catalog.sh`)** – syncs the output prefixes (unless `--skip-sync`), runs the STAC builder inside Docker and writes the resulting collection/items to the directory specified by `PIPELINE_STAC_OUTPUT_DIR` (for example `catalogs/meteogalicia_interpolation`).

Each CLI accepts `--help` for full option listings. The same scripts can be invoked individually to re-run single stages.

## Optional buoy comparisons

After publishing the STAC catalog you can automatically compare the interpolated predictions against one or more in situ buoys. Enable the step by pointing `PIPELINE_BUOY_CONFIG` to a JSON file that lists each buoy, its STAC catalog and the `node_id` used in the interpolation outputs:

```json
[
  {
    "id": "vilano",
    "catalog": "case_study/catalogs/pde_vilano_buoy/collection.json",
    "item_id": "Vilano",
    "node_id": "Vilano_buoy",
    "output_subdir": "Vilano"
  }
]
```

When `PIPELINE_BUOY_CONFIG` is set, `run_pipeline.sh` executes `scripts/compare_pde_buoy.R` once per entry, storing metrics (RMSE, RSR, bias), aligned CSVs, plots and Markdown reports under `PIPELINE_BUOY_REPORTS_DIR/<buoy>/` (defaults to `reports/buoys/<buoy>/`). The comparison script reads the GeoParquet predictions from `PIPELINE_BUOY_PREDICTION_PATH`; by default it targets `local_sync/`, the directory populated by `scripts/run_build_stac_catalog.sh` whenever it synchronises the interpolation outputs from S3.

The repository ships `case_study/buoy_comparison_config.json` with the PdE Vilano buoy definition so that case-study reproductions can enable the step with a single flag (see the case study README below).

## STAC catalog generation

Use `scripts/run_build_stac_catalog.sh` to wrap `scripts/11-build_stac_catalog.py` inside Docker:

```bash
scripts/run_build_stac_catalog.sh \
  --aws-profile "$PIPELINE_PROFILE" \
  --input-root "$PIPELINE_OUTPUT_PREFIX" \
  --output-dir catalogs/meteogalicia_interpolation \
  --collection-id meteogalicia-interpolation \
  --collection-title "MeteoGalicia Interpolation" \
  --metadata-prefix "$PIPELINE_METADATA_PREFIX" \
  --plots-prefix "$PIPELINE_PLOTS_PREFIX" \
  --item-overrides path/to/case-study/stac_overrides/item_override.json \
  --collection-overrides path/to/case-study/stac_overrides/collection_override.json
```

The wrapper automatically syncs S3 prefixes (data, metadata, plots) to `local_sync*` folders, mounts the repository into the container, and cleans up temporary directories. Override files let you add extra properties or assets without modifying the generator. If the target catalog already exists, compare manifests (for example with `diff -u <(find catalog -type f -print | sort) ...`) before publishing.

## Reproducibility and case studies

Case studies are published in a separate repository that stores the boundary files, validation points, STAC overrides and reference catalogs (for example, `hf-eolus-task-3-interpolation-outputs`). Clone that repository alongside the toolkit, configure the `PIPELINE_*` variables to point to the case-study assets, and run its `run_pipeline_case_study.sh` wrapper, explicitly passing the bucket and region label you want to use:

```bash
/path/to/case-study/run_pipeline_case_study.sh \
  --toolkit-dir /path/to/hf-eolus-interpolation-toolkit \
  --bucket <your-s3-bucket> \
  --region-name <your-region-label>
```

When reproducing the published Vilano dataset, replace the placeholders with the case-specific values (`hf-eolus` and `VILA-PRIO-HF`). Requiring explicit arguments prevents the toolkit scripts from silently reusing those identifiers in other deployments.

The wrapper exports all required variables, invokes every toolkit script sequentially and verifies that the regenerated STAC catalog matches the committed reference stored in the case-study repository.

By default it keeps the published copy of `case_study/catalogs/meteogalicia_interpolation` intact; pass `--force-overwrite-stac` only when you deliberately want to replace it after a successful verification. Each execution ends with a summary indicating whether the catalog comparison passed and where the buoy reports were written (if `PIPELINE_BUOY_CONFIG` is set).

## Citation

If you use this toolkit in research, please cite the Zenodo record: Fernández-Baladrón, A., Varela Benvenuto, R., & Herrera Cortijo, J. L. (2020). *Interrelationships between Surface Circulation and Wind in the Ría de Vigo.* Zenodo. https://doi.org/10.5281/zenodo.17490675.


## Acknowledgements

This work has been funded by the HF-EOLUS project (TED2021-129551B-I00), financed by MICIU/AEI /10.13039/501100011033 and by the European Union NextGenerationEU/PRTR - BDNS 598843 - Component 17 - Investment I3. Members of the Marine Research Centre (CIM) of the University of Vigo have participated in the development of this repository.



## Disclaimer

This software is provided "as is", without warranty of any kind, express or implied, including but not limited to the warranties of merchantability, fitness for a particular purpose, and noninfringement. In no event shall the authors or copyright holders be liable for any claim, damages, or other liability, whether in an action of contract, tort, or otherwise, arising from, out of, or in connection with the software or the use or other dealings in the software.



---
<p align="center">
  <a href="https://next-generation-eu.europa.eu/">
    <img src="logos/EN_Funded_by_the_European_Union_RGB_POS.png" alt="Funded by the European Union" height="80"/>
  </a>
  <a href="https://planderecuperacion.gob.es/">
    <img src="logos/LOGO%20COLOR.png" alt="Logo Color" height="80"/>
  </a>
  <a href="https://www.aei.gob.es/">
    <img src="logos/logo_aei.png" alt="AEI Logo" height="80"/>
  </a>
  <a href="https://www.ciencia.gob.es/">
    <img src="logos/MCIU_header.svg" alt="MCIU Header" height="80"/>
  </a>
  <a href="https://cim.uvigo.gal">
    <img src="logos/Logotipo_CIM_original.png" alt="CIM logo" height="80"/>
  </a>
  <a href="https://www.iim.csic.es/">
    <img src="logos/IIM.svg" alt="IIM logo" height="80"/>
  </a>

  
</p>
