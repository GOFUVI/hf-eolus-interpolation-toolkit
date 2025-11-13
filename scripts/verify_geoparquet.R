#!/usr/bin/env Rscript
# verify_geoparquet.R: Verify GeoParquet file for correct geometry and metadata.
#
# Usage:
#   Rscript verify_geoparquet.R <path_to_geoparquet>

args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 1) {
  stop("Usage: Rscript verify_geoparquet.R <path_to_geoparquet>")
}
file_path <- args[1]

if (!file.exists(file_path)) {
  stop("File does not exist: ", file_path)
}

library(reticulate)
use_condaenv("base", required = TRUE)
library(jsonlite)

cat("GeoParquet file:", file_path, "\n")

# Full inspection in Python via pyarrow and pandas
py_run_string(sprintf(
  "import pyarrow.parquet as pq\n\
import pandas as pd\n\
tbl = pq.read_table('%s')\n\
print('Schema:')\n\
print(tbl.schema)\n\
print('\\nMetadata:')\n\
if tbl.schema.metadata:\n\
    for k,v in tbl.schema.metadata.items(): print('  {}: {}'.format(k,v))\n\
else:\n\
    print('  <empty>')\n\
df = tbl.to_pandas()\n\
print('\\nDataFrame sample:')\n\
print(df.head())\n\
meta_cols = [col for col in ['date','hour','timestamp','input_count','interpolated_count','cv_model_u','cv_rsr_u','cv_bias_u','cv_model_v','cv_rsr_v','cv_bias_v','test_model_u','test_rsr_u','test_bias_u','test_model_v','test_rsr_v','test_bias_v','kriging_var_u','kriging_var_v','nearest_distance_km','neighbors_used','interpolation_source','vgm_model_u','vgm_range_u','vgm_sill_u','vgm_nugget_u','vgm_model_v','vgm_range_v','vgm_sill_v','vgm_nugget_v'] if col in df.columns]\n\
if meta_cols:\n\
    print('\\nPromoted metadata columns:')\n\
    print(df[meta_cols].head())\n\
else:\n\
    print('\\nPromoted metadata columns not present.')\n\
deprecated = [col for col in ['nc_proj_string','cv_all','test_metrics','regions','test_points','source_url'] if col in df.columns]\n\
if deprecated:\n\
    print('\\nDeprecated metadata columns still present:', deprecated)\n\
print('\\nRows: %%d, columns: %%d' %% (len(df), df.shape[1]))\n\
print('Geometry dtype:', df['geometry'].dtype)\n",
  file_path
))

# Attempt to load metadata sidecar aligned with promoted columns
hour_dir <- dirname(file_path)
day_dir <- dirname(hour_dir)
month_dir <- dirname(day_dir)
year_dir <- dirname(month_dir)
dataset_root <- dirname(year_dir)
metadata_root <- file.path(dirname(dataset_root), "metadata", basename(dataset_root))
sidecar_path <- file.path(metadata_root,
                          basename(year_dir),
                          basename(month_dir),
                          basename(day_dir),
                          basename(hour_dir),
                          "metadata.json")

cat("\nMetadata sidecar:\n")
if (file.exists(sidecar_path)) {
  cat("  Path: ", sidecar_path, "\n", sep = "")
  sidecar_contents <- read_json(sidecar_path, simplifyVector = FALSE)
  cat("  Keys: ", paste(names(sidecar_contents), collapse = ", "), "\n", sep = "")
} else {
  cat("  Not found at expected location: ", sidecar_path, "\n", sep = "")
}
