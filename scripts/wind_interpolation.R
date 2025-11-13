# wind_interpolation.R
# Orchestrates the interpolation workflow by combining ingestion, grid
# generation, spatial interpolation and GeoParquet export modules.

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(phylin)
  library(gstat)
  library(sp)
  library(sf)
  library(Metrics)
  library(jsonlite)
  library(parallel)
  library(RANN)
  library(reticulate)
  use_condaenv("base", required = TRUE)
})

get_script_dir <- function() {
  args <- commandArgs(trailingOnly = FALSE)
  file_pattern <- "^--file="
  file_arg <- grep(file_pattern, args, value = TRUE)
  if (length(file_arg)) {
    return(dirname(normalizePath(sub(file_pattern, "", file_arg[1]))))
  }
  if (!is.null(sys.frames()[[1]]$ofile)) {
    return(dirname(normalizePath(sys.frames()[[1]]$ofile)))
  }
  getwd()
}

script_dir <- get_script_dir()
module_path <- function(name) file.path(script_dir, "modules", name)

source(module_path("utils_module.R"))
source(module_path("ingestion_module.R"))
source(module_path("grid_module.R"))
source(module_path("interpolation_module.R"))
source(module_path("export_module.R"))

cli <- parse_interpolation_cli(commandArgs(trailingOnly = TRUE))
positional <- cli$positional
verbose <- cli$verbose
no_plots <- cli$no_plots

vmessage <- function(...) log_verbose(verbose, ...)
vprint <- function(x) if (isTRUE(verbose)) { print(x); flush.console() }

has_aws_cli <- nzchar(Sys.which("aws"))
if (!has_aws_cli && verbose) {
  warning("AWS CLI not found; skipping S3 uploads.")
}

aws_region <- cli$aws_region
if (is.null(aws_region) || aws_region == "") {
  aws_region <- Sys.getenv("AWS_REGION", Sys.getenv("AWS_DEFAULT_REGION", ""))
}
aws_region_args <- if (nzchar(aws_region)) c("--region", aws_region) else character()
vmessage("Using AWS CLI region: ", aws_region)

ingestion <- load_partition_dataset(
  positional,
  cli$region_name,
  cli$swap_latlon,
  aws_region_args,
  has_aws_cli,
  verbose
)
n_pts <- nrow(ingestion$data)

grid <- build_interpolation_grid(
  data = ingestion$data,
  unique_x_local = ingestion$unique_x_local,
  unique_y_local = ingestion$unique_y_local,
  min_orig_x = ingestion$min_orig_x,
  min_orig_y = ingestion$min_orig_y,
  res_factor = positional$res_factor,
  original_proj = ingestion$original_proj,
  test_points_df = ingestion$test_points_df,
  verbose = verbose
)

plot_remote_base <- if (!is.null(cli$plots_root)) cli$plots_root else positional$output_path
plot_remote_part <- file.path(
  plot_remote_base,
  sprintf("year=%04d", ingestion$year_val),
  sprintf("month=%02d", ingestion$month_val),
  sprintf("day=%02d", ingestion$day_val),
  sprintf("hour=%02d", ingestion$hour_val)
)
plot_local_part <- file.path(
  tempdir(),
  sprintf("year=%04d", ingestion$year_val),
  sprintf("month=%02d", ingestion$month_val),
  sprintf("day=%02d", ingestion$day_val),
  sprintf("hour=%02d", ingestion$hour_val)
)
if (!no_plots) {
  dir.create(plot_local_part, recursive = TRUE, showWarnings = FALSE)
}

plot_cfg <- list(
  local_part = plot_local_part,
  remote_part = plot_remote_part,
  remote_base = plot_remote_base,
  has_aws_cli = has_aws_cli,
  aws_region_args = aws_region_args,
  upload = !no_plots
)

interp_params <- list(
  subsample_pct = positional$subsample_pct,
  cutoff_km = positional$cutoff_km,
  width_km = positional$width_km,
  nfold_cv = positional$nfold_cv,
  nmax_model = positional$nmax_model,
  no_plots = no_plots,
  verbose = verbose,
  date_str = positional$date_str,
  hour_str = positional$hour_str,
  hour_val = ingestion$hour_val,
  plot_cfg = plot_cfg,
  test_point_names = grid$test_point_names,
  orig_points_df = grid$orig_points_df,
  test_pct = 10,
  data_count = n_pts
)

interp <- perform_interpolation_suite(
  data = grid$data_sp,
  grid_coords = grid$grid_sp,
  new_grid_coords = grid$new_grid_sp,
  params = interp_params
)

metadata <- list(
  date_str = positional$date_str,
  hour_str = positional$hour_str,
  hour_val = ingestion$hour_val,
  models = interp$models,
  input_count = n_pts,
  cv_metrics = interp$cv_metrics,
  test_metrics = interp$test_metrics
)

prepared <- prepare_output_sf(
  grid_sp = interp$grid_coords,
  original_proj = ingestion$original_proj,
  region_sf_original = ingestion$region_sf_original,
  metadata = metadata,
  verbose = verbose
)
grid_sf <- prepared$grid_sf

partition <- list(
  year = ingestion$year_val,
  month = ingestion$month_val,
  day = ingestion$day_val,
  hour = ingestion$hour_val
)

export_meta <- list(
  nc_proj_string_value = ingestion$nc_proj_string_value,
  regions_meta = ingestion$regions_meta,
  test_points_sidecar = ingestion$test_points_sidecar,
  source_url_value = ingestion$source_url_value,
  region_name_used = ingestion$region_name_used
)

aws_cfg <- list(
  has_aws_cli = has_aws_cli,
  aws_region_args = aws_region_args
)

output_base <- sub("/+$", "", positional$output_path)

artifacts <- write_geo_outputs(
  grid_sf = grid_sf,
  output_base = output_base,
  partition = partition,
  ingest = ingestion,
  export_meta = export_meta,
  aws_cfg = aws_cfg,
  verbose = verbose
)

vmessage("GeoParquet local path: ", artifacts$geo_file_local)
if (!is.null(artifacts$geo_file_remote)) {
  vmessage("GeoParquet remote path: ", artifacts$geo_file_remote)
}
vmessage("Interpolation workflow completed successfully.")
