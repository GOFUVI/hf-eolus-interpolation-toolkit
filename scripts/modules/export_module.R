#' Export helpers for GeoParquet generation and metadata sidecars.
#'
#' @details This module transforms the interpolated spatial objects into an
#'   `sf` layer, enriches it with diagnostic columns and persists the results
#'   alongside their metadata sidecars.
NULL

.calc_dir <- function(u, v) {
  ang <- atan2(-u, -v) * 180 / pi
  ifelse(ang < 0, ang + 360, ang)
}

#' Prepare the interpolated grid for export as an sf object.
#'
#' @param grid_sp SpatialPointsDataFrame returned by
#'   `perform_interpolation_suite()`.
#' @param original_proj CRS of the source data.
#' @param region_sf_original Region polygon used for filtering.
#' @param metadata List containing contextual information (date, hour, counts,
#'   models, metrics, etc.).
#' @param verbose Logical enabling verbose logging.
#' @return List with the filtered sf object and the interpolated count.
prepare_output_sf <- function(grid_sp,
                              original_proj,
                              region_sf_original,
                              metadata,
                              verbose) {
  grid_coords_df <- as.data.frame(grid_sp)
  coords_xy_local <- extract_geometry(grid_sp)
  colnames(coords_xy_local) <- c("x_local", "y_local")
  grid_coords_df$x_local <- coords_xy_local$x_local
  grid_coords_df$y_local <- coords_xy_local$y_local
  grid_sf <- sf::st_as_sf(grid_coords_df, coords = c("x", "y"), crs = original_proj)
  log_verbose(verbose, "Converted grid coordinates to sf with original projection")
  .print_verbose <- function(x) if (isTRUE(verbose)) { print(x); flush.console() }
  .print_verbose(head(grid_sf, n = 6))

  coords_xy <- extract_geometry(grid_sf)
  grid_sf <- sf::st_transform(grid_sf, crs = "+proj=longlat +datum=WGS84 +no_defs")
  grid_sf <- cbind(grid_sf, coords_xy)
  grid_sf$wind_speed <- sqrt(grid_sf$u^2 + grid_sf$v^2)
  grid_sf$wind_dir <- .calc_dir(grid_sf$u, grid_sf$v)
  grid_sf$date <- metadata$date_str
  grid_sf$hour <- metadata$hour_str
  timestamp_iso <- sprintf("%sT%02d:00:00Z", metadata$date_str, metadata$hour_val)
  grid_sf$timestamp <- timestamp_iso

  params_u <- extract_variogram_params(metadata$models$u)
  params_v <- extract_variogram_params(metadata$models$v)
  grid_sf$vgm_model_u <- params_u$model
  grid_sf$vgm_range_u <- params_u$range
  grid_sf$vgm_sill_u <- params_u$sill
  grid_sf$vgm_nugget_u <- params_u$nugget
  grid_sf$vgm_model_v <- params_v$model
  grid_sf$vgm_range_v <- params_v$range
  grid_sf$vgm_sill_v <- params_v$sill
  grid_sf$vgm_nugget_v <- params_v$nugget

  grid_sf$input_count <- metadata$input_count
  selected_u <- metadata$cv_metrics$u$selected %||% NA_character_
  selected_v <- metadata$cv_metrics$v$selected %||% NA_character_
  grid_sf$cv_model_u <- selected_u
  grid_sf$cv_model_v <- selected_v
  grid_sf$cv_rsr_u <- if (!is.na(selected_u) && nzchar(selected_u)) metadata$cv_metrics$u$rsr[[selected_u]] %||% NA_real_ else NA_real_
  grid_sf$cv_bias_u <- if (!is.na(selected_u) && nzchar(selected_u)) metadata$cv_metrics$u$bias[[selected_u]] %||% NA_real_ else NA_real_
  grid_sf$cv_rsr_v <- if (!is.na(selected_v) && nzchar(selected_v)) metadata$cv_metrics$v$rsr[[selected_v]] %||% NA_real_ else NA_real_
  grid_sf$cv_bias_v <- if (!is.na(selected_v) && nzchar(selected_v)) metadata$cv_metrics$v$bias[[selected_v]] %||% NA_real_ else NA_real_
  grid_sf$test_model_u <- grid_sf$cv_model_u
  grid_sf$test_rsr_u <- metadata$test_metrics$rsr_u
  grid_sf$test_bias_u <- metadata$test_metrics$bias_u
  grid_sf$test_model_v <- grid_sf$cv_model_v
  grid_sf$test_rsr_v <- metadata$test_metrics$rsr_v
  grid_sf$test_bias_v <- metadata$test_metrics$bias_v

  keep_idx <- sf::st_intersects(grid_sf, region_sf_original, sparse = FALSE)[, 1]
  if (!any(keep_idx)) {
    stop("No interpolated grid points fall within the defined region. Nothing to plot or save.")
  }
  grid_sf <- grid_sf[keep_idx, , drop = FALSE]
  interpolated_count_value <- sum(!grid_sf$is_orig, na.rm = TRUE)
  grid_sf$interpolated_count <- interpolated_count_value
  log_verbose(verbose, "Filtered grid to user-defined region in WGS84")
  .print_verbose(head(grid_sf, n = 6))

  list(
    grid_sf = grid_sf,
    interpolated_count = interpolated_count_value
  )
}

#' Persist the GeoParquet dataset and its metadata sidecar.
#'
#' @param grid_sf sf object prepared with `prepare_output_sf()`.
#' @param output_base Base output path (local or S3).
#' @param partition List with `year`, `month`, `day`, `hour`.
#' @param ingest Ingestion metadata list.
#' @param export_meta List containing `nc_proj_string_value`, `regions_meta`,
#'   `test_points_sidecar`, `source_url_value`, `region_name_used`.
#' @param aws_cfg List with `has_aws_cli` and `aws_region_args`.
#' @param verbose Logical controlling logging.
#' @return List with local and remote artifact paths.
write_geo_outputs <- function(grid_sf,
                              output_base,
                              partition,
                              ingest,
                              export_meta,
                              aws_cfg,
                              verbose) {
  remote_base <- sub("/+$", "", output_base)
  local_base <- tempdir()
  remote_part <- file.path(remote_base,
                           sprintf("year=%04d", partition$year),
                           sprintf("month=%02d", partition$month),
                           sprintf("day=%02d", partition$day),
                           sprintf("hour=%02d", partition$hour))
  local_part <- file.path(local_base,
                          sprintf("year=%04d", partition$year),
                          sprintf("month=%02d", partition$month),
                          sprintf("day=%02d", partition$day),
                          sprintf("hour=%02d", partition$hour))
  dir.create(local_part, recursive = TRUE, showWarnings = FALSE)
  geo_file_local <- file.path(local_part, "data.parquet")
  geo_file_remote <- file.path(remote_part, "data.parquet")

  message("Writing GeoParquet via Python (pyarrow + pandas) through reticulate")
  df_export <- sf::st_drop_geometry(grid_sf)
  df_export$geometry <- sf::st_as_binary(sf::st_geometry(grid_sf))
  pandas <- reticulate_import("pandas", convert = FALSE)
  pyarrow <- reticulate_import("pyarrow", convert = FALSE)
  parquet <- reticulate_import("pyarrow.parquet", convert = FALSE)
  py_table <- pyarrow$Table$from_pandas(pandas$DataFrame(df_export), preserve_index = FALSE)
  parquet$write_table(py_table, geo_file_local, compression = "snappy")

  netcdf_attrs <- ingest$ingest_sidecar$netcdf_attributes
  if (is.null(netcdf_attrs) || length(netcdf_attrs) == 0) {
    netcdf_attrs <- list()
  }
  sidecar_payload <- list(
    crs = export_meta$nc_proj_string_value,
    nc_proj_string = export_meta$nc_proj_string_value,
    netcdf_attributes = netcdf_attrs,
    generated = list(
      script = "wind_interpolation.R",
      timestamp = format(Sys.time(), tz = "UTC", usetz = TRUE)
    )
  )
  if (!is.null(export_meta$regions_meta) && length(export_meta$regions_meta) > 0) {
    sidecar_payload$regions <- export_meta$regions_meta
  }
  if (!is.null(export_meta$test_points_sidecar) && length(export_meta$test_points_sidecar) > 0) {
    sidecar_payload$test_points <- export_meta$test_points_sidecar
  }
  if (is_nonempty_string(export_meta$source_url_value)) {
    sidecar_payload$source_url <- as.character(export_meta$source_url_value)[1]
  }
  if (!is.null(export_meta$region_name_used) && nzchar(export_meta$region_name_used)) {
    sidecar_payload$region_name <- export_meta$region_name_used
  }

  metadata_output_prefix <- sidecar_prefix_for(remote_base)
  sidecar_remote_path <- join_paths(
    metadata_output_prefix,
    sprintf("year=%04d", partition$year),
    sprintf("month=%02d", partition$month),
    sprintf("day=%02d", partition$day),
    sprintf("hour=%02d", partition$hour),
    "metadata.json"
  )
  metadata_local_root <- file.path(tempdir(), "metadata_staging", basename(remote_base))
  sidecar_local_dir <- file.path(
    metadata_local_root,
    sprintf("year=%04d", partition$year),
    sprintf("month=%02d", partition$month),
    sprintf("day=%02d", partition$day),
    sprintf("hour=%02d", partition$hour)
  )
  dir.create(sidecar_local_dir, recursive = TRUE, showWarnings = FALSE)
  sidecar_local_path <- file.path(sidecar_local_dir, "metadata.json")
  jsonlite::write_json(sidecar_payload, sidecar_local_path, auto_unbox = TRUE, pretty = TRUE)

  if (grepl("^s3://", remote_base) && isTRUE(aws_cfg$has_aws_cli)) {
    log_verbose(verbose, sprintf("Uploading GeoParquet to %s", geo_file_remote))
    rc <- system2("aws", c("s3", "cp", geo_file_local, geo_file_remote, aws_cfg$aws_region_args %||% character()))
    if (rc != 0) {
      stop(sprintf("Failed to upload %s to %s (exit %d)", geo_file_local, geo_file_remote, rc))
    }
  }
  if (grepl("^s3://", sidecar_remote_path)) {
    if (isTRUE(aws_cfg$has_aws_cli)) {
      log_verbose(verbose, sprintf("Uploading metadata sidecar to %s", sidecar_remote_path))
      rc <- system2("aws", c("s3", "cp", sidecar_local_path, sidecar_remote_path, aws_cfg$aws_region_args %||% character()))
      if (rc != 0) {
        stop(sprintf("Failed to upload %s to %s (exit %d)", sidecar_local_path, sidecar_remote_path, rc))
      }
    } else {
      warning(sprintf("AWS CLI not available; skipping metadata sidecar upload to %s", sidecar_remote_path))
    }
  }

  list(
    geo_file_local = geo_file_local,
    geo_file_remote = geo_file_remote,
    sidecar_local_path = sidecar_local_path,
    sidecar_remote_path = sidecar_remote_path
  )
}
