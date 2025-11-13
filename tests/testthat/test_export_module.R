build_export_context <- function() {
  fixture <- create_partition_fixture()
  ingestion <- modules_env$load_partition_dataset(
    positional = fixture$positional,
    region_name_cli = NULL,
    swap_latlon = FALSE,
    aws_region_args = character(),
    has_aws_cli = FALSE,
    verbose = FALSE
  )
  grid <- modules_env$build_interpolation_grid(
    data = ingestion$data,
    unique_x_local = ingestion$unique_x_local,
    unique_y_local = ingestion$unique_y_local,
    min_orig_x = ingestion$min_orig_x,
    min_orig_y = ingestion$min_orig_y,
    res_factor = fixture$positional$res_factor,
    original_proj = ingestion$original_proj,
    test_points_df = ingestion$test_points_df,
    verbose = FALSE
  )
  params <- list(
    subsample_pct = 75,
    cutoff_km = 5000,
    width_km = 500,
    nfold_cv = 2,
    nmax_model = 4,
    no_plots = TRUE,
    verbose = FALSE,
    date_str = fixture$positional$date_str,
    hour_str = fixture$positional$hour_str,
    hour_val = fixture$hour,
    plot_cfg = list(upload = FALSE),
    test_point_names = grid$test_point_names,
    orig_points_df = grid$orig_points_df,
    data_count = nrow(ingestion$data)
  )
  interp <- modules_env$perform_interpolation_suite(
    data = grid$data_sp,
    grid_coords = grid$grid_sp,
    new_grid_coords = grid$new_grid_sp,
    params = params
  )
  list(fixture = fixture, ingestion = ingestion, grid = grid, interp = interp)
}

test_that("prepare_output_sf enriches attributes", {
  ctx <- build_export_context()
  metadata <- list(
    date_str = ctx$fixture$positional$date_str,
    hour_str = ctx$fixture$positional$hour_str,
    hour_val = ctx$fixture$hour,
    models = ctx$interp$models,
    input_count = nrow(ctx$ingestion$data),
    cv_metrics = ctx$interp$cv_metrics,
    test_metrics = ctx$interp$test_metrics
  )
  prepared <- modules_env$prepare_output_sf(
    grid_sp = ctx$interp$grid_coords,
    original_proj = ctx$ingestion$original_proj,
    region_sf_original = ctx$ingestion$region_sf_original,
    metadata = metadata,
    verbose = FALSE
  )
  grid_sf <- prepared$grid_sf
  expect_s3_class(grid_sf, "sf")
  expect_true(all(c("wind_speed", "wind_dir", "timestamp") %in% names(grid_sf)))
  expect_gt(prepared$interpolated_count, 0)
})

test_that("write_geo_outputs persists GeoParquet and sidecar", {
  ctx <- build_export_context()
  metadata <- list(
    date_str = ctx$fixture$positional$date_str,
    hour_str = ctx$fixture$positional$hour_str,
    hour_val = ctx$fixture$hour,
    models = ctx$interp$models,
    input_count = nrow(ctx$ingestion$data),
    cv_metrics = ctx$interp$cv_metrics,
    test_metrics = ctx$interp$test_metrics
  )
  prepared <- modules_env$prepare_output_sf(
    grid_sp = ctx$interp$grid_coords,
    original_proj = ctx$ingestion$original_proj,
    region_sf_original = ctx$ingestion$region_sf_original,
    metadata = metadata,
    verbose = FALSE
  )
  export_meta <- list(
    nc_proj_string_value = ctx$ingestion$nc_proj_string_value,
    regions_meta = ctx$ingestion$regions_meta,
    test_points_sidecar = ctx$ingestion$test_points_sidecar,
    source_url_value = ctx$ingestion$source_url_value,
    region_name_used = ctx$ingestion$region_name_used
  )
  tmp_output <- withr::local_tempdir()
  aws_cfg <- list(has_aws_cli = FALSE, aws_region_args = character())
  pandas_stub <- list(DataFrame = function(df) df)
  pyarrow_stub <- list(Table = list(from_pandas = function(df, preserve_index = FALSE) list(df = df)))
  parquet_stub <- list(write_table = function(table, path, compression = "snappy") {
    saveRDS(table$df, path)
  })
  original_import <- modules_env$reticulate_import
  withr::defer(assign("reticulate_import", original_import, envir = modules_env), envir = parent.frame())
  assign("reticulate_import", function(module, convert = FALSE) {
    switch(module,
           "pandas" = pandas_stub,
           "pyarrow" = pyarrow_stub,
           "pyarrow.parquet" = parquet_stub,
           stop(sprintf("Unexpected module %s", module)))
  }, envir = modules_env)
  result <- modules_env$write_geo_outputs(
    grid_sf = prepared$grid_sf,
    output_base = file.path(tmp_output, "interp"),
    partition = list(year = ctx$fixture$year, month = ctx$fixture$month, day = ctx$fixture$day, hour = ctx$fixture$hour),
    ingest = ctx$ingestion,
    export_meta = export_meta,
    aws_cfg = aws_cfg,
    verbose = FALSE
  )
  expect_true(file.exists(result$geo_file_local))
  expect_true(file.exists(result$sidecar_local_path))
  expect_match(result$sidecar_remote_path, "metadata")
})

test_that("write_geo_outputs uses real reticulate modules when available", {
  ensure_python_backend()
  ctx <- build_export_context()
  metadata <- list(
    date_str = ctx$fixture$positional$date_str,
    hour_str = ctx$fixture$positional$hour_str,
    hour_val = ctx$fixture$hour,
    models = ctx$interp$models,
    input_count = nrow(ctx$ingestion$data),
    cv_metrics = ctx$interp$cv_metrics,
    test_metrics = ctx$interp$test_metrics
  )
  prepared <- modules_env$prepare_output_sf(
    grid_sp = ctx$interp$grid_coords,
    original_proj = ctx$ingestion$original_proj,
    region_sf_original = ctx$ingestion$region_sf_original,
    metadata = metadata,
    verbose = FALSE
  )
  export_meta <- list(
    nc_proj_string_value = ctx$ingestion$nc_proj_string_value,
    regions_meta = ctx$ingestion$regions_meta,
    test_points_sidecar = ctx$ingestion$test_points_sidecar,
    source_url_value = ctx$ingestion$source_url_value,
    region_name_used = ctx$ingestion$region_name_used
  )
  tmp_output <- withr::local_tempdir()
  aws_cfg <- list(has_aws_cli = FALSE, aws_region_args = character())
  result <- modules_env$write_geo_outputs(
    grid_sf = prepared$grid_sf,
    output_base = file.path(tmp_output, "interp_real"),
    partition = list(year = ctx$fixture$year, month = ctx$fixture$month, day = ctx$fixture$day, hour = ctx$fixture$hour),
    ingest = ctx$ingestion,
    export_meta = export_meta,
    aws_cfg = aws_cfg,
    verbose = FALSE
  )
  expect_true(file.exists(result$geo_file_local))
  expect_true(file.exists(result$sidecar_local_path))
})
