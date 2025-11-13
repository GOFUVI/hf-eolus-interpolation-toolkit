build_interpolation_context <- function(...) {
  fixture <- create_partition_fixture(...)
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
  list(fixture = fixture, ingestion = ingestion, grid = grid)
}

test_that("select_variogram_model fits a model when variance exists", {
  ctx <- build_interpolation_context()
  data_sp <- ctx$grid$data_sp
  vgm_emp <- gstat::variogram(u ~ x_local + y_local, data_sp, cutoff = 3000, width = 500)
  set.seed(42)
  res <- modules_env$select_variogram_model(
    cv_data = data_sp,
    vgm_emp = vgm_emp,
    var_name = "u",
    nfold_cv = 2,
    nmax_model = 4,
    verbose = FALSE
  )
  expect_true(is.list(res))
  expect_true(res$metrics$selected %in% c("Exp", "Gau", "Sph", "IDW"))
})

test_that("select_variogram_model handles larger synthetic datasets without warnings", {
  ctx <- build_interpolation_context(grid_size = c(8, 8), grid_spacing = 150)
  data_sp <- ctx$grid$data_sp
  expect_warning(
    vgm_emp <- gstat::variogram(u ~ x_local + y_local, data_sp, cutoff = 6000, width = 400),
    regexp = NA
  )
  set.seed(123)
  res <- modules_env$select_variogram_model(
    cv_data = data_sp,
    vgm_emp = vgm_emp,
    var_name = "u",
    nfold_cv = 3,
    nmax_model = 8,
    verbose = FALSE
  )
  expect_true(is.list(res))
  expect_true(res$metrics$selected %in% c("Exp", "Gau", "Sph", "IDW"))
})

test_that("select_variogram_model falls back to IDW when fits fail", {
  ctx <- build_interpolation_context()
  data_sp <- ctx$grid$data_sp
  data_sp$u <- 0
  vgm_emp <- gstat::variogram(u ~ x_local + y_local, data_sp, cutoff = 1000, width = 200)
  res <- modules_env$select_variogram_model(
    cv_data = data_sp,
    vgm_emp = vgm_emp,
    var_name = "u",
    nfold_cv = 2,
    nmax_model = 3,
    verbose = FALSE
  )
  expect_identical(res$best, "IDW")
})

test_that("predict_component supports IDW and kriging", {
  ctx <- build_interpolation_context()
  data_sp <- ctx$grid$data_sp
  new_points <- ctx$grid$new_grid_sp
  idw_res <- modules_env$predict_component(
    var_name = "u",
    model = "IDW",
    data = data_sp,
    new_grid_coords = new_points,
    cutoff_km = 5000,
    nmax_model = 4,
    verbose = FALSE
  )
  expect_equal(length(idw_res$pred), nrow(new_points))
  expect_true(any(is.finite(idw_res$pred)))

  vgm_emp <- gstat::variogram(u ~ x_local + y_local, data_sp, cutoff = 3000, width = 500)
  model_fit <- suppressWarnings(try(gstat::fit.variogram(vgm_emp, gstat::vgm(1, "Exp", 1000)), silent = TRUE))
  if (!inherits(model_fit, "try-error") && !is.null(model_fit)) {
    krige_res <- modules_env$predict_component(
      var_name = "u",
      model = model_fit,
      data = data_sp,
      new_grid_coords = new_points,
      cutoff_km = 5000,
      nmax_model = 4,
      verbose = FALSE
    )
    expect_equal(length(krige_res$pred), nrow(new_points))
    expect_true(any(is.finite(krige_res$pred)))
  }
})

test_that("perform_interpolation_suite enriches the grid", {
  ctx <- build_interpolation_context()
  params <- list(
    subsample_pct = 75,
    cutoff_km = 5000,
    width_km = 500,
    nfold_cv = 2,
    nmax_model = 4,
    no_plots = TRUE,
    verbose = FALSE,
    date_str = ctx$fixture$positional$date_str,
    hour_str = ctx$fixture$positional$hour_str,
    hour_val = ctx$fixture$hour,
    plot_cfg = list(upload = FALSE),
    test_point_names = ctx$grid$test_point_names,
    orig_points_df = ctx$grid$orig_points_df,
    test_pct = 25,
    data_count = nrow(ctx$ingestion$data)
  )
  interp <- modules_env$perform_interpolation_suite(
    data = ctx$grid$data_sp,
    grid_coords = ctx$grid$grid_sp,
    new_grid_coords = ctx$grid$new_grid_sp,
    params = params
  )
  expect_true(all(c("grid_coords", "new_grid_coords", "models") %in% names(interp)))
  expect_true(all(c("u", "v") %in% names(interp$grid_coords@data)))
  expect_true(any(!is.na(interp$new_grid_coords$u_rkt)))
  expect_true(is.list(interp$test_metrics))
})

test_that("save_variogram_plot creates artefacts", {
  ctx <- build_interpolation_context()
  data_sp <- ctx$grid$data_sp
  vgm_emp <- gstat::variogram(u ~ x_local + y_local, data_sp, cutoff = 3000, width = 500)
  tmp <- withr::local_tempdir()
  plot_cfg <- list(
    local_part = tmp,
    remote_part = tmp,
    remote_base = tmp,
    has_aws_cli = FALSE,
    aws_region_args = character(),
    upload = FALSE
  )
  model_fit <- suppressWarnings(try(gstat::fit.variogram(vgm_emp, gstat::vgm(1, "Exp", 1000)), silent = TRUE))
  if (!inherits(model_fit, "try-error") && !is.null(model_fit)) {
    expect_silent(modules_env$save_variogram_plot(
      vgm_emp = vgm_emp,
      vgm_model = model_fit,
      var_name = "u",
      date_str = "2025-01-02",
      hour_str = "03",
      plot_cfg = plot_cfg,
      verbose = FALSE
    ))
    files <- list.files(tmp, pattern = "variogram_u", full.names = TRUE)
    expect_true(length(files) >= 1)
  }
  expect_null(modules_env$save_variogram_plot(NULL, "IDW", "u", "2025-01-02", "03", plot_cfg))
})

test_that("extract_variogram_params summarises models", {
  expect_equal(modules_env$extract_variogram_params("IDW")$model, "IDW")
  fake <- gstat::vgm(psill = 1, model = "Exp", range = 10, nugget = 0.1)
  params <- modules_env$extract_variogram_params(fake)
  expect_equal(params$model, "Exp")
  expect_gt(params$sill, 0)
})
