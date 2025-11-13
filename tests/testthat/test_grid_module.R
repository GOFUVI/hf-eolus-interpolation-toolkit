setup_fixture <- function() {
  fixture <- create_partition_fixture()
  ingestion <- modules_env$load_partition_dataset(
    positional = fixture$positional,
    region_name_cli = NULL,
    swap_latlon = FALSE,
    aws_region_args = character(),
    has_aws_cli = FALSE,
    verbose = FALSE
  )
  list(fixture = fixture, ingestion = ingestion)
}

test_that("build_interpolation_grid expands original mesh", {
  ctx <- setup_fixture()
  ing <- ctx$ingestion
  grid <- modules_env$build_interpolation_grid(
    data = ing$data,
    unique_x_local = ing$unique_x_local,
    unique_y_local = ing$unique_y_local,
    min_orig_x = ing$min_orig_x,
    min_orig_y = ing$min_orig_y,
    res_factor = ctx$fixture$positional$res_factor,
    original_proj = ing$original_proj,
    test_points_df = ing$test_points_df,
    verbose = FALSE
  )
  expect_s4_class(grid$data_sp, "SpatialPointsDataFrame")
  expect_s4_class(grid$grid_sp, "SpatialPointsDataFrame")
  expect_true(grid$grid_spacing > 0)
  expect_true(all(c("x_local", "y_local", "is_orig") %in% names(grid$grid_df)))
  expect_equal(length(grid$test_point_names), 1)
  expect_gt(nrow(grid$new_grid_sp), nrow(grid$orig_points_df))
})

test_that("build_interpolation_grid honours absence of test points", {
  ctx <- setup_fixture()
  ing <- ctx$ingestion
  ing$test_points_df <- NULL
  grid <- modules_env$build_interpolation_grid(
    data = ing$data,
    unique_x_local = ing$unique_x_local,
    unique_y_local = ing$unique_y_local,
    min_orig_x = ing$min_orig_x,
    min_orig_y = ing$min_orig_y,
    res_factor = ctx$fixture$positional$res_factor,
    original_proj = ing$original_proj,
    test_points_df = NULL,
    verbose = FALSE
  )
  expect_length(grid$test_point_names, 0)
  expect_null(grid$test_points_sp)
})
