test_that("parse_interpolation_cli preserves switches", {
  args <- c(
    "--verbose",
    "--region-name", "TEST-REGION",
    "--aws-region", "eu-west-3",
    "--no-plots",
    "--swap-latlon",
    "--plots-root", "s3://bucket/plots",
    "2025-01-02", "03", "2",
    "input", "output", "20", "10", "80", "5", "12"
  )
  cli <- modules_env$parse_interpolation_cli(args)
  expect_true(cli$verbose)
  expect_true(cli$no_plots)
  expect_true(cli$swap_latlon)
  expect_equal(cli$region_name, "TEST-REGION")
  expect_equal(cli$aws_region, "eu-west-3")
  expect_equal(cli$plots_root, "s3://bucket/plots")
  expect_equal(cli$positional$nmax_model, 12L)
})

test_that("parse_interpolation_cli validates arguments", {
  expect_error(modules_env$parse_interpolation_cli(c("--region-name", "X")), "Usage")
  expect_error(modules_env$parse_interpolation_cli(c("2025-01-01", "00", "-1", "a", "b", "1", "1", "1", "1", "1")), "positive")
})

test_that("load_partition_dataset returns spatial artefacts", {
  fixture <- create_partition_fixture()
  positional <- fixture$positional
  aws_args <- character()
  ingestion <- modules_env$load_partition_dataset(
    positional = positional,
    region_name_cli = "TEST-REGION",
    swap_latlon = FALSE,
    aws_region_args = aws_args,
    has_aws_cli = FALSE,
    verbose = FALSE
  )
  expect_true(inherits(ingestion$data, "data.frame"))
  expect_true(all(c("x_local", "y_local", "u", "v") %in% names(ingestion$data)))
  expect_true(inherits(ingestion$proj_df, "sf"))
  expect_equal(ingestion$region_name_used, "TEST-REGION")
  expect_s3_class(ingestion$region_sf_original, "sf")
  expect_equal(nrow(ingestion$test_points_df), 1)
  expect_type(ingestion$nc_proj_string_value, "character")
})

test_that("load_partition_dataset detects missing projection metadata", {
  fixture <- create_partition_fixture()
  meta_path <- file.path(fixture$metadata_root, "metadata.json")
  meta <- jsonlite::read_json(meta_path, simplifyVector = FALSE)
  meta$nc_proj_string <- NULL
  meta$netcdf_attributes <- NULL
  jsonlite::write_json(meta, meta_path, auto_unbox = TRUE, pretty = TRUE)
  expect_error(
    modules_env$load_partition_dataset(
      positional = fixture$positional,
      region_name_cli = NULL,
      swap_latlon = FALSE,
      aws_region_args = character(),
      has_aws_cli = FALSE,
      verbose = FALSE
    ),
    "Projection metadata"
  )
})
