test_that("log_verbose honours verbosity flag", {
  expect_message(modules_env$log_verbose(TRUE, "hello"), "hello")
  expect_silent(modules_env$log_verbose(FALSE, "hidden"))
})

test_that("extract_geometry handles sf and data.frame inputs", {
  pts <- sf::st_as_sf(data.frame(x = 1:2, y = 3:4), coords = c("x", "y"), crs = 4326)
  geom_df <- modules_env$extract_geometry(pts)
  expect_equal(colnames(geom_df), c("x", "y"))
  expect_equal(nrow(geom_df), 2)

  pts_df <- data.frame(lat = c(43, 44), lon = c(-8, -7))
  sf_df <- sf::st_as_sf(pts_df, coords = c("lon", "lat"), crs = 4326)
  geom_coords <- modules_env$extract_geometry(sf_df)
  expect_equal(unname(as.matrix(geom_coords)), unname(sf::st_coordinates(sf_df)))
})

test_that("join_paths preserves s3 prefixes and local paths", {
  expect_equal(modules_env$join_paths("/tmp/base", "child", "file.txt"), file.path("/tmp/base", "child", "file.txt"))
  expect_equal(modules_env$join_paths("s3://bucket/data"), "s3://bucket/data")
  expect_equal(
    modules_env$join_paths("s3://bucket/data/", "nested", "leaf"),
    "s3://bucket/data/nested/leaf"
  )
})

test_that("sidecar_prefix_for builds correct prefixes", {
  local <- modules_env$sidecar_prefix_for("/tmp/output/interpolation")
  expect_match(local, "metadata/interpolation$")
  remote <- modules_env$sidecar_prefix_for("s3://bucket/path/data")
  expect_equal(remote, "s3://bucket/path/metadata/data")
})

test_that("extract_scalar_column recovers first non-missing", {
  df <- data.frame(a = I(list(c("one", "one"), NA)), b = c(NA, "ok"))
  expect_equal(modules_env$extract_scalar_column(df, "b"), "ok")
  expect_warning(modules_env$extract_scalar_column(data.frame(b = c("x", "y")), "b"))
})

test_that("string helpers normalise inputs", {
  expect_true(modules_env$is_nonempty_string("value"))
  expect_false(modules_env$is_nonempty_string(NA_character_))
  json_payload <- jsonlite::toJSON(list(alpha = 1), auto_unbox = TRUE)
  parsed <- modules_env$parse_metadata_list(json_payload)
  expect_equal(parsed$alpha, 1)
  expect_null(modules_env$parse_metadata_list(""))
})

test_that("metadata coercion utilities flatten data", {
  nested <- list(level = list(name = "region", value = "R"))
  df <- modules_env$metadata_to_dataframe(nested)
  expect_true(is.list(df))
  scalars <- modules_env$collect_scalar_strings(list("a", NULL, list("b", 3)))
  expect_equal(sort(scalars), c("3", "a", "b"))
})

test_that("projection helpers adjust proj4 strings", {
  raw <- "+proj=lambert_conformal_conic +lon_0=190"
  norm <- modules_env$normalize_proj4_string(raw)
  expect_match(norm, "\\+no_defs")
  expect_match(norm, "\\+ellps=WGS84")
  expect_match(norm, "\\+lon_0=-170")
})

test_that("resolve_projection_candidate prefers sidecar data", {
  sidecar <- list(
    nc_proj_string = "+proj=utm +zone=29 +datum=WGS84",
    netcdf_attributes = list(global = list(crs = "EPSG:32629"))
  )
  df_tbl <- data.frame(nc_proj_string = "+proj=longlat +datum=WGS84")
  res <- modules_env$resolve_projection_candidate(sidecar, df_tbl)
  expect_s3_class(res$crs, "crs")
  expect_true(grepl("+proj=", res$text, fixed = TRUE))
})

test_that("read_metadata_sidecar loads local files and handles missing", {
  fixture <- create_partition_fixture()
  positional <- fixture$positional
  ingest <- modules_env$read_metadata_sidecar(
    positional$input_path,
    fixture$year,
    fixture$month,
    fixture$day,
    fixture$hour,
    aws_region_args = character(),
    has_aws_cli = FALSE
  )
  expect_equal(ingest$regions[[1]]$region_name, "TEST-REGION")

  expect_warning(
    modules_env$read_metadata_sidecar(
      "s3://bucket/data",
      2025, 1, 1, 0,
      aws_region_args = character(),
      has_aws_cli = FALSE
    )
  )
})

test_that("compute_knn falls back to FNN backend", {
  train <- matrix(c(0, 0, 1, 1, 2, 2, 3, 3), ncol = 2, byrow = TRUE)
  query <- matrix(c(0.5, 0.5, 2.5, 2.5), ncol = 2, byrow = TRUE)
  res <- modules_env$compute_knn(train, query, k = 2)
  expect_equal(res$backend, if (requireNamespace("RANN", quietly = TRUE)) "RANN" else "FNN")
  expect_equal(dim(res$nn.idx), c(nrow(query), 2))
  expect_equal(dim(res$nn.dists), c(nrow(query), 2))
  expect_true(all(res$nn.dists >= 0))
})
