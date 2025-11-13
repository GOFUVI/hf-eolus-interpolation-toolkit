# Helper utilities for module tests.
# These helpers are sourced automatically by testthat before any tests run.

project_root <- normalizePath(file.path(getwd(), ".."), mustWork = TRUE)
activate_script <- file.path(project_root, "renv", "activate.R")
if (file.exists(activate_script)) {
  source(activate_script)
  if ("renv" %in% loadedNamespaces()) {
    renv::activate(project = project_root)
  }
}

resolve_renv_lib <- function() {
  renv_root <- file.path("renv", "library")
  if (!dir.exists(renv_root)) {
    return(invisible(NULL))
  }
  platform_dirs <- list.dirs(renv_root, recursive = FALSE, full.names = TRUE)
  if (!length(platform_dirs)) {
    return(invisible(NULL))
  }
  version_dirs <- list.dirs(platform_dirs[[1]], recursive = FALSE, full.names = TRUE)
  if (!length(version_dirs)) {
    return(invisible(NULL))
  }
  renv_lib <- version_dirs[[1]]
  if (!renv_lib %in% .libPaths()) {
    .libPaths(c(renv_lib, .libPaths()))
  }
  invisible(renv_lib)
}

suppressPackageStartupMessages({
  resolve_renv_lib()
  if (!requireNamespace("testthat", quietly = TRUE)) {
    base_library <- R.home("library")
    .libPaths(unique(c(.libPaths(), base_library)))
  }
  library(testthat)
  library(withr)
  library(sf)
  library(arrow)
  library(jsonlite)
  library(sp)
  library(gstat)
  library(Metrics)
  if (requireNamespace("RANN", quietly = TRUE)) {
    library(RANN)
  }
  if (requireNamespace("FNN", quietly = TRUE)) {
    library(FNN)
  }
})

module_files <- list.files(file.path("scripts", "modules"), pattern = "_module\\.R$", full.names = TRUE)
module_order <- c(
  "utils_module.R",
  "ingestion_module.R",
  "grid_module.R",
  "interpolation_module.R",
  "export_module.R"
)
module_files <- module_files[order(match(basename(module_files), module_order))]
modules_env <- getOption("wind.modules_env")
if (is.null(modules_env)) {
  modules_env <- new.env(parent = .GlobalEnv)
  for (module_path in module_files) {
    sys.source(module_path, envir = modules_env, keep.source = TRUE)
  }
}
modules_env$`flush.console` <- get("flush.console", envir = asNamespace("base"))
if (requireNamespace("sp", quietly = TRUE)) {
  modules_env$`coordinates<-` <- get("coordinates<-", envir = asNamespace("sp"))
  modules_env$coordinates <- sp::coordinates
}
assign("modules_env", modules_env, envir = .GlobalEnv)
options(wind.modules_env = modules_env)

ensure_python_backend <- function() {
  if (!requireNamespace("reticulate", quietly = TRUE)) {
    testthat::skip("reticulate package is not installed")
  }
  if (!reticulate::py_available(initialize = TRUE)) {
    testthat::skip("No Python interpreter available for reticulate")
  }
  required <- c("pandas", "pyarrow", "pyarrow.parquet")
  missing <- required[!vapply(required, reticulate::py_module_available, logical(1))]
  if (length(missing)) {
    testthat::skip(sprintf("Python modules missing: %s", paste(missing, collapse = ", ")))
  }
}

create_partition_fixture <- function(base_dir = NULL,
                                     date = "2025-01-02",
                                     hour = "03",
                                     grid_size = c(2, 2),
                                     grid_spacing = 300) {
  caller_env <- parent.frame()
  if (is.null(base_dir)) {
    base_dir <- tempfile("fixture_", tmpdir = tempdir())
    withr::defer(unlink(base_dir, recursive = TRUE, force = TRUE), envir = caller_env)
  }
  dir.create(base_dir, recursive = TRUE, showWarnings = FALSE)
  year_val <- as.integer(substr(date, 1, 4))
  month_val <- as.integer(substr(date, 6, 7))
  day_val <- as.integer(substr(date, 9, 10))
  hour_val <- as.integer(hour)

  input_root <- file.path(base_dir, "input_dataset")
  data_path <- file.path(
    input_root,
    sprintf("year=%04d", year_val),
    sprintf("month=%02d", month_val),
    sprintf("day=%02d", day_val),
    sprintf("hour=%02d", hour_val)
  )
  dir.create(data_path, recursive = TRUE, showWarnings = FALSE)

  grid_size <- as.integer(grid_size)
  if (length(grid_size) != 2 || any(is.na(grid_size)) || any(grid_size <= 0)) {
    stop("grid_size must be a numeric vector of length 2 with positive values")
  }
  spacing <- as.numeric(grid_spacing[1])
  if (!is.finite(spacing) || spacing <= 0) {
    stop("grid_spacing must be a positive numeric value")
  }
  base_x <- 500000
  base_y <- 4800000
  offsets_x <- seq(0, by = spacing, length.out = grid_size[1])
  offsets_y <- seq(0, by = spacing, length.out = grid_size[2])
  coord_mesh <- expand.grid(offsets_x, offsets_y, KEEP.OUT.ATTRS = FALSE)
  coords_proj <- data.frame(
    x_proj = base_x + coord_mesh$Var1,
    y_proj = base_y + coord_mesh$Var2
  )
  x0 <- coords_proj$x_proj[1]
  y0 <- coords_proj$y_proj[1]
  x_scale <- (coords_proj$x_proj - x0) / max(1, (grid_size[1] - 1) * spacing)
  y_scale <- (coords_proj$y_proj - y0) / max(1, (grid_size[2] - 1) * spacing)

  sf_proj <- sf::st_as_sf(coords_proj, coords = c("x_proj", "y_proj"), crs = 32629)
  sf_wgs <- sf::st_transform(sf_proj, crs = 4326)
  geom_wkb <- sf::st_as_binary(sf::st_geometry(sf_wgs))

  data_tbl <- data.frame(
    node_id = as.character(seq_len(nrow(coords_proj))),
    x = coords_proj$x_proj - x0,
    y = coords_proj$y_proj - y0,
    u = 4.5 + 0.8 * sin(pi * x_scale) + 0.3 * cos(pi * y_scale),
    v = -1.2 + 0.5 * cos(pi * x_scale) + 0.4 * sin(pi * y_scale),
    topo = 8 + 4 * x_scale + 2 * y_scale,
    geometry = I(as.list(geom_wkb)),
    stringsAsFactors = FALSE
  )
  arrow::write_parquet(data_tbl, file.path(data_path, "data.parquet"))

  metadata_root <- file.path(base_dir, "metadata", "input_dataset",
                             sprintf("year=%04d", year_val),
                             sprintf("month=%02d", month_val),
                             sprintf("day=%02d", day_val),
                             sprintf("hour=%02d", hour_val))
  dir.create(metadata_root, recursive = TRUE, showWarnings = FALSE)

  metadata <- list(
    nc_proj_string = "+proj=utm +zone=29 +datum=WGS84 +units=m +no_defs",
    regions = list(list(
      region_name = "TEST-REGION",
      polygon = list(
        c(-8.6, 43.3),
        c(-8.4, 43.3),
        c(-8.4, 43.5),
        c(-8.6, 43.5),
        c(-8.6, 43.3)
      )
    )),
    test_points = list(list(
      name = "BOYA",
      lon = -8.5,
      lat = 43.4
    )),
    source_url = "s3://example/source.nc",
    netcdf_attributes = list(global = list(nc_proj_string = "+proj=utm +zone=29 +datum=WGS84 +units=m +no_defs"))
  )
  jsonlite::write_json(metadata, file.path(metadata_root, "metadata.json"), auto_unbox = TRUE, pretty = TRUE)

  list(
    positional = list(
      date_str = date,
      hour_str = sprintf("%02d", hour_val),
      res_factor = 2,
      input_path = input_root,
      output_path = file.path(base_dir, "output_dataset"),
      cutoff_km = 2000,
      width_km = 500,
      subsample_pct = 75,
      nfold_cv = 2,
      nmax_model = 5
    ),
    year = year_val,
    month = month_val,
    day = day_val,
    hour = hour_val,
    metadata_root = metadata_root,
    input_root = input_root,
    base_dir = base_dir
  )
}
