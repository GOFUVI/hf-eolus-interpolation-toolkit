#' Command-line interface parsing for the interpolation script.
#'
#' @param raw_args Character vector with trailing command line arguments.
#' @return Named list with parsed options and validated positional arguments.
#' @details The function mirrors the historical CLI accepted by
#'   `wind_interpolation.R`, preserving backwards compatibility for AWS Batch
#'   jobs and shell helpers.
parse_interpolation_cli <- function(raw_args) {
  verbose <- FALSE
  no_plots <- FALSE
  region_name_cli <- NULL
  aws_region_cli <- NULL
  swap_latlon <- FALSE
  plots_root_cli <- NULL

  args <- raw_args
  if ("--verbose" %in% args) {
    verbose <- TRUE
    args <- args[args != "--verbose"]
  }
  if ("--no-plots" %in% args) {
    no_plots <- TRUE
    args <- args[args != "--no-plots"]
  }
  if ("--region-name" %in% args) {
    idx <- which(args == "--region-name")
    if (idx == length(args)) stop("Missing value for --region-name")
    region_name_cli <- args[idx + 1]
    args <- args[-c(idx, idx + 1)]
  }
  if ("--aws-region" %in% args) {
    idx <- which(args == "--aws-region")
    if (idx == length(args)) stop("Missing value for --aws-region")
    aws_region_cli <- args[idx + 1]
    args <- args[-c(idx, idx + 1)]
  }
  if ("--swap-latlon" %in% args) {
    swap_latlon <- TRUE
    args <- args[args != "--swap-latlon"]
  }
  if ("--plots-root" %in% args) {
    idx <- which(args == "--plots-root")
    if (idx == length(args)) stop("Missing value for --plots-root")
    plots_root_cli <- args[idx + 1]
    args <- args[-c(idx, idx + 1)]
  }
  if (length(args) < 10) {
    stop(
      "Usage: Rscript wind_interpolation.R [--verbose] [--swap-latlon] ",
      "[--region-name <name>] [--aws-region <region>] [--plots-root <path>] ",
      "<date> <hour> <res_factor> <input_path> <output_path> ",
      "<cutoff_km> <width_km> <subsample_pct> <n_fold> <nmax_model>"
    )
  }

  positional <- list(
    date_str = args[1],
    hour_str = args[2],
    res_factor = as.numeric(args[3]),
    input_path = args[4],
    output_path = args[5],
    cutoff_km = as.numeric(args[6]),
    width_km = as.numeric(args[7]),
    subsample_pct = as.numeric(args[8]),
    nfold_cv = as.numeric(args[9]),
    nmax_model = as.integer(args[10])
  )

  if (is.na(positional$res_factor) || positional$res_factor <= 0) {
    stop("res_factor must be a positive number")
  }
  if (is.na(positional$nmax_model) || positional$nmax_model <= 0) {
    stop("nmax_model must be a positive integer")
  }
  if (is.na(positional$subsample_pct) || positional$subsample_pct <= 0 ||
      positional$subsample_pct > 100) {
    stop("subsample_pct must be a number between 0 and 100")
  }

  list(
    verbose = verbose,
    no_plots = no_plots,
    region_name = region_name_cli,
    aws_region = aws_region_cli,
    swap_latlon = swap_latlon,
    plots_root = plots_root_cli,
    positional = positional
  )
}

#' Load and prepare the GeoParquet partition for interpolation.
#'
#' @param positional List of positional arguments produced by
#'   `parse_interpolation_cli()`.
#' @param region_name_cli Optional region name filter.
#' @param swap_latlon Logical flag indicating whether to swap polygon
#'   coordinates.
#' @param aws_region_args Vector of AWS CLI arguments for the region.
#' @param has_aws_cli Logical flag indicating if AWS CLI is available.
#' @param verbose Logical flag enabling verbose logging.
#' @return List with ready-to-use data frames, metadata and spatial objects.
#' @details The function reads the GeoParquet partition, applies buffering
#'   filters based on the region polygon and computes local coordinate offsets.
load_partition_dataset <- function(positional,
                                   region_name_cli,
                                   swap_latlon,
                                   aws_region_args,
                                   has_aws_cli,
                                   verbose) {
  date_parts <- as.integer(strsplit(positional$date_str, "-")[[1]])
  year_val <- date_parts[1]
  month_val <- date_parts[2]
  day_val <- date_parts[3]
  hour_val <- as.integer(positional$hour_str)

  input_base <- sub("/$", "", positional$input_path)
  geoparquet_path <- sprintf(
    "%s/year=%04d/month=%02d/day=%02d/hour=%02d/data.parquet",
    input_base, year_val, month_val, day_val, hour_val
  )
  legacy_geoparquet_path <- sub("data.parquet$", "data.geoparquet", geoparquet_path)

  df_tbl <- tryCatch(
    arrow::read_parquet(geoparquet_path, as_data_frame = TRUE),
    error = function(e_primary) {
      log_verbose(verbose, sprintf("Primary Parquet not found (%s), attempting legacy geoparquet...", geoparquet_path))
      tryCatch(
        arrow::read_parquet(legacy_geoparquet_path, as_data_frame = TRUE),
        error = function(e_legacy) {
          stop(sprintf(
            "GeoParquet not found: %s (error: %s) and legacy path %s (error: %s)",
            geoparquet_path, e_primary$message, legacy_geoparquet_path, e_legacy$message
          ))
        }
      )
    }
  )

  ingest_sidecar <- read_metadata_sidecar(
    positional$input_path, year_val, month_val, day_val, hour_val,
    aws_region_args, has_aws_cli
  )

  proj_info <- resolve_projection_candidate(ingest_sidecar, df_tbl)
  nc_proj_string_value <- proj_info$text
  if (is.null(nc_proj_string_value) || !nzchar(nc_proj_string_value)) {
    stop("Projection metadata not found in sidecar or GeoParquet input.")
  }
  nc_proj_string_value <- normalize_proj4_string(nc_proj_string_value)

  original_proj <- proj_info$crs
  if (is.null(original_proj) || is.na(original_proj$input)) {
    original_proj <- tryCatch(sf::st_crs(nc_proj_string_value), warning = function(w) {
      log_verbose(verbose, paste("st_crs warning:", conditionMessage(w)))
      NULL
    }, error = function(e) {
      log_verbose(verbose, paste("st_crs error:", conditionMessage(e)))
      NULL
    })
  }
  if (is.null(original_proj) || is.na(original_proj$input)) {
    stop("Unable to construct CRS from projection metadata.")
  }
  if (is_nonempty_string(original_proj$proj4string)) {
    nc_proj_string_value <- normalize_proj4_string(original_proj$proj4string)
  }

  regions_meta_raw <- parse_metadata_list(ingest_sidecar$regions)
  if (is.null(regions_meta_raw)) {
    regions_meta_raw <- parse_metadata_list(extract_scalar_column(df_tbl, "regions"))
  }
  if (is.null(regions_meta_raw)) {
    stop("Region metadata missing from sidecar and GeoParquet input.")
  }
  if (is.data.frame(regions_meta_raw)) {
    regions_meta <- lapply(seq_len(nrow(regions_meta_raw)), function(i) as.list(regions_meta_raw[i, , drop = FALSE]))
  } else if (!is.null(regions_meta_raw$polygon)) {
    regions_meta <- list(regions_meta_raw)
  } else if (is.list(regions_meta_raw)) {
    regions_meta <- regions_meta_raw
  } else {
    stop("Unable to parse region metadata; expected list of region definitions.")
  }

  pick_region_name <- function(region) {
    if (!is.null(region$region_name)) return(region$region_name)
    if (!is.null(region$name)) return(region$name)
    NA_character_
  }

  if (!is.null(region_name_cli)) {
    sel <- vapply(regions_meta, function(r) identical(pick_region_name(r), region_name_cli), logical(1))
    if (!any(sel)) stop(sprintf("Region name '%s' not found in regions column", region_name_cli))
    region <- regions_meta[[which(sel)[1]]]
  } else {
    region <- regions_meta[[1]]
  }
  region_polygon <- region$polygon
  if (is.null(region_polygon)) {
    stop("Selected region does not contain a 'polygon' definition.")
  }
  if (swap_latlon) {
    log_verbose(verbose, "Swapping region polygon coordinates (lat/lon) as requested.")
    region_polygon <- lapply(region_polygon, function(pt) rev(as.numeric(pt)))
  } else {
    region_polygon <- lapply(region_polygon, function(pt) if (!is.numeric(pt)) as.numeric(pt) else pt)
  }
  region_name_used <- pick_region_name(region)

  test_points_meta <- parse_metadata_list(ingest_sidecar$test_points)
  if (is.null(test_points_meta)) {
    test_points_meta <- parse_metadata_list(extract_scalar_column(df_tbl, "test_points"))
  }
  test_points_df <- metadata_to_dataframe(test_points_meta)
  test_points_sidecar <- NULL
  if (!is.null(test_points_meta)) {
    if (is.data.frame(test_points_meta)) {
      test_points_sidecar <- lapply(seq_len(nrow(test_points_meta)), function(i) as.list(test_points_meta[i, , drop = FALSE]))
    } else {
      test_points_sidecar <- test_points_meta
    }
  }

  source_url_value <- NULL
  if (is_nonempty_string(ingest_sidecar$source_url)) {
    source_url_value <- as.character(ingest_sidecar$source_url)[1]
  } else {
    source_url_candidate <- extract_scalar_column(df_tbl, "source_url")
    if (is_nonempty_string(source_url_candidate)) {
      source_url_value <- as.character(source_url_candidate)[1]
    }
  }

  geom_sfc <- sf::st_as_sfc(df_tbl$geometry, EWKB = TRUE)
  df_tbl$geometry <- geom_sfc
  sf_data <- sf::st_sf(df_tbl, crs = "+proj=longlat +datum=WGS84 +no_defs")
  log_verbose(verbose, sprintf("Loaded %d GeoParquet records for %s %s",
                               nrow(sf_data), positional$date_str, positional$hour_str))

  proj_df <- sf::st_transform(sf_data, original_proj)
  coords <- extract_geometry(proj_df)
  x0_off <- coords$x[1]
  y0_off <- coords$y[1]
  sf_data$x <- sf_data$x + x0_off
  sf_data$y <- sf_data$y + y0_off

  region_sf_original <- sf::st_sf(
    geometry = sf::st_sfc(sf::st_polygon(list(do.call(rbind, region_polygon))), crs = 4326)
  )
  buffer_m <- positional$cutoff_km * 1000
  region_sf_buffered <- region_sf_original |>
    sf::st_transform(crs = 32629) |>
    sf::st_buffer(dist = buffer_m) |>
    sf::st_transform(crs = 4326)

  sel <- sf::st_intersects(sf_data, region_sf_buffered, sparse = FALSE)[, 1]
  sf_data <- sf_data[sel, ]
  data <- as.data.frame(sf_data)

  if (!all(c("u", "v") %in% names(data))) {
    if (all(c("wind_speed", "wind_dir") %in% names(data))) {
      deg2rad <- pi / 180
      data$u <- - data$wind_speed * sin(data$wind_dir * deg2rad)
      data$v <- - data$wind_speed * cos(data$wind_dir * deg2rad)
    } else {
      stop("Input data must contain u & v columns, or wind_speed & wind_dir to compute them.")
    }
  }

  unique_x <- sort(unique(sf_data$x))
  unique_y <- sort(unique(sf_data$y))
  min_orig_x <- min(unique_x)
  min_orig_y <- min(unique_y)
  unique_x_local <- unique_x - min_orig_x
  unique_y_local <- unique_y - min_orig_y

  data$x_local <- data$x - min_orig_x
  data$y_local <- data$y - min_orig_y

  list(
    data = data,
    sf_data = sf_data,
    proj_df = proj_df,
    original_proj = original_proj,
    ingest_sidecar = ingest_sidecar,
    nc_proj_string_value = nc_proj_string_value,
    regions_meta = regions_meta,
    region_polygon = region_polygon,
    region_sf_original = region_sf_original,
    region_sf_buffered = region_sf_buffered,
    region_name_used = region_name_used,
    test_points_df = test_points_df,
    test_points_sidecar = test_points_sidecar,
    source_url_value = source_url_value,
    year_val = year_val,
    month_val = month_val,
    day_val = day_val,
    hour_val = hour_val,
    x0_off = x0_off,
    y0_off = y0_off,
    min_orig_x = min_orig_x,
    min_orig_y = min_orig_y,
    unique_x_local = unique_x_local,
    unique_y_local = unique_y_local
  )
}
