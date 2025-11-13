#' Utility helpers shared across the wind interpolation workflow.
#'
#' @details This module provides small helper functions used across ingestion,
#'   grid generation, interpolation and export steps. The functions are
#'   intentionally side-effect free so that they can be reused from other
#'   orchestration scripts if needed.
NULL

#' Emit a message only when verbose mode is enabled.
#'
#' @param verbose Logical flag controlling verbosity.
#' @param ... Arguments passed to `base::message()`.
#' @details This helper mirrors the historical `vmessage` usage while keeping
#'   the implementation local to the caller.
log_verbose <- function(verbose, ...) {
  if (isTRUE(verbose)) {
    message(...)
    flush.console()
  }
}

#' Extract x and y coordinates from a geometry column.
#'
#' @param df A `data.frame` or `sf` object with a geometry column.
#' @return A `data.frame` with columns `x` and `y`.
#' @details When `df` is not an `sf` object, it is coerced using
#'   `sf::st_as_sf()` before the coordinates are extracted.
extract_geometry <- function(df) {
  if (!inherits(df, "sf")) {
    df <- sf::st_as_sf(df)
  }
  geom <- sf::st_geometry(df)
  geom_matrix <- do.call(rbind, lapply(geom, unclass))
  colnames(geom_matrix) <- c("x", "y")
  as.data.frame(geom_matrix)
}

#' Compose a path that preserves S3 prefixes when present.
#'
#' @param base Base path or prefix.
#' @param ... Additional path segments.
#' @return Character scalar with the joined path.
#' @details When the base path starts with `s3://` the segments are joined using
#'   forward slashes without collapsing double slashes.
join_paths <- function(base, ...) {
  segments <- c(...)
  if (length(segments) == 0) {
    return(base)
  }
  segments <- as.character(segments)
  if (grepl("^s3://", base)) {
    base_clean <- sub("/+$", "", base)
    extras <- sub("^/+", "", segments)
    extras <- extras[nzchar(extras)]
    if (length(extras) == 0) {
      return(base_clean)
    }
    return(paste(c(base_clean, extras), collapse = "/"))
  }
  do.call(file.path, c(list(base), segments))
}

#' Derive the metadata sidecar prefix located outside the data tree.
#'
#' @param base_path Base dataset path.
#' @return Character scalar pointing to the metadata prefix.
#' @details When working with S3 paths the prefix mirrors the data layout under
#'   a sibling `metadata/` key.
sidecar_prefix_for <- function(base_path) {
  clean_path <- sub("/+$", "", base_path)
  if (grepl("^s3://", clean_path)) {
    without_scheme <- sub("^s3://", "", clean_path)
    components <- strsplit(without_scheme, "/", fixed = TRUE)[[1]]
    bucket <- components[1]
    key_parts <- components[-1]
    dataset <- if (length(key_parts)) tail(key_parts, 1) else ""
    parent_parts <- if (length(key_parts) > 1) key_parts[-length(key_parts)] else character()
    new_parts <- c(parent_parts, "metadata", dataset)
    new_key <- paste(new_parts[new_parts != ""], collapse = "/")
    if (nzchar(new_key)) {
      return(paste0("s3://", bucket, "/", new_key))
    }
    return(paste0("s3://", bucket, "/metadata"))
  }
  parent_dir <- dirname(clean_path)
  dataset <- basename(clean_path)
  file.path(parent_dir, "metadata", dataset)
}

#' Extract the first non-missing scalar value from a column.
#'
#' @param df Data frame or tibble.
#' @param col Column name to inspect.
#' @return Scalar value or `NULL` when none can be extracted.
extract_scalar_column <- function(df, col) {
  if (!col %in% names(df)) {
    return(NULL)
  }
  values <- df[[col]]
  if (is.list(values)) {
    values <- unlist(values, recursive = TRUE, use.names = FALSE)
  }
  if (length(values) == 0) {
    return(NULL)
  }
  if (is.atomic(values)) {
    values <- values[!is.na(values)]
    if (!length(values)) {
      return(NULL)
    }
    unique_vals <- unique(values)
    if (length(unique_vals) > 1) {
      warning(sprintf("Column '%s' has multiple distinct values; using first", col))
    }
    return(unique_vals[[1]])
  }
  NULL
}

#' Determine whether an input is a non-empty character scalar.
#'
#' @param value Value to inspect.
#' @return Logical scalar indicating if the value is a non-empty string.
is_nonempty_string <- function(value) {
  if (!is.character(value) || length(value) < 1) {
    return(FALSE)
  }
  first_val <- value[[1]]
  if (is.na(first_val)) {
    return(FALSE)
  }
  nzchar(first_val)
}

#' Parse metadata that can be JSON text or structured lists.
#'
#' @param value Candidate metadata payload.
#' @return Parsed list or `NULL` when the input is empty.
#' @details The function accepts already-parsed lists, JSON strings or scalar
#'   character values and normalises them into a list structure.
parse_metadata_list <- function(value) {
  if (is.null(value)) {
    return(NULL)
  }
  if (inherits(value, "AsIs")) {
    value <- as.character(value)
  }
  if (is.character(value)) {
    if (!is_nonempty_string(value)) return(NULL)
    return(jsonlite::fromJSON(value[[1]], simplifyVector = FALSE))
  }
  value
}

#' Convert metadata to a data frame when possible.
#'
#' @param value Metadata payload to coerce.
#' @return `data.frame` or `NULL` if coercion is not possible.
metadata_to_dataframe <- function(value) {
  if (is.null(value)) {
    return(NULL)
  }
  if (is.data.frame(value)) {
    return(value)
  }
  if (is.list(value) && length(value) > 0) {
    json_rep <- tryCatch(jsonlite::toJSON(value, auto_unbox = TRUE), error = function(...) NULL)
    if (!is.null(json_rep)) {
      return(tryCatch(jsonlite::fromJSON(json_rep, simplifyVector = TRUE), error = function(...) NULL))
    }
  }
  NULL
}

#' Recursively collect scalar strings from nested metadata structures.
#'
#' @param value Arbitrary list or vector.
#' @return Character vector with flattened scalar values.
collect_scalar_strings <- function(value) {
  if (is.null(value)) {
    return(character())
  }
  if (is.character(value)) {
    return(value[nzchar(value)])
  }
  if (is.numeric(value) && length(value) == 1 && !is.na(value)) {
    return(as.character(value))
  }
  if (is.list(value)) {
    return(unlist(lapply(value, collect_scalar_strings), use.names = FALSE))
  }
  character()
}

#' Compute k-nearest neighbours using available backends.
#'
#' @param train_matrix Numeric matrix with reference coordinates.
#' @param query_matrix Numeric matrix with query coordinates.
#' @param k Integer number of neighbours to retrieve.
#' @return List with numeric matrices `nn.idx`, `nn.dists` and a character
#'   scalar `backend` indicating the implementation used.
#' @details The function prefers `RANN::nn2()` when the RANN package is
#'   installed. When it is not available the logic falls back to
#'   `FNN::get.knnx()`. Both code paths return matrices shaped as
#'   `nrow(query_matrix) x k`.
compute_knn <- function(train_matrix, query_matrix, k) {
  if (!is.matrix(train_matrix)) {
    train_matrix <- as.matrix(train_matrix)
  }
  if (!is.matrix(query_matrix)) {
    query_matrix <- as.matrix(query_matrix)
  }
  if (!is.numeric(k) || length(k) != 1 || is.na(k) || k < 1) {
    stop("k must be a positive integer", call. = FALSE)
  }
  k <- as.integer(k)
  if (requireNamespace("RANN", quietly = TRUE)) {
    res <- RANN::nn2(train_matrix, query_matrix, k = k)
    if (!is.matrix(res$nn.idx)) {
      res$nn.idx <- as.matrix(res$nn.idx)
    }
    if (!is.matrix(res$nn.dists)) {
      res$nn.dists <- as.matrix(res$nn.dists)
    }
    res$backend <- "RANN"
    return(res)
  }
  if (requireNamespace("FNN", quietly = TRUE)) {
    knn <- FNN::get.knnx(train_matrix, query_matrix, k = k)
    return(list(
      nn.idx = as.matrix(knn$nn.index),
      nn.dists = as.matrix(knn$nn.dist),
      backend = "FNN"
    ))
  }
  stop("Neither RANN nor FNN packages are available for nearest neighbour search", call. = FALSE)
}

#' Import a Python module via reticulate, ensuring availability.
#'
#' @param module Python module name passed to `reticulate::import()`.
#' @param convert Logical flag propagated to `reticulate::import()`.
#' @return The imported Python module.
#' @details Centralises the dependency on `reticulate` so that callers can
#'   override it during tests and the function fails fast when the package is
#'   missing.
reticulate_import <- function(module, convert = FALSE) {
  if (!requireNamespace("reticulate", quietly = TRUE)) {
    stop("reticulate package is required but not installed", call. = FALSE)
  }
  reticulate::import(module, convert = convert)
}

#' Normalise PROJ4 strings applying common fixes for legacy definitions.
#'
#' @param proj_str Input projection string.
#' @return Normalised projection string.
normalize_proj4_string <- function(proj_str) {
  if (!is_nonempty_string(proj_str)) {
    return(proj_str)
  }
  normalized <- proj_str
  if (grepl("lambert_conformal_conic", normalized, fixed = TRUE)) {
    normalized <- sub("lambert_conformal_conic", "lcc", normalized, fixed = TRUE)
  }
  if (!grepl("\\+no_defs", normalized, fixed = TRUE)) {
    normalized <- paste(normalized, "+no_defs")
  }
  if (!grepl("\\+ellps=", normalized, fixed = TRUE)) {
    normalized <- paste(normalized, "+ellps=WGS84")
  }
  if (!grepl("\\+datum=", normalized, fixed = TRUE)) {
    normalized <- paste(normalized, "+datum=WGS84")
  }
  if (grepl("\\+lon_0=", normalized)) {
    lon0 <- as.numeric(sub(".*\\+lon_0=([^ ]+).*", "\\1", normalized))
    if (!is.na(lon0)) {
      lon_wrapped <- ((lon0 + 180) %% 360) - 180
      normalized <- sub("\\+lon_0=[^ ]+", sprintf("+lon_0=%s", lon_wrapped), normalized)
    }
  }
  normalized
}

#' Resolve CRS information from sidecar metadata and dataset columns.
#'
#' @param sidecar Parsed metadata sidecar list.
#' @param df_tbl Data frame containing promoted metadata columns.
#' @return List with elements `crs` (an `sf::crs` object or `NULL`) and
#'   `text` (raw CRS string).
resolve_projection_candidate <- function(sidecar, df_tbl) {
  candidates <- character()
  append_candidate <- function(val) {
    candidates <<- c(candidates, collect_scalar_strings(val))
  }
  append_candidate(sidecar$nc_proj_string)
  append_candidate(sidecar$crs)
  append_candidate(sidecar$projection)
  append_candidate(sidecar$proj4)
  append_candidate(sidecar$proj4text)
  append_candidate(sidecar$proj4_string)
  if (!is.null(sidecar$netcdf_attributes)) {
    global <- sidecar$netcdf_attributes$global
    if (!is.null(global)) {
      append_candidate(global$nc_proj_string)
      append_candidate(global$crs)
      append_candidate(global$projection)
      append_candidate(global$proj4)
      append_candidate(global$proj4text)
      append_candidate(global$proj4_string)
    }
  }
  column_candidate <- extract_scalar_column(df_tbl, "nc_proj_string")
  append_candidate(column_candidate)
  if (!length(candidates) && length(sidecar)) {
    all_strings <- unique(collect_scalar_strings(sidecar))
    candidates <- all_strings[grepl("\\+proj=|^EPSG:|PROJCS\\[|GEOGCS\\[|ProjectedCRS|GeographicCRS", all_strings, ignore.case = TRUE)]
  }
  candidates <- unique(candidates[nzchar(candidates)])
  if (!length(candidates)) {
    return(list(crs = NULL, text = NULL))
  }
  first_text <- candidates[[1]]
  for (cand in candidates) {
    normalized <- cand
    if (grepl("^epsg:", cand, ignore.case = TRUE)) {
      normalized <- sub("^epsg:", "", cand, ignore.case = TRUE)
    }
    crs_obj <- tryCatch(sf::st_crs(normalized), error = function(...) NULL)
    if (!is.null(crs_obj) && !is.na(crs_obj$input)) {
      crs_string <- crs_obj$proj4string
      if (!is_nonempty_string(crs_string)) {
        crs_string <- crs_obj$wkt
      }
      if (!is_nonempty_string(crs_string)) {
        crs_string <- normalized
      }
      return(list(crs = crs_obj, text = crs_string))
    }
    if (is_nonempty_string(cand) && is.null(first_text)) {
      first_text <- cand
    }
  }
  list(crs = NULL, text = first_text)
}

#' Read metadata sidecar JSON if present locally or on S3.
#'
#' @param base_path Dataset base path.
#' @param year_val Numeric year.
#' @param month_val Numeric month.
#' @param day_val Numeric day.
#' @param hour_val Numeric hour.
#' @param aws_region_args Vector of AWS CLI region arguments.
#' @param has_aws_cli Logical indicating if AWS CLI is available.
#' @return Parsed metadata list or empty list when missing.
read_metadata_sidecar <- function(base_path, year_val, month_val, day_val, hour_val,
                                  aws_region_args, has_aws_cli) {
  prefix <- sidecar_prefix_for(base_path)
  sidecar_full <- join_paths(
    prefix,
    sprintf("year=%04d", year_val),
    sprintf("month=%02d", month_val),
    sprintf("day=%02d", day_val),
    sprintf("hour=%02d", hour_val),
    "metadata.json"
  )
  local_path <- sidecar_full
  if (grepl("^s3://", sidecar_full)) {
    if (!has_aws_cli) {
      warning(sprintf("AWS CLI not available; skipping download of %s", sidecar_full))
      return(list())
    }
    local_path <- file.path(tempdir(), sprintf("metadata_%04d%02d%02d_%02d.json", year_val, month_val, day_val, hour_val))
    rc <- system2("aws", c("s3", "cp", sidecar_full, local_path, aws_region_args))
    if (rc != 0) {
      warning(sprintf("Failed to download metadata sidecar %s (exit %s)", sidecar_full, rc))
      return(list())
    }
  }
  if (!file.exists(local_path)) {
    warning(sprintf("Metadata sidecar not found: %s", local_path))
    return(list())
  }
  jsonlite::fromJSON(local_path, simplifyVector = FALSE)
}
