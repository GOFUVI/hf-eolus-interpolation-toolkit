#' Build interpolation grids from ingested data.
#'
#' @param data Data frame produced by `load_partition_dataset()`.
#' @param unique_x_local Numeric vector with unique local X coordinates.
#' @param unique_y_local Numeric vector with unique local Y coordinates.
#' @param min_orig_x Minimum original X coordinate.
#' @param min_orig_y Minimum original Y coordinate.
#' @param res_factor Resolution multiplier applied to the native grid.
#' @param original_proj CRS of the input data (as returned by `sf::st_crs()`).
#' @param test_points_df Optional data frame with test points metadata.
#' @param verbose Logical flag enabling verbose logging.
#' @return List containing spatial objects and helper data required for
#'   downstream interpolation steps.
build_interpolation_grid <- function(data,
                                     unique_x_local,
                                     unique_y_local,
                                     min_orig_x,
                                     min_orig_y,
                                     res_factor,
                                     original_proj,
                                     test_points_df,
                                     verbose) {
  base_res <- min(min(diff(unique_x_local)), min(diff(unique_y_local)))
  min_x_local <- 0
  max_x_local <- max(unique_x_local)
  min_y_local <- 0
  max_y_local <- max(unique_y_local)
  grid_spacing <- base_res / res_factor
  grid_x_local <- seq(min_x_local, max_x_local, by = grid_spacing)
  grid_y_local <- seq(min_y_local, max_y_local, by = grid_spacing)
  grid_x_local_is_orig <- c(rep(c(TRUE, rep(FALSE, res_factor - 1)),
                                length(unique_x_local) - 1), TRUE)
  grid_y_local_is_orig <- c(rep(c(TRUE, rep(FALSE, res_factor - 1)),
                                length(unique_y_local) - 1), TRUE)
  grid_x_local_orig <- grid_x_local[grid_x_local_is_orig]
  grid_y_local_orig <- grid_y_local[grid_y_local_is_orig]

  grid_coords <- expand.grid(x_local = grid_x_local, y_local = grid_y_local)
  grid_coords$is_orig <- grid_coords$x_local %in% grid_x_local_orig &
    grid_coords$y_local %in% grid_y_local_orig
  grid_coords$x <- grid_coords$x_local + min_orig_x
  grid_coords$y <- grid_coords$y_local + min_orig_y
  grid_coords$node_id <- as.character(seq_len(nrow(grid_coords)))
  grid_coords$u <- NA_real_
  grid_coords$v <- NA_real_
  log_verbose(verbose, sprintf(
    "Output grid generated: %d points (factor = %s, spacing = %.6f)",
    nrow(grid_coords), res_factor, grid_spacing
  ))

  data <- dplyr::left_join(
    data,
    grid_coords[, c("x_local", "y_local", "node_id")],
    by = c("x_local", "y_local")
  )
  orig_points_df <- data
  if ("node_id" %in% names(orig_points_df)) {
    orig_points_df$node_id <- as.character(orig_points_df$node_id)
  }

  new_grid_coords <- grid_coords[!grid_coords$is_orig, , drop = FALSE]
  log_verbose(verbose, sprintf(
    "New mesh points to interpolate: %d of %d",
    nrow(new_grid_coords), nrow(grid_coords)
  ))

  tp_df <- NULL
  test_point_names <- character()
  if (!is.null(test_points_df) && is.data.frame(test_points_df) && nrow(test_points_df) > 0) {
    log_verbose(verbose, "Adding test_points as interpolation locations")
    tp_sf <- sf::st_as_sf(test_points_df, coords = c("lon", "lat"), crs = 4326)
    tp_proj <- sf::st_transform(tp_sf, crs = original_proj)
    coords_tp <- sf::st_coordinates(tp_proj)
    tp_names <- if ("name" %in% names(test_points_df)) {
      test_points_df$name
    } else {
      paste0("test_point_", seq_len(nrow(test_points_df)))
    }
    test_point_names <- tp_names
    tp_df <- data.frame(
      x = coords_tp[, 1],
      y = coords_tp[, 2],
      x_local = coords_tp[, 1] - min_orig_x,
      y_local = coords_tp[, 2] - min_orig_y,
      node_id = tp_names,
      u = NA_real_,
      v = NA_real_,
      is_orig = FALSE,
      stringsAsFactors = FALSE
    )
    log_verbose(verbose, sprintf("Added %d test_points to interpolation locations", nrow(tp_df)))
    new_grid_coords <- rbind(new_grid_coords, tp_df)
  } else if (!is.null(test_points_df) && nrow(test_points_df) == 0) {
    log_verbose(verbose, "test_points metadata present but empty; skipping additional interpolation points")
  }

  coordinates(data) <- ~ x_local + y_local
  coordinates(grid_coords) <- ~ x_local + y_local
  if (!is.null(tp_df)) {
    coordinates(tp_df) <- ~ x_local + y_local
    grid_coords <- rbind(grid_coords, tp_df)
  }
  coordinates(new_grid_coords) <- ~ x_local + y_local

  list(
    data_sp = data,
    grid_sp = grid_coords,
    new_grid_sp = new_grid_coords,
    grid_df = as.data.frame(grid_coords),
    orig_points_df = orig_points_df,
    test_point_names = test_point_names,
    test_points_sp = tp_df,
    grid_spacing = grid_spacing
  )
}
