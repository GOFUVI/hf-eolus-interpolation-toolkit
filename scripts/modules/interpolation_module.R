#' Helper functions for interpolation (IDW, kriging and regression kriging).
#'
#' @details This module encapsulates the logic that prepares variograms,
#'   performs the different interpolation strategies and evaluates the results
#'   on hold-out data. It returns enriched spatial objects that downstream
#'   steps can export to GeoParquet.
NULL

.print_verbose <- function(verbose, x) {
  if (isTRUE(verbose)) {
    print(x)
    flush.console()
  }
}

#' Fit variogram candidates with cross-validation fallbacks.
#'
#' @param cv_data Subsampled spatial dataset used for cross-validation.
#' @param vgm_emp Empirical variogram object.
#' @param var_name Variable name (`"u"` or `"v"`).
#' @param nfold_cv Number of folds for cross-validation.
#' @param nmax_model Maximum neighbour count used during CV.
#' @param verbose Logical enabling verbose logging.
#' @return List with best model (string `"IDW"`, `NULL` or `gstatModel`), the
#'   candidate models evaluated and cross-validation metrics.
select_variogram_model <- function(cv_data,
                                   vgm_emp,
                                   var_name,
                                   nfold_cv,
                                   nmax_model,
                                   verbose) {
  fits <- list()
  # Try exponential, Gaussian and spherical models
  for (model_name in c("Exp", "Gau", "Sph")) {
    init_model <- gstat::vgm(psill = stats::var(cv_data[[var_name]], na.rm = TRUE),
                             model = model_name,
                             nugget = 0)
    fit <- tryCatch({
      withCallingHandlers(
        gstat::fit.variogram(vgm_emp, init_model),
        warning = function(w) {
          if (grepl("singular|converge", w$message, ignore.case = TRUE)) {
            invokeRestart("muffleWarning")
          }
        }
      )
    }, error = function(e) {
      log_verbose(verbose, sprintf("%s variogram fit failed for %s: %s", model_name, var_name, e$message))
      NULL
    })
    if (!is.null(fit) && !any(is.na(unlist(fit)))) {
      fits[[model_name]] <- fit
    }
  }
  if (!length(fits)) {
    log_verbose(verbose, sprintf("Falling back to IDW for %s (no variogram fits)", var_name))
    return(list(
      best = "IDW",
      candidates = list("IDW"),
      metrics = list(
        models = "IDW",
        rsr = c(IDW = NA_real_),
        bias = c(IDW = NA_real_),
        selected = "IDW"
      )
    ))
  }
  rsr_vals <- bias_vals <- numeric()
  for (m in names(fits)) {
    start_time <- Sys.time()
    set.seed(123)
    cv_res <- tryCatch({
      gstat::krige.cv(
        stats::as.formula(paste(var_name, "~ x_local + y_local")),
        cv_data,
        model = fits[[m]],
        nfold = nfold_cv,
        nmax = nmax_model
      )
    }, error = function(e) {
      log_verbose(verbose, sprintf("CV failed for %s (%s): %s", var_name, m, e$message))
      NULL
    })
    if (is.null(cv_res)) {
      fits[[m]] <- NULL
      next
    }
    elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
    obs <- cv_res@data$observed
    pred <- cv_res@data$var1.pred
    rsr_val <- Metrics::rmse(obs, pred) / stats::sd(obs)
    bias_val <- Metrics::bias(obs, pred)
    log_verbose(verbose, sprintf("CV RSR for %s (%s): %.3f (%.2f s)", var_name, m, rsr_val, elapsed))
    log_verbose(verbose, sprintf("CV bias for %s (%s): %.3f", var_name, m, bias_val))
    rsr_vals[m] <- rsr_val
    bias_vals[m] <- bias_val
  }
  valid_models <- names(fits)[!vapply(fits, is.null, logical(1))]
  if (!length(valid_models)) {
    log_verbose(verbose, sprintf("Falling back to IDW for %s (no valid CV results)", var_name))
    return(list(
      best = "IDW",
      candidates = list("IDW"),
      metrics = list(
        models = "IDW",
        rsr = c(IDW = NA_real_),
        bias = c(IDW = NA_real_),
        selected = "IDW"
      )
    ))
  }
  fits <- fits[valid_models]
  rsr_vals <- stats::setNames(rsr_vals[valid_models], valid_models)
  bias_vals <- stats::setNames(bias_vals[valid_models], valid_models)
  metric_ok <- is.finite(rsr_vals)
  if (!any(metric_ok)) {
    log_verbose(verbose, sprintf("Falling back to IDW for %s (non-finite CV metrics)", var_name))
    return(list(
      best = "IDW",
      candidates = list("IDW"),
      metrics = list(
        models = "IDW",
        rsr = c(IDW = NA_real_),
        bias = c(IDW = NA_real_),
        selected = "IDW"
      )
    ))
  }
  rsr_vals <- rsr_vals[metric_ok]
  bias_vals <- bias_vals[names(rsr_vals)]
  best_name <- names(which.min(rsr_vals))
  list(
    best = fits[[best_name]],
    candidates = fits,
    metrics = list(
      models = names(fits),
      rsr = rsr_vals,
      bias = bias_vals,
      selected = best_name
    )
  )
}

#' Save empirical variogram plots when requested.
#'
#' @param vgm_emp Empirical variogram object.
#' @param vgm_model Variogram model (can be `"IDW"` or `NULL`).
#' @param var_name Variable name.
#' @param date_str Date string used in filenames.
#' @param hour_str Hour string used in filenames.
#' @param plot_cfg List with keys `local_part`, `remote_part`, `remote_base`,
#'   `has_aws_cli`, `aws_region_args`.
#' @param suffix Optional suffix for regression kriging plots.
#' @param verbose Logical enabling verbose logging.
save_variogram_plot <- function(vgm_emp,
                                vgm_model,
                                var_name,
                                date_str,
                                hour_str,
                                plot_cfg,
                                suffix = NULL,
                                verbose = FALSE) {
  if (identical(vgm_model, "IDW") || is.null(vgm_emp)) {
    return(invisible(NULL))
  }
  plot_local_part <- plot_cfg$local_part
  plot_remote_part <- plot_cfg$remote_part
  has_aws_cli <- isTRUE(plot_cfg$has_aws_cli)
  aws_region_args <- plot_cfg$aws_region_args %||% character()
  file_base <- sprintf("variogram_%s%s_empirical_%s_%s.png",
                       var_name,
                       if (!is.null(suffix)) paste0("_", suffix) else "",
                       date_str,
                       hour_str)
  png_path <- file.path(plot_local_part, file_base)
  log_verbose(verbose, sprintf("Saving variogram plot %s", png_path))
  grDevices::png(png_path, width = 800, height = 600, type = "cairo")
  tryCatch(
    print(plot(vgm_emp, vgm_model,
               main = sprintf("Empirical variogram for %s%s - %s %s",
                              var_name,
                              if (!is.null(suffix)) paste0(" (", suffix, ")") else "",
                              date_str,
                              hour_str))),
    error = function(e) log_verbose(verbose, sprintf("Error plotting variogram: %s", e$message))
  )
  grDevices::dev.off()
  if (!file.exists(png_path)) {
    stop(sprintf("Variogram plot not created: %s", png_path))
  }
  if (isTRUE(plot_cfg$upload) && grepl("^s3://", plot_cfg$remote_base) && has_aws_cli) {
    remote_path <- file.path(plot_remote_part, basename(png_path))
    log_verbose(verbose, sprintf("Uploading variogram plot to %s", remote_path))
    rc <- system2("aws", c("s3", "cp", png_path, remote_path, aws_region_args))
    if (rc != 0) {
      stop(sprintf("Failed to upload %s to %s (exit %d)", png_path, remote_path, rc))
    }
  }
  invisible(png_path)
}

#' Perform IDW or kriging predictions for a variable.
#'
#' @param var_name Variable name (`"u"` or `"v"`).
#' @param model Selected variogram model (list, `"IDW"` or `NULL`).
#' @param data Training spatial object.
#' @param new_grid_coords Target spatial grid.
#' @param cutoff_km Maximum distance for neighbours.
#' @param nmax_model Maximum neighbour count.
#' @param verbose Logical enabling verbose logging.
#' @return List with predictions and variance estimates.
predict_component <- function(var_name,
                              model,
                              data,
                              new_grid_coords,
                              cutoff_km,
                              nmax_model,
                              verbose) {
  log_verbose(verbose, sprintf("Interpolating %s component", var_name))
  if (identical(model, "IDW")) {
    grid_sp <- sp::SpatialPoints(new_grid_coords[, c("x", "y")], proj4string = sp::CRS(sp::proj4string(data)))
    idw_spdf <- gstat::idw(stats::as.formula(paste(var_name, "~ 1")), data, newdata = grid_sp, maxdist = cutoff_km)
    return(list(pred = idw_spdf@data$var1.pred,
                variance = rep(NA_real_, length(idw_spdf@data$var1.pred))))
  }
  formula <- stats::as.formula(paste(var_name, "~ x_local + y_local"))
  if (is.null(model)) {
    krig_spdf <- gstat::krige(formula, data, newdata = new_grid_coords, maxdist = cutoff_km)
  } else {
    krig_spdf <- gstat::krige(formula, data, newdata = new_grid_coords,
                              model = model, maxdist = cutoff_km, nmax = nmax_model)
  }
  list(pred = krig_spdf@data$var1.pred,
       variance = krig_spdf@data$var1.var)
}

#' Extract variogram parameters for metadata reporting.
#'
#' @param model Variogram model or string `"IDW"`.
#' @return List with `model`, `nugget`, `sill`, `range`.
extract_variogram_params <- function(model) {
  if (identical(model, "IDW")) {
    return(list(model = "IDW", nugget = NA_real_, sill = NA_real_, range = NA_real_))
  }
  if (is.null(model)) {
    return(list(model = "Universal", nugget = NA_real_, sill = NA_real_, range = NA_real_))
  }
  df <- as.data.frame(model)
  if (!"psill" %in% names(df)) {
    return(list(model = NA_character_, nugget = NA_real_, sill = NA_real_, range = NA_real_))
  }
  nugget <- sum(df$psill[df$model == "Nug"], na.rm = TRUE)
  structural <- df[df$model != "Nug", , drop = FALSE]
  if (nrow(structural) == 0) {
    structural <- df[1, , drop = FALSE]
  }
  sill <- nugget + sum(structural$psill, na.rm = TRUE)
  range_val <- structural$range[1]
  model_name <- as.character(structural$model[1])
  list(model = model_name, nugget = nugget, sill = sill, range = range_val)
}

#' Run the full interpolation workflow (IDW/Kriging/RKT).
#'
#' @param data SpatialPointsDataFrame with training samples.
#' @param grid_coords SpatialPointsDataFrame containing the full grid.
#' @param new_grid_coords SpatialPointsDataFrame with the positions that need interpolation.
#' @param params Named list of configuration values (see Details).
#' @return List with enriched spatial objects, fitted models, CV metrics and test metrics.
#'
#' @details Expected elements inside `params`:
#'   - `subsample_pct`, `cutoff_km`, `width_km`, `nfold_cv`, `nmax_model`
#'   - `no_plots`, `verbose`, `date_str`, `hour_str`
#'   - `plot_cfg` (list produced in the main script)
#'   - `test_point_names`, `orig_points_df`
#'   - `test_pct` (defaults to 10)
#'   - `data_count` original data count before sampling
perform_interpolation_suite <- function(data,
                                        grid_coords,
                                        new_grid_coords,
                                        params) {
  verbose <- isTRUE(params$verbose)
  no_plots <- isTRUE(params$no_plots)
  cutoff_km <- params$cutoff_km
  width_km <- params$width_km
  subsample_pct <- params$subsample_pct
  nfold_cv <- params$nfold_cv
  nmax_model <- params$nmax_model
  date_str <- params$date_str
  hour_str <- params$hour_str
  plot_cfg <- params$plot_cfg
  test_point_names <- params$test_point_names %||% character()
  orig_points_df <- params$orig_points_df
  data_count <- params$data_count

  set.seed(123)
  test_pct <- params$test_pct %||% 10
  test_n <- ceiling(nrow(data) * test_pct / 100)
  test_idx <- sample(seq_len(nrow(data)), test_n)
  test_data <- data[test_idx, ]
  train_data <- data[-test_idx, ]
  log_verbose(verbose, sprintf("Split data into %d training / %d test samples", nrow(train_data), nrow(test_data)))

  subsample_n <- ceiling(nrow(train_data) * subsample_pct / 100)
  if (nrow(train_data) > subsample_n) {
    data_sub <- train_data[sample(seq_len(nrow(train_data)), subsample_n), ]
  } else {
    data_sub <- train_data
  }
  log_verbose(verbose, sprintf("Using %d samples for variogram CV", nrow(data_sub)))

  vgm_u <- gstat::variogram(u ~ x_local + y_local, data_sub,
                            cutoff = cutoff_km,
                            width = width_km)
  sel_u <- select_variogram_model(data_sub, vgm_u, "u", nfold_cv, nmax_model, verbose)
  model_u <- sel_u$best
  if (!no_plots) {
    save_variogram_plot(vgm_u, sel_u$best, "u", date_str, hour_str, plot_cfg, verbose = verbose)
  }

  vgm_v <- gstat::variogram(v ~ x_local + y_local, data_sub,
                            cutoff = cutoff_km,
                            width = width_km)
  sel_v <- select_variogram_model(data_sub, vgm_v, "v", nfold_cv, nmax_model, verbose)
  model_v <- sel_v$best
  if (!no_plots) {
    save_variogram_plot(vgm_v, sel_v$best, "v", date_str, hour_str, plot_cfg, verbose = verbose)
  }

  models <- list(u = model_u, v = model_v)
  pred_list <- parallel::mclapply(names(models), function(var_name) {
    predict_component(var_name, models[[var_name]], data, new_grid_coords, cutoff_km, nmax_model, verbose)
  }, mc.cores = min(2, parallel::detectCores()))
  names(pred_list) <- names(models)

  new_grid_coords$u <- pred_list[["u"]]$pred
  new_grid_coords$kriging_var_u <- pred_list[["u"]]$variance
  new_grid_coords$v <- pred_list[["v"]]$pred
  new_grid_coords$kriging_var_v <- pred_list[["v"]]$variance

  new_grid_coords <- regression_kriging(new_grid_coords,
                                        data,
                                        data_sub,
                                        cutoff_km,
                                        width_km,
                                        nmax_model,
                                        date_str,
                                        hour_str,
                                        plot_cfg,
                                        no_plots,
                                        verbose)

  grid_coords <- propagate_predictions(grid_coords,
                                       new_grid_coords,
                                       data,
                                       test_point_names,
                                       cutoff_km,
                                       nmax_model,
                                       verbose)

  test_metrics <- evaluate_holdout(train_data,
                                   test_data,
                                   test_idx,
                                   models,
                                   nmax_model,
                                   cutoff_km,
                                   verbose)

  list(
    grid_coords = grid_coords,
    new_grid_coords = new_grid_coords,
    data = data,
    train_data = train_data,
    test_data = test_data,
    test_idx = test_idx,
    models = models,
    cv_metrics = list(u = sel_u$metrics, v = sel_v$metrics),
    test_metrics = test_metrics,
    data_sub = data_sub,
    data_count = data_count
  )
}

# Helper to provide default when NULL
`%||%` <- function(a, b) if (!is.null(a)) a else b

# Additional internal helpers -------------------------------------------------

regression_kriging <- function(new_grid_coords,
                               data,
                               data_sub,
                               cutoff_km,
                               width_km,
                               nmax_model,
                               date_str,
                               hour_str,
                               plot_cfg,
                               no_plots,
                               verbose) {
  topo_idw <- gstat::idw(stats::as.formula("topo ~ 1"), data, newdata = new_grid_coords, nmax = nmax_model)
  new_grid_coords$topo <- topo_idw@data$var1.pred
  for (var_name in c("u", "v")) {
    reg_formula <- stats::as.formula(paste(var_name, "~ topo"))
    lm_mod <- stats::lm(reg_formula, data = data)
    log_verbose(verbose, sprintf("Regression summary for %s ~ topo:", var_name))
    .print_verbose(verbose, summary(lm_mod))
    vgm_emp <- gstat::variogram(reg_formula, data_sub,
                                cutoff = cutoff_km,
                                width = width_km)
    candidate_models <- c("Exp", "Gau", "Sph")
    vgm_fit <- NULL
    for (m in candidate_models) {
      null_vgm <- gstat::vgm(stats::var(data_sub[[var_name]]), m, nugget = 0)
      fit_try <- tryCatch({
        withCallingHandlers(
          gstat::fit.variogram(vgm_emp, model = null_vgm),
          warning = function(w) {
            if (grepl("singular|converge", w$message, ignore.case = TRUE)) {
              invokeRestart("muffleWarning")
            }
          }
        )
      }, error = function(e) {
        log_verbose(verbose, sprintf("%s variogram fit failed for %s (RKT): %s", m, var_name, e$message))
        NULL
      })
      if (!is.null(fit_try) && !any(is.na(unlist(fit_try)))) {
        vgm_fit <- fit_try
        log_verbose(verbose, sprintf("Selected %s model for %s regression kriging variogram", m, var_name))
        break
      }
    }
    if (is.null(vgm_fit)) {
      stop(sprintf("All variogram model fits failed for %s regression kriging", var_name))
    }
    if (!no_plots) {
      save_variogram_plot(vgm_emp, vgm_fit, var_name, date_str, hour_str,
                          plot_cfg, suffix = "rkt", verbose = verbose)
    }
    rk_reg <- gstat::krige(reg_formula, locations = data, newdata = new_grid_coords)
    rk_res <- gstat::krige(stats::residuals(lm_mod) ~ 1,
                           locations = data,
                           newdata = new_grid_coords,
                           model = vgm_fit,
                           maxdist = cutoff_km,
                           nmax = nmax_model)
    new_grid_coords[[paste0(var_name, "_rkt")]] <- rk_reg@data$var1.pred + rk_res@data$var1.pred
  }
  new_grid_coords
}

propagate_predictions <- function(grid_coords,
                                  new_grid_coords,
                                  data,
                                  test_point_names,
                                  cutoff_km,
                                  nmax_model,
                                  verbose) {
  grid_coords$u[!grid_coords$is_orig] <- new_grid_coords$u
  grid_coords$v[!grid_coords$is_orig] <- new_grid_coords$v
  grid_coords$kriging_var_u <- NA_real_
  grid_coords$kriging_var_v <- NA_real_
  grid_coords$kriging_var_u[!grid_coords$is_orig] <- new_grid_coords$kriging_var_u
  grid_coords$kriging_var_v[!grid_coords$is_orig] <- new_grid_coords$kriging_var_v

  orig_match <- which(grid_coords@data$node_id %in% data$node_id)
  if (length(orig_match) > 0) {
    data_idx <- match(grid_coords@data$node_id[orig_match], data$node_id)
    coords_mat <- sp::coordinates(grid_coords)
    coords_mat[orig_match, ] <- sp::coordinates(data)[data_idx, ]
    grid_coords@coords <- coords_mat
    grid_coords@data$u[orig_match] <- data$u[data_idx]
    grid_coords@data$v[orig_match] <- data$v[data_idx]
  }
  grid_coords$u_rkt <- NA_real_
  grid_coords$v_rkt <- NA_real_
  grid_coords$u_rkt[!grid_coords$is_orig] <- new_grid_coords$u_rkt
  grid_coords$v_rkt[!grid_coords$is_orig] <- new_grid_coords$v_rkt
  if (length(orig_match) > 0) {
    data_idx <- match(grid_coords@data$node_id[orig_match], data$node_id)
    grid_coords@data$u_rkt[orig_match] <- data$u[data_idx]
    grid_coords@data$v_rkt[orig_match] <- data$v[data_idx]
  }

  train_coord_matrix <- sp::coordinates(data)
  grid_coord_matrix <- sp::coordinates(grid_coords)
  k_val <- min(nmax_model, nrow(train_coord_matrix))
  nn_lookup <- compute_knn(train_coord_matrix, grid_coord_matrix, k = k_val)
  nearest_distance <- nn_lookup$nn.dists[, 1]
  if (!is.na(cutoff_km) && is.finite(cutoff_km) && cutoff_km > 0) {
    neighbor_count <- rowSums(nn_lookup$nn.dists <= cutoff_km)
  } else {
    neighbor_count <- rowSums(is.finite(nn_lookup$nn.dists))
  }
  grid_coords$nearest_distance_km <- as.numeric(nearest_distance)
  grid_coords$neighbors_used <- as.integer(neighbor_count)

  interpolation_source <- rep("interpolated", nrow(grid_coords))
  interpolation_source[grid_coords$is_orig] <- "original"
  if (length(test_point_names)) {
    tp_idx <- which(grid_coords@data$node_id %in% test_point_names)
    interpolation_source[tp_idx] <- "test_point"
  }
  grid_coords$interpolation_source <- interpolation_source
  grid_coords
}

evaluate_holdout <- function(train_data,
                             test_data,
                             test_idx,
                             models,
                             nmax_model,
                             cutoff_km,
                             verbose) {
  log_verbose(verbose, sprintf("Interpolating test set (%d samples) for evaluation", nrow(test_data)))
  preds <- lapply(names(models), function(var_name) {
    model <- models[[var_name]]
    if (identical(model, "IDW")) {
      gstat::idw(stats::as.formula(paste(var_name, "~ 1")), train_data, newdata = test_data, nmax = nmax_model)@data$var1.pred
    } else if (is.null(model)) {
      gstat::krige(stats::as.formula(paste(var_name, "~ x_local + y_local")),
                   train_data,
                   newdata = test_data,
                   nmax = nmax_model)@data$var1.pred
    } else {
      gstat::krige(stats::as.formula(paste(var_name, "~ x_local + y_local")),
                   train_data,
                   newdata = test_data,
                   model = model,
                   nmax = nmax_model)@data$var1.pred
    }
  })
  names(preds) <- names(models)
  rsr_u <- Metrics::rmse(test_data$u, preds[["u"]]) / stats::sd(test_data$u)
  bias_u <- Metrics::bias(test_data$u, preds[["u"]])
  rsr_v <- Metrics::rmse(test_data$v, preds[["v"]]) / stats::sd(test_data$v)
  bias_v <- Metrics::bias(test_data$v, preds[["v"]])
  message(sprintf("Test RSR U: %.3f, bias U: %.3f", rsr_u, bias_u))
  message(sprintf("Test RSR V: %.3f, bias V: %.3f", rsr_v, bias_v))
  list(
    rsr_u = rsr_u,
    bias_u = bias_u,
    rsr_v = rsr_v,
    bias_v = bias_v
  )
}
