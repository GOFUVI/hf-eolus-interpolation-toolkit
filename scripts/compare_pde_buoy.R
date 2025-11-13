#!/usr/bin/env Rscript
# compare_pde_buoy.R
# Automate comparisons between interpolated predictions and buoy observations
# described through STAC catalogs and GeoParquet exports.

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(jsonlite)
  library(lubridate)
})

deg_to_rad <- function(deg) {
  deg * pi / 180
}

rad_to_deg <- function(rad) {
  rad * 180 / pi
}

usage <- '
Usage:
  Rscript scripts/compare_pde_buoy.R \\
    --prediction-path=<path_to_predictions_root_or_file> \\
    --output-dir=<reports_root_or_single_dir> \\
    [--buoy-catalog=<path_to_catalog.json>] \\
    [--buoy-item=Vilano] \\
    [--buoy-config=<path_to_json_plan>] \\
    [--prediction-node-id=Vilano_buoy] \\
    [--report-name=vilano_report.md] \\
    [--obs-start=YYYY-MM-DDTHH:MM:SSZ] \\
    [--obs-end=YYYY-MM-DDTHH:MM:SSZ] \\
    [--pred-start=YYYY-MM-DDTHH:MM:SSZ] \\
    [--pred-end=YYYY-MM-DDTHH:MM:SSZ]

Provide `--buoy-catalog`/`--buoy-item`/`--prediction-node-id` for single-buoy
comparisons. To run multiple comparisons in one call, point `--buoy-config` to a
JSON file describing each buoy entry:

[
  {
    "id": "vilano",
    "catalog": "case_study/catalogs/pde_vilano_buoy/collection.json",
    "item_id": "Vilano",
    "node_id": "Vilano_buoy",
    "output_subdir": "Vilano"
  }
]

When `--buoy-config` is provided, `--output-dir` is treated as the reports root
and the script creates `output-dir/<buoy-id>/` folders automatically.
'

slugify_id <- function(value, fallback) {
  if (is.null(value) || !nzchar(value)) {
    value <- fallback
  }
  slug <- gsub("[^A-Za-z0-9]+", "_", value)
  slug <- gsub("^_+|_+$", "", slug)
  if (!nzchar(slug)) {
    slug <- fallback
  }
  tolower(slug)
}

normalize_prediction_path <- function(path) {
  if (is.null(path) || !nzchar(path)) {
    return(NULL)
  }
  if (!file.exists(path) && !dir.exists(path)) {
    stop("Prediction path does not exist: ", path)
  }
  normalizePath(path, mustWork = TRUE)
}

format_relative_path <- function(path) {
  cwd <- getwd()
  prefix <- paste0(cwd, "/")
  if (startsWith(path, prefix)) {
    return(sub(prefix, "", path, fixed = TRUE))
  }
  path
}

resolve_path_from <- function(base_dir, path, must_exist = TRUE) {
  if (is.null(path) || !nzchar(path)) {
    return(NULL)
  }
  expanded <- path.expand(path)
  if (!startsWith(expanded, "/") && !grepl("^[A-Za-z]:", expanded)) {
    candidates <- c(
      file.path(base_dir, path),
      file.path(getwd(), path)
    )
    for (candidate in candidates) {
      if (file.exists(candidate) || dir.exists(candidate)) {
        return(normalizePath(candidate, mustWork = TRUE))
      }
    }
    expanded <- candidates[[1]]
  }
  normalizePath(expanded, mustWork = must_exist)
}

parse_args <- function(args) {
  if (!length(args)) {
    cat(usage, "\n")
    quit(status = 1)
  }
  parsed <- list()
  for (arg in args) {
    if (arg %in% c("-h", "--help")) {
      cat(usage, "\n")
      quit(status = 0)
    }
    if (!startsWith(arg, "--") || !grepl("=", arg, fixed = TRUE)) {
      stop("Arguments must follow the --key=value format. Offending token: ", arg)
    }
    key_value <- strsplit(sub("^--", "", arg), "=", fixed = TRUE)[[1]]
    key <- key_value[1]
    value <- key_value[2]
    if (key == "" || value == "") {
      stop("Invalid argument: ", arg)
    }
    parsed[[key]] <- value
  }
  parsed
}

resolve_datetime <- function(value) {
  if (is.null(value)) {
    return(NULL)
  }
  parsed <- suppressWarnings(lubridate::ymd_hms(value, tz = "UTC"))
  if (is.na(parsed)) {
    parsed <- suppressWarnings(lubridate::ymd(value, tz = "UTC"))
  }
  if (is.na(parsed)) {
    stop("Unable to parse datetime value: ", value)
  }
  parsed
}

resolve_href <- function(base_dir, href) {
  if (grepl("^(https?|s3)://", href)) {
    stop("Remote STAC assets are not supported in this offline workflow: ", href)
  }
  if (startsWith(href, "/")) {
    return(normalizePath(href, mustWork = TRUE))
  }
  normalizePath(file.path(base_dir, href), mustWork = TRUE)
}

load_buoy_plan <- function(config_path, global_cfg) {
  config_abs <- normalizePath(config_path, mustWork = TRUE)
  config_dir <- dirname(config_abs)
  raw <- jsonlite::fromJSON(config_abs, simplifyVector = FALSE)
  entries <- if (is.list(raw) && !is.null(raw$buoys)) {
    raw$buoys
  } else if (is.list(raw) && !is.null(raw[[1]]) && is.list(raw[[1]])) {
    raw
  } else if (is.list(raw)) {
    list(raw)
  } else {
    stop("Buoy config must be a JSON object or array of objects.")
  }
  if (!length(entries)) {
    stop("Buoy config does not contain any entries.")
  }
  lapply(seq_along(entries), function(idx) {
    entry <- entries[[idx]]
    if (!is.list(entry)) {
      stop("Each buoy entry must be a JSON object.")
    }
    label <- entry$label %||% entry$name %||% entry$id %||% sprintf("buoy_%02d", idx)
    slug_source <- entry$output_subdir %||% entry$output_dir_name %||% entry$id %||% label
    slug <- slugify_id(slug_source, sprintf("buoy_%02d", idx))

    output_dir <- entry$output_dir
    if (is.null(output_dir) || !nzchar(output_dir)) {
      root <- global_cfg$output_dir
      if (is.null(root) || !nzchar(root)) {
        stop("Multi-buoy mode requires --output-dir to indicate the reports root directory.")
      }
      output_dir <- file.path(root, slug)
    } else {
      expanded <- path.expand(output_dir)
      if (!startsWith(expanded, "/") && !grepl("^[A-Za-z]:", expanded)) {
        root <- global_cfg$output_dir
        if (!is.null(root) && nzchar(root)) {
          output_dir <- file.path(root, output_dir)
        }
      } else {
        output_dir <- expanded
      }
    }

    catalog_path <- entry$catalog %||% entry$buoy_catalog
    if (is.null(catalog_path) || !nzchar(catalog_path)) {
      stop(sprintf("Entry #%d ('%s') is missing the 'catalog' attribute.", idx, label))
    }
    catalog_abs <- resolve_path_from(config_dir, catalog_path, must_exist = TRUE)

    prediction_source <- entry$prediction_path
    prediction_abs <- NULL
    if (!is.null(prediction_source) && nzchar(prediction_source)) {
      expanded <- path.expand(prediction_source)
      if (!startsWith(expanded, "/") && !grepl("^[A-Za-z]:", expanded)) {
        candidate <- file.path(config_dir, prediction_source)
        if (file.exists(candidate) || dir.exists(candidate)) {
          prediction_abs <- normalizePath(candidate, mustWork = TRUE)
        }
      }
      if (is.null(prediction_abs)) {
        prediction_abs <- normalize_prediction_path(prediction_source)
      }
    } else if (!is.null(global_cfg$prediction_path)) {
      prediction_abs <- global_cfg$prediction_path
    }
    if (is.null(prediction_abs)) {
      stop(sprintf(
        "Entry #%d ('%s') is missing 'prediction_path' and no global --prediction-path was supplied.",
        idx,
        label
      ))
    }

    node_id <- entry$node_id %||% entry$prediction_node_id %||% slug
    report_name <- entry$report_name %||% sprintf("%s_report.md", slug)
    list(
      id = slug,
      label = label,
      buoy_catalog = catalog_abs,
      buoy_item = entry$item_id %||% entry$item %||% entry$buoy_item %||% label,
      prediction_path = prediction_abs,
      node_id = node_id,
      output_dir = output_dir,
      report_name = report_name,
      obs_start = resolve_datetime(entry$obs_start %||% entry$observation_start),
      obs_end = resolve_datetime(entry$obs_end %||% entry$observation_end),
      pred_start = resolve_datetime(entry$pred_start %||% entry$prediction_start),
      pred_end = resolve_datetime(entry$pred_end %||% entry$prediction_end)
    )
  })
}

load_buoy_data <- function(catalog_path, item_id, start_time, end_time) {
  catalog_dir <- normalizePath(dirname(catalog_path), mustWork = TRUE)
  item_path <- file.path(catalog_dir, "items", paste0(item_id, ".json"))
  if (!file.exists(item_path)) {
    stop("Unable to find STAC item for buoy: ", item_path)
  }
  item <- jsonlite::fromJSON(item_path, simplifyVector = FALSE)
  if (is.null(item$assets$data$href)) {
    stop("Item does not expose a 'data' asset: ", item_path)
  }
  asset_path <- resolve_href(dirname(item_path), item$assets$data$href)
  message("Loading buoy observations from ", asset_path)
  obs_tbl <- as_tibble(arrow::read_parquet(asset_path))

  obs_tbl <- obs_tbl %>%
    mutate(
      timestamp = as.POSIXct(timestamp, tz = "UTC"),
      wind_speed = as.numeric(wind_speed),
      wind_dir = as.numeric(wind_dir)
    ) %>%
    mutate(
      wind_speed = ifelse(!is.na(wind_speed) & wind_speed <= -9000, NA_real_, wind_speed),
      wind_dir = ifelse(!is.na(wind_dir) & wind_dir <= -9000, NA_real_, wind_dir)
    ) %>%
    filter(!is.na(timestamp), !is.na(wind_speed), !is.na(wind_dir))

  if (!is.null(start_time)) {
    obs_tbl <- obs_tbl %>% filter(timestamp >= start_time)
  }
  if (!is.null(end_time)) {
    obs_tbl <- obs_tbl %>% filter(timestamp <= end_time)
  }
  if (!nrow(obs_tbl)) {
    stop("No buoy observations left after applying the requested filters.")
  }

  obs_tbl <- obs_tbl %>%
    mutate(
      timestamp_obs_original = timestamp,
      dir_rad = (wind_dir %% 360) * pi / 180,
      u_component = -wind_speed * sin(dir_rad),
      v_component = -wind_speed * cos(dir_rad)
    ) %>%
    transmute(
      timestamp,
      timestamp_obs_original,
      wind_speed_obs = wind_speed,
      wind_dir_obs = wind_dir,
      u_obs = u_component,
      v_obs = v_component
    )
  obs_tbl
}

collect_predictions <- function(prediction_path, node_id) {
  if (dir.exists(prediction_path)) {
    ds <- arrow::open_dataset(
      prediction_path,
      format = "parquet",
      partitioning = NULL,
      unify_schemas = TRUE
    ) %>%
      filter(interpolation_source == "test_point", node_id == .env$node_id)
    return(ds %>% collect())
  }
  if (file.exists(prediction_path)) {
    tbl <- as_tibble(arrow::read_parquet(prediction_path))
    return(tbl %>% filter(interpolation_source == "test_point", node_id == node_id))
  }
  stop("Prediction path does not exist: ", prediction_path)
}

load_prediction_data <- function(prediction_path,
                                 node_id,
                                 start_time,
                                 end_time) {
  message("Loading predictions from ", prediction_path)
  predictions <- collect_predictions(prediction_path, node_id)
  predictions <- predictions %>%
    mutate(
      timestamp = ymd_hms(timestamp, tz = "UTC"),
      wind_speed = as.numeric(wind_speed),
      wind_dir = as.numeric(wind_dir)
    )

  if (!nrow(predictions)) {
    stop("No prediction rows found for node_id=", node_id,
         ". Ensure the interpolation output includes the specified test point.")
  }

  if (!is.null(start_time)) {
    predictions <- predictions %>% filter(timestamp >= start_time)
  }
  if (!is.null(end_time)) {
    predictions <- predictions %>% filter(timestamp <= end_time)
  }
  if (!nrow(predictions)) {
    stop("No predictions left after applying the requested temporal filters.")
  }

  predictions <- predictions %>%
    mutate(
      timestamp_pred_original = timestamp,
    ) %>%
    transmute(
      timestamp,
      timestamp_pred_original,
      wind_speed_pred = wind_speed,
      wind_dir_pred = wind_dir,
      u_pred = u,
      v_pred = v
    )
  predictions
}

align_datasets <- function(predictions, observations) {
  aligned <- inner_join(
    predictions,
    observations,
    by = "timestamp"
  ) %>%
    arrange(timestamp)

  if (!nrow(aligned)) {
    stop(
      paste(
        "No overlapping timestamps were found between observations and predictions.",
        "Choose overlapping --obs-* / --pred-* intervals or regenerate the inputs."
      )
    )
  }
  aligned <- aligned %>% rename(timestamp_aligned = timestamp)
  aligned
}

compute_metric_row <- function(df, pred_col, obs_col, label) {
  pred <- df[[pred_col]]
  obs <- df[[obs_col]]
  valid <- is.finite(pred) & is.finite(obs)
  n <- sum(valid)
  if (!n) {
    return(tibble(variable = label, rmse = NA_real_, rsr = NA_real_, bias = NA_real_, samples = 0))
  }
  diff <- pred[valid] - obs[valid]
  rmse <- sqrt(mean(diff ^ 2))
  obs_sd <- stats::sd(obs[valid])
  rsr <- if (is.na(obs_sd) || obs_sd == 0) NA_real_ else rmse / obs_sd
  bias <- mean(diff)
  tibble(variable = label, rmse = rmse, rsr = rsr, bias = bias, samples = n)
}

compute_component_metrics <- function(aligned) {
  bind_rows(
    compute_metric_row(aligned, "wind_speed_pred", "wind_speed_obs", "Wind speed (m/s)"),
    compute_metric_row(aligned, "u_pred", "u_obs", "U component (m/s)"),
    compute_metric_row(aligned, "v_pred", "v_obs", "V component (m/s)")
  )
}

compute_speed_metrics <- function(aligned) {
  pred_speed <- aligned$wind_speed_pred
  obs_speed <- aligned$wind_speed_obs
  valid_speed <- is.finite(pred_speed) & is.finite(obs_speed)
  n_samples <- sum(valid_speed)
  if (n_samples == 0) {
    return(tibble(
      variable = "wind_speed",
      samples = 0,
      rmse_speed = NA_real_,
      mae_speed = NA_real_,
      bias_speed = NA_real_,
      corr_speed = NA_real_,
      r2_speed = NA_real_,
      si_speed = NA_real_,
      si_speed_max = NA_real_,
      eam_dir = NA_real_,
      eaam_dir = NA_real_,
      rmse_dir = NA_real_,
      compcorr_dir = NA_real_
    ))
  }

  error_speed <- pred_speed[valid_speed] - obs_speed[valid_speed]
  rmse <- sqrt(mean(error_speed ^ 2))
  mae <- mean(abs(error_speed))
  bias <- mean(error_speed)
  corr <- if (n_samples > 1) stats::cor(pred_speed, obs_speed, use = "complete.obs") else NA_real_
  r2_den <- sum((obs_speed[valid_speed] - mean(obs_speed[valid_speed])) ^ 2)
  r2 <- if (r2_den > 0) 1 - ((rmse ^ 2 * n_samples) / r2_den) else NA_real_
  std_error <- sqrt(mean((error_speed - bias) ^ 2))
  mean_true_speed <- mean(abs(obs_speed[valid_speed]))
  max_true_speed <- max(abs(obs_speed[valid_speed]))
  si <- if (mean_true_speed > 0) std_error / mean_true_speed else NA_real_
  si_max <- if (max_true_speed > 0) std_error / max_true_speed else NA_real_

  direction_columns <- c("wind_dir_pred", "wind_dir_obs")
  if (all(direction_columns %in% names(aligned))) {
    pred_dir <- aligned$wind_dir_pred
    obs_dir <- aligned$wind_dir_obs
    valid_dir <- is.finite(pred_dir) & is.finite(obs_dir)
    if (any(valid_dir)) {
      pred_rad <- deg_to_rad(pred_dir[valid_dir])
      obs_rad <- deg_to_rad(obs_dir[valid_dir])
      diff_rad <- atan2(sin(pred_rad - obs_rad), cos(pred_rad - obs_rad))
      abs_diff_rad <- abs(diff_rad)
      eam_dir <- rad_to_deg(mean(diff_rad))
      eaam_dir <- rad_to_deg(mean(abs_diff_rad))
      rmse_dir <- rad_to_deg(sqrt(mean(diff_rad ^ 2)))
      compcorr_real <- sum(cos(pred_rad) * cos(obs_rad) + sin(pred_rad) * sin(obs_rad))
      compcorr_imag <- sum(sin(pred_rad) * cos(obs_rad) - cos(pred_rad) * sin(obs_rad))
      compcorr_dir <- sqrt(compcorr_real ^ 2 + compcorr_imag ^ 2) / length(diff_rad)
    } else {
      eam_dir <- eaam_dir <- rmse_dir <- compcorr_dir <- NA_real_
    }
  } else {
    eam_dir <- eaam_dir <- rmse_dir <- compcorr_dir <- NA_real_
  }

  tibble(
    variable = "wind_speed",
    samples = n_samples,
    rmse_speed = rmse,
    mae_speed = mae,
    bias_speed = bias,
    corr_speed = corr,
    r2_speed = r2,
    si_speed = si,
    si_speed_max = si_max,
    eam_dir = eam_dir,
    eaam_dir = eaam_dir,
    rmse_dir = rmse_dir,
    compcorr_dir = compcorr_dir
  )
}

plot_timeseries <- function(aligned, output_dir, label) {
  file_path <- file.path(output_dir, "wind_speed_timeseries.png")
  grDevices::png(file_path, width = 9, height = 4.5, units = "in", res = 150)
  on.exit(grDevices::dev.off(), add = TRUE)

  ts <- aligned$timestamp_aligned
  plot(ts, aligned$wind_speed_obs,
       type = "l",
       col = "#33a02c",
       lwd = 2,
       xlab = "Aligned timestamp (UTC)",
       ylab = "Wind speed (m/s)",
       main = sprintf("%s wind speed", label))
  lines(ts, aligned$wind_speed_pred,
        col = "#1f78b4",
        lwd = 2)
  legend("topright",
         legend = c("Observed", "Predicted"),
         col = c("#33a02c", "#1f78b4"),
         lwd = 2,
         bty = "n")
  file_path
}

plot_scatter <- function(aligned, output_dir, label) {
  file_path <- file.path(output_dir, "wind_speed_scatter.png")
  grDevices::png(file_path, width = 5.5, height = 5, units = "in", res = 150)
  on.exit(grDevices::dev.off(), add = TRUE)

  plot(aligned$wind_speed_obs, aligned$wind_speed_pred,
       pch = 19,
       col = rgb(31, 120, 180, maxColorValue = 255, alpha = 190),
       xlab = "Observed wind speed (m/s)",
       ylab = "Predicted wind speed (m/s)",
       main = sprintf("Observed vs. predicted wind speed (%s)", label))
  abline(a = 0, b = 1, lty = 2, col = "#636363")
  file_path
}

execute_comparison <- function(run_cfg) {
  dir.create(run_cfg$output_dir, recursive = TRUE, showWarnings = FALSE)

  observations <- load_buoy_data(
    catalog_path = run_cfg$buoy_catalog,
    item_id = run_cfg$buoy_item,
    start_time = run_cfg$obs_start,
    end_time = run_cfg$obs_end
  )

  predictions <- load_prediction_data(
    prediction_path = run_cfg$prediction_path,
    node_id = run_cfg$node_id,
    start_time = run_cfg$pred_start,
    end_time = run_cfg$pred_end
  )

  aligned <- tryCatch(
    align_datasets(predictions, observations),
    error = function(err) {
      stop(
        sprintf(
          "Unable to align predictions (first timestamp: %s) with observations (first timestamp: %s). %s",
          format(min(predictions$timestamp), tz = "UTC"),
          format(min(observations$timestamp), tz = "UTC"),
          conditionMessage(err)
        ),
        call. = FALSE
      )
    }
  )
  metrics_components <- compute_component_metrics(aligned)
  metrics_speed <- compute_speed_metrics(aligned)

  metrics_combined <- bind_rows(metrics_speed, metrics_components)
  metrics_path <- file.path(run_cfg$output_dir, "metrics.csv")
  write.csv(metrics_combined, metrics_path, row.names = FALSE)
  write.csv(metrics_speed, file.path(run_cfg$output_dir, "metrics_speed.csv"), row.names = FALSE)
  write.csv(metrics_components, file.path(run_cfg$output_dir, "metrics_components.csv"), row.names = FALSE)

  comparison_path <- file.path(run_cfg$output_dir, "aligned_records.csv")
  write.csv(aligned, comparison_path, row.names = FALSE)

  fig_ts <- plot_timeseries(aligned, run_cfg$output_dir, run_cfg$label)
  fig_scatter <- plot_scatter(aligned, run_cfg$output_dir, run_cfg$label)

  report_path <- write_report(
    output_dir = run_cfg$output_dir,
    report_name = run_cfg$report_name,
    metrics_speed = metrics_speed,
    metrics_components = metrics_components,
    aligned = aligned,
    figures = c(fig_ts, fig_scatter),
    config = list(
      label = run_cfg$label,
      buoy_catalog = run_cfg$buoy_catalog,
      buoy_item = run_cfg$buoy_item,
      prediction_path = run_cfg$prediction_path,
      prediction_node_id = run_cfg$node_id
    )
  )

  message(sprintf("[%s] Report written to: %s", run_cfg$label, report_path))
  message(sprintf("[%s] Metrics CSV: %s", run_cfg$label, metrics_path))
  list(
    id = run_cfg$id,
    label = run_cfg$label,
    metrics_speed = metrics_speed,
    metrics_components = metrics_components,
    report_path = report_path,
    output_dir = run_cfg$output_dir
  )
}

format_component_metrics_table <- function(metrics_tbl) {
  lines <- c(
    "| Variable | RMSE | RSR | Bias | Samples |",
    "| --- | ---: | ---: | ---: | ---: |"
  )
  for (i in seq_len(nrow(metrics_tbl))) {
    row <- metrics_tbl[i, ]
    lines <- c(
      lines,
      sprintf(
        "| %s | %.3f | %s | %.3f | %d |",
        row$variable,
        row$rmse,
        ifelse(is.na(row$rsr), "NA", sprintf("%.3f", row$rsr)),
        row$bias,
        row$samples
      )
    )
  }
  lines
}

format_speed_metrics_table <- function(metrics_tbl) {
  row <- metrics_tbl[1, ]
  fmt <- function(value, digits = 3) {
    ifelse(is.finite(value), formatC(value, digits = digits, format = "f"), "NA")
  }
  sprintf_row <- paste0(
    "| ", row$samples,
    " | ", fmt(row$rmse_speed),
    " | ", fmt(row$mae_speed),
    " | ", fmt(row$bias_speed),
    " | ", fmt(row$corr_speed, 4),
    " | ", fmt(row$r2_speed, 4),
    " | ", fmt(row$si_speed),
    " | ", fmt(row$si_speed_max),
    " | ", fmt(row$eam_dir),
    " | ", fmt(row$eaam_dir),
    " | ", fmt(row$rmse_dir),
    " | ", fmt(row$compcorr_dir, 4),
    " |"
  )
  c(
    "| Samples | RMSE | MAE | Bias | Corr | R² | SI | SI_max | EAM (deg) | EAAM (deg) | RMSE_dir (deg) | CompCorr |",
    "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    sprintf_row
  )
}

write_report <- function(output_dir,
                         report_name,
                         metrics_speed,
                         metrics_components,
                         aligned,
                         figures,
                         config) {
  catalog_path_display <- format_relative_path(config$buoy_catalog)
  prediction_path_display <- format_relative_path(config$prediction_path)

  report_lines <- c(
    sprintf("# %s Comparison", config$label),
    "",
    sprintf("- Buoy catalog: `%s` (item: `%s`)", catalog_path_display, config$buoy_item),
    sprintf("- Prediction dataset: `%s` (node: `%s`)", prediction_path_display, config$prediction_node_id),
    sprintf("- Observation window: %s to %s",
            format(min(aligned$timestamp_obs_original), "%Y-%m-%d %H:%MZ"),
            format(max(aligned$timestamp_obs_original), "%Y-%m-%d %H:%MZ")),
    sprintf("- Prediction window: %s to %s",
            format(min(aligned$timestamp_pred_original), "%Y-%m-%d %H:%MZ"),
            format(max(aligned$timestamp_pred_original), "%Y-%m-%d %H:%MZ")),
    sprintf("- Matched samples: %d", nrow(aligned)),
    "",
    "## Metrics",
    ""
  )
  report_lines <- c(
    report_lines,
    "## Speed Metrics",
    "",
    format_speed_metrics_table(metrics_speed),
    "",
    "## Component Metrics",
    ""
  )
  report_lines <- c(report_lines, format_component_metrics_table(metrics_components), "", "## Figures", "")
  for (fig in figures) {
    title <- tools::file_path_sans_ext(basename(fig))
    report_lines <- c(report_lines, sprintf("![%s](%s)", title, basename(fig)), "")
  }
  report_path <- file.path(output_dir, report_name)
  writeLines(report_lines, report_path, useBytes = TRUE)
  report_path
}

main <- function() {
  args <- parse_args(commandArgs(trailingOnly = TRUE))
  cfg <- list(
    buoy_catalog = args[["buoy-catalog"]],
    buoy_item = args[["buoy-item"]] %||% "Vilano",
    prediction_path = normalize_prediction_path(args[["prediction-path"]]),
    prediction_node_id = args[["prediction-node-id"]] %||% "Vilano_buoy",
    output_dir = args[["output-dir"]],
    report_name = args[["report-name"]] %||% "vilano_report.md",
    obs_start = resolve_datetime(args[["obs-start"]]),
    obs_end = resolve_datetime(args[["obs-end"]]),
    pred_start = resolve_datetime(args[["pred-start"]]),
    pred_end = resolve_datetime(args[["pred-end"]]),
    buoy_config = args[["buoy-config"]]
  )

  if (is.null(cfg$output_dir) || !nzchar(cfg$output_dir)) {
    stop("Missing required argument: --output-dir")
  }

  if (!is.null(cfg$buoy_config)) {
    plan <- load_buoy_plan(cfg$buoy_config, cfg)
    results <- lapply(plan, execute_comparison)
    if (length(results) > 1) {
      dir.create(cfg$output_dir, recursive = TRUE, showWarnings = FALSE)
      components_summary <- bind_rows(lapply(results, function(res) {
        res$metrics_components %>% mutate(buoy_id = res$id, buoy_label = res$label)
      }))
      speed_summary <- bind_rows(lapply(results, function(res) {
        res$metrics_speed %>% mutate(buoy_id = res$id, buoy_label = res$label)
      }))
      write.csv(components_summary, file.path(cfg$output_dir, "components_summary.csv"), row.names = FALSE)
      write.csv(speed_summary, file.path(cfg$output_dir, "speed_summary.csv"), row.names = FALSE)
      message(sprintf("Combined %d buoy comparisons. Summaries saved under %s", length(results), cfg$output_dir))
    }
    return(invisible(TRUE))
  }

  if (is.null(cfg$buoy_catalog) || !nzchar(cfg$buoy_catalog)) {
    stop("Missing required argument: --buoy-catalog (or use --buoy-config)")
  }
  if (is.null(cfg$prediction_path)) {
    stop("Missing required argument: --prediction-path (or provide per-entry prediction_path in the config file)")
  }

  cfg$buoy_catalog <- normalizePath(cfg$buoy_catalog, mustWork = TRUE)

  run_cfg <- list(
    id = slugify_id(cfg$buoy_item, cfg$prediction_node_id),
    label = cfg$buoy_item,
    buoy_catalog = cfg$buoy_catalog,
    buoy_item = cfg$buoy_item,
    prediction_path = cfg$prediction_path,
    node_id = cfg$prediction_node_id,
    output_dir = cfg$output_dir,
    report_name = cfg$report_name,
    obs_start = cfg$obs_start,
    obs_end = cfg$obs_end,
    pred_start = cfg$pred_start,
    pred_end = cfg$pred_end
  )

  execute_comparison(run_cfg)
}

`%||%` <- function(x, y) {
  if (!is.null(x) && nzchar(x)) x else y
}

if (identical(environment(), globalenv())) {
  main()
}
