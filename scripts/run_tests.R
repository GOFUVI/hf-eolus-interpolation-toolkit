project_root <- getwd()
activate_script <- file.path(project_root, "renv", "activate.R")
if (file.exists(activate_script)) {
  source(activate_script)
  if ("renv" %in% loadedNamespaces()) {
    renv::activate(project = project_root)
  }
}

ensure_namespace <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    base_library <- R.home("library")
    .libPaths(unique(c(.libPaths(), base_library)))
  }
}

ensure_namespace("testthat")
ensure_namespace("jsonlite")

suppressPackageStartupMessages({
  library(testthat)
  library(jsonlite)
})

resolve_renv_library <- function() {
  root <- file.path("renv", "library")
  if (!dir.exists(root)) {
    return(invisible(NULL))
  }
  platforms <- list.dirs(root, recursive = FALSE, full.names = TRUE)
  if (!length(platforms)) {
    return(invisible(NULL))
  }
  versions <- list.dirs(platforms[[1]], recursive = FALSE, full.names = TRUE)
  if (!length(versions)) {
    return(invisible(NULL))
  }
  libs <- list.dirs(versions[[1]], recursive = FALSE, full.names = TRUE)
  if (!length(libs)) {
    return(invisible(NULL))
  }
  target <- libs[[1]]
  if (!target %in% .libPaths()) {
    .libPaths(c(target, .libPaths()))
  }
  invisible(target)
}

 instrument_modules <- function(files) {
  modules_env <- new.env(parent = .GlobalEnv)
   module_defs <- list()
   for (file in files) {
     before <- ls(modules_env, all.names = TRUE)
     sys.source(file, envir = modules_env, keep.source = TRUE)
     after <- ls(modules_env, all.names = TRUE)
     module_defs[[file]] <- setdiff(after, before)
   }
   tracker <- new.env(parent = emptyenv())
   tracker$hits <- logical()
   tracker$files <- character()
   for (file in files) {
     defs <- module_defs[[file]]
     for (name in defs) {
       obj <- get(name, envir = modules_env)
       if (!is.function(obj)) {
         next
       }
       id <- sprintf("%s::%s", basename(file), name)
       tracker$hits[[id]] <- FALSE
       tracker$files[[id]] <- file
      wrapper <- local({
        original <- obj
        key <- id
        tracker_env <- tracker
        function(...) {
          tracker_env$hits[[key]] <- TRUE
          original(...)
        }
      })
      parent.env(environment(wrapper)) <- modules_env
      assign(name, wrapper, envir = modules_env)
    }
  }
  list(env = modules_env, tracker = tracker)
}

resolve_renv_library()
module_files <- list.files(file.path("scripts", "modules"), pattern = "_module\\.R$", full.names = TRUE)
module_order <- c(
  "utils_module.R",
  "ingestion_module.R",
  "grid_module.R",
  "interpolation_module.R",
  "export_module.R"
)
module_files <- module_files[order(match(basename(module_files), module_order))]
coverage_ctx <- instrument_modules(module_files)
assign("modules_env", coverage_ctx$env, envir = .GlobalEnv)
options(wind.modules_env = coverage_ctx$env)

test_results <- test_dir("tests/testthat", reporter = SummaryReporter$new())

issue_detected <- vapply(test_results, function(res) {
  isTRUE(res$failed > 0) || isTRUE(res$error > 0) || isTRUE(res$skipped > 0)
}, logical(1))
if (any(issue_detected)) {
  stop("Unit tests failed. See output above for details.")
}

hits <- coverage_ctx$tracker$hits
files_map <- coverage_ctx$tracker$files
covered <- sum(hits)
total <- length(hits)
percent <- if (total == 0) 100 else round(covered / total * 100, 2)

file_stats <- lapply(unique(files_map), function(file) {
  ids <- names(files_map)[files_map == file]
  list(
    file = file,
    functions = length(ids),
    covered = sum(hits[ids]),
    percent = if (length(ids)) round(sum(hits[ids]) / length(ids) * 100, 2) else 100
  )
})

coverage_summary <- list(
  overall = list(total_functions = total, covered_functions = covered, percent = percent),
  files = file_stats
)

dir.create("logs", showWarnings = FALSE)
jsonlite::write_json(coverage_summary, file.path("logs", "coverage_summary.json"), auto_unbox = TRUE, pretty = TRUE)

message(sprintf("Overall function coverage: %.2f%% (%d/%d)", percent, covered, total))

if (percent < 80) {
  stop(sprintf("Coverage %.2f%% is below target of 80%%", percent))
}
