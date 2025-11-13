#!/usr/bin/env Rscript
# debug_region.R: inspect region metadata, bboxes and intersection count

library(arrow)
library(sf)
library(jsonlite)

sidecar_path_for <- function(geo_path) {
  hour_dir <- dirname(geo_path)
  day_dir <- dirname(hour_dir)
  month_dir <- dirname(day_dir)
  year_dir <- dirname(month_dir)
  dataset_root <- dirname(year_dir)
  metadata_root <- file.path(dirname(dataset_root), "metadata", basename(dataset_root))
  file.path(metadata_root,
            basename(year_dir),
            basename(month_dir),
            basename(day_dir),
            basename(hour_dir),
            "metadata.json")
}

# Path to a sample GeoParquet file
parquet_path <- "/app/local_data/year=2025/month=01/day=01/hour=00/data.parquet"
tbl <- tryCatch(
  read_parquet(parquet_path, as_data_frame = FALSE),
  error = function(e) stop("Failed to read parquet: ", e$message)
)

df_tbl    <- as.data.frame(tbl)

regions_meta <- NULL
sidecar_path <- sidecar_path_for(parquet_path)
if (file.exists(sidecar_path)) {
  sidecar_contents <- read_json(sidecar_path, simplifyVector = FALSE)
  if (!is.null(sidecar_contents$regions) && length(sidecar_contents$regions) > 0) {
    regions_meta <- sidecar_contents$regions
  }
}

if (is.null(regions_meta)) {
  if (!"regions" %in% names(df_tbl)) {
    stop("Regions metadata not found in sidecar or GeoParquet columns for ", parquet_path)
  }
  col_vals <- df_tbl$regions
  col_vals <- col_vals[!is.na(col_vals) & nzchar(col_vals)]
  if (!length(col_vals)) stop("Column 'regions' is empty in ", parquet_path)
  regions_meta <- fromJSON(col_vals[[1]], simplifyVector = FALSE)
}

if (!is.list(regions_meta)) {
  stop("Unable to interpret regions metadata structure")
}
if (!is.null(regions_meta$polygon)) {
  regions_meta <- list(regions_meta)
}
poly_coords  <- regions_meta[[1]]$polygon

# Ensure numeric coords and close ring
coords_mat <- do.call(rbind, lapply(poly_coords, as.numeric))
if (!all(coords_mat[1,] == coords_mat[nrow(coords_mat),])) {
  coords_mat <- rbind(coords_mat, coords_mat[1,])
}

# Build sf polygon and sf points
region_sf <- st_sf(geometry = st_sfc(st_polygon(list(coords_mat))), crs = 4326)
df_tbl$geometry <- st_as_sfc(df_tbl$geometry, EWKB = TRUE)
pts_sf    <- st_sf(df_tbl, crs = 4326)

# Print bounding boxes and intersection count
cat("== REGION BBOX ==\n")
print(st_bbox(region_sf))
cat("\n== POINTS BBOX ==\n")
print(st_bbox(pts_sf))
cat("\n== INTERSECTION COUNT ==\n")
int_sel <- st_intersects(pts_sf, region_sf, sparse = FALSE)[,1]
cat(sum(int_sel), "points intersect the region\n")
