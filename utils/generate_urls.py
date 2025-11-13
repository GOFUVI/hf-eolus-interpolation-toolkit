#!/usr/bin/env python3
# generate_urls.py - Genera URLs de archivos NetCDF de MeteoGalicia para un rango de fechas.

import sys
import os
from datetime import datetime, timedelta
import json
import csv

MODEL_CONFIGS = {
    "wrf4km": {
        "base_url": "https://mandeo.meteogalicia.es/thredds/fileServer/modelos/WRF_HIST/d03",
        "filename_template": "wrf_arw_det_history_d03_{date}_0000.nc4",
        "folder_style": "year_month",
        "description": "WRF_HIST domain d03 (~4 km).",
    },
    "wrf1_3km": {
        "base_url": "https://mandeo.meteogalicia.es/thredds/fileServer/modelos/WRF_ARW_1KM_HIST/d05",
        "filename_template": "wrf_arw_det1km_history_d05_{date}_0000.nc4",
        "folder_style": "year_month",
        "description": "WRF_ARW_1KM_HIST domain d05 (1.3 km).",
    },
    "wrf1km": {
        "base_url": "https://mandeo.meteogalicia.es/thredds/fileServer/modelos/WRF_ARW_1KM_HIST_Novo",
        "filename_template": "wrf_arw_det_history_d02_{date}_0000.nc4",
        "folder_style": "yyyymmdd",
        "description": "WRF_ARW_1KM_HIST_Novo domain d02 (1 km).",
    },
}

DEFAULT_MODEL = "wrf4km"


def _print_usage():
    print(
        "Usage: generate_urls.py [-m wrf4km|wrf1_3km|wrf1km] "
        "<YYYY-MM-DD_start> <YYYY-MM-DD_end> "
        "[<boundary1.geojson> [<region_name1>]]... "
        "[<test_points.csv>]",
        file=sys.stderr,
    )


def _extract_model_option(argv):
    """Return (model_choice, remaining_args) after consuming -m/--model flags."""
    model = DEFAULT_MODEL
    remaining = []
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg in ("-m", "--model"):
            if i + 1 >= len(argv):
                print("Error: option -m/--model requires an argument.", file=sys.stderr)
                _print_usage()
                sys.exit(1)
            model = argv[i + 1].lower()
            i += 2
        elif arg.startswith("--model="):
            model = arg.split("=", 1)[1].lower()
            i += 1
        else:
            remaining.append(arg)
            i += 1
    return model, remaining


args = sys.argv[1:]
model_choice, positional_args = _extract_model_option(args)
if model_choice not in MODEL_CONFIGS:
    allowed = ", ".join(sorted(MODEL_CONFIGS.keys()))
    print(f"Error: unknown model '{model_choice}'. Allowed values: {allowed}", file=sys.stderr)
    _print_usage()
    sys.exit(1)

if len(positional_args) < 2:
    _print_usage()
    sys.exit(1)

try:
    start_date = datetime.strptime(positional_args[0], "%Y-%m-%d")
    end_date = datetime.strptime(positional_args[1], "%Y-%m-%d")
except ValueError as e:
    print(f"Error en formato de fecha: {e}", file=sys.stderr)
    sys.exit(1)

if end_date < start_date:
    print("Error: la fecha final es anterior a la inicial", file=sys.stderr)
    sys.exit(1)

def _build_url(model_key, date_obj):
    cfg = MODEL_CONFIGS[model_key]
    date_str = date_obj.strftime("%Y%m%d")
    filename = cfg["filename_template"].format(date=date_str)
    if cfg["folder_style"] == "year_month":
        year = date_obj.strftime("%Y")
        month = date_obj.strftime("%m")
        return f"{cfg['base_url']}/{year}/{month}/{filename}"
    if cfg["folder_style"] == "yyyymmdd":
        folder = date_obj.strftime("%Y%m%d")
        return f"{cfg['base_url']}/{folder}/{filename}"
    raise ValueError(f"Unsupported folder style {cfg['folder_style']} for model {model_key}")


urls = []
current = start_date
while current <= end_date:
    urls.append(_build_url(model_choice, current))
    current += timedelta(days=1)

# Create JSON output with list of URLs
output = {"urlList": urls, "source_model": model_choice}

# Separate optional test points CSV from region arguments
extra_args = positional_args[2:]
csv_args = [arg for arg in extra_args if arg.lower().endswith('.csv')]
if len(csv_args) > 1:
    print("Error: only one test_points CSV may be provided", file=sys.stderr)
    sys.exit(1)
csv_path = csv_args[0] if csv_args else None
region_args = [arg for arg in extra_args if not arg.lower().endswith('.csv')]

# Parse multiple boundary.geojson + optional region_name pairs
regions = []
i = 0
while i < len(region_args):
    geojson_path = region_args[i]
    region_name = None
    if i + 1 < len(region_args):
        region_name = region_args[i+1]
        i += 2
    else:
        i += 1
    try:
        with open(geojson_path, 'r') as f:
            gj = json.load(f)
        # Extract geometry
        if gj.get('type') == 'FeatureCollection':
            feats = gj.get('features', [])
            if not feats:
                raise ValueError('Empty FeatureCollection')
            geom = feats[0].get('geometry', {})
        elif gj.get('type') == 'Feature':
            geom = gj.get('geometry', {})
        elif gj.get('type') in ('Polygon', 'MultiPolygon'):
            geom = gj
        else:
            raise ValueError(f"Unsupported GeoJSON type: {gj.get('type')}")
        if geom.get('type') == 'Polygon':
            polygon = geom.get('coordinates', [])[0]
        elif geom.get('type') == 'MultiPolygon':
            polygon = geom.get('coordinates', [])[0][0]
        else:
            raise ValueError(f"Unsupported geometry type: {geom.get('type')}")
    except Exception as e:
        print(f"Error reading GeoJSON boundary '{geojson_path}': {e}", file=sys.stderr)
        sys.exit(1)
    if not region_name:
        region_name = os.path.splitext(os.path.basename(geojson_path))[0]
    regions.append({"region_name": region_name, "polygon": polygon})
# Add regions to output if provided
if regions:
    output["regions"] = regions

 # Parse single test points CSV if provided
test_points = []
if csv_path:
    try:
        with open(csv_path, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    lon = float(row.get('lon') or row.get('longitude'))
                    lat = float(row.get('lat') or row.get('latitude'))
                except (TypeError, ValueError):
                    raise ValueError(f"Invalid coordinates in {csv_path}: {row}")
                name = row.get('name') or row.get('test_point') or row.get('Name')
                if not name:
                    raise ValueError(f"Missing name for test point in {csv_path}: {row}")
                test_points.append({'name': name, 'lon': lon, 'lat': lat})
    except Exception as e:
        print(f"Error reading test points CSV '{csv_path}': {e}", file=sys.stderr)
        sys.exit(1)
if test_points:
    output['test_points'] = test_points

print(json.dumps(output))
