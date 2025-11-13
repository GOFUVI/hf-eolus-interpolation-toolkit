#!/usr/bin/env python3
"""
update_geoparquet_regions.py: Add or remove region definitions in a GeoParquet file.

Usage:
  update_geoparquet_regions.py --add '<region_json>' <geoparquet_file>
  update_geoparquet_regions.py --remove <region_name> <geoparquet_file>

Options:
  --add       Add a region defined by a JSON string with keys "name" and "polygon".
  --remove    Remove a region by its name.
"""

import sys
import json
import os
import pyarrow as pa
import pyarrow.parquet as pq

def discover_sidecar_path(parquet_path: str) -> str:
    hour_dir = os.path.dirname(parquet_path)
    day_dir = os.path.dirname(hour_dir)
    month_dir = os.path.dirname(day_dir)
    year_dir = os.path.dirname(month_dir)
    dataset_root = os.path.dirname(year_dir)
    metadata_root = os.path.join(os.path.dirname(dataset_root), "metadata", os.path.basename(dataset_root))
    return os.path.join(metadata_root,
                        os.path.basename(year_dir),
                        os.path.basename(month_dir),
                        os.path.basename(day_dir),
                        os.path.basename(hour_dir),
                        "metadata.json")


def load_sidecar(parquet_path: str):
    sidecar_path = discover_sidecar_path(parquet_path)
    if os.path.exists(sidecar_path):
        with open(sidecar_path, "r", encoding="utf-8") as fh:
            try:
                data = json.load(fh)
            except json.JSONDecodeError as exc:
                sys.exit(f"Failed to parse metadata sidecar {sidecar_path}: {exc}")
        return sidecar_path, data
    return sidecar_path, {}


def load_regions_from_table(table):
    df = table.to_pandas()
    regions_values = df.get('regions')
    if regions_values is not None:
        for value in regions_values:
            if isinstance(value, str) and value.strip():
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    break
    metadata = dict(table.schema.metadata or {})
    raw = metadata.get(b'regions')
    if raw:
        try:
            return json.loads(raw.decode('utf-8'))
        except Exception as exc:
            sys.exit(f"Failed to parse existing regions JSON metadata: {exc}")
    return []

def main():
    if len(sys.argv) != 4:
        sys.exit(__doc__)

    op, arg, path = sys.argv[1], sys.argv[2], sys.argv[3]
    table = pq.read_table(path)
    metadata = dict(table.schema.metadata or {})
    sidecar_path, sidecar_data = load_sidecar(path)

    regions = sidecar_data.get('regions')
    if regions is None:
        regions = load_regions_from_table(table)

    if op == '--add':
        try:
            region = json.loads(arg)
            if not isinstance(region, dict) or ('name' not in region and 'region_name' not in region) or 'polygon' not in region:
                raise ValueError
        except ValueError:
            sys.exit("Invalid region JSON. Must be a JSON object with 'name'/'region_name' and 'polygon' fields.")
        if 'name' in region and 'region_name' not in region:
            region['region_name'] = region.pop('name')
        regions.append(region)
    elif op == '--remove':
        name = arg
        regions = [r for r in regions if r.get('name') != name and r.get('region_name') != name]
    else:
        sys.exit(__doc__)

    if 'regions' in table.column_names:
        regions_json = json.dumps(regions)
        df = table.to_pandas()
        df['regions'] = regions_json
        new_table = pa.Table.from_pandas(df, preserve_index=False)
        new_meta = {k: v for k, v in metadata.items() if k != b'regions'}
        new_table = new_table.replace_schema_metadata(new_meta)
        pq.write_table(new_table, path)

    if sidecar_data.get('regions') != regions:
        sidecar_data['regions'] = regions
        os.makedirs(os.path.dirname(sidecar_path), exist_ok=True)
        with open(sidecar_path, "w", encoding="utf-8") as fh:
            json.dump(sidecar_data, fh, ensure_ascii=False, indent=2)
        print(f"Updated regions in sidecar {sidecar_path}")
    else:
        print("Regions unchanged; no sidecar update needed.")

if __name__ == '__main__':
    main()
