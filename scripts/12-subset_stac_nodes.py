#!/usr/bin/env python3
"""
Subset interpolation STAC nodes into a single GeoParquet and emit a derived STAC package.

This CLI:
1) Reads a source STAC collection of interpolated winds.
2) Selects rows by node_id and/or spatial intersection against a polygon.
3) Writes a single GeoParquet with the filtered rows.
4) Builds a new STAC collection (and item) that preserves provenance to the source catalog.

GeoParquet filtering is executed via DuckDB inside the `duckdb/duckdb:latest` Docker image.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Iterable, List, Optional, Sequence
import shutil

import pystac
from pystac import Asset, CatalogType, Collection, Extent, Item, Link

try:
    import pyarrow.parquet as pq  # type: ignore
except ImportError:
    pq = None  # pragma: no cover - optional, only used for row count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Extract a subset of nodes from an interpolation STAC catalog and package them "
            "into a single GeoParquet plus a derived STAC collection."
        )
    )
    parser.add_argument(
        "--source-catalog",
        required=True,
        help="Path to the source STAC collection.json containing interpolation items.",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Destination directory for the new STAC collection (will be created if missing).",
    )
    parser.add_argument(
        "--output-filename",
        default="subset.parquet",
        help="Name for the output Parquet/GeoParquet (stored under <output-dir>/assets/).",
    )
    parser.add_argument(
        "--asset-key",
        default="data",
        help="Asset key (in each item) pointing to the GeoParquet to subset.",
    )
    parser.add_argument(
        "--node-id",
        action="append",
        dest="node_ids",
        help="Node ID to retain (repeatable).",
    )
    parser.add_argument(
        "--node-id-file",
        help="File containing node IDs to retain (one per line).",
    )
    parser.add_argument(
        "--geometry-file",
        help="GeoJSON file with a Polygon/MultiPolygon geometry to intersect.",
    )
    parser.add_argument(
        "--geometry-wkt",
        help="WKT geometry string to intersect (Polygon/MultiPolygon).",
    )
    parser.add_argument(
        "--geometry-column",
        default="geometry",
        help="Geometry column present in the GeoParquet.",
    )
    parser.add_argument(
        "--geometry-format",
        choices=["wkb", "wkt"],
        default="wkb",
        help="How to interpret the geometry column (wkb or wkt).",
    )
    parser.add_argument(
        "--collection-id",
        help="ID for the derived collection (defaults to <source-id>-subset).",
    )
    parser.add_argument(
        "--collection-title",
        help="Title for the derived collection (defaults to source title + ' subset').",
    )
    parser.add_argument(
        "--collection-description",
        help="Description override for the derived collection.",
    )
    parser.add_argument(
        "--item-id",
        help="ID for the derived item (defaults to <collection-id>-subset).",
    )
    parser.add_argument(
        "--mount-root",
        help=(
            "Host path to mount into the DuckDB container; must enclose source assets and output. "
            "If omitted, the script will compute a common ancestor."
        ),
    )
    parser.add_argument(
        "--duckdb-image",
        default="duckdb/duckdb:latest",
        help="Docker image used to run DuckDB (default: duckdb/duckdb:latest).",
    )
    parser.add_argument(
        "--skip-source-items",
        action="store_true",
        help="Do not enumerate all source Items for traceability (useful when the catalog is incomplete).",
    )
    parser.add_argument(
        "--parent-catalog",
        help="Optional existing catalog.json to register the new collection link.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing outputs under --output-dir/assets/ and replace collection metadata.",
    )
    return parser.parse_args()


def load_collection(path: Path) -> Collection:
    collection = pystac.read_file(str(path))
    if not isinstance(collection, Collection):
        raise SystemExit("The provided source catalog is not a STAC Collection.")
    return collection


def read_node_ids(args: argparse.Namespace) -> List[str]:
    node_ids: List[str] = []
    if args.node_ids:
        node_ids.extend(args.node_ids)
    if args.node_id_file:
        with open(args.node_id_file, "r", encoding="utf-8") as handle:
            for line in handle:
                value = line.strip()
                if value:
                    node_ids.append(value)
    return sorted(set(node_ids))


def load_geometry_filter(args: argparse.Namespace) -> Optional[str]:
    if args.geometry_file:
        geojson = json.loads(Path(args.geometry_file).read_text(encoding="utf-8"))
        if geojson.get("type") == "FeatureCollection":
            features = geojson.get("features", [])
            if not features:
                raise SystemExit("The GeoJSON feature collection is empty.")
            geometry = features[0].get("geometry")
        elif geojson.get("type") == "Feature":
            geometry = geojson.get("geometry")
        else:
            geometry = geojson
        if not geometry:
            raise SystemExit("Could not extract a geometry from the GeoJSON input.")
        return json.dumps(geometry)
    if args.geometry_wkt:
        return args.geometry_wkt
    return None


def ensure_paths_under_root(paths: Sequence[Path], root: Path) -> None:
    for path in paths:
        try:
            path.resolve().relative_to(root)
        except ValueError:
            raise SystemExit(
                f"Path {path} is not under the mount root {root}. "
                "Provide an explicit --mount-root that encloses all assets and the output."
            )


def compute_mount_root(
    paths: Sequence[Path], override_root: Optional[str]
) -> Path:
    if override_root:
        root = Path(override_root).resolve()
    else:
        try:
            root = Path(os.path.commonpath([p.resolve() for p in paths]))
        except ValueError:
            raise SystemExit(
                "Assets and output live on different drives. Please provide --mount-root explicitly."
            )
    ensure_paths_under_root(paths, root)
    return root


def escape_single_quotes(text: str) -> str:
    return text.replace("'", "''")


def to_container_path(path: Path, mount_root: Path) -> str:
    rel = path.resolve().relative_to(mount_root)
    return f"/mnt/data/{rel.as_posix()}"


def build_duckdb_sql(
    asset_paths: Sequence[str],
    output_path: str,
    node_ids: Sequence[str],
    geometry_filter: Optional[str],
    geom_format: str,
    geometry_column: str,
) -> str:
    files_expr = ", ".join(f"'{escape_single_quotes(p)}'" for p in asset_paths)
    where_clauses: List[str] = []
    if node_ids:
        node_expr = ", ".join(f"'{escape_single_quotes(nid)}'" for nid in node_ids)
        where_clauses.append(f"node_id IN ({node_expr})")
    if geometry_filter:
        if geometry_filter.strip().startswith("{"):
            geom_expr = f"ST_GeomFromGeoJSON('{escape_single_quotes(geometry_filter)}')"
        else:
            geom_expr = f"ST_GeomFromText('{escape_single_quotes(geometry_filter)}')"
        if geom_format == "wkt":
            geom_column_expr = f"ST_GeomFromText({geometry_column})"
        else:
            geom_column_expr = f"ST_GeomFromWKB({geometry_column})"
        where_clauses.append(f"ST_Intersects({geom_column_expr}, {geom_expr})")
    where_sql = " AND ".join(where_clauses) if where_clauses else "TRUE"
    statements = [
        "INSTALL spatial",
        "LOAD spatial",
        f"COPY (SELECT * FROM read_parquet([{files_expr}]) WHERE {where_sql}) "
        f"TO '{escape_single_quotes(output_path)}' "
        "(FORMAT 'parquet', COMPRESSION 'ZSTD')",
    ]
    return "; ".join(statements) + ";"


def run_duckdb_container(
    sql_docker: str,
    sql_local: str,
    mount_root: Path,
    image: str,
) -> None:
    def run_duckdb_local() -> None:
        # Try Python duckdb module first
        try:
            import duckdb  # type: ignore
            try:
                con = duckdb.connect(database=str(mount_root / "tmp.duckdb"))
                try:
                    con.execute("PRAGMA disable_progress_bar")
                except Exception:
                    pass
                for stmt in [s.strip() for s in sql_local.split(";") if s.strip()]:
                    con.execute(stmt)
                con.close()
                return
            except Exception as exc:
                raise SystemExit(f"DuckDB execution failed locally via Python: {exc}")
        except ImportError:
            pass

        # Fallback to duckdb CLI if available
        if shutil.which("duckdb") is not None:
            cli_cmd = [
                "duckdb",
                "-c",
                sql_local,
            ]
            result = subprocess.run(
                cli_cmd, check=False, capture_output=True, text=True, cwd=mount_root
            )
            if result.returncode != 0:
                sys.stderr.write(result.stderr)
                raise SystemExit(
                    f"DuckDB CLI failed with exit code {result.returncode}."
                )
            return

        raise SystemExit(
            "DuckDB execution failed: neither docker, duckdb module, nor duckdb CLI available."
        )

    # Try docker first if present; fallback to local duckdb on failure or absence.
    if shutil.which("docker") is None:
        run_duckdb_local()
        return

    command = [
        "docker",
        "run",
        "--rm",
        "-v",
        f"{mount_root}:/mnt/data",
        "-w",
        "/mnt/data",
        image,
        "-c",
        sql_docker,
    ]
    result = subprocess.run(
        command, check=False, capture_output=True, text=True
    )
    if result.returncode != 0:
        sys.stderr.write(result.stderr)
        sys.stderr.write(
            "\nDocker execution failed; falling back to local duckdb if available...\n"
        )
        run_duckdb_local()


def snapshot_source_items(
    items: Sequence[Item], asset_key: str
) -> List[Dict[str, Any]]:
    snapshot: List[Dict[str, Any]] = []
    for item in items:
        asset = item.assets.get(asset_key)
        snapshot.append(
            {
                "id": item.id,
                "href": item.get_self_href(),
            }
        )
    return snapshot


def load_item_dict(item: Item, source_root: Path) -> Dict[str, Any]:
    href = item.get_self_href()
    if href and "://" not in href and Path(href).exists():
        try:
            return json.loads(Path(href).read_text(encoding="utf-8"))
        except Exception:
            pass
    candidate = source_root / "items" / "parquet" / item.id / f"{item.id}.json"
    if candidate.exists():
        try:
            return json.loads(candidate.read_text(encoding="utf-8"))
        except Exception:
            pass
    return item.to_dict()


def union_bbox(items: Sequence[Item]) -> Optional[List[float]]:
    mins = [float("inf"), float("inf")]
    maxs = [float("-inf"), float("-inf")]
    found = False
    for item in items:
        if not item.bbox:
            continue
        found = True
        mins[0] = min(mins[0], item.bbox[0])
        mins[1] = min(mins[1], item.bbox[1])
        maxs[0] = max(maxs[0], item.bbox[2])
        maxs[1] = max(maxs[1], item.bbox[3])
    if not found:
        return None
    return [mins[0], mins[1], maxs[0], maxs[1]]


def bbox_to_polygon(bbox: Optional[List[float]]) -> Optional[Dict[str, Any]]:
    if bbox is None:
        return None
    minx, miny, maxx, maxy = bbox
    return {
        "type": "Polygon",
        "coordinates": [
            [
                [minx, miny],
                [maxx, miny],
                [maxx, maxy],
                [minx, maxy],
                [minx, miny],
            ]
        ],
    }


def parse_item_datetimes(items: Sequence[Item]) -> Dict[str, Optional[datetime]]:
    def to_datetime(value: Any) -> Optional[datetime]:
        if not value:
            return None
        if isinstance(value, datetime):
            return value
        text = str(value)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None

    starts: List[datetime] = []
    ends: List[datetime] = []
    for item in items:
        props = item.properties
        start = to_datetime(
            props.get("start_datetime") or props.get("datetime") or item.datetime
        )
        end = to_datetime(props.get("end_datetime") or item.datetime)
        if start:
            starts.append(start)
        if end:
            ends.append(end)
    return {
        "start": min(starts) if starts else None,
        "end": max(ends) if ends else None,
    }


def safe_isoformat(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    iso = dt.isoformat()
    return iso if dt.utcoffset() else iso + "Z"


def copy_extent(source_extent: Extent) -> Extent:
    return Extent.from_dict(source_extent.to_dict())


def log(msg: str) -> None:
    now = datetime.utcnow().isoformat()
    print(f"[subset] {now} - {msg}", flush=True)


def read_row_count(path: Path) -> Optional[int]:
    if pq is None:
        return None
    try:
        return pq.ParquetFile(path).metadata.num_rows
    except Exception:
        return None


def register_parent_catalog(
    parent_catalog: Path, new_collection_href: Path, title: str
) -> None:
    data = json.loads(parent_catalog.read_text(encoding="utf-8"))
    links: List[Dict[str, Any]] = data.get("links", [])
    rel_href = os.path.relpath(
        new_collection_href, start=parent_catalog.parent
    ).replace(os.sep, "/")
    if any(link.get("href") == f"./{rel_href}" or link.get("href") == rel_href for link in links):
        return
    links.append(
        {
            "rel": "child",
            "href": f"./{rel_href}",
            "type": "application/json",
            "title": title,
        }
    )
    data["links"] = links
    parent_catalog.write_text(json.dumps(data, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    source_path = Path(args.source_catalog).resolve()
    log(f"Starting subset. Source collection: {source_path}")
    collection = load_collection(source_path)
    all_items: List[Any] = []
    if not args.skip_source_items:
        all_items = list(collection.get_all_items())

    node_ids = read_node_ids(args)
    geometry_filter = load_geometry_filter(args)
    if not node_ids and not geometry_filter:
        raise SystemExit("Provide at least one filter: --node-id/--node-id-file or --geometry-file/--geometry-wkt.")

    collection_base = source_path.parent
    asset_root = collection_base / "assets" / "parquet"
    asset_paths: List[Path] = sorted(asset_root.glob("**/data.parquet"))
    log(f"Found {len(asset_paths)} parquet partitions under {asset_root}")
    if not asset_paths:
        raise SystemExit(f"No Parquet assets found under {asset_root}")

    items_root = collection_base / "items" / "parquet"
    item_files = sorted(items_root.glob("**/*.json"))
    if not item_files:
        raise SystemExit("Could not locate any source item JSON to clone.")
    template_json_path = item_files[0]
    log(f"Using template item: {template_json_path}")
    template_dict = json.loads(template_json_path.read_text(encoding="utf-8"))
    source_item_href = str(template_json_path.resolve())
    source_items = []
    if not args.skip_source_items:
        source_items = [{"id": p.stem, "href": str(p.resolve())} for p in item_files]
    output_dir = Path(args.output_dir).resolve()
    assets_dir = output_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    output_file = assets_dir / args.output_filename
    if output_file.exists() and not args.force:
        raise SystemExit(
            f"Output file {output_file} already exists. Use --force to overwrite."
        )

    mount_root = compute_mount_root(asset_paths + [output_file], args.mount_root)
    container_assets = [to_container_path(p, mount_root) for p in asset_paths]
    container_output = to_container_path(output_file, mount_root)
    log("Building DuckDB SQL")
    sql_docker = build_duckdb_sql(
        container_assets,
        container_output,
        node_ids=node_ids,
        geometry_filter=geometry_filter,
        geom_format=args.geometry_format,
        geometry_column=args.geometry_column,
    )
    sql_local = build_duckdb_sql(
        [str(p) for p in asset_paths],
        str(output_file),
        node_ids=node_ids,
        geometry_filter=geometry_filter,
        geom_format=args.geometry_format,
        geometry_column=args.geometry_column,
    )
    log("Executing DuckDB (container or local fallback)")
    run_duckdb_container(
        sql_docker, sql_local, mount_root=mount_root, image=args.duckdb_image
    )
    if not output_file.exists():
        raise SystemExit("DuckDB reported success but the output file was not created.")
    log(f"DuckDB finished. Output written to {output_file}")

    bbox = template_dict.get("bbox")
    geom = template_dict.get("geometry") or bbox_to_polygon(bbox)
    # Compute temporal range from parquet paths
    times = []
    for p in asset_paths:
        parts = p.parts
        try:
            year = next(x for x in parts if x.startswith("year=")).split("=")[1]
            month = next(x for x in parts if x.startswith("month=")).split("=")[1]
            day = next(x for x in parts if x.startswith("day=")).split("=")[1]
            hour = next(x for x in parts if x.startswith("hour=")).split("=")[1]
            times.append(datetime.fromisoformat(f"{year}-{month}-{day}T{hour}:00:00"))
        except Exception:
            continue
    dt_range = {"start": min(times) if times else None, "end": max(times) if times else None}
    item_datetime = dt_range["start"] or dt_range["end"]
    row_count = read_row_count(output_file)

    derived_collection_id = args.collection_id or f"{collection.id}-subset"
    derived_title = args.collection_title or f"{collection.title or collection.id} subset"
    derived_description = args.collection_description or (
        collection.description or ""
    )

    derived_collection = Collection(
        id=derived_collection_id,
        description=derived_description,
        extent=copy_extent(collection.extent),
        title=derived_title,
        license=collection.license,
        keywords=collection.keywords,
        providers=collection.providers,
        summaries=collection.summaries,
    )
    derived_collection.stac_extensions = list(collection.stac_extensions)
    derived_collection.extra_fields = json.loads(
        json.dumps(collection.extra_fields, default=str)
    )
    source_href = collection.get_self_href() or str(source_path)
    derived_collection.add_link(
        Link(rel="derived_from", target=source_href, media_type="application/json")
    )

    item_id = args.item_id or "subset"

    # Clone source item dict to preserve all metadata (sci:doi, providers, links)
    new_item = json.loads(json.dumps(template_dict))
    new_item["id"] = item_id

    orig_desc = (
        str(template_dict.get("description") or "").strip()
        or str(collection.description or "").strip()
    )
    new_item["description"] = (
        f"Subset: {orig_desc}" if orig_desc else "Subset of interpolated winds"
    )

    # Update properties with subset traceability
    properties: Dict[str, Any] = new_item.get("properties", {})
    properties["subset:source_catalog"] = source_href
    if source_items:
        properties["subset:source_items"] = source_items
    properties["subset:filters"] = {
        "node_ids": node_ids,
        "geometry": geometry_filter,
        "geometry_column": args.geometry_column,
        "geometry_format": args.geometry_format,
    }
    if dt_range["start"]:
        properties["start_datetime"] = safe_isoformat(dt_range["start"])
    if dt_range["end"]:
        properties["end_datetime"] = safe_isoformat(dt_range["end"])
    if row_count is not None:
        properties["table:row_count"] = row_count
    new_item["properties"] = properties

    # Adjust assets
    if "assets" not in new_item:
        new_item["assets"] = {}
    if "data" not in new_item["assets"]:
        new_item["assets"]["data"] = {}
    data_asset = new_item["assets"]["data"]
    data_asset["href"] = "../assets/" + args.output_filename
    data_asset.setdefault("media_type", "application/vnd.apache.parquet")
    roles = data_asset.get("roles") or ["data"]
    data_asset["roles"] = roles
    title = data_asset.get("title") or "Interpolated winds GeoParquet"
    if "subset" not in title.lower():
        data_asset["title"] = f"{title} (subset)"
    else:
        data_asset["title"] = title

    # Adjust links: only root/parent pointing to new collection and via to source item
    new_links = []
    new_links.append(
        {
            "rel": "root",
            "href": "../collection.json",
            "type": "application/json",
            "title": derived_collection.title,
        }
    )
    new_links.append(
        {
            "rel": "parent",
            "href": "../collection.json",
            "type": "application/json",
            "title": derived_collection.title,
        }
    )
    if source_item_href:
        new_links.append(
            {
                "rel": "via",
                "href": source_item_href,
                "type": "application/json",
            }
        )
    new_item["links"] = new_links
    items_dir = output_dir / "items"
    items_dir.mkdir(parents=True, exist_ok=True)
    # clean previous items to avoid stale copies
    for old_item in items_dir.glob("*.json"):
        old_item.unlink()
    item_href = items_dir / f"{item_id}.json"
    item_href.write_text(json.dumps(new_item, indent=2), encoding="utf-8")
    log(f"Saved subset item to {item_href}")

    # Build collection JSON manually to avoid item rewriting
    collection_path = output_dir / "collection.json"
    collection_data = derived_collection.to_dict()
    desired_item_rel = PurePosixPath("items") / f"{item_id}.json"
    child_href = f"./{desired_item_rel.as_posix()}"
    collection_links = collection_data.get("links", [])
    # Ensure root/parent
    collection_links = [
        l for l in collection_links if l.get("rel") not in {"root", "parent", "item"}
    ]
    collection_links.insert(
        0,
        {
            "rel": "root",
            "href": "./collection.json",
            "type": "application/json",
        },
    )
    collection_links.insert(
        1,
        {
            "rel": "parent",
            "href": "./collection.json",
            "type": "application/json",
        },
    )
    collection_links.append(
        {
            "rel": "item",
            "href": child_href,
            "type": "application/geo+json",
            "title": derived_title,
        }
    )
    collection_data["links"] = collection_links
    collection_path.write_text(json.dumps(collection_data, indent=2), encoding="utf-8")
    log(f"Saved collection to {collection_path}")

    parent_catalog_path: Optional[Path] = None
    if args.parent_catalog:
        parent_catalog_path = Path(args.parent_catalog).resolve()
    elif (output_dir.parent / "catalog.json").exists():
        parent_catalog_path = (output_dir.parent / "catalog.json").resolve()

    if parent_catalog_path and parent_catalog_path.exists():
        register_parent_catalog(
            parent_catalog_path,
            new_collection_href=output_dir / "collection.json",
            title=derived_title,
        )
        log(f"Registered new collection in {parent_catalog_path}")

    print(f"Subset GeoParquet written to: {output_file}")
    print(f"Derived collection saved under: {output_dir}")
    if parent_catalog_path and parent_catalog_path.exists():
        print(f"Registered collection in parent catalog: {parent_catalog_path}")


if __name__ == "__main__":
    main()
