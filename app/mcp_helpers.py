import os
import json


CATALOG = {}


def load_catalog(cache_dir="cache"):
    global CATALOG
    for fname in os.listdir(cache_dir):
        if fname.endswith(".json"):
            with open(os.path.join(cache_dir, fname)) as f:
                brick_data = json.load(f)
                brick_name = fname.replace(".json", "")
                CATALOG[brick_name] = brick_data


def list_bricks():
    return [
        {"name": name, "assets": list(brick.keys())} for name, brick in CATALOG.items()
    ]


def get_preview(brick_name, asset_name: str = None, dump: bool = False):
    if brick_name not in CATALOG:
        return {"error": "Brick not found"}
    brick_info = CATALOG[brick_name]
    limit = 50 if dump else 10

    if asset_name:
        asset = brick_info.get(asset_name)
        if not asset:
            return {"error": "Asset not found"}
        return {
            "brick_name": brick_name,
            "asset": asset_name,
            "format": asset.get("format"),
            "schema": asset.get("schema"),
            "preview_rows": asset.get("preview_rows"),
        }

    return {
        "brick_name": brick_name,
        "assets": [
            {
                "asset": name,
                "format": info.get("format"),
                "schema": info.get("schema"),
                "preview_rows": (info.get("preview_rows") or [])[:limit],
            }
            for name, info in brick_info.items()
        ],
    }


def search_bricks(query):
    results = []
    for brick_name, assets in CATALOG.items():
        text_blob = json.dumps(assets).lower()
        if query.lower() in text_blob:
            results.append(brick_name)
    return results
