import json
import os
import sqlite3

import biobricks as bb
import pyarrow.parquet as pq
import pyarrow.compute as pc
from tqdm import tqdm


def extract_parquet(path):
    pd = pq.ParquetDataset(path)
    fragment = pd.fragments[0]

    # Read a batch
    batch = next(fragment.to_batches(batch_size=1000))

    # Build mask: keep rows where not all columns are null
    mask = None
    for col in batch.schema.names:
        col_is_valid = pc.invert(pc.is_null(batch[col]))
        mask = col_is_valid if mask is None else pc.or_(mask, col_is_valid)

    filtered_batch = batch.filter(mask)
    mcp_sample = filtered_batch.to_pylist()[0]
    mcp_schema = {field.name: str(field.type) for field in fragment.physical_schema}

    return mcp_schema, mcp_sample


def extract_sqlite(path):
    conn = sqlite3.connect(path)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    mcp_sample = {}
    mcp_schema = {}

    for (table_name,) in tables:
        # Filter out rows where all columns are NULL
        # cursor.execute(f"SELECT * FROM `{table_name}` WHERE NOT ({' AND '.join([f'`{col[1]}` IS NULL' for col in cursor.execute(f'PRAGMA table_info(`{table_name}`);').fetchall()])}) LIMIT 50;")
        cursor.execute(
            f"SELECT * FROM `{table_name}` WHERE NOT ({' AND '.join([f'`{col[1]}` IS NULL' for col in cursor.execute(f'PRAGMA table_info(`{table_name}`);').fetchall()])}) LIMIT 1;"
        )
        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]
        # Convert each row to a dict
        dict_rows = [dict(zip(col_names, row)) for row in rows]
        mcp_sample[table_name] = dict_rows

        # Get schema for this table
        cursor.execute(f"PRAGMA table_info(`{table_name}`);")
        table_schema = {row[1]: row[2] for row in cursor.fetchall()}
        mcp_schema[table_name] = table_schema

    conn.close()

    return mcp_schema, mcp_sample


def extract_context(brick: str):
    brick_assets = vars(bb.assets(brick))

    pbar = tqdm(
        brick_assets.items(),
        leave=False,
        position=1,
    )

    brick_metadata = []

    for asset, path in pbar:
        pbar.set_description(f"Processing {asset}")

        if asset.endswith("parquet"):
            fmt = "parquet"
            schema, sample = extract_parquet(path)
        elif asset.endswith("sqlite"):
            fmt = "sqlite"
            schema, sample = extract_sqlite(path)
        elif asset.endswith("hdt"):
            continue
        else:
            raise ValueError(f"Unsupported file type: {path}")

        asset_metadata = {
            "brick_name": brick,
            "asset_name": asset,
            "file_format": fmt,
            "schema": schema,
            "sample": sample,
        }

        brick_metadata.append(asset_metadata)
        with open(f"metadata/json/{brick}__{asset}.json", "w") as f_out:
            json.dump(asset_metadata, f_out, separators=(",", ":"), default=str)

    return brick_metadata


def main():
    os.makedirs("metadata/json", exist_ok=True)
    os.makedirs("metadata/jsonl", exist_ok=True)

    with open("list/bricks.txt", "r") as f:
        bricks = [line.strip() for line in f.readlines()]
    pbar = tqdm(bricks, position=0)

    for brick in pbar:
        pbar.set_description(f"Processing {brick}")

        if not len(vars(bb.assets(brick))):
            continue

        try:
            metadata = extract_context(brick)
            getattr()
            with open(f"metadata/jsonl/{brick}.jsonl", "w") as f_out:
                for item in metadata:
                    f_out.write(
                        json.dumps(item, separators=(",", ":"), default=str) + "\n"
                    )
        except Exception as e:
            print(f"Failed to extract {brick}: {e}")
            continue


if __name__ == "__main__":
    main()
