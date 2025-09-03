# Script to extract first 50 rows of each file in a brick

import biobricks as bb
import pyarrow.parquet as pq
import sqlite3
from rdflib_hdt import HDTStore
from rdflib import Graph
from tqdm import tqdm
import json
import os


MCP_CATALOG = {}


def extract_parquet(path):
    pd = pq.ParquetDataset(path)
    fragment = pd.fragments[0]
    batch = next(fragment.to_batches(batch_size=10))
    mcp_sample = batch.to_pylist()
    mcp_schema = {field.name: str(field.type) for field in fragment.physical_schema}

    return mcp_schema, mcp_sample


def extract_sqlite(path):
    conn = sqlite3.connect(path)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    mcp_sample = {}
    for (table_name,) in tables:
        cursor.execute(f"SELECT * FROM `{table_name}` LIMIT 10;")
        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]
        # Convert each row to a dict
        dict_rows = [dict(zip(col_names, row)) for row in rows]
        mcp_sample[table_name] = dict_rows

    cursor.execute(f"PRAGMA table_info(`{table_name}`);")
    mcp_schema = {row[1]: row[2] for row in cursor.fetchall()}

    conn.close()

    return mcp_schema, mcp_sample


def extract_hdt(path):
    # Load the HDT file using rdflib_hdt
    store = HDTStore(path)
    g = Graph(store=store)

    # Extract all unique (subject, predicate, object) triples
    triples_set = set()
    for s, p, o in g.triples((None, None, None)):
        triples_set.add((str(s), str(p), str(o)))

    # Unpack into separate sets for subjects, predicates, objects
    subjects = set()
    predicates = set()
    objects = set()
    for s, p, o in triples_set:
        subjects.add(s)
        predicates.add(p)
        objects.add(o)

    mcp_sample = {
        "subjects": list(subjects),
        "predicates": list(predicates),
        "objects": list(objects),
    }
    return mcp_sample


def extract_context(brick: str):
    brick_assets = vars(bb.assets(brick))
    for asset, path in tqdm(
        brick_assets.items(),
        desc=f"Processing assets for {brick}",
        leave=False,
        position=1,
    ):
        if asset.endswith("parquet"):
            fmt = "parquet"
            schema, sample = extract_parquet(path)
        elif asset.endswith("sqlite"):
            fmt = "sqlite"
            schema, sample = extract_sqlite(path)
        elif asset.endswith("hdt"):
            fmt = "hdt"
            schema = None
            sample = extract_hdt(path)
        else:
            raise ValueError(f"Unsupported file type: {path}")

        MCP_CATALOG[brick][asset] = {
            "brick_name": brick,
            "asset": asset,
            "format": fmt,
            "schema": schema,
            "preview_rows": sample,
        }


def read_list():
    os.makedirs("cache", exist_ok=True)
    f = open("list/bricks.txt", "r")
    lines = f.readlines()
    for line in tqdm(lines, desc="Processing bricks", position=0):
        brick = line.strip()
        MCP_CATALOG[brick] = {}
        try:
            extract_context(brick)
        except Exception as e:
            print(f"Failed to extract {brick}: {e}")
            continue
        if len(MCP_CATALOG[brick]):
            with open(f"cache/{brick}.json", "w") as f_out:
                json.dump(MCP_CATALOG[brick], f_out, indent=2, default=str)


def main():
    read_list()


if __name__ == "__main__":
    main()
