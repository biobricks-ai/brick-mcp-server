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
    pf = pq.ParquetFile(path)
    batch = next(pf.iter_batches(10))
    mcp_sample = batch.to_pylist()
    mcp_schema = {field.name: str(field.logical_type) for field in pf.schema}

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
    brick_assets = bb.assets(brick)
    brick_assets_dict = vars(brick_assets)

    for asset, path in brick_assets_dict.items():
        if asset.endswith("parquet"):
            schema, sample = extract_parquet(path)
            MCP_CATALOG[brick][asset] = (schema, sample)

        elif asset.endswith("sqlite"):
            schema, sample = extract_sqlite(path)
            MCP_CATALOG[brick][asset] = (schema, sample)

        elif asset.endswith("hdt"):
            sample = extract_hdt(path)
            MCP_CATALOG[brick][asset] = sample


def read_list():
    os.makedirs("cache", exist_ok=True)
    with open("list/bricks.txt", "r") as f:
        for line in tqdm(f, desc="Processing bricks"):
            brick = line.strip()

            MCP_CATALOG[brick] = {}
            extract_context(brick)

            with open(f"cache/{brick}.json", "w") as f:
                json.dump(MCP_CATALOG[brick], f, indent=2)


def main():
    read_list()


if __name__ == "__main__":
    main()
