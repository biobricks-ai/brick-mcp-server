# Script to extract first 50 rows of each file in a brick

import biobricks as bb
import pyarrow.parquet as pq
import sqlite3
from tqdm import tqdm
import json
import os

BRICK_INFO = {}


def extract_parquet(path):
    pd = pq.ParquetDataset(path)
    fragment = pd.fragments[0]
    batch = next(fragment.to_batches(batch_size=5))
    mcp_sample = batch.to_pylist()
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
        cursor.execute(f"SELECT * FROM `{table_name}` LIMIT 5;")
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


# def extract_hdt(path):
#     # Load the HDT file using rdflib_hdt
#     optimize_sparql()

#     store = HDTStore(path)
#     g = Graph(store)

#     # Get unique rdf:type objects (entity types) using SPARQL
#     q_types = g.query("""
#     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
#     SELECT DISTINCT ?type WHERE {
#         ?s rdf:type ?type .
#     }
#     """)
#     types = set()
#     for row in q_types:
#         types.add(str(row.type))

#     q_labels = g.query("""
#     PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
#     SELECT DISTINCT ?label WHERE {
#         ?s rdfs:label ?label .
#     }
#     """)
#     labels = set()
#     for row in q_labels:
#         labels.add(str(row.label))

#     # Get all unique predicate namespaces (ontologies/schemas)
#     q_preds = g.query("""
#     SELECT DISTINCT ?pred WHERE {
#         ?s ?p ?o .
#     }
#     """)
#     preds = set()
#     for row in q_preds:
#         preds.add(row.pred)

#     mcp_sample = {
#         "total_triples": len(store),
#         "num_subjects": store.nb_subjects,
#         "num_predicates": store.nb_predicates,
#         "num_objects": store.nb_objects,
#         "num_shared_subject_object": store.nb_shared,
#         "entity_types": types,
#         "labels": labels,
#         "ontologies": preds,
#     }

#     store.close()

#     return mcp_sample


def extract_context(brick: str):
    global BRICK_INFO

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
            continue
        else:
            raise ValueError(f"Unsupported file type: {path}")

        BRICK_INFO[asset] = {
            "brick_name": brick,
            "asset": asset,
            "format": fmt,
            "schema": schema,
            "preview_rows": sample,
        }


def read_list():
    global BRICK_INFO

    os.makedirs("tmp/other", exist_ok=True)
    f = open("list/bricks.txt", "r")
    lines = f.readlines()
    for line in tqdm(lines, desc="Processing bricks", position=0):
        BRICK_INFO = {}

        brick = line.strip()
        try:
            extract_context(brick)
        except Exception as e:
            print(f"Failed to extract {brick}: {e}")
            continue

        if len(BRICK_INFO):
            with open(f"tmp/other/{brick}.json", "w") as f_out:
                json.dump(BRICK_INFO, f_out, indent=2, default=str)


def main():
    read_list()


if __name__ == "__main__":
    main()
