import os
import sys
import glob
import pandas as pd
import sqlite3
from tqdm import tqdm
import duckdb


def main():
    curdir = os.getcwd()
    dst = os.path.join(curdir, "mcp")

    # 1. Parquet to CSV
    parquet_files = glob.glob(os.path.join(dst, "**", "*.parquet"), recursive=True)
    for parquet_file in tqdm(parquet_files, desc="Converting parquet files"):
        # Get the brick name (parent directory of the file)
        brick_name = os.path.basename(os.path.dirname(parquet_file))
        base_name = os.path.basename(parquet_file)
        csv_file = f"{brick_name}__{base_name}.csv"
        try:
            with duckdb.connect() as con:
                con.execute(
                    f"COPY (SELECT * FROM '{parquet_file}') TO '{csv_file}' (HEADER, FORMAT 'csv')"
                )
                os.remove(parquet_file)
        except Exception as e:
            print(f"Failed to convert {parquet_file}: {e}", file=sys.stderr)

    # 2. SQLite to CSV
    sqlite_exts = ("*.sqlite", "*.db")
    for ext in tqdm(sqlite_exts, desc="Converting sqlite files"):
        for sqlite_file in glob.glob(os.path.join(dst, "**", ext), recursive=True):
            try:
                brick_name = os.path.basename(os.path.dirname(sqlite_file))
                base_name = os.path.basename(sqlite_file)
                with sqlite3.connect(sqlite_file) as conn:
                    cursor = conn.cursor()
                    # Get table names
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                    tables = [row[0] for row in cursor.fetchall()]
                    for table in tables:
                        csv_file = f"{brick_name}__{base_name}__{table}.csv"
                        try:
                            df = pd.read_sql_query(f'SELECT * FROM "{table}"', conn)
                            df.to_csv(csv_file, index=False)
                        except Exception as e:
                            print(
                                f"Failed to export table {table} from {sqlite_file}: {e}",
                                file=sys.stderr,
                            )
                os.remove(sqlite_file)
            except Exception as e:
                print(f"Failed to convert {sqlite_file}: {e}", file=sys.stderr)

    # 3. HDT to TXT
    hdt_files = glob.glob(os.path.join(dst, "**", "*.hdt"), recursive=True)
    for hdt_file in tqdm(hdt_files, desc="Converting hdt files"):
        brick_name = os.path.basename(os.path.dirname(hdt_file))
        base_name = os.path.basename(hdt_file)
        txt_file = f"{brick_name}__{base_name}.txt"
        try:
            with open(hdt_file, "rb") as src, open(txt_file, "wb") as dstf:
                dstf.write(src.read())
            os.remove(hdt_file)
        except Exception as e:
            print(f"Failed to copy {hdt_file}: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
