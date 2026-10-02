"""
Sqoop-style initial bulk load, at toy scale.

Real Sqoop splits a table by primary-key ranges and runs N parallel mappers, each
pulling its own slice straight from the relational database into HDFS. This script
mirrors that shape: it splits transaction_id into N ranges, pulls each range with
its own worker thread, and writes each worker's output as its own partitioned
Parquet file underneath a directory layout that matches a Hive external table
partitioned by (year, month) -- exactly the "HDFS Raw Zone, partitioned by ingest
date" box in the architecture diagram.

Toy scale: 50k rows / a handful of workers, running against a local SQLite file
and writing to local disk instead of a real cluster. See README.md for how each
piece maps to the real Sqoop -> HDFS -> Hive stack at petabyte scale.
"""
import sqlite3
import os
import shutil
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from checkpoint import save_checkpoint

HERE = os.path.dirname(__file__)
DB_PATH = os.path.join(HERE, "..", "source_db", "transactions.db")
LAKE_ROOT = os.path.join(HERE, "..", "lake", "transactions")


def _fetch_range(lo, hi):
    """One 'mapper': pull one primary-key range from the source DB."""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        "SELECT * FROM transactions WHERE transaction_id >= ? AND transaction_id < ?",
        conn, params=(lo, hi),
    )
    conn.close()
    return df


def migrate(num_workers=4):
    if os.path.exists(LAKE_ROOT):
        shutil.rmtree(LAKE_ROOT)

    conn = sqlite3.connect(DB_PATH)
    max_id = conn.execute("SELECT MAX(transaction_id) FROM transactions").fetchone()[0]
    conn.close()

    chunk = (max_id // num_workers) + 1
    ranges = [(i * chunk + 1, (i + 1) * chunk + 1) for i in range(num_workers)]

    total_written = 0
    with ThreadPoolExecutor(max_workers=num_workers) as pool:
        futures = {pool.submit(_fetch_range, lo, hi): (lo, hi) for lo, hi in ranges}
        for fut in as_completed(futures):
            lo, hi = futures[fut]
            df = fut.result()
            if df.empty:
                continue
            df["transaction_ts"] = pd.to_datetime(df["transaction_ts"])
            df["year"] = df["transaction_ts"].dt.year
            df["month"] = df["transaction_ts"].dt.month

            # Partition by (year, month) -- mirrors "partitioned by transaction date"
            for (yr, mo), part_df in df.groupby(["year", "month"]):
                part_dir = os.path.join(LAKE_ROOT, f"year={yr}", f"month={mo:02d}")
                os.makedirs(part_dir, exist_ok=True)
                out_path = os.path.join(part_dir, f"part-worker{lo}-{hi}.parquet")
                part_df.drop(columns=["year", "month"]).to_parquet(out_path, index=False)
                total_written += len(part_df)
            print(f"  worker range [{lo},{hi}) -> {len(df):,} rows written")

    save_checkpoint(max_id)
    print(f"Migration complete: {total_written:,} rows written to {LAKE_ROOT}")
    print(f"Checkpoint set to {max_id} -- incremental_sync.py will start from here.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=4)
    args = ap.parse_args()
    migrate(num_workers=args.workers)
