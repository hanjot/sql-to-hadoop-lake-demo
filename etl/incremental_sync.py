"""
CDC-style incremental sync, at toy scale.

Real CDC (or Kafka Connect against a database's change log) streams new/changed
rows continuously so the lake never falls far behind the source, without repeating
the full historical export. This script mirrors that shape with a checkpoint file:
it remembers the highest transaction_id it has already synced, pulls only rows
newer than that, and appends them into the correct existing partition (or creates
a new one, e.g. for a new month).

Usage:
    python etl/migrate_to_lake.py         # initial full load
    python source_db/simulate_new_rows.py # (optional) add fresh rows to the source
    python etl/incremental_sync.py        # pull just what's new
"""
import sqlite3
import os
import pandas as pd
from checkpoint import load_checkpoint, save_checkpoint

HERE = os.path.dirname(__file__)
DB_PATH = os.path.join(HERE, "..", "source_db", "transactions.db")
LAKE_ROOT = os.path.join(HERE, "..", "lake", "transactions")


def sync():
    last_synced_id = load_checkpoint()
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        "SELECT * FROM transactions WHERE transaction_id > ? ORDER BY transaction_id",
        conn, params=(last_synced_id,),
    )
    conn.close()

    if df.empty:
        print(f"No new rows since transaction_id={last_synced_id}. Lake is current.")
        return

    df["transaction_ts"] = pd.to_datetime(df["transaction_ts"])
    df["year"] = df["transaction_ts"].dt.year
    df["month"] = df["transaction_ts"].dt.month

    for (yr, mo), part_df in df.groupby(["year", "month"]):
        part_dir = os.path.join(LAKE_ROOT, f"year={yr}", f"month={mo:02d}")
        os.makedirs(part_dir, exist_ok=True)
        out_path = os.path.join(part_dir, f"part-cdc-{part_df['transaction_id'].min()}.parquet")
        part_df.drop(columns=["year", "month"]).to_parquet(out_path, index=False)
        print(f"  synced {len(part_df):,} new rows -> year={yr}/month={mo:02d}")

    new_max = int(df["transaction_id"].max())
    save_checkpoint(new_max)
    print(f"Incremental sync complete. {len(df):,} new rows. Checkpoint advanced to {new_max}.")


if __name__ == "__main__":
    sync()
