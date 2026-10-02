"""
The validation gate: the migration is not considered "done" until this passes.

Compares the source database against the lake on two things:
  1. Row count       -- did every row make it across?
  2. Checksum         -- a hash of sorted transaction_ids + a sum of amounts, so a
                         row that made it across with silently corrupted data would
                         still be caught, not just a row that went missing entirely.

Exits non-zero (and prints FAIL) on any mismatch, so this can be wired into a CI
step or a migration runbook as a hard gate, not an informal spot-check.
"""
import sqlite3
import os
import sys
import glob
import hashlib
import pandas as pd

HERE = os.path.dirname(__file__)
DB_PATH = os.path.join(HERE, "..", "source_db", "transactions.db")
LAKE_ROOT = os.path.join(HERE, "..", "lake", "transactions")


def _source_stats():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT transaction_id, amount FROM transactions", conn)
    conn.close()
    return _stats_from_df(df)


def _lake_stats():
    files = glob.glob(os.path.join(LAKE_ROOT, "**", "*.parquet"), recursive=True)
    if not files:
        return {"row_count": 0, "checksum": None}
    df = pd.concat([pd.read_parquet(f, columns=["transaction_id", "amount"]) for f in files], ignore_index=True)
    return _stats_from_df(df)


def _stats_from_df(df):
    row_count = len(df)
    ids_sorted = ",".join(str(i) for i in sorted(df["transaction_id"].tolist()))
    amount_sum = round(float(df["amount"].sum()), 2)
    checksum = hashlib.sha256(f"{ids_sorted}|{amount_sum}".encode()).hexdigest()
    return {"row_count": row_count, "checksum": checksum, "amount_sum": amount_sum}


def reconcile():
    src = _source_stats()
    lake = _lake_stats()

    print("Validation Gate")
    print("-" * 50)
    print(f"  source row count : {src['row_count']:,}")
    print(f"  lake   row count : {lake['row_count']:,}")
    print(f"  source amount sum: {src['amount_sum']:,}")
    print(f"  lake   amount sum: {lake.get('amount_sum', 'N/A')}")

    row_count_ok = src["row_count"] == lake["row_count"]
    checksum_ok = src["checksum"] == lake["checksum"]

    if row_count_ok and checksum_ok:
        print("-" * 50)
        print("PASS -- lake matches source exactly. Safe to promote to curated tier.")
        return 0
    else:
        print("-" * 50)
        print("FAIL -- lake does not match source. Do NOT promote. Investigate before re-running.")
        if not row_count_ok:
            print(f"  -> row count mismatch: missing {src['row_count'] - lake['row_count']:,} rows")
        if not checksum_ok and row_count_ok:
            print("  -> row counts match but checksum differs: data may be corrupted or duplicated")
        return 1


if __name__ == "__main__":
    sys.exit(reconcile())
