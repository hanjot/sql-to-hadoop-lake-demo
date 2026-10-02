"""
Creates a small synthetic 'OLTP' source database (transactions.db) that stands in
for the relational database in the architecture proposal. This is a TOY-SCALE demo
(tens of thousands of rows, not petabytes) meant to prove the migration pipeline's
logic end-to-end -- see README.md for how this maps to the real, petabyte-scale design.
"""
import sqlite3
import random
import os
from datetime import datetime, timedelta

random.seed(42)

DB_PATH = os.path.join(os.path.dirname(__file__), "transactions.db")
NUM_ROWS = 50_000
START_DATE = datetime(2024, 1, 1)
MERCHANTS = [f"M{str(i).zfill(4)}" for i in range(1, 201)]
CURRENCIES = ["USD", "EUR", "GBP", "CAD"]
STATUSES = ["APPROVED", "APPROVED", "APPROVED", "APPROVED", "DECLINED"]


def build_source_db():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE transactions (
            transaction_id INTEGER PRIMARY KEY,
            account_id     TEXT NOT NULL,
            merchant_id    TEXT NOT NULL,
            amount         REAL NOT NULL,
            currency       TEXT NOT NULL,
            transaction_ts TEXT NOT NULL,
            status         TEXT NOT NULL
        )
    """)
    rows = []
    for i in range(1, NUM_ROWS + 1):
        ts = START_DATE + timedelta(minutes=random.randint(0, 60 * 24 * 300))
        rows.append((
            i,
            f"A{random.randint(1, 5000):06d}",
            random.choice(MERCHANTS),
            round(random.uniform(1.0, 2500.0), 2),
            random.choice(CURRENCIES),
            ts.strftime("%Y-%m-%d %H:%M:%S"),
            random.choice(STATUSES),
        ))
    cur.executemany("INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()
    print(f"Built source DB at {DB_PATH} with {NUM_ROWS:,} rows.")


if __name__ == "__main__":
    build_source_db()
