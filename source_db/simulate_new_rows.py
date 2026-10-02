"""
Adds a small batch of new rows to the source DB, as if new transactions had just
happened live -- lets incremental_sync.py demonstrate pulling only what's new
instead of a full re-export.
"""
import sqlite3
import random
import os
from datetime import datetime, timedelta
from generate_source import DB_PATH, MERCHANTS, CURRENCIES, STATUSES

NUM_NEW_ROWS = 500


def simulate(n=NUM_NEW_ROWS):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    max_id = cur.execute("SELECT MAX(transaction_id) FROM transactions").fetchone()[0]
    now = datetime.now()
    rows = []
    for i in range(1, n + 1):
        ts = now - timedelta(minutes=random.randint(0, 60))
        rows.append((
            max_id + i,
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
    print(f"Simulated {n} new live transactions (ids {max_id+1}-{max_id+n}).")


if __name__ == "__main__":
    simulate()
