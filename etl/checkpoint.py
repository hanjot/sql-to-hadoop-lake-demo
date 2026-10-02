"""Shared checkpoint helper -- both the initial full load and the incremental sync
use this so incremental_sync.py always knows exactly where the last load (full or
incremental) left off, instead of re-copying rows that already made it across."""
import os
import json

HERE = os.path.dirname(__file__)
CHECKPOINT_PATH = os.path.join(HERE, "..", "lake", ".sync_checkpoint.json")


def load_checkpoint():
    if os.path.exists(CHECKPOINT_PATH):
        with open(CHECKPOINT_PATH) as f:
            return json.load(f).get("last_synced_id", 0)
    return 0


def save_checkpoint(last_id):
    os.makedirs(os.path.dirname(CHECKPOINT_PATH), exist_ok=True)
    with open(CHECKPOINT_PATH, "w") as f:
        json.dump({"last_synced_id": last_id}, f)
