"""
End-to-end test: build the source DB, run the full load, run an incremental sync,
and assert the validation gate passes after each step -- and that re-running sync
with nothing new is a safe no-op. This is the same sequence a human would run by
hand; it's just asserted automatically here.
"""
import os
import sys
import subprocess

ROOT = os.path.dirname(os.path.dirname(__file__))


def run(*args):
    result = subprocess.run([sys.executable, *args], cwd=ROOT, capture_output=True, text=True)
    return result


def test_full_pipeline_end_to_end():
    assert run("source_db/generate_source.py").returncode == 0
    assert run("etl/migrate_to_lake.py", "--workers", "4").returncode == 0

    reconcile_1 = run("validation/reconcile.py")
    assert reconcile_1.returncode == 0, reconcile_1.stdout

    assert run("source_db/simulate_new_rows.py").returncode == 0
    sync_1 = run("etl/incremental_sync.py")
    assert sync_1.returncode == 0
    assert "No new rows" not in sync_1.stdout

    reconcile_2 = run("validation/reconcile.py")
    assert reconcile_2.returncode == 0, reconcile_2.stdout

    # Re-running sync with nothing new should be a safe no-op, not a duplicate load
    sync_2 = run("etl/incremental_sync.py")
    assert "No new rows" in sync_2.stdout

    reconcile_3 = run("validation/reconcile.py")
    assert reconcile_3.returncode == 0, reconcile_3.stdout
