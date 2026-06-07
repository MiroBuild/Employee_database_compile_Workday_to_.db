"""
run.py

Entry point for the employee database pipeline.
Run this script each time new Workday exports land in data/raw/.

Usage
-----
    python run.py                        # uses default paths
    python run.py --raw data/raw         # explicit raw folder
    python run.py --db db/employees.db   # explicit DB path
    python run.py --dry-run              # ingest + transform only, no DB writes

What it does
------------
1. Scans data/raw/ for .xlsx files
2. Renames files to short consistent names (handles Windows path length limits)
3. Ingests each file into a clean DataFrame (pipeline/ingest.py)
4. Groups DataFrames by target table and runs transforms (pipeline/transform.py)
5. Writes to the SQLite database (pipeline/load.py)
6. Prints a summary of what was inserted, updated, and skipped
7. Moves processed files to data/processed/ with a date prefix

Files that cannot be matched to a known pattern are skipped with a warning
rather than aborting the entire run.
"""

import argparse
import os
import shutil
import sys
from collections import defaultdict
from datetime import datetime

# Resolve the project root as an absolute path so the import works regardless
# of which directory Python is launched from (important on Windows).
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pipeline import ingest, transform, load


# ---------------------------------------------------------------------------
# PATHS  (relative to the project root; override via CLI args)
# ---------------------------------------------------------------------------
DEFAULT_RAW_DIR  = os.path.join(os.path.expanduser('~'), 'Downloads')
DEFAULT_PROC_DIR = os.path.join(PROJECT_ROOT, 'data', 'processed')
DEFAULT_DB_PATH  = os.path.join(PROJECT_ROOT, 'db', 'employees.db')
DEFAULT_SCHEMA   = os.path.join(PROJECT_ROOT, 'sql', 'schema.sql')


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def _collect_files(raw_dir: str) -> list[str]:
    """Returns full paths of all .xlsx files in raw_dir, sorted by name."""
    if not os.path.isdir(raw_dir):
        print(f"[ERROR] Raw folder not found: {raw_dir}")
        sys.exit(1)
    return sorted(
        os.path.join(raw_dir, f)
        for f in os.listdir(raw_dir)
        if f.lower().endswith('.xlsx') and not f.startswith('~')
    )


def _archive_file(filepath: str, proc_dir: str) -> None:
    """
    Moves a processed file to data/processed/ prefixed with today's date.
    Uses the \\?\\ prefix on Windows to bypass the MAX_PATH limit.
    """
    os.makedirs(proc_dir, exist_ok=True)
    basename  = os.path.basename(filepath)
    today     = datetime.today().strftime('%Y-%m-%d')
    dest_name = f"{today}_{basename}"
    dest_path = os.path.join(proc_dir, dest_name)

    counter = 1
    while os.path.exists(dest_path):
        dest_path = os.path.join(proc_dir, f"{today}_{counter}_{basename}")
        counter += 1

    try:
        shutil.move(_win_path(filepath), _win_path(dest_path))
    except (OSError, shutil.Error) as e:
        print(f"    [WARNING] Could not archive '{basename}': {e}")
        print(f"    File remains in data/raw/")


def _print_summary(results: dict) -> None:
    """Prints a formatted run summary to stdout."""
    print("\n" + "=" * 60)
    print("  Pipeline run complete")
    print("=" * 60)

    skipped  = results.get('skipped_files', [])
    ingested = results.get('ingested_files', [])
    db_counts = results.get('db_counts', {})

    if skipped:
        print(f"\n  Skipped ({len(skipped)} unrecognised files):")
        for f in skipped:
            print(f"    • {os.path.basename(f)}")

    if ingested:
        print(f"\n  Ingested ({len(ingested)} files):")
        for f in ingested:
            print(f"    • {os.path.basename(f)}")

    if db_counts:
        print(f"\n  Database changes:")
        for table, counts in db_counts.items():
            ins = counts.get('inserted', 0)
            upd = counts.get('updated', 0)
            skp = counts.get('skipped', 0)
            print(f"    {table:<35} +{ins} inserted  ~{upd} updated  /{skp} skipped")

    print()


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def run(raw_dir: str, db_path: str, schema_path: str,
        proc_dir: str, dry_run: bool = False) -> None:

    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting pipeline run")
    print(f"  Raw folder : {raw_dir}")
    if dry_run:
        print("  Mode       : DRY RUN (no DB writes)")
    else:
        print(f"  Database   : {db_path}")

    # --- 1. Collect files ---
    all_files = _collect_files(raw_dir)
    if not all_files:
        print("\n  No .xlsx files found in raw folder. Nothing to do.")
        return

    print(f"\n  Found {len(all_files)} file(s):")
    for f in all_files:
        print(f"    {os.path.basename(f)}")

    # --- 2. Ingest ---
    print("\n  Ingesting...")
    ingested_by_table: dict[str, list] = defaultdict(list)
    source_filenames:  dict[str, list] = defaultdict(list)
    ingested_files = []
    skipped_files  = []

    for filepath in all_files:
        fname = os.path.basename(filepath)
        try:
            table, df = ingest.ingest(filepath)
            ingested_by_table[table].append(df)
            source_filenames[table].append(fname)
            ingested_files.append(filepath)
            print(f"    OK  {fname}  ->  {table}  ({len(df)} rows)")
        except ValueError as e:
            skipped_files.append(filepath)
            print(f"    --  {fname}  (skipped: {e})")
        except Exception as e:
            skipped_files.append(filepath)
            print(f"    !!  {fname}  (error: {e})")

    if not ingested_files:
        print("\n  No files could be ingested. Nothing to write.")
        return

    # --- 3. Transform ---
    print("\n  Transforming...")

    if dry_run:
        existing_ids = []
        if os.path.exists(db_path):
            conn = load.get_connection(db_path)
            existing_ids = load.get_existing_employee_ids(conn)
            conn.close()
    else:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        conn = load.get_connection(db_path)
        load.initialise_db(conn, schema_path)
        existing_ids = load.get_existing_employee_ids(conn)

    transformed = transform.transform_all(dict(ingested_by_table), existing_ids)

    # Report transform diagnostics
    import pandas as pd
    emp_df = transformed.get('employees')
    if isinstance(emp_df, pd.DataFrame) and len(emp_df) > 0:
        print(f"    employees merged       : {len(emp_df)} rows")

    dep_df = transformed.get('employee_departures')
    if isinstance(dep_df, pd.DataFrame) and len(dep_df) > 0:
        print(f"    departures detected    : {len(dep_df)} employees marked Terminated")

    term_frames = transformed.get('terminations', [])
    if isinstance(term_frames, list) and len(term_frames) > 0:
        roster = next((df for df in term_frames if 'workday_id' in df.columns), None)
        if roster is not None and 'match_status' in roster.columns:
            matched   = (roster['match_status'] == 'matched').sum()
            ambiguous = (roster['match_status'] == 'ambiguous').sum()
            unmatched = (roster['match_status'] == 'unmatched').sum()
            print(f"    term roster resolution : {matched} matched, "
                  f"{ambiguous} ambiguous, {unmatched} unmatched")

    if dry_run:
        print("\n  DRY RUN — no data written to database.")
        _print_summary({'skipped_files': skipped_files, 'ingested_files': ingested_files})
        return

    # --- 4. Load ---
    print("\n  Loading...")
    try:
        load.load_all(conn, transformed, dict(source_filenames))
        print("    Done.")
    except Exception as e:
        print(f"\n  [ERROR] Load failed: {e}")
        conn.close()
        sys.exit(1)

    # Collect row counts from pipeline_log
    db_counts = {}
    log_rows = conn.execute("""
        SELECT target_table, SUM(rows_inserted), SUM(rows_updated), SUM(rows_skipped)
        FROM   pipeline_log
        WHERE  run_timestamp >= datetime('now', '-1 minute')
        GROUP  BY target_table
    """).fetchall()
    for row in log_rows:
        db_counts[row[0]] = {
            'inserted': row[1] or 0,
            'updated':  row[2] or 0,
            'skipped':  row[3] or 0,
        }

    conn.close()

    # --- 5. Archive processed files ---
    print("\n  Archiving processed files...")
    for filepath in ingested_files:
        _archive_file(filepath, proc_dir)
        print(f"    -> {os.path.basename(filepath)}")

    _print_summary({
        'skipped_files':  skipped_files,
        'ingested_files': ingested_files,
        'db_counts':      db_counts,
    })


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Run the Workday -> SQLite employee database pipeline.'
    )
    parser.add_argument('--raw',       default=DEFAULT_RAW_DIR)
    parser.add_argument('--db',        default=DEFAULT_DB_PATH)
    parser.add_argument('--schema',    default=DEFAULT_SCHEMA)
    parser.add_argument('--processed', default=DEFAULT_PROC_DIR)
    parser.add_argument('--dry-run',   action='store_true')

    args = parser.parse_args()

    run(
        raw_dir     = args.raw,
        db_path     = args.db,
        schema_path = args.schema,
        proc_dir    = args.processed,
        dry_run     = args.dry_run,
    )
