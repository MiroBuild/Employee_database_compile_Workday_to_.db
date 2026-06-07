"""
pipeline/load.py

Responsible for writing transformed DataFrames to the SQLite database.

Each table has a specific load strategy:

  employees         UPSERT — insert new employees, update existing ones in full.
                    Departed employees are handled separately via a partial update
                    that only touches worker_status and report_date.

  terminations      ENRICHMENT UPSERT — on conflict(employee_id, termination_date),
                    fill NULL columns from the incoming row rather than overwriting
                    existing data. This lets the weekly SCD file and monthly report
                    complement each other without creating duplicates.

  new_hires         IGNORE ON CONFLICT — append only; skip exact duplicates.
  manager_flags     IGNORE ON CONFLICT — append only; skip exact duplicates.
  feedback          IGNORE ON CONFLICT — append only; skip exact duplicates.
  staff_dev_scholarship   IGNORE ON CONFLICT
  family_scholarship      IGNORE ON CONFLICT
  recruiting_pipeline     APPEND — no deduplication (same candidate can appear
                           multiple times legitimately).
  agg_*             IGNORE ON CONFLICT — append snapshots; skip if already loaded.
  pipeline_log      APPEND always — audit trail, never deduplicated.

All writes are wrapped in a single transaction per pipeline run. If any table
fails, the entire run is rolled back so the DB is never left in a partial state.
"""

import sqlite3
import os
import pandas as pd
from datetime import datetime


# ---------------------------------------------------------------------------
# CONNECTION
# ---------------------------------------------------------------------------

def get_connection(db_path: str) -> sqlite3.Connection:
    """
    Opens a SQLite connection with foreign key enforcement enabled.
    Foreign keys are OFF by default in SQLite — this pragma activates them.
    """
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")   # safer for concurrent reads
    return conn


def initialise_db(conn: sqlite3.Connection, schema_path: str) -> None:
    """
    Runs schema.sql against the connection. Uses CREATE TABLE IF NOT EXISTS
    so it is safe to call on an existing database — it will only create
    tables that don't exist yet.
    """
    with open(schema_path, 'r') as f:
        sql = f.read()
    conn.executescript(sql)
    conn.commit()


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def _df_to_records(df: pd.DataFrame) -> list[dict]:
    """
    Converts a DataFrame to a list of dicts, replacing pandas NA/NaN with
    Python None so SQLite receives proper NULLs rather than the string 'nan'.
    """
    return [
        {k: (None if pd.isna(v) else v) for k, v in row.items()}
        for row in df.to_dict(orient='records')
    ]


def get_existing_employee_ids(conn: sqlite3.Connection) -> list[str]:
    """
    Returns all employee_ids currently in the employees table.
    Called by run.py before transform so detect_terminations knows
    which IDs were previously active.
    """
    cursor = conn.execute("SELECT employee_id FROM employees")
    return [row[0] for row in cursor.fetchall()]


# ---------------------------------------------------------------------------
# LOAD STRATEGIES
# ---------------------------------------------------------------------------

def load_employees(
    conn: sqlite3.Connection,
    df: pd.DataFrame,
) -> tuple[int, int]:
    """
    Full upsert: insert new employees, overwrite all columns for returning ones.
    Returns (rows_inserted, rows_updated).
    """
    if df.empty:
        return 0, 0

    cols = list(df.columns)
    placeholders = ', '.join([f':{c}' for c in cols])
    col_list     = ', '.join(cols)

    # On conflict, update every column except the PK itself
    update_set = ',\n        '.join(
        f"{c} = excluded.{c}"
        for c in cols if c != 'employee_id'
    )

    sql = f"""
        INSERT INTO employees ({col_list})
        VALUES ({placeholders})
        ON CONFLICT(employee_id) DO UPDATE SET
        {update_set}
    """

    records = _df_to_records(df)
    before  = conn.execute("SELECT COUNT(*) FROM employees").fetchone()[0]
    conn.executemany(sql, records)
    after   = conn.execute("SELECT COUNT(*) FROM employees").fetchone()[0]

    inserted = after - before
    updated  = len(records) - inserted
    return inserted, updated


def load_employee_departures(
    conn: sqlite3.Connection,
    df: pd.DataFrame,
) -> int:
    """
    Partial update: marks departed employees as Terminated.
    Only touches worker_status and report_date — all other columns unchanged.
    Returns number of rows updated.
    """
    if df.empty:
        return 0

    sql = """
        UPDATE employees
        SET    worker_status = :worker_status,
               report_date   = :report_date
        WHERE  employee_id   = :employee_id
    """
    records = _df_to_records(df)
    conn.executemany(sql, records)
    return len(records)


def _stub_missing_employees(
    conn: sqlite3.Connection,
    df: pd.DataFrame,
    id_col: str = 'employee_id',
) -> int:
    """
    Ensures every employee_id referenced in df exists in the employees table.
    Called before any insert that has a FK to employees.

    Any ID not found in employees gets a minimal stub row. The stub carries
    whatever fields are available in df (name, job_profile, cost_center, etc.)
    and sets worker_status = 'Terminated' since the employee is absent from
    the current active roster.

    Stub rows are enriched automatically if the employee re-appears in a
    future roster file — the employees upsert will fill in the blanks.

    Parameters
    ----------
    conn   : active SQLite connection
    df     : source DataFrame whose id_col column references employees
    id_col : name of the employee ID column in df (default 'employee_id')

    Returns the number of stub rows inserted.
    """
    named_ids = df[id_col].dropna().unique().tolist()
    if not named_ids:
        return 0

    # Find which IDs are already in employees
    placeholders = ','.join(['?' for _ in named_ids])
    existing = {
        row[0] for row in conn.execute(
            f"SELECT employee_id FROM employees "
            f"WHERE employee_id IN ({placeholders})",
            named_ids
        ).fetchall()
    }

    missing_ids = [eid for eid in named_ids if eid not in existing]
    if not missing_ids:
        return 0

    # Fields we can attempt to pull from the source DataFrame
    # — only columns that exist in both df and the employees table
    candidate_fields = [
        'last_name', 'first_name', 'job_profile', 'cost_center',
        'school_program', 'employee_type', 'time_type', 'supervisory_org',
        'most_recent_hire_date', 'report_date',
    ]

    stub_cols = ['employee_id', 'worker_status'] + [
        f for f in candidate_fields if f in df.columns
    ]

    stubs = []
    for eid in missing_ids:
        source_rows = df[df[id_col] == eid]
        # Pick the most informative row: prefer rows with a name populated
        named_rows = source_rows[source_rows.get('last_name', pd.Series()).notna()] \
            if 'last_name' in source_rows.columns else pd.DataFrame()
        row = named_rows.iloc[0] if not named_rows.empty else source_rows.iloc[0]

        stub = {'employee_id': eid, 'worker_status': 'Terminated'}
        for field in candidate_fields:
            if field in df.columns:
                val = row.get(field)
                stub[field] = None if pd.isna(val) else val

        stubs.append(stub)

    col_list     = ', '.join(stub_cols)
    placeholders = ', '.join([f':{c}' for c in stub_cols])

    conn.executemany(
        f"INSERT OR IGNORE INTO employees ({col_list}) VALUES ({placeholders})",
        stubs
    )

    return len(stubs)


def load_terminations(
    conn: sqlite3.Connection,
    df: pd.DataFrame,
) -> tuple[int, int]:
    """
    Loads termination rows using two strategies depending on whether the row
    has been identified (employee_id populated) or remains anonymous (workday_id only).

    NAMED ROWS (employee_id IS NOT NULL):
        Enrichment upsert — ON CONFLICT(employee_id, termination_date) fills
        NULL columns from the incoming row without overwriting existing data.
        workday_id is NULLed out on named rows so only the employee_id
        constraint can fire (SQLite ON CONFLICT requires a single target).

    ANONYMOUS ROWS (employee_id IS NULL, workday_id IS NOT NULL):
        INSERT OR IGNORE — the UNIQUE(workday_id, termination_date) constraint
        deduplicates re-runs silently.

    Before any inserts, stubs any missing employee_ids into the employees table
    to satisfy the FK constraint.

    Returns (rows_inserted, rows_updated).
    """
    if df.empty:
        return 0, 0

    _stub_missing_employees(conn, df, id_col='employee_id')

    named = df[df['employee_id'].notna()].copy()
    anon  = df[df['employee_id'].isna()].copy()

    before = conn.execute("SELECT COUNT(*) FROM terminations").fetchone()[0]

    # --- Named rows: enrichment upsert ---
    if not named.empty:
        # NULL out workday_id so only UNIQUE(employee_id, termination_date) fires
        named['workday_id'] = None

        cols         = list(named.columns)
        col_list     = ', '.join(cols)
        placeholders = ', '.join([f':{c}' for c in cols])

        enrichment_cols = [
            c for c in cols
            if c not in ('employee_id', 'termination_date', 'workday_id',
                         'id', 'source_file')
        ]
        enrich_set = ',\n        '.join(
            f"{c} = COALESCE(terminations.{c}, excluded.{c})"
            for c in enrichment_cols
        )

        sql = f"""
            INSERT INTO terminations ({col_list})
            VALUES ({placeholders})
            ON CONFLICT(employee_id, termination_date) DO UPDATE SET
            {enrich_set}
        """
        conn.executemany(sql, _df_to_records(named))

    # --- Anonymous rows: insert or ignore ---
    if not anon.empty:
        cols         = list(anon.columns)
        col_list     = ', '.join(cols)
        placeholders = ', '.join([f':{c}' for c in cols])

        sql = f"""
            INSERT OR IGNORE INTO terminations ({col_list})
            VALUES ({placeholders})
        """
        conn.executemany(sql, _df_to_records(anon))

    after    = conn.execute("SELECT COUNT(*) FROM terminations").fetchone()[0]
    inserted = after - before
    updated  = len(df) - inserted
    return inserted, updated


def load_append_dedup(
    conn: sqlite3.Connection,
    table: str,
    df: pd.DataFrame,
) -> tuple[int, int]:
    """
    Append with deduplication: inserts new rows, silently skips rows that
    violate a UNIQUE constraint. Used for all event/snapshot tables where
    re-running the same file should be a no-op.

    For tables that reference employees via a FK, stubs any missing employee
    IDs before inserting to prevent FK constraint failures.

    Returns (rows_inserted, rows_skipped).
    """
    if df.empty:
        return 0, 0

    # Tables with FK columns pointing to employees — stub as needed
    employee_fk_cols = {
        'new_hires':            'employee_id',
        'manager_flags':        'employee_id',
        'feedback':             'recipient_employee_id',
        'staff_dev_scholarship':'employee_id',
        'family_scholarship':   'employee_id',
    }
    if table in employee_fk_cols:
        _stub_missing_employees(conn, df, id_col=employee_fk_cols[table])

    cols         = list(df.columns)
    col_list     = ', '.join(cols)
    placeholders = ', '.join([f':{c}' for c in cols])

    sql = f"""
        INSERT OR IGNORE INTO {table} ({col_list})
        VALUES ({placeholders})
    """

    records = _df_to_records(df)
    before  = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    conn.executemany(sql, records)
    after   = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    inserted = after - before
    skipped  = len(records) - inserted
    return inserted, skipped


def load_append(
    conn: sqlite3.Connection,
    table: str,
    df: pd.DataFrame,
) -> int:
    """
    Pure append: no conflict handling. Used for recruiting_pipeline (where
    the same candidate legitimately appears multiple times) and pipeline_log.
    Returns rows inserted.
    """
    if df.empty:
        return 0

    cols         = list(df.columns)
    col_list     = ', '.join(cols)
    placeholders = ', '.join([f':{c}' for c in cols])

    sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
    records = _df_to_records(df)
    conn.executemany(sql, records)
    return len(records)


# ---------------------------------------------------------------------------
# PIPELINE LOG
# ---------------------------------------------------------------------------

def log_run(
    conn: sqlite3.Connection,
    filename: str,
    target_table: str,
    rows_processed: int,
    rows_inserted: int,
    rows_updated: int,
    rows_skipped: int,
    status: str,
    error_message: str | None = None,
) -> None:
    """Writes one row to pipeline_log for each file processed."""
    conn.execute(
        """
        INSERT INTO pipeline_log
            (run_timestamp, filename, target_table, rows_processed,
             rows_inserted, rows_updated, rows_skipped, status, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            datetime.now().isoformat(),
            filename,
            target_table,
            rows_processed,
            rows_inserted,
            rows_updated,
            rows_skipped,
            status,
            error_message,
        )
    )


# ---------------------------------------------------------------------------
# MAIN LOAD ORCHESTRATOR
# ---------------------------------------------------------------------------

def load_all(
    conn: sqlite3.Connection,
    transformed: dict,
    source_filenames: dict[str, list[str]],
) -> list[dict]:
    """
    Writes all transformed DataFrames to the database inside a single
    transaction. If any table fails, the entire run is rolled back.

    Parameters
    ----------
    conn              : active SQLite connection
    transformed       : output dict from transform.transform_all()
    source_filenames  : {table_name: [filename, ...]} — maps each table to
                        the source file(s) that produced it, for pipeline_log

    Returns
    -------
    List of log dicts, one per table loaded (also written to pipeline_log).
    """
    log_entries = []

    try:
        with conn:  # context manager: commits on exit, rolls back on exception

            # 1. employees — full upsert
            emp_df = transformed.get('employees', pd.DataFrame())
            if not emp_df.empty:
                inserted, updated = load_employees(conn, emp_df)
                log_entries.append({
                    'table': 'employees', 'inserted': inserted,
                    'updated': updated, 'skipped': 0,
                    'status': 'success',
                })
                _log_entry(conn, source_filenames, 'employees',
                           len(emp_df), inserted, updated, 0, 'success')

            # 2. employee departures — partial update (worker_status only)
            dep_df = transformed.get('employee_departures', pd.DataFrame())
            if not dep_df.empty:
                updated = load_employee_departures(conn, dep_df)
                _log_entry(conn, source_filenames, 'employees (departures)',
                           len(dep_df), 0, updated, 0, 'success')

            # 3. terminations — enrichment upsert across all three sources
            term_frames = transformed.get('terminations', [])
            if isinstance(term_frames, list):
                term_df = pd.concat(term_frames, ignore_index=True) if term_frames else pd.DataFrame()
            else:
                term_df = term_frames

            if not term_df.empty:
                # Remove the match_status column — it's for pipeline diagnostics,
                # not stored in the DB
                term_df = term_df.drop(columns=['match_status'], errors='ignore')
                inserted, updated = load_terminations(conn, term_df)
                # Log using actual DB counts (inserted + updated), not input row
                # count — input includes duplicates across three source files
                _log_entry(conn, source_filenames, 'terminations',
                           inserted + updated, inserted, updated, 0, 'success')

            # 4. Append-with-dedup tables
            dedup_tables = [
                'new_hires', 'manager_flags', 'feedback',
                'staff_dev_scholarship', 'family_scholarship',
                'agg_headcount_gender', 'agg_headcount_birth_year',
            ]
            for table in dedup_tables:
                df = transformed.get(table, pd.DataFrame())
                if df is not None and not df.empty:
                    inserted, skipped = load_append_dedup(conn, table, df)
                    _log_entry(conn, source_filenames, table,
                               len(df), inserted, 0, skipped, 'success')

            # 5. Pure append tables
            for table in ['recruiting_pipeline']:
                df = transformed.get(table, pd.DataFrame())
                if df is not None and not df.empty:
                    inserted = load_append(conn, table, df)
                    _log_entry(conn, source_filenames, table,
                               len(df), inserted, 0, 0, 'success')

    except Exception as e:
        # Transaction rolled back automatically by the context manager.
        # Log the failure — this insert runs outside the rolled-back transaction.
        conn.execute(
            """
            INSERT INTO pipeline_log
                (run_timestamp, filename, target_table, rows_processed,
                 rows_inserted, rows_updated, rows_skipped, status, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (datetime.now().isoformat(), 'PIPELINE', 'ALL',
             0, 0, 0, 0, 'error', str(e))
        )
        conn.commit()
        raise   # re-raise so run.py can report the failure

    return log_entries


def _log_entry(
    conn: sqlite3.Connection,
    source_filenames: dict,
    table: str,
    processed: int,
    inserted: int,
    updated: int,
    skipped: int,
    status: str,
) -> None:
    """Internal helper: writes one pipeline_log row during load_all."""
    filenames = source_filenames.get(table, ['unknown'])
    for fname in filenames:
        log_run(
            conn       = conn,
            filename   = fname,
            target_table   = table,
            rows_processed = processed,
            rows_inserted  = inserted,
            rows_updated   = updated,
            rows_skipped   = skipped,
            status         = status,
        )
