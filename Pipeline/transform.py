"""
pipeline/transform.py

Responsible for two things:

1. EMPLOYEE MERGE
   The Email Roster and Years of Service files both target the `employees`
   table but carry different columns. This module merges them on employee_id
   into a single unified DataFrame ready for upsert.

   It also compares the incoming roster against the existing database to
   detect employees who have disappeared from the file — meaning they left —
   and marks them as Terminated.

2. COMPOSITE KEY MATCHING (Term Roster)
   The anonymised Term Roster has no employee_id. This module attempts to
   assign one by matching each row against named termination sources on a
   four-field composite key:
       most_recent_hire_date + termination_date + cost_center + job_profile

   Matches are only assigned when unambiguous: exactly one named record maps
   to the key AND exactly one roster row carries that key. Ambiguous or
   unresolvable rows get employee_id = NULL and a match_status flag so they
   can be reviewed or retried later as more named data accumulates.

This module does NOT read files or write to the database.
ingest.py feeds DataFrames in; load.py takes DataFrames out.
"""

import pandas as pd
from datetime import date


# ---------------------------------------------------------------------------
# EMPLOYEE MERGE
# ---------------------------------------------------------------------------

def merge_employee_sources(
    email_roster_df: pd.DataFrame,
    years_of_service_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Combines the Email Roster and Years of Service DataFrames into one
    unified employees DataFrame.

    The Email Roster is the spine — it defines who is currently active.
    Years of Service contributes columns the roster doesn't have:
    hire dates, YOS, manager flag, remote flag, job title, employee type,
    time type, cost center hierarchy.

    Where a column exists in both files (cost_center, supervisory_org,
    manager_email, school_program), the Email Roster value takes precedence
    since it is the more frequently updated source.

    Returns a DataFrame whose columns match the `employees` table in schema.sql.
    """
    # Columns contributed exclusively by Years of Service
    yos_only_cols = [
        'employee_id',
        'is_manager',
        'is_remote',
        'job_title',
        'employee_type',
        'time_type',
        'cost_center_hierarchy',
        'school_program',
        'original_hire_date',
        'most_recent_hire_date',
        'vesting_date',
        'years_of_service',
    ]

    yos_subset = years_of_service_df[yos_only_cols].copy()

    # Left join: every employee in the roster gets YOS data where available.
    # Employees in YOS but absent from the roster are excluded — they won't
    # appear in the current active file and will be handled by detect_terminations.
    merged = email_roster_df.merge(yos_subset, on='employee_id', how='left')

    return merged


def detect_terminations(
    incoming_df: pd.DataFrame,
    existing_ids: list[str],
    report_date: str,
) -> pd.DataFrame:
    """
    Compares employee_ids in the incoming roster against the list of IDs
    currently in the database. Any ID present in the DB but absent from
    the new roster is assumed to have left.

    Returns a DataFrame of update records — one row per departed employee —
    with worker_status set to 'Terminated' and last_seen_date unchanged
    (it retains whatever date they last appeared in the roster).

    These records are passed to load.py which applies them as partial updates
    to the employees table (only worker_status and report_date are touched).

    Parameters
    ----------
    incoming_df   : merged employees DataFrame from merge_employee_sources()
    existing_ids  : list of employee_ids currently in the employees table,
                    queried by load.py before calling this function
    report_date   : ISO date string from the roster filename
    """
    incoming_ids = set(incoming_df['employee_id'].dropna().unique())
    db_ids       = set(existing_ids)

    departed_ids = db_ids - incoming_ids

    if not departed_ids:
        return pd.DataFrame(columns=['employee_id', 'worker_status', 'report_date'])

    updates = pd.DataFrame({
        'employee_id':   list(departed_ids),
        'worker_status': 'Terminated',
        'report_date':   report_date,
    })

    return updates


# ---------------------------------------------------------------------------
# COMPOSITE KEY MATCHING (Term Roster)
# ---------------------------------------------------------------------------

# The four fields that together uniquely identify a termination event.
# Using all four minimises false matches — same job title in same cost center
# on different dates, or same dates but different role, won't collide.
COMPOSITE_KEY = [
    'most_recent_hire_date',
    'termination_date',
    'cost_center',
    'job_profile',
]


def resolve_term_roster_ids(
    term_roster_df: pd.DataFrame,
    named_terminations: list[pd.DataFrame],
) -> pd.DataFrame:
    """
    Attempts to assign employee_id to each row in the anonymised Term Roster
    using a composite key match against one or more named termination sources.

    Named termination sources are the SCD Terminations file and/or the Monthly
    Termination Report — any DataFrame that has both employee_id and at least
    termination_date + cost_center + job_profile. The Monthly Report also has
    most_recent_hire_date, making it a stronger match source.

    Matching rules
    --------------
    A match is only assigned when:
      - Exactly one named record maps to the composite key (no ambiguity
        on the named side — e.g. two people with same role/cost center/dates)
      - Exactly one roster row carries that composite key (no ambiguity
        on the anonymous side)

    Any key that fails either check gets employee_id = NULL and
    match_status = 'ambiguous' or 'unmatched' so it can be retried
    when more named data is available.

    Parameters
    ----------
    term_roster_df      : DataFrame from ingest.read_term_roster()
    named_terminations  : list of DataFrames from ingest.read_terminations_*()
                          Pass all available named sources — more coverage =
                          more matches.

    Returns
    -------
    The term_roster_df with two new columns added:
      employee_id   : matched ID string, or None
      match_status  : 'matched' | 'ambiguous' | 'unmatched'
    """
    # Build a unified lookup table from all named sources.
    # SCD file lacks most_recent_hire_date — fill with None so it can still
    # contribute 3-field matches on termination_date + cost_center + job_profile
    # for cases where the hire date key is also None in the roster.
    lookup_frames = []
    for df in named_terminations:
        cols = ['employee_id'] + [c for c in COMPOSITE_KEY if c in df.columns]
        frame = df[cols].copy()
        # Add missing key columns as None so all frames stack cleanly
        for col in COMPOSITE_KEY:
            if col not in frame.columns:
                frame[col] = None
        lookup_frames.append(frame[['employee_id'] + COMPOSITE_KEY])

    if not lookup_frames:
        # No named sources available — nothing to match against
        result = term_roster_df.copy()
        result['employee_id'] = None
        result['match_status'] = 'unmatched'
        return result

    named = pd.concat(lookup_frames, ignore_index=True).drop_duplicates()

    # Count how many distinct employee_ids map to each composite key
    # in the named sources. Keys with count > 1 are ambiguous.
    named_key_counts = (
        named.groupby(COMPOSITE_KEY, dropna=False)['employee_id']
        .nunique()
        .reset_index(name='named_id_count')
    )

    # Count how many rows in the roster share each composite key.
    # Keys with count > 1 mean two anonymous people can't be told apart.
    roster_key_counts = (
        term_roster_df.groupby(COMPOSITE_KEY, dropna=False)
        .size()
        .reset_index(name='roster_row_count')
    )

    # Unambiguous keys: exactly 1 match on both sides
    unambiguous = (
        named_key_counts[named_key_counts['named_id_count'] == 1]
        .merge(roster_key_counts[roster_key_counts['roster_row_count'] == 1], on=COMPOSITE_KEY)
    )

    # Build the final lookup: unambiguous key -> employee_id
    id_lookup = named.merge(unambiguous[COMPOSITE_KEY], on=COMPOSITE_KEY)

    # Apply to term roster
    result = term_roster_df.merge(
        id_lookup[COMPOSITE_KEY + ['employee_id']],
        on=COMPOSITE_KEY,
        how='left',
    )

    # Assign match_status
    # First mark everything unmatched, then label ambiguous keys, then matched
    result['match_status'] = 'unmatched'

    ambiguous_keys = named_key_counts[named_key_counts['named_id_count'] > 1][COMPOSITE_KEY]
    ambiguous_mask = result.merge(
        ambiguous_keys.assign(_flag=True),
        on=COMPOSITE_KEY,
        how='left',
    )['_flag'].notna().values

    result.loc[ambiguous_mask, 'match_status'] = 'ambiguous'
    result.loc[result['employee_id'].notna(), 'match_status'] = 'matched'

    return result


# ---------------------------------------------------------------------------
# CONVENIENCE WRAPPER
# ---------------------------------------------------------------------------

def transform_all(
    ingested: dict[str, list[pd.DataFrame]],
    existing_employee_ids: list[str],
) -> dict[str, pd.DataFrame | list]:
    """
    Convenience function called by run.py. Accepts the dict of ingested
    DataFrames (keyed by table name, values are lists because multiple files
    can target the same table), applies all transforms, and returns a dict
    of ready-to-load DataFrames keyed by table name.

    Parameters
    ----------
    ingested : {
        'employees':    [email_roster_df, years_of_service_df],  # order matters
        'terminations': [scd_df, monthly_df, term_roster_df],
        'new_hires':    [new_hires_df],
        ...
    }
    existing_employee_ids : list of employee_ids currently in the DB,
                            used to detect departures. Pass [] on first run.

    Returns
    -------
    {
        'employees':            merged + departure-flagged DataFrame,
        'employee_departures':  partial-update DataFrame (worker_status only),
        'terminations':         [named_dfs..., resolved_roster_df],
        'new_hires':            DataFrame,
        ...                     all other tables passed through unchanged
    }
    """
    result = {}

    # --- employees ---
    employee_frames = ingested.get('employees', [])
    email_df = next((df for df in employee_frames if 'last_seen_date' in df.columns), None)
    yos_df   = next((df for df in employee_frames if 'years_of_service' in df.columns), None)

    if email_df is not None and yos_df is not None:
        merged_employees = merge_employee_sources(email_df, yos_df)
    elif email_df is not None:
        merged_employees = email_df
    elif yos_df is not None:
        merged_employees = yos_df
    else:
        merged_employees = pd.DataFrame()

    result['employees'] = merged_employees

    # Detect departures only when we have a roster and existing DB data
    if not merged_employees.empty and existing_employee_ids:
        report_date = merged_employees['report_date'].iloc[0]
        result['employee_departures'] = detect_terminations(
            merged_employees, existing_employee_ids, report_date
        )
    else:
        result['employee_departures'] = pd.DataFrame()

    # --- terminations: resolve Term Roster IDs ---
    term_frames = ingested.get('terminations', [])
    roster_df = next(
        (df for df in term_frames if 'workday_id' in df.columns), None
    )
    named_dfs = [df for df in term_frames if 'workday_id' not in df.columns]

    if roster_df is not None and named_dfs:
        resolved_roster = resolve_term_roster_ids(roster_df, named_dfs)
        result['terminations'] = named_dfs + [resolved_roster]
    else:
        result['terminations'] = term_frames

    # --- all other tables passed through unchanged ---
    passthrough = [
        'new_hires', 'manager_flags', 'feedback',
        'staff_dev_scholarship', 'family_scholarship',
        'recruiting_pipeline', 'agg_headcount_gender',
        'agg_headcount_birth_year',
    ]
    for table in passthrough:
        frames = ingested.get(table, [])
        if frames:
            result[table] = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]

    return result