"""
pipeline/ingest.py

Responsible for ONE thing: reading a raw Excel file and returning a clean
pandas DataFrame with:
  - Correct header row (each Workday file has a different number of metadata
    rows above the actual column headers)
  - Columns renamed to snake_case names that match schema.sql
  - Date columns converted to ISO strings (YYYY-MM-DD)
  - Employee IDs read and stored as plain strings (leading zeros preserved
    by reading the column as dtype=str before pandas can coerce to int)
  - report_date extracted from the filename and added as a column
  - Empty/whitespace-only strings converted to None (NULL in SQLite)

This module does NOT connect to the database. It only reads and cleans.
transform.py handles any logic that combines columns or resolves keys.
load.py handles writing to SQLite.
"""

import re
import os
import pandas as pd
from datetime import datetime


# ---------------------------------------------------------------------------
# FILE REGISTRY
# Maps a filename pattern (substring match) to its ingestion config:
#   skip_rows   : how many rows to skip before the header row
#   reader      : which read function to call (defined below)
# ---------------------------------------------------------------------------
FILE_REGISTRY = [
    {
        "pattern":  "SCD_Email_Roster",
        "skip_rows": 1,
        "reader":   "read_email_roster",
    },
    {
        "pattern":  "Years_of_Service",
        "skip_rows": 1,
        "reader":   "read_years_of_service",
    },
    {
        "pattern":  "Active_Manager_Roster",
        "skip_rows": 1,
        "reader":   "read_manager_roster",
    },
    {
        "pattern":  "SCD_Full_Sail_New_Hire_Report",
        "skip_rows": 3,
        "reader":   "read_new_hires",
    },
    {
        "pattern":  "SCD_Full_Sail_Terminations_-_Staff_Development_Scholarship",
        "skip_rows": 5,
        "reader":   "read_staff_dev_scholarship",
    },
    {
        "pattern":  "SCD_Full_Sail_Terminations_-_Family_Scholarship",
        "skip_rows": 5,
        "reader":   "read_family_scholarship",
    },
    {
        "pattern":  "SCD_Full_Sail_Terminations",
        "skip_rows": 5,
        "reader":   "read_terminations_scd",
    },
    {
        "pattern":  "HCM_RPT_Monthly_Termination_Report",
        "skip_rows": 7,
        "reader":   "read_terminations_monthly",
    },
    {
        "pattern":  "SCD_Term_Roster",
        "skip_rows": 2,
        "reader":   "read_term_roster",
    },
    {
        "pattern":  "HCM_RPT_Anytime_Feedback",
        "skip_rows": 7,
        "reader":   "read_feedback",
    },
    {
        "pattern":  "Source_to_Pipeline",
        "skip_rows": 1,
        "reader":   "read_recruiting_pipeline",
    },
    {
        "pattern":  "Headcount_by_Gender",
        "skip_rows": 2,
        "reader":   "read_headcount_gender",
    },
    {
        "pattern":  "Headcount_by_Birth_Year",
        "skip_rows": 2,
        "reader":   "read_headcount_birth_year",
    },
]

# Note: order matters — more specific patterns (e.g. the two scholarship
# files) must appear BEFORE the generic "SCD_Full_Sail_Terminations" entry,
# otherwise the generic pattern will match them first.


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def extract_report_date(filepath: str) -> str:
    """
    Pulls the YYYY-MM-DD date embedded in every Workday filename.
    e.g. 'SCD_Email_Roster_2026-06-07_01_03_EDT.xlsx' -> '2026-06-07'
    Returns today's date as a fallback if no date is found.
    """
    match = re.search(r'(\d{4}-\d{2}-\d{2})', os.path.basename(filepath))
    if match:
        return match.group(1)
    return datetime.today().strftime('%Y-%m-%d')


def normalise_employee_id(series: pd.Series) -> pd.Series:
    """
    Cleans an employee ID column that has already been read as dtype=str.
    The only job left is stripping the '.0' suffix that openpyxl occasionally
    adds to numeric-looking cells (e.g. '42.0' -> '42', '105235.0' -> '105235').
    Leading zeros are preserved because the column never passed through pandas
    integer coercion.
    """
    def _fix(val):
        try:
            if pd.isna(val):
                return None
        except (TypeError, ValueError):
            pass
        s = str(val).strip()
        if s.lower() in ('nan', 'none', ''):
            return None
        if s.endswith('.0'):
            s = s[:-2]
        return s
    return series.astype(object).apply(_fix)


def to_iso_date(series: pd.Series) -> pd.Series:
    """
    Converts a datetime64 column to ISO date strings (YYYY-MM-DD).
    Strips the time component. Returns None for NaT/null values.
    """
    return series.apply(
        lambda v: v.strftime('%Y-%m-%d') if pd.notna(v) else None
    )


def clean_strings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts empty strings and whitespace-only strings to None across the
    entire DataFrame so they become NULL in SQLite rather than empty strings.
    """
    str_cols = df.select_dtypes(include='object').columns
    df[str_cols] = df[str_cols].apply(
        lambda col: col.str.strip().replace('', None)
    )
    return df


def read_raw(filepath: str, skip_rows: int) -> pd.DataFrame:
    """Base Excel read with shared options."""
    return pd.read_excel(
        filepath,
        skiprows=skip_rows,
        engine='openpyxl',
        dtype=str,          # read everything as string initially;
    )                       # date columns are re-parsed per reader below


# ---------------------------------------------------------------------------
# DISPATCH
# ---------------------------------------------------------------------------

def ingest(filepath: str) -> tuple[str, pd.DataFrame]:
    """
    Main entry point. Matches the file to a registry entry, calls the
    appropriate reader, and returns (target_table_name, clean_dataframe).

    Raises ValueError if the filename doesn't match any known pattern.
    """
    filename = os.path.basename(filepath)
    for entry in FILE_REGISTRY:
        if entry["pattern"] in filename:
            reader_fn = globals()[entry["reader"]]
            df = reader_fn(filepath, entry["skip_rows"])
            df = clean_strings(df)
            return _table_for_reader(entry["reader"]), df

    raise ValueError(
        f"Unrecognised file: '{filename}'. "
        f"Add it to FILE_REGISTRY in ingest.py."
    )


def _table_for_reader(reader_name: str) -> str:
    """Maps reader function names to their target DB table."""
    mapping = {
        "read_email_roster":           "employees",
        "read_years_of_service":       "employees",
        "read_manager_roster":         "manager_flags",
        "read_new_hires":              "new_hires",
        "read_terminations_scd":       "terminations",
        "read_terminations_monthly":   "terminations",
        "read_term_roster":            "terminations",
        "read_feedback":               "feedback",
        "read_staff_dev_scholarship":  "staff_dev_scholarship",
        "read_family_scholarship":     "family_scholarship",
        "read_recruiting_pipeline":    "recruiting_pipeline",
        "read_headcount_gender":       "agg_headcount_gender",
        "read_headcount_birth_year":   "agg_headcount_birth_year",
    }
    return mapping[reader_name]


# ---------------------------------------------------------------------------
# READERS
# One function per file type. Each function:
#   1. Reads the raw Excel file
#   2. Renames columns to match schema.sql
#   3. Converts date columns to ISO strings
#   4. Normalises employee_id
#   5. Adds report_date from filename
#   6. Returns only the columns the target table expects
# ---------------------------------------------------------------------------

def read_email_roster(filepath: str, skip_rows: int) -> pd.DataFrame:
    df = read_raw(filepath, skip_rows)
    report_date = extract_report_date(filepath)

    df = df.rename(columns={
        'Worker Status':                  'worker_status',
        'Staff or Faculty':               'staff_or_faculty',
        'Employee or Contingent':         'employee_or_contingent',
        'Job Profile':                    'job_profile',
        'Employee ID':                    'employee_id',
        'Employee Last Name':             'last_name',
        'Employee First Name':            'first_name',
        'Preferred First Name':           'preferred_name',
        'Email - Primary Work':           'email_work',
        'Public Primary Work Email Address': 'email_work_public',
        'Cost Center':                    'cost_center',
        'Supervisory Organization':       'supervisory_org',
        'Manager Work Email':             'manager_email',
    })

    df['employee_id'] = normalise_employee_id(df['employee_id'])
    df['report_date'] = report_date
    df['last_seen_date'] = report_date

    # Drop the public email duplicate — we only keep the primary
    df = df.drop(columns=['email_work_public'], errors='ignore')

    return df[[
        'employee_id', 'worker_status', 'staff_or_faculty',
        'employee_or_contingent', 'job_profile', 'last_name', 'first_name',
        'preferred_name', 'email_work', 'cost_center', 'supervisory_org',
        'manager_email', 'report_date', 'last_seen_date',
    ]]


def read_years_of_service(filepath: str, skip_rows: int) -> pd.DataFrame:
    df = read_raw(filepath, skip_rows)
    report_date = extract_report_date(filepath)

    df = df.rename(columns={
        'Employee ID':                                      'employee_id',
        'Is Manager':                                       'is_manager',
        'Remote WFH or >= 50 away':                        'is_remote',
        'Job Title':                                        'job_title',
        'Employee Type':                                    'employee_type',
        'Time Type':                                        'time_type',
        'Cost Center Hierarchy':                            'cost_center_hierarchy',
        'Cost Center':                                      'cost_center',
        'School Program':                                   'school_program',
        'Supervisory Organization':                         'supervisory_org',
        'Managers Email Address':                           'manager_email',
        'Original Hire Date':                               'original_hire_date',
        'Most Recent Hire Date':                            'most_recent_hire_date',
        'Vesting Date is used for Calc YOS for re-hires':  'vesting_date',
        'CF_EE Total YOS':                                  'years_of_service',
    })

    df['employee_id'] = normalise_employee_id(df['employee_id'])
    df['original_hire_date']     = to_iso_date(pd.to_datetime(df['original_hire_date'],     errors='coerce'))
    df['most_recent_hire_date']  = to_iso_date(pd.to_datetime(df['most_recent_hire_date'],  errors='coerce'))
    df['vesting_date']           = to_iso_date(pd.to_datetime(df['vesting_date'],           errors='coerce'))
    df['report_date'] = report_date

    return df[[
        'employee_id', 'is_manager', 'is_remote', 'job_title', 'employee_type',
        'time_type', 'cost_center_hierarchy', 'cost_center', 'school_program',
        'supervisory_org', 'manager_email', 'original_hire_date',
        'most_recent_hire_date', 'vesting_date', 'years_of_service',
        'report_date',
    ]]


def read_manager_roster(filepath: str, skip_rows: int) -> pd.DataFrame:
    df = read_raw(filepath, skip_rows)
    report_date = extract_report_date(filepath)

    df = df.rename(columns={
        'Employee ID':          'employee_id',
        'Worker':               'worker_name',
        'Email - Work':         'email_work',
        'Assigned Organization':'assigned_org',
        'Job Title':            'job_title',
        'Active Status':        'active_status',
        'Is Manager':           'is_manager',
        'Cost Center':          'cost_center',
    })

    df['employee_id']    = normalise_employee_id(df['employee_id'])
    df['snapshot_date']  = report_date
    df['report_date']    = report_date

    return df[[
        'employee_id', 'is_manager', 'active_status', 'job_title',
        'assigned_org', 'cost_center', 'snapshot_date', 'report_date',
    ]]


def read_new_hires(filepath: str, skip_rows: int) -> pd.DataFrame:
    df = read_raw(filepath, skip_rows)
    report_date = extract_report_date(filepath)

    df = df.rename(columns={
        'Employee Id':              'employee_id',
        'Work Email':               'email_work',
        'Last Name':                'last_name',
        'First Name':               'first_name',
        'Hire Date':                'hire_date',
        'Worker Type':              'worker_type',
        'Time Type':                'time_type',
        'Manager':                  'manager_email',
        'Supervisory Organization': 'supervisory_org',
        'Cost Center':              'cost_center',
        'School Program':           'school_program',
        'Business Title':           'business_title',
    })

    df['employee_id'] = normalise_employee_id(df['employee_id'])
    df['hire_date']   = to_iso_date(pd.to_datetime(df['hire_date'], errors='coerce'))
    df['report_date'] = report_date

    return df[[
        'employee_id', 'email_work', 'last_name', 'first_name', 'hire_date',
        'worker_type', 'time_type', 'manager_email', 'supervisory_org',
        'cost_center', 'school_program', 'business_title', 'report_date',
    ]]


def read_terminations_scd(filepath: str, skip_rows: int) -> pd.DataFrame:
    """Weekly named terminations file."""
    df = read_raw(filepath, skip_rows)
    report_date = extract_report_date(filepath)

    df = df.rename(columns={
        'Employee ID':                          'employee_id',
        'Worker':                               'worker_name',
        'Worker Email Address':                 'email_work',
        'Effective Date':                       'termination_date',
        "Worker's Manager":                     'manager_name',
        'Supervisory Organization - Current':   'supervisory_org',
        'Cost Center - Current':                'cost_center',
        'ESI Assigned Org Proposed is School Program': 'school_program',
        'Job Profile - Current':                'job_profile',
        'Time Type - Current':                  'time_type',
        'Employee/Contingent Worker Type - Current': 'employee_type',
    })

    # Split worker_name into last/first
    name_split = df['worker_name'].str.split(' ', n=1, expand=True)
    df['first_name'] = name_split[0]
    df['last_name']  = name_split[1] if 1 in name_split.columns else None

    df['employee_id']       = normalise_employee_id(df['employee_id'])
    df['termination_date']  = to_iso_date(pd.to_datetime(df['termination_date'], errors='coerce'))
    df['source_file']       = os.path.basename(filepath)
    df['report_date']       = report_date

    return df[[
        'employee_id', 'email_work', 'first_name', 'last_name',
        'termination_date', 'manager_name', 'supervisory_org', 'cost_center',
        'school_program', 'job_profile', 'time_type', 'employee_type',
        'source_file', 'report_date',
    ]]


def read_terminations_monthly(filepath: str, skip_rows: int) -> pd.DataFrame:
    """Monthly termination report — richer, includes Enterprise ID."""
    df = read_raw(filepath, skip_rows)
    report_date = extract_report_date(filepath)

    df = df.rename(columns={
        'Employee ID':                  'employee_id',
        'Enterprise ID':                'enterprise_id',
        'Legal Last Name':              'last_name',
        'Legal First Name':             'first_name',
        'Employee Type':                'employee_type',
        'Time Type':                    'time_type',
        'Job Profile':                  'job_profile',
        'Last Hire Date':               'most_recent_hire_date',
        'Termination Effective Date':   'termination_date',
        "Worker's Manager":             'manager_name',
        'Supervisory Organization':     'supervisory_org',
        'Cost Center':                  'cost_center',
        'Cost Center - School Program': 'school_program',
        'Cost Center Hierarchy':        'cost_center_hierarchy',
    })

    df['employee_id']           = normalise_employee_id(df['employee_id'])
    df['termination_date']      = to_iso_date(pd.to_datetime(df['termination_date'],     errors='coerce'))
    df['most_recent_hire_date'] = to_iso_date(pd.to_datetime(df['most_recent_hire_date'], errors='coerce'))
    df['source_file']           = os.path.basename(filepath)
    df['report_date']           = report_date

    return df[[
        'employee_id', 'enterprise_id', 'last_name', 'first_name',
        'employee_type', 'time_type', 'job_profile', 'most_recent_hire_date',
        'termination_date', 'manager_name', 'supervisory_org', 'cost_center',
        'school_program', 'cost_center_hierarchy', 'source_file', 'report_date',
    ]]


def read_term_roster(filepath: str, skip_rows: int) -> pd.DataFrame:
    """
    Anonymised historical termination roster (3,650+ rows, back to 2021).
    No Employee ID — identity resolution via composite key happens in transform.py.
    """
    df = read_raw(filepath, skip_rows)
    report_date = extract_report_date(filepath)

    df = df.rename(columns={
        'Workday ID':                       'workday_id',
        'Age Group':                        'age_group',
        'Gender':                           'gender',
        'Job Profile - Primary Position':   'job_profile',
        'Time Type':                        'time_type',
        'Cost Center':                      'cost_center',
        'Program':                          'school_program',
        'Manager':                          'manager_name',
        'Most Recent Hire Date':            'most_recent_hire_date',
        'Termination date':                 'termination_date',
        'Days Between Hire and Term':       'days_to_term',
        'Bucket - Days between Hire and Term': 'tenure_bucket',
        'Termed within a year':             'termed_within_year',
        'Termination Category':             'term_category',
        'Termination Reason':               'term_reason',
    })

    df['most_recent_hire_date'] = to_iso_date(pd.to_datetime(df['most_recent_hire_date'], errors='coerce'))
    df['termination_date']      = to_iso_date(pd.to_datetime(df['termination_date'],      errors='coerce'))
    df['days_to_term']          = pd.to_numeric(df['days_to_term'], errors='coerce').astype('Int64')
    df['source_file']           = os.path.basename(filepath)
    df['report_date']           = report_date

    # employee_id is intentionally absent here — transform.py will attempt to
    # populate it via composite key match before load.py writes these rows.
    return df[[
        'workday_id', 'age_group', 'gender', 'job_profile', 'time_type',
        'cost_center', 'school_program', 'manager_name', 'most_recent_hire_date',
        'termination_date', 'days_to_term', 'tenure_bucket', 'termed_within_year',
        'term_category', 'term_reason', 'source_file', 'report_date',
    ]]


def read_feedback(filepath: str, skip_rows: int) -> pd.DataFrame:
    df = read_raw(filepath, skip_rows)
    report_date = extract_report_date(filepath)

    df = df.rename(columns={
        'Employee ID':              'recipient_employee_id',
        'Full Legal Name':          'recipient_name',
        'Position':                 'recipient_position',
        'Cost Center':              'recipient_cost_center',
        'School Program':           'recipient_school_program',
        'Feedback From':            'giver_name',
        'Feedback From Employee ID':'giver_employee_id',
        'Date':                     'feedback_date',
        'Feedback  Badge':          'badge',
        'Position.1':               'giver_position',
        'Department':               'giver_department',
        'Comment':                  'comment',
    })

    df['recipient_employee_id'] = normalise_employee_id(df['recipient_employee_id'])
    df['giver_employee_id']     = normalise_employee_id(df['giver_employee_id'])
    # Feedback date includes a time component — strip to date only
    df['feedback_date']         = to_iso_date(pd.to_datetime(df['feedback_date'], errors='coerce'))
    df['report_date']           = report_date

    return df[[
        'recipient_employee_id', 'recipient_name', 'recipient_position',
        'recipient_cost_center', 'recipient_school_program', 'giver_name',
        'giver_employee_id', 'feedback_date', 'badge', 'giver_position',
        'giver_department', 'comment', 'report_date',
    ]]


def read_staff_dev_scholarship(filepath: str, skip_rows: int) -> pd.DataFrame:
    df = read_raw(filepath, skip_rows)
    report_date = extract_report_date(filepath)

    df = df.rename(columns={
        'Employee ID':                                      'employee_id',
        'Worker':                                           'worker_name',
        'Worker Email Address':                             'email_work',
        'Effective Date':                                   'effective_date',
        "Worker's Manager":                                 'manager_name',
        'Supervisory Organization - Current':               'supervisory_org',
        'Cost Center - Current':                            'cost_center',
        'ESI Assigned Org Proposed is School Program':      'school_program',
        'Job Profile - Current':                            'job_profile',
        'Employee/Contingent Worker Type - Current':        'employee_type',
        'Is Employee Participating in the Staff Development Scholarship': 'is_participating',
        'Student Number of Employee':                       'student_number',
    })

    df['employee_id']    = normalise_employee_id(df['employee_id'])
    df['effective_date'] = to_iso_date(pd.to_datetime(df['effective_date'], errors='coerce'))
    df['report_date']    = report_date

    return df[[
        'employee_id', 'email_work', 'effective_date', 'manager_name',
        'supervisory_org', 'cost_center', 'school_program', 'job_profile',
        'employee_type', 'is_participating', 'student_number', 'report_date',
    ]]


def read_family_scholarship(filepath: str, skip_rows: int) -> pd.DataFrame:
    df = read_raw(filepath, skip_rows)
    report_date = extract_report_date(filepath)

    df = df.rename(columns={
        'Employee ID':                                      'employee_id',
        'Worker':                                           'worker_name',
        'Worker Email Address':                             'email_work',
        'Effective Date':                                   'effective_date',
        "Worker's Manager":                                 'manager_name',
        'Supervisory Organization - Current':               'supervisory_org',
        'Cost Center - Current':                            'cost_center',
        'ESI Assigned Org Proposed is School Program':      'school_program',
        'Job Profile - Current':                            'job_profile',
        'Employee/Contingent Worker Type - Current':        'employee_type',
        'Is Employee Sponsoring a Student in the Family Scholarship?': 'is_sponsoring',
        'Name of Sponsored Family Member 1':                'member_1_name',
        'Student Number of Sponsored Family Member 1':      'member_1_student_number',
        'Relationship of Family Member 1 to Employee':      'member_1_relation',
        'Name of Sponsored Family Member 2':                'member_2_name',
        'Student Number of Sponsored Family Member 2':      'member_2_student_number',
        'Relationship of Family Member 2 to Employee':      'member_2_relation',
    })

    df['employee_id']    = normalise_employee_id(df['employee_id'])
    df['effective_date'] = to_iso_date(pd.to_datetime(df['effective_date'], errors='coerce'))
    df['report_date']    = report_date

    return df[[
        'employee_id', 'email_work', 'effective_date', 'manager_name',
        'supervisory_org', 'cost_center', 'school_program', 'job_profile',
        'employee_type', 'is_sponsoring',
        'member_1_name', 'member_1_student_number', 'member_1_relation',
        'member_2_name', 'member_2_student_number', 'member_2_relation',
        'report_date',
    ]]


def read_recruiting_pipeline(filepath: str, skip_rows: int) -> pd.DataFrame:
    df = read_raw(filepath, skip_rows)
    report_date = extract_report_date(filepath)

    df = df.rename(columns={
        'Added Date':                   'added_date',
        'Candidate Stage':              'candidate_stage',
        'Disposition Reason':           'disposition_reason',
        'Source':                       'source',
        'Job Requisition':              'job_requisition',
        'Job Posting Title':            'job_title',
        'Recruiter':                    'recruiter',
        'CF JA Recruiting Start Date':  'recruiting_start_date',
        'Job Posting Start Date':       'posting_start_date',
        'Referral':                     'referral',
        'CF LRV - Employee ID':         'referrer_employee_id',
        'CF LRV - Currently Terminated?': 'referrer_terminated',
    })

    df['added_date']            = to_iso_date(pd.to_datetime(df['added_date'],            errors='coerce'))
    df['recruiting_start_date'] = to_iso_date(pd.to_datetime(df['recruiting_start_date'], errors='coerce'))
    df['posting_start_date']    = to_iso_date(pd.to_datetime(df['posting_start_date'],    errors='coerce'))
    df['referrer_employee_id']  = normalise_employee_id(df['referrer_employee_id'])
    df['report_date']           = report_date

    return df[[
        'added_date', 'candidate_stage', 'disposition_reason', 'source',
        'job_requisition', 'job_title', 'recruiter', 'recruiting_start_date',
        'posting_start_date', 'referral', 'referrer_employee_id',
        'referrer_terminated', 'report_date',
    ]]


def read_headcount_gender(filepath: str, skip_rows: int) -> pd.DataFrame:
    df = read_raw(filepath, skip_rows)
    report_date = extract_report_date(filepath)

    df = df.rename(columns={
        'Gender': 'gender',
        'Count':  'count',
    })

    # Drop the 'Total' summary row Workday appends at the bottom
    df = df[df['gender'].str.lower() != 'total']

    df['count']         = pd.to_numeric(df['count'], errors='coerce').astype('Int64')
    df['snapshot_date'] = report_date

    return df[['gender', 'count', 'snapshot_date']]


def read_headcount_birth_year(filepath: str, skip_rows: int) -> pd.DataFrame:
    df = read_raw(filepath, skip_rows)
    report_date = extract_report_date(filepath)

    df = df.rename(columns={
        'Year from Date of Birth': 'birth_year',
        'Count':                   'count',
    })

    df['birth_year']    = pd.to_numeric(df['birth_year'], errors='coerce').astype('Int64')
    df['count']         = pd.to_numeric(df['count'],      errors='coerce').astype('Int64')
    df['snapshot_date'] = report_date

    return df[['birth_year', 'count', 'snapshot_date']]
