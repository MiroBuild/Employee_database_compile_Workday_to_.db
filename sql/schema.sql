-- =============================================================================
-- Employee Database Schema
-- Source: Workday exports (Full Sail University)
-- =============================================================================
-- CONVENTIONS:
--   - All column names are snake_case
--   - Dates stored as TEXT in ISO format: YYYY-MM-DD
--   - Employee IDs stored as TEXT to preserve leading zeros (e.g. '00004648')
--   - Surrogate PKs (id INTEGER PRIMARY KEY) used on append tables so
--     re-ingesting the same file twice is detectable and preventable
--   - report_date = the file generation date extracted from the filename
-- =============================================================================


-- -----------------------------------------------------------------------------
-- CORE HUB
-- Built by merging: Email Roster + Years of Service
-- Load behaviour: UPSERT on employee_id
--                 Employees absent from the new roster are marked 'Terminated'
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS employees (
    employee_id             TEXT PRIMARY KEY,   -- source: Employee ID

    -- Identity (Email Roster)
    worker_status           TEXT,               -- Active / On Leave / Terminated
    staff_or_faculty        TEXT,               -- Staff / Faculty
    employee_or_contingent  TEXT,               -- Employee / Contingent Worker
    job_profile             TEXT,               -- e.g. "Course Director-SE"
    last_name               TEXT,
    first_name              TEXT,
    preferred_name          TEXT,
    email_work              TEXT,
    cost_center             TEXT,               -- e.g. "100001 Education"
    supervisory_org         TEXT,               -- e.g. "EDU - DC - CABS (Tim Bowser)"
    manager_email           TEXT,

    -- Enrichment (Years of Service)
    job_title               TEXT,               -- business title, may differ from job_profile
    employee_type           TEXT,               -- Staff / Faculty
    time_type               TEXT,               -- Full time / Part time
    cost_center_hierarchy   TEXT,               -- e.g. "Education"
    school_program          TEXT,               -- e.g. "10000 Overhead"
    is_manager              TEXT,               -- Yes / (blank)
    is_remote               TEXT,               -- Yes / (blank)
    original_hire_date      TEXT,               -- ISO date
    most_recent_hire_date   TEXT,               -- ISO date
    vesting_date            TEXT,               -- ISO date; used for YOS calc on re-hires
    years_of_service        REAL,               -- decimal, e.g. 12.75

    -- Pipeline metadata
    last_seen_date          TEXT,               -- ISO date of most recent roster that included this employee
    report_date             TEXT                -- ISO date this record was last updated from a file
);


-- -----------------------------------------------------------------------------
-- NEW HIRES
-- Source: SCD Full Sail New Hire Report
-- Load behaviour: APPEND; de-duplicate on (employee_id, hire_date)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS new_hires (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    employee_id         TEXT REFERENCES employees(employee_id),
    email_work          TEXT,
    last_name           TEXT,
    first_name          TEXT,
    hire_date           TEXT,               -- ISO date
    worker_type         TEXT,               -- Staff / Faculty / (blank for FWS)
    time_type           TEXT,               -- Full time / Part time
    manager_email       TEXT,
    supervisory_org     TEXT,
    cost_center         TEXT,
    school_program      TEXT,
    business_title      TEXT,
    report_date         TEXT,               -- ISO date from filename
    UNIQUE(employee_id, hire_date)          -- prevents duplicate ingestion
);


-- -----------------------------------------------------------------------------
-- TERMINATIONS
-- Sources merged into one table:
--   1. SCD Full Sail Terminations          (named, weekly, Employee ID)
--   2. HCM Monthly Termination Report      (named, monthly, Employee ID + Enterprise ID)
--   3. SCD Term Roster                     (anonymised, historical, Workday ID only)
--
-- Rows from source 3 with a successful composite key match get employee_id populated.
-- Rows without a match get NULL employee_id and retain workday_id for future resolution.
--
-- Composite key for matching source 3 → sources 1/2:
--   most_recent_hire_date + termination_date + cost_center + job_profile
--
-- Load behaviour: APPEND; de-duplicate on (employee_id, termination_date) for
--                 named rows, and on (workday_id, termination_date) for anon rows
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS terminations (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Identity (NULL for unresolved anon rows)
    employee_id             TEXT REFERENCES employees(employee_id),
    workday_id              TEXT,               -- from Term Roster (anon source)
    enterprise_id           TEXT,               -- from Monthly Term Report

    -- Core event fields (present in all three sources)
    termination_date        TEXT,               -- ISO date
    most_recent_hire_date   TEXT,               -- ISO date; part of composite key
    cost_center             TEXT,               -- part of composite key
    job_profile             TEXT,               -- part of composite key

    -- Extended fields (named sources only; NULL for unresolved anon rows)
    last_name               TEXT,
    first_name              TEXT,
    email_work              TEXT,
    employee_type           TEXT,
    time_type               TEXT,
    school_program          TEXT,
    supervisory_org         TEXT,
    manager_name            TEXT,
    cost_center_hierarchy   TEXT,

    -- Anonymised enrichment (Term Roster)
    age_group               TEXT,               -- e.g. "25 - 29"
    gender                  TEXT,
    days_to_term            INTEGER,
    tenure_bucket           TEXT,               -- e.g. "61 - 90"
    termed_within_year      TEXT,               -- Yes / No
    term_category           TEXT,               -- e.g. "Terminate Employee > Voluntary"
    term_reason             TEXT,               -- e.g. "Other Employment"

    -- Pipeline metadata
    source_file             TEXT,               -- which file this row came from
    report_date             TEXT,               -- ISO date from filename

    UNIQUE(employee_id, termination_date),
    UNIQUE(workday_id,  termination_date)
);


-- -----------------------------------------------------------------------------
-- MANAGER FLAGS
-- Source: Active Manager Roster
-- Load behaviour: APPEND with snapshot_date; tracks manager status over time
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS manager_flags (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    employee_id         TEXT REFERENCES employees(employee_id),
    is_manager          TEXT,               -- Yes
    active_status       TEXT,               -- Yes
    job_title           TEXT,
    assigned_org        TEXT,
    cost_center         TEXT,
    snapshot_date       TEXT,               -- ISO date from filename
    report_date         TEXT,
    UNIQUE(employee_id, snapshot_date)
);


-- -----------------------------------------------------------------------------
-- FEEDBACK
-- Source: HCM Anytime Feedback
-- Load behaviour: APPEND; de-duplicate on (recipient_employee_id, giver_employee_id, feedback_date)
-- Note: giver may be a terminated employee not present in employees table — FK nullable
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS feedback (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    recipient_employee_id       TEXT REFERENCES employees(employee_id),
    recipient_name              TEXT,
    recipient_position          TEXT,
    recipient_cost_center       TEXT,
    recipient_school_program    TEXT,
    giver_name                  TEXT,
    giver_employee_id           TEXT,           -- not a hard FK; giver may be termed
    feedback_date               TEXT,           -- ISO date
    badge                       TEXT,           -- e.g. "Thank You", "Give Back to Others"
    giver_position              TEXT,
    giver_department            TEXT,
    comment                     TEXT,
    report_date                 TEXT,
    UNIQUE(recipient_employee_id, giver_employee_id, feedback_date)
);


-- -----------------------------------------------------------------------------
-- STAFF DEVELOPMENT SCHOLARSHIP (terminating participants)
-- Source: SCD Full Sail Terminations - Staff Development Scholarship
-- Load behaviour: APPEND; de-duplicate on (employee_id, effective_date)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staff_dev_scholarship (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    employee_id             TEXT REFERENCES employees(employee_id),
    email_work              TEXT,
    effective_date          TEXT,               -- ISO date (termination date)
    manager_name            TEXT,
    supervisory_org         TEXT,
    cost_center             TEXT,
    school_program          TEXT,
    job_profile             TEXT,
    employee_type           TEXT,
    is_participating        TEXT,               -- Yes
    student_number          TEXT,
    report_date             TEXT,
    UNIQUE(employee_id, effective_date)
);


-- -----------------------------------------------------------------------------
-- FAMILY SCHOLARSHIP (terminating participants)
-- Source: SCD Full Sail Terminations - Family Scholarship Participants
-- Load behaviour: APPEND; de-duplicate on (employee_id, effective_date)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS family_scholarship (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    employee_id             TEXT REFERENCES employees(employee_id),
    email_work              TEXT,
    effective_date          TEXT,               -- ISO date (termination date)
    manager_name            TEXT,
    supervisory_org         TEXT,
    cost_center             TEXT,
    school_program          TEXT,
    job_profile             TEXT,
    employee_type           TEXT,
    is_sponsoring           TEXT,               -- Yes
    member_1_name           TEXT,
    member_1_student_number TEXT,
    member_1_relation       TEXT,
    member_2_name           TEXT,
    member_2_student_number TEXT,
    member_2_relation       TEXT,
    report_date             TEXT,
    UNIQUE(employee_id, effective_date)
);


-- -----------------------------------------------------------------------------
-- RECRUITING PIPELINE
-- Source: Source to Pipeline (Detail) - Last Month
-- Load behaviour: APPEND; no hard de-duplication (same candidate can appear
--                 multiple times at different stages for different requisitions)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS recruiting_pipeline (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    added_date              TEXT,               -- ISO date candidate entered pipeline
    candidate_stage         TEXT,               -- Review / Screen / Rejected / etc.
    disposition_reason      TEXT,
    source                  TEXT,               -- e.g. "Job Sites -> Indeed"
    job_requisition         TEXT,               -- e.g. "R00011248 Senior Staff Accountant"
    job_title               TEXT,
    recruiter               TEXT,
    recruiting_start_date   TEXT,               -- ISO date
    posting_start_date      TEXT,               -- ISO date
    referral                TEXT,               -- referral flag / description
    referrer_employee_id    TEXT,               -- FK to employees if internal referral
    referrer_terminated     TEXT,               -- Yes / (blank)
    report_date             TEXT
);


-- -----------------------------------------------------------------------------
-- AGGREGATE: HEADCOUNT BY GENDER (snapshot)
-- Source: Headcount by Gender
-- Load behaviour: APPEND with snapshot_date (one row per gender per week)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS agg_headcount_gender (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    gender          TEXT,
    count           INTEGER,
    snapshot_date   TEXT,               -- ISO date from filename
    UNIQUE(gender, snapshot_date)
);


-- -----------------------------------------------------------------------------
-- AGGREGATE: HEADCOUNT BY BIRTH YEAR (snapshot)
-- Source: Headcount by Birth Year
-- Load behaviour: APPEND with snapshot_date
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS agg_headcount_birth_year (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    birth_year      INTEGER,
    count           INTEGER,
    snapshot_date   TEXT,               -- ISO date from filename
    UNIQUE(birth_year, snapshot_date)
);


-- -----------------------------------------------------------------------------
-- SCHOLARSHIP STUDENTS (monthly snapshot)
-- Source: Monthly Report (Copy tab)
-- Tracks Full Sail employees and their family members enrolled as students
-- under the Staff Development Scholarship (SDS), Family Scholarship (FSFS),
-- or other scholarship types (LAFS etc.)
-- Load behaviour: UPSERT on print_id — new students inserted, existing rows
--                 fully updated to reflect latest status each month.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS scholarship_students (
    print_id                TEXT PRIMARY KEY,   -- student ID from school system
    employee_id             TEXT REFERENCES employees(employee_id),
    student_name            TEXT,               -- Last, First format
    last_name               TEXT,
    first_name              TEXT,
    preferred_name          TEXT,
    student_email           TEXT,
    other_email             TEXT,
    scholarship_type        TEXT,               -- SDS / FSFS / LAFS
    employee_associated     TEXT,               -- name of sponsoring employee if FSFS
    school_status           TEXT,               -- Graduate / Active / Drop / etc.
    program_group           TEXT,               -- e.g. "Online Ed"
    program_code            TEXT,               -- e.g. "CWRM01E-O"
    program_version         TEXT,
    dpa                     TEXT,               -- degree program advisor
    adm_rep                 TEXT,               -- admissions rep
    gpa                     REAL,
    vet                     TEXT,               -- veteran flag
    ar_balance              REAL,               -- accounts receivable balance
    original_start_date     TEXT,               -- ISO date
    lead_date               TEXT,               -- ISO date
    expected_start_date     TEXT,               -- ISO date
    enroll_date             TEXT,               -- ISO date
    grad_date               TEXT,               -- ISO date
    withdrawal_date         TEXT,               -- ISO date
    active_or_termed        TEXT,               -- Active / Term'ed
    term_date               TEXT,               -- ISO date if termed
    current_enrollment_info TEXT,
    gender                  TEXT,
    comments                TEXT,
    report_date             TEXT                -- ISO date from filename
);



-- Tracks every file ingested: when, what, how many rows, any errors
-- Useful for debugging and for knowing whether this week's files have been run
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_timestamp   TEXT,               -- ISO datetime of pipeline run
    filename        TEXT,               -- original filename
    target_table    TEXT,               -- which DB table it loaded into
    rows_processed  INTEGER,
    rows_inserted   INTEGER,
    rows_updated    INTEGER,
    rows_skipped    INTEGER,            -- duplicates ignored
    status          TEXT,               -- success / error
    error_message   TEXT                -- NULL on success
);