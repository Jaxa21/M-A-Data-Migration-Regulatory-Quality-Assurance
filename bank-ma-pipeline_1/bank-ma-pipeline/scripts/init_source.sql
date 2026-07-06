-- ─────────────────────────────────────────────────────────────
--  Bank B Legacy System — Source Database Schema
--  This schema mirrors a real legacy core banking system.
--  Data quality issues are intentional (seeded by generator).
-- ─────────────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS legacy;

CREATE TABLE IF NOT EXISTS legacy.legacy_loans (
    loan_id          VARCHAR(36)     NOT NULL,
    customer_id      VARCHAR(36)     NOT NULL,
    loan_amount      NUMERIC(18, 2),          -- intentionally nullable; some rows will be NULL
    currency         VARCHAR(10),             -- intentionally wide; will contain typos
    interest_rate    NUMERIC(8, 4),
    wibor_rate       NUMERIC(8, 4),
    client_age       INTEGER,                 -- will contain values <18 and >100
    probability_of_default NUMERIC(6, 4),    -- PD: 0.0000 to 1.0000
    loan_start_date  DATE,
    loan_end_date    DATE,
    loan_status      VARCHAR(20),             -- ACTIVE, CLOSED, DEFAULT, RESTRUCTURED
    branch_code      VARCHAR(10),
    product_type     VARCHAR(30),             -- MORTGAGE, CONSUMER, SME, AUTO
    created_at       TIMESTAMP DEFAULT NOW(),
    source_system    VARCHAR(50) DEFAULT 'BANK_B_LEGACY_V2'
);

-- Index for faster dbt model queries
CREATE INDEX IF NOT EXISTS idx_legacy_loans_customer ON legacy.legacy_loans(customer_id);
CREATE INDEX IF NOT EXISTS idx_legacy_loans_status ON legacy.legacy_loans(loan_status);
CREATE INDEX IF NOT EXISTS idx_legacy_loans_currency ON legacy.legacy_loans(currency);

-- Audit log table — required for BCBS 239 data lineage
CREATE TABLE IF NOT EXISTS legacy.pipeline_audit_log (
    log_id          SERIAL PRIMARY KEY,
    run_id          VARCHAR(50),
    event_type      VARCHAR(30),   -- RUN_START, RUN_END, ERROR
    table_name      VARCHAR(50),
    records_count   INTEGER,
    message         TEXT,
    logged_at       TIMESTAMP DEFAULT NOW()
);
