-- ─────────────────────────────────────────────────────────────
--  Bank A Data Warehouse — Target Database Schema
--  Three-layer architecture: raw → clean → rejected (DLQ)
-- ─────────────────────────────────────────────────────────────

-- Layer 1: Raw ingestion (exact copy from source, untouched)
CREATE SCHEMA IF NOT EXISTS raw;

-- Layer 2: Clean, validated data ready for risk reporting
CREATE SCHEMA IF NOT EXISTS clean;

-- Layer 3: Dead Letter Queue — quarantine for bad records
CREATE SCHEMA IF NOT EXISTS quarantine;

-- ── RAW LAYER ────────────────────────────────────────────────
-- Exact mirror of source — no transformations, no filters.
-- Required for BCBS 239 data lineage (you can always trace back to raw).

CREATE TABLE IF NOT EXISTS raw.raw_loans (
    loan_id          VARCHAR(36),
    customer_id      VARCHAR(36),
    loan_amount      NUMERIC(18, 2),
    currency         VARCHAR(10),
    interest_rate    NUMERIC(8, 4),
    wibor_rate       NUMERIC(8, 4),
    client_age       INTEGER,
    probability_of_default NUMERIC(6, 4),
    loan_start_date  DATE,
    loan_end_date    DATE,
    loan_status      VARCHAR(20),
    branch_code      VARCHAR(10),
    product_type     VARCHAR(30),
    source_system    VARCHAR(50),
    ingested_at      TIMESTAMP DEFAULT NOW(),
    pipeline_run_id  VARCHAR(50)
);

-- ── CLEAN LAYER ───────────────────────────────────────────────
-- Transformed, standardised, validated records.
-- This is what risk models and regulatory reports consume.

CREATE TABLE IF NOT EXISTS clean.clean_loans (
    loan_id                  VARCHAR(36)   NOT NULL,
    customer_id              VARCHAR(36)   NOT NULL,
    loan_amount_pln          NUMERIC(18, 2) NOT NULL,   -- always normalised to PLN
    currency_original        VARCHAR(10),               -- preserved for audit trail
    currency_standardised    VARCHAR(3)    NOT NULL,    -- PLN / EUR / USD
    interest_rate            NUMERIC(8, 4) NOT NULL,
    wibor_rate               NUMERIC(8, 4),
    margin_rate              NUMERIC(8, 4),             -- derived: interest_rate - wibor_rate
    client_age               INTEGER       NOT NULL,
    probability_of_default   NUMERIC(6, 4) NOT NULL,
    pd_band                  VARCHAR(10),               -- LOW / MEDIUM / HIGH / CRITICAL
    loan_start_date          DATE,
    loan_end_date            DATE,
    loan_status              VARCHAR(20),
    branch_code              VARCHAR(10),
    product_type             VARCHAR(30),
    source_system            VARCHAR(50),
    pipeline_run_id          VARCHAR(50),
    loaded_at                TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_clean_loans_customer ON clean.clean_loans(customer_id);
CREATE INDEX IF NOT EXISTS idx_clean_loans_pd_band ON clean.clean_loans(pd_band);
CREATE INDEX IF NOT EXISTS idx_clean_loans_status ON clean.clean_loans(loan_status);

-- ── DEAD LETTER QUEUE (QUARANTINE) ───────────────────────────
-- Every rejected record lands here with a reason code.
-- Operations team reviews this table — records are never silently dropped.

CREATE TABLE IF NOT EXISTS quarantine.rejected_loans (
    rejection_id             SERIAL PRIMARY KEY,
    loan_id                  VARCHAR(36),
    customer_id              VARCHAR(36),
    loan_amount              NUMERIC(18, 2),
    currency                 VARCHAR(10),
    client_age               INTEGER,
    probability_of_default   NUMERIC(6, 4),
    rejection_reason         VARCHAR(200)  NOT NULL,   -- human-readable reason
    rejection_rule           VARCHAR(50)   NOT NULL,   -- e.g. CHK_NEGATIVE_AMOUNT
    pipeline_run_id          VARCHAR(50),
    raw_record               JSONB,                    -- full original record preserved
    rejected_at              TIMESTAMP DEFAULT NOW()
);

-- ── PIPELINE REPORTING ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS clean.migration_run_summary (
    run_id               VARCHAR(50) PRIMARY KEY,
    run_date             TIMESTAMP DEFAULT NOW(),
    records_processed    INTEGER,
    records_accepted     INTEGER,
    records_rejected     INTEGER,
    rejection_rate_pct   NUMERIC(6, 3),
    ks_test_pvalue       NUMERIC(10, 6),
    ks_test_passed       BOOLEAN,
    psi_value            NUMERIC(10, 6),
    psi_status           VARCHAR(20),    -- STABLE / MONITOR / ALARM
    overall_status       VARCHAR(20),    -- CLEARED / REVIEW_REQUIRED / BLOCKED
    notes                TEXT
);
