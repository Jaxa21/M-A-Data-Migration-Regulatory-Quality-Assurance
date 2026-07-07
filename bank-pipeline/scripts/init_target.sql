-- Target database schema: Bank A Data Warehouse.
--
-- Three-layer architecture:
--   raw        — verbatim copy from source (BCBS 239 lineage)
--   clean      — validated, transformed records for regulatory reporting
--   quarantine — Dead Letter Queue for rejected records

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS clean;
CREATE SCHEMA IF NOT EXISTS quarantine;

CREATE TABLE IF NOT EXISTS raw.raw_loans (
    loan_id                  VARCHAR(36),
    customer_id              VARCHAR(36),
    loan_amount              NUMERIC(18, 2),
    currency                 VARCHAR(10),
    interest_rate            NUMERIC(8, 4),
    wibor_rate               NUMERIC(8, 4),
    client_age               INTEGER,
    probability_of_default   NUMERIC(6, 4),
    loan_start_date          DATE,
    loan_end_date            DATE,
    loan_status              VARCHAR(20),
    branch_code              VARCHAR(10),
    product_type             VARCHAR(30),
    source_system            VARCHAR(50),
    ingested_at              TIMESTAMP DEFAULT NOW(),
    pipeline_run_id          VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS clean.clean_loans (
    loan_id                  VARCHAR(36)    NOT NULL,
    customer_id              VARCHAR(36)    NOT NULL,
    loan_amount_pln          NUMERIC(18, 2) NOT NULL,
    currency_original        VARCHAR(10),
    currency_standardised    VARCHAR(3)     NOT NULL,
    interest_rate            NUMERIC(8, 4)  NOT NULL,
    wibor_rate               NUMERIC(8, 4),
    margin_rate              NUMERIC(8, 4),
    client_age               INTEGER        NOT NULL,
    probability_of_default   NUMERIC(6, 4)  NOT NULL,
    pd_band                  VARCHAR(10),
    loan_start_date          DATE,
    loan_end_date            DATE,
    loan_duration_months     NUMERIC(6, 1),
    loan_status              VARCHAR(20),
    branch_code              VARCHAR(10),
    product_type             VARCHAR(30),
    source_system            VARCHAR(50),
    pipeline_run_id          VARCHAR(50),
    loaded_at                TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_clean_loans_customer ON clean.clean_loans(customer_id);
CREATE INDEX IF NOT EXISTS idx_clean_loans_pd_band  ON clean.clean_loans(pd_band);
CREATE INDEX IF NOT EXISTS idx_clean_loans_status   ON clean.clean_loans(loan_status);

CREATE TABLE IF NOT EXISTS quarantine.rejected_loans (
    rejection_id             SERIAL PRIMARY KEY,
    loan_id                  VARCHAR(36),
    customer_id              VARCHAR(36),
    loan_amount              NUMERIC(18, 2),
    currency                 VARCHAR(10),
    client_age               INTEGER,
    probability_of_default   NUMERIC(6, 4),
    rejection_reason         VARCHAR(200) NOT NULL,
    rejection_rule           VARCHAR(50)  NOT NULL,
    pipeline_run_id          VARCHAR(50),
    raw_record               JSONB,
    rejected_at              TIMESTAMP DEFAULT NOW()
);

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
    psi_status           VARCHAR(20),
    overall_status       VARCHAR(20),
    notes                TEXT
);
