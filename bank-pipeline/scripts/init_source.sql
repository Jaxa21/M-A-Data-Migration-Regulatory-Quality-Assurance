-- Source database schema: Bank B legacy core banking system.

CREATE SCHEMA IF NOT EXISTS legacy;

CREATE TABLE IF NOT EXISTS legacy.legacy_loans (
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
    created_at               TIMESTAMP DEFAULT NOW(),
    source_system            VARCHAR(50) DEFAULT 'BANK_B_LEGACY_V2'
);

CREATE INDEX IF NOT EXISTS idx_legacy_loans_customer ON legacy.legacy_loans(customer_id);
CREATE INDEX IF NOT EXISTS idx_legacy_loans_status   ON legacy.legacy_loans(loan_status);
CREATE INDEX IF NOT EXISTS idx_legacy_loans_currency ON legacy.legacy_loans(currency);

CREATE TABLE IF NOT EXISTS legacy.pipeline_audit_log (
    log_id        SERIAL PRIMARY KEY,
    run_id        VARCHAR(50),
    event_type    VARCHAR(30),
    table_name    VARCHAR(50),
    records_count INTEGER,
    message       TEXT,
    logged_at     TIMESTAMP DEFAULT NOW()
);
