-- models/raw/raw_loans.sql
-- ─────────────────────────────────────────────────────────────
-- RAW LAYER: Direct copy from Bank B legacy source.
-- NO transformations. NO filters. NO business logic.
--
-- BCBS 239 Principle 2 requirement: every source record must be
-- preserved verbatim in the raw layer to support full data lineage.
-- Auditors can always trace any clean record back to its raw origin.
-- ─────────────────────────────────────────────────────────────

{{
  config(
    materialized = 'table',
    schema = 'raw',
    alias = 'raw_loans',
    tags = ['raw', 'bcbs239_lineage', 'daily']
  )
}}

SELECT
    loan_id,
    customer_id,
    loan_amount,
    currency,
    interest_rate,
    wibor_rate,
    client_age,
    probability_of_default,
    loan_start_date,
    loan_end_date,
    loan_status,
    branch_code,
    product_type,
    source_system,
    NOW()                        AS ingested_at,
    '{{ run_started_at }}'       AS pipeline_run_id

FROM {{ source('bank_b_legacy', 'legacy_loans') }}
