-- raw_loans: verbatim copy of Bank B legacy loans.
--
-- No transformations. Required for BCBS 239 data lineage —
-- every clean record must be traceable to an unmodified source row.

{{
  config(
    materialized = 'table',
    schema = 'raw',
    alias = 'raw_loans',
    tags = ['raw', 'bcbs239', 'daily']
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
    NOW()                   AS ingested_at,
    '{{ run_started_at }}'  AS pipeline_run_id

FROM {{ source('bank_b_legacy', 'legacy_loans') }}
