-- models/clean/clean_loans.sql
-- ─────────────────────────────────────────────────────────────
-- CLEAN LAYER: Transformation + validation.
-- Only records passing ALL 6 quality rules enter this table.
-- Rejected records are written to quarantine.rejected_loans
-- by the validate_and_route.py script run after dbt.
--
-- Transformations applied:
--   1. Currency standardisation (upper-case, known aliases mapped)
--   2. Amount normalisation to PLN using fixed FX rates
--   3. Margin rate derivation (interest_rate - wibor_rate)
--   4. PD banding: LOW / MEDIUM / HIGH / CRITICAL
--   5. Loan duration in months (derived)
-- ─────────────────────────────────────────────────────────────

{{
  config(
    materialized = 'table',
    schema = 'clean',
    alias = 'clean_loans',
    tags = ['clean', 'regulatory_reporting', 'daily']
  )
}}

WITH

-- Step 1: Pull from raw layer
source AS (
    SELECT * FROM {{ ref('raw_loans') }}
),

-- Step 2: Standardise currency codes
-- Maps known legacy typos → canonical ISO 4217 codes
currency_fixed AS (
    SELECT
        *,
        CASE
            WHEN UPPER(TRIM(currency)) IN ('PLN', 'PL')              THEN 'PLN'
            WHEN UPPER(TRIM(currency)) IN ('EUR', 'EURO', 'EU')      THEN 'EUR'
            WHEN UPPER(TRIM(currency)) IN ('USD', 'US', 'DOLLAR')    THEN 'USD'
            ELSE UPPER(TRIM(currency))   -- keep as-is; will fail QA test
        END AS currency_standardised
    FROM source
),

-- Step 3: Apply quality filters — only clean records proceed
validated AS (
    SELECT *
    FROM currency_fixed
    WHERE
        -- Rule 1: Amount must be positive and not null
        loan_amount IS NOT NULL
        AND loan_amount > 0

        -- Rule 2: Currency must be a known valid code
        AND currency_standardised IN ('PLN', 'EUR', 'USD')

        -- Rule 3: Client must be a legal adult and plausibly alive
        AND client_age BETWEEN 18 AND 100

        -- Rule 4: PD must be a valid probability
        AND probability_of_default BETWEEN 0.0 AND 1.0

        -- Rule 5: Core identifiers must not be null
        AND loan_id IS NOT NULL
        AND customer_id IS NOT NULL

        -- Rule 6: Interest rate must be non-negative
        AND interest_rate >= 0
),

-- Step 4: Apply business transformations
transformed AS (
    SELECT
        loan_id,
        customer_id,

        -- Normalise amount to PLN
        ROUND(
            CASE currency_standardised
                WHEN 'EUR' THEN loan_amount * {{ var('eur_to_pln') }}
                WHEN 'USD' THEN loan_amount * {{ var('usd_to_pln') }}
                ELSE loan_amount
            END,
            2
        )                                           AS loan_amount_pln,

        currency                                    AS currency_original,
        currency_standardised,
        interest_rate,
        wibor_rate,

        -- Margin = spread over WIBOR (credit pricing component)
        ROUND(interest_rate - COALESCE(wibor_rate, 0), 4)  AS margin_rate,

        client_age,
        probability_of_default,

        -- PD banding for risk segmentation (EBA standard buckets)
        CASE
            WHEN probability_of_default < 0.05  THEN 'LOW'
            WHEN probability_of_default < 0.20  THEN 'MEDIUM'
            WHEN probability_of_default < 0.50  THEN 'HIGH'
            ELSE                                     'CRITICAL'
        END                                         AS pd_band,

        loan_start_date,
        loan_end_date,

        -- Loan duration in months (for vintage analysis)
        EXTRACT(YEAR FROM AGE(loan_end_date, loan_start_date)) * 12
        + EXTRACT(MONTH FROM AGE(loan_end_date, loan_start_date))
                                                    AS loan_duration_months,

        loan_status,
        branch_code,
        product_type,
        source_system,
        pipeline_run_id,
        NOW()                                       AS loaded_at

    FROM validated
)

SELECT * FROM transformed
