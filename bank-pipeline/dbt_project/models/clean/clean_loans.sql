-- clean_loans: validated and standardised loan records.
--
-- Only records passing all quality rules appear here.
-- Records failing any rule are routed to quarantine.rejected_loans
-- by route_to_dlq.py after this model runs.
--
-- Transformations:
--   - Currency aliases normalised to ISO 4217 (eur -> EUR, PL -> PLN)
--   - Loan amount converted to PLN using fixed FX rates
--   - Margin rate derived (interest_rate - wibor_rate)
--   - PD band assigned (LOW / MEDIUM / HIGH / CRITICAL)

{{
  config(
    materialized = 'table',
    schema = 'clean',
    alias = 'clean_loans',
    tags = ['clean', 'regulatory', 'daily']
  )
}}

WITH source AS (
    SELECT * FROM {{ ref('raw_loans') }}
),

currency_normalised AS (
    SELECT
        *,
        CASE
            WHEN UPPER(TRIM(currency)) IN ('PLN', 'PL')           THEN 'PLN'
            WHEN UPPER(TRIM(currency)) IN ('EUR', 'EURO', 'EU')   THEN 'EUR'
            WHEN UPPER(TRIM(currency)) IN ('USD', 'US', 'DOLLAR') THEN 'USD'
            ELSE UPPER(TRIM(currency))
        END AS currency_standardised
    FROM source
),

validated AS (
    SELECT *
    FROM currency_normalised
    WHERE
        loan_amount        IS NOT NULL
        AND loan_amount    > 0
        AND currency_standardised IN ('PLN', 'EUR', 'USD')
        AND client_age     BETWEEN 18 AND 100
        AND probability_of_default BETWEEN 0.0 AND 1.0
        AND loan_id        IS NOT NULL
        AND customer_id    IS NOT NULL
        AND interest_rate  >= 0
),

transformed AS (
    SELECT
        loan_id,
        customer_id,

        ROUND(
            CASE currency_standardised
                WHEN 'EUR' THEN loan_amount * {{ var('eur_to_pln') }}
                WHEN 'USD' THEN loan_amount * {{ var('usd_to_pln') }}
                ELSE loan_amount
            END,
            2
        )                                               AS loan_amount_pln,

        currency                                        AS currency_original,
        currency_standardised,
        interest_rate,
        wibor_rate,
        ROUND(interest_rate - COALESCE(wibor_rate, 0), 4) AS margin_rate,
        client_age,
        probability_of_default,

        CASE
            WHEN probability_of_default < 0.05 THEN 'LOW'
            WHEN probability_of_default < 0.20 THEN 'MEDIUM'
            WHEN probability_of_default < 0.50 THEN 'HIGH'
            ELSE                                    'CRITICAL'
        END                                             AS pd_band,

        loan_start_date,
        loan_end_date,
        EXTRACT(YEAR  FROM AGE(loan_end_date, loan_start_date)) * 12
        + EXTRACT(MONTH FROM AGE(loan_end_date, loan_start_date))
                                                        AS loan_duration_months,
        loan_status,
        branch_code,
        product_type,
        source_system,
        pipeline_run_id,
        NOW()                                           AS loaded_at

    FROM validated
)

SELECT * FROM transformed
