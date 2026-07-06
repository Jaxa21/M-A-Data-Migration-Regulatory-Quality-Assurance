-- macros/classify_rejection.sql
-- ─────────────────────────────────────────────────────────────
-- Reusable macro: returns the rejection rule code for a record.
-- Used in any dbt model that needs to tag records as rejected.
-- ─────────────────────────────────────────────────────────────

{% macro classify_rejection(
    loan_id_col='loan_id',
    customer_id_col='customer_id',
    loan_amount_col='loan_amount',
    currency_col='currency',
    client_age_col='client_age',
    pd_col='probability_of_default',
    interest_rate_col='interest_rate'
) %}

CASE
    WHEN {{ loan_id_col }} IS NULL
        THEN 'CHK_NULL_LOAN_ID'
    WHEN {{ customer_id_col }} IS NULL
        THEN 'CHK_NULL_CUSTOMER_ID'
    WHEN {{ loan_amount_col }} IS NULL
        THEN 'CHK_NULL_AMOUNT'
    WHEN {{ loan_amount_col }} <= 0
        THEN 'CHK_NEGATIVE_AMOUNT'
    WHEN {{ client_age_col }} < 18
        THEN 'CHK_CLIENT_AGE_MINOR'
    WHEN {{ client_age_col }} > 100
        THEN 'CHK_CLIENT_AGE_IMPLAUSIBLE'
    WHEN {{ pd_col }} NOT BETWEEN 0.0 AND 1.0
        THEN 'CHK_PD_RANGE'
    WHEN UPPER(TRIM({{ currency_col }})) NOT IN ('PLN','EUR','USD','PL','EURO','EU','US','DOLLAR','PLN','EUR','USD')
        THEN 'CHK_UNKNOWN_CURRENCY'
    WHEN {{ interest_rate_col }} < 0
        THEN 'CHK_NEGATIVE_RATE'
    ELSE NULL   -- NULL means record is clean
END

{% endmacro %}
