"""
Dead Letter Queue router.

Identifies records present in raw.raw_loans but absent from clean.clean_loans,
classifies each rejection by rule code, and writes them to quarantine.rejected_loans.

Rejection rule codes:
  CHK_NULL_LOAN_ID         - loan_id is null
  CHK_NULL_CUSTOMER_ID     - customer_id is null
  CHK_NULL_AMOUNT          - loan_amount is null
  CHK_NEGATIVE_AMOUNT      - loan_amount <= 0
  CHK_CLIENT_AGE_MINOR     - client_age < 18
  CHK_CLIENT_AGE_IMPLAUSIBLE - client_age > 100
  CHK_PD_RANGE             - probability_of_default outside [0, 1]
  CHK_UNKNOWN_CURRENCY     - currency code not mappable to ISO 4217
  CHK_NEGATIVE_RATE        - interest_rate < 0
"""

import json
import logging
import os
import uuid

import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

TARGET_DB = os.getenv(
    "TARGET_DB_CONN",
    "postgresql+psycopg2://bankuser:bankpass@target-db:5432/bank_a_dwh",
)
CHUNK_SIZE = 10_000

# Currency codes auto-mapped by dbt clean_loans.sql — not quarantined.
# Only codes outside this set go to DLQ as CHK_UNKNOWN_CURRENCY.
MAPPABLE_CURRENCIES = {
    "PLN", "EUR", "USD",
    "PL", "EURO", "EU", "US", "DOLLAR",
    "eur", "pln", "usd", "Eur", "Pln", "Usd",
}


def classify_rejection(row: dict) -> tuple[str, str]:
    """Return (rule_code, human_readable_reason) for the primary violation in row."""
    if row.get("loan_id") is None:
        return "CHK_NULL_LOAN_ID", "loan_id is null"

    if row.get("customer_id") is None:
        return "CHK_NULL_CUSTOMER_ID", "customer_id is null"

    amount = row.get("loan_amount")
    if amount is None:
        return "CHK_NULL_AMOUNT", "loan_amount is null"
    if float(amount) <= 0:
        return "CHK_NEGATIVE_AMOUNT", f"loan_amount={amount}"

    age = row.get("client_age")
    if age is not None:
        if int(age) < 18:
            return "CHK_CLIENT_AGE_MINOR", f"client_age={age}"
        if int(age) > 100:
            return "CHK_CLIENT_AGE_IMPLAUSIBLE", f"client_age={age}"

    pd_val = row.get("probability_of_default")
    if pd_val is not None and not (0.0 <= float(pd_val) <= 1.0):
        return "CHK_PD_RANGE", f"probability_of_default={pd_val}"

    currency_raw = row.get("currency")
    currency_str = str(currency_raw) if currency_raw is not None else ""
    if currency_str.strip() not in MAPPABLE_CURRENCIES and \
            currency_str.upper().strip() not in MAPPABLE_CURRENCIES:
        return "CHK_UNKNOWN_CURRENCY", f"currency='{currency_raw}'"

    rate = row.get("interest_rate")
    if rate is not None and float(rate) < 0:
        return "CHK_NEGATIVE_RATE", f"interest_rate={rate}"

    return "CHK_UNKNOWN", "record failed validation — reason undetermined"


def main(run_id: str | None = None) -> int:
    run_id = run_id or str(uuid.uuid4())[:8]
    engine = create_engine(TARGET_DB, pool_pre_ping=True)

    log.info("DLQ router starting — run_id=%s", run_id)

    find_rejected_sql = text("""
        SELECT r.*
        FROM raw.raw_loans r
        LEFT JOIN clean.clean_loans c ON r.loan_id = c.loan_id
        WHERE c.loan_id IS NULL
    """)

    total_rejected = 0
    rule_counts: dict = {}

    for chunk_df in pd.read_sql(find_rejected_sql, engine, chunksize=CHUNK_SIZE):
        dlq_records = []
        for _, row in chunk_df.iterrows():
            row_dict = row.to_dict()
            rule_code, reason = classify_rejection(row_dict)
            raw_record = {
                k: str(v) if hasattr(v, "isoformat") else v
                for k, v in row_dict.items()
            }
            dlq_records.append({
                "loan_id":                row_dict.get("loan_id"),
                "customer_id":            row_dict.get("customer_id"),
                "loan_amount":            row_dict.get("loan_amount"),
                "currency":               row_dict.get("currency"),
                "client_age":             row_dict.get("client_age"),
                "probability_of_default": row_dict.get("probability_of_default"),
                "rejection_reason":       reason,
                "rejection_rule":         rule_code,
                "pipeline_run_id":        run_id,
                "raw_record":             json.dumps(raw_record),
            })
            rule_counts[rule_code] = rule_counts.get(rule_code, 0) + 1

        pd.DataFrame(dlq_records).to_sql(
            name="rejected_loans",
            schema="quarantine",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
        )
        total_rejected += len(dlq_records)
        log.info("Processed %s records", f"{total_rejected:,}")

    if total_rejected == 0:
        log.info("No rejections — all records passed quality checks")
        return 0

    log.info("Rejection breakdown:")
    for rule, count in sorted(rule_counts.items(), key=lambda x: -x[1]):
        log.info("  %-35s %s", rule, f"{count:,}")

    return total_rejected


if __name__ == "__main__":
    main()
