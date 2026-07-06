"""
route_to_dlq.py
─────────────────────────────────────────────────────────────────
Dead Letter Queue router — runs after dbt to find records that
were in raw_loans but did NOT make it into clean_loans, then
writes them to quarantine.rejected_loans with a rejection reason.

This is the "Dead Letter Queue" pattern:
  - Bad records are NEVER silently dropped
  - Every rejection has a reason code (for ops team review)
  - The full original record is preserved as JSONB for debugging
  - Pipeline never crashes due to bad data
"""

import os
import json
import logging
import uuid
from datetime import datetime

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
    "postgresql+psycopg2://bankuser:bankpass@target-db:5432/bank_a_dwh"
)
CHUNK_SIZE = 10_000


def classify_rejection(row: dict) -> tuple[str, str]:
    """
    Determine the primary rejection reason for a record.
    Returns (reason_code, human_readable_reason).
    Multiple violations: report the first one found (priority order).
    """
    if row.get("loan_id") is None:
        return "CHK_NULL_LOAN_ID", "loan_id is NULL — cannot identify record"

    if row.get("customer_id") is None:
        return "CHK_NULL_CUSTOMER_ID", "customer_id is NULL — cannot identify customer"

    amount = row.get("loan_amount")
    if amount is None:
        return "CHK_NULL_AMOUNT", "loan_amount is NULL"
    if float(amount) <= 0:
        return "CHK_NEGATIVE_AMOUNT", f"loan_amount is {amount} — must be > 0"

    age = row.get("client_age")
    if age is not None:
        if int(age) < 18:
            return "CHK_CLIENT_AGE_MINOR", f"client_age is {age} — below legal minimum (18)"
        if int(age) > 100:
            return "CHK_CLIENT_AGE_IMPLAUSIBLE", f"client_age is {age} — exceeds maximum (100)"

    pd_val = row.get("probability_of_default")
    if pd_val is not None and (float(pd_val) < 0 or float(pd_val) > 1):
        return "CHK_PD_RANGE", f"probability_of_default is {pd_val} — must be in [0.0, 1.0]"

    currency_raw = row.get("currency")
    currency_str = str(currency_raw) if currency_raw is not None else ""
    currency_upper = currency_str.upper().strip()

    # Aligned with clean_loans.sql: these aliases are auto-mapped by dbt
    # (e.g. 'eur' -> 'EUR', 'PL' -> 'PLN') and logged as transformations.
    # Only truly unrecognisable codes go to DLQ.
    MAPPABLE_ALIASES = {"PL", "EURO", "EU", "US", "DOLLAR",
                        "PLN", "EUR", "USD",
                        "eur", "pln", "usd", "Eur", "Pln", "Usd"}
    if currency_str.strip() not in MAPPABLE_ALIASES and currency_upper not in MAPPABLE_ALIASES:
        return "CHK_UNKNOWN_CURRENCY", f"currency '{currency_raw}' is not a recognised code — cannot map to ISO 4217"

    interest = row.get("interest_rate")
    if interest is not None and float(interest) < 0:
        return "CHK_NEGATIVE_RATE", f"interest_rate is {interest} — must be >= 0"

    return "CHK_UNKNOWN", "Record failed validation — reason undetermined"


def main(run_id: str | None = None):
    run_id = run_id or str(uuid.uuid4())[:8]
    engine = create_engine(TARGET_DB, pool_pre_ping=True)

    log.info("DLQ Router starting — run_id: %s", run_id)

    # ── Find rejected records: in raw but not in clean ─────────
    find_rejected_sql = text("""
        SELECT
            r.loan_id,
            r.customer_id,
            r.loan_amount,
            r.currency,
            r.interest_rate,
            r.wibor_rate,
            r.client_age,
            r.probability_of_default,
            r.loan_start_date,
            r.loan_end_date,
            r.loan_status,
            r.branch_code,
            r.product_type,
            r.source_system,
            r.pipeline_run_id
        FROM raw.raw_loans r
        LEFT JOIN clean.clean_loans c ON r.loan_id = c.loan_id
        WHERE c.loan_id IS NULL
    """)

    log.info("Querying for rejected records (raw - clean) in chunks…")

    total_rejected = 0
    rule_counts: dict = {}

    # ── Process in chunks — never load full DLQ into memory ────
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

        dlq_df = pd.DataFrame(dlq_records)
        dlq_df.to_sql(
            name="rejected_loans",
            schema="quarantine",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
        )
        total_rejected += len(dlq_df)
        log.info("  DLQ: processed %s records so far…", f"{total_rejected:,}")

    if total_rejected == 0:
        log.info("No rejections — all records passed quality checks.")
        return 0

    # ── Rejection breakdown by rule ────────────────────────────
    log.info("─" * 60)
    log.info("Rejection breakdown by rule:")
    for rule, count in sorted(rule_counts.items(), key=lambda x: -x[1]):
        log.info("  %-35s  %s", rule, f"{count:,}")
    log.info("─" * 60)

    return total_rejected


if __name__ == "__main__":
    main()
