"""
Post-migration statistical validation.

Verifies that data transformations have not altered the credit risk profile
of the loan portfolio. Two tests are applied to probability_of_default:

  Kolmogorov-Smirnov test  — detects distributional shift (threshold: p < 0.05)
  Population Stability Index — industry-standard scorecard stability metric
                               STABLE   PSI < 0.10
                               MONITOR  PSI 0.10 – 0.25
                               ALARM    PSI > 0.25

Returns exit code 0 if both tests pass, 1 otherwise.
"""

import logging
import os
import sys
import uuid
from datetime import datetime

import numpy as np
import pandas as pd
from scipy import stats
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

SOURCE_DB = os.getenv(
    "SOURCE_DB_CONN",
    "postgresql+psycopg2://bankuser:bankpass@source-db:5432/bank_b_legacy",
)
TARGET_DB = os.getenv(
    "TARGET_DB_CONN",
    "postgresql+psycopg2://bankuser:bankpass@target-db:5432/bank_a_dwh",
)

KS_PVALUE_THRESHOLD = 0.05
PSI_STABLE_THRESHOLD = 0.10
PSI_MONITOR_THRESHOLD = 0.25
PSI_BUCKETS = 10


def fetch_pd_distributions(source_engine, target_engine) -> tuple[np.ndarray, np.ndarray]:
    source_sql = text("""
        SELECT probability_of_default
        FROM legacy.legacy_loans
        WHERE probability_of_default BETWEEN 0 AND 1
    """)
    target_sql = text("""
        SELECT probability_of_default
        FROM clean.clean_loans
    """)

    with source_engine.connect() as conn:
        source = pd.read_sql(source_sql, conn)
    with target_engine.connect() as conn:
        target = pd.read_sql(target_sql, conn)

    log.info("Source PD records: %s", f"{len(source):,}")
    log.info("Target PD records: %s", f"{len(target):,}")

    return source["probability_of_default"].values, target["probability_of_default"].values


def run_ks_test(source_pd: np.ndarray, target_pd: np.ndarray) -> dict:
    ks_stat, p_value = stats.ks_2samp(source_pd, target_pd)
    passed = bool(p_value >= KS_PVALUE_THRESHOLD)

    log.info("KS test  — statistic=%.6f  p=%.6f  %s",
             ks_stat, p_value, "PASS" if passed else "FAIL")

    return {
        "ks_statistic":   round(float(ks_stat), 6),
        "p_value":        round(float(p_value), 6),
        "passed":         passed,
        "interpretation": f"p={p_value:.4f} — {'no significant shift' if passed else 'distributions differ'}",
    }


def calculate_psi(source_pd: np.ndarray, target_pd: np.ndarray,
                  buckets: int = PSI_BUCKETS) -> dict:
    breakpoints = np.percentile(source_pd, np.linspace(0, 100, buckets + 1))
    breakpoints[0] = -np.inf
    breakpoints[-1] = np.inf

    source_pct = np.histogram(source_pd, bins=breakpoints)[0] / len(source_pd)
    target_pct = np.histogram(target_pd, bins=breakpoints)[0] / len(target_pd)

    eps = 1e-6
    source_pct = np.where(source_pct == 0, eps, source_pct)
    target_pct = np.where(target_pct == 0, eps, target_pct)

    psi_total = float(np.sum((target_pct - source_pct) * np.log(target_pct / source_pct)))

    if psi_total < PSI_STABLE_THRESHOLD:
        status = "STABLE"
    elif psi_total < PSI_MONITOR_THRESHOLD:
        status = "MONITOR"
    else:
        status = "ALARM"

    log.info("PSI      — value=%.6f  status=%s", psi_total, status)

    return {
        "psi_value":    round(psi_total, 6),
        "psi_status":   status,
        "passed":       status in ("STABLE", "MONITOR"),
        "bucket_detail": (target_pct - source_pct).tolist(),
    }


def write_summary(engine, run_id, records_processed, records_accepted,
                  records_rejected, ks_result, psi_result, overall_status):
    summary = {
        "run_id":             run_id,
        "run_date":           datetime.utcnow(),
        "records_processed":  records_processed,
        "records_accepted":   records_accepted,
        "records_rejected":   records_rejected,
        "rejection_rate_pct": round(100 * records_rejected / max(records_processed, 1), 3),
        "ks_test_pvalue":     ks_result["p_value"],
        "ks_test_passed":     ks_result["passed"],
        "psi_value":          psi_result["psi_value"],
        "psi_status":         psi_result["psi_status"],
        "overall_status":     overall_status,
        "notes":              f"KS: {ks_result['interpretation']} | PSI: {psi_result['psi_status']}",
    }
    pd.DataFrame([summary]).to_sql(
        name="migration_run_summary",
        schema="clean",
        con=engine,
        if_exists="append",
        index=False,
    )


def main(run_id: str | None = None) -> int:
    run_id = run_id or str(uuid.uuid4())[:8]
    source_engine = create_engine(SOURCE_DB, pool_pre_ping=True)
    target_engine = create_engine(TARGET_DB, pool_pre_ping=True)

    log.info("Statistical validation starting — run_id=%s", run_id)

    with target_engine.connect() as conn:
        records_accepted = conn.execute(
            text("SELECT COUNT(*) FROM clean.clean_loans")
        ).scalar()
        records_rejected = conn.execute(
            text("SELECT COUNT(*) FROM quarantine.rejected_loans WHERE pipeline_run_id = :rid"),
            {"rid": run_id},
        ).scalar()

    records_processed = records_accepted + records_rejected

    source_pd, target_pd = fetch_pd_distributions(source_engine, target_engine)

    if len(target_pd) == 0:
        log.error("clean_loans is empty — pipeline may have failed")
        sys.exit(1)

    ks_result = run_ks_test(source_pd, target_pd)
    psi_result = calculate_psi(source_pd, target_pd)

    all_passed = ks_result["passed"] and psi_result["passed"]
    overall_status = "CLEARED" if all_passed else "REVIEW_REQUIRED"

    log.info("Run summary — run_id=%s", run_id)
    log.info("  Processed   : %s", f"{records_processed:,}")
    log.info("  Accepted    : %s (%.2f%%)", f"{records_accepted:,}",
             100 * records_accepted / max(records_processed, 1))
    log.info("  Quarantined : %s (%.2f%%)", f"{records_rejected:,}",
             100 * records_rejected / max(records_processed, 1))
    log.info("  Status      : %s", overall_status)

    if not all_passed:
        log.error("ALARM: migration has altered the credit risk profile")
        log.error("Do not use this dataset for regulatory reporting")

    write_summary(target_engine, run_id, records_processed, records_accepted,
                  records_rejected, ks_result, psi_result, overall_status)

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
