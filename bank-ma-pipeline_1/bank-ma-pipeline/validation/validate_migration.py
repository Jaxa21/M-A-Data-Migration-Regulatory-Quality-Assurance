"""
validate_migration.py
─────────────────────────────────────────────────────────────────
Post-migration statistical validation.
Confirms that data transformations did NOT alter the credit risk
profile of the loan portfolio.

Tests applied:
  1. Kolmogorov-Smirnov Test (scipy.stats.ks_2samp)
     Detects any distributional shift in probability_of_default
     between source and clean target datasets.
     ALARM if: p-value < 0.05

  2. Population Stability Index (PSI)
     Industry-standard metric used by credit risk teams at Polish
     and European banks to assess model stability after data changes.
     STABLE  if: PSI < 0.10
     MONITOR if: PSI 0.10 – 0.25
     ALARM   if: PSI > 0.25

Both tests must pass for the pipeline to be cleared for
regulatory reporting.
"""

import os
import sys
import logging
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
    "postgresql+psycopg2://bankuser:bankpass@source-db:5432/bank_b_legacy"
)
TARGET_DB = os.getenv(
    "TARGET_DB_CONN",
    "postgresql+psycopg2://bankuser:bankpass@target-db:5432/bank_a_dwh"
)

# ── Thresholds ─────────────────────────────────────────────────
KS_PVALUE_THRESHOLD = 0.05    # p < 0.05 → distributions differ significantly
PSI_STABLE_THRESHOLD = 0.10   # PSI < 0.10 → stable
PSI_MONITOR_THRESHOLD = 0.25  # PSI 0.10-0.25 → monitor; > 0.25 → alarm
PSI_BUCKETS = 10              # number of decile buckets for PSI


def fetch_pd_distributions(
    source_engine, target_engine
) -> tuple[np.ndarray, np.ndarray]:
    """Fetch PD values from source (all valid records) and clean target."""

    source_sql = text("""
        SELECT probability_of_default
        FROM legacy.legacy_loans
        WHERE probability_of_default IS NOT NULL
          AND probability_of_default BETWEEN 0 AND 1
    """)

    target_sql = text("""
        SELECT probability_of_default
        FROM clean.clean_loans
        WHERE probability_of_default IS NOT NULL
    """)

    log.info("Fetching PD distribution from source (Bank B legacy)…")
    with source_engine.connect() as conn:
        source_df = pd.read_sql(source_sql, conn)
    log.info("  Source records: %s", f"{len(source_df):,}")

    log.info("Fetching PD distribution from target (Bank A DWH clean layer)…")
    with target_engine.connect() as conn:
        target_df = pd.read_sql(target_sql, conn)
    log.info("  Target records: %s", f"{len(target_df):,}")

    return (
        source_df["probability_of_default"].values,
        target_df["probability_of_default"].values,
    )


def run_ks_test(source_pd: np.ndarray, target_pd: np.ndarray) -> dict:
    """
    Kolmogorov-Smirnov two-sample test.
    H0: source and target distributions are identical.
    Reject H0 (alarm) if p-value < 0.05.
    """
    ks_stat, p_value = stats.ks_2samp(source_pd, target_pd)

    passed = bool(p_value >= KS_PVALUE_THRESHOLD)
    result = {
        "ks_statistic": round(float(ks_stat), 6),
        "p_value":       round(float(p_value), 6),
        "passed":        passed,
        "interpretation": (
            f"p={p_value:.4f} → {'PASS — no significant distributional shift' if passed else 'FAIL — distributions differ significantly'}"
        ),
    }

    log.info("─" * 60)
    log.info("Kolmogorov-Smirnov Test")
    log.info("  KS statistic : %.6f", ks_stat)
    log.info("  p-value      : %.6f  (threshold: %.2f)", p_value, KS_PVALUE_THRESHOLD)
    log.info("  Result       : %s", "✅ PASS" if passed else "⛔ FAIL")

    return result


def calculate_psi(
    source_pd: np.ndarray, target_pd: np.ndarray, buckets: int = PSI_BUCKETS
) -> dict:
    """
    Population Stability Index (PSI).
    Measures how much the distribution of a variable has shifted
    between a baseline (source) and a comparison (target) population.

    Formula: PSI = Σ (Actual% - Expected%) × ln(Actual% / Expected%)

    Interpretation:
      PSI < 0.10  → Stable — no significant population shift
      PSI 0.10-0.25 → Monitor — some shift, investigate
      PSI > 0.25  → Alarm — significant shift, do not use for reporting
    """
    # Build decile breakpoints on the source distribution
    breakpoints = np.percentile(source_pd, np.linspace(0, 100, buckets + 1))
    breakpoints[0] = -np.inf
    breakpoints[-1] = np.inf

    source_counts = np.histogram(source_pd, bins=breakpoints)[0]
    target_counts = np.histogram(target_pd, bins=breakpoints)[0]

    source_pct = source_counts / len(source_pd)
    target_pct = target_counts / len(target_pd)

    # Avoid division by zero and log(0) with small epsilon
    eps = 1e-6
    source_pct = np.where(source_pct == 0, eps, source_pct)
    target_pct = np.where(target_pct == 0, eps, target_pct)

    psi_buckets = (target_pct - source_pct) * np.log(target_pct / source_pct)
    psi_total = float(np.sum(psi_buckets))

    if psi_total < PSI_STABLE_THRESHOLD:
        status = "STABLE"
    elif psi_total < PSI_MONITOR_THRESHOLD:
        status = "MONITOR"
    else:
        status = "ALARM"

    passed = status in ("STABLE", "MONITOR")

    log.info("─" * 60)
    log.info("Population Stability Index (PSI)")
    log.info("  PSI value : %.6f", psi_total)
    log.info("  Status    : %s  (< 0.10 stable, 0.10-0.25 monitor, > 0.25 alarm)", status)
    log.info("  Result    : %s", "✅ PASS" if passed else "⛔ FAIL")

    return {
        "psi_value":     round(psi_total, 6),
        "psi_status":    status,
        "passed":        passed,
        "bucket_detail": psi_buckets.tolist(),
    }


def write_summary(
    engine,
    run_id: str,
    records_processed: int,
    records_accepted: int,
    records_rejected: int,
    ks_result: dict,
    psi_result: dict,
    overall_status: str,
):
    """Persist run summary to clean.migration_run_summary."""
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
    log.info("Run summary written to clean.migration_run_summary")


def main(run_id: str | None = None) -> int:
    """
    Returns:
        0 — all checks passed, cleared for regulatory reporting
        1 — one or more checks failed, do NOT use for reporting
    """
    run_id = run_id or str(uuid.uuid4())[:8]

    source_engine = create_engine(SOURCE_DB, pool_pre_ping=True)
    target_engine = create_engine(TARGET_DB, pool_pre_ping=True)

    log.info("=" * 60)
    log.info("Post-Migration Statistical Validation")
    log.info("Run ID: %s", run_id)
    log.info("=" * 60)

    # ── Fetch counts ───────────────────────────────────────────
    with target_engine.connect() as conn:
        records_accepted = conn.execute(
            text("SELECT COUNT(*) FROM clean.clean_loans")
        ).scalar()
        records_rejected = conn.execute(
            text("SELECT COUNT(*) FROM quarantine.rejected_loans WHERE pipeline_run_id = :rid"),
            {"rid": run_id},
        ).scalar()

    records_processed = records_accepted + records_rejected

    # ── Fetch PD distributions ─────────────────────────────────
    source_pd, target_pd = fetch_pd_distributions(source_engine, target_engine)

    if len(target_pd) == 0:
        log.error("⛔ CRITICAL: clean_loans table is empty. Pipeline may have failed.")
        sys.exit(1)

    # ── Run statistical tests ──────────────────────────────────
    ks_result = run_ks_test(source_pd, target_pd)
    psi_result = calculate_psi(source_pd, target_pd)

    # ── Overall verdict ────────────────────────────────────────
    all_passed = ks_result["passed"] and psi_result["passed"]
    overall_status = "CLEARED" if all_passed else "REVIEW_REQUIRED"

    log.info("=" * 60)
    log.info("MIGRATION RUN SUMMARY")
    log.info("  Run ID             : %s", run_id)
    log.info("  Records processed  : %s", f"{records_processed:,}")
    log.info("  Records accepted   : %s  (%.2f%%)",
             f"{records_accepted:,}", 100 * records_accepted / max(records_processed, 1))
    log.info("  Records quarantined: %s  (%.2f%%)",
             f"{records_rejected:,}", 100 * records_rejected / max(records_processed, 1))
    log.info("  KS test            : %s  (p=%.4f)", "PASS" if ks_result["passed"] else "FAIL", ks_result["p_value"])
    log.info("  PSI                : %s  (%.4f)", psi_result["psi_status"], psi_result["psi_value"])
    log.info("─" * 60)

    if all_passed:
        log.info("  ✅ OVERALL STATUS: %s — cleared for regulatory reporting", overall_status)
    else:
        log.error("  ⛔ OVERALL STATUS: %s", overall_status)
        if not ks_result["passed"]:
            log.error("  ALARM: Migration has statistically altered the PD distribution!")
            log.error("  KS p-value %.4f is below threshold %.2f", ks_result["p_value"], KS_PVALUE_THRESHOLD)
        if not psi_result["passed"]:
            log.error("  ALARM: PSI=%.4f exceeds ALARM threshold (0.25)!", psi_result["psi_value"])
            log.error("  Credit risk models may be compromised. Manual review required.")

    log.info("=" * 60)

    # Persist summary
    write_summary(
        target_engine, run_id, records_processed, records_accepted,
        records_rejected, ks_result, psi_result, overall_status
    )

    return 0 if all_passed else 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
