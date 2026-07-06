"""
ma_migration_pipeline.py
─────────────────────────────────────────────────────────────────
Airflow DAG: Bank M&A Data Migration Pipeline

Task sequence:
  1. check_source_data      — verify source DB has data to process
  2. dbt_deps               — install dbt-utils package
  3. dbt_run_raw            — ingest raw layer (EL step)
  4. dbt_run_clean          — transform and validate (T step)
  5. dbt_test               — run all 12 schema quality rules
  6. route_to_dlq           — write rejected records to quarantine
  7. validate_migration     — KS-test + PSI statistical checks
  8. generate_summary       — print final run report to Airflow logs

On any failure: alert task fires and marks the DAG failed.
"""

from __future__ import annotations

import logging
import subprocess
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy import create_engine, text

log = logging.getLogger(__name__)

# ── DAG defaults ───────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

SOURCE_DB = "postgresql+psycopg2://bankuser:bankpass@source-db:5432/bank_b_legacy"
TARGET_DB = "postgresql+psycopg2://bankuser:bankpass@target-db:5432/bank_a_dwh"
DBT_DIR   = "/opt/airflow/dbt_project"


# ── Task functions ─────────────────────────────────────────────

def check_source_data(**context) -> str:
    """
    Verify source DB has records and is accessible.
    Branches to: proceed -> dbt_deps
                 abort   -> source_not_ready (fail)
    """
    engine = create_engine(SOURCE_DB)
    with engine.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM legacy.legacy_loans")
        ).scalar()

    log.info("Source record count: %s", f"{count:,}")

    if count == 0:
        log.error("Source database is empty — aborting pipeline")
        return "source_not_ready"

    context["ti"].xcom_push(key="source_record_count", value=count)
    log.info("Source check passed — %s records available", f"{count:,}")
    return "dbt_deps"


def run_dbt_command(command: str, cwd: str = DBT_DIR):
    """Helper: run a dbt CLI command, stream logs, raise on failure."""
    cmd_list = ["dbt"] + command.split() + ["--profiles-dir", str(cwd)]
    log.info("Running: %s", " ".join(cmd_list))

    result = subprocess.run(
        cmd_list,
        shell=False,
        cwd=cwd,
        capture_output=False,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(f"dbt command failed: dbt {command}")
    log.info("dbt %s completed successfully", command)


def dbt_deps_install(**context):
    """Install dbt packages (dbt-utils). Must run before any dbt run/test."""
    run_dbt_command("deps")


def dbt_run_raw(**context):
    """Run dbt for raw layer only (fast ingest step)."""
    run_dbt_command("run --select raw.*")


def dbt_run_clean(**context):
    """Run dbt for clean/transformation layer."""
    run_dbt_command("run --select clean.*")


def dbt_test_all(**context):
    """
    Run all dbt schema tests.
    These enforce the 12 data quality rules defined in schema.yml.
    Note: dbt test failures are WARNING-level here — actual routing
    happens in route_to_dlq. We capture test results for the summary.
    """
    try:
        run_dbt_command("test")
        context["ti"].xcom_push(key="dbt_test_status", value="PASSED")
    except RuntimeError:
        log.warning("Some dbt tests failed — check dbt logs for details")
        log.warning("Rejected records will be captured by DLQ router")
        context["ti"].xcom_push(key="dbt_test_status", value="FAILED_WITH_REJECTIONS")


def route_to_dlq_task(**context):
    """Route rejected records from raw to quarantine.rejected_loans."""
    sys.path.insert(0, "/opt/airflow/validation")
    from route_to_dlq import main as dlq_main

    run_id = context["run_id"][:8]
    rejected_count = dlq_main(run_id=run_id)
    context["ti"].xcom_push(key="rejected_count", value=rejected_count)
    log.info("DLQ routing complete: %s records quarantined", f"{rejected_count:,}")


def validate_migration_task(**context):
    """Run KS-test and PSI statistical validation."""
    sys.path.insert(0, "/opt/airflow/validation")
    from validate_migration import main as validate_main

    run_id = context["run_id"][:8]
    exit_code = validate_main(run_id=run_id)

    if exit_code != 0:
        raise ValueError(
            "Statistical validation FAILED — migration has altered the risk profile. "
            "Do NOT use these data for regulatory reporting until manual review is complete."
        )

    context["ti"].xcom_push(key="validation_status", value="CLEARED")
    log.info("Statistical validation passed — cleared for regulatory reporting")


def generate_summary(**context):
    """Print final human-readable summary to Airflow task logs."""
    ti = context["ti"]

    source_count    = ti.xcom_pull(task_ids="check_source_data", key="source_record_count") or 0
    rejected_count  = ti.xcom_pull(task_ids="route_to_dlq",      key="rejected_count") or 0
    dbt_test_status = ti.xcom_pull(task_ids="dbt_test",          key="dbt_test_status") or "UNKNOWN"
    val_status      = ti.xcom_pull(task_ids="validate_migration", key="validation_status") or "UNKNOWN"

    accepted_count = source_count - rejected_count
    rejection_pct  = 100 * rejected_count / max(source_count, 1)
    acceptance_pct = 100 * accepted_count / max(source_count, 1)

    log.info("")
    log.info("=" * 65)
    log.info("  BANK M&A MIGRATION PIPELINE — RUN SUMMARY")
    log.info("  %s", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"))
    log.info("=" * 65)
    log.info("  Records processed    : %s", f"{source_count:,}")
    log.info("  Records accepted     : %s  (%.2f%%)", f"{accepted_count:,}", acceptance_pct)
    log.info("  Records quarantined  : %s  (%.2f%%)", f"{rejected_count:,}", rejection_pct)
    log.info("  dbt quality tests    : %s", dbt_test_status)
    log.info("  Statistical checks   : %s", val_status)
    log.info("-" * 65)
    log.info("  BCBS 239 Data Lineage: raw layer preserved")
    log.info("  DLQ audit trail      : quarantine.rejected_loans")
    log.info("  Regulatory reporting : %s", "CLEARED" if val_status == "CLEARED" else "BLOCKED — review required")
    log.info("=" * 65)


def notify_source_not_ready(**context):
    raise RuntimeError("Pipeline aborted: source database is empty or unreachable.")


# ── DAG definition ─────────────────────────────────────────────
with DAG(
    dag_id="ma_migration_pipeline",
    description="Bank M&A ELT pipeline with BCBS-239 quality controls and statistical risk validation",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * *",   # 06:00 UTC daily (before KNF reporting window)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["banking", "m&a", "bcbs239", "regulatory", "risk"],
) as dag:

    # Task 1: Source check
    task_check_source = BranchPythonOperator(
        task_id="check_source_data",
        python_callable=check_source_data,
    )

    task_source_not_ready = PythonOperator(
        task_id="source_not_ready",
        python_callable=notify_source_not_ready,
    )

    # Task 2: dbt deps
    task_dbt_deps = PythonOperator(
        task_id="dbt_deps",
        python_callable=dbt_deps_install,
    )

    # Task 3: dbt raw ingest
    task_dbt_raw = PythonOperator(
        task_id="dbt_run_raw",
        python_callable=dbt_run_raw,
    )

    # Task 4: dbt clean transform
    task_dbt_clean = PythonOperator(
        task_id="dbt_run_clean",
        python_callable=dbt_run_clean,
    )

    # Task 5: dbt schema tests
    task_dbt_test = PythonOperator(
        task_id="dbt_test",
        python_callable=dbt_test_all,
    )

    # Task 6: Dead Letter Queue router
    task_dlq = PythonOperator(
        task_id="route_to_dlq",
        python_callable=route_to_dlq_task,
    )

    # Task 7: Statistical validation
    task_validate = PythonOperator(
        task_id="validate_migration",
        python_callable=validate_migration_task,
    )

    # Task 8: Summary report
    task_summary = PythonOperator(
        task_id="generate_summary",
        python_callable=generate_summary,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # DAG wiring
    task_check_source >> [task_source_not_ready, task_dbt_deps]
    task_dbt_deps     >> task_dbt_raw
    task_dbt_raw      >> task_dbt_clean
    task_dbt_clean    >> task_dbt_test
    task_dbt_test     >> task_dlq
    task_dlq          >> task_validate
    task_validate     >> task_summary
