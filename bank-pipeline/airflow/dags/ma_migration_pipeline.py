"""
Airflow DAG: ma_migration_pipeline

Orchestrates the Bank M&A ELT pipeline end-to-end.

Task order:
    check_source_data -> dbt_deps -> dbt_run_raw -> dbt_run_clean
    -> dbt_test -> route_to_dlq -> validate_migration -> generate_summary
"""

from __future__ import annotations

import logging
import subprocess
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy import create_engine, text

log = logging.getLogger(__name__)

SOURCE_DB = "postgresql+psycopg2://bankuser:bankpass@source-db:5432/bank_b_legacy"
TARGET_DB = "postgresql+psycopg2://bankuser:bankpass@target-db:5432/bank_a_dwh"
DBT_DIR = "/opt/airflow/dbt_project"

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}


def run_dbt(command: str):
    cmd = ["dbt"] + command.split() + ["--profiles-dir", DBT_DIR]
    log.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, shell=False, cwd=DBT_DIR, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"dbt {command} failed")


def check_source_data(**ctx) -> str:
    engine = create_engine(SOURCE_DB)
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM legacy.legacy_loans")).scalar()
    log.info("Source records: %s", f"{count:,}")
    if count == 0:
        return "source_not_ready"
    ctx["ti"].xcom_push(key="source_record_count", value=count)
    return "dbt_deps"


def dbt_deps_install(**ctx):
    run_dbt("deps")


def dbt_run_raw(**ctx):
    run_dbt("run --select raw.*")


def dbt_run_clean(**ctx):
    run_dbt("run --select clean.*")


def dbt_test_all(**ctx):
    try:
        run_dbt("test")
        ctx["ti"].xcom_push(key="dbt_test_status", value="PASSED")
    except RuntimeError:
        log.warning("dbt tests failed — rejected records will be routed to DLQ")
        ctx["ti"].xcom_push(key="dbt_test_status", value="FAILED_WITH_REJECTIONS")


def route_to_dlq_task(**ctx):
    sys.path.insert(0, "/opt/airflow/validation")
    from route_to_dlq import main as dlq_main
    run_id = ctx["run_id"][:8]
    rejected = dlq_main(run_id=run_id)
    ctx["ti"].xcom_push(key="rejected_count", value=rejected)


def validate_migration_task(**ctx):
    sys.path.insert(0, "/opt/airflow/validation")
    from validate_migration import main as val_main
    run_id = ctx["run_id"][:8]
    if val_main(run_id=run_id) != 0:
        raise ValueError(
            "Statistical validation failed — do not use for regulatory reporting"
        )
    ctx["ti"].xcom_push(key="validation_status", value="CLEARED")


def generate_summary(**ctx):
    ti = ctx["ti"]
    source_count  = ti.xcom_pull(task_ids="check_source_data", key="source_record_count") or 0
    rejected      = ti.xcom_pull(task_ids="route_to_dlq",      key="rejected_count") or 0
    dbt_status    = ti.xcom_pull(task_ids="dbt_test",          key="dbt_test_status") or "UNKNOWN"
    val_status    = ti.xcom_pull(task_ids="validate_migration", key="validation_status") or "UNKNOWN"
    accepted      = source_count - rejected

    log.info("run_id          : %s", ctx["run_id"][:8])
    log.info("records_processed : %s", f"{source_count:,}")
    log.info("records_accepted  : %s (%.2f%%)", f"{accepted:,}",
             100 * accepted / max(source_count, 1))
    log.info("records_quarantined : %s (%.2f%%)", f"{rejected:,}",
             100 * rejected / max(source_count, 1))
    log.info("dbt_tests         : %s", dbt_status)
    log.info("statistical_check : %s", val_status)
    log.info("regulatory_status : %s", "CLEARED" if val_status == "CLEARED" else "BLOCKED")


def abort_source_not_ready(**ctx):
    raise RuntimeError("Source database is empty — pipeline aborted")


with DAG(
    dag_id="ma_migration_pipeline",
    description="Bank M&A ELT pipeline with BCBS-239 quality controls",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["banking", "bcbs239", "regulatory"],
) as dag:

    t_check   = BranchPythonOperator(task_id="check_source_data",  python_callable=check_source_data)
    t_abort   = PythonOperator(task_id="source_not_ready",         python_callable=abort_source_not_ready)
    t_deps    = PythonOperator(task_id="dbt_deps",                 python_callable=dbt_deps_install)
    t_raw     = PythonOperator(task_id="dbt_run_raw",              python_callable=dbt_run_raw)
    t_clean   = PythonOperator(task_id="dbt_run_clean",            python_callable=dbt_run_clean)
    t_test    = PythonOperator(task_id="dbt_test",                 python_callable=dbt_test_all)
    t_dlq     = PythonOperator(task_id="route_to_dlq",             python_callable=route_to_dlq_task)
    t_val     = PythonOperator(task_id="validate_migration",       python_callable=validate_migration_task)
    t_summary = PythonOperator(task_id="generate_summary",         python_callable=generate_summary,
                               trigger_rule=TriggerRule.ALL_DONE)

    t_check >> [t_abort, t_deps]
    t_deps >> t_raw >> t_clean >> t_test >> t_dlq >> t_val >> t_summary
