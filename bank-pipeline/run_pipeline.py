"""
Local pipeline runner.

Executes the full ELT + QA flow without Airflow — useful for local
development, debugging, and CI smoke tests.

Usage:
    python run_pipeline.py                   # 100k records, fail-fast
    python run_pipeline.py --skip-generate   # reuse existing source data
    python run_pipeline.py --records 500000  # custom record count
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import uuid
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("run_pipeline")

ROOT_DIR = Path(__file__).resolve().parent
DBT_DIR = ROOT_DIR / "dbt_project"
VALIDATION_DIR = ROOT_DIR / "validation"
DATA_GEN_DIR = ROOT_DIR / "data_generator"

sys.path.insert(0, str(VALIDATION_DIR))
sys.path.insert(0, str(DATA_GEN_DIR))


def run_step(title: str, fn, *args, **kwargs):
    log.info("--- %s", title)
    try:
        result = fn(*args, **kwargs)
        log.info("OK: %s", title)
        return result
    except Exception as exc:
        log.error("FAILED: %s — %s", title, exc)
        sys.exit(1)


def run_dbt_command(command: str):
    cmd = ["dbt"] + command.split() + ["--profiles-dir", str(DBT_DIR)]
    log.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, shell=False, cwd=DBT_DIR, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"dbt {command} failed")


def check_source():
    from sqlalchemy import create_engine, text
    conn_str = os.getenv(
        "SOURCE_DB_CONN",
        "postgresql+psycopg2://bankuser:bankpass@localhost:5433/bank_b_legacy",
    )
    with create_engine(conn_str).connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM legacy.legacy_loans")).scalar()
    log.info("Source records: %s", f"{count:,}")
    if count == 0:
        raise RuntimeError("Source database is empty — run without --skip-generate")
    return count


def generate_data(num_records: int):
    os.environ["SOURCE_DB_HOST"] = os.getenv("SOURCE_DB_HOST", "localhost")
    os.environ["SOURCE_DB_PORT"] = os.getenv("SOURCE_DB_PORT", "5433")
    os.environ["NUM_RECORDS"] = str(num_records)
    from generate_loans import main as gen_main
    gen_main()


def dbt_deps():
    run_dbt_command("deps")


def dbt_run_raw():
    run_dbt_command("run --select raw.*")


def dbt_run_clean():
    run_dbt_command("run --select clean.*")


def dbt_test():
    try:
        run_dbt_command("test")
    except RuntimeError:
        log.warning("Some dbt tests failed — rejected records will be routed to DLQ")


def route_dlq(run_id: str) -> int:
    os.environ.setdefault(
        "TARGET_DB_CONN",
        "postgresql+psycopg2://bankuser:bankpass@localhost:5434/bank_a_dwh",
    )
    from route_to_dlq import main as dlq_main
    return dlq_main(run_id=run_id)


def validate(run_id: str):
    os.environ.setdefault(
        "SOURCE_DB_CONN",
        "postgresql+psycopg2://bankuser:bankpass@localhost:5433/bank_b_legacy",
    )
    os.environ.setdefault(
        "TARGET_DB_CONN",
        "postgresql+psycopg2://bankuser:bankpass@localhost:5434/bank_a_dwh",
    )
    from validate_migration import main as val_main
    if val_main(run_id=run_id) != 0:
        raise RuntimeError("Statistical validation failed — risk profile shifted beyond threshold")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-generate", action="store_true")
    parser.add_argument("--records", type=int, default=100_000)
    args = parser.parse_args()

    run_id = str(uuid.uuid4())[:8]
    log.info("Pipeline starting — run_id=%s", run_id)

    if not args.skip_generate:
        run_step(f"Generate data ({args.records:,} records)", generate_data, args.records)

    run_step("Check source data", check_source)
    run_step("dbt deps", dbt_deps)
    run_step("dbt run — raw layer", dbt_run_raw)
    run_step("dbt run — clean layer", dbt_run_clean)
    run_step("dbt test", dbt_test)
    rejected = run_step("Route DLQ", route_dlq, run_id)
    run_step("Statistical validation", validate, run_id)

    log.info("Pipeline complete — run_id=%s  quarantined=%s", run_id, rejected or 0)
    log.info("SELECT * FROM clean.migration_run_summary WHERE run_id = '%s';", run_id)


if __name__ == "__main__":
    main()
