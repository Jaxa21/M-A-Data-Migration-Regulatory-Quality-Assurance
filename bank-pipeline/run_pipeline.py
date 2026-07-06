"""
run_pipeline.py
─────────────────────────────────────────────────────────────────
Local pipeline runner — executes the full ELT + QA flow WITHOUT
Airflow. Useful for:
  - Fast local development and debugging
  - CI smoke tests
  - Demoing the pipeline without waiting for the Airflow scheduler

This mirrors exactly what the Airflow DAG does, task by task,
but runs sequentially in a single Python process so you can see
all output in one terminal.

Usage:
    python run_pipeline.py                  # full run, fail-fast
    python run_pipeline.py --skip-generate   # reuse existing source data
    python run_pipeline.py --records 50000   # smaller dataset for quick tests
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
    """Wrap a pipeline step with consistent logging and fail-fast behaviour."""
    log.info("")
    log.info("=" * 65)
    log.info("STEP: %s", title)
    log.info("=" * 65)
    try:
        result = fn(*args, **kwargs)
        log.info("✅ %s — completed", title)
        return result
    except Exception as exc:
        log.error("⛔ %s — FAILED: %s", title, exc)
        log.error("Pipeline halted. Fix the error above and re-run.")
        sys.exit(1)


def run_dbt_command(command: str):
    """Run a dbt CLI command from the dbt_project directory."""
    cmd_list = ["dbt"] + command.split() + ["--profiles-dir", str(DBT_DIR)]
    log.info("Running: %s", " ".join(cmd_list))
    result = subprocess.run(cmd_list, shell=False, cwd=DBT_DIR, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"dbt command failed: dbt {command}")


def step_check_source():
    """Verify source DB is reachable and has data."""
    from sqlalchemy import create_engine, text

    source_db = os.getenv(
        "SOURCE_DB_CONN",
        "postgresql+psycopg2://bankuser:bankpass@localhost:5433/bank_b_legacy",
    )
    engine = create_engine(source_db, pool_pre_ping=True)
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM legacy.legacy_loans")).scalar()
    log.info("Source records found: %s", f"{count:,}")
    if count == 0:
        raise RuntimeError(
            "Source database is empty. Run with --skip-generate=False "
            "or run the data generator manually first."
        )
    return count


def step_generate_data(num_records: int):
    """Run the synthetic data generator."""
    os.environ["SOURCE_DB_HOST"] = os.getenv("SOURCE_DB_HOST", "localhost")
    os.environ["SOURCE_DB_PORT"] = os.getenv("SOURCE_DB_PORT", "5433")
    os.environ["NUM_RECORDS"] = str(num_records)

    from generate_loans import main as generate_main
    generate_main()


def step_dbt_deps():
    run_dbt_command("deps")


def step_dbt_run_raw():
    run_dbt_command("run --select raw.*")


def step_dbt_run_clean():
    run_dbt_command("run --select clean.*")


def step_dbt_test():
    """dbt test failures are expected (bad records exist by design) — don't fail the run."""
    try:
        run_dbt_command("test")
    except RuntimeError as exc:
        log.warning("Some dbt tests failed (expected — quarantine will route the rejects): %s", exc)


def step_route_to_dlq(run_id: str):
    os.environ.setdefault(
        "TARGET_DB_CONN",
        "postgresql+psycopg2://bankuser:bankpass@localhost:5434/bank_a_dwh",
    )
    from route_to_dlq import main as dlq_main
    return dlq_main(run_id=run_id)


def step_validate_migration(run_id: str):
    os.environ.setdefault(
        "SOURCE_DB_CONN",
        "postgresql+psycopg2://bankuser:bankpass@localhost:5433/bank_b_legacy",
    )
    os.environ.setdefault(
        "TARGET_DB_CONN",
        "postgresql+psycopg2://bankuser:bankpass@localhost:5434/bank_a_dwh",
    )
    from validate_migration import main as validate_main
    exit_code = validate_main(run_id=run_id)
    if exit_code != 0:
        raise RuntimeError("Statistical validation failed — risk profile shifted beyond threshold")


def main():
    parser = argparse.ArgumentParser(description="Run the full bank M&A migration pipeline locally")
    parser.add_argument(
        "--skip-generate", action="store_true",
        help="Skip data generation step and reuse existing source data",
    )
    parser.add_argument(
        "--records", type=int, default=100_000,
        help="Number of synthetic loan records to generate (default: 100,000 for fast local runs)",
    )
    args = parser.parse_args()

    run_id = str(uuid.uuid4())[:8]
    log.info("Bank M&A Migration Pipeline — Local Run")
    log.info("Run ID: %s", run_id)
    log.info("Note: this script assumes Docker containers are already running.")
    log.info("If not, start them first with: docker-compose up -d source-db target-db")

    if not args.skip_generate:
        run_step(f"Generate synthetic data ({args.records:,} records)", step_generate_data, args.records)
    else:
        log.info("Skipping data generation (--skip-generate flag set)")

    run_step("Check source data", step_check_source)
    run_step("dbt deps (install dbt-utils)", step_dbt_deps)
    run_step("dbt run — raw layer", step_dbt_run_raw)
    run_step("dbt run — clean layer", step_dbt_run_clean)
    run_step("dbt test — quality rules", step_dbt_test)
    rejected = run_step("Route rejected records to DLQ", step_route_to_dlq, run_id)
    run_step("Statistical validation (KS-test + PSI)", step_validate_migration, run_id)

    log.info("")
    log.info("=" * 65)
    log.info("  PIPELINE COMPLETE — run_id: %s", run_id)
    log.info("  Records quarantined: %s", f"{rejected:,}" if rejected else "0")
    log.info("=" * 65)
    log.info("Query clean.migration_run_summary for the full report:")
    log.info("  SELECT * FROM clean.migration_run_summary WHERE run_id = '%s';", run_id)


if __name__ == "__main__":
    main()
