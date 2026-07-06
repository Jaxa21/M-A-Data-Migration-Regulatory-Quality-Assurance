"""
generate_loans.py
─────────────────────────────────────────────────────────────────
Generates a synthetic loan portfolio for Bank B (legacy source).
Deliberately seeds data quality errors that the pipeline must catch.

Data quality issues injected (by design):
  - ~0.5%  records: negative loan_amount        → rule CHK_NEGATIVE_AMOUNT
  - ~0.3%  records: NULL loan_amount            → rule CHK_NULL_AMOUNT
  - ~1.0%  records: currency typos (eur, PL…)  → rule CHK_CURRENCY_CODE
  - ~0.8%  records: client_age < 18 (minor)    → rule CHK_CLIENT_AGE
  - ~0.3%  records: client_age > 100            → rule CHK_CLIENT_AGE
  - ~0.4%  records: PD outside [0, 1]           → rule CHK_PD_RANGE

Total error rate: ~3.3% → expect ~33,000 rejections per 1M records.
"""

import os
import uuid
import logging
import numpy as np
import pandas as pd
from faker import Faker
from datetime import date, timedelta
from sqlalchemy import create_engine, text

# ── Config ────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

DB_HOST = os.getenv("SOURCE_DB_HOST", "localhost")
DB_PORT = os.getenv("SOURCE_DB_PORT", "5433")
DB_USER = os.getenv("SOURCE_DB_USER", "bankuser")
DB_PASS = os.getenv("SOURCE_DB_PASS", "bankpass")
DB_NAME = os.getenv("SOURCE_DB_NAME", "bank_b_legacy")
NUM_RECORDS = int(os.getenv("NUM_RECORDS", "1000000"))
CHUNK_SIZE = 50_000   # write in batches to avoid memory issues
RANDOM_SEED = 42

# ── Constants ──────────────────────────────────────────────────
VALID_CURRENCIES = ["PLN", "EUR", "USD"]
DIRTY_CURRENCIES = ["eur", "PL", "pln", "Eur", "US", "EURO", "usd"]  # typos
LOAN_STATUSES = ["ACTIVE", "CLOSED", "DEFAULT", "RESTRUCTURED"]
PRODUCT_TYPES = ["MORTGAGE", "CONSUMER", "SME", "AUTO"]
BRANCH_CODES = [f"BR{str(i).zfill(3)}" for i in range(1, 51)]


def make_db_engine() -> object:
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(url, pool_pre_ping=True)
    log.info("Connected to source database: %s", DB_NAME)
    return engine


def generate_chunk(fake: Faker, rng: np.random.Generator, size: int) -> pd.DataFrame:
    """Generate one batch of loan records with controlled error injection."""

    # ── Base distributions ──────────────────────────────────────
    # Loan amounts: lognormal distribution (realistic credit portfolio)
    loan_amounts = rng.lognormal(mean=11.5, sigma=1.2, size=size).round(2)
    # PD: beta distribution skewed toward low values (most clients are good)
    pd_values = rng.beta(a=1.2, b=8.0, size=size).round(4)
    # WIBOR: normal around 5.85% (current Polish rate environment)
    wibor_rates = np.clip(rng.normal(5.85, 0.5, size), 0.5, 15.0).round(4)
    # Margin over WIBOR: lognormal
    margins = np.clip(rng.lognormal(0.8, 0.4, size), 0.1, 8.0).round(4)
    interest_rates = (wibor_rates + margins).round(4)
    # Client ages: normal distribution centered at 42
    client_ages = rng.normal(42, 12, size).astype(int)

    # ── Loan dates ─────────────────────────────────────────────
    start_days_ago = rng.integers(30, 3650, size)
    loan_durations = rng.integers(12, 360, size)  # months
    today = date.today()
    loan_start_dates = [today - timedelta(days=int(d)) for d in start_days_ago]
    loan_end_dates = [
        s + timedelta(days=int(dur * 30))
        for s, dur in zip(loan_start_dates, loan_durations)
    ]

    # ── Currency: mostly valid, some dirty ────────────────────
    currency_choices = rng.choice(
        VALID_CURRENCIES + DIRTY_CURRENCIES,
        size=size,
        p=[0.60, 0.25, 0.06, 0.02, 0.02, 0.01, 0.01, 0.01, 0.01, 0.01],
    )

    df = pd.DataFrame({
        "loan_id":                [str(uuid.uuid4()) for _ in range(size)],
        "customer_id":            [str(uuid.uuid4()) for _ in range(size)],
        "loan_amount":            loan_amounts,
        "currency":               currency_choices,
        "interest_rate":          interest_rates,
        "wibor_rate":             wibor_rates,
        "client_age":             client_ages,
        "probability_of_default": pd_values,
        "loan_start_date":        loan_start_dates,
        "loan_end_date":          loan_end_dates,
        "loan_status":            rng.choice(LOAN_STATUSES, size, p=[0.70, 0.18, 0.08, 0.04]),
        "branch_code":            rng.choice(BRANCH_CODES, size),
        "product_type":           rng.choice(PRODUCT_TYPES, size, p=[0.45, 0.30, 0.15, 0.10]),
        "source_system":          "BANK_B_LEGACY_V2",
    })

    # ── Inject errors ──────────────────────────────────────────
    n = len(df)

    # Negative loan amounts (~0.5%)
    neg_mask = rng.random(n) < 0.005
    df.loc[neg_mask, "loan_amount"] = -df.loc[neg_mask, "loan_amount"]

    # NULL loan amounts (~0.3%)
    null_amount_mask = rng.random(n) < 0.003
    df.loc[null_amount_mask, "loan_amount"] = None

    # Underage clients — age < 18 (~0.8%)
    minor_mask = rng.random(n) < 0.008
    df.loc[minor_mask, "client_age"] = rng.integers(5, 17, minor_mask.sum())

    # Implausible ages > 100 (~0.3%)
    old_mask = rng.random(n) < 0.003
    df.loc[old_mask, "client_age"] = rng.integers(101, 135, old_mask.sum())

    # PD out of range (~0.4%)
    pd_bad_mask = rng.random(n) < 0.004
    df.loc[pd_bad_mask, "probability_of_default"] = rng.uniform(1.01, 2.0, pd_bad_mask.sum()).round(4)

    return df


def main():
    rng = np.random.default_rng(RANDOM_SEED)
    fake = Faker("pl_PL")
    Faker.seed(RANDOM_SEED)

    engine = make_db_engine()

    # ── Idempotency: truncate before each run so re-runs don't duplicate data
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE legacy.legacy_loans"))
        conn.commit()
    log.info("Table legacy.legacy_loans truncated — starting fresh load")

    total_written = 0
    chunks = (NUM_RECORDS // CHUNK_SIZE) + (1 if NUM_RECORDS % CHUNK_SIZE else 0)

    log.info("Starting data generation: %s records in %s chunks", NUM_RECORDS, chunks)

    for i in range(chunks):
        chunk_size = min(CHUNK_SIZE, NUM_RECORDS - total_written)
        if chunk_size <= 0:
            break

        log.info("Generating chunk %s/%s (%s records)…", i + 1, chunks, chunk_size)
        df = generate_chunk(fake, rng, chunk_size)

        df.to_sql(
            name="legacy_loans",
            schema="legacy",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000,
        )
        total_written += chunk_size
        log.info("Progress: %s/%s records written (%.1f%%)",
                 total_written, NUM_RECORDS, 100 * total_written / NUM_RECORDS)

    # ── Summary stats ──────────────────────────────────────────
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM legacy.legacy_loans")).scalar()
        neg_count = conn.execute(
            text("SELECT COUNT(*) FROM legacy.legacy_loans WHERE loan_amount < 0")
        ).scalar()
        minor_count = conn.execute(
            text("SELECT COUNT(*) FROM legacy.legacy_loans WHERE client_age < 18")
        ).scalar()
        bad_currency = conn.execute(
            text("SELECT COUNT(*) FROM legacy.legacy_loans WHERE currency NOT IN ('PLN','EUR','USD')")
        ).scalar()

    log.info("─" * 60)
    log.info("Data generation complete.")
    log.info("  Total records:          %s", f"{count:,}")
    log.info("  Negative amounts:       %s  (%.2f%%)", neg_count, 100 * neg_count / count)
    log.info("  Underage clients:       %s  (%.2f%%)", minor_count, 100 * minor_count / count)
    log.info("  Invalid currency codes: %s  (%.2f%%)", bad_currency, 100 * bad_currency / count)
    log.info("─" * 60)
    log.info("Ready for dbt pipeline.")


if __name__ == "__main__":
    main()
