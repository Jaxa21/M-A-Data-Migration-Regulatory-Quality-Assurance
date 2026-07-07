"""
test_route_to_dlq.py
─────────────────────────────────────────────────────────────────
Unit tests for the Dead Letter Queue rejection classifier.
Each test verifies that a specific data quality violation is
correctly identified and labelled with the right rule code.

Run with:
    pytest tests/test_route_to_dlq.py -v
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "validation"))
from route_to_dlq import classify_rejection


def make_valid_record(**overrides) -> dict:
    """Baseline valid record — tests override one field at a time."""
    record = {
        "loan_id": "loan-001",
        "customer_id": "cust-001",
        "loan_amount": 250_000.0,
        "currency": "PLN",
        "client_age": 35,
        "probability_of_default": 0.05,
        "interest_rate": 7.5,
    }
    record.update(overrides)
    return record


class TestNullChecks:
    def test_null_loan_id_rejected(self):
        record = make_valid_record(loan_id=None)
        rule, reason = classify_rejection(record)
        assert rule == "CHK_NULL_LOAN_ID"
        assert "loan_id" in reason

    def test_null_customer_id_rejected(self):
        record = make_valid_record(customer_id=None)
        rule, reason = classify_rejection(record)
        assert rule == "CHK_NULL_CUSTOMER_ID"

    def test_null_loan_amount_rejected(self):
        record = make_valid_record(loan_amount=None)
        rule, reason = classify_rejection(record)
        assert rule == "CHK_NULL_AMOUNT"


class TestAmountChecks:
    def test_negative_amount_rejected(self):
        record = make_valid_record(loan_amount=-1000.0)
        rule, reason = classify_rejection(record)
        assert rule == "CHK_NEGATIVE_AMOUNT"
        assert "-1000" in reason

    def test_zero_amount_rejected(self):
        record = make_valid_record(loan_amount=0.0)
        rule, reason = classify_rejection(record)
        assert rule == "CHK_NEGATIVE_AMOUNT"

    def test_small_positive_amount_accepted(self):
        """Amount of 0.01 PLN is technically valid — should not be rejected on amount grounds."""
        record = make_valid_record(loan_amount=0.01)
        rule, reason = classify_rejection(record)
        assert rule != "CHK_NEGATIVE_AMOUNT"
        assert rule != "CHK_NULL_AMOUNT"


class TestAgeChecks:
    def test_minor_client_rejected(self):
        record = make_valid_record(client_age=15)
        rule, reason = classify_rejection(record)
        assert rule == "CHK_CLIENT_AGE_MINOR"
        assert "15" in reason

    def test_age_exactly_18_accepted(self):
        """Boundary case: 18 is the legal minimum and should be accepted."""
        record = make_valid_record(client_age=18)
        rule, reason = classify_rejection(record)
        assert rule != "CHK_CLIENT_AGE_MINOR"

    def test_age_exactly_100_accepted(self):
        """Boundary case: 100 is the maximum plausible age and should be accepted."""
        record = make_valid_record(client_age=100)
        rule, reason = classify_rejection(record)
        assert rule != "CHK_CLIENT_AGE_IMPLAUSIBLE"

    def test_implausible_age_rejected(self):
        record = make_valid_record(client_age=120)
        rule, reason = classify_rejection(record)
        assert rule == "CHK_CLIENT_AGE_IMPLAUSIBLE"
        assert "120" in reason


class TestProbabilityOfDefaultChecks:
    def test_pd_above_one_rejected(self):
        record = make_valid_record(probability_of_default=1.5)
        rule, reason = classify_rejection(record)
        assert rule == "CHK_PD_RANGE"

    def test_pd_negative_rejected(self):
        record = make_valid_record(probability_of_default=-0.1)
        rule, reason = classify_rejection(record)
        assert rule == "CHK_PD_RANGE"

    def test_pd_exactly_zero_accepted(self):
        record = make_valid_record(probability_of_default=0.0)
        rule, reason = classify_rejection(record)
        assert rule != "CHK_PD_RANGE"

    def test_pd_exactly_one_accepted(self):
        record = make_valid_record(probability_of_default=1.0)
        rule, reason = classify_rejection(record)
        assert rule != "CHK_PD_RANGE"


class TestCurrencyChecks:
    @pytest.mark.parametrize("currency", ["PLN", "EUR", "USD"])
    def test_valid_currency_accepted(self, currency):
        record = make_valid_record(currency=currency)
        rule, reason = classify_rejection(record)
        assert rule not in ("CHK_UNKNOWN_CURRENCY", "CHK_CURRENCY_TYPO")

    @pytest.mark.parametrize("currency", ["eur", "PL", "pln", "Eur"])
    def test_known_alias_not_quarantined(self, currency):
        # These aliases are auto-mapped by dbt clean_loans.sql (eur->EUR, PL->PLN).
        # They must NOT be quarantined — only truly unknown codes go to DLQ.
        record = make_valid_record(currency=currency)
        rule, reason = classify_rejection(record)
        assert rule != "CHK_UNKNOWN_CURRENCY"
        assert rule != "CHK_CURRENCY_TYPO"

    def test_completely_unknown_currency_rejected(self):
        record = make_valid_record(currency="XYZ")
        rule, reason = classify_rejection(record)
        assert rule == "CHK_UNKNOWN_CURRENCY"


class TestInterestRateChecks:
    def test_negative_rate_rejected(self):
        record = make_valid_record(interest_rate=-2.0)
        rule, reason = classify_rejection(record)
        assert rule == "CHK_NEGATIVE_RATE"

    def test_zero_rate_accepted(self):
        """0% interest is unusual but not invalid."""
        record = make_valid_record(interest_rate=0.0)
        rule, reason = classify_rejection(record)
        assert rule != "CHK_NEGATIVE_RATE"


class TestPriorityOrder:
    """When a record has multiple violations, the classifier should
    return the highest-priority rule (null checks before value checks)."""

    def test_null_loan_id_takes_priority_over_negative_amount(self):
        record = make_valid_record(loan_id=None, loan_amount=-500.0)
        rule, reason = classify_rejection(record)
        assert rule == "CHK_NULL_LOAN_ID"

    def test_negative_amount_takes_priority_over_bad_age(self):
        record = make_valid_record(loan_amount=-500.0, client_age=15)
        rule, reason = classify_rejection(record)
        assert rule == "CHK_NEGATIVE_AMOUNT"


class TestValidRecord:
    def test_fully_valid_record_has_no_rejection_rule(self):
        """A clean record should not match any rejection rule.
        Note: classify_rejection is only called on records already
        confirmed absent from clean_loans, so in production this
        case implies an undetected issue — tests CHK_UNKNOWN fallback."""
        record = make_valid_record()
        rule, reason = classify_rejection(record)
        # A fully valid record falling through to classify_rejection
        # (which only runs on already-rejected records) should hit
        # the fallback unknown rule, not a false positive.
        assert rule == "CHK_UNKNOWN"
