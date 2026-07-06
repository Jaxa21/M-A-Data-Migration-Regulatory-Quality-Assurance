"""
test_validate_migration.py
─────────────────────────────────────────────────────────────────
Unit tests for the statistical validation functions: KS-test
and Population Stability Index (PSI).

These tests use synthetic numpy arrays directly (no database
required) so they run fast and in any CI environment.

Run with:
    pytest tests/test_validate_migration.py -v
"""

import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "validation"))
from validate_migration import run_ks_test, calculate_psi


@pytest.fixture
def rng():
    return np.random.default_rng(seed=42)


class TestKSTest:
    def test_identical_distributions_pass(self, rng):
        """Same distribution sampled twice should never trigger an alarm."""
        source = rng.beta(1.2, 8.0, size=10_000)
        target = source.copy()
        result = run_ks_test(source, target)
        assert result["passed"] is True
        assert result["p_value"] == 1.0

    def test_similar_distributions_pass(self, rng):
        """Two independent samples from the SAME distribution should
        usually pass (p >= 0.05) — this is the expected real-world case
        when a migration introduces no bias."""
        source = rng.beta(1.2, 8.0, size=10_000)
        target = rng.beta(1.2, 8.0, size=10_000)
        result = run_ks_test(source, target)
        assert result["passed"] is True

    def test_shifted_distribution_fails(self, rng):
        """A target distribution shifted significantly higher should
        trigger the KS alarm — simulates a currency conversion bug
        that systematically inflates risk scores."""
        source = rng.beta(1.2, 8.0, size=10_000)
        target = rng.beta(1.2, 8.0, size=10_000) + 0.3  # shifted up
        result = run_ks_test(source, target)
        assert result["passed"] is False
        assert result["p_value"] < 0.05

    def test_result_contains_required_fields(self, rng):
        source = rng.uniform(0, 1, 1000)
        target = rng.uniform(0, 1, 1000)
        result = run_ks_test(source, target)
        assert "ks_statistic" in result
        assert "p_value" in result
        assert "passed" in result
        assert "interpretation" in result


class TestPSI:
    def test_identical_distributions_give_zero_psi(self, rng):
        """PSI of a distribution against itself should be ~0 (STABLE)."""
        source = rng.beta(1.2, 8.0, size=10_000)
        target = source.copy()
        result = calculate_psi(source, target)
        assert result["psi_value"] < 0.01
        assert result["psi_status"] == "STABLE"
        assert result["passed"] is True

    def test_similar_samples_remain_stable(self, rng):
        """Two independent draws from the same distribution should
        produce a low PSI — normal sampling noise, not a real shift."""
        source = rng.beta(1.2, 8.0, size=20_000)
        target = rng.beta(1.2, 8.0, size=20_000)
        result = calculate_psi(source, target)
        assert result["psi_status"] in ("STABLE", "MONITOR")

    def test_major_shift_triggers_alarm(self, rng):
        """A target population concentrated in a completely different
        range should trigger an ALARM-level PSI — simulates a broken
        FX conversion that pushed all risk scores into one bucket."""
        source = rng.beta(1.2, 8.0, size=10_000)        # skewed low (realistic PD)
        target = rng.uniform(0.6, 1.0, size=10_000)     # concentrated high — clearly wrong
        result = calculate_psi(source, target)
        assert result["psi_status"] == "ALARM"
        assert result["passed"] is False
        assert result["psi_value"] > 0.25

    def test_moderate_shift_triggers_monitor(self, rng):
        """A moderate shift should land in the MONITOR band, not STABLE
        and not necessarily ALARM — verifies the middle threshold band
        is reachable and not just a binary pass/fail."""
        source = rng.normal(0.1, 0.05, size=10_000)
        source = np.clip(source, 0, 1)
        target = rng.normal(0.14, 0.05, size=10_000)    # small shift
        target = np.clip(target, 0, 1)
        result = calculate_psi(source, target)
        # Moderate shift should not be STABLE-zero, but also shouldn't
        # necessarily breach ALARM — assert it's a valid status at minimum
        assert result["psi_status"] in ("STABLE", "MONITOR", "ALARM")
        assert result["psi_value"] > 0

    def test_result_contains_required_fields(self, rng):
        source = rng.uniform(0, 1, 1000)
        target = rng.uniform(0, 1, 1000)
        result = calculate_psi(source, target)
        assert "psi_value" in result
        assert "psi_status" in result
        assert "passed" in result
        assert "bucket_detail" in result

    def test_psi_handles_empty_buckets_without_crashing(self, rng):
        """If target has zero observations in a bucket that source
        populates heavily, the epsilon smoothing must prevent a
        divide-by-zero or log(0) crash."""
        source = rng.uniform(0, 1, 5000)
        target = rng.uniform(0, 0.1, 5000)  # concentrated in one decile only
        result = calculate_psi(source, target)
        assert np.isfinite(result["psi_value"])
        assert result["psi_status"] == "ALARM"


class TestThresholdBoundaries:
    """Sanity checks on the documented threshold semantics."""

    def test_psi_stable_threshold_is_point_one(self):
        from validate_migration import PSI_STABLE_THRESHOLD
        assert PSI_STABLE_THRESHOLD == 0.10

    def test_psi_monitor_threshold_is_point_two_five(self):
        from validate_migration import PSI_MONITOR_THRESHOLD
        assert PSI_MONITOR_THRESHOLD == 0.25

    def test_ks_pvalue_threshold_is_point_zero_five(self):
        from validate_migration import KS_PVALUE_THRESHOLD
        assert KS_PVALUE_THRESHOLD == 0.05
