"""Validation gates for the support loop.

After each repair attempt:
1. Re-verify the target PDF (zero discrepancies required)
2. Run bank-specific regression tests
3. Optionally run broader smoke tests when extractor changed
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import List, Tuple

from anthropic import Anthropic

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.profiles.factory import BankProfileFactory
from src.utils.logger import get_logger
from verifystatement.task_state import (
    AttemptRecord,
    GateResult,
    Strategy,
    ValidationGate,
)

logger = get_logger(__name__)


def run_all_gates(
    target_pdf: str,
    bank_name: str,
    bank_key: str,
    strategy: Strategy,
    client: Anthropic,
    model: str,
) -> Tuple[List[ValidationGate], bool, bool]:
    """Run all validation gates. Returns (gates, target_passes, regressions_pass)."""
    gates: List[ValidationGate] = []

    # Gate 1: Re-verify target PDF
    target_gate = _run_target_verification(target_pdf, client, model)
    gates.append(target_gate)
    target_passes = target_gate.result == GateResult.PASSED

    # Gate 2: Bank-specific regression tests
    regression_gate = _run_bank_regression(bank_key)
    gates.append(regression_gate)
    regressions_pass = regression_gate.result in (GateResult.PASSED, GateResult.SKIPPED)

    # Gate 3: Broader smoke tests (only for extractor changes)
    if strategy == Strategy.EXTRACTOR_PATCH:
        smoke_gate = _run_smoke_tests()
        gates.append(smoke_gate)
        # Smoke failure doesn't block completion but is recorded
        if smoke_gate.result == GateResult.FAILED:
            regressions_pass = False

    return gates, target_passes, regressions_pass


def _run_target_verification(
    target_pdf: str,
    client: Anthropic,
    model: str,
) -> ValidationGate:
    """Re-verify the target PDF and check for zero discrepancies."""
    gate = ValidationGate(name="target_pdf_verification")
    try:
        from verifystatement.verify import verify_pdf

        report = verify_pdf(target_pdf, client, model)
        issues = (
            report.get("total_missing", 0)
            + report.get("total_incorrect", 0)
            + report.get("total_extra", 0)
        )
        if issues == 0:
            gate.result = GateResult.PASSED
            gate.detail = "All transactions verified correctly"
        else:
            gate.result = GateResult.FAILED
            gate.detail = (
                f"Still {issues} issue(s): "
                f"missing={report.get('total_missing', 0)}, "
                f"incorrect={report.get('total_incorrect', 0)}, "
                f"extra={report.get('total_extra', 0)}"
            )
        gate.stdout_excerpt = f"total_extracted={report.get('total_extracted', 0)}"
    except Exception as exc:
        gate.result = GateResult.FAILED
        gate.detail = f"Verification error: {exc}"
        gate.stderr_excerpt = str(exc)[:1000]

    return gate


def _run_bank_regression(bank_key: str) -> ValidationGate:
    """Run bank-specific regression tests via pytest."""
    gate = ValidationGate(name=f"bank_regression_{bank_key}")

    if not bank_key:
        gate.result = GateResult.SKIPPED
        gate.detail = "No bank key — skipping regression tests"
        return gate

    # Build pytest -k expression with name variants
    name_variants = {bank_key}
    if bank_key.endswith("_bank"):
        name_variants.add(bank_key[:-5])

    k_expr = " or ".join(sorted(name_variants))

    result = subprocess.run(
        [
            sys.executable, "-m", "pytest",
            "tests/test_api_server.py",
            "-k", k_expr,
            "-v", "--tb=short",
        ],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
    )

    combined = result.stdout + result.stderr

    no_tests = (
        result.returncode == 5
        or "no tests ran" in combined.lower()
        or "0 selected" in combined.lower()
    )

    if no_tests:
        gate.result = GateResult.SKIPPED
        gate.detail = f"No regression tests found for '{bank_key}'"
    elif result.returncode == 0:
        gate.result = GateResult.PASSED
        gate.detail = "All regression tests passed"
    else:
        gate.result = GateResult.FAILED
        gate.detail = "Regression tests failed"

    gate.stdout_excerpt = combined[-2000:]

    return gate


def _run_smoke_tests() -> ValidationGate:
    """Run a broader smoke test subset across all bank fixtures.

    Used when extractor code changed to ensure no cross-bank regressions.
    """
    gate = ValidationGate(name="smoke_tests")

    result = subprocess.run(
        [
            sys.executable, "-m", "pytest",
            "tests/test_api_server.py",
            "-v", "--tb=short",
            "-x",  # Stop at first failure for speed
        ],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
    )

    combined = result.stdout + result.stderr

    if result.returncode == 0:
        gate.result = GateResult.PASSED
        gate.detail = "All smoke tests passed"
    else:
        gate.result = GateResult.FAILED
        gate.detail = "Cross-bank regression detected"

    gate.stdout_excerpt = combined[-2000:]

    return gate


def compute_verification_delta(
    initial_report: dict,
    new_report: dict,
) -> dict:
    """Compute the delta between two verification reports."""
    return {
        "missing_delta": (
            new_report.get("total_missing", 0)
            - initial_report.get("total_missing", 0)
        ),
        "incorrect_delta": (
            new_report.get("total_incorrect", 0)
            - initial_report.get("total_incorrect", 0)
        ),
        "extra_delta": (
            new_report.get("total_extra", 0)
            - initial_report.get("total_extra", 0)
        ),
        "initial_issues": (
            initial_report.get("total_missing", 0)
            + initial_report.get("total_incorrect", 0)
            + initial_report.get("total_extra", 0)
        ),
        "current_issues": (
            new_report.get("total_missing", 0)
            + new_report.get("total_incorrect", 0)
            + new_report.get("total_extra", 0)
        ),
    }
