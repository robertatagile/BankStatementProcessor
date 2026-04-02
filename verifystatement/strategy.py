"""Strategy selection for the support loop.

Classifies the issue before any fix attempt. Decides among:
- profile_patch: known bank, structurally similar layout
- extractor_patch: known bank, layout not representable in profile-only settings
- new_profile: Generic detection with stable branding
- manual_review: ambiguous or repeated failures
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.logger import get_logger
from verifystatement.task_state import LayoutSignature, Strategy, TaskState

logger = get_logger(__name__)


def select_strategy(
    task: TaskState,
    evidence: dict,
    verification_report: dict,
) -> Strategy:
    """Select the narrowest valid repair strategy.

    Decision tree:
    1. Generic detection → new_profile (if branding is stable)
    2. Known bank + collapsed/no tables + profile can't represent → extractor_patch
    3. Known bank + structurally similar → profile_patch
    4. Repeated failures or ambiguous → manual_review
    """
    sig = task.layout_signature
    bank = task.detected_bank
    is_generic = sig.is_generic

    # Check for repeated failures (escalation guard)
    if task.attempt_count >= 3:
        prev_strategies = [a.strategy for a in task.attempts]
        if all(s == Strategy.PROFILE_PATCH for s in prev_strategies):
            logger.info(
                "Strategy: escalating to extractor_patch after 3 failed "
                "profile_patch attempts"
            )
            return Strategy.EXTRACTOR_PATCH
        if task.attempt_count >= 5:
            logger.info(
                "Strategy: recommending manual_review after 5 failed attempts"
            )
            return Strategy.MANUAL_REVIEW

    # Path 1: Generic bank → new profile
    if is_generic:
        if _has_stable_branding(evidence):
            logger.info("Strategy: new_profile (Generic detection + stable branding)")
            return Strategy.NEW_PROFILE
        else:
            logger.info(
                "Strategy: manual_review (Generic detection without stable branding)"
            )
            return Strategy.MANUAL_REVIEW

    # Path 2: Known bank — check if layout is representable via profile settings
    if _needs_extractor_change(sig, verification_report):
        logger.info(
            f"Strategy: extractor_patch ({bank} layout not representable "
            f"via profile-only settings)"
        )
        return Strategy.EXTRACTOR_PATCH

    # Path 3: Known bank — profile patch should suffice
    logger.info(f"Strategy: profile_patch ({bank} with similar layout)")
    return Strategy.PROFILE_PATCH


def _has_stable_branding(evidence: dict) -> bool:
    """Check if the PDF has consistent bank branding in page text."""
    page_texts = evidence.get("page_texts", {})
    if not page_texts:
        return False
    # Check if page 1 has recognizable bank-like keywords
    page1 = page_texts.get("1", "")
    branding_indicators = [
        "bank", "financial", "account", "statement", "balance",
        "transaction", "credit", "debit",
    ]
    matches = sum(1 for kw in branding_indicators if kw in page1.lower())
    return matches >= 3


def _needs_extractor_change(
    sig: LayoutSignature,
    verification_report: dict,
) -> bool:
    """Determine if the issue requires extractor-level changes.

    Indicators that profile-only changes are insufficient:
    - Tables collapse into 1-2 columns (extractor table-parsing logic at fault)
    - No tables at all but text extraction works (may need prefer_text_extraction flag)
    - Very high missing count relative to total pages (fundamental parsing failure)
    """
    # Collapsed tables → extractor may need to handle differently
    if sig.table_shape == "collapsed":
        return True

    # No tables but decent text → may need prefer_text_extraction support
    if sig.table_shape == "none" and sig.text_extraction_quality == "good":
        return True

    # If more than 80% of transactions are missing, the extractor is
    # fundamentally not parsing this layout
    total_pages = verification_report.get("total_pages", 1)
    total_missing = verification_report.get("total_missing", 0)
    total_extracted = verification_report.get("total_extracted", 1)
    if total_extracted > 0:
        missing_ratio = total_missing / max(total_extracted + total_missing, 1)
        if missing_ratio > 0.8:
            return True

    return False


def get_allowed_files(strategy: Strategy, bank_key: str) -> List[str]:
    """Return the list of files the repair stage is allowed to modify."""
    banks_dir = "src/profiles/banks"
    profile_file = f"{banks_dir}/{bank_key}.py"
    test_file = "tests/test_bank_profiles.py"
    extractor_file = "src/pipeline/pdf_extractor.py"
    init_file = f"{banks_dir}/__init__.py"

    if strategy == Strategy.PROFILE_PATCH:
        return [profile_file, test_file]

    if strategy == Strategy.EXTRACTOR_PATCH:
        return [profile_file, test_file, extractor_file]

    if strategy == Strategy.NEW_PROFILE:
        return [
            f"{banks_dir}/<new_bank>.py",
            init_file,
            test_file,
            "tests/test_api_server.py",
            "tests/fixtures/expected/<new_bank>_regression_statement.json",
        ]

    return []  # manual_review → no auto-edits
