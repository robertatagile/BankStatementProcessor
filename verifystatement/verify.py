#!/usr/bin/env python3
"""Vision Verification Tool for Bank Statement PDF Extraction.

Runs the extraction pipeline on a PDF, sends each page's image + extracted
text to Claude for verification, reports discrepancies, and optionally
auto-fixes bank profiles (creating new ones or patching existing ones)
in a regression-safe loop.

Usage:
    python verifystatement/verify.py --pdf-file tests/fixtures/pdfs/fnb_regression_statement.pdf
    python verifystatement/verify.py --pdf-dir verifystatement/input --auto-fix
"""
from __future__ import annotations

import argparse
import base64
import io
import json
import os
import re
import subprocess
import sys
import textwrap
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

import pdfplumber
from anthropic import Anthropic

# ---------------------------------------------------------------------------
# Ensure the project root is on sys.path so ``src`` imports work when
# this script is executed directly.
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.pipeline.pdf_extractor import PDFExtractorStage
from src.profiles.base import BankProfile
from src.profiles.factory import BankProfileFactory
from src.utils.logger import get_logger

logger = get_logger(__name__)

DEFAULT_MODEL = "claude-sonnet-4-20250514"
MAX_FIX_ATTEMPTS = 10
# Maximum page images to include in new-profile creation prompts.  Each page
# renders at ~300 DPI and is sent as a base64 PNG, so we cap to avoid
# exceeding Claude's context window for very large statements.
MAX_PROFILE_CREATION_PAGES = 10

# ---------------------------------------------------------------------------
# Prompt templates
# ---------------------------------------------------------------------------

VERIFICATION_SYSTEM_PROMPT = textwrap.dedent("""\
    You are verifying bank statement extraction accuracy.

    You receive:
    1. An image of a bank statement page
    2. The transactions our system extracted from this page

    Check every visible transaction against the extracted data:
    - Missing: visible in image but not extracted
    - Incorrect: extracted but wrong date/amount/balance/description/type
    - Extra: extracted but not visible in image
    - For South African statements: Cr=credit, Dr=debit, R=Rand

    Return **only** valid JSON (no markdown fences):
    {
      "page_correct": true/false,
      "missing_transactions": [{"date": "...", "description": "...", "amount": ..., "balance": ..., "transaction_type": "..."}],
      "incorrect_transactions": [{"extracted": {...}, "correct": {...}, "issue": "..."}],
      "extra_transactions": [{"date": "...", "description": "...", "amount": ..., "balance": ..., "transaction_type": "..."}],
      "notes": "observations about layout or formatting"
    }
""")

NEW_PROFILE_SYSTEM_PROMPT = textwrap.dedent("""\
    You are creating a bank profile for a South African bank statement PDF extractor.

    You receive:
    1. Page images from the bank statement
    2. The raw text pdfplumber extracted from page 1
    3. The BankProfile dataclass (base.py)
    4. SA common helpers (_sa_common.py)
    5. An example bank profile module (nedbank.py)
    6. Transaction data Claude identified from the images

    Create a new profile module. It must:
    - Import from src.profiles.banks._sa_common and call sa_base_profile() with overrides
    - Set detection_keywords from bank branding visible in the image
    - Set appropriate date_formats for observed date patterns
    - Set text_line_pattern or default_column_map based on layout
    - Override header_patterns as needed for this bank's format
    - Follow the exact code style of the example profile
    - The factory function must be named {bank_key}_profile and return a BankProfile

    Return **only** valid JSON (no markdown fences):
    {
      "bank_key": "new_bank",
      "module_filename": "new_bank.py",
      "profile_code": "...full Python module source..."
    }
""")

FIX_PROFILE_SYSTEM_PROMPT = textwrap.dedent("""\
    You are fixing a bank statement PDF extractor profile.

    You receive:
    1. The current bank profile Python source code
    2. Extraction errors (missing/incorrect transactions)
    3. The raw text pdfplumber extracted from the problematic pages
    4. [If retry] Previous fix attempt and why it was rejected (test failures)

    Rules:
    - Only modify the profile, NOT the core extractor
    - Must be backward-compatible with existing statements
    - Focus on: regex patterns, date formats, column mappings, text_line_pattern
    - If a previous attempt broke tests, adjust narrowly

    Return **only** valid JSON (no markdown fences):
    {
      "explanation": "what was wrong and how you fixed it",
      "profile_code": "...full updated profile module source..."
    }
""")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _page_to_base64_png(page) -> str:
    """Render a pdfplumber page to a base64-encoded PNG string."""
    page_image = page.to_image(resolution=300)
    pil_image = page_image.original
    buf = io.BytesIO()
    pil_image.save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode("utf-8")


def _extract_lines_for_page(
    extractor: PDFExtractorStage,
    page,
    page_num: int,
    profile: BankProfile,
) -> list[dict]:
    """Replicate the per-page extraction logic from PDFExtractorStage._extract_lines.

    We call the internal helpers of the extractor for a single page so that
    we can attribute lines to specific pages for verification.  The logic
    mirrors ``_extract_lines`` (including OCR supplementation and fallback)
    so that the verification path matches the production extraction path.
    """
    page_lines: list[dict] = []

    if not profile.prefer_text_extraction:
        tables = page.extract_tables()
        if tables:
            for table in tables:
                lines = extractor._parse_table(table, page_num, profile)
                page_lines.extend(lines)

            valid_amounts = sum(
                1 for ln in page_lines
                if ln.get("amount") is not None and not ln.get("_continuation")
            )
            total_lines = sum(
                1 for ln in page_lines if not ln.get("_continuation")
            )

            if total_lines > 0 and valid_amounts >= total_lines * 0.5:
                if profile.name == "Capitec":
                    text = page.extract_text()
                    if text:
                        page_lines = extractor._supplement_capitec_text_lines(
                            page_lines, text, profile
                        )
            else:
                text = page.extract_text()
                if text:
                    text_lines = extractor._parse_text(text, page_num, profile)
                    if text_lines:
                        page_lines = text_lines

    if not page_lines:
        text = page.extract_text()
        if text:
            page_lines = extractor._parse_text(text, page_num, profile)
            # FNB OCR fee-line supplementation (matches production path)
            if page_lines and extractor._enable_ocr:
                page_lines = extractor._supplement_fnb_ocr_fee_lines(
                    page, text, page_lines, page_num, profile
                )

    # OCR fallback for scanned pages (matches production path)
    if not page_lines and extractor._enable_ocr:
        ocr_text = extractor._ocr_page(page)
        if ocr_text:
            page_lines = extractor._parse_text(ocr_text, page_num, profile)

    # Merge multi-line descriptions (matches production post-processing)
    page_lines = extractor._merge_multiline_descriptions(page_lines)

    # Filter balance/summary artifact lines
    page_lines = [
        ln for ln in page_lines
        if not re.match(
            r"(?:Opening\s*Balance|Closing\s*Balance|Available\s+Balance|"
            r"Balance\s+(?:brought|as\s+at)|Saldo\s+[Oo]or(?:gedra|gebring)|"
            r"Afsluitingsaldo|Openingsaldo)\b",
            ln.get("description", ""), re.IGNORECASE,
        )
    ]

    # Infer debit/credit from running balance (matches production post-processing)
    page_lines = PDFExtractorStage._infer_transaction_types(page_lines)

    return page_lines


def _format_lines_as_text(lines: list[dict]) -> str:
    """Format extracted transaction lines as a readable text table."""
    if not lines:
        return "(no transactions extracted for this page)"

    header = f"{'Date':<12} {'Description':<50} {'Amount':>12} {'Balance':>12} {'Type':<8}"
    separator = "-" * len(header)
    rows = [header, separator]
    for ln in lines:
        date = ln.get("date", "")
        desc = (ln.get("description", "") or "")[:50]
        amt = ln.get("amount")
        bal = ln.get("balance")
        ttype = ln.get("transaction_type", "")
        amt_str = f"{amt:.2f}" if amt is not None else ""
        bal_str = f"{bal:.2f}" if bal is not None else ""
        rows.append(f"{date:<12} {desc:<50} {amt_str:>12} {bal_str:>12} {ttype:<8}")
    return "\n".join(rows)


def _read_source_file(path: Path) -> str:
    """Read a source file and return its contents."""
    return path.read_text(encoding="utf-8")


def _parse_json_response(text: str) -> dict:
    """Extract JSON from a Claude response, stripping markdown fences if present."""
    cleaned = text.strip()
    # Strip ```json ... ``` fences
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        # Remove first and last fence lines
        start = 1
        end = len(lines)
        for i in range(len(lines) - 1, 0, -1):
            if lines[i].strip().startswith("```"):
                end = i
                break
        cleaned = "\n".join(lines[start:end])
    return json.loads(cleaned)


class _DecimalEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal values."""

    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super().default(o)


# ---------------------------------------------------------------------------
# Phase 1: Verification
# ---------------------------------------------------------------------------

def verify_pdf(
    pdf_path: str,
    client: Anthropic,
    model: str = DEFAULT_MODEL,
) -> dict:
    """Run extraction and Claude-based verification on a PDF.

    Returns a report dict with per-page results and aggregated discrepancies.
    """
    pdf_path = Path(pdf_path)
    logger.info(f"Verifying: {pdf_path}")

    with pdfplumber.open(str(pdf_path)) as pdf:
        # Auto-detect bank profile
        page1_text = pdf.pages[0].extract_text() or ""
        profile = BankProfileFactory.detect(page1_text)
        bank_name = profile.name
        logger.info(f"Detected bank: {bank_name}")

        # Create extractor for per-page line extraction (OCR enabled to match production)
        extractor = PDFExtractorStage(profile=profile, auto_detect=False, enable_ocr=True)

        # Also run the full pipeline extraction for the complete result
        full_lines, extraction_method = extractor._extract_lines(pdf, profile)

        page_results = []
        all_missing = []
        all_incorrect = []
        all_extra = []

        for page_num, page in enumerate(pdf.pages, 1):
            logger.info(f"  Verifying page {page_num}/{len(pdf.pages)}...")

            # Extract lines for this specific page
            page_lines = _extract_lines_for_page(extractor, page, page_num, profile)

            # Render page as image
            page_b64 = _page_to_base64_png(page)

            # Format extracted lines
            lines_text = _format_lines_as_text(page_lines)

            # Send to Claude for verification
            user_content = [
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/png",
                        "data": page_b64,
                    },
                },
                {
                    "type": "text",
                    "text": (
                        f"Page {page_num} of {len(pdf.pages)} — Bank: {bank_name}\n\n"
                        f"Extracted transactions:\n{lines_text}"
                    ),
                },
            ]

            response = client.messages.create(
                model=model,
                max_tokens=4096,
                system=VERIFICATION_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_content}],
            )

            try:
                result = _parse_json_response(response.content[0].text)
            except (json.JSONDecodeError, IndexError, KeyError) as exc:
                logger.warning(f"  Page {page_num}: failed to parse Claude response: {exc}")
                result = {
                    "page_correct": None,
                    "missing_transactions": [],
                    "incorrect_transactions": [],
                    "extra_transactions": [],
                    "notes": f"Parse error: {exc}",
                }

            result["page_number"] = page_num
            result["extracted_count"] = len(page_lines)
            page_results.append(result)

            all_missing.extend(result.get("missing_transactions", []))
            all_incorrect.extend(result.get("incorrect_transactions", []))
            all_extra.extend(result.get("extra_transactions", []))

    report = {
        "pdf_file": str(pdf_path),
        "bank": bank_name,
        "extraction_method": extraction_method,
        "total_extracted": len(full_lines),
        "total_pages": len(page_results),
        "pages_correct": sum(1 for p in page_results if p.get("page_correct")),
        "pages_with_issues": sum(1 for p in page_results if not p.get("page_correct")),
        "total_missing": len(all_missing),
        "total_incorrect": len(all_incorrect),
        "total_extra": len(all_extra),
        "page_results": page_results,
        "aggregated": {
            "missing_transactions": all_missing,
            "incorrect_transactions": all_incorrect,
            "extra_transactions": all_extra,
        },
        "timestamp": datetime.now().isoformat(),
    }

    return report


# ---------------------------------------------------------------------------
# Regression test runner
# ---------------------------------------------------------------------------

def run_bank_regression_tests(bank_name: str) -> tuple[bool, str]:
    """Run pytest for a specific bank. Returns (passed, output).

    Uses the bank *name* (from the profile, e.g. "Discovery Bank") to find
    relevant regression tests.  Falls back to the bank registry key if no
    match is found by name.  Detects the "no tests selected" case so callers
    know when there is no regression coverage.
    """
    # Normalise to a pytest -k expression that will match test function names.
    # Test names use the pattern: test_{bank}_statement_regression_...
    # Bank registry keys: fnb, absa, capitec, nedbank, discovery_bank, etc.
    # We try multiple variants to maximise hit rate.
    name_variants = {bank_name}
    # Add lowered/underscored form
    normalised = bank_name.lower().replace(" ", "_").replace("-", "_")
    name_variants.add(normalised)
    # Strip trailing "_bank" — e.g. "discovery_bank" → "discovery"
    if normalised.endswith("_bank"):
        name_variants.add(normalised[:-5])
    # Build a pytest -k expression with OR
    k_expr = " or ".join(sorted(name_variants))

    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/test_api_server.py",
         "-k", k_expr, "-v", "--tb=short"],
        capture_output=True, text=True,
        cwd=str(PROJECT_ROOT),
    )
    combined = result.stdout + result.stderr
    # Detect pytest's "no tests ran" exit code (5) or explicit message
    no_tests = (
        result.returncode == 5
        or "no tests ran" in combined.lower()
        or "0 selected" in combined.lower()
    )
    if no_tests:
        logger.warning(
            f"  No regression tests matched for '{bank_name}' "
            f"(k_expr='{k_expr}')"
        )
    return result.returncode == 0, combined


# ---------------------------------------------------------------------------
# Phase 2a: Create new bank profile
# ---------------------------------------------------------------------------

def _create_new_profile(
    pdf_path: Path,
    report: dict,
    client: Anthropic,
    model: str,
    max_attempts: int,
) -> dict:
    """Create a new bank profile from scratch for an unknown bank."""
    logger.info("Phase 2a: Creating new bank profile...")

    # Read reference files
    base_py = _read_source_file(PROJECT_ROOT / "src" / "profiles" / "base.py")
    sa_common = _read_source_file(PROJECT_ROOT / "src" / "profiles" / "banks" / "_sa_common.py")
    nedbank_example = _read_source_file(PROJECT_ROOT / "src" / "profiles" / "banks" / "nedbank.py")

    # Collect page images and raw text (capped at MAX_PROFILE_CREATION_PAGES
    # to stay within Claude's context window for large statements)
    with pdfplumber.open(str(pdf_path)) as pdf:
        page1_text = pdf.pages[0].extract_text() or ""
        pages_to_send = pdf.pages[:MAX_PROFILE_CREATION_PAGES]
        if len(pdf.pages) > MAX_PROFILE_CREATION_PAGES:
            logger.info(
                f"  Statement has {len(pdf.pages)} pages; sending first "
                f"{MAX_PROFILE_CREATION_PAGES} page images for profile creation"
            )
        page_images = []
        for page in pages_to_send:
            page_images.append({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": "image/png",
                    "data": _page_to_base64_png(page),
                },
            })

    # Corrected transaction data from Phase 1
    corrected_data = json.dumps(report["aggregated"], indent=2, cls=_DecimalEncoder)

    user_content = page_images + [
        {
            "type": "text",
            "text": (
                f"Raw text from page 1:\n```\n{page1_text}\n```\n\n"
                f"BankProfile dataclass (base.py):\n```python\n{base_py}\n```\n\n"
                f"SA common helpers (_sa_common.py):\n```python\n{sa_common}\n```\n\n"
                f"Example profile (nedbank.py):\n```python\n{nedbank_example}\n```\n\n"
                f"Corrected transaction data from verification:\n```json\n{corrected_data}\n```"
            ),
        },
    ]

    profile_path: Optional[Path] = None
    original_init_code: Optional[str] = None

    for attempt in range(1, max_attempts + 1):
        logger.info(f"  Attempt {attempt}/{max_attempts}...")

        response = client.messages.create(
            model=model,
            max_tokens=8192,
            system=NEW_PROFILE_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_content}],
        )

        try:
            result = _parse_json_response(response.content[0].text)
        except (json.JSONDecodeError, IndexError) as exc:
            logger.warning(f"  Failed to parse response: {exc}")
            continue

        bank_key = result.get("bank_key", "unknown_bank")
        module_filename = result.get("module_filename", f"{bank_key}.py")
        profile_code = result.get("profile_code", "")

        if not profile_code:
            logger.warning("  Empty profile code returned")
            continue

        # Write the new profile module
        profile_path = PROJECT_ROOT / "src" / "profiles" / "banks" / module_filename
        init_path = PROJECT_ROOT / "src" / "profiles" / "banks" / "__init__.py"
        original_init_code = init_path.read_text(encoding="utf-8")

        try:
            profile_path.write_text(profile_code, encoding="utf-8")
            logger.info(f"  Wrote: {profile_path}")

            # Register in __init__.py — append import + register at end of
            # register_all() body to avoid fragile mid-function insertion.
            func_name = f"{bank_key}_profile"
            import_line = f"    from src.profiles.banks.{bank_key} import {func_name}"
            register_line = f'    BankProfileFactory.register("{bank_key}", {func_name})'

            init_code = original_init_code
            needs_update = import_line not in init_code
            if needs_update:
                init_code = init_code.rstrip() + f"\n\n{import_line}\n{register_line}\n"
                init_path.write_text(init_code, encoding="utf-8")
                logger.info(f"  Updated: {init_path}")
        except Exception as exc:
            # Rollback: remove the profile file and restore __init__.py
            logger.warning(f"  Profile creation/registration failed: {exc}")
            if profile_path.exists():
                profile_path.unlink()
            init_path.write_text(original_init_code, encoding="utf-8")
            BankProfileFactory._registry.clear()
            continue

        # Clear the factory registry to force re-registration
        BankProfileFactory._registry.clear()

        # Re-verify extraction with the new profile
        try:
            re_report = verify_pdf(str(pdf_path), client, model)
            issues = (
                re_report["total_missing"]
                + re_report["total_incorrect"]
                + re_report["total_extra"]
            )
            if issues == 0:
                logger.info("  New profile verified successfully!")
                return {
                    "status": "success",
                    "bank_key": bank_key,
                    "profile_path": str(profile_path),
                    "attempts": attempt,
                }
            else:
                logger.info(
                    f"  Still {issues} issue(s) — "
                    f"missing={re_report['total_missing']}, "
                    f"incorrect={re_report['total_incorrect']}, "
                    f"extra={re_report['total_extra']}"
                )
                # Feed issues back for next attempt
                user_content = page_images + [
                    {
                        "type": "text",
                        "text": (
                            f"Previous profile code:\n```python\n{profile_code}\n```\n\n"
                            f"Still has issues:\n```json\n{json.dumps(re_report['aggregated'], indent=2, cls=_DecimalEncoder)}\n```\n\n"
                            f"Raw text from page 1:\n```\n{page1_text}\n```\n\n"
                            f"BankProfile dataclass (base.py):\n```python\n{base_py}\n```\n\n"
                            f"SA common helpers (_sa_common.py):\n```python\n{sa_common}\n```\n\n"
                            f"Example profile (nedbank.py):\n```python\n{nedbank_example}\n```\n\n"
                            f"Fix the profile to resolve the remaining issues."
                        ),
                    },
                ]
        except Exception as exc:
            logger.warning(f"  Re-verification failed: {exc}")
            # Feed error back
            user_content = page_images + [
                {
                    "type": "text",
                    "text": (
                        f"Previous profile code caused an error:\n```\n{exc}\n```\n\n"
                        f"Raw text from page 1:\n```\n{page1_text}\n```\n\n"
                        f"BankProfile dataclass (base.py):\n```python\n{base_py}\n```\n\n"
                        f"SA common helpers (_sa_common.py):\n```python\n{sa_common}\n```\n\n"
                        f"Example profile (nedbank.py):\n```python\n{nedbank_example}\n```\n\n"
                        f"Fix the profile code to resolve the error."
                    ),
                },
            ]

    # All attempts exhausted — rollback generated profile and __init__.py
    if profile_path and profile_path.exists():
        logger.warning("  Removing generated profile after failed attempts")
        profile_path.unlink()
    init_path = PROJECT_ROOT / "src" / "profiles" / "banks" / "__init__.py"
    if original_init_code:
        init_path.write_text(original_init_code, encoding="utf-8")
    BankProfileFactory._registry.clear()
    return {"status": "failed", "attempts": max_attempts}


# ---------------------------------------------------------------------------
# Phase 2b: Fix existing profile (regression-safe)
# ---------------------------------------------------------------------------

def _find_profile_source_path(bank_name: str) -> Optional[Path]:
    """Locate the profile source file for a given bank name."""
    banks_dir = PROJECT_ROOT / "src" / "profiles" / "banks"
    # Try exact match on bank_name lowered/underscored
    candidate = bank_name.lower().replace(" ", "_").replace("-", "_")
    path = banks_dir / f"{candidate}.py"
    if path.exists():
        return path

    # Search all profile modules for matching detection_keywords or name
    for py_file in banks_dir.glob("*.py"):
        if py_file.name.startswith("_") or py_file.name == "__init__.py":
            continue
        content = py_file.read_text(encoding="utf-8")
        if f'name="{bank_name}"' in content or f"name='{bank_name}'" in content:
            return py_file

    return None


def _find_bank_key(bank_name: str) -> Optional[str]:
    """Find the registry key for a bank by name."""
    BankProfileFactory._ensure_registered()
    for key, factory_fn in BankProfileFactory._registry.items():
        profile = factory_fn()
        if profile.name.lower() == bank_name.lower():
            return key
    return None


def _fix_existing_profile(
    pdf_path: Path,
    report: dict,
    client: Anthropic,
    model: str,
    max_attempts: int,
) -> dict:
    """Fix an existing bank profile in a regression-safe loop."""
    bank_name = report["bank"]
    logger.info(f"Phase 2b: Fixing profile for {bank_name}...")

    profile_path = _find_profile_source_path(bank_name)
    if not profile_path:
        logger.error(f"  Could not find profile source for '{bank_name}'")
        return {"status": "failed", "reason": "profile_not_found"}

    bank_key = _find_bank_key(bank_name)
    if not bank_key:
        logger.error(f"  Could not find registry key for '{bank_name}'")
        return {"status": "failed", "reason": "bank_key_not_found"}

    # Snapshot current profile code (backup)
    original_code = _read_source_file(profile_path)
    logger.info(f"  Profile: {profile_path}")

    # Baseline: run existing regression tests (must all pass)
    logger.info("  Running baseline regression tests...")
    baseline_passed, baseline_output = run_bank_regression_tests(bank_key)
    no_tests = (
        "no tests ran" in baseline_output.lower()
        or "0 selected" in baseline_output.lower()
    )
    if no_tests:
        logger.error(
            f"  No regression tests found for '{bank_key}' — "
            f"cannot guarantee regression safety. Aborting auto-fix."
        )
        return {"status": "failed", "reason": "no_regression_tests"}
    if not baseline_passed:
        logger.error(
            "  Baseline regression tests already failing — "
            "cannot safely modify profile. Fix existing failures first."
        )
        return {"status": "failed", "reason": "baseline_tests_failing"}

    # Collect raw pdfplumber text from problematic pages
    with pdfplumber.open(str(pdf_path)) as pdf:
        raw_texts = {}
        for page_num, page in enumerate(pdf.pages, 1):
            text = page.extract_text() or ""
            if text:
                raw_texts[page_num] = text

    discrepancies = json.dumps(report["aggregated"], indent=2, cls=_DecimalEncoder)
    raw_text_str = "\n\n".join(
        f"--- Page {pn} ---\n{txt}" for pn, txt in raw_texts.items()
    )

    previous_attempt_info = ""

    for attempt in range(1, max_attempts + 1):
        logger.info(f"  Fix attempt {attempt}/{max_attempts}...")

        current_code = _read_source_file(profile_path)

        user_prompt = (
            f"Current profile source ({profile_path.name}):\n"
            f"```python\n{current_code}\n```\n\n"
            f"Extraction errors:\n```json\n{discrepancies}\n```\n\n"
            f"Raw pdfplumber text from statement:\n```\n{raw_text_str}\n```"
        )

        if previous_attempt_info:
            user_prompt += f"\n\n{previous_attempt_info}"

        response = client.messages.create(
            model=model,
            max_tokens=8192,
            system=FIX_PROFILE_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )

        try:
            result = _parse_json_response(response.content[0].text)
        except (json.JSONDecodeError, IndexError) as exc:
            logger.warning(f"  Failed to parse fix response: {exc}")
            continue

        explanation = result.get("explanation", "")
        new_code = result.get("profile_code", "")
        logger.info(f"  Fix explanation: {explanation}")

        if not new_code:
            logger.warning("  Empty profile code returned")
            continue

        # Apply fix
        profile_path.write_text(new_code, encoding="utf-8")

        # Clear factory registry to pick up changes
        BankProfileFactory._registry.clear()

        # Run ALL existing regression tests for this bank
        logger.info("  Running regression tests...")
        tests_passed, test_output = run_bank_regression_tests(bank_key)

        if not tests_passed:
            logger.warning("  Regression tests FAILED — reverting")
            profile_path.write_text(original_code, encoding="utf-8")
            BankProfileFactory._registry.clear()

            previous_attempt_info = (
                f"Previous fix attempt (rejected due to test failures):\n"
                f"```python\n{new_code}\n```\n\n"
                f"Test failure output:\n```\n{test_output[-3000:]}\n```\n\n"
                f"Please provide a more conservative fix that won't break existing tests."
            )
            continue

        # Regression tests pass — re-verify the new PDF
        logger.info("  Regression tests passed. Re-verifying new PDF...")
        try:
            re_report = verify_pdf(str(pdf_path), client, model)
            issues = (
                re_report["total_missing"]
                + re_report["total_incorrect"]
                + re_report["total_extra"]
            )
            if issues == 0:
                logger.info("  Fix verified successfully!")
                return {
                    "status": "success",
                    "bank": bank_name,
                    "profile_path": str(profile_path),
                    "explanation": explanation,
                    "attempts": attempt,
                }
            else:
                logger.info(f"  Still {issues} issue(s) remaining — iterating...")
                discrepancies = json.dumps(
                    re_report["aggregated"], indent=2, cls=_DecimalEncoder
                )
                previous_attempt_info = ""
        except Exception as exc:
            logger.warning(f"  Re-verification failed: {exc}")

    # Exhausted attempts — revert to original
    logger.warning("  Max attempts reached — reverting to original profile")
    profile_path.write_text(original_code, encoding="utf-8")
    BankProfileFactory._registry.clear()
    return {"status": "failed", "bank": bank_name, "attempts": max_attempts}


# ---------------------------------------------------------------------------
# Phase 2: Auto-fix dispatcher
# ---------------------------------------------------------------------------

def auto_fix(
    pdf_path: str,
    report: dict,
    client: Anthropic,
    model: str = DEFAULT_MODEL,
    max_attempts: int = MAX_FIX_ATTEMPTS,
) -> dict:
    """Dispatch auto-fix: create new profile or fix existing one."""
    pdf_path = Path(pdf_path)
    bank = report.get("bank", "Generic")
    has_issues = (
        report["total_missing"] + report["total_incorrect"] + report["total_extra"]
    ) > 0

    if not has_issues:
        logger.info("No issues found — nothing to fix.")
        return {"status": "no_issues"}

    if bank == "Generic":
        return _create_new_profile(pdf_path, report, client, model, max_attempts)
    else:
        return _fix_existing_profile(pdf_path, report, client, model, max_attempts)


# ---------------------------------------------------------------------------
# Report I/O
# ---------------------------------------------------------------------------

def save_report(report: dict, report_dir: Path, pdf_path: Path) -> Path:
    """Save a verification report as JSON."""
    report_dir.mkdir(parents=True, exist_ok=True)
    stem = pdf_path.stem
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = report_dir / f"{stem}_verify_{timestamp}.json"
    report_file.write_text(
        json.dumps(report, indent=2, cls=_DecimalEncoder, ensure_ascii=False),
        encoding="utf-8",
    )
    return report_file


def print_summary(report: dict) -> None:
    """Print a human-readable summary of the verification report."""
    print(f"\n{'=' * 60}")
    print(f"  Verification Report: {report['pdf_file']}")
    print(f"{'=' * 60}")
    print(f"  Bank:              {report['bank']}")
    print(f"  Extraction method: {report['extraction_method']}")
    print(f"  Total extracted:   {report['total_extracted']}")
    print(f"  Pages verified:    {report['total_pages']}")
    print(f"  Pages correct:     {report['pages_correct']}")
    print(f"  Pages with issues: {report['pages_with_issues']}")
    print(f"  Missing txns:      {report['total_missing']}")
    print(f"  Incorrect txns:    {report['total_incorrect']}")
    print(f"  Extra txns:        {report['total_extra']}")

    if report["total_missing"] + report["total_incorrect"] + report["total_extra"] == 0:
        print(f"\n  *** ALL TRANSACTIONS VERIFIED CORRECTLY ***")
    else:
        if report["aggregated"]["missing_transactions"]:
            print(f"\n  Missing transactions:")
            for txn in report["aggregated"]["missing_transactions"]:
                print(f"    - {txn.get('date', '?')} | {txn.get('description', '?')} | {txn.get('amount', '?')}")

        if report["aggregated"]["incorrect_transactions"]:
            print(f"\n  Incorrect transactions:")
            for txn in report["aggregated"]["incorrect_transactions"]:
                print(f"    - {txn.get('issue', '?')}")

        if report["aggregated"]["extra_transactions"]:
            print(f"\n  Extra transactions:")
            for txn in report["aggregated"]["extra_transactions"]:
                print(f"    - {txn.get('date', '?')} | {txn.get('description', '?')} | {txn.get('amount', '?')}")

    print(f"\n  Timestamp: {report['timestamp']}")
    print(f"{'=' * 60}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify bank statement PDF extraction accuracy using Claude vision."
    )
    parser.add_argument(
        "--pdf-file",
        type=str,
        default=None,
        help="Path to a single PDF file to verify",
    )
    parser.add_argument(
        "--pdf-dir",
        type=str,
        default=None,
        help="Directory of PDF files to verify",
    )
    parser.add_argument(
        "--auto-fix",
        action="store_true",
        help="Enable auto-fix mode (modifies bank profiles)",
    )
    parser.add_argument(
        "--support-loop",
        action="store_true",
        help="Run the task-driven support loop: discover → classify → repair → validate → learn",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume the most recent support-loop run for the given PDF",
    )
    parser.add_argument(
        "--inspect-run",
        type=str,
        default=None,
        help="Inspect the state of a support-loop run directory",
    )
    parser.add_argument(
        "--archive-run",
        type=str,
        default=None,
        help="Archive a completed support-loop run directory",
    )
    parser.add_argument(
        "--list-runs",
        action="store_true",
        help="List all support-loop runs",
    )
    parser.add_argument(
        "--report-dir",
        type=str,
        default=str(PROJECT_ROOT / "verifystatement" / "reports"),
        help="Directory to save JSON reports",
    )
    parser.add_argument(
        "--model",
        type=str,
        default=DEFAULT_MODEL,
        help=f"Claude model to use (default: {DEFAULT_MODEL})",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=MAX_FIX_ATTEMPTS,
        help=f"Max auto-fix iterations (default: {MAX_FIX_ATTEMPTS})",
    )

    args = parser.parse_args()

    # -- Support-loop management commands (no PDF needed) --
    if args.list_runs:
        from verifystatement.support_loop import list_runs

        runs = list_runs()
        if not runs:
            print("No support-loop runs found.")
        else:
            print(f"{'Run ID':<45} {'Status':<15} {'Bank':<20} {'Attempts'}")
            print("-" * 90)
            for r in runs:
                print(
                    f"{r['run_id']:<45} {r['status']:<15} "
                    f"{r.get('detected_bank', ''):<20} "
                    f"{r.get('attempt_count', 0)}"
                )
        return

    if args.inspect_run:
        from verifystatement.support_loop import inspect_run

        info = inspect_run(args.inspect_run)
        print(json.dumps(info, indent=2))
        return

    if args.archive_run:
        from verifystatement.support_loop import archive_run

        dest = archive_run(args.archive_run)
        print(f"Archived to: {dest}")
        return

    # -- Modes that require a PDF --
    if not args.pdf_file and not args.pdf_dir:
        parser.error("Provide --pdf-file or --pdf-dir")

    # API key
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY environment variable not set")
        sys.exit(1)

    client = Anthropic(api_key=api_key)
    report_dir = Path(args.report_dir)

    # Collect PDFs
    if args.pdf_file:
        pdf_files = [Path(args.pdf_file)]
    else:
        pdf_files = sorted(Path(args.pdf_dir).glob("*.pdf"))

    if not pdf_files:
        print("No PDF files found.")
        sys.exit(1)

    print(f"Found {len(pdf_files)} PDF(s) to process\n")

    # -- Support-loop mode --
    if args.support_loop:
        from verifystatement.support_loop import run_support_loop

        for pdf_path in pdf_files:
            result = run_support_loop(
                str(pdf_path),
                client=client,
                model=args.model,
                max_attempts=args.max_attempts,
                resume=args.resume,
            )
            print(f"\n  Support-loop result: {result.get('status', 'unknown')}")
            print(f"  Run directory: {result.get('run_dir', '?')}")
        return

    # -- Default: verify (+ optional auto-fix) --
    for pdf_path in pdf_files:
        # Phase 1: Verify
        report = verify_pdf(str(pdf_path), client, args.model)
        print_summary(report)

        # Save report
        report_file = save_report(report, report_dir, pdf_path)
        print(f"  Report saved: {report_file}")

        # Phase 2: Auto-fix (legacy compatibility path)
        if args.auto_fix:
            fix_result = auto_fix(
                str(pdf_path), report, client, args.model, args.max_attempts,
            )
            print(f"\n  Auto-fix result: {fix_result.get('status', 'unknown')}")
            if fix_result.get("status") == "success":
                print(f"  Profile: {fix_result.get('profile_path', '?')}")
                print(f"  Attempts: {fix_result.get('attempts', '?')}")


if __name__ == "__main__":
    main()
