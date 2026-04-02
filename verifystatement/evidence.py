"""Evidence collection for the support loop.

Extracts structured evidence from a PDF: page text, table samples,
detected profile, extracted lines, layout signature, and discrepancy
report. Produces artifacts suitable for strategy selection and repair
prompts without requiring Claude.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.pipeline.pdf_extractor import PDFExtractorStage
from src.profiles.base import BankProfile
from src.profiles.factory import BankProfileFactory
from src.utils.logger import get_logger
from verifystatement.task_state import LayoutSignature

logger = get_logger(__name__)

# Date format families for layout signature detection
DATE_FAMILIES = {
    r"\d{2}\s+\w{3}\s+\d{4}": "dd MMM yyyy",
    r"\d{2}\s+\w{3}": "dd MMM",
    r"\d{2}/\d{2}/\d{4}": "dd/MM/yyyy",
    r"\d{4}-\d{2}-\d{2}": "yyyy-MM-dd",
    r"\d{2}-\d{2}-\d{4}": "dd-MM-yyyy",
    r"\d{2}/\d{2}/\d{2}": "dd/MM/yy",
}


def detect_date_family(text: str) -> str:
    """Identify the predominant date format family in page text."""
    best_family = ""
    best_count = 0
    for pattern, family in DATE_FAMILIES.items():
        count = len(re.findall(pattern, text))
        if count > best_count:
            best_count = count
            best_family = family
    return best_family


def detect_table_shape(tables: list) -> str:
    """Classify table structure from pdfplumber extract_tables() output."""
    if not tables:
        return "none"
    max_cols = max(len(row) for table in tables for row in table if row)
    if max_cols <= 2:
        return "collapsed"
    if max_cols <= 4:
        return "narrow"
    return "multi_column"


def detect_header_columns(tables: list) -> List[str]:
    """Extract header column names from the first table row."""
    if not tables:
        return []
    for table in tables:
        if table and table[0]:
            return [str(c).strip() for c in table[0] if c and str(c).strip()]
    return []


def assess_text_quality(text: str, line_count: int) -> str:
    """Rate text extraction quality relative to expected content."""
    if not text or len(text.strip()) < 50:
        return "poor"
    # Check for garbled characters (common in OCR failures)
    non_ascii_ratio = sum(1 for c in text if ord(c) > 127) / max(len(text), 1)
    if non_ascii_ratio > 0.15:
        return "poor"
    if line_count == 0 and len(text) > 200:
        return "partial"
    return "good"


def collect_evidence(pdf_path: Path) -> Dict[str, Any]:
    """Collect structured evidence from a PDF without calling Claude.

    Returns a dict with: page_texts, table_samples, detected_profile,
    extracted_lines, layout_signature, and raw metadata.
    """
    pdf_path = Path(pdf_path)
    evidence: Dict[str, Any] = {
        "pdf_path": str(pdf_path),
        "page_count": 0,
        "detected_bank": "",
        "bank_key": "",
        "is_generic": False,
        "page_texts": {},
        "page_text_snippets": {},
        "table_samples": {},
        "extracted_lines_per_page": {},
        "total_extracted_lines": 0,
        "extraction_method": "",
        "layout_signature": {},
    }

    with pdfplumber.open(str(pdf_path)) as pdf:
        evidence["page_count"] = len(pdf.pages)

        # -- Detect profile --
        page1_text = pdf.pages[0].extract_text() or ""
        profile = BankProfileFactory.detect(page1_text)
        evidence["detected_bank"] = profile.name
        evidence["is_generic"] = profile.name == "Generic"

        # Find the bank key
        bank_key = _find_bank_key(profile.name)
        evidence["bank_key"] = bank_key or ""

        # -- Page text and tables --
        all_tables: list = []
        full_text = ""
        for page_num, page in enumerate(pdf.pages, 1):
            text = page.extract_text() or ""
            evidence["page_texts"][str(page_num)] = text
            # Keep first 500 chars as snippet
            evidence["page_text_snippets"][str(page_num)] = text[:500]
            full_text += text + "\n"

            tables = page.extract_tables()
            if tables:
                # Store first 5 rows of each table as sample
                samples = []
                for table in tables:
                    samples.append(table[:5])
                    all_tables.append(table)
                evidence["table_samples"][str(page_num)] = samples

        # -- Extract lines via production path --
        extractor = PDFExtractorStage(
            profile=profile, auto_detect=False, enable_ocr=True
        )
        full_lines, extraction_method = extractor._extract_lines(pdf, profile)
        evidence["total_extracted_lines"] = len(full_lines)
        evidence["extraction_method"] = extraction_method

        # Per-page extraction using the verify helper
        from verifystatement.verify import _extract_lines_for_page

        for page_num, page in enumerate(pdf.pages, 1):
            page_lines = _extract_lines_for_page(extractor, page, page_num, profile)
            evidence["extracted_lines_per_page"][str(page_num)] = [
                _serialize_line(ln) for ln in page_lines
            ]

        # -- Layout signature --
        sig = LayoutSignature(
            date_family=detect_date_family(full_text),
            header_columns=detect_header_columns(all_tables),
            table_shape=detect_table_shape(all_tables),
            text_extraction_quality=assess_text_quality(
                page1_text, len(full_lines)
            ),
            detected_bank=profile.name,
            is_generic=evidence["is_generic"],
        )
        # Hint: if tables collapse to 1-2 columns, text extraction may be needed
        if sig.table_shape in ("collapsed", "none") and sig.text_extraction_quality == "good":
            sig.preferred_strategy_hint = "text_extraction_preferred"
        evidence["layout_signature"] = {
            "date_family": sig.date_family,
            "header_columns": sig.header_columns,
            "table_shape": sig.table_shape,
            "text_extraction_quality": sig.text_extraction_quality,
            "preferred_strategy_hint": sig.preferred_strategy_hint,
            "detected_bank": sig.detected_bank,
            "is_generic": sig.is_generic,
        }

    return evidence


def _serialize_line(line: dict) -> dict:
    """Convert a transaction line dict to JSON-safe types."""
    out = {}
    for k, v in line.items():
        if hasattr(v, "as_integer_ratio"):  # Decimal / float
            out[k] = float(v)
        else:
            out[k] = v
    return out


def _find_bank_key(bank_name: str) -> Optional[str]:
    """Find the registry key for a bank by name."""
    BankProfileFactory._ensure_registered()
    for key, factory_fn in BankProfileFactory._registry.items():
        profile = factory_fn()
        if profile.name.lower() == bank_name.lower():
            return key
    return None


def build_layout_signature(evidence: dict) -> LayoutSignature:
    """Reconstruct a LayoutSignature from a stored evidence dict."""
    sig_data = evidence.get("layout_signature", {})
    return LayoutSignature(**sig_data)
