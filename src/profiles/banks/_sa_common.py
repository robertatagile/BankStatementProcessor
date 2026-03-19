"""Shared South African bank profile helpers.

This module contains common patterns, date formats, column keywords, and the
base profile factory used by all SA bank profiles.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List

from src.profiles.base import BankProfile


def sa_header_patterns() -> Dict[str, re.Pattern]:
    """Shared South African bank header patterns.

    Handles both spaced (``Statement Period : 31 March 2025 to 30 April 2025``)
    and no-space (``StatementPeriod:31January2025to28February2025``) layouts
    that FNB PDFs produce depending on the rendering.
    """
    return {
        "bank_name": re.compile(
            r"^(.+?(?:African\s+Bank|Bank|Capitec|ABSA|FNB|Nedbank))", re.IGNORECASE
        ),
        "account_number": re.compile(
            r"(?:Account\s*(?:Number|No\.?|#)?)\s*[:\-]?\s*(\d[\d\s\-]{6,})",
            re.IGNORECASE,
        ),
        "branch_code": re.compile(
            r"(?:Branch\s*(?:Code)?)\s*[:\-]?\s*(\d{4,6})", re.IGNORECASE
        ),
        "period_start": re.compile(
            r"(?:Statement\s*Period|Statement\s*Date|Statement\s*From)\s*[:\-]?\s*"
            r"(\d{1,2}\s*(?:[A-Za-z]+|\d{1,2})\s*[\s\/\-]?\s*\d{2,4})",
            re.IGNORECASE,
        ),
        "period_end": re.compile(
            r"(?:to|ending|through)\s*[:\-]?\s*"
            r"(\d{1,2}\s*(?:[A-Za-z]+|\d{1,2})\s*[\s\/\-]?\s*\d{2,4})",
            re.IGNORECASE,
        ),
        "opening_balance": re.compile(
            r"(?:Opening|Start|Beginning|Brought\s+Forward)\s*(?:Balance)?\s*"
            r"[:\-]?\s*R?\s*([\d\s,]+\.\d{2})",
            re.IGNORECASE,
        ),
        "closing_balance": re.compile(
            r"(?:Closing|End|Ending|Carried\s+Forward)\s*(?:Balance)?\s*"
            r"[:\-]?\s*R?\s*([\d\s,]+\.\d{2})",
            re.IGNORECASE,
        ),
    }


def sa_date_formats() -> List[str]:
    """Date formats common to South African bank statements.

    Includes no-space variants (e.g. ``31January2025``) produced by some
    FNB PDF renderings where spaces between fields are missing.
    """
    return [
        "%d/%m/%Y",
        "%d %B %Y",
        "%d %b %Y",
        "%d%B%Y",       # no-space: 31January2025
        "%d-%m-%Y",
        "%Y-%m-%d",
        "%d/%m/%y",
        "%d-%m-%y",
    ]


def sa_column_keywords() -> Dict[str, List[str]]:
    """Column keywords common to South African bank statements."""
    return {
        "date": ["date", "transaction date", "trans date"],
        "description": [
            "description",
            "details",
            "particulars",
            "narrative",
            "transaction description",
        ],
        "debit": ["debit", "withdrawal", "dr", "debit (r)", "money out"],
        "credit": ["credit", "deposit", "cr", "credit (r)", "money in"],
        "balance": ["balance", "running balance", "available balance"],
        "amount": ["amount", "transaction amount"],
    }


def sa_text_line_pattern() -> str:
    """Text-based extraction pattern for SA bank statements."""
    return (
        r"^(\d{4}-\d{2}-\d{2}|\d{1,2}[\/\-\s](?:\w+|\d{1,2})[\/\-\s]\d{2,4})\s+"
        r"(.+?)\s+"
        r"(-?R?\s?[\d\s,]+\.\d{2})"
        r"(?:\s+(-?R?\s?[\d\s,]+\.\d{2}))?"
    )


def sa_base_profile(**overrides: Any) -> BankProfile:
    """Create a BankProfile with shared South African defaults."""
    defaults = dict(
        currency_symbol="R",
        thousands_separator=" ",
        header_patterns=sa_header_patterns(),
        date_formats=sa_date_formats(),
        column_keywords=sa_column_keywords(),
        default_column_map={"date": 0, "description": 1, "debit": 2, "credit": 3, "balance": 4},
        text_line_pattern=sa_text_line_pattern(),
        unsigned_is_debit=True,
    )
    defaults.update(overrides)
    return BankProfile(**defaults)
