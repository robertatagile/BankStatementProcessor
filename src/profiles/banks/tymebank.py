"""TymeBank profile."""
from __future__ import annotations

import re

from src.profiles.base import BankProfile
from src.profiles.banks._sa_common import (
    sa_base_profile,
    sa_column_keywords,
    sa_date_formats,
    sa_header_patterns,
)


def _tymebank_text_line_pattern() -> str:
    """TymeBank-specific text extraction pattern.

    TymeBank statements have 6 columns:
        Date | Description | Fees | Money Out | Money In | Balance

    Empty columns are rendered as ``-``.  Amounts use space as thousands
    separator (e.g. ``22 138.95``), so we use a structured digit pattern
    ``\\d{1,3}(?: \\d{3})*\\.\\d{2}`` to avoid ambiguity with field-separating
    spaces.
    """
    amt = r"\d{1,3}(?: \d{3})*\.\d{2}"  # e.g. "750.00" or "22 138.95"
    fld = rf"(?:{amt}|-)"               # amount or dash
    return (
        rf"(\d{{2}}\s+\w{{3}}\s+\d{{4}})\s+"   # Group 1: Date (DD Mon YYYY)
        rf"(.+?)\s+"                             # Group 2: Description (lazy)
        rf"({fld})\s+"                           # Group 3: Fees
        rf"({fld})\s+"                           # Group 4: Money Out
        rf"({fld})\s+"                           # Group 5: Money In
        rf"({amt})"                              # Group 6: Balance (always present)
    )


def tymebank_profile() -> BankProfile:
    """TymeBank profile."""
    patterns = sa_header_patterns()

    # TymeBank: "Period 01 Jun 2024 - 30 Jun 2024"
    patterns["period_start"] = re.compile(
        r"Period\s+(\d{1,2}\s+\w+\s+\d{4})",
        re.IGNORECASE,
    )
    patterns["period_end"] = re.compile(
        r"Period\s+\d{1,2}\s+\w+\s+\d{4}\s*[-\u2013]\s*(\d{1,2}\s+\w+\s+\d{4})",
        re.IGNORECASE,
    )

    # TymeBank: "Account Num. 53001401661"
    patterns["account_number"] = re.compile(
        r"Account\s*Num\.?\s*(\d{8,12})",
        re.IGNORECASE,
    )

    # Column keywords: add "fees" mapped to bank_charges
    col_kw = sa_column_keywords()
    col_kw["bank_charges"] = ["fees"]

    return sa_base_profile(
        name="TymeBank",
        detection_keywords=["tymebank", "tyme bank"],
        header_patterns=patterns,
        column_keywords=col_kw,
        default_column_map={
            "date": 0,
            "description": 1,
            "bank_charges": 2,
            "debit": 3,
            "credit": 4,
            "balance": 5,
        },
        text_line_pattern=_tymebank_text_line_pattern(),
        date_formats=["%d %b %Y"] + sa_date_formats(),
    )
