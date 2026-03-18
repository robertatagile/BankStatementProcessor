"""Investec Private Bank profile.

Investec statements are text-only (no pdfplumber tables). Transaction lines
have 6 aligned columns: Action Date | Trans Date | Description | Debit | Credit | Balance.

Key characteristics:
- Two date columns (action date and transaction date) — we use the action date
- Comma as thousands separator (e.g. ``42,773.55``), NOT space like other SA banks
- Trailing ``-`` for negative amounts/balances (e.g. ``24,321.25-``)
- Debit and credit appear in the same positional column (only one per line)
- Uses named capture groups in text_line_pattern for the extended column layout
"""
from __future__ import annotations

import re

from src.profiles.base import BankProfile
from src.profiles.banks._sa_common import sa_base_profile, sa_header_patterns, sa_date_formats


def _investec_text_line_pattern() -> str:
    """Investec text extraction pattern with named groups.

    Matches lines like::

        16 Nov 2025 15 Nov 2025 CX Castle Gate PRETORIA ZA 334.25 24,321.25-
        28 Nov 2025 28 Nov 2025 MVNE Pty Ltd 100,867.88 74,352.86

    The ``amount`` field may have a trailing ``-`` indicating a debit;
    no trailing ``-`` indicates a credit.
    """
    return (
        r"(?P<date>\d{1,2}\s+\w{3}\s+\d{4})\s+"    # Action date: d Mon YYYY
        r"\d{1,2}\s+\w{3}\s+\d{4}\s+"                # Trans date (captured but not named — skipped)
        r"(?P<description>.+?)\s+"                     # Description (non-greedy)
        r"(?P<amount>[\d,]+\.\d{2}-?)\s+"             # Amount (trailing - = debit)
        r"(?P<balance>[\d,]+\.\d{2}-?)\s*$"           # Balance (trailing - = negative)
    )


def investec_profile() -> BankProfile:
    """Investec Private Bank profile."""
    patterns = sa_header_patterns()

    # Bank name
    patterns["bank_name"] = re.compile(
        r"(Investec(?:\s+Private\s+Bank)?)", re.IGNORECASE
    )

    # Account holder: "Hugo Frederik Mokken" — standalone name line on page 1
    patterns["account_holder"] = re.compile(
        r"ZAR\s+Pocket\s+Statement\s*\n\s*(.+?)(?:\n|$)",
        re.IGNORECASE,
    )

    # Account number: "Account Number 10012327755"
    patterns["account_number"] = re.compile(
        r"Account\s+Number\s+(\d{10,12})", re.IGNORECASE
    )

    # Opening balance: "Opening Balance 23,987.00" (in ZAR Pocket summary)
    patterns["opening_balance"] = re.compile(
        r"Opening\s+Balance\s+([\d,]+\.\d{2}-?)", re.IGNORECASE
    )

    # Closing balance: "Closing Balance 42,773.55-"
    patterns["closing_balance"] = re.compile(
        r"Closing\s+Balance\s+([\d,]+\.\d{2}-?)", re.IGNORECASE
    )

    # Statement date: "Statement Date 15 December 2025"
    patterns["period_end"] = re.compile(
        r"Statement\s+Date\s+(\d{1,2}\s+\w+\s+\d{4})", re.IGNORECASE
    )

    return sa_base_profile(
        name="Investec",
        detection_keywords=["investec", "investec private bank", "investec bank", "investec.com"],
        header_patterns=patterns,
        text_line_pattern=_investec_text_line_pattern(),
        date_formats=["%d %b %Y", "%d %B %Y"] + sa_date_formats(),
        # Investec uses comma thousands separator (not space like other SA banks)
        thousands_separator=",",
        # Investec text output merges debit/credit into one column;
        # default unsigned amounts to debit (most common on credit card)
        unsigned_is_debit=True,
    )
