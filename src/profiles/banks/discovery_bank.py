"""Discovery Bank profile.

Discovery Bank statements have a clean structure:
- 5-column table: Date | Description | Debit | Credit | Balance
- Date format: YYYY-MM-DD
- Amounts prefixed with ``R`` and space (e.g. ``R 150.00``, ``R 12,841.42``)
- Negative balances use trailing ``-`` (e.g. ``R 125.54-``)
- Header on first line: ``Account holder: NAME From: YYYY-MM-DD To: YYYY-MM-DD``
- Account type and number on second line
- No separate address block for the customer
"""
from __future__ import annotations

import re

from src.profiles.base import BankProfile
from src.profiles.banks._sa_common import sa_base_profile, sa_header_patterns, sa_column_keywords, sa_date_formats


def discovery_bank_profile() -> BankProfile:
    """Discovery Bank profile."""
    patterns = sa_header_patterns()

    # Bank name
    patterns["bank_name"] = re.compile(
        r"(Discovery\s+Bank)", re.IGNORECASE
    )

    # Account holder: "Account holder: C Meyer"
    patterns["account_holder"] = re.compile(
        r"Account\s+holder:\s*(.+?)(?:\s+From:|\n|$)",
        re.IGNORECASE,
    )

    # Account number: "Account number: 17111028413"
    patterns["account_number"] = re.compile(
        r"Account\s+number:\s*(\d{8,15})",
        re.IGNORECASE,
    )

    # Account type: "Account type: Credit Card Account"
    patterns["account_type"] = re.compile(
        r"Account\s+type:\s*(.+?)(?:\s+Account\s+number|\n|$)",
        re.IGNORECASE,
    )

    # Period: "From: 2025-10-29 To: 2026-01-29"
    patterns["period_start"] = re.compile(
        r"From:\s*(\d{4}-\d{2}-\d{2})",
        re.IGNORECASE,
    )
    patterns["period_end"] = re.compile(
        r"To:\s*(\d{4}-\d{2}-\d{2})",
        re.IGNORECASE,
    )

    # Discovery doesn't show explicit opening/closing balance in header,
    # but we can derive from first/last transaction balance
    patterns["opening_balance"] = re.compile(
        r"Opening\s+Balance[:\s]*R?\s*([\d\s,]+\.\d{2})",
        re.IGNORECASE,
    )
    patterns["closing_balance"] = re.compile(
        r"Closing\s+Balance[:\s]*R?\s*([\d\s,]+\.\d{2})",
        re.IGNORECASE,
    )

    keywords = sa_column_keywords()

    return sa_base_profile(
        name="Discovery Bank",
        detection_keywords=[
            "discovery bank", "discovery place",
            "vitality money", "0800 07 96 97",
        ],
        header_patterns=patterns,
        column_keywords=keywords,
        date_formats=["%Y-%m-%d"] + sa_date_formats(),
        # Standard debit/credit columns
        default_column_map={"date": 0, "description": 1, "debit": 2, "credit": 3, "balance": 4},
    )
