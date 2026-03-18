"""African Bank profile."""
from __future__ import annotations

import re

from src.profiles.base import BankProfile
from src.profiles.banks._sa_common import sa_base_profile, sa_header_patterns, sa_column_keywords, sa_date_formats


def african_bank_profile() -> BankProfile:
    """African Bank profile."""
    patterns = sa_header_patterns()

    # African Bank shows "African Bank" at top of every page
    patterns["bank_name"] = re.compile(
        r"(African\s+Bank)", re.IGNORECASE
    )

    # Period is on a separate line: "2025/10/21 to 2026/01/04"
    patterns["period_start"] = re.compile(
        r"(\d{4}/\d{2}/\d{2})\s+to\s+\d{4}/\d{2}/\d{2}",
        re.IGNORECASE,
    )
    patterns["period_end"] = re.compile(
        r"\d{4}/\d{2}/\d{2}\s+to\s+(\d{4}/\d{2}/\d{2})",
        re.IGNORECASE,
    )

    # Account holder from product info table: "Account Holder  LUCHAN"
    patterns["account_holder"] = re.compile(
        r"Account\s+Holder\s+([A-Z][A-Z\s]+?)(?:\n|$)",
        re.IGNORECASE,
    )

    # Account type from product info table
    patterns["account_type"] = re.compile(
        r"Account\s+(?:Type|Name)\s+(.+?)(?:\n|$)",
        re.IGNORECASE,
    )

    # Account number from product info table: "Account Number  20114025968"
    patterns["account_number"] = re.compile(
        r"Account\s+Number\s+(\d{8,15})",
        re.IGNORECASE,
    )

    # Opening/closing balance (no R prefix, no Cr/Dr suffix)
    patterns["opening_balance"] = re.compile(
        r"Opening\s+Balance\s+([\d\s,]+\.\d{2})",
        re.IGNORECASE,
    )
    patterns["closing_balance"] = re.compile(
        r"Closing\s+Balance\s+([\d\s,]+\.\d{2})",
        re.IGNORECASE,
    )

    # Branch code: "Branch Code\n430000"
    patterns["branch_code"] = re.compile(
        r"Branch\s+Code\s*\n?\s*(\d{4,6})",
        re.IGNORECASE,
    )

    # African Bank column keywords — has a BANK CHARGES column between description and amount
    keywords = sa_column_keywords()
    keywords["bank_charges"] = ["bank charges", "charges"]

    return sa_base_profile(
        name="African Bank",
        detection_keywords=["african bank", "myworld", "my world"],
        header_patterns=patterns,
        column_keywords=keywords,
        date_formats=["%Y/%m/%d"] + sa_date_formats(),
        # African Bank uses negative amounts for debits, positive for credits
        unsigned_is_debit=False,
        # Col 0=date, 1=description, 2=bank_charges(skip), 3=amount, 4=balance
        default_column_map={"date": 0, "description": 1, "amount": 3, "balance": 4},
    )
