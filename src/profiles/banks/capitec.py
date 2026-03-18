"""Capitec Bank profile."""
from __future__ import annotations

import re

from src.profiles.base import BankProfile
from src.profiles.banks._sa_common import sa_base_profile, sa_header_patterns


def capitec_profile() -> BankProfile:
    """Capitec Bank profile."""
    patterns = sa_header_patterns()
    # Capitec uses "Branch" (without "Code") and "Account Number"
    patterns["branch_code"] = re.compile(
        r"(?:Branch)\s*[:\-]?\s*(\d{4,6})", re.IGNORECASE
    )
    patterns["account_number"] = re.compile(
        r"(?:Account\s*(?:Number|No\.?))\s*[:\-]?\s*(\d{10,12})",
        re.IGNORECASE,
    )

    return sa_base_profile(
        name="Capitec",
        detection_keywords=["capitec", "capitec bank", "global one"],
        header_patterns=patterns,
        # Capitec often uses a single Amount column instead of separate debit/credit
        default_column_map={"date": 0, "description": 1, "amount": 2, "balance": 3},
    )
