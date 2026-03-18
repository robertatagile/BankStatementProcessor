"""Nedbank profile."""
from __future__ import annotations

import re

from src.profiles.base import BankProfile
from src.profiles.banks._sa_common import sa_base_profile, sa_header_patterns, sa_column_keywords


def nedbank_profile() -> BankProfile:
    """Nedbank profile."""
    patterns = sa_header_patterns()
    # Nedbank uses "Account No" label
    patterns["account_number"] = re.compile(
        r"(?:Account\s*No\.?)\s*[:\-]?\s*(\d[\d\s\-]{6,})",
        re.IGNORECASE,
    )

    keywords = sa_column_keywords()
    # Nedbank may have a "Greenbacks" column to ignore
    keywords["description"].append("transaction")

    return sa_base_profile(
        name="Nedbank",
        detection_keywords=["nedbank", "nedbank ltd", "greenbacks"],
        header_patterns=patterns,
        column_keywords=keywords,
    )
