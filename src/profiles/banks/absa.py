"""ABSA Bank (formerly Barclays Africa) profile."""
from __future__ import annotations

import re

from src.profiles.base import BankProfile
from src.profiles.banks._sa_common import sa_base_profile, sa_header_patterns, sa_column_keywords


def absa_profile() -> BankProfile:
    """ABSA Bank (formerly Barclays Africa) profile."""
    patterns = sa_header_patterns()
    # ABSA uses "Cheque Account" and period format "01 January 2024 to 31 January 2024"
    patterns["account_type"] = re.compile(
        r"(Cheque\s+Account|Savings\s+Account|Credit\s+Card)", re.IGNORECASE
    )
    patterns["period_start"] = re.compile(
        r"(?:Statement\s+(?:Period|Date|From)|Period)\s*[:\-]?\s*"
        r"(\d{1,2}\s+\w+\s+\d{4})",
        re.IGNORECASE,
    )
    patterns["period_end"] = re.compile(
        r"(?:to|ending|through|-)\s*(\d{1,2}\s+\w+\s+\d{4})",
        re.IGNORECASE,
    )

    keywords = sa_column_keywords()
    keywords["description"].append("cheque")

    return sa_base_profile(
        name="ABSA",
        detection_keywords=["absa", "absa bank", "cheque account"],
        header_patterns=patterns,
        column_keywords=keywords,
    )
