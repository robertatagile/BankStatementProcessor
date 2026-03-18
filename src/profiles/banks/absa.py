"""ABSA Bank (formerly Barclays Africa) profile."""
from __future__ import annotations

import re

from src.profiles.base import BankProfile
from src.profiles.banks._sa_common import (
    sa_base_profile, sa_header_patterns, sa_column_keywords, sa_date_formats,
)


def absa_profile() -> BankProfile:
    """ABSA Bank (formerly Barclays Africa) profile."""
    patterns = sa_header_patterns()
    patterns["account_type"] = re.compile(
        r"(Cheque\s+Account|Savings\s+Account|Credit\s+Card)", re.IGNORECASE
    )
    # Period: "01 January 2024 to 31 January 2024" OR "2024-04-01 - 2024-06-13"
    # Handles "Statement for Period" (with optional "for")
    patterns["period_start"] = re.compile(
        r"(?:Statement\s+(?:for\s+)?(?:Period|Date|From)|Period)\s*[:\-]?\s*"
        r"(\d{4}-\d{2}-\d{2}|\d{1,2}\s+\w+\s+\d{4})",
        re.IGNORECASE,
    )
    # "to 31 January 2024" or "- 2024-06-13"
    patterns["period_end"] = re.compile(
        r"(?:to|ending|through|-)\s*(\d{4}-\d{2}-\d{2}|\d{1,2}\s+\w+\s+\d{4})",
        re.IGNORECASE,
    )
    # Opening balance: "Balance Brought Forward -10,391.82" or "Opening Balance: R 10 000.00"
    # Use [:]? (not [:\-]?) so colon is consumed but minus sign is captured with the amount
    patterns["opening_balance"] = re.compile(
        r"(?:Opening|Start|Beginning|Brought\s+Forward)\s*(?:Balance)?\s*"
        r"[:]?\s*(-?R?\s?[\d\s,]+\.\d{2})",
        re.IGNORECASE,
    )

    keywords = sa_column_keywords()
    keywords["description"].append("cheque")

    return sa_base_profile(
        name="ABSA",
        detection_keywords=["absa", "absa bank", "cheque account"],
        header_patterns=patterns,
        column_keywords=keywords,
        date_formats=["%Y-%m-%d"] + sa_date_formats(),
    )
