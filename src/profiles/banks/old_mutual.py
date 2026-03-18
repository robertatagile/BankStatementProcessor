"""Old Mutual Money Account profile.

Old Mutual Money Account statements are scanned image PDFs requiring OCR.
Once OCR'd, the text format is:
- 5 columns: Date | Transactional Description | Amount | Charges | Balance
- Date format: ``DD Mon YYYY`` (e.g. ``23 Dec 2024``)
- Amounts: signed with comma thousands (``-150.00``, ``13,191.45``)
- Associated with Bidvest Bank Limited
"""
from __future__ import annotations

import re

from src.profiles.base import BankProfile
from src.profiles.banks._sa_common import sa_base_profile, sa_header_patterns, sa_column_keywords, sa_date_formats


def old_mutual_profile() -> BankProfile:
    """Old Mutual Money Account profile."""
    patterns = sa_header_patterns()

    # Bank name
    patterns["bank_name"] = re.compile(
        r"(Old\s+Mutual|Money\s+Account)", re.IGNORECASE
    )

    # Account holder: "MS NQ THEODORE" or similar title+name
    patterns["account_holder"] = re.compile(
        r"(?:^|\n)((?:MS|MR|MRS|MISS|DR|PROF|MNR|MEV)\s+[A-Z][A-Z\s]+?)(?:\n|$)",
        re.MULTILINE,
    )

    # Account number: "Account Number: 24210936301" or "24210936301"
    patterns["account_number"] = re.compile(
        r"(?:Account\s*(?:Number|No\.?)?)\s*[:\-]?\s*(\d{10,12})",
        re.IGNORECASE,
    )

    # Period: "2024/12/06 - 2025/02/11" or "Date Range"
    patterns["period_start"] = re.compile(
        r"(\d{4}/\d{2}/\d{2})\s*[-–]\s*\d{4}/\d{2}/\d{2}",
        re.IGNORECASE,
    )
    patterns["period_end"] = re.compile(
        r"\d{4}/\d{2}/\d{2}\s*[-–]\s*(\d{4}/\d{2}/\d{2})",
        re.IGNORECASE,
    )

    # Opening/closing balance
    patterns["opening_balance"] = re.compile(
        r"(?:Opening|Available)\s+Balance[:\s]*R?\s*([\d,]+\.\d{2})",
        re.IGNORECASE,
    )
    patterns["closing_balance"] = re.compile(
        r"(?:Current|Closing)\s+Balance[:\s]*R?\s*([\d,]+\.\d{2})",
        re.IGNORECASE,
    )

    keywords = sa_column_keywords()
    keywords["charges"] = ["charges", "charge"]

    return sa_base_profile(
        name="Old Mutual",
        detection_keywords=[
            "old mutual", "money account", "bidvest bank",
            "moneyaccount", "old mutual transaction",
        ],
        header_patterns=patterns,
        column_keywords=keywords,
        date_formats=["%d %b %Y", "%d %B %Y", "%Y/%m/%d"] + sa_date_formats(),
        # Negative amounts = debits
        unsigned_is_debit=False,
        # Col layout: Date(0) | Description(1) | Amount(2) | Charges(3) | Balance(4)
        default_column_map={"date": 0, "description": 1, "amount": 2, "balance": 4},
    )
