"""Capitec Bank profile.

Capitec statements have a unique structure:
- 9-column layout: Date | Description | Category | Money In | (empty) | Money Out | (empty) | Fee* | Balance
- pdfplumber fragments each transaction row into its own 1-row table
- "From Date:" / "To Date:" for statement period
- Amounts are signed: negative = debit, positive = credit
- Space as thousands separator (e.g. ``1 000.00``)
- Personal info block on page 1 with name and address
"""
from __future__ import annotations

import re

from src.profiles.base import BankProfile
from src.profiles.banks._sa_common import sa_base_profile, sa_header_patterns, sa_column_keywords


def capitec_profile() -> BankProfile:
    """Capitec Bank profile."""
    patterns = sa_header_patterns()

    # Account number: "Account 2423516890"
    patterns["account_number"] = re.compile(
        r"Account\s+(\d{10,12})", re.IGNORECASE
    )

    # Period: "From Date: 01/11/2025" and "To Date: 24/02/2026"
    patterns["period_start"] = re.compile(
        r"From\s+Date:\s*(\d{2}/\d{2}/\d{4})", re.IGNORECASE
    )
    patterns["period_end"] = re.compile(
        r"To\s+Date:\s*(\d{2}/\d{2}/\d{4})", re.IGNORECASE
    )

    # Opening/closing: "Opening Balance: R24.36" / "Closing Balance: R57.35"
    patterns["opening_balance"] = re.compile(
        r"Opening\s+Balance:\s*R?([\d\s,]+\.\d{2})", re.IGNORECASE
    )
    patterns["closing_balance"] = re.compile(
        r"Closing\s+Balance:\s*R?([\d\s,]+\.\d{2})", re.IGNORECASE
    )

    # Account holder: "MR CORNELIUS PETRUS KLOPPERS" — line with MR/MRS/MS/DR prefix
    # followed by Capitec Bank Limited on the next column
    patterns["account_holder"] = re.compile(
        r"(?:^|\n)((?:MR|MRS|MS|MISS|DR|PROF|ME|MEV|MNR)\s+[A-Z][A-Z\s]+?)(?:\s+Capitec|\n|$)",
        re.MULTILINE,
    )

    # Account type: "Main Account Statement"
    patterns["account_type"] = re.compile(
        r"(Main\s+Account|Global\s+One|Savings\s+Account|Credit\s+Facility)",
        re.IGNORECASE,
    )

    # Capitec column keywords — includes "category", "money in", "money out", "fee"
    keywords = sa_column_keywords()
    keywords["credit"] = ["money in", "credit", "deposit"]
    keywords["debit"] = ["money out", "debit", "withdrawal"]
    keywords["fee"] = ["fee", "fee*", "fees"]
    keywords["category"] = ["category"]

    return sa_base_profile(
        name="Capitec",
        detection_keywords=[
            "capitec", "capitec bank", "global one",
            "capitecbank.co.za", "main account statement",
        ],
        header_patterns=patterns,
        column_keywords=keywords,
        # Capitec uses negative amounts for debits (signed)
        unsigned_is_debit=False,
        # 9-col: Date(0) | Desc(1) | Category(2) | MoneyIn(3) | empty(4) | MoneyOut(5) | empty(6) | Fee(7) | Balance(8)
        default_column_map={
            "date": 0, "description": 1, "category": 2,
            "credit": 3, "debit": 5, "fee": 7, "balance": 8,
        },
    )
