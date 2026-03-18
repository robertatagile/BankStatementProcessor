"""Nedbank profile.

Supports both scanned PDFs (OCR fallback) and digital PDFs with text extraction.
Digital PDF (Nedbank2) text format:
  [Tranlistno] DD/MM/YYYY Description Amount[*] Balance
Amounts use commas as thousands separators (no spaces in numbers).
"""
from __future__ import annotations

import re

from src.profiles.base import BankProfile
from src.profiles.banks._sa_common import sa_base_profile, sa_header_patterns, sa_column_keywords


def nedbank_profile() -> BankProfile:
    """Nedbank profile."""
    patterns = sa_header_patterns()
    # Nedbank uses "Account No" (scanned) or "Current account" (digital PDF)
    patterns["account_number"] = re.compile(
        r"(?:Account\s*No\.?|Current\s+account)\s*[:\-]?\s*(\d[\d\s\-]{6,})",
        re.IGNORECASE,
    )
    # Nedbank format: "Openingbalance -R6,805.16" or Afrikaans "Beginsaldo 199.26"
    patterns["opening_balance"] = re.compile(
        r"(?:Opening\s*balance|Beginsaldo)\s*(-?R?[\d,]+\.\d{2})", re.IGNORECASE,
    )
    patterns["closing_balance"] = re.compile(
        r"(?:Closing\s*balance|Eindsaldo|Sluitingsaldo)\s*(-?R?[\d,]+\.\d{2})", re.IGNORECASE,
    )
    # Period: Afrikaans "tydperk 12 Oktober 2023 tot 11 November 2023"
    # or English "Statementperiod: 03/08/2023 ... 02/09/2023"
    patterns["period_start"] = re.compile(
        r"(?:tydperk|Statement\s*period|Staat\s*periode)\s*:?\s*"
        r"(\d{1,2}\s+\w+\s+\d{4}|\d{2}/\d{2}/\d{4})",
        re.IGNORECASE,
    )
    patterns["period_end"] = re.compile(
        r"(?:tot|to)\s+(\d{1,2}\s+\w+\s+\d{4}|\d{2}/\d{2}/\d{4})"
        r"|(?:Statement\s*period|Staat\s*periode)\s*:?\s*\d{2}/\d{2}/\d{4}\s*\S+\s*(\d{2}/\d{2}/\d{4})",
        re.IGNORECASE,
    )

    keywords = sa_column_keywords()
    keywords["description"].extend(["transaction", "beskrywing"])
    keywords["date"].append("datum")
    keywords["debit"].append("debiete")
    keywords["credit"].append("krediete")
    keywords["balance"].append("saldo")
    # Digital Nedbank PDFs have a "Fees(R)" or "Geld(R)" column
    keywords["fee"] = ["fee", "fees", "geld"]

    # Nedbank text pattern: amounts use commas (not spaces) as thousands separators.
    # Anchored to end-of-line to prevent description numbers from being captured as amounts.
    # Optional 6-digit transaction list number prefix.
    text_pattern = (
        r"(?:\d{6}\s+)?"
        r"(\d{2}/\d{2}/\d{4})\s+"
        r"(.+?)\s+"
        r"(-?[\d,]+\.\d{2})\*?"
        r"(?:\s+(-?[\d,]+\.\d{2}))?$"
    )

    return sa_base_profile(
        name="Nedbank",
        detection_keywords=["nedbank", "nedbank ltd", "greenbacks", "nedbank.co.za"],
        header_patterns=patterns,
        column_keywords=keywords,
        text_line_pattern=text_pattern,
        # Nedbank2 digital: Tranlistno(0) | Date(1) | Desc(2) | Fees(3) | Debits(4) | Credits(5) | Balance(6)
        default_column_map={
            "date": 1, "description": 2, "fee": 3,
            "debit": 4, "credit": 5, "balance": 6,
        },
    )
