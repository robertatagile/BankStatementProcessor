from __future__ import annotations

import re
from typing import Any, Dict, List

from src.profiles.base import BankProfile


def _sa_header_patterns() -> Dict[str, re.Pattern]:
    """Shared South African bank header patterns."""
    return {
        "bank_name": re.compile(
            r"^(.+?(?:Bank|Capitec|ABSA|FNB|Nedbank))", re.IGNORECASE
        ),
        "account_number": re.compile(
            r"(?:Account\s*(?:Number|No\.?|#)?)\s*[:\-]?\s*(\d[\d\s\-]{6,})",
            re.IGNORECASE,
        ),
        "branch_code": re.compile(
            r"(?:Branch\s*(?:Code)?)\s*[:\-]?\s*(\d{4,6})", re.IGNORECASE
        ),
        "period_start": re.compile(
            r"(?:Statement\s+(?:Period|Date|From))\s*[:\-]?\s*"
            r"(\d{1,2}[\s\/\-](?:\w+|\d{1,2})[\s\/\-]\d{2,4})",
            re.IGNORECASE,
        ),
        "period_end": re.compile(
            r"(?:to|ending|through)\s*[:\-]?\s*"
            r"(\d{1,2}[\s\/\-](?:\w+|\d{1,2})[\s\/\-]\d{2,4})",
            re.IGNORECASE,
        ),
        "opening_balance": re.compile(
            r"(?:Opening|Start|Beginning|Brought\s+Forward)\s*(?:Balance)?\s*"
            r"[:\-]?\s*R?\s*([\d\s,]+\.\d{2})",
            re.IGNORECASE,
        ),
        "closing_balance": re.compile(
            r"(?:Closing|End|Ending|Carried\s+Forward)\s*(?:Balance)?\s*"
            r"[:\-]?\s*R?\s*([\d\s,]+\.\d{2})",
            re.IGNORECASE,
        ),
    }


def _sa_date_formats() -> List[str]:
    """Date formats common to South African bank statements."""
    return [
        "%d/%m/%Y",
        "%d %B %Y",
        "%d %b %Y",
        "%d-%m-%Y",
        "%Y-%m-%d",
        "%d/%m/%y",
        "%d-%m-%y",
    ]


def _sa_column_keywords() -> Dict[str, List[str]]:
    """Column keywords common to South African bank statements."""
    return {
        "date": ["date", "transaction date", "trans date"],
        "description": [
            "description",
            "details",
            "particulars",
            "narrative",
            "transaction description",
        ],
        "debit": ["debit", "withdrawal", "dr", "debit (r)", "money out"],
        "credit": ["credit", "deposit", "cr", "credit (r)", "money in"],
        "balance": ["balance", "running balance", "available balance"],
        "amount": ["amount", "transaction amount"],
    }


def _sa_text_line_pattern() -> str:
    """Text-based extraction pattern for SA bank statements."""
    return (
        r"(\d{1,2}[\/\-\s](?:\w+|\d{1,2})[\/\-\s]\d{2,4})\s+"
        r"(.+?)\s+"
        r"(-?R?\s?[\d\s,]+\.\d{2})"
        r"(?:\s+(-?R?\s?[\d\s,]+\.\d{2}))?"
    )


def _sa_base_profile(**overrides: Any) -> BankProfile:
    """Create a BankProfile with shared South African defaults."""
    defaults = dict(
        currency_symbol="R",
        thousands_separator=" ",
        header_patterns=_sa_header_patterns(),
        date_formats=_sa_date_formats(),
        column_keywords=_sa_column_keywords(),
        default_column_map={"date": 0, "description": 1, "debit": 2, "credit": 3, "balance": 4},
        text_line_pattern=_sa_text_line_pattern(),
    )
    defaults.update(overrides)
    return BankProfile(**defaults)


# ---------------------------------------------------------------------------
# Individual bank profiles
# ---------------------------------------------------------------------------


def absa_profile() -> BankProfile:
    """ABSA Bank (formerly Barclays Africa) profile."""
    patterns = _sa_header_patterns()
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

    keywords = _sa_column_keywords()
    keywords["description"].append("cheque")

    return _sa_base_profile(
        name="ABSA",
        detection_keywords=["absa", "absa bank", "cheque account"],
        header_patterns=patterns,
        column_keywords=keywords,
    )


def fnb_profile() -> BankProfile:
    """First National Bank (FirstRand) profile."""
    patterns = _sa_header_patterns()
    # FNB uses clean "Account Number" and "Branch Code" labels
    patterns["account_number"] = re.compile(
        r"(?:Account\s*(?:Number|No\.?))\s*[:\-]?\s*(\d{10,12})",
        re.IGNORECASE,
    )

    return _sa_base_profile(
        name="FNB",
        detection_keywords=["fnb", "first national bank", "firstrand"],
        header_patterns=patterns,
    )


def nedbank_profile() -> BankProfile:
    """Nedbank profile."""
    patterns = _sa_header_patterns()
    # Nedbank uses "Account No" label
    patterns["account_number"] = re.compile(
        r"(?:Account\s*No\.?)\s*[:\-]?\s*(\d[\d\s\-]{6,})",
        re.IGNORECASE,
    )

    keywords = _sa_column_keywords()
    # Nedbank may have a "Greenbacks" column to ignore
    keywords["description"].append("transaction")

    return _sa_base_profile(
        name="Nedbank",
        detection_keywords=["nedbank", "nedbank ltd", "greenbacks"],
        header_patterns=patterns,
        column_keywords=keywords,
    )


def standard_bank_profile() -> BankProfile:
    """Standard Bank of South Africa profile."""
    patterns = _sa_header_patterns()
    # Standard Bank uses "Statement Period" label
    patterns["period_start"] = re.compile(
        r"(?:Statement\s+Period)\s*[:\-]?\s*"
        r"(\d{1,2}[\s\/\-](?:\w+|\d{1,2})[\s\/\-]\d{2,4})",
        re.IGNORECASE,
    )

    return _sa_base_profile(
        name="Standard Bank",
        detection_keywords=["standard bank", "sbsa", "standard bank of south africa"],
        header_patterns=patterns,
    )


def capitec_profile() -> BankProfile:
    """Capitec Bank profile."""
    patterns = _sa_header_patterns()
    # Capitec uses "Branch" (without "Code") and "Account Number"
    patterns["branch_code"] = re.compile(
        r"(?:Branch)\s*[:\-]?\s*(\d{4,6})", re.IGNORECASE
    )
    patterns["account_number"] = re.compile(
        r"(?:Account\s*(?:Number|No\.?))\s*[:\-]?\s*(\d{10,12})",
        re.IGNORECASE,
    )

    return _sa_base_profile(
        name="Capitec",
        detection_keywords=["capitec", "capitec bank", "global one"],
        header_patterns=patterns,
        # Capitec often uses a single Amount column instead of separate debit/credit
        default_column_map={"date": 0, "description": 1, "amount": 2, "balance": 3},
    )


# ---------------------------------------------------------------------------
# Auto-register all SA profiles with the factory
# (Import this module to trigger registration)
# ---------------------------------------------------------------------------

def register_all() -> None:
    """Register all South African bank profiles with the factory."""
    from src.profiles.factory import BankProfileFactory

    BankProfileFactory.register("absa", absa_profile)
    BankProfileFactory.register("fnb", fnb_profile)
    BankProfileFactory.register("nedbank", nedbank_profile)
    BankProfileFactory.register("standard_bank", standard_bank_profile)
    BankProfileFactory.register("capitec", capitec_profile)
