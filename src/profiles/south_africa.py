from __future__ import annotations

import re
from typing import Any, Dict, List

from src.profiles.base import BankProfile


def _sa_header_patterns() -> Dict[str, re.Pattern]:
    """Shared South African bank header patterns.

    Handles both spaced (``Statement Period : 31 March 2025 to 30 April 2025``)
    and no-space (``StatementPeriod:31January2025to28February2025``) layouts
    that FNB PDFs produce depending on the rendering.
    """
    return {
        "bank_name": re.compile(
            r"(?im)^\s*((?:ABSA(?:\s+Bank)?|Capitec(?:\s+Bank)?|"
            r"First\s+National\s+Bank|FNB|Nedbank(?:\s+Ltd)?|"
            r"Standard\s+Bank(?:\s+of\s+South\s+Africa)?))\b",
            re.IGNORECASE,
        ),
        "account_number": re.compile(
            r"(?:Account\s*(?:Number|No\.?|#)?)\s*[:\-]?\s*(\d[\d\s\-]{6,})",
            re.IGNORECASE,
        ),
        "branch_code": re.compile(
            r"(?:Branch\s*(?:Code)?)\s*[:\-]?\s*(\d{4,6})", re.IGNORECASE
        ),
        "period_start": re.compile(
            r"(?:Statement\s*Period|Statement\s*Date|Statement\s*From)\s*[:\-]?\s*"
            r"(\d{1,2}\s*(?:[A-Za-z]+|\d{1,2})\s*[\s\/\-]?\s*\d{2,4})",
            re.IGNORECASE,
        ),
        "period_end": re.compile(
            r"(?:to|ending|through)\s*[:\-]?\s*"
            r"(\d{1,2}\s*(?:[A-Za-z]+|\d{1,2})\s*[\s\/\-]?\s*\d{2,4})",
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
    """Date formats common to South African bank statements.

    Includes no-space variants (e.g. ``31January2025``) produced by some
    FNB PDF renderings where spaces between fields are missing.
    """
    return [
        "%d/%m/%Y",
        "%d %B %Y",
        "%d %b %Y",
        "%d%B%Y",       # no-space: 31January2025
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
        unsigned_is_debit=True,
    )
    defaults.update(overrides)
    return BankProfile(**defaults)


# ---------------------------------------------------------------------------
# Individual bank profiles
# ---------------------------------------------------------------------------


def absa_profile() -> BankProfile:
    """ABSA Bank (formerly Barclays Africa) profile."""
    patterns = _sa_header_patterns()
    patterns["bank_name"] = re.compile(
        r"(?im)^\s*(ABSA(?:\s+Bank(?:\s+Ltd)?)?)\b",
        re.IGNORECASE,
    )
    patterns["account_number"] = re.compile(
        r"(?:Account\s*Number|Tjekrekeningnommer|Savings\s*Account|Cheque\s*Account)\s*[:\-]?\s*([\d\-\s]{8,})",
        re.IGNORECASE,
    )
    # ABSA uses "Cheque Account" and period format "01 January 2024 to 31 January 2024"
    patterns["account_type"] = re.compile(
        r"(Cheque\s+Account|Savings\s+Account|Credit\s+Card|Tjekrekeningstaat|Flexi\s+Rekening|TJEKREK)",
        re.IGNORECASE,
    )
    patterns["period_start"] = re.compile(
        r"(?:Statement\s+(?:Period|Date|From)|Period|Staat\s+vir\s+die\s+Periode|Tjekrekeningstaat)\s*[:\-]?\s*"
        r"(\d{1,4}(?:[\-/]\d{1,2}[\-/]\d{1,4}|\s+\w+\s+\d{4}))",
        re.IGNORECASE,
    )
    patterns["period_end"] = re.compile(
        r"(?:to|tot|ending|through|-)\s*(\d{1,4}(?:[\-/]\d{1,2}[\-/]\d{1,4}|\s+\w+\s+\d{4}))",
        re.IGNORECASE,
    )
    patterns["opening_balance"] = re.compile(
        r"(?:Opening\s+Balance|Openingsaldo|Saldo\s+oorgedra)\s*:?[ \t]*(-?R?[\d\s,.]+)",
        re.IGNORECASE,
    )
    patterns["closing_balance"] = re.compile(
        r"(?:Closing\s+Balance|Afsluitingsaldo|Huidige\s+Saldo|Saldo)\s*:?[ \t]*(-?R?[\d\s,.]+)",
        re.IGNORECASE,
    )

    keywords = _sa_column_keywords()
    keywords["description"].append("cheque")
    keywords["description"].append("transaksiebeskrywing")
    keywords["debit"].append("debietbedrag")
    keywords["credit"].append("kredietbedrag")
    keywords["balance"].append("saldo")
    keywords["amount"].append("bedrag")

    return _sa_base_profile(
        name="ABSA",
        detection_keywords=[
            "absa",
            "absa bank",
            "absa bank ltd",
            "cheque account",
            "tjekrekeningstaat",
            "tjekrekeningnommer",
            "transaksiegeskiedenis",
            "rekeningopsomming",
        ],
        header_patterns=patterns,
        column_keywords=keywords,
        default_column_map={"date": 0, "description": 1, "debit": 3, "credit": 4, "balance": 5},
        unsigned_is_debit=False,
        date_formats=_sa_date_formats() + ["%d%b%Y", "%Y/%m/%d"],
    )


def _fnb_text_line_pattern() -> str:
    """FNB-specific text extraction pattern.

    FNB uses ``DD Mon`` or ``DDMon`` dates (no year), amounts with optional
    ``Cr``/``Dr`` suffix, and balances that always end with ``Cr`` or ``Dr``.
    The space between day and month is optional because some FNB PDF renderings
    concatenate them (e.g. ``01Feb`` instead of ``01 Feb``).
    """
    return (
        r"(\d{2}\s*\w{3})\s+"                     # Date: DD[space?]Mon (e.g. "01Feb" or "01 Feb")
        r"(.+?)"                                   # Description (non-greedy, at least 1 char)
        r"\s+([\d,]+\.\d{2}(?:Cr|Dr|Kt|Dt)?)"     # Amount (optional Cr/Dr/Kt/Dt)
        r"\s+([\d,]+\.\d{2}(?:Cr|Dr|Kt|Dt)?)"     # Balance (suffix optional in some FNB layouts)
    )


def fnb_profile() -> BankProfile:
    """First National Bank (FirstRand) profile."""
    patterns = _sa_header_patterns()
    patterns["bank_name"] = re.compile(
        r"(?im)^\s*((?:First\s+National\s+Bank|FNB))\b",
        re.IGNORECASE,
    )
    # FNB: English and Afrikaans account-number labels / product lines.
    patterns["account_number"] = re.compile(
        r"(?:Account\s*(?:Number|No\.?)|Rekeningnommer|"
        r"Gold\s*Business\s*Account|"
        r"(?:FNB\s+)?(?:Aspire|Fusion\s+Aspire|Private\s+Clients\s+Current|Savings)\s+"
        r"(?:Account|Acc))\s*[:\-]?\s*(\d{10,12})",
        re.IGNORECASE,
    )
    patterns["period_start"] = re.compile(
        r"(?:Statement\s*Period|Staat\s*Periode|Statement\s*Date|Staatdatum|Statement\s*From)\s*[:\-]?\s*"
        r"(\d{1,2}\s*(?:[A-Za-z]+|\d{1,2})\s*[\s\/\-]?\s*\d{2,4})",
        re.IGNORECASE,
    )
    patterns["period_end"] = re.compile(
        r"(?:to|tot|ending|through)\s*[:\-]?\s*"
        r"(\d{1,2}\s*(?:[A-Za-z]+|\d{1,2})\s*[\s\/\-]?\s*\d{2,4})",
        re.IGNORECASE,
    )
    patterns["opening_balance"] = re.compile(
        r"(?:Opening\s*Balance|Openingsaldo)\s*[:\-]?\s*R?\s*([\d ,]+\.\d{2}(?:Cr|Dr|Kt|Dt)?)",
        re.IGNORECASE,
    )
    patterns["closing_balance"] = re.compile(
        r"(?:Closing\s*Balance|Afsluitingsaldo)\s*[:\-]?\s*R?\s*([\d ,]+\.\d{2}(?:Cr|Dr|Kt|Dt)?)",
        re.IGNORECASE,
    )

    # FNB personal info: account holder marked with leading asterisk
    patterns["account_holder"] = re.compile(
        r"\*\s*(.+?(?:PTY|LTD|CC|INC|TRUST).*?)(?:\s*Universal|\n|$)",
        re.IGNORECASE,
    )

    # FNB date formats: DD Mon / DDMon (no year) + Online Banking header
    # dates (MM/DD/YY) + standard SA formats.
    fnb_dates = ["%d%b", "%d %b", "%m/%d/%y", "%m/%d/%Y"] + _sa_date_formats()

    return _sa_base_profile(
        name="FNB",
        detection_keywords=[
            "fnb",
            "first national bank",
            "firstrand",
            "fnby transact account",
            "fnb fusion aspire account",
            "online.fnb.co.za",
        ],
        header_patterns=patterns,
        text_line_pattern=_fnb_text_line_pattern(),
        date_formats=fnb_dates,
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
