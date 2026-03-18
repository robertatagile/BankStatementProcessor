"""ABSA Bank Afrikaans statement profile.

ABSA issues statements in Afrikaans with different terminology:
- "Tjekrekeningstaat" (cheque account statement)
- "Saldo oorgedra" (balance brought forward)
- "Debietbedrag" / "Kredietbedrag" (debit/credit amount)
- "Transaksiebeskrywing" (transaction description)
- Period uses "tot" instead of "to"
- All text-based extraction (no pdfplumber tables)
"""
from __future__ import annotations

import re

from src.profiles.base import BankProfile
from src.profiles.banks._sa_common import sa_base_profile, sa_header_patterns, sa_date_formats


def _absa_afrikaans_text_line_pattern() -> str:
    """ABSA Afrikaans text extraction pattern.

    ABSA Afrikaans statements are entirely text-based (no pdfplumber tables).
    Transaction lines follow: ``DD/MM/YYYY Description [Cost] Amount Balance``

    The balance uses space as thousands separator (e.g. ``16 270.50``).
    Amounts without a preceding balance are the transaction amount.
    Credits contain "Kt" in the description; everything else is a debit.
    """
    return (
        r"(\d{2}/\d{2}/\d{4})\s+"                     # Date: DD/MM/YYYY
        r"(.+?)\s+"                                     # Description (non-greedy)
        r"(\d{1,3}(?:\s\d{3})*\.\d{2}-?)\s+"          # Amount (valid thousands grouping)
        r"(\d{1,3}(?:\s\d{3})*\.\d{2})\s*$"           # Balance (valid thousands grouping, EOL)
    )


def absa_afrikaans_profile() -> BankProfile:
    """ABSA Bank Afrikaans statement profile."""
    patterns = sa_header_patterns()

    # Bank name: matches "Absa Bank" from the text
    patterns["bank_name"] = re.compile(
        r"(Absa\s+Bank)", re.IGNORECASE
    )

    # Account number: "Tjekrekeningnommer: 7-1323-1819"
    patterns["account_number"] = re.compile(
        r"(?:Tjekrekeningnommer|Rekeningnommer)\s*:\s*([\d\-]+)",
        re.IGNORECASE,
    )

    # Period: "17 Okt 2025 tot 16 Nov 2025"
    patterns["period_start"] = re.compile(
        r"(\d{1,2}\s+\w{3,10}\s+\d{4})\s+tot\s+",
        re.IGNORECASE,
    )
    patterns["period_end"] = re.compile(
        r"tot\s+(\d{1,2}\s+\w{3,10}\s+\d{4})",
        re.IGNORECASE,
    )

    # Opening balance: "Saldo oorgedra 16 270,50" (comma decimal in header summary)
    # But in transaction lines it's "Saldo Oorgedra 16 270.50" (dot decimal)
    patterns["opening_balance"] = re.compile(
        r"Saldo\s+[Oo]orgedra\s+([\d\s]+[.,]\d{2})",
        re.IGNORECASE,
    )

    # Closing balance: "Saldo 10 095,87" (from header summary)
    patterns["closing_balance"] = re.compile(
        r"(?:^|\n)\s*Saldo\s+([\d\s]+[.,]\d{2})\s*$",
        re.IGNORECASE | re.MULTILINE,
    )

    # Account type: "Flexi Rekening", "Tjekrekeningstaat"
    patterns["account_type"] = re.compile(
        r"Rekeningtipe:\s*(.+?)(?:\s+Uitgereik|\n|$)",
        re.IGNORECASE,
    )

    # Account holder: "MEV L SENEKAL" — line with title prefix
    patterns["account_holder"] = re.compile(
        r"(?:^|\n)((?:MEV|MNR|ME|DR|PROF)\s+[A-Z][A-Z\s]+?)(?:\n|$)",
        re.MULTILINE,
    )

    # Afrikaans column keywords
    keywords = {
        "date": ["datum", "date", "transaction date"],
        "description": [
            "transaksiebeskrywing", "beskrywing", "description", "details",
        ],
        "debit": ["debietbedrag", "debiet", "debit"],
        "credit": ["kredietbedrag", "krediet", "credit"],
        "balance": ["saldo", "balance"],
        "amount": ["bedrag", "amount"],
        "cost": ["koste", "cost", "fees"],
    }

    # Afrikaans date formats: "17 Okt 2025", "16Nov2025"
    afr_dates = [
        "%d/%m/%Y",
        "%d %b %Y",     # 17 Okt 2025
        "%d%b%Y",        # 16Nov2025
        "%d %B %Y",     # 17 Oktober 2025
    ] + sa_date_formats()

    return sa_base_profile(
        name="ABSA Afrikaans",
        detection_keywords=[
            "tjekrekeningnommer", "tjekrekeningstaat",
            "transaksiebeskrywing", "saldo oorgedra",
            "debietbedrag", "kredietbedrag",
        ],
        header_patterns=patterns,
        column_keywords=keywords,
        date_formats=afr_dates,
        text_line_pattern=_absa_afrikaans_text_line_pattern(),
        # ABSA uses separate debit/credit columns, text is positional
        default_column_map={"date": 0, "description": 1, "debit": 3, "credit": 4, "balance": 5},
    )
