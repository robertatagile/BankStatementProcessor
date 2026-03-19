"""ABSA Bank Afrikaans statement profile.

ABSA issues statements in Afrikaans with different terminology:
- "Tjekrekeningstaat" or "Rekeningstaat" (account statement)
- "Saldo oorgedra" / "Saldo oorgebring" (balance brought forward)
- "Debietbedrag" / "Kredietbedrag" (debit/credit amount)
- "Transaksiebeskrywing" / "beskrywing" (transaction description)
- Period uses "tot" instead of "to"
- All text-based extraction (no pdfplumber tables)

Two known sub-formats:
1. Old: DD/MM/YYYY dates, period-decimal amounts (16 270.50)
2. New: DDMonYYYY no-space dates (1Sep2023), comma-decimal amounts (7 720,70-)
"""
from __future__ import annotations

import re

from src.profiles.base import BankProfile
from src.profiles.banks._sa_common import sa_base_profile, sa_header_patterns, sa_date_formats


def _absa_afrikaans_text_line_pattern() -> str:
    """ABSA Afrikaans text extraction pattern.

    Handles both old (DD/MM/YYYY + period decimal) and new (DDMonYYYY + comma
    decimal) formats.  Transaction lines follow:

      Date Description Amount [Balance]

    The amount may have a trailing minus for debits.
    Credits contain "Kt" in the description; everything else is a debit.
    """
    # Date: DD/MM/YYYY  OR  D{1,2}Mon{3}YYYY (no spaces, e.g. 1Sep2023)
    date = r"(\d{1,2}/\d{2}/\d{4}|\d{1,2}[A-Z][a-z]{2}\d{4})"
    # Description starts with a letter (handles no-space after date like "23Sep2023Direkte")
    desc = r"([A-Za-z].+?)"
    # Amount: digits with optional space thousands, comma OR period decimal, optional trailing minus
    amt = r"(\d[\d\s]*[.,]\d{2}-?)"
    # Balance (optional): same format but no trailing minus
    bal = r"(\d[\d\s]*[.,]\d{2})"
    return date + r"\s*" + desc + r"\s+" + amt + r"(?:\s+" + bal + r")?"


def absa_afrikaans_profile() -> BankProfile:
    """ABSA Bank Afrikaans statement profile."""
    patterns = sa_header_patterns()

    # Bank name: matches "Absa Bank" from the text
    patterns["bank_name"] = re.compile(
        r"(Absa\s+Bank(?:\s+Ltd)?)", re.IGNORECASE
    )

    # Account number: "Tjekrekeningnommer: 7-1323-1819" or "Rekeningnommer 92 4428 9156"
    patterns["account_number"] = re.compile(
        r"(?:Tjekrekeningnommer|Rekeningnommer)\s*[:\s]\s*([\d\s\-]+\d)",
        re.IGNORECASE,
    )

    # Period: "17 Okt 2025 tot 16 Nov 2025"
    #     or: "Staat vir Periode 2024-05-08 - 2024-06-27"
    #     or: "1 Sep 2023 tot 30 Sep 2023"
    patterns["period_start"] = re.compile(
        r"(\d{1,2}\s+\w{3,10}\s+\d{4})\s+tot\s+"
        r"|Staat\s+vir\s+Periode\s*[:\-]?\s*(\d{4}-\d{2}-\d{2})",
        re.IGNORECASE,
    )
    patterns["period_end"] = re.compile(
        r"tot\s+(\d{1,2}\s+\w{3,10}\s+\d{4})"
        r"|Staat\s+vir\s+Periode\s*[:\-]?\s*\d{4}-\d{2}-\d{2}\s*-\s*(\d{4}-\d{2}-\d{2})",
        re.IGNORECASE,
    )

    # Opening balance: "Saldo oorgedra 16 270,50" or "Saldo oorgebring 18 490,46"
    patterns["opening_balance"] = re.compile(
        r"Saldo\s+[Oo]or(?:gedra|gebring)\s+([\d\s]+[.,]\d{2})",
        re.IGNORECASE,
    )

    # Closing balance:
    # - "Saldo 10 095,87" (standalone on a line)
    # - "Saldo op30Sep2023 35 538,04" (no-space date variant)
    # - "Huidige balans R 35,014.11"
    patterns["closing_balance"] = re.compile(
        r"(?:(?:^|\n)\s*Saldo\s+([\d\s]+[.,]\d{2})\s*$"
        r"|Saldo\s+op\s*\d{1,2}\s*\w{3,10}\s*\d{4}\s+([\d\s]+[.,]\d{2})"
        r"|Huidige\s+balans\s*R?\s*([\d\s,]+\.\d{2}))",
        re.IGNORECASE | re.MULTILINE,
    )

    # Account type: "Flexi Rekening", "Tjekrekeningstaat", "Flexi Account"
    patterns["account_type"] = re.compile(
        r"(?:Rekeningtipe:\s*(.+?)(?:\s+Uitgereik|\n|$)"
        r"|Rekeningopsomming\s+van\s+u\s+(.+?)(?:\n|$))",
        re.IGNORECASE,
    )

    # Account holder: "MEV L SENEKAL" or "GP PRETORIUS" — line with title or uppercase name
    patterns["account_holder"] = re.compile(
        r"(?:^|\n)((?:MEV|MNR|ME|DR|PROF)\s+[A-Z][A-Z\s]+?)(?:\n|$)"
        r"|(?:^|\n)([A-Z]{2,}\s+[A-Z][A-Z\s]+?)(?:\n(?:POSBUS|Posbus|Tel:))",
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
        "amount": ["bedrag", "amount", "transaksiebedrag"],
        "cost": ["koste", "cost", "fees", "diensfooie"],
    }

    # Afrikaans date formats: "17 Okt 2025", "1Sep2023", "16Nov2025", "2024-05-08"
    afr_dates = [
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d %b %Y",     # 17 Okt 2025
        "%d%b%Y",        # 1Sep2023 (no spaces)
        "%d %B %Y",     # 17 Oktober 2025
    ] + sa_date_formats()

    # Fee lines have no date: "Transaksie Fooi 3,50- 10 766,26"
    # Capture: (description) (amount with optional trailing minus) (optional balance)
    fee_pattern = (
        r"^([A-Za-z].*?(?:[Ff]ooi|[Ff]ee|[Ss]ms).*?)\s+"
        r"(\d[\d\s]*[.,]\d{2}-?)"
        r"(?:\s+(\d[\d\s]*[.,]\d{2}))?"
    )

    return sa_base_profile(
        name="ABSA Afrikaans",
        detection_keywords=[
            # Old format keywords
            "tjekrekeningnommer", "tjekrekeningstaat",
            "transaksiebeskrywing", "saldo oorgedra",
            "debietbedrag", "kredietbedrag",
            "staat vir periode", "saldo oorgebring",
            "transaksie geskiedenis",
            # New format keywords (Rekeningstaat, Transaksiegeskiedenis, etc.)
            "rekeningstaat", "transaksiegeskiedenis",
            "transaksiebedrag", "rekeningopsomming",
            "absa.co.za",
        ],
        header_patterns=patterns,
        column_keywords=keywords,
        date_formats=afr_dates,
        text_line_pattern=_absa_afrikaans_text_line_pattern(),
        fee_line_pattern=fee_pattern,
        # ABSA uses separate debit/credit columns, text is positional
        default_column_map={"date": 0, "description": 1, "debit": 3, "credit": 4, "balance": 5},
    )
