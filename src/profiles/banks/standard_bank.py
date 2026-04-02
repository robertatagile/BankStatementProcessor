"""Standard Bank of South Africa — multi-variant profile.

Supports five statement layouts from a single module:

* **OCR**          – scanned/image-based statements (mm-dd dates, R-prefixed amounts)
* **Online**       – PureSave / digital statements (dd MMM dates, +/- amounts)
* **Prestige**     – 3-month / Prestige statements (dd MMM yy dates, signed amounts)
* **Achieva**      – Achieva current account English (description-first, MM DD dates)
* **Achieva (AF)** – Achieva current account Afrikaans (same layout, Afrikaans labels)

Each variant is a lightweight factory that shares a common keyword/column base
via ``_sb_column_keywords`` and ``_sb_base_patterns``, then overrides only
the fields that differ.
"""
from __future__ import annotations

import re

from src.profiles.base import BankProfile
from src.profiles.banks._sa_common import (
    sa_base_profile,
    sa_date_formats,
    sa_header_patterns,
)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

# Detection keywords common to every Standard Bank statement.
_COMMON_KEYWORDS = [
    "standard bank", "standardbank", "standardbank.co.za",
    "sbsa", "standard bank of south africa",
]


def _sb_column_keywords(*, afrikaans: bool = False) -> dict:
    """Column keywords shared by all Standard Bank variants.

    When *afrikaans* is True the Afrikaans labels take priority.
    """
    base = {
        "date": ["date", "transaction date", "trans date", "datum"],
        "description": [
            "description", "details", "particulars", "narrative",
            "transaction description", "beskrywing", "besonderhede",
        ],
        "debit": [
            "debit", "withdrawal", "dr", "debit (r)", "money out",
            "payments", "debiet", "betaling", "debiete",
            "out (r)",
        ],
        "credit": [
            "credit", "deposit", "cr", "credit (r)", "money in",
            "deposits", "krediet", "inbetaling", "krediete",
            "in (r)",
        ],
        "balance": ["balance", "running balance", "available balance", "saldo"],
        "amount": ["amount", "transaction amount", "bedrag", "debits credits",
                    "debiete krediete"],
        "fee": ["fee", "fees", "fooi", "fooie", "bank fees", "bank fees (r)",
                "service fee", "diensgeld"],
    }
    return base


def _sb_base_patterns() -> dict:
    """Header patterns shared by all Standard Bank variants."""
    patterns = sa_header_patterns()

    # Standard Bank "Statement Period" / "Statement period"
    patterns["period_start"] = re.compile(
        r"(?:Statement\s+[Pp]eriod|Statement\s+[Ff]rom|"
        r"Staat\s+van|Transaction\s+date\s+range)\s*[:\-]?\s*"
        r"(\d{1,2}\s+\w+\s+\d{4})",
        re.IGNORECASE,
    )

    # "to <date>" or "- <date>" after period_start
    patterns["period_end"] = re.compile(
        r"(?:to|tot|ending|through|\-)\s*[:\-]?\s*"
        r"(\d{1,2}\s+\w+\s+\d{2,4})",
        re.IGNORECASE,
    )

    # Account holder: "Account holder: NAME" or "MR./MRS./MS. NAME"
    patterns["account_holder"] = re.compile(
        r"(?:Account\s+holder|Rekeninghouer)\s*:\s*(.+?)$",
        re.IGNORECASE | re.MULTILINE,
    )

    # Account type: "ACHIEVA CURRENT ACCOUNT" or "ACHIEVA TJEKREKENING" or "PureSave"
    patterns["account_type"] = re.compile(
        r"(ACHIEVA\s+(?:CURRENT\s+ACCOUNT|TJEKREKENING)|"
        r"PRESTIGEACC|PureSave)",
        re.IGNORECASE,
    )

    return patterns


# ---------------------------------------------------------------------------
# Variant 1 — OCR-scanned statements (legacy)
# ---------------------------------------------------------------------------

def standard_bank_profile() -> BankProfile:
    """Standard Bank OCR-scanned variant.

    Handles OCR-scanned statements with:
    - mm-dd date format (no year — year inferred from statement period)
    - R-prefixed amounts with mixed comma/period decimal separators
    - "Balance brought forward" / "Balance as at" patterns
    - Columns: Date | Description | Fee | Payments | Deposits | Balance
    """
    patterns = _sb_base_patterns()

    # "Balance brought forward R 13 357,89" (comma or period decimal)
    # OCR may produce "Ba'lance", "Ealance", "Ba]ance" etc.
    patterns["opening_balance"] = re.compile(
        r"(?:[BbEe]a[l'\]]*ance\s+brought\s+f\s*orward)\s+"
        r"R\s*(-?\d[\d\s]*[.,]\d{2})",
        re.IGNORECASE,
    )

    # "Balance as at 26 November 2023 R 12 691,51"
    patterns["closing_balance"] = re.compile(
        r"(?:[BbEe]a[l\]']*ance\s+as\s+at\s+\d{1,2}\s+\w+\s+\d{4})\s+"
        r"R\s*(\d[\d\s]*[.,]\d{2})",
        re.IGNORECASE,
    )

    # Allow common OCR digit substitutions (I/i/l/L/t→1, O/o→0) in the
    # date portion so lines like "1i-02" or "tL-24" are still captured.
    _ocr_digit = r"[0-9IilLtO]"
    text_line_pattern = (
        r"^(" + _ocr_digit + r"{1,2}\s*[-\.]\s*" + _ocr_digit + r"{1,2})\s+"
        r"(.+?)\s+"                              # description (non-greedy)
        r"(R\s*-?\s*[\d\s]+[.,]\s*\d{2})"       # amount (R prefix, optional minus)
        r"(?:\s+(R\s*[\d\s]+[.,]\s*\d{2}))?"     # optional balance
    )

    # Add mm-dd date format
    date_fmts = sa_date_formats() + ["%m-%d"]

    return sa_base_profile(
        name="Standard Bank",
        detection_keywords=_COMMON_KEYWORDS + ["universal branch"],
        header_patterns=patterns,
        text_line_pattern=text_line_pattern,
        date_formats=date_fmts,
        column_keywords=_sb_column_keywords(),
        unsigned_is_debit=False,
    )


# ---------------------------------------------------------------------------
# Variant 2 — Online / PureSave digital statements
# ---------------------------------------------------------------------------

def standard_bank_online_profile() -> BankProfile:
    """Standard Bank Online / PureSave digital variant.

    Layout: Date | Description | In (R) | Out (R) | Bank fees (R) | Balance (R)
    Dates: ``24 Dec`` (dd MMM, no year — year on separate marker line).
    Amounts: ``+ 2 250.00`` / ``- 300.00`` with space-thousands.
    """
    patterns = _sb_base_patterns()

    # "Transaction date range: 28 November 2025 - 26 February 2026"
    patterns["period_start"] = re.compile(
        r"Transaction\s+date\s+range\s*:\s*"
        r"(\d{1,2}\s+\w+\s+\d{4})",
        re.IGNORECASE,
    )

    # "Available balance: R65.05"
    patterns["closing_balance"] = re.compile(
        r"Available\s+balance\s*:\s*R\s*(-?[\d\s]+\.\d{2})",
        re.IGNORECASE,
    )
    # No explicit opening balance in this format — will be inferred from first
    # transaction's running balance.
    patterns["opening_balance"] = re.compile(
        r"(?:WILL_NOT_MATCH_PLACEHOLDER)", re.IGNORECASE,
    )

    # Named-group pattern for online format:
    #   24 Dec description + 2 250.00 2 238.17
    #   24 Dec description - 300.00 -11.83
    text_line_pattern = (
        r"^(?P<date>\d{1,2}\s+\w{3})\s+"
        r"(?P<description>.+?)\s+"
        r"(?P<amount>[+-]\s*\d[\d\s]*\.\d{2})\s+"
        r"(?P<balance>-?\d[\d\s]*\.\d{2})\s*$"
    )

    # dd MMM (no year → 1900, fixed by _fix_yearless_dates)
    date_fmts = sa_date_formats() + ["%d %b"]

    return sa_base_profile(
        name="Standard Bank Online",
        detection_keywords=_COMMON_KEYWORDS + [
            "puresave", "transaction date range",
            "in (r)", "out (r)", "bank fees (r)",
        ],
        header_patterns=patterns,
        text_line_pattern=text_line_pattern,
        date_formats=date_fmts,
        column_keywords=_sb_column_keywords(),
        prefer_text_extraction=True,
        unsigned_is_debit=False,
    )


# ---------------------------------------------------------------------------
# Variant 3 — Prestige / 3-month statements
# ---------------------------------------------------------------------------

def standard_bank_prestige_profile() -> BankProfile:
    """Standard Bank Prestige / 3-month statement variant.

    Layout: Date | Description | Payments | Deposits | Balance
    Dates: ``06 Dec 25`` (dd MMM yy).
    Amounts: ``-55.97`` / ``165.00`` (comma-thousands, dot-decimal).
    Multi-line descriptions (second line is continuation).
    """
    patterns = _sb_base_patterns()

    # "From: 04 Dec 25"
    patterns["period_start"] = re.compile(
        r"From\s*:\s*(\d{1,2}\s+\w{3}\s+\d{2,4})",
        re.IGNORECASE,
    )
    # "To: 04 Mar 26"
    patterns["period_end"] = re.compile(
        r"To\s*:\s*(\d{1,2}\s+\w{3}\s+\d{2,4})",
        re.IGNORECASE,
    )

    # "STATEMENT OPENING BALANCE 3,081.07"
    patterns["opening_balance"] = re.compile(
        r"STATEMENT\s+OPENING\s+BALANCE\s+([\d,]+\.\d{2})",
        re.IGNORECASE,
    )
    # Closing balance: use "Available Balance:R1,007.68"
    patterns["closing_balance"] = re.compile(
        r"Available\s+Balance\s*:\s*R\s*([\d,]+\.\d{2})",
        re.IGNORECASE,
    )

    # dd MMM yy  DESCRIPTION  -amount  balance
    text_line_pattern = (
        r"^(?P<date>\d{1,2}\s+\w{3}\s+\d{2})\s+"
        r"(?P<description>.+?)\s+"
        r"(?P<amount>-?[\d,]+\.\d{2})\s+"
        r"(?P<balance>-?[\d,]+\.\d{2})\s*$"
    )

    date_fmts = sa_date_formats() + ["%d %b %y"]

    return sa_base_profile(
        name="Standard Bank Prestige",
        detection_keywords=_COMMON_KEYWORDS + [
            "prestigeacc", "3 month statement", "mooirivier",
            "statement opening balance",
        ],
        header_patterns=patterns,
        text_line_pattern=text_line_pattern,
        date_formats=date_fmts,
        column_keywords=_sb_column_keywords(),
        thousands_separator=",",
        prefer_text_extraction=True,
        unsigned_is_debit=False,
    )


# ---------------------------------------------------------------------------
# Variant 4 — Achieva Current Account (English)
# ---------------------------------------------------------------------------

def standard_bank_achieva_profile() -> BankProfile:
    """Standard Bank Achieva current-account English variant.

    Text layout (columns: Details | Service Fee | Debits Credits | Date | Balance):
        CHEQUE CARD PURCHASE  376.35-  12 13  27,123.72
        NEW UBER EATS 5222*6036 11 DEC      ← continuation line

    Dates: ``12 13`` (MM DD — month-space-day, no year).
    Amounts: ``376.35-`` (trailing minus for debits, comma-thousands, dot-decimal).
    Multi-line descriptions.
    """
    patterns = _sb_base_patterns()

    # "Statement from 12 December 2025 to 12 January 2026"
    patterns["period_start"] = re.compile(
        r"Statement\s+from\s+(\d{1,2}\s+\w+\s+\d{4})",
        re.IGNORECASE,
    )

    # "BALANCE BROUGHT FORWARD  12 12  27,500.07"
    patterns["opening_balance"] = re.compile(
        r"BALANCE\s+BROUGHT\s+FORWARD\s+\d{1,2}\s+\d{1,2}\s+([\d,]+\.\d{2})",
        re.IGNORECASE,
    )
    # "Month-end Balance R1,721.62"
    patterns["closing_balance"] = re.compile(
        r"Month[\-\s]*end\s+Balance\s+R\s*([\d,]+\.\d{2})",
        re.IGNORECASE,
    )

    # Named-group pattern — description-first, date in the middle:
    #   DESCRIPTION  amount[-]  MM DD  balance
    text_line_pattern = (
        r"^(?P<description>[A-Z].+?)\s+"
        r"(?P<amount>[\d,]+\.\d{2}-?)\s+"
        r"(?P<date>\d{1,2}\s+\d{1,2})\s+"
        r"(?P<balance>[\d,]+\.\d{2})\s*$"
    )

    # MM DD → year 1900, corrected by _fix_yearless_dates
    date_fmts = sa_date_formats() + ["%m %d"]

    return sa_base_profile(
        name="Standard Bank Achieva",
        detection_keywords=_COMMON_KEYWORDS + [
            "achieva current account", "bank statement / tax invoice",
            "balance brought forward", "month-end balance",
            "statement frequency",
        ],
        header_patterns=patterns,
        text_line_pattern=text_line_pattern,
        date_formats=date_fmts,
        column_keywords=_sb_column_keywords(),
        thousands_separator=",",
        prefer_text_extraction=True,
        unsigned_is_debit=True,
    )


# ---------------------------------------------------------------------------
# Variant 5 — Achieva Current Account (Afrikaans)
# ---------------------------------------------------------------------------

def standard_bank_achieva_afrikaans_profile() -> BankProfile:
    """Standard Bank Achieva current-account Afrikaans variant.

    Same layout as the English Achieva but with Afrikaans labels:
        Besonderhede | Diensgeld | Debiete Krediete | Datum | Saldo

    Text layout:
        TJEKKAART-AANKOOP  47.98-  11 13  2,937.09
        OK EXPRESS GR 4451*5208 11 NOV        ← continuation line
    """
    patterns = _sb_base_patterns()

    # "Staat van 12 November 2025 tot 12 Desember 2025"
    patterns["period_start"] = re.compile(
        r"Staat\s+van\s+(\d{1,2}\s+\w+\s+\d{4})",
        re.IGNORECASE,
    )

    # "SALDO OORGEBRING  11 12  2,985.07"
    patterns["opening_balance"] = re.compile(
        r"SALDO\s+OORGEBRING\s+\d{1,2}\s+\d{1,2}\s+([\d,]+\.\d{2})",
        re.IGNORECASE,
    )
    # "Maandeindesaldo R16,378.17"
    patterns["closing_balance"] = re.compile(
        r"Maandeindesaldo\s+R\s*([\d,]+\.\d{2})",
        re.IGNORECASE,
    )

    # Same description-first layout as English Achieva
    text_line_pattern = (
        r"^(?P<description>[A-Z].+?)\s+"
        r"(?P<amount>[\d,]+\.\d{2}-?)\s+"
        r"(?P<date>\d{1,2}\s+\d{1,2})\s+"
        r"(?P<balance>[\d,]+\.\d{2})\s*$"
    )

    date_fmts = sa_date_formats() + ["%m %d"]

    return sa_base_profile(
        name="Standard Bank Achieva Afrikaans",
        detection_keywords=_COMMON_KEYWORDS + [
            "achieva tjekrekening", "bankstaat / belastingfaktuur",
            "saldo oorgebring", "maandeindesaldo",
            "staatfrekwensie",
        ],
        header_patterns=patterns,
        text_line_pattern=text_line_pattern,
        date_formats=date_fmts,
        column_keywords=_sb_column_keywords(afrikaans=True),
        thousands_separator=",",
        prefer_text_extraction=True,
        unsigned_is_debit=True,
    )
