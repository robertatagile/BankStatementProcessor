"""Standard Bank of South Africa profile."""
from __future__ import annotations

import re

from src.profiles.base import BankProfile
from src.profiles.banks._sa_common import (
    sa_base_profile,
    sa_date_formats,
    sa_header_patterns,
)


def standard_bank_profile() -> BankProfile:
    """Standard Bank of South Africa profile.

    Handles OCR-scanned statements in Afrikaans with:
    - mm-dd date format (no year — year inferred from statement period)
    - R-prefixed amounts with mixed comma/period decimal separators
    - "Balance brought forward" / "Balance as at" patterns
    - Columns: Date | Description | Fee | Payments | Deposits | Balance
    """
    patterns = sa_header_patterns()

    # Standard Bank uses "Statement Period" or "Statement period" label.
    # OCR text: "Statement period 30 August 2023 to\n27 November 2023"
    patterns["period_start"] = re.compile(
        r"(?:Statement\s+[Pp]eriod)\s*[:\-]?\s*"
        r"(\d{1,2}\s+\w+\s+\d{4})",
        re.IGNORECASE,
    )

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

    # Custom text_line_pattern for Standard Bank OCR:
    # - Dates are mm-dd or mm.dd (no year), anchored to line start
    # - Amounts are R-prefixed (R-498.40 or R 7 300,00)
    # - Balance is R-prefixed at end of line
    # - Balance is optional (OCR may corrupt it)
    # - Anchored with ^ to avoid capturing partial dates from OCR artifacts
    # Allow common OCR digit substitutions (I/i/l/L/t→1, O/o→0) in the
    # date portion so lines like "1i-02" or "tL-24" are still captured.
    _ocr_digit = r"[0-9IilLtO]"
    text_line_pattern = (
        r"^(" + _ocr_digit + r"{1,2}\s*[-\.]\s*" + _ocr_digit + r"{1,2})\s+"
        r"(.+?)\s+"                              # description (non-greedy)
        r"(R\s*-?\s*[\d\s]+[.,]\s*\d{2})"       # amount (R prefix, optional minus)
        r"(?:\s+(R\s*[\d\s]+[.,]\s*\d{2}))?"     # optional balance
    )

    # Add mm-dd date format (strptime yields year=1900, fixed by _fix_yearless_dates)
    date_fmts = sa_date_formats() + ["%m-%d"]

    # Add Afrikaans column keywords for table detection
    column_keywords = {
        "date": ["date", "transaction date", "trans date", "datum"],
        "description": [
            "description", "details", "particulars", "narrative",
            "transaction description", "beskrywing",
        ],
        "debit": [
            "debit", "withdrawal", "dr", "debit (r)", "money out",
            "payments", "debiet", "betaling",
        ],
        "credit": [
            "credit", "deposit", "cr", "credit (r)", "money in",
            "deposits", "krediet", "inbetaling",
        ],
        "balance": ["balance", "running balance", "available balance", "saldo"],
        "amount": ["amount", "transaction amount", "bedrag"],
        "fee": ["fee", "fees", "fooi", "fooie"],
    }

    return sa_base_profile(
        name="Standard Bank",
        detection_keywords=[
            "standard bank", "sbsa", "standard bank of south africa",
            "universal branch",
        ],
        header_patterns=patterns,
        text_line_pattern=text_line_pattern,
        date_formats=date_fmts,
        column_keywords=column_keywords,
        # Standard Bank uses R- prefix for debits, R (no minus) for credits.
        # Unsigned amounts should default to credit, not debit.
        unsigned_is_debit=False,
    )
