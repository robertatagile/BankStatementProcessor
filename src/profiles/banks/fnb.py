"""First National Bank (FirstRand) profile.

Supports English and Afrikaans FNB statements:
- English: Cr/Dr suffixes, "Account Number", "Opening Balance"
- Afrikaans: Kt/Dt suffixes, "Rekeningnommer", "Openingsaldo"/"Afsluitingsaldo"
- Both: DD Mon dates (no year), fnb.co.za
"""
from __future__ import annotations

import re

from src.profiles.base import BankProfile
from src.profiles.banks._sa_common import sa_base_profile, sa_header_patterns, sa_date_formats


def _fnb_text_line_pattern() -> str:
    """FNB-specific text extraction pattern.

    FNB uses ``DD Mon`` or ``DDMon`` dates (no year), amounts with optional
    ``Cr``/``Dr`` or ``Kt``/``Dt`` suffix.  The balance usually has a suffix
    but OCR-scanned overdraft statements may omit it.

    The space between day and month is optional because some FNB PDF renderings
    concatenate them (e.g. ``01Feb`` instead of ``01 Feb``).

    Leading OCR pipe characters (``|``) in descriptions are tolerated.
    """
    return (
        r"(\d{2}\s*\w{3})\s+\|?"                           # Date: DD[space?]Mon + optional OCR pipe
        r"(.+?)"                                            # Description (non-greedy)
        r"\s+([\d,]+\.\d{2}(?:Cr|Dr|Kt|Dt)?)"             # Amount (optional suffix)
        r"\s+([\d,]+\.\d{2}(?:Cr|Dr|Kt|Dt)?)"             # Balance (suffix optional for OCR/overdraft)
    )


def fnb_profile() -> BankProfile:
    """First National Bank (FirstRand) profile."""
    patterns = sa_header_patterns()

    # Account number: English or Afrikaans
    # "Account Number: 12345" or "Savings Account : 62317315548" or "Rekeningnommer 62317436740"
    patterns["account_number"] = re.compile(
        r"(?:Account\s*(?:Number|No\.?)|Gold\s*Business\s*Account"
        r"|Savings\s+Account|Rekeningnommer)\s*[:\-]?\s*(\d{10,12})",
        re.IGNORECASE,
    )

    # Account holder: asterisk-prefixed business names or MNR/MEV title
    patterns["account_holder"] = re.compile(
        r"(?:\*\s*(.+?(?:PTY|LTD|CC|INC|TRUST).*?)(?:\s*Universal|\n|$)"
        r"|(?:^|\n)((?:MNR|MEV|MR|MRS|MS|DR|PROF)\s+[A-Z][A-Z\s\-]+?)(?:\s+Universele|\n|$))",
        re.IGNORECASE | re.MULTILINE,
    )

    # Period: "Staat Periode : 4 Oktober 2023 tot 4 Januarie 2024"
    # or English: "Statement Period : 01 January 2024 to 31 January 2024"
    # Period: "Staat Periode : ..." or "Staat Periode - ..." (OCR dash) or "Statement Period : ..."
    patterns["period_start"] = re.compile(
        r"(?:Staat\s*Periode|Statement\s*Period)\s*[:\-]\s*"
        r"(\d{1,2}\s+\w+\s+\d{4})",
        re.IGNORECASE,
    )
    # Period end: "to 26 January 2024" or "tot 1 Maart 2024"
    # Avoid matching the dash in "Staat Periode - 1 Feb 2024" as a period_end
    patterns["period_end"] = re.compile(
        r"(?:to|tot|ending|through)\s+(\d{1,2}\s+\w+\s+\d{4})"
        r"|Datum\s+.*?(\d{4}/\d{2}/\d{2})",
        re.IGNORECASE,
    )

    # Opening balance: "Openingsaldo 127.36Kt" or "Opening Balance 127.36Cr"
    # OCR may produce "22 347 86 Dt" (spaces instead of comma/period)
    patterns["opening_balance"] = re.compile(
        r"(?:Openingsaldo|Opening\s*Balance)\s+([\d\s,]+[.,\s]\d{2}(?:Kt|Cr|Dt|Dr)?)\|?",
        re.IGNORECASE,
    )

    # Closing balance: "Afsluitingsaldo 5,497.24Kt" or "Closing Balance 5,497.24Cr"
    patterns["closing_balance"] = re.compile(
        r"(?:Afsluitingsaldo|Closing\s*Balance)\s+([\d\s,]+[.,\s]\d{2}(?:Kt|Cr|Dt|Dr)?)\|?",
        re.IGNORECASE,
    )

    # FNB date formats: DD Mon / DDMon (no year) + YYYY/MM/DD + standard SA formats
    fnb_dates = ["%d%b", "%d %b", "%Y/%m/%d", "%m/%d/%y", "%m/%d/%Y"] + sa_date_formats()

    return sa_base_profile(
        name="FNB",
        detection_keywords=[
            "fnb", "first national bank", "firstrand",
            "fnb.co.za", "universele takkode", "universal branch code",
            "fnb private", "fnb premier", "fnb app",
        ],
        header_patterns=patterns,
        text_line_pattern=_fnb_text_line_pattern(),
        date_formats=fnb_dates,
        # FNB tables split Cr/Dr suffixes across cells (e.g. "400.00C" + "r"),
        # losing credit/debit classification. Text extraction preserves them.
        prefer_text_extraction=True,
    )
