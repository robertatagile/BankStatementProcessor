"""First National Bank (FirstRand) profile."""
from __future__ import annotations

import re

from src.profiles.base import BankProfile
from src.profiles.banks._sa_common import sa_base_profile, sa_header_patterns, sa_date_formats


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
        r"\s+([\d,]+\.\d{2}(?:Cr|Dr)?)"           # Amount (optional Cr/Dr)
        r"\s+([\d,]+\.\d{2}(?:Cr|Dr))"            # Balance (must have Cr or Dr)
    )


def fnb_profile() -> BankProfile:
    """First National Bank (FirstRand) profile."""
    patterns = sa_header_patterns()
    # FNB: match "Account Number: 12345" or "Gold Business Account : 12345"
    patterns["account_number"] = re.compile(
        r"(?:Account\s*(?:Number|No\.?)|Gold\s*Business\s*Account)\s*[:\-]?\s*(\d{10,12})",
        re.IGNORECASE,
    )

    # FNB personal info: account holder marked with leading asterisk
    patterns["account_holder"] = re.compile(
        r"\*\s*(.+?(?:PTY|LTD|CC|INC|TRUST).*?)(?:\s*Universal|\n|$)",
        re.IGNORECASE,
    )

    # FNB date formats: DD Mon / DDMon (no year) + standard SA formats
    fnb_dates = ["%d%b", "%d %b"] + sa_date_formats()

    return sa_base_profile(
        name="FNB",
        detection_keywords=["fnb", "first national bank", "firstrand"],
        header_patterns=patterns,
        text_line_pattern=_fnb_text_line_pattern(),
        date_formats=fnb_dates,
    )
