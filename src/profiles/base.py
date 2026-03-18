from __future__ import annotations

import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional


def _default_header_patterns() -> Dict[str, re.Pattern]:
    """Return the default (generic/UK) header regex patterns."""
    return {
        "bank_name": re.compile(
            r"^(.+?(?:Bank|Building Society|Credit Union|Financial))",
            re.IGNORECASE,
        ),
        "account_number": re.compile(
            r"(?:Account\s*(?:Number|No\.?|#)?)\s*[:\-]?\s*(\d[\d\s\-]{4,})",
            re.IGNORECASE,
        ),
        "sort_code": re.compile(
            r"(?:Sort\s*Code)\s*[:\-]?\s*(\d{2}[\-\s]?\d{2}[\-\s]?\d{2})",
            re.IGNORECASE,
        ),
        "period_start": re.compile(
            r"(?:Statement\s+(?:Period|Date|From))\s*[:\-]?\s*"
            r"(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})",
            re.IGNORECASE,
        ),
        "period_end": re.compile(
            r"(?:to|ending|through)\s*[:\-]?\s*"
            r"(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})",
            re.IGNORECASE,
        ),
        "opening_balance": re.compile(
            r"(?:Opening|Start|Beginning|Brought\s+Forward)\s*(?:Balance)?\s*"
            r"[:\-]?\s*[£$€]?\s*([\d,]+\.\d{2})",
            re.IGNORECASE,
        ),
        "closing_balance": re.compile(
            r"(?:Closing|End|Ending|Carried\s+Forward)\s*(?:Balance)?\s*"
            r"[:\-]?\s*[£$€]?\s*([\d,]+\.\d{2})",
            re.IGNORECASE,
        ),
    }


def _default_date_formats() -> List[str]:
    """Return the default date format strings."""
    return [
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%d %b %Y",
        "%d %B %Y",
        "%m/%d/%Y",
        "%Y-%m-%d",
        "%d/%m/%y",
        "%d-%m-%y",
    ]


def _default_column_keywords() -> Dict[str, List[str]]:
    """Return the default column keyword mappings."""
    return {
        "date": ["date"],
        "description": ["description", "details", "particulars", "narrative"],
        "debit": ["debit", "withdrawal", "dr"],
        "credit": ["credit", "deposit", "cr"],
        "balance": ["balance"],
        "amount": ["amount"],
    }


def _default_column_map() -> Dict[str, int]:
    """Return the default positional column mapping."""
    return {"date": 0, "description": 1, "debit": 2, "credit": 3, "balance": 4}


@dataclass
class BankProfile:
    """Configuration profile for bank-specific PDF parsing."""

    name: str = "Generic"
    detection_keywords: List[str] = field(default_factory=list)

    # Currency
    currency_symbol: str = "£"
    thousands_separator: str = ","

    # Header extraction
    header_patterns: Dict[str, re.Pattern] = field(
        default_factory=_default_header_patterns
    )

    # Date parsing
    date_formats: List[str] = field(default_factory=_default_date_formats)

    # Table column identification
    column_keywords: Dict[str, List[str]] = field(
        default_factory=_default_column_keywords
    )
    default_column_map: Dict[str, int] = field(default_factory=_default_column_map)

    # Text-based line extraction pattern
    text_line_pattern: str = (
        r"(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})\s+"
        r"(.+?)\s+"
        r"(-?[£$€R]?\s?[\d,]+\.\d{2})"
        r"(?:\s+(-?[£$€R]?\s?[\d,]+\.\d{2}))?"
    )

    # Whether unsigned amounts (no Cr/Dr suffix, no minus sign) default to debit.
    # True for SA banks (which use Cr/Dr suffixes), False for generic (sign-based).
    unsigned_is_debit: bool = False

    def parse_amount(self, amount_str: str) -> Optional[Decimal]:
        """Parse an amount string into a Decimal, handling bank-specific formatting.

        Handles Cr/Dr suffixes (common in South African bank statements):
        - ``Cr`` suffix is stripped (credit indicator, amount stays positive)
        - ``Dr`` suffix is stripped and the amount is negated
        """
        if not amount_str:
            return None

        cleaned = amount_str.strip()

        # Detect and strip Cr/Dr suffix (e.g. "22,865.96Cr" or "113.68Dr")
        negate = False
        upper = cleaned.upper()
        if upper.endswith("CR"):
            cleaned = cleaned[:-2].strip()
        elif upper.endswith("DR"):
            cleaned = cleaned[:-2].strip()
            negate = True

        # Remove currency symbols (profile-specific + common ones)
        symbols = set(self.currency_symbol + "£$€R")
        cleaned = "".join(ch for ch in cleaned if ch not in symbols)

        # Handle trailing minus (e.g. "27 212,96-")
        if cleaned.endswith("-"):
            cleaned = cleaned[:-1].strip()
            negate = True

        # Remove thousands separators
        if self.thousands_separator == " ":
            # For space separator: remove spaces between digit groups
            cleaned = re.sub(r"(?<=\d)\s+(?=\d)", "", cleaned)

        # Handle comma-as-decimal (e.g. Afrikaans "16 270,50")
        # Detect: if there's a comma followed by exactly 2 digits at end and no dot
        if re.search(r",\d{2}$", cleaned) and "." not in cleaned:
            cleaned = cleaned.replace(",", ".")
        else:
            # Always remove commas as a universal thousands separator fallback
            cleaned = cleaned.replace(",", "")

        # Remove any other configured separator
        if self.thousands_separator not in (" ", ","):
            cleaned = cleaned.replace(self.thousands_separator, "")

        cleaned = cleaned.strip()
        if not cleaned:
            return None

        try:
            value = Decimal(cleaned)
            return -value if negate else value
        except (InvalidOperation, ValueError):
            return None

    def compile_text_pattern(self) -> re.Pattern:
        """Compile the text line extraction pattern."""
        return re.compile(self.text_line_pattern)
