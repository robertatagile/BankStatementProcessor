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
            r"^(.+?(?:African\s+Bank|Bank|Capitec|ABSA|FNB|Nedbank))", re.IGNORECASE
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
    """ABSA Bank Afrikaans statement profile.

    ABSA issues statements in Afrikaans with different terminology:
    - "Tjekrekeningstaat" (cheque account statement)
    - "Saldo oorgedra" (balance brought forward)
    - "Debietbedrag" / "Kredietbedrag" (debit/credit amount)
    - "Transaksiebeskrywing" (transaction description)
    - Period uses "tot" instead of "to"
    - All text-based extraction (no pdfplumber tables)
    """
    patterns = _sa_header_patterns()

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
    ] + _sa_date_formats()

    return _sa_base_profile(
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
    patterns = _sa_header_patterns()
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
    fnb_dates = ["%d%b", "%d %b"] + _sa_date_formats()

    return _sa_base_profile(
        name="FNB",
        detection_keywords=["fnb", "first national bank", "firstrand"],
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


def african_bank_profile() -> BankProfile:
    """African Bank profile."""
    patterns = _sa_header_patterns()

    # African Bank shows "African Bank" at top of every page
    patterns["bank_name"] = re.compile(
        r"(African\s+Bank)", re.IGNORECASE
    )

    # Period is on a separate line: "2025/10/21 to 2026/01/04"
    patterns["period_start"] = re.compile(
        r"(\d{4}/\d{2}/\d{2})\s+to\s+\d{4}/\d{2}/\d{2}",
        re.IGNORECASE,
    )
    patterns["period_end"] = re.compile(
        r"\d{4}/\d{2}/\d{2}\s+to\s+(\d{4}/\d{2}/\d{2})",
        re.IGNORECASE,
    )

    # Account holder from product info table: "Account Holder  LUCHAN"
    patterns["account_holder"] = re.compile(
        r"Account\s+Holder\s+([A-Z][A-Z\s]+?)(?:\n|$)",
        re.IGNORECASE,
    )

    # Account type from product info table
    patterns["account_type"] = re.compile(
        r"Account\s+(?:Type|Name)\s+(.+?)(?:\n|$)",
        re.IGNORECASE,
    )

    # Account number from product info table: "Account Number  20114025968"
    patterns["account_number"] = re.compile(
        r"Account\s+Number\s+(\d{8,15})",
        re.IGNORECASE,
    )

    # Opening/closing balance (no R prefix, no Cr/Dr suffix)
    patterns["opening_balance"] = re.compile(
        r"Opening\s+Balance\s+([\d\s,]+\.\d{2})",
        re.IGNORECASE,
    )
    patterns["closing_balance"] = re.compile(
        r"Closing\s+Balance\s+([\d\s,]+\.\d{2})",
        re.IGNORECASE,
    )

    # Branch code: "Branch Code\n430000"
    patterns["branch_code"] = re.compile(
        r"Branch\s+Code\s*\n?\s*(\d{4,6})",
        re.IGNORECASE,
    )

    # African Bank column keywords — has a BANK CHARGES column between description and amount
    keywords = _sa_column_keywords()
    keywords["bank_charges"] = ["bank charges", "charges"]

    return _sa_base_profile(
        name="African Bank",
        detection_keywords=["african bank", "myworld", "my world"],
        header_patterns=patterns,
        column_keywords=keywords,
        date_formats=["%Y/%m/%d"] + _sa_date_formats(),
        # African Bank uses negative amounts for debits, positive for credits
        unsigned_is_debit=False,
        # Col 0=date, 1=description, 2=bank_charges(skip), 3=amount, 4=balance
        default_column_map={"date": 0, "description": 1, "amount": 3, "balance": 4},
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
    BankProfileFactory.register("african_bank", african_bank_profile)
    BankProfileFactory.register("absa_afrikaans", absa_afrikaans_profile)
