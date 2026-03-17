from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Optional

import pdfplumber

from src.pipeline.queue import PipelineContext, Stage
from src.profiles.base import BankProfile
from src.profiles.factory import BankProfileFactory
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Keep module-level constants for backward compatibility
DATE_FORMATS = BankProfile().date_formats
HEADER_PATTERNS = BankProfile().header_patterns


class PDFExtractorStage(Stage):
    """Stage 1: Extract statement headers and transaction lines from a PDF."""

    def __init__(
        self,
        profile: Optional[BankProfile] = None,
        auto_detect: bool = True,
    ):
        self._profile = profile
        self._auto_detect = auto_detect

    def process(self, context: PipelineContext) -> PipelineContext:
        logger.info(f"Extracting data from: {context.file_path}")

        with pdfplumber.open(context.file_path) as pdf:
            # Resolve the bank profile
            profile = self._resolve_profile(pdf)

            full_text = "\n".join(
                page.extract_text() or "" for page in pdf.pages
            )
            context.raw_header = self._extract_header(full_text, profile)
            context.raw_lines = self._extract_lines(pdf, profile)

        logger.info(
            f"Extracted header: {context.raw_header.get('bank_name', 'Unknown')} | "
            f"Profile: {profile.name} | Lines: {len(context.raw_lines)}"
        )
        return context

    def _resolve_profile(self, pdf: pdfplumber.PDF) -> BankProfile:
        """Determine which bank profile to use."""
        if self._profile is not None:
            return self._profile

        if self._auto_detect and pdf.pages:
            page1_text = pdf.pages[0].extract_text() or ""
            return BankProfileFactory.detect(page1_text)

        return BankProfile()

    def _extract_header(self, text: str, profile: BankProfile) -> dict:
        """Extract statement header fields from the full document text."""
        header = {}
        for field_name, pattern in profile.header_patterns.items():
            match = pattern.search(text)
            if match:
                value = match.group(1).strip()
                if field_name in ("opening_balance", "closing_balance"):
                    value = profile.parse_amount(value)
                elif field_name in ("period_start", "period_end"):
                    value = self._parse_date_with_profile(value, profile)
                header[field_name] = value

        # Default missing fields
        header.setdefault("bank_name", "Unknown Bank")
        header.setdefault("account_number", "Unknown")
        header.setdefault("period_start", None)
        header.setdefault("period_end", None)
        header.setdefault("opening_balance", Decimal("0.00"))
        header.setdefault("closing_balance", Decimal("0.00"))

        return header

    def _extract_lines(
        self, pdf: pdfplumber.PDF, profile: BankProfile
    ) -> list[dict]:
        """Extract transaction lines from all pages."""
        all_lines = []

        for page_num, page in enumerate(pdf.pages, 1):
            # Try table extraction first
            tables = page.extract_tables()
            if tables:
                for table in tables:
                    lines = self._parse_table(table, page_num, profile)
                    all_lines.extend(lines)
            else:
                # Fall back to text-based extraction
                text = page.extract_text()
                if text:
                    lines = self._parse_text(text, page_num, profile)
                    all_lines.extend(lines)

        # Handle multi-line descriptions (rows with description but no amount)
        all_lines = self._merge_multiline_descriptions(all_lines)

        logger.debug(f"Total transaction lines extracted: {len(all_lines)}")
        return all_lines

    def _parse_table(
        self, table: list[list], page_num: int, profile: BankProfile
    ) -> list[dict]:
        """Parse a pdfplumber extracted table into transaction dicts."""
        lines = []
        if not table or len(table) < 2:
            return lines

        # Try to identify column positions from the header row
        header_row = [
            str(cell).strip().lower() if cell else "" for cell in table[0]
        ]
        col_map = self._identify_columns(header_row, profile)

        for row_idx, row in enumerate(table[1:], 2):
            if not row or all(
                cell is None or str(cell).strip() == "" for cell in row
            ):
                continue

            line = self._parse_row(row, col_map, page_num, row_idx, profile)
            if line:
                lines.append(line)

        return lines

    def _identify_columns(
        self, header_row: list[str], profile: BankProfile
    ) -> dict:
        """Map column names to their indices using the profile's keyword mappings."""
        col_map = {}
        for idx, col_name in enumerate(header_row):
            if not col_name:
                continue
            for logical_col, keywords in profile.column_keywords.items():
                if any(kw in col_name for kw in keywords):
                    col_map.setdefault(logical_col, idx)
                    break

        # If no header match, use profile's positional defaults
        if not col_map:
            col_map = dict(profile.default_column_map)

        return col_map

    def _parse_row(
        self,
        row: list,
        col_map: dict,
        page_num: int,
        row_idx: int,
        profile: BankProfile,
    ) -> Optional[dict]:
        """Parse a single table row into a transaction dict."""

        def get_cell(key: str) -> str:
            idx = col_map.get(key)
            if idx is not None and idx < len(row) and row[idx]:
                return str(row[idx]).strip()
            return ""

        date_str = get_cell("date")
        description = get_cell("description")

        # Must have at least a description to be a valid transaction
        if not description:
            return None

        # Parse date
        parsed_date = (
            self._parse_date_with_profile(date_str, profile)
            if date_str
            else None
        )
        if not parsed_date and not date_str:
            # This might be a continuation line (multi-line description)
            return {"_continuation": True, "description": description}

        # Parse amounts
        if "amount" in col_map:
            amount_str = get_cell("amount")
            amount = profile.parse_amount(amount_str)
            # Negative amount = debit, positive = credit
            if amount is not None and amount < 0:
                transaction_type = "debit"
                amount = abs(amount)
            else:
                transaction_type = "credit"
        else:
            debit_str = get_cell("debit")
            credit_str = get_cell("credit")
            debit = profile.parse_amount(debit_str)
            credit = profile.parse_amount(credit_str)

            if debit and debit > 0:
                amount = debit
                transaction_type = "debit"
            elif credit and credit > 0:
                amount = credit
                transaction_type = "credit"
            else:
                # No amount found — skip or treat as continuation
                if parsed_date:
                    logger.debug(
                        f"Page {page_num}, row {row_idx}: no amount found, skipping"
                    )
                return None

        balance_str = get_cell("balance")
        balance = profile.parse_amount(balance_str)

        return {
            "date": parsed_date,
            "description": description,
            "amount": amount,
            "balance": balance,
            "transaction_type": transaction_type,
        }

    def _parse_text(
        self, text: str, page_num: int, profile: BankProfile
    ) -> list[dict]:
        """Fall back to line-by-line regex extraction from raw text."""
        lines = []
        pattern = profile.compile_text_pattern()

        for line in text.split("\n"):
            match = pattern.search(line.strip())
            if match:
                date_str, desc, amount_str, balance_str = match.groups()
                parsed_date = self._parse_date_with_profile(date_str, profile)
                amount = profile.parse_amount(amount_str)

                if parsed_date and amount is not None:
                    if amount < 0:
                        transaction_type = "debit"
                        amount = abs(amount)
                    else:
                        transaction_type = "credit"

                    lines.append(
                        {
                            "date": parsed_date,
                            "description": desc.strip(),
                            "amount": amount,
                            "balance": (
                                profile.parse_amount(balance_str)
                                if balance_str
                                else None
                            ),
                            "transaction_type": transaction_type,
                        }
                    )

        return lines

    def _merge_multiline_descriptions(self, lines: list[dict]) -> list[dict]:
        """Merge continuation lines into the previous transaction's description."""
        merged = []
        for line in lines:
            if line.get("_continuation") and merged:
                merged[-1]["description"] += " " + line["description"]
            elif not line.get("_continuation"):
                merged.append(line)
        return merged

    @staticmethod
    def _parse_date_with_profile(
        date_str: str, profile: BankProfile
    ) -> Optional[datetime]:
        """Try profile's date formats to parse a date string."""
        if not date_str:
            return None
        date_str = date_str.strip()
        for fmt in profile.date_formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        return None

    # ------------------------------------------------------------------
    # Backward-compatible static methods (used by existing tests)
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_date(date_str: str) -> Optional[datetime]:
        """Try multiple date formats to parse a date string."""
        if not date_str:
            return None
        date_str = date_str.strip()
        for fmt in DATE_FORMATS:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        return None

    @staticmethod
    def _parse_amount(amount_str: str) -> Optional[Decimal]:
        """Parse an amount string into a Decimal, handling currency symbols and commas."""
        if not amount_str:
            return None
        # Remove currency symbols and whitespace
        cleaned = re.sub(r"[£$€\s]", "", amount_str.strip())
        # Remove commas (thousands separators)
        cleaned = cleaned.replace(",", "")
        try:
            return Decimal(cleaned)
        except (InvalidOperation, ValueError):
            return None
