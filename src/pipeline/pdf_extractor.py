from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Optional

import pdfplumber

from src.pipeline.queue import PipelineContext, Stage
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Date formats to try when parsing transaction dates
DATE_FORMATS = [
    "%d/%m/%Y",
    "%d-%m-%Y",
    "%d %b %Y",
    "%d %B %Y",
    "%m/%d/%Y",
    "%Y-%m-%d",
    "%d/%m/%y",
    "%d-%m-%y",
]

# Regex patterns for extracting header fields
HEADER_PATTERNS = {
    "bank_name": re.compile(
        r"^(.+?(?:Bank|Building Society|Credit Union|Financial))", re.IGNORECASE
    ),
    "account_number": re.compile(
        r"(?:Account\s*(?:Number|No\.?|#)?)\s*[:\-]?\s*(\d[\d\s\-]{4,})", re.IGNORECASE
    ),
    "sort_code": re.compile(
        r"(?:Sort\s*Code)\s*[:\-]?\s*(\d{2}[\-\s]?\d{2}[\-\s]?\d{2})", re.IGNORECASE
    ),
    "period_start": re.compile(
        r"(?:Statement\s+(?:Period|Date|From))\s*[:\-]?\s*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})",
        re.IGNORECASE,
    ),
    "period_end": re.compile(
        r"(?:to|ending|through)\s*[:\-]?\s*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})",
        re.IGNORECASE,
    ),
    "opening_balance": re.compile(
        r"(?:Opening|Start|Beginning|Brought\s+Forward)\s*(?:Balance)?\s*[:\-]?\s*[£$€]?\s*([\d,]+\.\d{2})",
        re.IGNORECASE,
    ),
    "closing_balance": re.compile(
        r"(?:Closing|End|Ending|Carried\s+Forward)\s*(?:Balance)?\s*[:\-]?\s*[£$€]?\s*([\d,]+\.\d{2})",
        re.IGNORECASE,
    ),
}


class PDFExtractorStage(Stage):
    """Stage 1: Extract statement headers and transaction lines from a PDF."""

    def process(self, context: PipelineContext) -> PipelineContext:
        logger.info(f"Extracting data from: {context.file_path}")

        with pdfplumber.open(context.file_path) as pdf:
            full_text = "\n".join(
                page.extract_text() or "" for page in pdf.pages
            )
            context.raw_header = self._extract_header(full_text)
            context.raw_lines = self._extract_lines(pdf)

        logger.info(
            f"Extracted header: {context.raw_header.get('bank_name', 'Unknown')} | "
            f"Lines: {len(context.raw_lines)}"
        )
        return context

    def _extract_header(self, text: str) -> dict:
        """Extract statement header fields from the full document text."""
        header = {}
        for field_name, pattern in HEADER_PATTERNS.items():
            match = pattern.search(text)
            if match:
                value = match.group(1).strip()
                if field_name in ("opening_balance", "closing_balance"):
                    value = self._parse_amount(value)
                elif field_name in ("period_start", "period_end"):
                    value = self._parse_date(value)
                header[field_name] = value

        # Default missing fields
        header.setdefault("bank_name", "Unknown Bank")
        header.setdefault("account_number", "Unknown")
        header.setdefault("period_start", None)
        header.setdefault("period_end", None)
        header.setdefault("opening_balance", Decimal("0.00"))
        header.setdefault("closing_balance", Decimal("0.00"))

        return header

    def _extract_lines(self, pdf: pdfplumber.PDF) -> list[dict]:
        """Extract transaction lines from all pages."""
        all_lines = []

        for page_num, page in enumerate(pdf.pages, 1):
            # Try table extraction first
            tables = page.extract_tables()
            if tables:
                for table in tables:
                    lines = self._parse_table(table, page_num)
                    all_lines.extend(lines)
            else:
                # Fall back to text-based extraction
                text = page.extract_text()
                if text:
                    lines = self._parse_text(text, page_num)
                    all_lines.extend(lines)

        # Handle multi-line descriptions (rows with description but no amount)
        all_lines = self._merge_multiline_descriptions(all_lines)

        logger.debug(f"Total transaction lines extracted: {len(all_lines)}")
        return all_lines

    def _parse_table(self, table: list[list], page_num: int) -> list[dict]:
        """Parse a pdfplumber extracted table into transaction dicts."""
        lines = []
        if not table or len(table) < 2:
            return lines

        # Try to identify column positions from the header row
        header_row = [str(cell).strip().lower() if cell else "" for cell in table[0]]
        col_map = self._identify_columns(header_row)

        for row_idx, row in enumerate(table[1:], 2):
            if not row or all(cell is None or str(cell).strip() == "" for cell in row):
                continue

            line = self._parse_row(row, col_map, page_num, row_idx)
            if line:
                lines.append(line)

        return lines

    def _identify_columns(self, header_row: list[str]) -> dict:
        """Map column names to their indices."""
        col_map = {}
        for idx, col_name in enumerate(header_row):
            if not col_name:
                continue
            if any(kw in col_name for kw in ("date",)):
                col_map.setdefault("date", idx)
            elif any(kw in col_name for kw in ("description", "details", "particulars", "narrative")):
                col_map.setdefault("description", idx)
            elif any(kw in col_name for kw in ("debit", "withdrawal", "out", "dr")):
                col_map.setdefault("debit", idx)
            elif any(kw in col_name for kw in ("credit", "deposit", "in", "cr")):
                col_map.setdefault("credit", idx)
            elif any(kw in col_name for kw in ("balance",)):
                col_map.setdefault("balance", idx)
            elif any(kw in col_name for kw in ("amount",)):
                col_map.setdefault("amount", idx)

        # If no header match, use positional defaults:
        # [0]=Date, [1]=Description, [2]=Debit, [3]=Credit, [4]=Balance
        if not col_map:
            col_map = {"date": 0, "description": 1, "debit": 2, "credit": 3, "balance": 4}

        return col_map

    def _parse_row(self, row: list, col_map: dict, page_num: int, row_idx: int) -> dict | None:
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
        parsed_date = self._parse_date(date_str) if date_str else None
        if not parsed_date and not date_str:
            # This might be a continuation line (multi-line description)
            return {"_continuation": True, "description": description}

        # Parse amounts
        if "amount" in col_map:
            amount_str = get_cell("amount")
            amount = self._parse_amount(amount_str)
            # Negative amount = debit, positive = credit
            if amount is not None and amount < 0:
                transaction_type = "debit"
                amount = abs(amount)
            else:
                transaction_type = "credit"
        else:
            debit_str = get_cell("debit")
            credit_str = get_cell("credit")
            debit = self._parse_amount(debit_str)
            credit = self._parse_amount(credit_str)

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
        balance = self._parse_amount(balance_str)

        return {
            "date": parsed_date,
            "description": description,
            "amount": amount,
            "balance": balance,
            "transaction_type": transaction_type,
        }

    def _parse_text(self, text: str, page_num: int) -> list[dict]:
        """Fall back to line-by-line regex extraction from raw text."""
        lines = []
        # Pattern: Date Description Amount (optional balance)
        # e.g. "01/01/2024  TESCO STORES  -45.67  1,234.56"
        pattern = re.compile(
            r"(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})\s+"  # date
            r"(.+?)\s+"                                     # description
            r"(-?[£$€]?\s?[\d,]+\.\d{2})"                 # amount
            r"(?:\s+(-?[£$€]?\s?[\d,]+\.\d{2}))?"         # optional balance
        )

        for line in text.split("\n"):
            match = pattern.search(line.strip())
            if match:
                date_str, desc, amount_str, balance_str = match.groups()
                parsed_date = self._parse_date(date_str)
                amount = self._parse_amount(amount_str)

                if parsed_date and amount is not None:
                    if amount < 0:
                        transaction_type = "debit"
                        amount = abs(amount)
                    else:
                        transaction_type = "credit"

                    lines.append({
                        "date": parsed_date,
                        "description": desc.strip(),
                        "amount": amount,
                        "balance": self._parse_amount(balance_str) if balance_str else None,
                        "transaction_type": transaction_type,
                    })

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
    def _parse_date(date_str: str) -> datetime | None:
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
    def _parse_amount(amount_str: str) -> Decimal | None:
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
