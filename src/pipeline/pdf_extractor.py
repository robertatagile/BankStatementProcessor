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

AFRIKAANS_MONTH_ALIASES = {
    "januarie": "January",
    "jan": "Jan",
    "februarie": "February",
    "feb": "Feb",
    "maart": "March",
    "mrt": "Mar",
    "april": "April",
    "apr": "Apr",
    "mei": "May",
    "junie": "June",
    "jun": "Jun",
    "julie": "July",
    "jul": "Jul",
    "augustus": "August",
    "aug": "Aug",
    "september": "September",
    "sep": "Sep",
    "oktober": "October",
    "okt": "Oct",
    "november": "November",
    "nov": "Nov",
    "desember": "December",
    "des": "Dec",
}

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

        # Use profile name if bank_name couldn't be parsed from the text
        if context.raw_header.get("bank_name") == "Unknown Bank" and profile.name != "Generic":
            context.raw_header["bank_name"] = profile.name

        # Fix year-less dates (e.g. DD Mon → year defaults to 1900)
        self._fix_yearless_dates(context.raw_lines, context.raw_header)

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
                if field_name == "account_number":
                    first_numeric_line = next(
                        (line.strip() for line in value.splitlines() if re.search(r"\d", line)),
                        value,
                    )
                    value = re.sub(r"\s+", " ", first_numeric_line).strip()
                elif field_name in ("opening_balance", "closing_balance"):
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

        self._apply_profile_header_fallbacks(text, header, profile)

        # Extract personal/address info (account holder, street, suburb, postal code, account type)
        self._extract_personal_info(text, header, profile)

        return header

    def _apply_profile_header_fallbacks(
        self, text: str, header: dict, profile: BankProfile
    ) -> None:
        """Apply bank-specific header fallbacks for noisy PDF renderings."""
        if profile.name == "ABSA":
            self._apply_absa_header_fallbacks(text, header, profile)
            return

        if profile.name == "TymeBank":
            self._apply_tymebank_header_fallbacks(text, header, profile)
            return

        if profile.name != "FNB":
            return

        bank_name = (header.get("bank_name") or "").strip()
        if (
            not bank_name
            or bank_name == "Unknown Bank"
            or "online bank" in bank_name.lower()
        ):
            header["bank_name"] = profile.name

        if header.get("account_number") == "Unknown":
            afrikaans_account_match = re.search(
                r"(?im)^\s*Rekeningnommer\s*$.*?^\s*(\d{10,12})\b",
                text,
                re.DOTALL,
            )
            if afrikaans_account_match:
                header["account_number"] = afrikaans_account_match.group(1)

        if header.get("period_end") or header.get("period_start"):
            return

        online_bank_match = re.search(
            r"(?im)^\s*(\d{1,2}/\d{1,2}/\d{2,4})"
            r"(?:,\s*\d{1,2}:\d{2}\s*(?:AM|PM))?\s+Online\s+Bank(?:ing)?\b",
            text,
        )
        if not online_bank_match:
            return

        statement_date = self._parse_date_with_profile(
            online_bank_match.group(1), profile
        )
        if statement_date is not None:
            header["period_end"] = statement_date

    def _apply_absa_header_fallbacks(
        self, text: str, header: dict, profile: BankProfile
    ) -> None:
        """Apply ABSA-specific fallbacks for Afrikaans and eStamp layouts."""
        bank_name_match = re.search(
            r"(?im)\b(Absa\s+Bank\s+Ltd|Absa\s+Bank|ABSA\s+BANK)\b",
            text,
        )
        if bank_name_match:
            header["bank_name"] = bank_name_match.group(1)
        elif header.get("bank_name") == "Unknown Bank":
            header["bank_name"] = profile.name

        if header.get("account_number") == "Unknown":
            account_match = re.search(
                r"(?im)Tjekrekeningnommer\s*:\s*([\d\-\s]{8,})",
                text,
            )
            if not account_match:
                account_match = re.search(
                    r"(?im)\bABSA\b.*?\n.*?\b(\d{10})\b\s*$",
                    text,
                    re.DOTALL,
                )
            if account_match:
                header["account_number"] = re.sub(r"\D", "", account_match.group(1))

        if header.get("period_start") is None or header.get("period_end") is None:
            period_match = re.search(
                r"(?im)(?:Staat\s+vir\s+die\s+Periode|Tjekrekeningstaat)\s*[:\-]?\s*"
                r"([^\n]+?)\s+(?:tot|to|-)\s+([^\n]+)",
                text,
            )
            if period_match:
                header["period_start"] = header.get("period_start") or self._parse_date_with_profile(period_match.group(1).strip(), profile)
                header["period_end"] = header.get("period_end") or self._parse_date_with_profile(period_match.group(2).strip(), profile)

        if header.get("period_end") is None:
            issued_match = re.search(
                r"(?im)Uitgereik\s+op\s*:\s*([0-9A-Za-z/\-]+)",
                text,
            )
            if issued_match:
                header["period_end"] = self._parse_date_with_profile(issued_match.group(1), profile)

        if header.get("period_end") is None:
            estamp_match = re.search(r"(?im)^\s*(\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4})\s*$", text)
            if estamp_match:
                header["period_end"] = self._parse_date_with_profile(estamp_match.group(1), profile)

        if header.get("period_start") is None and header.get("period_end") is not None:
            header["period_start"] = header["period_end"]

        if header.get("opening_balance") == Decimal("0.00"):
            opening_match = re.search(r"(?im)Saldo\s+oorgedra\s+(-?R?[\d\s,.]+)", text)
            if opening_match:
                parsed = profile.parse_amount(opening_match.group(1))
                if parsed is not None:
                    header["opening_balance"] = parsed

        if header.get("closing_balance") in (Decimal("0.00"), None):
            closing_match = re.search(r"(?im)(?:Huidige\s+Saldo|Saldo)\s+(-?R?[\d\s,.]+)", text)
            if closing_match:
                parsed = profile.parse_amount(closing_match.group(1))
                if parsed is not None:
                    header["closing_balance"] = parsed
            if header.get("closing_balance") in (Decimal("0.00"), None):
                summary_balance_match = re.search(
                    r"(?im)^\s*Saldo\s+(-?R?[\d\s,.]+)\s*$",
                    text,
                )
                if summary_balance_match:
                    parsed = profile.parse_amount(summary_balance_match.group(1))
                    if parsed is not None:
                        header["closing_balance"] = parsed

    def _apply_tymebank_header_fallbacks(
        self, text: str, header: dict, profile: BankProfile
    ) -> None:
        """Apply TymeBank-specific fallbacks."""
        # Bank name is in the footer, not the header area
        if header.get("bank_name") == "Unknown Bank":
            header["bank_name"] = profile.name

        # Account type: "EveryDay Business account" or "EveryDay account"
        if "account_type" not in header:
            acct_match = re.search(
                r"(EveryDay(?:\s+Business)?\s+account)", text, re.IGNORECASE
            )
            if acct_match:
                header["account_type"] = acct_match.group(1)

        # Opening/Closing balance from transaction pages
        if header.get("opening_balance") == Decimal("0.00"):
            opening_match = re.search(
                r"Opening\s+Balance\s+([\d ]+\.\d{2})", text
            )
            if opening_match:
                parsed = profile.parse_amount(opening_match.group(1))
                if parsed is not None:
                    header["opening_balance"] = parsed

        if header.get("closing_balance") in (Decimal("0.00"), None):
            closing_match = re.search(
                r"Closing\s+Balance\s+([\d ]+\.\d{2})", text
            )
            if closing_match:
                parsed = profile.parse_amount(closing_match.group(1))
                if parsed is not None:
                    header["closing_balance"] = parsed

    def _extract_personal_info(self, text: str, header: dict, profile: Optional[BankProfile] = None) -> None:
        """Extract personal, address, and account info from bank statement text.

        Supports multiple bank formats:
        - FNB: ``*COMPANY_NAME (PTY) LTD`` followed by street address lines
        - African Bank: ``Statement for: NAME`` followed by address block
        - Generic: street address lines, suburb lines, postal codes
        """
        # --- 1. Account holder from profile header_patterns (if available) ---
        if profile and "account_holder" in profile.header_patterns:
            if "account_holder" not in header:
                m = profile.header_patterns["account_holder"].search(text)
                if m:
                    header["account_holder"] = m.group(1).strip()

        # --- 2. Account type from profile header_patterns (if available) ---
        if profile and "account_type" in profile.header_patterns:
            if "account_type" not in header:
                m = profile.header_patterns["account_type"].search(text)
                if m:
                    header["account_type"] = m.group(1).strip()

        # --- 3. Try "Statement for: NAME" address block (African Bank style) ---
        stmt_for_match = re.search(
            r"Statement\s+for:\s*(.+?)\s*(?:Tax\s+Invoice|\n)",
            text, re.IGNORECASE,
        )
        if stmt_for_match:
            name = stmt_for_match.group(1).strip()
            if "account_holder" not in header:
                header["account_holder"] = name

            # Extract the address block after "Statement for: NAME"
            # African Bank format: name\nline1\nline2\ncity\nprovince\npostal
            # Note: pdfplumber may merge columns, e.g. "14 AVENUE Tax Invoice"
            block_start = stmt_for_match.end()
            address_lines = []
            for line in text[block_start:block_start + 500].split("\n"):
                line = line.strip()
                if not line:
                    continue
                # Stop at known section headers
                if re.match(r"(?:STATEMENT\s+FOR|ACCOUNT\s+SUMMARY|PRODUCT\s+INFO|NUMBER\s+OF)", line, re.IGNORECASE):
                    break
                # Strip right-column noise merged by pdfplumber
                line = re.split(r"\s+(?:Tax\s+Invoice|VAT\s+registration|MyWORLD|Account\b)", line, maxsplit=1)[0].strip()
                if not line:
                    continue
                # Skip noise lines
                if re.search(r"(?:VAT|registration|fnb\.co\.za|Lost\s+Cards|Enquiries|Fraud|Relationship|087-|011\s)", line, re.IGNORECASE):
                    continue
                address_lines.append(line)
                if len(address_lines) >= 5:
                    break

            if address_lines:
                header.setdefault("address_line1", address_lines[0])
            if len(address_lines) > 1:
                header.setdefault("address_line2", address_lines[1])
            if len(address_lines) > 2:
                header.setdefault("address_line3", address_lines[2])
            # Check last line(s) for postal code (3-4 digits)
            for addr_line in reversed(address_lines):
                postal_m = re.match(r"^(\d{3,5})$", addr_line.strip())
                if postal_m:
                    header.setdefault("postal_code", postal_m.group(1))
                    break

        # --- 4a. Address block between account holder and "Tax Invoice" ---
        # Handles Capitec's two-column layout where customer address is merged
        # with the bank's address (e.g. "152 wilde amandel 5 Neutron Road").
        if "address_line1" not in header:
            holder_name = header.get("account_holder", "")
            if holder_name:
                holder_idx = text.find(holder_name)
                if holder_idx >= 0:
                    after_holder = text[holder_idx + len(holder_name):]
                    stop_m = re.search(r"(?:Tax\s+Invoice|Account\s+\d)", after_holder, re.IGNORECASE)
                    block = after_holder[:stop_m.start()] if stop_m else after_holder[:500]
                    addr_lines = []
                    # Known bank-side address fragments to strip from merged lines
                    bank_noise = re.compile(
                        r"(?:^|\s+)(?:Capitec\s+Bank|Stellenbosch|Techno\s+Park|\d+\s+Neutron\s+Road|"
                        r"Privaatsak|Private\s+Bag|fnb\.co\.za|Braampark|Forum\s+\d)",
                        re.IGNORECASE,
                    )
                    for line in block.split("\n"):
                        line = line.strip()
                        if not line:
                            continue
                        if re.match(r"(?:Tax|Account|Statement|VAT|24hr|Capitec\s+Bank\s+is)", line, re.IGNORECASE):
                            break
                        # Strip bank address noise (may be at start or mid-line)
                        left = bank_noise.split(line, maxsplit=1)[0].strip()
                        # Skip lines that are entirely bank address content
                        if not left or len(left) < 3:
                            continue
                        addr_lines.append(left)
                        if len(addr_lines) >= 4:
                            break
                    if addr_lines:
                        header.setdefault("address_line1", addr_lines[0])
                    if len(addr_lines) > 1:
                        header.setdefault("address_line2", addr_lines[1])
                    if len(addr_lines) > 2:
                        header.setdefault("address_line3", addr_lines[2])
                    # Postal code: 4-digit code (standalone or at start of merged line)
                    for addr_line in reversed(addr_lines):
                        postal_m = re.match(r"^(\d{4})\b", addr_line.strip())
                        if postal_m:
                            header.setdefault("postal_code", postal_m.group(1))
                            break

        # --- 4b. FNB-style: street address starting with digits ---
        if "address_line1" not in header:
            street_match = re.search(
                r"(?:^|\n)(\d+\s*[A-Z][A-Z\s]*(?:STREET|STR|STRAAT|ROAD|RD|WEG|AVENUE|AVE|LAAN|DRIVE|DR|RYLAAN|LANE|LN|WAY|CRESCENT|CRES|CLOSE|CL|PLACE|PL|PLEK|BOULEVARD|BLVD))\b",
                text, re.IGNORECASE,
            )
            if street_match:
                header["address_line1"] = street_match.group(1).strip()

        # --- 5. Suburb: all-caps word(s) on a line by themselves, or at start of line ---
        if "address_line2" not in header:
            # First try: pure uppercase line by itself
            suburb_pattern = re.compile(r"(?:^|\n)([A-Z]{4,}(?:\s+[A-Z]{3,})*)\s*$", re.MULTILINE)
            suburbs = []
            for m in suburb_pattern.finditer(text):
                candidate = m.group(1).strip()
                if re.search(r"(?:ACCOUNT|STATEMENT|BALANCE|TRANSACTION|DELIVERY|BRANCH|GOLD|PAGE|RAND|TURNOVER|CLOSING|OPENING|CHARGES|PRODUCT|SUMMARY)", candidate):
                    continue
                if candidate not in suburbs:
                    suburbs.append(candidate)

            # Second try: uppercase word(s) at start of line followed by non-alpha
            # (handles "MONUMENTPARK UIT 8 Posbus 7263")
            if not suburbs:
                start_pattern = re.compile(r"(?:^|\n)([A-Z]{4,}(?:\s+[A-Z]{3,})*)\s+(?=[a-z0-9])", re.MULTILINE)
                for m in start_pattern.finditer(text):
                    candidate = m.group(1).strip()
                    # Remove Afrikaans noise words
                    candidate = re.sub(r"\s+(?:UIT|VAN|NA)\s*$", "", candidate)
                    if re.search(r"(?:ACCOUNT|STATEMENT|BALANCE|TRANSACTION|DELIVERY|BRANCH|GOLD|PAGE|RAND|TURNOVER|CLOSING|OPENING|CHARGES|PRODUCT|SUMMARY)", candidate):
                        continue
                    if candidate not in suburbs and len(candidate) >= 4:
                        suburbs.append(candidate)

            if suburbs:
                header.setdefault("address_line2", suburbs[0])
                if len(suburbs) > 1:
                    header.setdefault("address_line3", suburbs[1])

        # --- 6. Postal code: 3-4 digit number at start of a line ---
        # Match 4-digit code followed by space+letter (city name) to avoid phone numbers
        if "postal_code" not in header:
            postal_match = re.search(r"(?:^|\n)(\d{4})\s+[A-Za-z]", text)
            if not postal_match:
                # Fallback: 3-digit code on its own line
                postal_match = re.search(r"(?:^|\n)(\d{3,4})\s*$", text, re.MULTILINE)
            if postal_match:
                header["postal_code"] = postal_match.group(1)

        # --- 7. Account type fallback: common SA account types ---
        if "account_type" not in header:
            acct_type_match = re.search(
                r"(Gold\s*Business\s*Account|Cheque\s*Account|Savings\s*Account|Current\s*Account|Credit\s*Card|Primary\s*Account|My\s*World\s*Account)",
                text, re.IGNORECASE,
            )
            if acct_type_match:
                header["account_type"] = acct_type_match.group(1)

    def _extract_lines(
        self, pdf: pdfplumber.PDF, profile: BankProfile
    ) -> list[dict]:
        """Extract transaction lines from all pages.

        Tries table extraction first; if a page's tables yield mostly empty
        amounts (common with FNB's merged-cell layout), falls back to text
        extraction for that page.
        """
        all_lines = []

        for page_num, page in enumerate(pdf.pages, 1):
            page_lines: list[dict] = []

            # Try table extraction first
            tables = page.extract_tables()
            if tables:
                for table in tables:
                    lines = self._parse_table(table, page_num, profile)
                    page_lines.extend(lines)

            # Check quality: if most lines have None amounts, tables are broken
            valid_amounts = sum(
                1 for ln in page_lines
                if ln.get("amount") is not None and not ln.get("_continuation")
            )
            total_lines = sum(
                1 for ln in page_lines if not ln.get("_continuation")
            )

            if total_lines == 0 or valid_amounts < total_lines * 0.5:
                # Tables produced bad data — fall back to text extraction
                text = page.extract_text()
                if text:
                    text_lines = self._parse_text(text, page_num, profile)
                    if text_lines:
                        logger.debug(
                            f"Page {page_num}: table extraction had "
                            f"{valid_amounts}/{total_lines} valid amounts; "
                            f"using text extraction ({len(text_lines)} lines)"
                        )
                        page_lines = text_lines

            if not page_lines:
                # No tables at all — try text extraction
                text = page.extract_text()
                if text:
                    page_lines = self._parse_text(text, page_num, profile)

            all_lines.extend(page_lines)

        # Handle multi-line descriptions (rows with description but no amount)
        all_lines = self._merge_multiline_descriptions(all_lines)

        # Filter out non-transaction lines (summary/header artifacts)
        all_lines = [
            ln for ln in all_lines
            if not re.match(
                r"(?:Opening\s+Balance|Closing\s+Balance|Available\s+Balance|"
                r"Saldo\s+Oorgedra)\b",
                ln.get("description", ""), re.IGNORECASE,
            )
        ]

        logger.debug(f"Total transaction lines extracted: {len(all_lines)}")
        return all_lines

    def _parse_table(
        self, table: list[list], page_num: int, profile: BankProfile
    ) -> list[dict]:
        """Parse a pdfplumber extracted table into transaction dicts."""
        lines = []
        if not table:
            return lines

        # Handle 1-row tables (Capitec style: each transaction is its own table)
        if len(table) == 1:
            row = table[0]
            if row and len(row) >= 3:
                # Check if this is the header row (Date, Description, ...)
                first_cell = str(row[0]).strip().lower() if row[0] else ""
                if first_cell in ("date", "datum"):
                    return lines  # skip header-only tables
                # Check if this is a footer/noise row
                if row[0] and str(row[0]).strip().startswith("*"):
                    return lines
                # Try to parse as a data row using the profile's default column map
                line = self._parse_row(row, profile.default_column_map, page_num, 1, profile)
                if line:
                    lines.append(line)
            return lines

        if len(table) < 2:
            return lines

        # Try to identify column positions from the header row
        header_row = [
            str(cell).strip().lower() if cell else "" for cell in table[0]
        ]
        col_map = self._identify_columns(header_row, profile)

        # Detect FNB-style merged-cell layout where amounts/balances are
        # packed into a single cell as newline-separated values in row[1],
        # while rows 2+ have the date/description but None for amount/balance.
        if len(table) >= 3:
            merged = self._extract_merged_cells(table, col_map)
            if merged is not None:
                merged_amounts, merged_balances = merged
                return self._parse_merged_table(
                    table, col_map, merged_amounts, merged_balances,
                    page_num, profile,
                )

        for row_idx, row in enumerate(table[1:], 2):
            if not row or all(
                cell is None or str(cell).strip() == "" for cell in row
            ):
                continue

            line = self._parse_row(row, col_map, page_num, row_idx, profile)
            if line:
                lines.append(line)

        return lines

    def _extract_merged_cells(
        self, table: list[list], col_map: dict
    ) -> Optional[tuple]:
        """Detect FNB merged-cell layout and return split amounts/balances.

        Returns ``(amounts_list, balances_list)`` if the layout is detected,
        or ``None`` if the table uses a normal layout.
        """
        if len(table) < 3:
            return None

        row1 = table[1]

        # Find the amount column index — try "amount", then "debit"
        amt_idx = col_map.get("amount", col_map.get("debit"))
        bal_idx = col_map.get("balance")

        if amt_idx is None:
            return None

        # Check if the amount cell in row[1] contains newline-separated values
        amt_cell = row1[amt_idx] if amt_idx < len(row1) and row1[amt_idx] else ""
        amt_cell = str(amt_cell).strip()
        if "\n" not in amt_cell:
            return None

        amounts = [v.strip() for v in amt_cell.split("\n") if v.strip()]
        if len(amounts) < 2:
            return None

        balances = []
        if bal_idx is not None and bal_idx < len(row1) and row1[bal_idx]:
            bal_cell = str(row1[bal_idx]).strip()
            balances = [v.strip() for v in bal_cell.split("\n") if v.strip()]

        # The "Cr"/"Dr" suffix may be split into an adjacent column
        # (e.g. col[5] has "r\nr\nr..." when the balance col has "1,234.56C")
        if balances:
            # Check the column after the balance for suffix fragments
            suffix_idx = bal_idx + 1 if bal_idx is not None else None
            if suffix_idx and suffix_idx < len(row1) and row1[suffix_idx]:
                suffix_cell = str(row1[suffix_idx]).strip()
                suffixes = [s.strip() for s in suffix_cell.split("\n") if s.strip()]
                if len(suffixes) == len(balances):
                    balances = [b + s for b, s in zip(balances, suffixes)]

        logger.debug(
            f"Detected merged-cell layout: {len(amounts)} amounts, "
            f"{len(balances)} balances"
        )
        return amounts, balances

    def _parse_merged_table(
        self,
        table: list[list],
        col_map: dict,
        amounts: list[str],
        balances: list[str],
        page_num: int,
        profile: BankProfile,
    ) -> list[dict]:
        """Parse a table with FNB-style merged amount/balance cells."""
        lines = []
        data_rows = table[2:]  # Skip header (row 0) and merged-values row (row 1)

        date_idx = col_map.get("date")
        desc_idx = col_map.get("description")

        for i, row in enumerate(data_rows):
            if not row or all(
                cell is None or str(cell).strip() == "" for cell in row
            ):
                continue

            date_str = ""
            if date_idx is not None and date_idx < len(row) and row[date_idx]:
                date_str = str(row[date_idx]).strip()

            description = ""
            if desc_idx is not None and desc_idx < len(row) and row[desc_idx]:
                description = str(row[desc_idx]).strip()

            if not description:
                continue

            parsed_date = (
                self._parse_date_with_profile(date_str, profile)
                if date_str else None
            )
            if not parsed_date and not date_str:
                lines.append({"_continuation": True, "description": description})
                continue

            amount_str = amounts[i] if i < len(amounts) else ""
            balance_str = balances[i] if i < len(balances) else ""

            amount = profile.parse_amount(amount_str)
            if amount is None:
                logger.debug(
                    f"Page {page_num}, row {i+2}: could not parse amount "
                    f"'{amount_str}', skipping"
                )
                continue

            # Determine transaction type from Cr/Dr suffix or sign
            amount_upper = amount_str.strip().upper()
            if amount_upper.endswith(("CR", "KT")):
                transaction_type = "credit"
            elif amount_upper.endswith(("DR", "DT")):
                transaction_type = "debit"
            elif amount < 0:
                transaction_type = "debit"
                amount = abs(amount)
            elif profile.unsigned_is_debit:
                transaction_type = "debit"
            else:
                transaction_type = "credit"

            lines.append({
                "date": parsed_date,
                "description": description,
                "amount": abs(amount),
                "balance": profile.parse_amount(balance_str) if balance_str else None,
                "transaction_type": transaction_type,
            })

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

            # If amount column is empty, try bank_charges column as fallback
            if amount is None and "bank_charges" in col_map:
                amount_str = get_cell("bank_charges")
                amount = profile.parse_amount(amount_str)

            # Negative amount = debit, positive = credit
            amount_upper = (amount_str or "").strip().upper()
            if amount_upper.endswith(("CR", "KT")):
                transaction_type = "credit"
            elif amount_upper.endswith(("DR", "DT")):
                transaction_type = "debit"
                amount = abs(amount) if amount is not None else amount
            elif amount is not None and amount < 0:
                transaction_type = "debit"
                amount = abs(amount)
            elif profile.unsigned_is_debit:
                transaction_type = "debit"
            else:
                transaction_type = "credit"
        else:
            debit_str = get_cell("debit")
            credit_str = get_cell("credit")
            debit = profile.parse_amount(debit_str)
            credit = profile.parse_amount(credit_str)

            if debit and abs(debit) > 0:
                amount = abs(debit)
                transaction_type = "debit"
            elif credit and abs(credit) > 0:
                amount = abs(credit)
                transaction_type = "credit"
            elif "bank_charges" in col_map:
                charges_str = get_cell("bank_charges")
                charges = profile.parse_amount(charges_str)
                if charges and charges > 0:
                    amount = charges
                    transaction_type = "debit"
                else:
                    if parsed_date:
                        logger.debug(
                            f"Page {page_num}, row {row_idx}: no amount found, skipping"
                        )
                    return None
            else:
                # Try fee column as fallback (Capitec puts fees in a separate column)
                fee_str = get_cell("fee")
                fee = profile.parse_amount(fee_str)
                if fee and abs(fee) > 0:
                    amount = abs(fee)
                    transaction_type = "debit"
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
                groups = match.groups()
                if len(groups) == 6:
                    # Multi-column format (e.g. TymeBank):
                    # date, desc, fees, money_out, money_in, balance
                    date_str, desc = groups[0], groups[1]
                    fees_str, out_str, in_str, balance_str = (
                        groups[2], groups[3], groups[4], groups[5],
                    )
                    # Pick the actual transaction amount from the right column
                    if out_str and out_str != "-":
                        amount_str = out_str
                        transaction_type = "debit"
                    elif in_str and in_str != "-":
                        amount_str = in_str
                        transaction_type = "credit"
                    elif fees_str and fees_str != "-":
                        amount_str = fees_str
                        transaction_type = "debit"
                    else:
                        continue
                else:
                    date_str, desc, amount_str, balance_str = groups
                    transaction_type = None  # determined below

                parsed_date = self._parse_date_with_profile(date_str, profile)
                amount = profile.parse_amount(amount_str)

                if parsed_date and amount is not None:
                    # Determine transaction type if not already set by column position
                    if transaction_type is None:
                        amount_upper = (amount_str or "").strip().upper()
                        desc_upper = desc.upper()
                        if amount_upper.endswith(("CR", "KT")):
                            transaction_type = "credit"
                        elif amount_upper.endswith(("DR", "DT")):
                            transaction_type = "debit"
                        elif amount < 0:
                            transaction_type = "debit"
                            amount = abs(amount)
                        elif re.search(r"\bKT\b", desc_upper):
                            # Afrikaans: "Kt" = Krediet (credit)
                            transaction_type = "credit"
                        elif profile.unsigned_is_debit:
                            transaction_type = "debit"
                        else:
                            transaction_type = "credit"

                    lines.append(
                        {
                            "date": parsed_date,
                            "description": desc.strip(),
                            "amount": abs(amount),
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
    def _fix_yearless_dates(lines: list, header: dict) -> None:
        """Replace year=1900 dates with the year from the statement period.

        When a date format like ``%d %b`` is used (FNB), ``strptime`` defaults
        the year to 1900. This method infers the correct year from the
        statement's ``period_end`` (or ``period_start``) date.
        """
        period_end = header.get("period_end")
        period_start = header.get("period_start")
        ref_date = period_end or period_start
        if ref_date is None:
            return

        # ref_date may be a date or datetime
        ref_year = ref_date.year if hasattr(ref_date, "year") else None
        if ref_year is None or ref_year < 1901:
            return

        for line in lines:
            d = line.get("date")
            if d is not None and hasattr(d, "year") and d.year == 1900:
                line["date"] = d.replace(year=ref_year)

    # Afrikaans → English month name mapping for date parsing
    _AFR_MONTHS = {
        "Jan": "Jan", "Feb": "Feb", "Mrt": "Mar", "Maa": "Mar",
        "Apr": "Apr", "Mei": "May", "Jun": "Jun", "Jul": "Jul",
        "Aug": "Aug", "Sep": "Sep", "Okt": "Oct", "Nov": "Nov",
        "Des": "Dec",
        "Januarie": "January", "Februarie": "February",
        "Maart": "March", "April": "April", "Mei": "May",
        "Junie": "June", "Julie": "July", "Augustus": "August",
        "September": "September", "Oktober": "October",
        "November": "November", "Desember": "December",
    }

    @classmethod
    def _normalize_afrikaans_date(cls, date_str: str) -> str:
        """Translate Afrikaans month names to English for strptime."""
        for afr, eng in cls._AFR_MONTHS.items():
            if afr in date_str:
                return date_str.replace(afr, eng)
        return date_str

    @classmethod
    def _parse_date_with_profile(
        cls, date_str: str, profile: BankProfile
    ) -> Optional[datetime]:
        """Try profile's date formats to parse a date string."""
        if not date_str:
            return None
        date_str = date_str.strip()
        # Try original, then module-level Afrikaans normalization, then class-level normalization
        candidates = {date_str, PDFExtractorStage._normalize_date_text(date_str), cls._normalize_afrikaans_date(date_str)}
        for candidate in candidates:
            for fmt in profile.date_formats:
                try:
                    return datetime.strptime(candidate, fmt).date()
                except ValueError:
                    continue
        return None

    @staticmethod
    def _normalize_date_text(date_str: str) -> str:
        """Normalize month names from FNB Afrikaans renderings to English."""
        normalized = date_str
        for source, target in AFRIKAANS_MONTH_ALIASES.items():
            normalized = re.sub(
                rf"\b{source}\b",
                target,
                normalized,
                flags=re.IGNORECASE,
            )
        return normalized

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
