"""Tests for the PDF Extractor stage."""

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from src.pipeline.pdf_extractor import PDFExtractorStage
from src.pipeline.queue import PipelineContext
from src.profiles.base import BankProfile
from src.profiles.south_africa import absa_profile, capitec_profile, fnb_profile


class TestParseDateHelper:
    """Test the backward-compatible static _parse_date method."""

    def test_dd_mm_yyyy(self):
        result = PDFExtractorStage._parse_date("01/01/2024")
        assert result == date(2024, 1, 1)

    def test_dd_mm_yy(self):
        result = PDFExtractorStage._parse_date("01/01/24")
        assert result == date(2024, 1, 1)

    def test_dd_mon_yyyy(self):
        result = PDFExtractorStage._parse_date("15 Jan 2024")
        assert result == date(2024, 1, 15)

    def test_yyyy_mm_dd(self):
        result = PDFExtractorStage._parse_date("2024-01-15")
        assert result == date(2024, 1, 15)

    def test_invalid_date(self):
        result = PDFExtractorStage._parse_date("not a date")
        assert result is None

    def test_empty_string(self):
        result = PDFExtractorStage._parse_date("")
        assert result is None

    def test_none(self):
        result = PDFExtractorStage._parse_date(None)
        assert result is None


class TestParseAmountHelper:
    """Test the backward-compatible static _parse_amount method."""

    def test_simple_amount(self):
        assert PDFExtractorStage._parse_amount("45.67") == Decimal("45.67")

    def test_with_currency_symbol(self):
        assert PDFExtractorStage._parse_amount("£1,234.56") == Decimal("1234.56")

    def test_negative_amount(self):
        assert PDFExtractorStage._parse_amount("-100.00") == Decimal("-100.00")

    def test_with_spaces(self):
        assert PDFExtractorStage._parse_amount("£ 45.67") == Decimal("45.67")

    def test_dollar_sign(self):
        assert PDFExtractorStage._parse_amount("$99.99") == Decimal("99.99")

    def test_empty_string(self):
        assert PDFExtractorStage._parse_amount("") is None

    def test_none(self):
        assert PDFExtractorStage._parse_amount(None) is None

    def test_invalid(self):
        assert PDFExtractorStage._parse_amount("abc") is None


class TestColumnIdentification:
    def test_standard_headers(self):
        stage = PDFExtractorStage()
        profile = BankProfile()
        headers = ["date", "description", "debit", "credit", "balance"]
        col_map = stage._identify_columns(headers, profile)
        assert col_map["date"] == 0
        assert col_map["description"] == 1
        assert col_map["debit"] == 2
        assert col_map["credit"] == 3
        assert col_map["balance"] == 4

    def test_alternative_headers(self):
        stage = PDFExtractorStage()
        profile = BankProfile()
        headers = ["date", "particulars", "withdrawal", "deposit", "balance"]
        col_map = stage._identify_columns(headers, profile)
        assert col_map["date"] == 0
        assert col_map["description"] == 1
        assert col_map["debit"] == 2
        assert col_map["credit"] == 3

    def test_empty_headers_uses_defaults(self):
        stage = PDFExtractorStage()
        profile = BankProfile()
        headers = ["", "", "", "", ""]
        col_map = stage._identify_columns(headers, profile)
        assert col_map == {
            "date": 0,
            "description": 1,
            "debit": 2,
            "credit": 3,
            "balance": 4,
        }

    def test_sa_style_headers(self):
        stage = PDFExtractorStage()
        profile = fnb_profile()
        headers = ["transaction date", "description", "debit (r)", "credit (r)", "running balance"]
        col_map = stage._identify_columns(headers, profile)
        assert col_map["date"] == 0
        assert col_map["description"] == 1
        assert col_map["debit"] == 2
        assert col_map["credit"] == 3
        assert col_map["balance"] == 4

    def test_capitec_amount_column_defaults(self):
        stage = PDFExtractorStage()
        profile = capitec_profile()
        headers = ["", "", "", ""]
        col_map = stage._identify_columns(headers, profile)
        assert col_map["amount"] == 2
        assert "debit" not in col_map


class TestMergeMultilineDescriptions:
    def test_merges_continuation(self):
        stage = PDFExtractorStage()
        lines = [
            {
                "date": date(2024, 1, 1),
                "description": "PAYMENT TO",
                "amount": Decimal("100.00"),
                "transaction_type": "debit",
            },
            {"_continuation": True, "description": "JOHN SMITH REF 123"},
        ]
        merged = stage._merge_multiline_descriptions(lines)
        assert len(merged) == 1
        assert merged[0]["description"] == "PAYMENT TO JOHN SMITH REF 123"

    def test_no_continuation(self):
        stage = PDFExtractorStage()
        lines = [
            {
                "date": date(2024, 1, 1),
                "description": "PAYMENT",
                "amount": Decimal("100.00"),
                "transaction_type": "debit",
            },
        ]
        merged = stage._merge_multiline_descriptions(lines)
        assert len(merged) == 1


class TestHeaderExtraction:
    def test_extracts_header_fields(self):
        stage = PDFExtractorStage()
        profile = BankProfile()
        text = """
        Acme Bank
        Account Number: 12345678
        Sort Code: 12-34-56
        Statement Period: 01/01/2024
        to: 31/01/2024
        Opening Balance: £1,000.00
        Closing Balance: £1,150.00
        """
        header = stage._extract_header(text, profile)
        assert header["account_number"] == "12345678"
        assert header["opening_balance"] == Decimal("1000.00")
        assert header["closing_balance"] == Decimal("1150.00")

    def test_defaults_for_missing_fields(self):
        stage = PDFExtractorStage()
        profile = BankProfile()
        header = stage._extract_header("Some random text with no header info", profile)
        assert header["bank_name"] == "Unknown Bank"
        assert header["account_number"] == "Unknown"
        assert header["opening_balance"] == Decimal("0.00")

    def test_sa_bank_header_extraction(self):
        stage = PDFExtractorStage()
        profile = absa_profile()
        text = """
        ABSA Bank
        Account Number: 1234567890
        Branch Code: 632005
        Statement Period: 01 January 2024 to 31 January 2024
        Opening Balance: R 10 000.00
        Closing Balance: R 12 500.00
        """
        header = stage._extract_header(text, profile)
        assert "1234567890" in header.get("account_number", "")
        assert header.get("branch_code") == "632005"
        assert header["opening_balance"] == Decimal("10000.00")
        assert header["closing_balance"] == Decimal("12500.00")


class TestProfileDateParsing:
    """Test date parsing with bank-specific profile formats."""

    def test_sa_date_dd_month_yyyy(self):
        profile = absa_profile()
        result = PDFExtractorStage._parse_date_with_profile(
            "15 January 2024", profile
        )
        assert result == date(2024, 1, 15)

    def test_sa_date_dd_mon_yyyy(self):
        profile = fnb_profile()
        result = PDFExtractorStage._parse_date_with_profile(
            "15 Jan 2024", profile
        )
        assert result == date(2024, 1, 15)

    def test_sa_date_dd_mm_yyyy(self):
        profile = fnb_profile()
        result = PDFExtractorStage._parse_date_with_profile(
            "15/01/2024", profile
        )
        assert result == date(2024, 1, 15)


class TestPDFExtractorWithProfile:
    """Test that PDFExtractorStage correctly uses provided profiles."""

    def test_constructor_with_profile(self):
        profile = absa_profile()
        stage = PDFExtractorStage(profile=profile, auto_detect=False)
        assert stage._profile.name == "ABSA"

    def test_constructor_default(self):
        stage = PDFExtractorStage()
        assert stage._profile is None
        assert stage._auto_detect is True
