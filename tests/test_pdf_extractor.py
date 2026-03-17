"""Tests for the PDF Extractor stage."""

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from src.pipeline.pdf_extractor import PDFExtractorStage
from src.pipeline.queue import PipelineContext


class TestParseDateHelper:
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
        headers = ["date", "description", "debit", "credit", "balance"]
        col_map = stage._identify_columns(headers)
        assert col_map["date"] == 0
        assert col_map["description"] == 1
        assert col_map["debit"] == 2
        assert col_map["credit"] == 3
        assert col_map["balance"] == 4

    def test_alternative_headers(self):
        stage = PDFExtractorStage()
        headers = ["date", "particulars", "withdrawal", "deposit", "balance"]
        col_map = stage._identify_columns(headers)
        assert col_map["date"] == 0
        assert col_map["description"] == 1
        assert col_map["debit"] == 2
        assert col_map["credit"] == 3

    def test_empty_headers_uses_defaults(self):
        stage = PDFExtractorStage()
        headers = ["", "", "", "", ""]
        col_map = stage._identify_columns(headers)
        assert col_map == {
            "date": 0,
            "description": 1,
            "debit": 2,
            "credit": 3,
            "balance": 4,
        }


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
        text = """
        Acme Bank
        Account Number: 12345678
        Sort Code: 12-34-56
        Statement Period: 01/01/2024
        to: 31/01/2024
        Opening Balance: £1,000.00
        Closing Balance: £1,150.00
        """
        header = stage._extract_header(text)
        assert header["account_number"] == "12345678"
        assert header["opening_balance"] == Decimal("1000.00")
        assert header["closing_balance"] == Decimal("1150.00")

    def test_defaults_for_missing_fields(self):
        stage = PDFExtractorStage()
        header = stage._extract_header("Some random text with no header info")
        assert header["bank_name"] == "Unknown Bank"
        assert header["account_number"] == "Unknown"
        assert header["opening_balance"] == Decimal("0.00")
