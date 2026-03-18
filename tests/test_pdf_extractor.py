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

    def test_absa_afrikaans_header_extraction(self):
        stage = PDFExtractorStage()
        profile = absa_profile()
        text = """
        Stuur terug na: Absa Bank Ltd
        Tjekrekeningnommer: 7-1323-1819
        Tjekrekeningstaat
        17 Des 2025 tot 16 Jan 2026
        Rekeningopsomming:
        Saldo oorgedra 4 940,60
        Saldo 1 807,39
        """
        header = stage._extract_header(text, profile)

        assert header["bank_name"] == "Absa Bank Ltd"
        assert header["account_number"] == "7-1323-1819"
        assert header["period_start"] == date(2025, 12, 17)
        assert header["period_end"] == date(2026, 1, 16)
        assert header["opening_balance"] == Decimal("4940.60")
        assert header["closing_balance"] == Decimal("1807.39")

    def test_absa_estamp_header_fallbacks(self):
        stage = PDFExtractorStage()
        profile = absa_profile()
        text = """
        eStempel
        2026-02-23
        Transaksiegeskiedenis (2026-02-23 07:42:52)
        MELISSA ANNE BRAND ABSA
        VLAMBOOMSTRAAT 35, PO BOX 1641 4065399361
        KATHU SILVER TJEKREK
        Huidige Saldo -R1 158.44
        Staat vir die Periode 2025-11-23 - 2026-02-23
        """
        header = stage._extract_header(text, profile)

        assert header["bank_name"] == "ABSA"
        assert header["account_number"] == "4065399361"
        assert header["period_start"] == date(2025, 11, 23)
        assert header["period_end"] == date(2026, 2, 23)
        assert header["closing_balance"] == Decimal("-1158.44")

    def test_fnb_online_bank_header_uses_profile_name_and_statement_date(self):
        stage = PDFExtractorStage()
        profile = fnb_profile()
        text = """
        1/15/24, 9:07 PM Online Banking
        Gold Business Account: 62384104940
        """
        header = stage._extract_header(text, profile)

        assert header["bank_name"] == "FNB"
        assert header["account_number"] == "62384104940"
        assert header["period_end"] == date(2024, 1, 15)


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

    def test_fnb_online_banking_date_mm_dd_yy(self):
        profile = fnb_profile()
        result = PDFExtractorStage._parse_date_with_profile(
            "1/15/24", profile
        )
        assert result == date(2024, 1, 15)

    def test_fnb_afrikaans_date_parsing(self):
        profile = fnb_profile()
        result = PDFExtractorStage._parse_date_with_profile(
            "18 Desember 2023", profile
        )
        assert result == date(2023, 12, 18)

    def test_fnb_afrikaans_date_abbreviation_parsing(self):
        profile = fnb_profile()
        result = PDFExtractorStage._parse_date_with_profile(
            "18 Okt 2023", profile
        )
        assert result == date(2023, 10, 18)

    def test_fnb_afrikaans_header_extraction(self):
        stage = PDFExtractorStage()
        profile = fnb_profile()
        text = """
        Rekeningnommer
        62020354255
        Staat Periode : 18 September 2023 tot 18 Oktober 2023
        Staatdatum : 18 Oktober 2023
        Openingsaldo 44,809.60Dt
        Afsluitingsaldo 45,916.26Dt
        """
        header = stage._extract_header(text, profile)

        assert header["account_number"] == "62020354255"
        assert header["period_start"] == date(2023, 9, 18)
        assert header["period_end"] == date(2023, 10, 18)
        assert header["opening_balance"] == Decimal("-44809.60")
        assert header["closing_balance"] == Decimal("-45916.26")

    def test_fnb_afrikaans_text_line_extraction(self):
        stage = PDFExtractorStage()
        profile = fnb_profile()
        text = "18 Sep FNB OB Betaling Salaris 2,000.00Kt 2,742.29Kt"

        lines = stage._parse_text(text, 1, profile)

        assert len(lines) == 1
        assert lines[0]["date"] == date(1900, 9, 18)
        assert lines[0]["transaction_type"] == "credit"
        assert lines[0]["amount"] == Decimal("2000.00")
        assert lines[0]["balance"] == Decimal("2742.29")


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
