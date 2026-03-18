"""Tests for bank profiles and the profile factory."""

from decimal import Decimal

import pytest

from src.profiles.base import BankProfile
from src.profiles.factory import BankProfileFactory
from src.profiles.south_africa import (
    absa_profile,
    capitec_profile,
    fnb_profile,
    nedbank_profile,
    standard_bank_profile,
)


class TestBankProfileDefaults:
    """Verify that the generic BankProfile matches the original global constants."""

    def test_default_name(self):
        profile = BankProfile()
        assert profile.name == "Generic"

    def test_default_currency(self):
        profile = BankProfile()
        assert profile.currency_symbol == "£"
        assert profile.thousands_separator == ","

    def test_default_date_formats(self):
        profile = BankProfile()
        assert "%d/%m/%Y" in profile.date_formats
        assert "%Y-%m-%d" in profile.date_formats
        assert len(profile.date_formats) == 8

    def test_default_header_patterns_keys(self):
        profile = BankProfile()
        expected_keys = {
            "bank_name",
            "account_number",
            "sort_code",
            "period_start",
            "period_end",
            "opening_balance",
            "closing_balance",
        }
        assert set(profile.header_patterns.keys()) == expected_keys

    def test_default_column_keywords(self):
        profile = BankProfile()
        assert "date" in profile.column_keywords
        assert "description" in profile.column_keywords
        assert "debit" in profile.column_keywords
        assert "credit" in profile.column_keywords

    def test_default_column_map(self):
        profile = BankProfile()
        assert profile.default_column_map == {
            "date": 0,
            "description": 1,
            "debit": 2,
            "credit": 3,
            "balance": 4,
        }

    def test_parse_amount_gbp(self):
        profile = BankProfile()
        assert profile.parse_amount("£1,234.56") == Decimal("1234.56")

    def test_parse_amount_negative(self):
        profile = BankProfile()
        assert profile.parse_amount("-100.00") == Decimal("-100.00")

    def test_parse_amount_empty(self):
        profile = BankProfile()
        assert profile.parse_amount("") is None
        assert profile.parse_amount(None) is None

    def test_compile_text_pattern(self):
        profile = BankProfile()
        pattern = profile.compile_text_pattern()
        assert pattern is not None


class TestSAProfiles:
    """Test each South African bank profile."""

    @pytest.mark.parametrize(
        "factory_fn,expected_name",
        [
            (absa_profile, "ABSA"),
            (fnb_profile, "FNB"),
            (nedbank_profile, "Nedbank"),
            (standard_bank_profile, "Standard Bank"),
            (capitec_profile, "Capitec"),
        ],
    )
    def test_profile_name(self, factory_fn, expected_name):
        profile = factory_fn()
        assert profile.name == expected_name

    @pytest.mark.parametrize(
        "factory_fn",
        [absa_profile, fnb_profile, nedbank_profile, standard_bank_profile, capitec_profile],
    )
    def test_zar_currency(self, factory_fn):
        profile = factory_fn()
        assert profile.currency_symbol == "R"

    @pytest.mark.parametrize(
        "factory_fn",
        [absa_profile, fnb_profile, nedbank_profile, standard_bank_profile, capitec_profile],
    )
    def test_detection_keywords_populated(self, factory_fn):
        profile = factory_fn()
        assert len(profile.detection_keywords) >= 2

    @pytest.mark.parametrize(
        "factory_fn",
        [absa_profile, fnb_profile, nedbank_profile, standard_bank_profile, capitec_profile],
    )
    def test_has_branch_code_pattern(self, factory_fn):
        profile = factory_fn()
        assert "branch_code" in profile.header_patterns

    @pytest.mark.parametrize(
        "factory_fn",
        [absa_profile, fnb_profile, nedbank_profile, standard_bank_profile, capitec_profile],
    )
    def test_parse_rand_amount_with_spaces(self, factory_fn):
        profile = factory_fn()
        # R 1 234.56 — space thousands separator
        assert profile.parse_amount("R 1 234.56") == Decimal("1234.56")

    @pytest.mark.parametrize(
        "factory_fn",
        [absa_profile, fnb_profile, nedbank_profile, standard_bank_profile, capitec_profile],
    )
    def test_parse_rand_amount_with_commas(self, factory_fn):
        profile = factory_fn()
        # R1,234.56 — comma thousands separator (should also work)
        assert profile.parse_amount("R1,234.56") == Decimal("1234.56")

    @pytest.mark.parametrize(
        "factory_fn",
        [absa_profile, fnb_profile, nedbank_profile, standard_bank_profile, capitec_profile],
    )
    def test_parse_simple_rand_amount(self, factory_fn):
        profile = factory_fn()
        assert profile.parse_amount("R45.99") == Decimal("45.99")

    def test_fnb_parse_amount_with_kt_suffix(self):
        profile = fnb_profile()
        assert profile.parse_amount("45,916.26Kt") == Decimal("45916.26")

    def test_fnb_parse_amount_with_dt_suffix(self):
        profile = fnb_profile()
        assert profile.parse_amount("45,916.26Dt") == Decimal("-45916.26")

    @pytest.mark.parametrize(
        "factory_fn",
        [absa_profile, fnb_profile, nedbank_profile, standard_bank_profile, capitec_profile],
    )
    def test_sa_date_formats(self, factory_fn):
        profile = factory_fn()
        assert "%d/%m/%Y" in profile.date_formats
        assert "%d %B %Y" in profile.date_formats

    def test_capitec_single_amount_column(self):
        profile = capitec_profile()
        assert "amount" in profile.default_column_map
        assert "debit" not in profile.default_column_map

    def test_absa_cheque_keyword(self):
        profile = absa_profile()
        assert "cheque" in profile.column_keywords["description"]

    def test_absa_parse_decimal_comma_amount(self):
        profile = absa_profile()
        assert profile.parse_amount("4 940,60") == Decimal("4940.60")

    def test_absa_detection_afrikaans_statement(self):
        text = "Absa Bank Ltd\nTjekrekeningstaat\nTjekrekeningnommer: 7-1323-1819"
        profile = BankProfileFactory.detect(text)
        assert profile.name == "ABSA"


class TestBankProfileFactory:
    def test_get_known_bank(self):
        profile = BankProfileFactory.get("absa")
        assert profile.name == "ABSA"

    def test_get_case_insensitive(self):
        profile = BankProfileFactory.get("FNB")
        assert profile.name == "FNB"

    def test_get_unknown_bank_raises(self):
        with pytest.raises(ValueError, match="Unknown bank profile"):
            BankProfileFactory.get("nonexistent")

    def test_available_banks(self):
        banks = BankProfileFactory.available_banks()
        assert "absa" in banks
        assert "fnb" in banks
        assert "nedbank" in banks
        assert "standard_bank" in banks
        assert "capitec" in banks
        assert len(banks) == 5

    def test_detect_absa(self):
        text = "ABSA Bank\nCheque Account\nAccount Number: 1234567890"
        profile = BankProfileFactory.detect(text)
        assert profile.name == "ABSA"

    def test_detect_fnb(self):
        text = "First National Bank\nFNB Statement\nAccount: 9876543210"
        profile = BankProfileFactory.detect(text)
        assert profile.name == "FNB"

    def test_detect_fnb_from_account_type_indicators(self):
        text = (
            "Name FNBy Transact Account\n"
            "Account Number 62384104940\n"
            "Type FNB Fusion Aspire Account\n"
            "https://www.online.fnb.co.za/banking/main.jsp"
        )
        profile = BankProfileFactory.detect(text)
        assert profile.name == "FNB"

    def test_detect_nedbank(self):
        text = "Nedbank Ltd\nGreenbacks Account\nStatement Period"
        profile = BankProfileFactory.detect(text)
        assert profile.name == "Nedbank"

    def test_detect_standard_bank(self):
        text = "Standard Bank of South Africa\nSBSA\nStatement Period"
        profile = BankProfileFactory.detect(text)
        assert profile.name == "Standard Bank"

    def test_detect_capitec(self):
        text = "Capitec Bank\nGlobal One Account\nBranch: 470010"
        profile = BankProfileFactory.detect(text)
        assert profile.name == "Capitec"

    def test_detect_unknown_falls_back_to_generic(self):
        text = "Some Random Document\nWith no bank keywords"
        profile = BankProfileFactory.detect(text)
        assert profile.name == "Generic"

    def test_detect_highest_score_wins(self):
        # Text mentions both "bank" (generic) and multiple FNB keywords
        text = "FNB\nFirst National Bank\nFirstRand Group\nAccount Statement"
        profile = BankProfileFactory.detect(text)
        assert profile.name == "FNB"
