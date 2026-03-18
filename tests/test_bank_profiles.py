"""Tests for bank profiles and the profile factory."""

from decimal import Decimal

import pytest

from src.profiles.base import BankProfile
from src.profiles.factory import BankProfileFactory
from src.profiles.south_africa import (
    absa_afrikaans_profile,
    absa_profile,
    african_bank_profile,
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
        assert "african_bank" in banks
        assert "absa_afrikaans" in banks
        assert "tymebank" in banks
        assert len(banks) == 8

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

    def test_detect_african_bank(self):
        text = "African Bank\nBranch Code\n430000\nStatement for: L MOUTON"
        profile = BankProfileFactory.detect(text)
        assert profile.name == "African Bank"

    def test_detect_highest_score_wins(self):
        # Text mentions both "bank" (generic) and multiple FNB keywords
        text = "FNB\nFirst National Bank\nFirstRand Group\nAccount Statement"
        profile = BankProfileFactory.detect(text)
        assert profile.name == "FNB"


class TestAfricanBankProfile:
    """Tests for the African Bank profile."""

    def test_profile_name(self):
        profile = african_bank_profile()
        assert profile.name == "African Bank"

    def test_zar_currency(self):
        profile = african_bank_profile()
        assert profile.currency_symbol == "R"

    def test_date_format_yyyy_mm_dd_first(self):
        """African Bank uses YYYY/MM/DD — must be first in the list."""
        profile = african_bank_profile()
        assert profile.date_formats[0] == "%Y/%m/%d"

    def test_unsigned_is_not_debit(self):
        """African Bank uses negative amounts for debits, not unsigned."""
        profile = african_bank_profile()
        assert profile.unsigned_is_debit is False

    def test_default_column_map_skips_bank_charges(self):
        """Column map should skip col 2 (bank charges)."""
        profile = african_bank_profile()
        assert profile.default_column_map == {
            "date": 0, "description": 1, "amount": 3, "balance": 4
        }

    def test_detection_keywords(self):
        profile = african_bank_profile()
        assert "african bank" in profile.detection_keywords

    def test_parse_positive_amount(self):
        """Positive amount = credit for African Bank."""
        profile = african_bank_profile()
        assert profile.parse_amount("700.00") == Decimal("700.00")

    def test_parse_negative_amount(self):
        """Negative amount = debit for African Bank."""
        profile = african_bank_profile()
        assert profile.parse_amount("-68.00") == Decimal("-68.00")

    def test_parse_amount_with_spaces(self):
        """African Bank uses space as thousands separator."""
        profile = african_bank_profile()
        assert profile.parse_amount("13 161.00") == Decimal("13161.00")

    def test_period_start_extraction(self):
        """Period start should extract from 'YYYY/MM/DD to YYYY/MM/DD' format."""
        profile = african_bank_profile()
        text = "2025/10/21 to 2026/01/04"
        m = profile.header_patterns["period_start"].search(text)
        assert m is not None
        assert m.group(1) == "2025/10/21"

    def test_period_end_extraction(self):
        """Period end should extract from 'YYYY/MM/DD to YYYY/MM/DD' format."""
        profile = african_bank_profile()
        text = "2025/10/21 to 2026/01/04"
        m = profile.header_patterns["period_end"].search(text)
        assert m is not None
        assert m.group(1) == "2026/01/04"

    def test_account_holder_extraction(self):
        profile = african_bank_profile()
        text = "Account Holder LUCHAN\nAccount Number 20114025968"
        m = profile.header_patterns["account_holder"].search(text)
        assert m is not None
        assert m.group(1).strip() == "LUCHAN"

    def test_account_number_extraction(self):
        profile = african_bank_profile()
        text = "Account Number 20114025968"
        m = profile.header_patterns["account_number"].search(text)
        assert m is not None
        assert m.group(1) == "20114025968"

    def test_branch_code_extraction(self):
        profile = african_bank_profile()
        text = "Branch Code\n430000"
        m = profile.header_patterns["branch_code"].search(text)
        assert m is not None
        assert m.group(1) == "430000"

    def test_bank_charges_column_keyword(self):
        """Column keywords should include bank_charges."""
        profile = african_bank_profile()
        assert "bank_charges" in profile.column_keywords


class TestAbsaAfrikaansProfile:
    """Tests for the ABSA Afrikaans profile."""

    def test_profile_name(self):
        profile = absa_afrikaans_profile()
        assert profile.name == "ABSA Afrikaans"

    def test_detection_keywords(self):
        profile = absa_afrikaans_profile()
        assert "tjekrekeningnommer" in profile.detection_keywords
        assert "transaksiebeskrywing" in profile.detection_keywords

    def test_detect_absa_afrikaans(self):
        text = "Absa Bank Ltd\nTjekrekeningnommer: 7-1323-1819\nSaldo oorgedra\nTransaksiebeskrywing"
        profile = BankProfileFactory.detect(text)
        assert profile.name == "ABSA Afrikaans"

    def test_account_number_extraction(self):
        profile = absa_afrikaans_profile()
        text = "Tjekrekeningnommer: 7-1323-1819"
        m = profile.header_patterns["account_number"].search(text)
        assert m is not None
        assert m.group(1) == "7-1323-1819"

    def test_period_extraction_afrikaans(self):
        """Period uses 'tot' instead of 'to'."""
        profile = absa_afrikaans_profile()
        text = "17 Okt 2025 tot 16 Nov 2025"
        m = profile.header_patterns["period_start"].search(text)
        assert m is not None
        assert m.group(1) == "17 Okt 2025"
        m = profile.header_patterns["period_end"].search(text)
        assert m is not None
        assert m.group(1) == "16 Nov 2025"

    def test_opening_balance_comma_decimal(self):
        """Header uses comma as decimal separator: 16 270,50."""
        profile = absa_afrikaans_profile()
        m = profile.header_patterns["opening_balance"].search("Saldo oorgedra 16 270,50")
        assert m is not None
        amount = profile.parse_amount(m.group(1))
        assert amount == Decimal("16270.50")

    def test_account_holder_extraction(self):
        profile = absa_afrikaans_profile()
        text = "Lyttelton\nMEV L SENEKAL\nW B03 Lyttelton"
        m = profile.header_patterns["account_holder"].search(text)
        assert m is not None
        assert m.group(1).strip() == "MEV L SENEKAL"

    def test_account_type_extraction(self):
        profile = absa_afrikaans_profile()
        text = "Rekeningtipe: Flexi Rekening Uitgereik op: 16Nov2025"
        m = profile.header_patterns["account_type"].search(text)
        assert m is not None
        assert m.group(1).strip() == "Flexi Rekening"

    def test_parse_amount_dot_decimal(self):
        """Transaction lines use dot decimal: 300.00."""
        profile = absa_afrikaans_profile()
        assert profile.parse_amount("300.00") == Decimal("300.00")

    def test_parse_amount_space_thousands(self):
        profile = absa_afrikaans_profile()
        assert profile.parse_amount("16 263.00") == Decimal("16263.00")

    def test_parse_amount_trailing_minus(self):
        """Amounts with trailing minus: 280.00-."""
        profile = absa_afrikaans_profile()
        assert profile.parse_amount("280.00-") == Decimal("-280.00")

    def test_afrikaans_column_keywords(self):
        profile = absa_afrikaans_profile()
        assert "transaksiebeskrywing" in profile.column_keywords["description"]
        assert "debietbedrag" in profile.column_keywords["debit"]
        assert "kredietbedrag" in profile.column_keywords["credit"]
        assert "saldo" in profile.column_keywords["balance"]
