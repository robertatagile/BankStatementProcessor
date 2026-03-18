"""Backward-compatibility shim for South African bank profiles.

All profile implementations have moved to ``src.profiles.banks.*``.
This module re-exports them so that existing imports continue to work::

    from src.profiles.south_africa import fnb_profile  # still works
"""
from __future__ import annotations

# Re-export shared helpers (prefixed with underscore for backward compat)
from src.profiles.banks._sa_common import (  # noqa: F401
    sa_header_patterns as _sa_header_patterns,
    sa_date_formats as _sa_date_formats,
    sa_column_keywords as _sa_column_keywords,
    sa_text_line_pattern as _sa_text_line_pattern,
    sa_base_profile as _sa_base_profile,
)

# Re-export all profile factory functions
from src.profiles.banks.absa import absa_profile  # noqa: F401
from src.profiles.banks.absa_afrikaans import absa_afrikaans_profile  # noqa: F401
from src.profiles.banks.fnb import fnb_profile  # noqa: F401
from src.profiles.banks.nedbank import nedbank_profile  # noqa: F401
from src.profiles.banks.standard_bank import standard_bank_profile  # noqa: F401
from src.profiles.banks.capitec import capitec_profile  # noqa: F401
from src.profiles.banks.african_bank import african_bank_profile  # noqa: F401


def register_all() -> None:
    """Register all South African bank profiles with the factory.

    Delegates to ``src.profiles.banks.register_all()``.
    """
    from src.profiles.banks import register_all as _register_all
    _register_all()
