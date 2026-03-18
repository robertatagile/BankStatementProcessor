"""South African bank profile package.

Each bank has its own module for isolation. Shared SA helpers live in
``_sa_common``. Call ``register_all()`` to register every profile with
the :class:`BankProfileFactory`.
"""
from __future__ import annotations


def register_all() -> None:
    """Register all South African bank profiles with the factory."""
    from src.profiles.factory import BankProfileFactory

    from src.profiles.banks.absa import absa_profile
    from src.profiles.banks.absa_afrikaans import absa_afrikaans_profile
    from src.profiles.banks.fnb import fnb_profile
    from src.profiles.banks.nedbank import nedbank_profile
    from src.profiles.banks.standard_bank import standard_bank_profile
    from src.profiles.banks.capitec import capitec_profile
    from src.profiles.banks.african_bank import african_bank_profile
    from src.profiles.banks.tymebank import tymebank_profile

    BankProfileFactory.register("absa", absa_profile)
    BankProfileFactory.register("absa_afrikaans", absa_afrikaans_profile)
    BankProfileFactory.register("fnb", fnb_profile)
    BankProfileFactory.register("nedbank", nedbank_profile)
    BankProfileFactory.register("standard_bank", standard_bank_profile)
    BankProfileFactory.register("capitec", capitec_profile)
    BankProfileFactory.register("african_bank", african_bank_profile)
    BankProfileFactory.register("tymebank", tymebank_profile)
