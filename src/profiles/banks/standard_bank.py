"""Standard Bank of South Africa profile."""
from __future__ import annotations

import re

from src.profiles.base import BankProfile
from src.profiles.banks._sa_common import sa_base_profile, sa_header_patterns


def standard_bank_profile() -> BankProfile:
    """Standard Bank of South Africa profile."""
    patterns = sa_header_patterns()
    # Standard Bank uses "Statement Period" label
    patterns["period_start"] = re.compile(
        r"(?:Statement\s+Period)\s*[:\-]?\s*"
        r"(\d{1,2}[\s\/\-](?:\w+|\d{1,2})[\s\/\-]\d{2,4})",
        re.IGNORECASE,
    )

    return sa_base_profile(
        name="Standard Bank",
        detection_keywords=["standard bank", "sbsa", "standard bank of south africa"],
        header_patterns=patterns,
    )
