"""Single source of truth for transaction categories emitted by the Bank Statement Processor.

Every category that the BSP regex rules, AI classifier, or bank-native extraction can
produce must appear in *KNOWN_CATEGORIES*.  The *CATEGORY_ALIASES* dict maps legacy or
variant labels to the preferred canonical form so downstream consumers receive a
predictable vocabulary.

DEFAULT_CATEGORIES is the list passed to the AI classifier.  It intentionally excludes
niche labels that only originate from regex rules or bank-native extraction (e.g. Fees)
because the AI should not be encouraged to invent them — those arrive through deterministic
paths instead.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Canonical outbound categories
# ---------------------------------------------------------------------------
DEFAULT_CATEGORIES: list[str] = [
    "Groceries",
    "Utilities",
    "Rent/Mortgage",
    "Salary",
    "Transfer",
    "Subscriptions",
    "Transport",
    "Dining",
    "Entertainment",
    "Healthcare",
    "Insurance",
    "Cash Withdrawal",
    "Shopping",
    "Clothing/Apparel",
    "Electronics/Home",
    "Education",
    "Charity",
    "Fees",
    "Other",
]

# All categories the BSP can emit (DEFAULT_CATEGORIES + bank-native labels).
KNOWN_CATEGORIES: frozenset[str] = frozenset(DEFAULT_CATEGORIES) | frozenset([
    # Capitec bank-native categories
    "Other Income",
    "Digital Payments",
    "Loan Payments",
    "Uncategorised",
    "Cellphone",
    "Interest",
    "Internet",
])

# Legacy / variant labels that should be collapsed into the canonical form.
# Keys are case-folded for lookup; values are the canonical label.
CATEGORY_ALIASES: dict[str, str] = {
    # No aliases needed yet — all current labels are canonical.
}
