from __future__ import annotations

from typing import Callable, Dict, List

from src.profiles.base import BankProfile
from src.utils.logger import get_logger

logger = get_logger(__name__)


class BankProfileFactory:
    """Registry and auto-detection for bank profiles."""

    _registry: Dict[str, Callable[[], BankProfile]] = {}

    @classmethod
    def register(cls, key: str, factory_fn: Callable[[], BankProfile]) -> None:
        """Register a profile factory under a case-insensitive key."""
        cls._registry[key.lower()] = factory_fn

    @classmethod
    def get(cls, bank_key: str) -> BankProfile:
        """Return a profile by registered key. Raises ValueError if unknown."""
        cls._ensure_registered()
        key = bank_key.lower()
        if key not in cls._registry:
            available = ", ".join(sorted(cls._registry.keys()))
            raise ValueError(
                f"Unknown bank profile: '{bank_key}'. "
                f"Available profiles: {available}"
            )
        return cls._registry[key]()

    @classmethod
    def detect(cls, page1_text: str) -> BankProfile:
        """Auto-detect bank from page 1 text. Returns generic profile if no match."""
        cls._ensure_registered()
        text_lower = page1_text.lower()

        best_profile = None
        best_score = 0

        for key, factory_fn in cls._registry.items():
            profile = factory_fn()
            score = sum(
                1 for kw in profile.detection_keywords if kw.lower() in text_lower
            )
            if score > best_score:
                best_score = score
                best_profile = profile

        if best_profile and best_score >= 1:
            logger.info(
                f"Auto-detected bank: {best_profile.name} (score: {best_score})"
            )
            return best_profile

        logger.info("No bank detected — using generic profile")
        return BankProfile()

    @classmethod
    def available_banks(cls) -> List[str]:
        """Return sorted list of registered bank keys."""
        cls._ensure_registered()
        return sorted(cls._registry.keys())

    @classmethod
    def _ensure_registered(cls) -> None:
        """Ensure SA profiles are registered (lazy import)."""
        if not cls._registry:
            from src.profiles.south_africa import register_all
            register_all()
