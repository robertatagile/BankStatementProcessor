from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional

from src.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class PipelineContext:
    """Mutable context object passed through all pipeline stages."""

    file_path: str
    raw_header: dict = field(default_factory=dict)
    raw_lines: list[dict] = field(default_factory=list)
    statement_id: int | None = None
    classified_lines: list[dict] = field(default_factory=list)
    unclassified_lines: list[dict] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return len(self.errors) > 0

    def summary(self) -> dict:
        return {
            "file": self.file_path,
            "statement_id": self.statement_id,
            "total_lines": len(self.classified_lines) + len(self.unclassified_lines),
            "classified": len(self.classified_lines),
            "unclassified": len(self.unclassified_lines),
            "errors": len(self.errors),
        }


class Stage(ABC):
    """Base class for all pipeline stages."""

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    def process(self, context: PipelineContext) -> PipelineContext:
        """Process the context and return it (possibly mutated)."""
        ...


class Pipeline:
    """Orchestrates a sequence of processing stages."""

    def __init__(self, stages: list[Stage]):
        self._stages = stages

    def run(self, context: PipelineContext) -> PipelineContext:
        logger.info(f"Pipeline started for: {context.file_path}")

        for stage in self._stages:
            logger.info(f"  Entering stage: {stage.name}")
            try:
                context = stage.process(context)
            except Exception as e:
                logger.error(f"  Stage {stage.name} failed: {e}")
                context.errors.append(f"{stage.name}: {e}")
                raise
            logger.info(f"  Completed stage: {stage.name}")

            if context.errors:
                logger.warning(
                    f"  Warnings after {stage.name}: {context.errors}"
                )

        logger.info(f"Pipeline completed. Summary: {context.summary()}")
        return context
