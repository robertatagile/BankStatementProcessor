from __future__ import annotations

import json
import re

from sqlalchemy.orm import sessionmaker

from src.models.database import StatementLine
from src.pipeline.queue import PipelineContext, Stage
from src.utils.logger import get_logger

logger = get_logger(__name__)


class RegexClassifierStage(Stage):
    """Stage 3: Classify statement lines using an ordered list of regex rules."""

    def __init__(self, rules_path: str, session_factory: sessionmaker):
        self._rules_path = rules_path
        self._session_factory = session_factory
        self._compiled_rules: list[dict] = []

    def process(self, context: PipelineContext) -> PipelineContext:
        self._load_rules()

        if not self._compiled_rules:
            logger.warning("No classification rules loaded — skipping regex stage")
            return context

        classified = []
        still_unclassified = []

        for line in context.unclassified_lines:
            category = self._classify(line["description"])
            if category:
                classified.append({**line, "category": category})
            else:
                still_unclassified.append(line)

        # Batch-update classified lines in the database
        if classified:
            self._update_db(classified)

        context.classified_lines.extend(classified)
        context.unclassified_lines = still_unclassified

        logger.info(
            f"Regex classified: {len(classified)} | "
            f"Still unclassified: {len(still_unclassified)}"
        )
        return context

    def _load_rules(self) -> None:
        """Load and compile regex rules from the JSON config file."""
        try:
            with open(self._rules_path, "r") as f:
                data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Failed to load rules from {self._rules_path}: {e}")
            self._compiled_rules = []
            return

        rules = data.get("rules", [])
        # Sort by priority (lower number = higher priority)
        rules.sort(key=lambda r: r.get("priority", 999))

        self._compiled_rules = []
        for rule in rules:
            try:
                compiled = re.compile(rule["pattern"])
                self._compiled_rules.append({
                    "compiled": compiled,
                    "pattern": rule["pattern"],
                    "category": rule["category"],
                    "priority": rule.get("priority", 999),
                    "source": rule.get("source", "manual"),
                })
            except re.error as e:
                logger.warning(
                    f"Invalid regex pattern '{rule['pattern']}': {e} — skipping"
                )

        logger.debug(f"Loaded {len(self._compiled_rules)} classification rules")

    def _classify(self, description: str) -> str | None:
        """Match a description against rules. First match wins."""
        for rule in self._compiled_rules:
            if rule["compiled"].search(description):
                return rule["category"]
        return None

    def _update_db(self, classified_lines: list[dict]) -> None:
        """Update the database with classification results."""
        with self._session_factory() as session:
            for line in classified_lines:
                stmt_line = session.get(StatementLine, line["id"])
                if stmt_line:
                    stmt_line.category = line["category"]
                    stmt_line.classification_method = "regex"
            session.commit()
