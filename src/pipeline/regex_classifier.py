from __future__ import annotations

import json
import re

from sqlalchemy.orm import sessionmaker

from src.models.database import ClassificationRule, StatementLine
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
            match_info = self._classify(line["description"])
            if match_info:
                classified.append({
                    **line,
                    "category": match_info["category"],
                    "matched_rule_id": match_info.get("rule_id"),
                    "matched_pattern": match_info.get("pattern"),
                    "classification_reason": f"Matched regex: {match_info.get('pattern', 'unknown')}",
                })
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
        """Load and compile regex rules from the JSON config file and DB."""
        # Load from JSON first
        try:
            with open(self._rules_path, "r") as f:
                data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Failed to load rules from {self._rules_path}: {e}")
            data = {"rules": []}

        # Also load DB rules to get IDs
        db_rules_by_pattern = {}
        try:
            with self._session_factory() as session:
                from src.models.database import ClassificationRule
                for rule in session.query(ClassificationRule).filter_by(enabled=True).all():
                    db_rules_by_pattern[rule.pattern] = rule.id
        except Exception as e:
            logger.warning(f"Could not load DB rules for ID mapping: {e}")

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
                    "rule_id": db_rules_by_pattern.get(rule["pattern"]),
                })
            except re.error as e:
                logger.warning(
                    f"Invalid regex pattern '{rule['pattern']}': {e} — skipping"
                )

        logger.debug(f"Loaded {len(self._compiled_rules)} classification rules")

    def _classify(self, description: str) -> dict | None:
        """Match a description against rules. First match wins. Returns match info."""
        for rule in self._compiled_rules:
            if rule["compiled"].search(description):
                return {
                    "category": rule["category"],
                    "rule_id": rule.get("rule_id"),
                    "pattern": rule["pattern"],
                }
        return None

    def _update_db(self, classified_lines: list[dict]) -> None:
        """Update the database with classification results and provenance."""
        with self._session_factory() as session:
            for line in classified_lines:
                stmt_line = session.get(StatementLine, line["id"])
                if stmt_line:
                    stmt_line.category = line["category"]
                    stmt_line.classification_method = "regex"
                    stmt_line.matched_rule_id = line.get("matched_rule_id")
                    stmt_line.matched_pattern = line.get("matched_pattern")
                    stmt_line.classification_reason = line.get("classification_reason")
            session.commit()

            # Update match counts
            rule_match_counts: dict[int, int] = {}
            for line in classified_lines:
                rid = line.get("matched_rule_id")
                if rid:
                    rule_match_counts[rid] = rule_match_counts.get(rid, 0) + 1
            if rule_match_counts:
                from src.models.database import ClassificationRule
                for rid, count in rule_match_counts.items():
                    rule = session.get(ClassificationRule, rid)
                    if rule:
                        rule.match_count = (rule.match_count or 0) + count
                session.commit()


def seed_classification_rules(session_factory: sessionmaker, rules_path: str) -> None:
    """Seed the classification_rules table from the JSON config file.

    Only inserts if the table is empty (avoids duplicating on re-runs).
    """
    with session_factory() as session:
        count = session.query(ClassificationRule).count()
        if count > 0:
            logger.debug(f"Classification rules table already has {count} rows — skipping seed")
            return

        try:
            with open(rules_path, "r") as f:
                data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Cannot seed rules from {rules_path}: {e}")
            return

        rules = data.get("rules", [])
        for rule in rules:
            session.add(ClassificationRule(
                pattern=rule["pattern"],
                category=rule["category"],
                priority=rule.get("priority", 999),
                source=rule.get("source", "manual"),
            ))
        session.commit()
        logger.info(f"Seeded {len(rules)} classification rules into database")
