from __future__ import annotations

import json
from decimal import Decimal

from anthropic import Anthropic
from pydantic import BaseModel
from sqlalchemy.orm import sessionmaker

from src.models.database import ClassificationRule, RefinementProposal, StatementLine
from src.pipeline.queue import PipelineContext, Stage
from src.utils.logger import get_logger

logger = get_logger(__name__)

BATCH_SIZE = 20
CONFIDENCE_THRESHOLD = 0.8

SYSTEM_PROMPT = """You are a bank transaction classifier. You will be given a list of bank transaction descriptions.

For each transaction, you must:
1. Classify it into exactly ONE of the provided categories.
2. Generate a case-insensitive regex pattern that would match this and similar transactions in the future.
   - The regex should be specific enough to avoid false positives.
   - The regex should be general enough to catch variations of the same merchant/type.
   - Use (?i) flag for case-insensitivity.
   - Focus on the merchant name or key identifying words.
3. Provide a confidence score between 0.0 and 1.0.

Return a JSON array of objects, one per transaction, in the same order as the input.
Each object must have: category (string), regex_pattern (string), confidence (float).

Example output:
[
  {"category": "Groceries", "regex_pattern": "(?i)whole\\\\s*foods", "confidence": 0.95},
  {"category": "Transport", "regex_pattern": "(?i)uber(?!\\\\s*eats)", "confidence": 0.90}
]"""


class ClassificationResult(BaseModel):
    category: str
    regex_pattern: str
    confidence: float


class AIClassifierStage(Stage):
    """Stage 4: Classify unmatched lines using Claude API and generate new regex rules."""

    def __init__(
        self,
        api_key: str,
        rules_path: str,
        session_factory: sessionmaker,
        categories: list[str],
        job_id: str | None = None,
    ):
        self._client = Anthropic(api_key=api_key)
        self._rules_path = rules_path
        self._session_factory = session_factory
        self._categories = categories
        self._job_id = job_id

    def process(self, context: PipelineContext) -> PipelineContext:
        if not context.unclassified_lines:
            logger.info("No unclassified lines — skipping AI stage")
            return context

        logger.info(
            f"Sending {len(context.unclassified_lines)} lines to AI for classification"
        )

        # Process in batches
        all_results = []
        for i in range(0, len(context.unclassified_lines), BATCH_SIZE):
            batch = context.unclassified_lines[i : i + BATCH_SIZE]
            results = self._classify_batch(batch)
            all_results.extend(results)

        # Update database and rules
        newly_classified = []
        with self._session_factory() as session:
            for line, result in zip(context.unclassified_lines, all_results):
                # Update the DB record with provenance
                stmt_line = session.get(StatementLine, line["id"])
                if stmt_line:
                    stmt_line.category = result.category
                    stmt_line.classification_method = "ai"
                    stmt_line.confidence = result.confidence
                    stmt_line.matched_pattern = result.regex_pattern or None
                    stmt_line.classification_reason = (
                        f"AI classified as '{result.category}' "
                        f"with {result.confidence:.0%} confidence"
                    )

                newly_classified.append({**line, "category": result.category})

                # Auto-append high-confidence rules to JSON and DB
                if result.confidence >= CONFIDENCE_THRESHOLD and result.regex_pattern:
                    self._append_rule(session, result.regex_pattern, result.category)

            session.commit()

        context.classified_lines.extend(newly_classified)
        context.unclassified_lines = []

        logger.info(f"AI classified {len(newly_classified)} lines")
        return context

    def _classify_batch(self, batch: list[dict]) -> list[ClassificationResult]:
        """Send a batch of descriptions to Claude for classification."""
        descriptions = [
            f"{i + 1}. {line['description']}"
            for i, line in enumerate(batch)
        ]
        descriptions_text = "\n".join(descriptions)

        user_prompt = (
            f"Categories: {', '.join(self._categories)}\n\n"
            f"Transactions to classify:\n{descriptions_text}\n\n"
            f"Return a JSON array with exactly {len(batch)} classification objects."
        )

        try:
            response = self._client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=4096,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_prompt}],
            )

            response_text = response.content[0].text
            # Extract JSON from the response (handle markdown code blocks)
            json_text = self._extract_json(response_text)
            raw_results = json.loads(json_text)

            results = [ClassificationResult(**item) for item in raw_results]

            # Ensure we got the right number of results
            if len(results) != len(batch):
                logger.warning(
                    f"Expected {len(batch)} results, got {len(results)}. "
                    f"Padding with defaults."
                )
                while len(results) < len(batch):
                    results.append(
                        ClassificationResult(
                            category="Other",
                            regex_pattern="",
                            confidence=0.0,
                        )
                    )

            return results[: len(batch)]

        except Exception as e:
            logger.error(f"AI classification failed: {e}")
            # Return default classifications on failure
            return [
                ClassificationResult(
                    category="Other", regex_pattern="", confidence=0.0
                )
                for _ in batch
            ]

    def _extract_json(self, text: str) -> str:
        """Extract JSON array from response text, handling markdown code blocks."""
        text = text.strip()
        # Check for markdown code block
        if "```" in text:
            start = text.find("```")
            # Skip the language identifier line
            start = text.find("\n", start) + 1
            end = text.find("```", start)
            if end > start:
                return text[start:end].strip()
        # Try to find a JSON array directly
        start = text.find("[")
        end = text.rfind("]") + 1
        if start >= 0 and end > start:
            return text[start:end]
        return text

    def _append_rule(
        self, session, regex_pattern: str, category: str,
    ) -> None:
        """Append a new AI-generated rule to the JSON config and database."""
        if not regex_pattern:
            return

        # Load existing rules
        try:
            with open(self._rules_path, "r") as f:
                data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            data = {"rules": []}

        rules = data.get("rules", [])

        # Check if a similar pattern already exists
        for existing in rules:
            if existing["pattern"] == regex_pattern:
                logger.debug(f"Rule already exists: {regex_pattern}")
                return

        # Compute next priority
        max_priority = max((r.get("priority", 0) for r in rules), default=0)
        new_priority = max_priority + 1

        # Append to JSON file
        new_rule = {
            "pattern": regex_pattern,
            "category": category,
            "priority": new_priority,
            "source": "ai",
        }
        rules.append(new_rule)
        data["rules"] = rules

        with open(self._rules_path, "w") as f:
            json.dump(data, f, indent=2)

        logger.info(f"New AI rule added: '{regex_pattern}' → {category}")

        # Also insert into the database
        db_rule = ClassificationRule(
            pattern=regex_pattern,
            category=category,
            priority=new_priority,
            source="ai",
        )
        session.add(db_rule)
