"""Tests for the AI Classifier stage."""

import json
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from src.models.database import ClassificationRule, StatementLine, init_db
from src.pipeline.ai_classifier import AIClassifierStage, ClassificationResult
from src.pipeline.data_cleanser import DataCleanserStage
from src.pipeline.queue import PipelineContext


@pytest.fixture
def session_factory(tmp_path):
    db_path = str(tmp_path / "test.db")
    return init_db(db_path)


@pytest.fixture
def rules_file(tmp_path):
    rules_path = tmp_path / "rules.json"
    rules_path.write_text(json.dumps({"rules": []}))
    return str(rules_path)


@pytest.fixture
def context_with_unclassified(session_factory):
    """Create a context with unclassified lines in the DB."""
    ctx = PipelineContext(file_path="test.pdf")
    ctx.raw_header = {
        "bank_name": "Test Bank",
        "account_number": "12345678",
        "period_end": date(2024, 1, 31),
        "opening_balance": Decimal("1000.00"),
        "closing_balance": Decimal("950.00"),
    }
    ctx.raw_lines = [
        {
            "date": date(2024, 1, 5),
            "description": "WEIRD MERCHANT XYZ",
            "amount": Decimal("25.00"),
            "balance": None,
            "transaction_type": "debit",
        },
        {
            "date": date(2024, 1, 10),
            "description": "OBSCURE SERVICE ABC",
            "amount": Decimal("25.00"),
            "balance": None,
            "transaction_type": "debit",
        },
    ]

    cleanser = DataCleanserStage(session_factory)
    return cleanser.process(ctx)


def mock_api_response(results: list[dict]) -> MagicMock:
    """Create a mock Anthropic API response."""
    response = MagicMock()
    content_block = MagicMock()
    content_block.text = json.dumps(results)
    response.content = [content_block]
    return response


class TestAIClassifier:
    @patch("src.pipeline.ai_classifier.Anthropic")
    def test_classifies_lines(
        self, mock_anthropic_cls, session_factory, rules_file, context_with_unclassified
    ):
        mock_client = MagicMock()
        mock_anthropic_cls.return_value = mock_client
        mock_client.messages.create.return_value = mock_api_response(
            [
                {
                    "category": "Shopping",
                    "regex_pattern": "(?i)weird\\s*merchant",
                    "confidence": 0.9,
                },
                {
                    "category": "Subscriptions",
                    "regex_pattern": "(?i)obscure\\s*service",
                    "confidence": 0.85,
                },
            ]
        )

        stage = AIClassifierStage(
            api_key="test-key",
            rules_path=rules_file,
            session_factory=session_factory,
            categories=["Shopping", "Subscriptions", "Other"],
        )

        result = stage.process(context_with_unclassified)

        assert len(result.classified_lines) == 2
        assert len(result.unclassified_lines) == 0
        assert result.classified_lines[0]["category"] == "Shopping"
        assert result.classified_lines[1]["category"] == "Subscriptions"

    @patch("src.pipeline.ai_classifier.Anthropic")
    def test_updates_database(
        self, mock_anthropic_cls, session_factory, rules_file, context_with_unclassified
    ):
        mock_client = MagicMock()
        mock_anthropic_cls.return_value = mock_client
        mock_client.messages.create.return_value = mock_api_response(
            [
                {
                    "category": "Shopping",
                    "regex_pattern": "(?i)weird",
                    "confidence": 0.9,
                },
                {
                    "category": "Other",
                    "regex_pattern": "(?i)obscure",
                    "confidence": 0.5,
                },
            ]
        )

        stage = AIClassifierStage(
            api_key="test-key",
            rules_path=rules_file,
            session_factory=session_factory,
            categories=["Shopping", "Other"],
        )
        stage.process(context_with_unclassified)

        with session_factory() as session:
            lines = session.query(StatementLine).all()
            assert all(line.category is not None for line in lines)
            assert all(
                line.classification_method == "ai" for line in lines
            )

    @patch("src.pipeline.ai_classifier.Anthropic")
    def test_appends_high_confidence_rules(
        self, mock_anthropic_cls, session_factory, rules_file, context_with_unclassified
    ):
        mock_client = MagicMock()
        mock_anthropic_cls.return_value = mock_client
        mock_client.messages.create.return_value = mock_api_response(
            [
                {
                    "category": "Shopping",
                    "regex_pattern": "(?i)weird\\s*merchant",
                    "confidence": 0.95,  # above threshold
                },
                {
                    "category": "Other",
                    "regex_pattern": "(?i)obscure",
                    "confidence": 0.5,  # below threshold
                },
            ]
        )

        stage = AIClassifierStage(
            api_key="test-key",
            rules_path=rules_file,
            session_factory=session_factory,
            categories=["Shopping", "Other"],
        )
        stage.process(context_with_unclassified)

        # Check JSON file — only high-confidence rule should be appended
        with open(rules_file) as f:
            data = json.load(f)
        assert len(data["rules"]) == 1
        assert data["rules"][0]["category"] == "Shopping"
        assert data["rules"][0]["source"] == "ai"

        # Check DB
        with session_factory() as session:
            db_rules = session.query(ClassificationRule).all()
            assert len(db_rules) == 1

    @patch("src.pipeline.ai_classifier.Anthropic")
    def test_handles_api_failure(
        self, mock_anthropic_cls, session_factory, rules_file, context_with_unclassified
    ):
        mock_client = MagicMock()
        mock_anthropic_cls.return_value = mock_client
        mock_client.messages.create.side_effect = Exception("API error")

        stage = AIClassifierStage(
            api_key="test-key",
            rules_path=rules_file,
            session_factory=session_factory,
            categories=["Other"],
        )
        result = stage.process(context_with_unclassified)

        # Should gracefully fall back to "Other" category
        assert len(result.classified_lines) == 2
        assert all(
            line["category"] == "Other" for line in result.classified_lines
        )

    def test_skips_when_no_unclassified(self, session_factory, rules_file):
        ctx = PipelineContext(file_path="test.pdf")
        ctx.unclassified_lines = []

        stage = AIClassifierStage(
            api_key="test-key",
            rules_path=rules_file,
            session_factory=session_factory,
            categories=["Other"],
        )
        result = stage.process(ctx)
        assert len(result.classified_lines) == 0


class TestClassificationResult:
    def test_valid_result(self):
        result = ClassificationResult(
            category="Groceries",
            regex_pattern="(?i)tesco",
            confidence=0.95,
        )
        assert result.category == "Groceries"
        assert result.confidence == 0.95
