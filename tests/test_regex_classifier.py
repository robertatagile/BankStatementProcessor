"""Tests for the Regex Classifier stage."""

import json
from datetime import date
from decimal import Decimal

import pytest

from src.models.database import StatementLine, init_db
from src.pipeline.data_cleanser import DataCleanserStage
from src.pipeline.queue import PipelineContext
from src.pipeline.regex_classifier import RegexClassifierStage


@pytest.fixture
def session_factory(tmp_path):
    db_path = str(tmp_path / "test.db")
    return init_db(db_path)


@pytest.fixture
def rules_file(tmp_path):
    rules_path = tmp_path / "rules.json"
    rules_path.write_text(
        json.dumps(
            {
                "rules": [
                    {
                        "pattern": "(?i)tesco|sainsbury",
                        "category": "Groceries",
                        "priority": 1,
                        "source": "manual",
                    },
                    {
                        "pattern": "(?i)netflix|spotify",
                        "category": "Subscriptions",
                        "priority": 2,
                        "source": "manual",
                    },
                    {
                        "pattern": "(?i)salary|wages",
                        "category": "Salary",
                        "priority": 3,
                        "source": "manual",
                    },
                ]
            }
        )
    )
    return str(rules_path)


@pytest.fixture
def context_with_db_lines(session_factory):
    """Create a context with lines already inserted into the DB."""
    ctx = PipelineContext(file_path="test.pdf")
    ctx.raw_header = {
        "bank_name": "Test Bank",
        "account_number": "12345678",
        "period_start": date(2024, 1, 1),
        "period_end": date(2024, 1, 31),
        "opening_balance": Decimal("1000.00"),
        "closing_balance": Decimal("1000.00"),
    }
    ctx.raw_lines = [
        {
            "date": date(2024, 1, 5),
            "description": "TESCO STORES 1234",
            "amount": Decimal("45.00"),
            "balance": None,
            "transaction_type": "debit",
        },
        {
            "date": date(2024, 1, 10),
            "description": "NETFLIX MONTHLY",
            "amount": Decimal("15.99"),
            "balance": None,
            "transaction_type": "debit",
        },
        {
            "date": date(2024, 1, 15),
            "description": "SALARY PAYMENT",
            "amount": Decimal("2000.00"),
            "balance": None,
            "transaction_type": "credit",
        },
        {
            "date": date(2024, 1, 20),
            "description": "RANDOM UNKNOWN SHOP",
            "amount": Decimal("25.00"),
            "balance": None,
            "transaction_type": "debit",
        },
    ]

    # Run through data cleanser to insert into DB
    cleanser = DataCleanserStage(session_factory)
    return cleanser.process(ctx)


class TestRegexClassification:
    def test_classifies_matching_lines(
        self, session_factory, rules_file, context_with_db_lines
    ):
        stage = RegexClassifierStage(rules_file, session_factory)
        result = stage.process(context_with_db_lines)

        # 3 should match (Tesco, Netflix, Salary), 1 unclassified
        assert len(result.classified_lines) == 3
        assert len(result.unclassified_lines) == 1

        categories = {line["category"] for line in result.classified_lines}
        assert "Groceries" in categories
        assert "Subscriptions" in categories
        assert "Salary" in categories

    def test_unclassified_remain(
        self, session_factory, rules_file, context_with_db_lines
    ):
        stage = RegexClassifierStage(rules_file, session_factory)
        result = stage.process(context_with_db_lines)

        assert len(result.unclassified_lines) == 1
        assert result.unclassified_lines[0]["description"] == "RANDOM UNKNOWN SHOP"

    def test_updates_database(
        self, session_factory, rules_file, context_with_db_lines
    ):
        stage = RegexClassifierStage(rules_file, session_factory)
        stage.process(context_with_db_lines)

        with session_factory() as session:
            classified = (
                session.query(StatementLine)
                .filter(StatementLine.category.isnot(None))
                .all()
            )
            assert len(classified) == 3
            assert all(
                line.classification_method == "regex" for line in classified
            )

    def test_priority_order(self, session_factory, tmp_path):
        """Test that lower priority number wins when multiple rules match."""
        rules_path = tmp_path / "priority_rules.json"
        rules_path.write_text(
            json.dumps(
                {
                    "rules": [
                        {
                            "pattern": "(?i)store",
                            "category": "Shopping",
                            "priority": 10,
                            "source": "manual",
                        },
                        {
                            "pattern": "(?i)tesco",
                            "category": "Groceries",
                            "priority": 1,
                            "source": "manual",
                        },
                    ]
                }
            )
        )

        ctx = PipelineContext(file_path="test.pdf")
        ctx.raw_header = {
            "bank_name": "Test Bank",
            "account_number": "12345678",
            "period_end": date(2024, 1, 31),
            "opening_balance": Decimal("100.00"),
            "closing_balance": Decimal("50.00"),
        }
        ctx.raw_lines = [
            {
                "date": date(2024, 1, 5),
                "description": "TESCO STORE",
                "amount": Decimal("50.00"),
                "balance": None,
                "transaction_type": "debit",
            }
        ]

        cleanser = DataCleanserStage(session_factory)
        ctx = cleanser.process(ctx)

        stage = RegexClassifierStage(str(rules_path), session_factory)
        result = stage.process(ctx)

        # "TESCO STORE" matches both rules, but Groceries (priority 1) wins
        assert result.classified_lines[0]["category"] == "Groceries"

    def test_missing_rules_file(self, session_factory, context_with_db_lines):
        stage = RegexClassifierStage("/nonexistent/rules.json", session_factory)
        result = stage.process(context_with_db_lines)
        # All lines remain unclassified
        assert len(result.unclassified_lines) == 4
        assert len(result.classified_lines) == 0
