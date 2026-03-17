"""Tests for the Data Cleanser stage."""

from datetime import date
from decimal import Decimal

import pytest

from src.models.database import Base, Statement, StatementLine, init_db
from src.pipeline.data_cleanser import DataCleanserStage
from src.pipeline.queue import PipelineContext


@pytest.fixture
def session_factory(tmp_path):
    """Create an in-memory SQLite database for testing."""
    db_path = str(tmp_path / "test.db")
    return init_db(db_path)


@pytest.fixture
def sample_context():
    """A context with sample extracted data."""
    ctx = PipelineContext(file_path="test.pdf")
    ctx.raw_header = {
        "bank_name": "Test Bank",
        "account_number": "12345678",
        "period_start": date(2024, 1, 1),
        "period_end": date(2024, 1, 31),
        "opening_balance": Decimal("1000.00"),
        "closing_balance": Decimal("1150.00"),
    }
    ctx.raw_lines = [
        {
            "date": date(2024, 1, 5),
            "description": "SALARY PAYMENT",
            "amount": Decimal("500.00"),
            "balance": Decimal("1500.00"),
            "transaction_type": "credit",
        },
        {
            "date": date(2024, 1, 10),
            "description": "TESCO STORES",
            "amount": Decimal("50.00"),
            "balance": Decimal("1450.00"),
            "transaction_type": "debit",
        },
        {
            "date": date(2024, 1, 15),
            "description": "NETFLIX SUBSCRIPTION",
            "amount": Decimal("15.99"),
            "balance": Decimal("1434.01"),
            "transaction_type": "debit",
        },
        {
            "date": date(2024, 1, 20),
            "description": "ELECTRICITY BILL",
            "amount": Decimal("284.01"),
            "balance": Decimal("1150.00"),
            "transaction_type": "debit",
        },
    ]
    return ctx


class TestDeduplication:
    def test_no_duplicates(self, session_factory, sample_context):
        stage = DataCleanserStage(session_factory)
        result = stage.process(sample_context)
        # All 4 unique lines preserved
        assert len(result.unclassified_lines) == 4

    def test_removes_duplicates(self, session_factory, sample_context):
        # Add a duplicate line
        sample_context.raw_lines.append(sample_context.raw_lines[0].copy())
        stage = DataCleanserStage(session_factory)
        result = stage.process(sample_context)
        assert len(result.unclassified_lines) == 4  # duplicate removed


class TestValidation:
    def test_balanced_totals(self, session_factory, sample_context):
        stage = DataCleanserStage(session_factory)
        result = stage.process(sample_context)
        # Credits(500) - Debits(50+15.99+284.01=350) = 150
        # Closing(1150) - Opening(1000) = 150 ✓
        balance_errors = [e for e in result.errors if "mismatch" in e.lower()]
        assert len(balance_errors) == 0

    def test_unbalanced_totals(self, session_factory, sample_context):
        # Change closing balance to create a mismatch
        sample_context.raw_header["closing_balance"] = Decimal("9999.99")
        stage = DataCleanserStage(session_factory)
        result = stage.process(sample_context)
        balance_errors = [e for e in result.errors if "mismatch" in e.lower()]
        assert len(balance_errors) == 1


class TestDatabaseInsert:
    def test_inserts_statement(self, session_factory, sample_context):
        stage = DataCleanserStage(session_factory)
        result = stage.process(sample_context)

        assert result.statement_id is not None

        with session_factory() as session:
            stmt = session.get(Statement, result.statement_id)
            assert stmt is not None
            assert stmt.bank_name == "Test Bank"
            assert stmt.account_number == "12345678"
            assert len(stmt.lines) == 4

    def test_inserts_lines(self, session_factory, sample_context):
        stage = DataCleanserStage(session_factory)
        result = stage.process(sample_context)

        with session_factory() as session:
            lines = (
                session.query(StatementLine)
                .filter_by(statement_id=result.statement_id)
                .all()
            )
            assert len(lines) == 4
            assert all(line.category is None for line in lines)
            assert all(line.classification_method is None for line in lines)

    def test_unclassified_lines_have_ids(self, session_factory, sample_context):
        stage = DataCleanserStage(session_factory)
        result = stage.process(sample_context)

        for line in result.unclassified_lines:
            assert "id" in line
            assert "description" in line
            assert line["id"] is not None
