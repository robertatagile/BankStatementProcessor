"""Integration tests — full pipeline end-to-end with file management."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import List

import pytest
from fpdf import FPDF

from main import process_files, _safe_move
from src.models.database import init_db, Statement, StatementLine
from src.pipeline.data_cleanser import DataCleanserStage
from src.pipeline.pdf_extractor import PDFExtractorStage
from src.pipeline.queue import Pipeline, PipelineContext
from src.pipeline.regex_classifier import RegexClassifierStage


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REAL_RULES_PATH = Path(__file__).parent.parent / "config" / "classification_rules.json"


def _create_statement_pdf(path: Path, extra_lines: List[dict] = None) -> None:
    """Generate a minimal PDF that the Generic BankProfile can parse.

    The PDF extractor falls back to text-based extraction when no tables are
    found.  Each transaction line must match the ``text_line_pattern`` regex::

        (\\d{1,2}[\\/\\-]\\d{1,2}[\\/\\-]\\d{2,4})\\s+(.+?)\\s+(-?[£$€R]?\\s?[\\d,]+\\.\\d{2})(?:\\s+(-?[£$€R]?\\s?[\\d,]+\\.\\d{2}))?

    So the format is:  ``DD/MM/YYYY  DESCRIPTION  AMOUNT  [BALANCE]``
    """
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Helvetica", size=10)

    # Header — must match Generic BankProfile header_patterns
    header_lines = [
        "Test Bank",
        "Account Number: 12345678",
        "Statement Period: 01/01/2024",
        "to 31/01/2024",
        "Opening Balance: 1000.00",
        "Closing Balance: 1200.00",
    ]
    for line in header_lines:
        pdf.cell(w=0, h=6, text=line, new_x="LMARGIN", new_y="NEXT")

    pdf.ln(4)

    # Default transaction lines (SA-focused so regex classification works)
    default_lines = [
        {"date": "05/01/2024", "desc": "SALARY PAYMENT JAN",     "amount": "500.00",  "balance": "1500.00"},
        {"date": "10/01/2024", "desc": "CHECKERS SANDTON",        "amount": "-100.00", "balance": "1400.00"},
        {"date": "15/01/2024", "desc": "NETFLIX SUBSCRIPTION",    "amount": "-15.00",  "balance": "1385.00"},
        {"date": "20/01/2024", "desc": "ESKOM PREPAID",           "amount": "-185.00", "balance": "1200.00"},
    ]
    lines_to_write = extra_lines if extra_lines is not None else default_lines

    for txn in lines_to_write:
        text = f"{txn['date']}  {txn['desc']}  {txn['amount']}  {txn['balance']}"
        pdf.cell(w=0, h=6, text=text, new_x="LMARGIN", new_y="NEXT")

    pdf.output(str(path))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def input_dir(tmp_path):
    """Create an input/ directory inside tmp_path."""
    d = tmp_path / "input"
    d.mkdir()
    return d


@pytest.fixture
def session_factory(tmp_path):
    """Temporary SQLite database."""
    return init_db(str(tmp_path / "test.db"))


@pytest.fixture
def rules_file(tmp_path):
    """Copy the real classification rules into tmp_path."""
    dest = tmp_path / "rules.json"
    shutil.copy(str(REAL_RULES_PATH), str(dest))
    return str(dest)


@pytest.fixture
def valid_pdf(input_dir):
    """A minimal valid bank statement PDF."""
    pdf_path = input_dir / "valid_statement.pdf"
    _create_statement_pdf(pdf_path)
    return pdf_path


@pytest.fixture
def invalid_text_file(input_dir):
    """Plain text with a .pdf extension — will fail PDF parsing."""
    pdf_path = input_dir / "fake.pdf"
    pdf_path.write_bytes(b"This is not a PDF file at all.")
    return pdf_path


@pytest.fixture
def empty_pdf(input_dir):
    """Zero-byte file with a .pdf extension."""
    pdf_path = input_dir / "empty.pdf"
    pdf_path.write_bytes(b"")
    return pdf_path


@pytest.fixture
def corrupted_pdf(input_dir):
    """File with PDF magic bytes but invalid structure."""
    pdf_path = input_dir / "corrupted.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\ncorrupted content without valid xref table")
    return pdf_path


def _build_pipeline(session_factory, rules_file):
    """Build a 3-stage pipeline (no AI) for integration testing."""
    return Pipeline([
        PDFExtractorStage(),
        DataCleanserStage(session_factory),
        RegexClassifierStage(rules_file, session_factory),
    ])


# ---------------------------------------------------------------------------
# TestFileManagement
# ---------------------------------------------------------------------------

class TestFileManagement:
    """Verify that files are moved to processed/ or failed/ correctly."""

    def test_valid_pdf_moved_to_processed(self, input_dir, session_factory, rules_file, valid_pdf):
        pipeline = _build_pipeline(session_factory, rules_file)
        results = process_files([valid_pdf], pipeline, input_dir)

        assert len(results) == 1
        assert not valid_pdf.exists(), "original file should have been moved"
        assert (input_dir / "processed" / "valid_statement.pdf").exists()

    def test_invalid_file_moved_to_failed(self, input_dir, session_factory, rules_file, invalid_text_file):
        pipeline = _build_pipeline(session_factory, rules_file)
        results = process_files([invalid_text_file], pipeline, input_dir)

        assert len(results) == 0
        assert not invalid_text_file.exists()
        assert (input_dir / "failed" / "fake.pdf").exists()

    def test_empty_file_moved_to_failed(self, input_dir, session_factory, rules_file, empty_pdf):
        pipeline = _build_pipeline(session_factory, rules_file)
        results = process_files([empty_pdf], pipeline, input_dir)

        assert len(results) == 0
        assert not empty_pdf.exists()
        assert (input_dir / "failed" / "empty.pdf").exists()

    def test_corrupted_pdf_moved_to_failed(self, input_dir, session_factory, rules_file, corrupted_pdf):
        pipeline = _build_pipeline(session_factory, rules_file)
        results = process_files([corrupted_pdf], pipeline, input_dir)

        assert len(results) == 0
        assert not corrupted_pdf.exists()
        assert (input_dir / "failed" / "corrupted.pdf").exists()

    def test_mixed_valid_and_invalid(self, input_dir, session_factory, rules_file, valid_pdf, invalid_text_file):
        pipeline = _build_pipeline(session_factory, rules_file)
        pdf_files = sorted([valid_pdf, invalid_text_file])
        results = process_files(pdf_files, pipeline, input_dir)

        assert len(results) == 1
        assert (input_dir / "processed" / "valid_statement.pdf").exists()
        assert (input_dir / "failed" / "fake.pdf").exists()

    def test_dirs_created_automatically(self, input_dir, session_factory, rules_file, valid_pdf):
        # Ensure processed/ and failed/ do NOT exist yet
        assert not (input_dir / "processed").exists()
        assert not (input_dir / "failed").exists()

        pipeline = _build_pipeline(session_factory, rules_file)
        process_files([valid_pdf], pipeline, input_dir)

        assert (input_dir / "processed").is_dir()
        assert (input_dir / "failed").is_dir()

    def test_duplicate_filename_gets_timestamp_suffix(self, input_dir, session_factory, rules_file):
        # Create two valid PDFs with the same name (process one, then another copy)
        pdf1 = input_dir / "statement.pdf"
        _create_statement_pdf(pdf1)

        pipeline = _build_pipeline(session_factory, rules_file)
        process_files([pdf1], pipeline, input_dir)
        assert (input_dir / "processed" / "statement.pdf").exists()

        # Create another file with the same name and process it
        pdf2 = input_dir / "statement.pdf"
        _create_statement_pdf(pdf2)
        process_files([pdf2], pipeline, input_dir)

        # There should now be two files in processed/
        processed_files = list((input_dir / "processed").glob("statement*.pdf"))
        assert len(processed_files) == 2, f"Expected 2 files, got: {processed_files}"


# ---------------------------------------------------------------------------
# TestPipelineEndToEnd
# ---------------------------------------------------------------------------

class TestPipelineEndToEnd:
    """Verify the full pipeline produces correct database records and classifications."""

    def test_database_populated(self, input_dir, session_factory, rules_file, valid_pdf):
        pipeline = _build_pipeline(session_factory, rules_file)
        results = process_files([valid_pdf], pipeline, input_dir)

        assert len(results) == 1

        with session_factory() as session:
            statements = session.query(Statement).all()
            assert len(statements) == 1
            stmt = statements[0]
            assert stmt.account_number is not None

            lines = session.query(StatementLine).filter_by(statement_id=stmt.id).all()
            assert len(lines) == 4

    def test_regex_classification_applied(self, input_dir, session_factory, rules_file, valid_pdf):
        pipeline = _build_pipeline(session_factory, rules_file)
        process_files([valid_pdf], pipeline, input_dir)

        with session_factory() as session:
            lines = session.query(StatementLine).all()
            categories = {line.description: line.category for line in lines}

            # These should be classified by the SA-focused regex rules
            assert categories.get("SALARY PAYMENT JAN") == "Salary"
            assert categories.get("CHECKERS SANDTON") == "Groceries"
            assert categories.get("NETFLIX SUBSCRIPTION") == "Subscriptions"
            assert categories.get("ESKOM PREPAID") == "Utilities"

    def test_classification_method_is_regex(self, input_dir, session_factory, rules_file, valid_pdf):
        pipeline = _build_pipeline(session_factory, rules_file)
        process_files([valid_pdf], pipeline, input_dir)

        with session_factory() as session:
            lines = session.query(StatementLine).all()
            for line in lines:
                if line.category:
                    assert line.classification_method == "regex"

    def test_unclassified_line_remains_without_ai(self, input_dir, session_factory, rules_file):
        """A transaction that doesn't match any regex rule stays unclassified when AI is skipped."""
        pdf_path = input_dir / "unknown.pdf"
        _create_statement_pdf(pdf_path, extra_lines=[
            {"date": "05/01/2024", "desc": "SALARY PAYMENT JAN",         "amount": "500.00",  "balance": "1500.00"},
            {"date": "10/01/2024", "desc": "XYZZY UNKNOWN MERCHANT 999", "amount": "-100.00", "balance": "1400.00"},
            {"date": "15/01/2024", "desc": "CHECKERS SANDTON",           "amount": "-50.00",  "balance": "1350.00"},
            {"date": "20/01/2024", "desc": "BLORP MYSTERY PAYMENT",      "amount": "-150.00", "balance": "1200.00"},
        ])

        pipeline = _build_pipeline(session_factory, rules_file)
        process_files([pdf_path], pipeline, input_dir)

        with session_factory() as session:
            lines = session.query(StatementLine).all()
            classified = [l for l in lines if l.category is not None]
            unclassified = [l for l in lines if l.category is None]

            # SALARY and CHECKERS should be classified; the two unknown merchants should not
            assert len(classified) == 2
            assert len(unclassified) == 2


# ---------------------------------------------------------------------------
# TestSafeMove
# ---------------------------------------------------------------------------

class TestSafeMove:
    """Unit tests for the _safe_move helper."""

    def test_moves_file(self, tmp_path):
        src = tmp_path / "file.pdf"
        src.write_text("data")
        dest_dir = tmp_path / "dest"
        dest_dir.mkdir()

        result = _safe_move(src, dest_dir)
        assert result == dest_dir / "file.pdf"
        assert not src.exists()
        assert result.exists()

    def test_adds_timestamp_on_conflict(self, tmp_path):
        dest_dir = tmp_path / "dest"
        dest_dir.mkdir()
        # Pre-existing file
        (dest_dir / "file.pdf").write_text("old")

        src = tmp_path / "file.pdf"
        src.write_text("new")

        result = _safe_move(src, dest_dir)
        # Should NOT be the plain name (that already existed)
        assert result.name != "file.pdf"
        assert result.name.startswith("file_")
        assert result.name.endswith(".pdf")
        assert result.exists()
        # Original is still there too
        assert (dest_dir / "file.pdf").exists()
