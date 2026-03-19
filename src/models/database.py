from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from sqlalchemy import Boolean, ForeignKey, Float, Integer, Numeric, String, Text, create_engine
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
    sessionmaker,
)


class Base(DeclarativeBase):
    pass


class Statement(Base):
    __tablename__ = "statements"

    id: Mapped[int] = mapped_column(primary_key=True)
    bank_name: Mapped[str] = mapped_column(String(200))
    account_number: Mapped[str] = mapped_column(String(50))
    statement_date: Mapped[date]
    opening_balance: Mapped[Decimal] = mapped_column(
        Numeric(precision=12, scale=2, asdecimal=True)
    )
    closing_balance: Mapped[Decimal] = mapped_column(
        Numeric(precision=12, scale=2, asdecimal=True)
    )
    file_path: Mapped[str] = mapped_column(String(500))
    extraction_method: Mapped[Optional[str]] = mapped_column(
        String(20), nullable=True
    )  # "text", "table", "ocr", or null
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    lines: Mapped[List["StatementLine"]] = relationship(
        back_populates="statement", cascade="all, delete-orphan"
    )
    job: Mapped[Optional["ProcessingJob"]] = relationship(
        back_populates="statement"
    )
    info: Mapped[Optional["StatementInfo"]] = relationship(
        back_populates="statement", uselist=False, cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return (
            f"<Statement(id={self.id}, bank={self.bank_name}, "
            f"account={self.account_number}, date={self.statement_date})>"
        )


class StatementLine(Base):
    __tablename__ = "statement_lines"

    id: Mapped[int] = mapped_column(primary_key=True)
    statement_id: Mapped[int] = mapped_column(ForeignKey("statements.id"))
    date: Mapped[date]
    description: Mapped[str] = mapped_column(String(500))
    amount: Mapped[Decimal] = mapped_column(
        Numeric(precision=12, scale=2, asdecimal=True)
    )
    balance: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(precision=12, scale=2, asdecimal=True), nullable=True
    )
    transaction_type: Mapped[str] = mapped_column(String(10))  # "debit" or "credit"
    category: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    classification_method: Mapped[Optional[str]] = mapped_column(
        String(20), nullable=True
    )  # "regex", "ai", "manual", or null
    matched_rule_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("classification_rules.id"), nullable=True
    )
    matched_pattern: Mapped[Optional[str]] = mapped_column(
        String(500), nullable=True
    )
    confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    classification_reason: Mapped[Optional[str]] = mapped_column(
        String(500), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    statement: Mapped["Statement"] = relationship(back_populates="lines")
    matched_rule: Mapped[Optional["ClassificationRule"]] = relationship()

    def __repr__(self) -> str:
        return (
            f"<StatementLine(id={self.id}, date={self.date}, "
            f"desc={self.description[:30]}, amount={self.amount})>"
        )


class StatementInfo(Base):
    """Personal and address information extracted from a bank statement."""
    __tablename__ = "statement_info"

    id: Mapped[int] = mapped_column(primary_key=True)
    statement_id: Mapped[int] = mapped_column(ForeignKey("statements.id"))
    account_number: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    account_holder: Mapped[Optional[str]] = mapped_column(String(300), nullable=True)
    address_line1: Mapped[Optional[str]] = mapped_column(String(300), nullable=True)
    address_line2: Mapped[Optional[str]] = mapped_column(String(300), nullable=True)
    address_line3: Mapped[Optional[str]] = mapped_column(String(300), nullable=True)
    postal_code: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    account_type: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    branch_code: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    statement: Mapped["Statement"] = relationship(back_populates="info")

    def __repr__(self) -> str:
        return (
            f"<StatementInfo(id={self.id}, holder={self.account_holder}, "
            f"postal_code={self.postal_code})>"
        )


class ClassificationRule(Base):
    __tablename__ = "classification_rules"

    id: Mapped[int] = mapped_column(primary_key=True)
    pattern: Mapped[str] = mapped_column(String(500))
    category: Mapped[str] = mapped_column(String(100))
    priority: Mapped[int]
    source: Mapped[str] = mapped_column(String(20))  # "manual" or "ai"
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    description: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    match_count: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    def __repr__(self) -> str:
        return (
            f"<ClassificationRule(id={self.id}, category={self.category}, "
            f"source={self.source}, enabled={self.enabled})>"
        )


class RefinementProposal(Base):
    """AI-suggested classification rule awaiting human approval."""

    __tablename__ = "refinement_proposals"

    id: Mapped[int] = mapped_column(primary_key=True)
    pattern: Mapped[str] = mapped_column(String(500))
    category: Mapped[str] = mapped_column(String(100))
    confidence: Mapped[float] = mapped_column(Float)
    source_description: Mapped[Optional[str]] = mapped_column(
        String(500), nullable=True
    )  # The transaction description that triggered this
    source_job_id: Mapped[Optional[str]] = mapped_column(
        String(36), nullable=True
    )
    status: Mapped[str] = mapped_column(
        String(20), default="pending"
    )  # pending, approved, rejected
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    reviewer_note: Mapped[Optional[str]] = mapped_column(
        String(500), nullable=True
    )
    created_rule_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("classification_rules.id"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    created_rule: Mapped[Optional["ClassificationRule"]] = relationship()

    def __repr__(self) -> str:
        return (
            f"<RefinementProposal(id={self.id}, pattern={self.pattern}, "
            f"category={self.category}, status={self.status})>"
        )


class ProcessingJob(Base):
    """Tracks a single upload‑and‑process lifecycle for the web UI."""

    __tablename__ = "processing_jobs"

    id: Mapped[int] = mapped_column(primary_key=True)
    job_id: Mapped[str] = mapped_column(String(36), unique=True, index=True)
    original_filename: Mapped[str] = mapped_column(String(500))
    stored_pdf_path: Mapped[str] = mapped_column(String(500))
    requested_bank: Mapped[Optional[str]] = mapped_column(
        String(50), nullable=True
    )
    status: Mapped[str] = mapped_column(
        String(20), default="queued"
    )  # queued, processing, completed, failed
    current_stage: Mapped[Optional[str]] = mapped_column(
        String(50), nullable=True
    )
    error_message: Mapped[Optional[str]] = mapped_column(
        String(2000), nullable=True
    )
    statement_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("statements.id"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    completed_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)

    statement: Mapped[Optional["Statement"]] = relationship(
        back_populates="job"
    )

    def __repr__(self) -> str:
        return (
            f"<ProcessingJob(job_id={self.job_id}, status={self.status}, "
            f"file={self.original_filename})>"
        )


def _migrate(engine):
    """Apply schema migrations for columns/tables added after initial release."""
    import sqlite3

    raw = engine.raw_connection()
    cur = raw.cursor()

    def _has_column(table, column):
        cur.execute(f"PRAGMA table_info({table})")
        return any(row[1] == column for row in cur.fetchall())

    def _has_table(table):
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        )
        return cur.fetchone() is not None

    # Statement extraction_method column
    if _has_table("statements"):
        if not _has_column("statements", "extraction_method"):
            cur.execute(
                "ALTER TABLE statements ADD COLUMN extraction_method VARCHAR(20)"
            )

    # StatementLine provenance columns
    if _has_table("statement_lines"):
        if not _has_column("statement_lines", "matched_rule_id"):
            cur.execute(
                "ALTER TABLE statement_lines ADD COLUMN matched_rule_id INTEGER"
            )
        if not _has_column("statement_lines", "matched_pattern"):
            cur.execute(
                "ALTER TABLE statement_lines ADD COLUMN matched_pattern VARCHAR(500)"
            )
        if not _has_column("statement_lines", "confidence"):
            cur.execute(
                "ALTER TABLE statement_lines ADD COLUMN confidence FLOAT"
            )
        if not _has_column("statement_lines", "classification_reason"):
            cur.execute(
                "ALTER TABLE statement_lines ADD COLUMN classification_reason VARCHAR(500)"
            )

    # ClassificationRule new columns
    if _has_table("classification_rules"):
        if not _has_column("classification_rules", "enabled"):
            cur.execute(
                "ALTER TABLE classification_rules ADD COLUMN enabled BOOLEAN DEFAULT 1"
            )
        if not _has_column("classification_rules", "description"):
            cur.execute(
                "ALTER TABLE classification_rules ADD COLUMN description VARCHAR(500)"
            )
        if not _has_column("classification_rules", "match_count"):
            cur.execute(
                "ALTER TABLE classification_rules ADD COLUMN match_count INTEGER DEFAULT 0"
            )

    raw.commit()
    cur.close()
    raw.close()


def init_db(db_path: str) -> sessionmaker:
    """Initialize the database and return a session factory."""
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    _migrate(engine)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)
