from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from sqlalchemy import ForeignKey, Integer, Numeric, String, create_engine
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
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    lines: Mapped[List["StatementLine"]] = relationship(
        back_populates="statement", cascade="all, delete-orphan"
    )
    job: Mapped[Optional["ProcessingJob"]] = relationship(
        back_populates="statement"
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
    )  # "regex", "ai", or null
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    statement: Mapped["Statement"] = relationship(back_populates="lines")

    def __repr__(self) -> str:
        return (
            f"<StatementLine(id={self.id}, date={self.date}, "
            f"desc={self.description[:30]}, amount={self.amount})>"
        )


class ClassificationRule(Base):
    __tablename__ = "classification_rules"

    id: Mapped[int] = mapped_column(primary_key=True)
    pattern: Mapped[str] = mapped_column(String(500))
    category: Mapped[str] = mapped_column(String(100))
    priority: Mapped[int]
    source: Mapped[str] = mapped_column(String(20))  # "manual" or "ai"
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    def __repr__(self) -> str:
        return (
            f"<ClassificationRule(id={self.id}, category={self.category}, "
            f"source={self.source})>"
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


def init_db(db_path: str) -> sessionmaker:
    """Initialize the database and return a session factory."""
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)
