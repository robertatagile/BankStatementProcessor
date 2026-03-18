"""Background job runner — executes the existing pipeline for a single uploaded PDF."""

from __future__ import annotations

import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from sqlalchemy.orm import sessionmaker

from src.models.database import ProcessingJob, Statement, StatementLine
from src.pipeline.ai_classifier import AIClassifierStage
from src.pipeline.data_cleanser import DataCleanserStage
from src.pipeline.pdf_extractor import PDFExtractorStage
from src.pipeline.queue import Pipeline, PipelineContext
from src.pipeline.regex_classifier import RegexClassifierStage
from src.profiles import BankProfileFactory
from src.utils.logger import get_logger

logger = get_logger(__name__)

DEFAULT_CATEGORIES = [
    "Groceries", "Utilities", "Rent/Mortgage", "Salary", "Transfer",
    "Subscriptions", "Transport", "Dining", "Entertainment", "Healthcare",
    "Insurance", "Cash Withdrawal", "Shopping", "Education", "Charity", "Other",
]

# Single‑thread pool so uploads are processed sequentially (safe for SQLite).
_executor = ThreadPoolExecutor(max_workers=1)


def enqueue_job(
    session_factory: sessionmaker,
    pdf_path: str,
    original_filename: str,
    rules_path: str,
    bank: str | None = None,
) -> str:
    """Create a ProcessingJob row, submit background work, and return the job_id."""
    job_id = uuid.uuid4().hex

    with session_factory() as session:
        job = ProcessingJob(
            job_id=job_id,
            original_filename=original_filename,
            stored_pdf_path=pdf_path,
            requested_bank=bank,
            status="queued",
        )
        session.add(job)
        session.commit()

    _executor.submit(
        _run_pipeline,
        session_factory,
        job_id,
        pdf_path,
        rules_path,
        bank,
    )
    return job_id


def _set_status(
    session_factory: sessionmaker,
    job_id: str,
    *,
    status: str | None = None,
    stage: str | None = None,
    error: str | None = None,
    statement_id: int | None = None,
    completed_at: datetime | None = None,
) -> None:
    with session_factory() as session:
        job = session.query(ProcessingJob).filter_by(job_id=job_id).one()
        if status is not None:
            job.status = status
        if stage is not None:
            job.current_stage = stage
        if error is not None:
            job.error_message = error
        if statement_id is not None:
            job.statement_id = statement_id
        if completed_at is not None:
            job.completed_at = completed_at
        session.commit()


def _run_pipeline(
    session_factory: sessionmaker,
    job_id: str,
    pdf_path: str,
    rules_path: str,
    bank: str | None,
) -> None:
    """Execute the 4‑stage pipeline and update job status along the way."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")

    try:
        _set_status(session_factory, job_id, status="processing", stage="extractor")

        # Build profile
        if bank:
            profile = BankProfileFactory.get(bank)
            extractor = PDFExtractorStage(profile=profile, auto_detect=False)
        else:
            extractor = PDFExtractorStage()

        # Extraction
        ctx = PipelineContext(file_path=pdf_path)
        ctx = extractor.process(ctx)

        # Cleansing
        _set_status(session_factory, job_id, stage="cleanser")
        cleanser = DataCleanserStage(session_factory)
        ctx = cleanser.process(ctx)

        # Regex classification
        _set_status(session_factory, job_id, stage="regex_classifier")
        regex = RegexClassifierStage(rules_path, session_factory)
        ctx = regex.process(ctx)

        # AI classification (optional)
        if api_key and ctx.unclassified_lines:
            _set_status(session_factory, job_id, stage="ai_classifier")
            ai = AIClassifierStage(
                api_key=api_key,
                rules_path=rules_path,
                session_factory=session_factory,
                categories=DEFAULT_CATEGORIES,
            )
            ctx = ai.process(ctx)

        _set_status(
            session_factory,
            job_id,
            status="completed",
            stage=None,
            statement_id=ctx.statement_id,
            completed_at=datetime.utcnow(),
        )
        logger.info(f"Job {job_id} completed — statement_id={ctx.statement_id}")

    except Exception as exc:
        logger.error(f"Job {job_id} failed: {exc}")
        _set_status(
            session_factory,
            job_id,
            status="failed",
            error=str(exc)[:2000],
            completed_at=datetime.utcnow(),
        )
