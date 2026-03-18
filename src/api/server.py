"""FastAPI application — upload, poll, results, history, and PDF streaming."""

from __future__ import annotations

import os
import uuid
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from sqlalchemy.orm import sessionmaker

from src.api.jobs import enqueue_job
from src.api.schemas import (
    BanksResponse,
    HistoryResponse,
    JobDetailResponse,
    JobStatusResponse,
    JobSummary,
    StatementLineResponse,
    StatementResultResponse,
    UploadResponse,
)
from src.models.database import ProcessingJob, Statement, StatementLine, init_db
from src.profiles import BankProfileFactory
from src.utils.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DB_PATH = os.environ.get("DB_PATH", "data/statements.db")
RULES_PATH = os.environ.get("RULES_PATH", "config/classification_rules.json")
UPLOAD_DIR = os.environ.get("UPLOAD_DIR", "uploads")

# ---------------------------------------------------------------------------
# App & middleware
# ---------------------------------------------------------------------------
app = FastAPI(title="Bank Statement Processor")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Startup — ensure directories and DB exist
# ---------------------------------------------------------------------------
_session_factory: sessionmaker | None = None


def _get_session_factory() -> sessionmaker:
    global _session_factory
    if _session_factory is None:
        os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
        os.makedirs(UPLOAD_DIR, exist_ok=True)
        _session_factory = init_db(DB_PATH)
        logger.info(f"Database initialised at {DB_PATH}")
    return _session_factory


@app.on_event("startup")
def on_startup() -> None:
    _get_session_factory()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/api/upload", response_model=UploadResponse)
async def upload(
    file: UploadFile = File(...),
    bank: str | None = Form(default=None),
) -> UploadResponse:
    if not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are accepted")

    sf = _get_session_factory()

    # Save the uploaded file with a unique name to avoid collisions
    safe_name = f"{uuid.uuid4().hex}_{file.filename}"
    dest = Path(UPLOAD_DIR) / safe_name
    contents = await file.read()
    dest.write_bytes(contents)

    job_id = enqueue_job(
        session_factory=sf,
        pdf_path=str(dest),
        original_filename=file.filename,
        rules_path=os.path.abspath(RULES_PATH),
        bank=bank if bank else None,
    )
    return UploadResponse(job_id=job_id, status="queued", original_filename=file.filename)


@app.get("/api/jobs/{job_id}", response_model=JobDetailResponse)
def get_job(job_id: str) -> JobDetailResponse:
    sf = _get_session_factory()
    with sf() as session:
        job = session.query(ProcessingJob).filter_by(job_id=job_id).first()
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        result = None
        if job.status == "completed" and job.statement_id:
            stmt = session.get(Statement, job.statement_id)
            if stmt:
                lines = (
                    session.query(StatementLine)
                    .filter_by(statement_id=stmt.id)
                    .order_by(StatementLine.date, StatementLine.id)
                    .all()
                )
                result = StatementResultResponse(
                    statement_id=stmt.id,
                    bank_name=stmt.bank_name,
                    account_number=stmt.account_number,
                    statement_date=stmt.statement_date,
                    opening_balance=float(stmt.opening_balance),
                    closing_balance=float(stmt.closing_balance),
                    lines=[
                        StatementLineResponse(
                            id=ln.id,
                            date=ln.date,
                            description=ln.description,
                            amount=float(ln.amount),
                            balance=float(ln.balance) if ln.balance is not None else None,
                            transaction_type=ln.transaction_type,
                            category=ln.category,
                            classification_method=ln.classification_method,
                        )
                        for ln in lines
                    ],
                )

        return JobDetailResponse(
            job_id=job.job_id,
            status=job.status,
            original_filename=job.original_filename,
            requested_bank=job.requested_bank,
            current_stage=job.current_stage,
            error_message=job.error_message,
            created_at=job.created_at,
            completed_at=job.completed_at,
            result=result,
        )


@app.get("/api/jobs/{job_id}/status", response_model=JobStatusResponse)
def get_job_status(job_id: str) -> JobStatusResponse:
    sf = _get_session_factory()
    with sf() as session:
        job = session.query(ProcessingJob).filter_by(job_id=job_id).first()
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return JobStatusResponse(
            job_id=job.job_id,
            status=job.status,
            current_stage=job.current_stage,
            error_message=job.error_message,
            statement_id=job.statement_id,
            created_at=job.created_at,
            completed_at=job.completed_at,
        )


@app.get("/api/jobs/{job_id}/pdf")
def get_job_pdf(job_id: str) -> FileResponse:
    sf = _get_session_factory()
    with sf() as session:
        job = session.query(ProcessingJob).filter_by(job_id=job_id).first()
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        pdf_path = job.stored_pdf_path

    if not os.path.isfile(pdf_path):
        raise HTTPException(status_code=404, detail="PDF file not found on disk")

    return FileResponse(pdf_path, media_type="application/pdf")


@app.get("/api/history", response_model=HistoryResponse)
def list_history() -> HistoryResponse:
    sf = _get_session_factory()
    with sf() as session:
        jobs = (
            session.query(ProcessingJob)
            .order_by(ProcessingJob.created_at.desc())
            .all()
        )
        return HistoryResponse(
            jobs=[
                JobSummary(
                    job_id=j.job_id,
                    original_filename=j.original_filename,
                    status=j.status,
                    requested_bank=j.requested_bank,
                    created_at=j.created_at,
                    completed_at=j.completed_at,
                )
                for j in jobs
            ]
        )


@app.get("/api/banks", response_model=BanksResponse)
def list_banks() -> BanksResponse:
    return BanksResponse(banks=BankProfileFactory.available_banks())
