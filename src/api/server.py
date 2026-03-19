"""FastAPI application — upload, poll, results, history, PDF streaming,
rules management, refinement review, reprocessing, and dashboard stats."""

from __future__ import annotations

import json
import os
import platform
import subprocess
import uuid
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker

from src.api.jobs import enqueue_job
from src.api.schemas import (
    BanksResponse,
    DashboardStatsResponse,
    HistoryResponse,
    JobDetailResponse,
    JobStatusResponse,
    JobSummary,
    RefinementActionRequest,
    RefinementResponse,
    RefinementsListResponse,
    RuleCreateRequest,
    RuleResponse,
    RuleUpdateRequest,
    RulesListResponse,
    StatementLineResponse,
    StatementResultResponse,
    UploadResponse,
)
from src.models.database import (
    ClassificationRule,
    ProcessingJob,
    RefinementProposal,
    Statement,
    StatementLine,
    init_db,
)
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

PDF_SIGNATURE = b"%PDF-"

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

    contents = await file.read()
    if not contents[:1024].lstrip().startswith(PDF_SIGNATURE):
        raise HTTPException(status_code=400, detail="Only PDF files are accepted")

    sf = _get_session_factory()

    # Save the uploaded file with a unique name to avoid collisions
    safe_name = f"{uuid.uuid4().hex}_{file.filename}"
    dest = Path(UPLOAD_DIR) / safe_name
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
                            matched_rule_id=ln.matched_rule_id,
                            matched_pattern=ln.matched_pattern,
                            confidence=ln.confidence,
                            classification_reason=ln.classification_reason,
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
            stored_pdf_path=job.stored_pdf_path,
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
def list_history(
    status: str | None = Query(default=None),
    bank: str | None = Query(default=None),
    search: str | None = Query(default=None),
) -> HistoryResponse:
    sf = _get_session_factory()
    with sf() as session:
        q = session.query(ProcessingJob)
        if status:
            q = q.filter(ProcessingJob.status == status)
        if bank:
            q = q.filter(ProcessingJob.requested_bank == bank)
        if search:
            q = q.filter(ProcessingJob.original_filename.ilike(f"%{search}%"))
        jobs = q.order_by(ProcessingJob.created_at.desc()).all()
        return HistoryResponse(
            jobs=[
                JobSummary(
                    job_id=j.job_id,
                    original_filename=j.original_filename,
                    status=j.status,
                    requested_bank=j.requested_bank,
                    current_stage=j.current_stage,
                    error_message=j.error_message,
                    created_at=j.created_at,
                    completed_at=j.completed_at,
                )
                for j in jobs
            ],
            total=len(jobs),
        )


@app.get("/api/banks", response_model=BanksResponse)
def list_banks() -> BanksResponse:
    return BanksResponse(banks=BankProfileFactory.available_banks())


# ---------------------------------------------------------------------------
# Reprocess
# ---------------------------------------------------------------------------

@app.post("/api/jobs/{job_id}/reprocess", response_model=UploadResponse)
def reprocess_job(job_id: str) -> UploadResponse:
    """Full rerun from the original uploaded PDF."""
    sf = _get_session_factory()
    with sf() as session:
        job = session.query(ProcessingJob).filter_by(job_id=job_id).first()
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        pdf_path = job.stored_pdf_path
        original_filename = job.original_filename
        bank = job.requested_bank

    if not os.path.isfile(pdf_path):
        raise HTTPException(
            status_code=404,
            detail="Original PDF file not found on disk — cannot reprocess",
        )

    new_job_id = enqueue_job(
        session_factory=sf,
        pdf_path=pdf_path,
        original_filename=original_filename,
        rules_path=os.path.abspath(RULES_PATH),
        bank=bank,
    )
    return UploadResponse(
        job_id=new_job_id, status="queued", original_filename=original_filename
    )


# ---------------------------------------------------------------------------
# File Explorer
# ---------------------------------------------------------------------------

@app.post("/api/jobs/{job_id}/open-file")
def open_file_in_explorer(job_id: str) -> dict:
    """Open the original uploaded PDF's containing folder and select the file."""
    sf = _get_session_factory()
    with sf() as session:
        job = session.query(ProcessingJob).filter_by(job_id=job_id).first()
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        pdf_path = job.stored_pdf_path

    abs_path = os.path.abspath(pdf_path)
    if not os.path.isfile(abs_path):
        raise HTTPException(
            status_code=404, detail="PDF file no longer exists on disk"
        )

    # Validate path is inside the expected upload directory
    upload_abs = os.path.abspath(UPLOAD_DIR)
    if not abs_path.startswith(upload_abs):
        raise HTTPException(status_code=403, detail="Access denied")

    try:
        if platform.system() == "Windows":
            subprocess.Popen(["explorer", "/select,", abs_path])
        elif platform.system() == "Darwin":
            subprocess.Popen(["open", "-R", abs_path])
        else:
            subprocess.Popen(["xdg-open", os.path.dirname(abs_path)])
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Could not open file explorer: {exc}"
        )

    return {"status": "ok", "path": abs_path}


# ---------------------------------------------------------------------------
# Classification Rules CRUD
# ---------------------------------------------------------------------------

@app.get("/api/rules", response_model=RulesListResponse)
def list_rules(
    category: str | None = Query(default=None),
    source: str | None = Query(default=None),
    enabled_only: bool = Query(default=False),
) -> RulesListResponse:
    sf = _get_session_factory()
    with sf() as session:
        q = session.query(ClassificationRule)
        if category:
            q = q.filter(ClassificationRule.category == category)
        if source:
            q = q.filter(ClassificationRule.source == source)
        if enabled_only:
            q = q.filter(ClassificationRule.enabled == True)
        rules = q.order_by(ClassificationRule.priority).all()
        return RulesListResponse(
            rules=[
                RuleResponse(
                    id=r.id,
                    pattern=r.pattern,
                    category=r.category,
                    priority=r.priority,
                    source=r.source,
                    enabled=r.enabled if r.enabled is not None else True,
                    description=r.description,
                    match_count=r.match_count or 0,
                    created_at=r.created_at,
                )
                for r in rules
            ],
            total=len(rules),
        )


@app.post("/api/rules", response_model=RuleResponse, status_code=201)
def create_rule(req: RuleCreateRequest) -> RuleResponse:
    sf = _get_session_factory()
    with sf() as session:
        rule = ClassificationRule(
            pattern=req.pattern,
            category=req.category,
            priority=req.priority,
            source="manual",
            enabled=True,
            description=req.description,
            match_count=0,
        )
        session.add(rule)
        session.commit()
        session.refresh(rule)

        # Also sync to JSON file
        _sync_rule_to_json(rule)

        return RuleResponse(
            id=rule.id,
            pattern=rule.pattern,
            category=rule.category,
            priority=rule.priority,
            source=rule.source,
            enabled=rule.enabled,
            description=rule.description,
            match_count=rule.match_count or 0,
            created_at=rule.created_at,
        )


@app.put("/api/rules/{rule_id}", response_model=RuleResponse)
def update_rule(rule_id: int, req: RuleUpdateRequest) -> RuleResponse:
    sf = _get_session_factory()
    with sf() as session:
        rule = session.get(ClassificationRule, rule_id)
        if not rule:
            raise HTTPException(status_code=404, detail="Rule not found")
        if req.pattern is not None:
            rule.pattern = req.pattern
        if req.category is not None:
            rule.category = req.category
        if req.priority is not None:
            rule.priority = req.priority
        if req.enabled is not None:
            rule.enabled = req.enabled
        if req.description is not None:
            rule.description = req.description
        session.commit()
        session.refresh(rule)

        # Sync all rules to JSON
        _sync_all_rules_to_json(session)

        return RuleResponse(
            id=rule.id,
            pattern=rule.pattern,
            category=rule.category,
            priority=rule.priority,
            source=rule.source,
            enabled=rule.enabled if rule.enabled is not None else True,
            description=rule.description,
            match_count=rule.match_count or 0,
            created_at=rule.created_at,
        )


@app.delete("/api/rules/{rule_id}")
def delete_rule(rule_id: int) -> dict:
    sf = _get_session_factory()
    with sf() as session:
        rule = session.get(ClassificationRule, rule_id)
        if not rule:
            raise HTTPException(status_code=404, detail="Rule not found")
        session.delete(rule)
        session.commit()

        # Sync all rules to JSON
        _sync_all_rules_to_json(session)

    return {"status": "deleted", "id": rule_id}


# ---------------------------------------------------------------------------
# Refinement Proposals
# ---------------------------------------------------------------------------

@app.get("/api/refinements", response_model=RefinementsListResponse)
def list_refinements(
    status: str | None = Query(default=None),
) -> RefinementsListResponse:
    sf = _get_session_factory()
    with sf() as session:
        q = session.query(RefinementProposal)
        if status:
            q = q.filter(RefinementProposal.status == status)
        proposals = q.order_by(RefinementProposal.created_at.desc()).all()
        return RefinementsListResponse(
            proposals=[
                RefinementResponse(
                    id=p.id,
                    pattern=p.pattern,
                    category=p.category,
                    confidence=p.confidence,
                    source_description=p.source_description,
                    source_job_id=p.source_job_id,
                    status=p.status,
                    reviewed_at=p.reviewed_at,
                    reviewer_note=p.reviewer_note,
                    created_rule_id=p.created_rule_id,
                    created_at=p.created_at,
                )
                for p in proposals
            ],
            total=len(proposals),
        )


@app.post("/api/refinements/{proposal_id}/review", response_model=RefinementResponse)
def review_refinement(proposal_id: int, req: RefinementActionRequest) -> RefinementResponse:
    if req.action not in ("approve", "reject"):
        raise HTTPException(status_code=400, detail="Action must be 'approve' or 'reject'")

    sf = _get_session_factory()
    with sf() as session:
        proposal = session.get(RefinementProposal, proposal_id)
        if not proposal:
            raise HTTPException(status_code=404, detail="Proposal not found")
        if proposal.status != "pending":
            raise HTTPException(status_code=400, detail="Proposal already reviewed")

        proposal.status = "approved" if req.action == "approve" else "rejected"
        proposal.reviewed_at = datetime.utcnow()
        proposal.reviewer_note = req.note

        # Allow editing the pattern/category before approval
        if req.pattern:
            proposal.pattern = req.pattern
        if req.category:
            proposal.category = req.category

        created_rule_id = None
        if req.action == "approve":
            # Create an active classification rule from the approved proposal
            max_priority = (
                session.query(func.max(ClassificationRule.priority)).scalar() or 0
            )
            rule = ClassificationRule(
                pattern=proposal.pattern,
                category=proposal.category,
                priority=max_priority + 1,
                source="ai",
                enabled=True,
                description=f"Auto-generated from refinement #{proposal.id}",
                match_count=0,
            )
            session.add(rule)
            session.flush()
            proposal.created_rule_id = rule.id
            created_rule_id = rule.id

            # Sync to JSON
            _sync_rule_to_json(rule)

        session.commit()
        session.refresh(proposal)

        return RefinementResponse(
            id=proposal.id,
            pattern=proposal.pattern,
            category=proposal.category,
            confidence=proposal.confidence,
            source_description=proposal.source_description,
            source_job_id=proposal.source_job_id,
            status=proposal.status,
            reviewed_at=proposal.reviewed_at,
            reviewer_note=proposal.reviewer_note,
            created_rule_id=proposal.created_rule_id,
            created_at=proposal.created_at,
        )


# ---------------------------------------------------------------------------
# Dashboard Stats
# ---------------------------------------------------------------------------

@app.get("/api/dashboard/stats", response_model=DashboardStatsResponse)
def dashboard_stats() -> DashboardStatsResponse:
    sf = _get_session_factory()
    with sf() as session:
        total_jobs = session.query(ProcessingJob).count()
        completed = session.query(ProcessingJob).filter_by(status="completed").count()
        failed = session.query(ProcessingJob).filter_by(status="failed").count()
        processing = session.query(ProcessingJob).filter_by(status="processing").count()
        queued = session.query(ProcessingJob).filter_by(status="queued").count()

        total_lines = session.query(StatementLine).count()
        classified = session.query(StatementLine).filter(
            StatementLine.category.isnot(None)
        ).count()
        regex_cls = session.query(StatementLine).filter_by(
            classification_method="regex"
        ).count()
        ai_cls = session.query(StatementLine).filter_by(
            classification_method="ai"
        ).count()

        total_rules = session.query(ClassificationRule).count()
        active_rules = session.query(ClassificationRule).filter_by(enabled=True).count()
        ai_rules = session.query(ClassificationRule).filter_by(source="ai").count()
        manual_rules = session.query(ClassificationRule).filter_by(source="manual").count()

        pending_refs = session.query(RefinementProposal).filter_by(
            status="pending"
        ).count()

        return DashboardStatsResponse(
            total_jobs=total_jobs,
            completed_jobs=completed,
            failed_jobs=failed,
            processing_jobs=processing,
            queued_jobs=queued,
            total_lines=total_lines,
            classified_lines=classified,
            regex_classified=regex_cls,
            ai_classified=ai_cls,
            unclassified_lines=total_lines - classified,
            total_rules=total_rules,
            active_rules=active_rules,
            ai_rules=ai_rules,
            manual_rules=manual_rules,
            pending_refinements=pending_refs,
        )


# ---------------------------------------------------------------------------
# JSON sync helpers
# ---------------------------------------------------------------------------

def _sync_rule_to_json(rule: ClassificationRule) -> None:
    """Append a single rule to the JSON config file."""
    try:
        with open(RULES_PATH, "r") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        data = {"rules": []}

    rules = data.get("rules", [])
    # Avoid duplicates
    for existing in rules:
        if existing["pattern"] == rule.pattern:
            return

    rules.append({
        "pattern": rule.pattern,
        "category": rule.category,
        "priority": rule.priority,
        "source": rule.source,
    })
    data["rules"] = rules
    with open(RULES_PATH, "w") as f:
        json.dump(data, f, indent=2)


def _sync_all_rules_to_json(session) -> None:
    """Replace the JSON config with all enabled DB rules."""
    rules = (
        session.query(ClassificationRule)
        .filter_by(enabled=True)
        .order_by(ClassificationRule.priority)
        .all()
    )
    data = {
        "rules": [
            {
                "pattern": r.pattern,
                "category": r.category,
                "priority": r.priority,
                "source": r.source,
            }
            for r in rules
        ]
    }
    with open(RULES_PATH, "w") as f:
        json.dump(data, f, indent=2)
