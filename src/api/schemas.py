"""Pydantic response / request schemas for the web API."""

from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel


# -- Responses ----------------------------------------------------------------

class UploadResponse(BaseModel):
    job_id: str
    status: str
    original_filename: str


class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    current_stage: Optional[str] = None
    error_message: Optional[str] = None
    statement_id: Optional[int] = None
    created_at: datetime
    completed_at: Optional[datetime] = None


class StatementLineResponse(BaseModel):
    id: int
    date: date
    description: str
    amount: float
    balance: Optional[float] = None
    transaction_type: str
    category: Optional[str] = None
    classification_method: Optional[str] = None
    matched_rule_id: Optional[int] = None
    matched_pattern: Optional[str] = None
    confidence: Optional[float] = None
    classification_reason: Optional[str] = None


class StatementResultResponse(BaseModel):
    statement_id: int
    bank_name: str
    account_number: str
    statement_date: Optional[date] = None
    opening_balance: float
    closing_balance: float
    lines: List[StatementLineResponse]


class JobDetailResponse(BaseModel):
    job_id: str
    status: str
    original_filename: str
    requested_bank: Optional[str] = None
    current_stage: Optional[str] = None
    error_message: Optional[str] = None
    created_at: datetime
    completed_at: Optional[datetime] = None
    stored_pdf_path: Optional[str] = None
    result: Optional[StatementResultResponse] = None


class JobSummary(BaseModel):
    job_id: str
    original_filename: str
    status: str
    requested_bank: Optional[str] = None
    current_stage: Optional[str] = None
    error_message: Optional[str] = None
    created_at: datetime
    completed_at: Optional[datetime] = None


class HistoryResponse(BaseModel):
    jobs: List[JobSummary]
    total: int


class BanksResponse(BaseModel):
    banks: List[str]


# -- Classification Rules -----------------------------------------------------

class RuleResponse(BaseModel):
    id: int
    pattern: str
    category: str
    priority: int
    source: str
    enabled: bool
    description: Optional[str] = None
    match_count: int = 0
    created_at: datetime


class RulesListResponse(BaseModel):
    rules: List[RuleResponse]
    total: int


class RuleCreateRequest(BaseModel):
    pattern: str
    category: str
    priority: int = 999
    description: Optional[str] = None


class RuleUpdateRequest(BaseModel):
    pattern: Optional[str] = None
    category: Optional[str] = None
    priority: Optional[int] = None
    enabled: Optional[bool] = None
    description: Optional[str] = None


# -- Refinement Proposals ------------------------------------------------------

class RefinementResponse(BaseModel):
    id: int
    pattern: str
    category: str
    confidence: float
    source_description: Optional[str] = None
    source_job_id: Optional[str] = None
    status: str
    reviewed_at: Optional[datetime] = None
    reviewer_note: Optional[str] = None
    created_rule_id: Optional[int] = None
    created_at: datetime


class RefinementsListResponse(BaseModel):
    proposals: List[RefinementResponse]
    total: int


class RefinementActionRequest(BaseModel):
    action: str  # "approve" or "reject"
    note: Optional[str] = None
    # Allow editing the pattern/category before approval
    pattern: Optional[str] = None
    category: Optional[str] = None


# -- Dashboard Stats -----------------------------------------------------------

class DashboardStatsResponse(BaseModel):
    total_jobs: int
    completed_jobs: int
    failed_jobs: int
    processing_jobs: int
    queued_jobs: int
    total_lines: int
    classified_lines: int
    regex_classified: int
    ai_classified: int
    unclassified_lines: int
    total_rules: int
    active_rules: int
    ai_rules: int
    manual_rules: int
    pending_refinements: int
