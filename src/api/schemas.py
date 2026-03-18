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
    result: Optional[StatementResultResponse] = None


class JobSummary(BaseModel):
    job_id: str
    original_filename: str
    status: str
    requested_bank: Optional[str] = None
    created_at: datetime
    completed_at: Optional[datetime] = None


class HistoryResponse(BaseModel):
    jobs: List[JobSummary]


class BanksResponse(BaseModel):
    banks: List[str]
