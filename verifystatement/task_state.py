"""Persistent run state and task artifacts for the support loop.

Each support session lives under ``verifystatement/runs/{stem}_{timestamp}/``
and stores structured JSON artifacts so every iteration starts from saved
evidence instead of a growing prompt.
"""
from __future__ import annotations

import json
import shutil
from dataclasses import asdict, dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional


class TaskStatus(str, Enum):
    PENDING = "pending"
    DISCOVERING = "discovering"
    CLASSIFYING = "classifying"
    REPAIRING = "repairing"
    VALIDATING = "validating"
    COMPLETED = "completed"
    FAILED = "failed"
    MANUAL_REVIEW = "manual_review"


class Strategy(str, Enum):
    PROFILE_PATCH = "profile_patch"
    EXTRACTOR_PATCH = "extractor_patch"
    NEW_PROFILE = "new_profile"
    MANUAL_REVIEW = "manual_review"


class GateResult(str, Enum):
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"


class _TaskEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal and Enum values."""

    def default(self, o: Any) -> Any:
        if isinstance(o, Decimal):
            return float(o)
        if isinstance(o, Enum):
            return o.value
        if isinstance(o, Path):
            return str(o)
        return super().default(o)


@dataclass
class ValidationGate:
    """Result of a single validation gate."""
    name: str
    result: GateResult = GateResult.SKIPPED
    detail: str = ""
    stdout_excerpt: str = ""
    stderr_excerpt: str = ""


@dataclass
class AttemptRecord:
    """Record of a single repair attempt."""
    attempt_number: int
    strategy: Strategy
    hypothesis: str = ""
    changed_files: List[str] = field(default_factory=list)
    explanation: str = ""
    gates: List[ValidationGate] = field(default_factory=list)
    target_pdf_passes: bool = False
    regressions_pass: bool = False
    verification_delta: Dict[str, Any] = field(default_factory=dict)
    error: str = ""
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    @property
    def all_gates_pass(self) -> bool:
        return self.target_pdf_passes and self.regressions_pass


@dataclass
class LayoutSignature:
    """Lightweight layout descriptor for strategy selection."""
    date_family: str = ""  # e.g. "dd MMM", "dd/MM/yyyy"
    header_columns: List[str] = field(default_factory=list)
    table_shape: str = ""  # e.g. "multi_column", "collapsed", "none"
    text_extraction_quality: str = ""  # "good", "partial", "poor"
    preferred_strategy_hint: str = ""
    detected_bank: str = ""
    is_generic: bool = False


@dataclass
class TaskState:
    """Persistent state for a single support-loop run."""

    # Identity
    run_id: str = ""
    target_pdf: str = ""
    detected_bank: str = ""
    bank_key: str = ""
    layout_signature: LayoutSignature = field(default_factory=LayoutSignature)

    # Status
    status: TaskStatus = TaskStatus.PENDING
    strategy: Optional[Strategy] = None
    allowed_edit_scope: List[str] = field(default_factory=list)

    # Tracking
    current_hypothesis: str = ""
    attempt_count: int = 0
    max_attempts: int = 10
    attempts: List[AttemptRecord] = field(default_factory=list)

    # Results
    target_pdf_passes: bool = False
    required_regressions_pass: bool = False

    # Discrepancy tracking
    initial_issues: int = 0
    current_issues: int = 0

    # Timestamps
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())
    completed_at: str = ""

    @property
    def is_complete(self) -> bool:
        return self.target_pdf_passes and self.required_regressions_pass

    @property
    def can_retry(self) -> bool:
        return self.attempt_count < self.max_attempts and self.status not in (
            TaskStatus.COMPLETED,
            TaskStatus.MANUAL_REVIEW,
        )

    @property
    def latest_attempt(self) -> Optional[AttemptRecord]:
        return self.attempts[-1] if self.attempts else None

    @property
    def latest_failure_reason(self) -> str:
        if not self.attempts:
            return ""
        last = self.attempts[-1]
        if last.all_gates_pass:
            return ""
        failed_gates = [g for g in last.gates if g.result == GateResult.FAILED]
        if failed_gates:
            return "; ".join(f"{g.name}: {g.detail}" for g in failed_gates)
        if last.error:
            return last.error
        return "Unknown failure"


class RunDirectory:
    """Manages the filesystem layout for a single support-loop run."""

    def __init__(self, run_dir: Path):
        self._dir = run_dir

    @classmethod
    def create(cls, base_dir: Path, pdf_path: Path) -> "RunDirectory":
        stem = pdf_path.stem
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_dir = base_dir / f"{stem}_{timestamp}"
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "attempts").mkdir(exist_ok=True)
        return cls(run_dir)

    @classmethod
    def from_existing(cls, run_dir: Path) -> "RunDirectory":
        if not run_dir.exists():
            raise FileNotFoundError(f"Run directory not found: {run_dir}")
        return cls(run_dir)

    @classmethod
    def find_latest(cls, base_dir: Path, pdf_stem: str) -> Optional["RunDirectory"]:
        """Find the most recent run directory for a given PDF stem."""
        if not base_dir.exists():
            return None
        candidates = sorted(
            (d for d in base_dir.iterdir() if d.is_dir() and d.name.startswith(pdf_stem)),
            key=lambda d: d.stat().st_mtime,
            reverse=True,
        )
        return cls(candidates[0]) if candidates else None

    @property
    def path(self) -> Path:
        return self._dir

    @property
    def task_path(self) -> Path:
        return self._dir / "task.json"

    @property
    def evidence_path(self) -> Path:
        return self._dir / "evidence.json"

    @property
    def verification_report_path(self) -> Path:
        return self._dir / "verification_report.json"

    @property
    def summary_path(self) -> Path:
        return self._dir / "summary.json"

    @property
    def attempts_dir(self) -> Path:
        return self._dir / "attempts"

    def attempt_path(self, n: int) -> Path:
        return self.attempts_dir / f"attempt-{n}.json"

    # -- Persistence --

    def save_task(self, task: TaskState) -> None:
        task.updated_at = datetime.now().isoformat()
        self.task_path.write_text(
            json.dumps(asdict(task), indent=2, cls=_TaskEncoder, ensure_ascii=False),
            encoding="utf-8",
        )

    def load_task(self) -> TaskState:
        data = json.loads(self.task_path.read_text(encoding="utf-8"))
        # Reconstruct enums
        data["status"] = TaskStatus(data["status"])
        if data.get("strategy"):
            data["strategy"] = Strategy(data["strategy"])
        data["layout_signature"] = LayoutSignature(**data.get("layout_signature", {}))
        attempts = []
        for a in data.get("attempts", []):
            a["strategy"] = Strategy(a["strategy"])
            gates = [ValidationGate(**g) for g in a.get("gates", [])]
            for g in gates:
                g.result = GateResult(g.result)
            a["gates"] = gates
            attempts.append(AttemptRecord(**a))
        data["attempts"] = attempts
        return TaskState(**data)

    def save_evidence(self, evidence: dict) -> None:
        self.evidence_path.write_text(
            json.dumps(evidence, indent=2, cls=_TaskEncoder, ensure_ascii=False),
            encoding="utf-8",
        )

    def load_evidence(self) -> dict:
        if self.evidence_path.exists():
            return json.loads(self.evidence_path.read_text(encoding="utf-8"))
        return {}

    def save_verification_report(self, report: dict) -> None:
        self.verification_report_path.write_text(
            json.dumps(report, indent=2, cls=_TaskEncoder, ensure_ascii=False),
            encoding="utf-8",
        )

    def load_verification_report(self) -> dict:
        if self.verification_report_path.exists():
            return json.loads(self.verification_report_path.read_text(encoding="utf-8"))
        return {}

    def save_attempt(self, attempt: AttemptRecord) -> None:
        self.attempts_dir.mkdir(exist_ok=True)
        path = self.attempt_path(attempt.attempt_number)
        path.write_text(
            json.dumps(asdict(attempt), indent=2, cls=_TaskEncoder, ensure_ascii=False),
            encoding="utf-8",
        )

    def save_summary(self, summary: dict) -> None:
        self.summary_path.write_text(
            json.dumps(summary, indent=2, cls=_TaskEncoder, ensure_ascii=False),
            encoding="utf-8",
        )

    def archive(self, archive_dir: Path) -> Path:
        """Move run to an archive directory."""
        archive_dir.mkdir(parents=True, exist_ok=True)
        dest = archive_dir / self._dir.name
        shutil.move(str(self._dir), str(dest))
        return dest
