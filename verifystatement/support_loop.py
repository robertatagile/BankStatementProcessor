"""Support-loop orchestrator.

Implements the Ralph-style verification loop:
  discover → classify → repair → validate → persist learnings

Each iteration uses persistent run state so it can resume from failure
without recomputing evidence or reprompting.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from anthropic import Anthropic

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.logger import get_logger
from verifystatement.evidence import build_layout_signature, collect_evidence
from verifystatement.learnings import (
    format_learnings_summary,
    load_relevant_learnings,
    persist_learning,
)
from verifystatement.repair import (
    execute_extractor_patch,
    execute_new_profile,
    execute_profile_patch,
    revert_repair,
    run_triage,
)
from verifystatement.strategy import get_allowed_files, select_strategy
from verifystatement.task_state import (
    AttemptRecord,
    GateResult,
    RunDirectory,
    Strategy,
    TaskState,
    TaskStatus,
    ValidationGate,
)
from verifystatement.validation import run_all_gates

logger = get_logger(__name__)

RUNS_DIR = PROJECT_ROOT / "verifystatement" / "runs"
DEFAULT_MODEL = "claude-sonnet-4-20250514"
DEFAULT_MAX_ATTEMPTS = 10


def run_support_loop(
    pdf_path: str,
    *,
    client: Optional[Anthropic] = None,
    model: str = DEFAULT_MODEL,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    resume: bool = False,
) -> dict:
    """Run the full support loop for a single PDF.

    Stages:
    1. Discover — collect evidence and verify
    2. Classify — select repair strategy
    3. Repair — execute strategy-specific fix
    4. Validate — run all gates
    5. Persist — save learnings on success

    Returns a summary dict with final status.
    """
    pdf_path = Path(pdf_path).resolve()
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    if client is None:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError("ANTHROPIC_API_KEY environment variable not set")
        client = Anthropic(api_key=api_key)

    # -- Initialize or resume run --
    run_dir: RunDirectory
    task: TaskState

    if resume:
        run_dir_obj = RunDirectory.find_latest(RUNS_DIR, pdf_path.stem)
        if run_dir_obj and run_dir_obj.task_path.exists():
            run_dir = run_dir_obj
            task = run_dir.load_task()
            logger.info(f"Resuming run: {run_dir.path}")
            logger.info(
                f"  Status: {task.status.value}, "
                f"Attempts: {task.attempt_count}/{task.max_attempts}"
            )
        else:
            logger.info("No existing run found — starting fresh")
            resume = False

    if not resume:
        run_dir = RunDirectory.create(RUNS_DIR, pdf_path)
        task = TaskState(
            run_id=run_dir.path.name,
            target_pdf=str(pdf_path),
            max_attempts=max_attempts,
        )
        run_dir.save_task(task)
        logger.info(f"New run: {run_dir.path}")

    # -- Stage 1: Discover --
    if task.status in (TaskStatus.PENDING, TaskStatus.DISCOVERING):
        task.status = TaskStatus.DISCOVERING
        run_dir.save_task(task)

        _print_stage("DISCOVER", "Collecting evidence and verifying PDF")

        # Collect evidence (no Claude call)
        evidence = collect_evidence(pdf_path)
        run_dir.save_evidence(evidence)

        task.detected_bank = evidence["detected_bank"]
        task.bank_key = evidence.get("bank_key", "")
        task.layout_signature = build_layout_signature(evidence)

        # Run initial verification (requires Claude)
        from verifystatement.verify import verify_pdf

        verification_report = verify_pdf(str(pdf_path), client, model)
        run_dir.save_verification_report(verification_report)

        task.initial_issues = (
            verification_report.get("total_missing", 0)
            + verification_report.get("total_incorrect", 0)
            + verification_report.get("total_extra", 0)
        )
        task.current_issues = task.initial_issues

        if task.initial_issues == 0:
            task.status = TaskStatus.COMPLETED
            task.target_pdf_passes = True
            task.required_regressions_pass = True
            task.completed_at = datetime.now().isoformat()
            run_dir.save_task(task)
            _print_summary(task, run_dir)
            return _build_result(task, run_dir)

        # Show relevant prior learnings
        learnings = load_relevant_learnings(task.detected_bank)
        if learnings:
            logger.info(format_learnings_summary(learnings))

        task.status = TaskStatus.CLASSIFYING
        run_dir.save_task(task)

    # -- Stage 2: Classify --
    if task.status == TaskStatus.CLASSIFYING:
        _print_stage("CLASSIFY", "Selecting repair strategy")

        evidence = run_dir.load_evidence()
        verification_report = run_dir.load_verification_report()

        strategy = select_strategy(task, evidence, verification_report)
        task.strategy = strategy
        task.allowed_edit_scope = get_allowed_files(strategy, task.bank_key)

        logger.info(f"  Strategy: {strategy.value}")
        logger.info(f"  Allowed files: {task.allowed_edit_scope}")

        if strategy == Strategy.MANUAL_REVIEW:
            task.status = TaskStatus.MANUAL_REVIEW
            run_dir.save_task(task)
            _print_summary(task, run_dir)
            return _build_result(task, run_dir)

        # Run triage for context
        triage = run_triage(evidence, verification_report, client, model)
        task.current_hypothesis = triage.get("hypothesis", "")
        logger.info(f"  Hypothesis: {task.current_hypothesis}")

        task.status = TaskStatus.REPAIRING
        run_dir.save_task(task)

        # Store triage in evidence for repair prompts
        evidence["triage"] = triage
        run_dir.save_evidence(evidence)

    # -- Stage 3 & 4: Repair + Validate loop --
    while task.can_retry and task.status in (
        TaskStatus.REPAIRING,
        TaskStatus.VALIDATING,
    ):
        task.attempt_count += 1
        task.status = TaskStatus.REPAIRING
        run_dir.save_task(task)

        _print_stage(
            "REPAIR",
            f"Attempt {task.attempt_count}/{task.max_attempts} "
            f"({task.strategy.value})",
        )

        evidence = run_dir.load_evidence()
        verification_report = run_dir.load_verification_report()
        triage = evidence.get("triage", {})

        # Execute repair
        repair_result = _execute_repair(
            task, evidence, verification_report, triage, client, model
        )

        if repair_result.get("error"):
            logger.warning(f"  Repair error: {repair_result['error']}")
            attempt = AttemptRecord(
                attempt_number=task.attempt_count,
                strategy=task.strategy,
                error=repair_result["error"],
            )
            task.attempts.append(attempt)
            run_dir.save_attempt(attempt)
            run_dir.save_task(task)
            continue

        logger.info(f"  Explanation: {repair_result.get('explanation', '')}")
        logger.info(f"  Changed: {repair_result.get('changed_files', [])}")

        # -- Validate --
        task.status = TaskStatus.VALIDATING
        run_dir.save_task(task)

        _print_stage("VALIDATE", "Running validation gates")

        gates, target_passes, regressions_pass = run_all_gates(
            task.target_pdf,
            task.detected_bank,
            task.bank_key,
            task.strategy,
            client,
            model,
        )

        for gate in gates:
            logger.info(f"  {gate.name}: {gate.result.value} — {gate.detail}")

        attempt = AttemptRecord(
            attempt_number=task.attempt_count,
            strategy=task.strategy,
            hypothesis=task.current_hypothesis,
            changed_files=repair_result.get("changed_files", []),
            explanation=repair_result.get("explanation", ""),
            gates=gates,
            target_pdf_passes=target_passes,
            regressions_pass=regressions_pass,
        )
        task.attempts.append(attempt)
        run_dir.save_attempt(attempt)

        if attempt.all_gates_pass:
            task.target_pdf_passes = True
            task.required_regressions_pass = True
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.now().isoformat()
            run_dir.save_task(task)

            # -- Stage 5: Persist learnings --
            _print_stage("LEARN", "Persisting resolution learnings")
            persist_learning(task, evidence)

            _print_summary(task, run_dir)
            return _build_result(task, run_dir)

        # Validation failed — revert and retry
        logger.info("  Gates failed — reverting changes for next attempt")
        revert_repair(repair_result)

        task.status = TaskStatus.REPAIRING
        run_dir.save_task(task)

    # Exhausted attempts
    if not task.is_complete:
        task.status = TaskStatus.FAILED
        run_dir.save_task(task)

    _print_summary(task, run_dir)
    return _build_result(task, run_dir)


def _execute_repair(
    task: TaskState,
    evidence: dict,
    verification_report: dict,
    triage: dict,
    client: Anthropic,
    model: str,
) -> dict:
    """Dispatch repair execution based on strategy."""
    if task.strategy == Strategy.PROFILE_PATCH:
        return execute_profile_patch(
            task, evidence, verification_report, triage, client, model
        )
    elif task.strategy == Strategy.EXTRACTOR_PATCH:
        return execute_extractor_patch(
            task, evidence, verification_report, triage, client, model
        )
    elif task.strategy == Strategy.NEW_PROFILE:
        return execute_new_profile(
            task, evidence, verification_report, client, model
        )
    else:
        return {"error": f"No repair handler for strategy: {task.strategy}"}


def inspect_run(run_dir_path: str) -> dict:
    """Load and return the current state of a run for inspection."""
    run_dir = RunDirectory.from_existing(Path(run_dir_path))
    task = run_dir.load_task()
    evidence = run_dir.load_evidence()
    report = run_dir.load_verification_report()

    return {
        "run_id": task.run_id,
        "target_pdf": task.target_pdf,
        "detected_bank": task.detected_bank,
        "status": task.status.value,
        "strategy": task.strategy.value if task.strategy else None,
        "attempt_count": task.attempt_count,
        "max_attempts": task.max_attempts,
        "initial_issues": task.initial_issues,
        "current_issues": task.current_issues,
        "target_pdf_passes": task.target_pdf_passes,
        "regressions_pass": task.required_regressions_pass,
        "latest_failure": task.latest_failure_reason,
        "layout_signature": evidence.get("layout_signature", {}),
        "created_at": task.created_at,
        "updated_at": task.updated_at,
    }


def archive_run(run_dir_path: str) -> str:
    """Archive a completed run."""
    run_dir = RunDirectory.from_existing(Path(run_dir_path))
    archive_dir = RUNS_DIR / "archived"
    dest = run_dir.archive(archive_dir)
    return str(dest)


def list_runs() -> list:
    """List all run directories with basic status."""
    if not RUNS_DIR.exists():
        return []

    runs = []
    for d in sorted(RUNS_DIR.iterdir()):
        if not d.is_dir() or d.name == "archived":
            continue
        task_file = d / "task.json"
        if task_file.exists():
            try:
                data = json.loads(task_file.read_text(encoding="utf-8"))
                runs.append({
                    "run_id": data.get("run_id", d.name),
                    "target_pdf": data.get("target_pdf", ""),
                    "status": data.get("status", "unknown"),
                    "detected_bank": data.get("detected_bank", ""),
                    "attempt_count": data.get("attempt_count", 0),
                    "path": str(d),
                })
            except (json.JSONDecodeError, OSError):
                runs.append({"run_id": d.name, "status": "corrupt", "path": str(d)})

    return runs


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def _print_stage(stage: str, message: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  [{stage}] {message}")
    print(f"{'─' * 60}")


def _print_summary(task: TaskState, run_dir: RunDirectory) -> None:
    """Print an operator-friendly end-of-iteration summary."""
    print(f"\n{'═' * 60}")
    print(f"  Support Loop Summary")
    print(f"{'═' * 60}")
    print(f"  Run:               {task.run_id}")
    print(f"  Target PDF:        {Path(task.target_pdf).name}")
    print(f"  Bank:              {task.detected_bank}")
    print(f"  Status:            {task.status.value}")
    print(f"  Strategy:          {task.strategy.value if task.strategy else 'N/A'}")
    print(f"  Attempts:          {task.attempt_count}/{task.max_attempts}")
    print(f"  Initial issues:    {task.initial_issues}")
    print(f"  Target passes:     {task.target_pdf_passes}")
    print(f"  Regressions pass:  {task.required_regressions_pass}")

    if task.attempts:
        latest = task.attempts[-1]
        print(f"\n  Last attempt:")
        print(f"    Strategy:   {latest.strategy.value}")
        print(f"    Changed:    {latest.changed_files}")
        print(f"    Explanation: {latest.explanation[:120]}")
        for gate in latest.gates:
            symbol = "✓" if gate.result == GateResult.PASSED else "✗"
            print(f"    {symbol} {gate.name}: {gate.detail}")

    if task.status == TaskStatus.FAILED and task.latest_failure_reason:
        print(f"\n  Failure reason: {task.latest_failure_reason}")

    if task.status == TaskStatus.MANUAL_REVIEW:
        print(f"\n  → Manual review required. Inspect run at:")
        print(f"    {run_dir.path}")

    print(f"\n  Run directory: {run_dir.path}")
    print(f"{'═' * 60}\n")


def _build_result(task: TaskState, run_dir: RunDirectory) -> dict:
    """Build the final result dict."""
    return {
        "status": task.status.value,
        "run_id": task.run_id,
        "run_dir": str(run_dir.path),
        "target_pdf": task.target_pdf,
        "detected_bank": task.detected_bank,
        "strategy": task.strategy.value if task.strategy else None,
        "attempts": task.attempt_count,
        "initial_issues": task.initial_issues,
        "target_pdf_passes": task.target_pdf_passes,
        "regressions_pass": task.required_regressions_pass,
        "completed_at": task.completed_at,
    }
