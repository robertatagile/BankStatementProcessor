"""Tests for the support-loop modules: state, strategy, evidence, validation, learnings."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from verifystatement.task_state import (
    AttemptRecord,
    GateResult,
    LayoutSignature,
    RunDirectory,
    Strategy,
    TaskState,
    TaskStatus,
    ValidationGate,
)
from verifystatement.strategy import select_strategy, get_allowed_files, _needs_extractor_change
from verifystatement.learnings import persist_learning, load_relevant_learnings


# ---------------------------------------------------------------------------
# TaskState unit tests
# ---------------------------------------------------------------------------

class TestTaskState:
    def test_initial_state(self):
        task = TaskState(run_id="test_run", target_pdf="/tmp/test.pdf")
        assert task.status == TaskStatus.PENDING
        assert task.attempt_count == 0
        assert task.is_complete is False
        assert task.can_retry is True

    def test_is_complete(self):
        task = TaskState(
            target_pdf_passes=True,
            required_regressions_pass=True,
        )
        assert task.is_complete is True

    def test_can_retry_exhausted(self):
        task = TaskState(attempt_count=10, max_attempts=10)
        assert task.can_retry is False

    def test_can_retry_completed(self):
        task = TaskState(status=TaskStatus.COMPLETED)
        assert task.can_retry is False

    def test_latest_attempt(self):
        attempt = AttemptRecord(attempt_number=1, strategy=Strategy.PROFILE_PATCH)
        task = TaskState(attempts=[attempt])
        assert task.latest_attempt is attempt

    def test_latest_failure_reason(self):
        gate = ValidationGate(
            name="regression", result=GateResult.FAILED, detail="test_fnb failed"
        )
        attempt = AttemptRecord(
            attempt_number=1,
            strategy=Strategy.PROFILE_PATCH,
            gates=[gate],
        )
        task = TaskState(attempts=[attempt])
        assert "test_fnb failed" in task.latest_failure_reason

    def test_latest_failure_reason_no_attempts(self):
        task = TaskState()
        assert task.latest_failure_reason == ""


# ---------------------------------------------------------------------------
# RunDirectory unit tests
# ---------------------------------------------------------------------------

class TestRunDirectory:
    def test_create_and_save_task(self, tmp_path):
        run_dir = RunDirectory.create(tmp_path, Path("/tmp/test_statement.pdf"))
        assert run_dir.path.exists()
        assert run_dir.attempts_dir.exists()
        assert "test_statement_" in run_dir.path.name

        task = TaskState(
            run_id=run_dir.path.name,
            target_pdf="/tmp/test_statement.pdf",
            detected_bank="FNB",
            status=TaskStatus.DISCOVERING,
        )
        run_dir.save_task(task)
        assert run_dir.task_path.exists()

        loaded = run_dir.load_task()
        assert loaded.detected_bank == "FNB"
        assert loaded.status == TaskStatus.DISCOVERING

    def test_save_and_load_evidence(self, tmp_path):
        run_dir = RunDirectory.create(tmp_path, Path("/tmp/test.pdf"))
        evidence = {"detected_bank": "ABSA", "page_count": 3}
        run_dir.save_evidence(evidence)
        loaded = run_dir.load_evidence()
        assert loaded["detected_bank"] == "ABSA"

    def test_save_attempt(self, tmp_path):
        run_dir = RunDirectory.create(tmp_path, Path("/tmp/test.pdf"))
        attempt = AttemptRecord(
            attempt_number=1,
            strategy=Strategy.PROFILE_PATCH,
            explanation="Fixed date parsing",
            changed_files=["src/profiles/banks/fnb.py"],
        )
        run_dir.save_attempt(attempt)
        assert run_dir.attempt_path(1).exists()

    def test_find_latest(self, tmp_path):
        # Create two runs for the same stem
        rd1 = RunDirectory.create(tmp_path, Path("/tmp/my_statement.pdf"))
        TaskState(run_id=rd1.path.name)
        rd1.save_task(TaskState(run_id=rd1.path.name))

        rd2 = RunDirectory.create(tmp_path, Path("/tmp/my_statement.pdf"))
        rd2.save_task(TaskState(run_id=rd2.path.name))

        found = RunDirectory.find_latest(tmp_path, "my_statement")
        assert found is not None
        assert found.path == rd2.path

    def test_find_latest_none(self, tmp_path):
        found = RunDirectory.find_latest(tmp_path, "nonexistent")
        assert found is None

    def test_task_roundtrip_with_attempts(self, tmp_path):
        """Verify full task state serialization including nested objects."""
        run_dir = RunDirectory.create(tmp_path, Path("/tmp/test.pdf"))
        task = TaskState(
            run_id="test",
            target_pdf="/tmp/test.pdf",
            detected_bank="FNB",
            bank_key="fnb",
            status=TaskStatus.REPAIRING,
            strategy=Strategy.PROFILE_PATCH,
            layout_signature=LayoutSignature(
                date_family="dd MMM",
                table_shape="multi_column",
                detected_bank="FNB",
            ),
            attempts=[
                AttemptRecord(
                    attempt_number=1,
                    strategy=Strategy.PROFILE_PATCH,
                    gates=[
                        ValidationGate(
                            name="target_pdf",
                            result=GateResult.PASSED,
                            detail="ok",
                        ),
                        ValidationGate(
                            name="regression",
                            result=GateResult.FAILED,
                            detail="test_fnb failed",
                        ),
                    ],
                    target_pdf_passes=True,
                    regressions_pass=False,
                )
            ],
        )
        run_dir.save_task(task)
        loaded = run_dir.load_task()
        assert loaded.strategy == Strategy.PROFILE_PATCH
        assert loaded.layout_signature.date_family == "dd MMM"
        assert loaded.attempts[0].gates[1].result == GateResult.FAILED
        assert loaded.attempts[0].target_pdf_passes is True

    def test_archive(self, tmp_path):
        run_dir = RunDirectory.create(tmp_path / "runs", Path("/tmp/test.pdf"))
        run_dir.save_task(TaskState(run_id=run_dir.path.name))
        archive_dir = tmp_path / "archive"
        dest = run_dir.archive(archive_dir)
        assert dest.exists()
        assert (dest / "task.json").exists()


# ---------------------------------------------------------------------------
# Strategy selection tests
# ---------------------------------------------------------------------------

class TestStrategySelection:
    def _make_task(self, **overrides) -> TaskState:
        defaults = dict(
            run_id="test",
            target_pdf="/tmp/test.pdf",
            detected_bank="FNB",
            bank_key="fnb",
            layout_signature=LayoutSignature(
                detected_bank="FNB",
                is_generic=False,
                table_shape="multi_column",
                text_extraction_quality="good",
            ),
        )
        defaults.update(overrides)
        return TaskState(**defaults)

    def _make_report(self, **overrides) -> dict:
        defaults = {
            "total_pages": 3,
            "total_missing": 5,
            "total_incorrect": 2,
            "total_extra": 0,
            "total_extracted": 20,
        }
        defaults.update(overrides)
        return defaults

    def test_known_bank_profile_patch(self):
        task = self._make_task()
        report = self._make_report()
        strategy = select_strategy(task, {}, report)
        assert strategy == Strategy.PROFILE_PATCH

    def test_generic_bank_new_profile(self):
        task = self._make_task(
            detected_bank="Generic",
            layout_signature=LayoutSignature(
                detected_bank="Generic", is_generic=True
            ),
        )
        evidence = {
            "page_texts": {
                "1": "Bank of Excellence Account Statement Balance Transaction Credit Debit"
            }
        }
        report = self._make_report()
        strategy = select_strategy(task, evidence, report)
        assert strategy == Strategy.NEW_PROFILE

    def test_generic_bank_no_branding(self):
        task = self._make_task(
            detected_bank="Generic",
            layout_signature=LayoutSignature(
                detected_bank="Generic", is_generic=True
            ),
        )
        evidence = {"page_texts": {"1": "some random text"}}
        report = self._make_report()
        strategy = select_strategy(task, evidence, report)
        assert strategy == Strategy.MANUAL_REVIEW

    def test_collapsed_tables_escalate_to_extractor(self):
        task = self._make_task(
            layout_signature=LayoutSignature(
                detected_bank="FNB",
                is_generic=False,
                table_shape="collapsed",
                text_extraction_quality="good",
            ),
        )
        report = self._make_report()
        strategy = select_strategy(task, {}, report)
        assert strategy == Strategy.EXTRACTOR_PATCH

    def test_escalation_after_3_profile_failures(self):
        task = self._make_task(
            attempt_count=3,
            attempts=[
                AttemptRecord(
                    attempt_number=i,
                    strategy=Strategy.PROFILE_PATCH,
                )
                for i in range(1, 4)
            ],
        )
        report = self._make_report()
        strategy = select_strategy(task, {}, report)
        assert strategy == Strategy.EXTRACTOR_PATCH

    def test_manual_review_after_5_failures(self):
        task = self._make_task(
            attempt_count=5,
            attempts=[
                AttemptRecord(attempt_number=i, strategy=Strategy.EXTRACTOR_PATCH)
                for i in range(1, 6)
            ],
        )
        report = self._make_report()
        strategy = select_strategy(task, {}, report)
        assert strategy == Strategy.MANUAL_REVIEW

    def test_needs_extractor_change_high_missing(self):
        sig = LayoutSignature(table_shape="multi_column")
        report = {"total_missing": 50, "total_extracted": 5}
        assert _needs_extractor_change(sig, report) is True

    def test_allowed_files_profile_patch(self):
        files = get_allowed_files(Strategy.PROFILE_PATCH, "fnb")
        assert "src/profiles/banks/fnb.py" in files
        assert "src/pipeline/pdf_extractor.py" not in files

    def test_allowed_files_extractor_patch(self):
        files = get_allowed_files(Strategy.EXTRACTOR_PATCH, "fnb")
        assert "src/pipeline/pdf_extractor.py" in files
        assert "src/profiles/banks/fnb.py" in files

    def test_allowed_files_manual_review(self):
        files = get_allowed_files(Strategy.MANUAL_REVIEW, "fnb")
        assert files == []


# ---------------------------------------------------------------------------
# Learning persistence tests
# ---------------------------------------------------------------------------

class TestLearnings:
    def test_persist_and_load(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "verifystatement.learnings.LEARNINGS_DIR", tmp_path
        )

        task = TaskState(
            run_id="test_run",
            target_pdf="/tmp/fnb_statement.pdf",
            detected_bank="FNB",
            bank_key="fnb",
            strategy=Strategy.PROFILE_PATCH,
            attempt_count=2,
            target_pdf_passes=True,
            required_regressions_pass=True,
            initial_issues=5,
            layout_signature=LayoutSignature(
                date_family="dd MMM",
                table_shape="multi_column",
                detected_bank="FNB",
            ),
            attempts=[
                AttemptRecord(
                    attempt_number=2,
                    strategy=Strategy.PROFILE_PATCH,
                    explanation="Fixed date format",
                    changed_files=["src/profiles/banks/fnb.py"],
                    target_pdf_passes=True,
                    regressions_pass=True,
                )
            ],
        )

        path = persist_learning(task, {})
        assert path is not None
        assert path.exists()

        learnings = load_relevant_learnings("FNB")
        assert len(learnings) == 1
        assert learnings[0]["bank"] == "FNB"
        assert learnings[0]["strategy"] == "profile_patch"

    def test_no_persist_if_incomplete(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "verifystatement.learnings.LEARNINGS_DIR", tmp_path
        )
        task = TaskState(target_pdf_passes=False)
        path = persist_learning(task, {})
        assert path is None
