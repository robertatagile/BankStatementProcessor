from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from src.api import jobs, server
from src.models.database import ProcessingJob


MINIMAL_PDF = b"%PDF-1.1\n1 0 obj<<>>endobj\ntrailer<<>>\n%%EOF\n"


def test_upload_returns_json_and_creates_job(tmp_path, monkeypatch):
    upload_dir = tmp_path / "uploads"
    upload_dir.mkdir()

    db_path = tmp_path / "test.db"
    rules_path = Path(__file__).resolve().parent.parent / "config" / "classification_rules.json"

    server._session_factory = None
    monkeypatch.setattr(server, "UPLOAD_DIR", str(upload_dir))
    monkeypatch.setattr(server, "DB_PATH", str(db_path))
    monkeypatch.setattr(server, "RULES_PATH", str(rules_path))
    monkeypatch.setattr(jobs._executor, "submit", lambda *args, **kwargs: None)

    with TestClient(server.app) as client:
        response = client.post(
            "/api/upload",
            files={"file": ("statement.pdf", MINIMAL_PDF, "application/pdf")},
        )

        assert response.status_code == 200

        payload = response.json()
        assert payload["status"] == "queued"
        assert payload["original_filename"] == "statement.pdf"
        assert payload["job_id"]

        session_factory = server._get_session_factory()
        with session_factory() as session:
            jobs_in_db = session.query(ProcessingJob).all()
            assert len(jobs_in_db) == 1
            assert jobs_in_db[0].job_id == payload["job_id"]
            assert jobs_in_db[0].statement is None

    server._session_factory = None
