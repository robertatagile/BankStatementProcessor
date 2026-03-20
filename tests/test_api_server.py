from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from src.api import jobs, server
from src.models.database import ProcessingJob


MINIMAL_PDF = b"%PDF-1.1\n1 0 obj<<>>endobj\ntrailer<<>>\n%%EOF\n"
FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"
ABSA_REGRESSION_PDF = FIXTURES_DIR / "pdfs" / "absa_regression_statement.pdf"
ABSA_REGRESSION_EXPECTED = (
    FIXTURES_DIR / "expected" / "absa_regression_statement.json"
)
FNB_REGRESSION_PDF = FIXTURES_DIR / "pdfs" / "fnb_regression_statement.pdf"
FNB_REGRESSION_EXPECTED = (
    FIXTURES_DIR / "expected" / "fnb_regression_statement.json"
)
CAPITEC_REGRESSION_PDF = FIXTURES_DIR / "pdfs" / "capitec_regression_statement.pdf"
CAPITEC_REGRESSION_EXPECTED = (
    FIXTURES_DIR / "expected" / "capitec_regression_statement.json"
)
NEDBANK_REGRESSION_PDF = FIXTURES_DIR / "pdfs" / "nedbank_regression_statement.pdf"
NEDBANK_REGRESSION_EXPECTED = (
    FIXTURES_DIR / "expected" / "nedbank_regression_statement.json"
)


def _run_jobs_inline(monkeypatch):
    def run_inline(fn, *args, **kwargs):
        fn(*args, **kwargs)

        class _Done:
            def result(self, timeout=None):
                return None

        return _Done()

    monkeypatch.setattr(jobs._executor, "submit", run_inline)


def _configure_api_test_env(tmp_path, monkeypatch):
    upload_dir = tmp_path / "uploads"
    upload_dir.mkdir()

    db_path = tmp_path / "test.db"
    rules_path = Path(__file__).resolve().parent.parent / "config" / "classification_rules.json"

    server._session_factory = None
    monkeypatch.setattr(server, "UPLOAD_DIR", str(upload_dir))
    monkeypatch.setattr(server, "DB_PATH", str(db_path))
    monkeypatch.setattr(server, "RULES_PATH", str(rules_path))


def _normalize_job_lines(payload: dict) -> dict:
    result = payload["result"]
    return {
        "line_count": len(result["lines"]),
        "lines": [
            {
                "date": line["date"],
                "description": line["description"],
                "amount": line["amount"],
                "balance": line["balance"],
                "transaction_type": line["transaction_type"],
                "category": line["category"],
                "confidence": line["confidence"],
            }
            for line in result["lines"]
        ],
    }


def _sort_normalized_lines(payload: dict) -> dict:
    return {
        "line_count": payload["line_count"],
        "lines": sorted(
            payload["lines"],
            key=lambda line: (
                line["date"],
                line["balance"] is None,
                line["balance"] if line["balance"] is not None else 0,
                line["amount"],
                line["description"],
            ),
        ),
    }


def _normalize_extracted_job_lines(payload: dict) -> dict:
    result = payload["result"]
    return {
        "line_count": len(result["lines"]),
        "lines": [
            {
                "date": line["date"],
                "description": line["description"],
                "amount": line["amount"],
                "balance": line["balance"],
                "transaction_type": line["transaction_type"],
            }
            for line in result["lines"]
        ],
    }


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


def test_upload_rejects_non_pdf_extension(tmp_path, monkeypatch):
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
            files={"file": ("statement.txt", MINIMAL_PDF, "application/pdf")},
        )

        assert response.status_code == 400
        assert response.json() == {"detail": "Only PDF files are accepted"}

    server._session_factory = None


def test_upload_rejects_non_pdf_content(tmp_path, monkeypatch):
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
            files={"file": ("statement.pdf", b"not a pdf", "application/pdf")},
        )

        assert response.status_code == 400
        assert response.json() == {"detail": "Only PDF files are accepted"}

    server._session_factory = None


def test_fnb_statement_regression_returns_expected_lines(tmp_path, monkeypatch):
    _configure_api_test_env(tmp_path, monkeypatch)
    _run_jobs_inline(monkeypatch)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    expected = json.loads(FNB_REGRESSION_EXPECTED.read_text(encoding="utf-8"))

    with TestClient(server.app) as client:
        response = client.post(
            "/api/upload",
            files={
                "file": (
                    FNB_REGRESSION_PDF.name,
                    FNB_REGRESSION_PDF.read_bytes(),
                    "application/pdf",
                )
            },
        )

        assert response.status_code == 200

        job_id = response.json()["job_id"]
        detail = client.get(f"/api/jobs/{job_id}")

        assert detail.status_code == 200
        payload = detail.json()
        assert payload["status"] == "completed"

        normalized = _normalize_job_lines(payload)

        assert normalized["line_count"] == 44
        assert {
            "date": "2026-01-23",
            "description": "#Service Fees #Eft Charge-FNB To Other",
            "amount": 8.0,
            "balance": 10091.29,
            "transaction_type": "debit",
            "category": "Transfer",
            "confidence": None,
        } in normalized["lines"]
        assert {
            "date": "2026-01-27",
            "description": "#Service Fees #Eft Charge-FNB To Other",
            "amount": 2.0,
            "balance": 42065.49,
            "transaction_type": "debit",
            "category": "Transfer",
            "confidence": None,
        } in normalized["lines"]
        assert {
            "date": "2026-02-21",
            "description": "#Monthly Account Fee",
            "amount": 250.0,
            "balance": 8769.84,
            "transaction_type": "debit",
            "category": "Fees",
            "confidence": None,
        } in normalized["lines"]
        assert _sort_normalized_lines(normalized) == _sort_normalized_lines(expected)

    server._session_factory = None


def test_absa_statement_regression_returns_expected_lines(tmp_path, monkeypatch):
    _configure_api_test_env(tmp_path, monkeypatch)
    _run_jobs_inline(monkeypatch)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    expected = json.loads(ABSA_REGRESSION_EXPECTED.read_text(encoding="utf-8"))

    with TestClient(server.app) as client:
        response = client.post(
            "/api/upload",
            files={
                "file": (
                    ABSA_REGRESSION_PDF.name,
                    ABSA_REGRESSION_PDF.read_bytes(),
                    "application/pdf",
                )
            },
        )

        assert response.status_code == 200

        job_id = response.json()["job_id"]
        detail = client.get(f"/api/jobs/{job_id}")

        assert detail.status_code == 200
        payload = detail.json()
        assert payload["status"] == "completed"

        normalized = _normalize_job_lines(payload)

        assert normalized["line_count"] == 23
        assert {
            "date": "2025-09-22",
            "description": "TRANSACTION FEE ARCHIVE STMT ENQ (EFF: 220925 AMT: 0,00 )",
            "amount": 10.0,
            "balance": 4472.96,
            "transaction_type": "debit",
            "category": "Other",
            "confidence": None,
        } in normalized["lines"]
        assert {
            "date": "2025-09-22",
            "description": "TRANSFER FROM CSR P/LEND 30-6296-5160 ABSA LOAN",
            "amount": 100000.0,
            "balance": 104472.96,
            "transaction_type": "credit",
            "category": "Transfer",
            "confidence": None,
        } in normalized["lines"]
        assert {
            "date": "2025-10-01",
            "description": "ACB DEBIT:EXTERNAL LIBERTY050 77000534080",
            "amount": 199.65,
            "balance": 80082.35,
            "transaction_type": "debit",
            "category": "Insurance",
            "confidence": None,
        } in normalized["lines"]
        assert _sort_normalized_lines(normalized) == _sort_normalized_lines(expected)

    server._session_factory = None


def test_capitec_statement_regression_returns_expected_extracted_lines(tmp_path, monkeypatch):
    _configure_api_test_env(tmp_path, monkeypatch)
    _run_jobs_inline(monkeypatch)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    expected = json.loads(CAPITEC_REGRESSION_EXPECTED.read_text(encoding="utf-8"))

    with TestClient(server.app) as client:
        response = client.post(
            "/api/upload",
            files={
                "file": (
                    CAPITEC_REGRESSION_PDF.name,
                    CAPITEC_REGRESSION_PDF.read_bytes(),
                    "application/pdf",
                )
            },
        )

        assert response.status_code == 200

        job_id = response.json()["job_id"]
        detail = client.get(f"/api/jobs/{job_id}")

        assert detail.status_code == 200
        payload = detail.json()
        assert payload["status"] == "completed"

        normalized = _normalize_extracted_job_lines(payload)

        assert normalized["line_count"] == 69
        assert {
            "date": "2025-11-01",
            "description": "Eft Debit Order Insufficient Funds Fee",
            "amount": 6.0,
            "balance": -5.84,
            "transaction_type": "debit",
        } in normalized["lines"]
        assert {
            "date": "2025-10-30",
            "description": "DebiCheck Debit Order (2760398567): Npfinafbfn\n(DCPRD0001QB5SZ) Fee",
            "amount": 3.0,
            "balance": 16989.68,
            "transaction_type": "debit",
        } in normalized["lines"]
        assert {
            "date": "2025-11-28",
            "description": "Banking App Immediate Payment: Amanda Smit Fee",
            "amount": 1.0,
            "balance": 20136.0,
            "transaction_type": "debit",
        } in normalized["lines"]
        assert {
            "date": "2025-11-28",
            "description": "Banking App Immediate Payment: Amanda Smit",
            "amount": 600.0,
            "balance": 20137.0,
            "transaction_type": "debit",
        } in normalized["lines"]
        assert {
            "date": "2025-11-28",
            "description": "Banking App External Payment: Iclix",
            "amount": 1000.0,
            "balance": 19120.0,
            "transaction_type": "debit",
        } in normalized["lines"]
        assert {
            "date": "2025-12-15",
            "description": "Banking App External PayShap Payment: Sanet Minnie Fee",
            "amount": 6.0,
            "balance": 7445.0,
            "transaction_type": "debit",
        } in normalized["lines"]
        assert {
            "date": "2025-12-15",
            "description": "Banking App External PayShap Payment: Sanet Minnie",
            "amount": 1000.0,
            "balance": 7451.0,
            "transaction_type": "debit",
        } in normalized["lines"]
        assert {
            "date": "2025-12-31",
            "description": "PayShap Payment Received: Ina",
            "amount": 1200.0,
            "balance": 5848.8,
            "transaction_type": "credit",
        } in normalized["lines"]
        assert {
            "date": "2026-01-01",
            "description": "Banking App Immediate Payment: Johanna L Visag Fee",
            "amount": 1.0,
            "balance": 4012.63,
            "transaction_type": "debit",
        } in normalized["lines"]
        assert {
            "date": "2026-01-01",
            "description": "Banking App Immediate Payment: Johanna L Visag",
            "amount": 201.0,
            "balance": 4013.63,
            "transaction_type": "debit",
        } in normalized["lines"]
        assert not any(line["description"] == "Npfinafbfn" for line in normalized["lines"])
        assert not any(line["description"] == "Loancirc" for line in normalized["lines"])
        assert not any("Insufficient Funds (" in line["description"] for line in normalized["lines"])
        assert _sort_normalized_lines(normalized) == _sort_normalized_lines(expected)

    server._session_factory = None


def test_nedbank_statement_regression_returns_expected_extracted_lines(tmp_path, monkeypatch):
    _configure_api_test_env(tmp_path, monkeypatch)
    _run_jobs_inline(monkeypatch)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    expected = json.loads(NEDBANK_REGRESSION_EXPECTED.read_text(encoding="utf-8"))

    with TestClient(server.app) as client:
        response = client.post(
            "/api/upload",
            files={
                "file": (
                    NEDBANK_REGRESSION_PDF.name,
                    NEDBANK_REGRESSION_PDF.read_bytes(),
                    "application/pdf",
                )
            },
        )

        assert response.status_code == 200

        job_id = response.json()["job_id"]
        detail = client.get(f"/api/jobs/{job_id}")

        assert detail.status_code == 200
        payload = detail.json()
        assert payload["status"] == "completed"

        normalized = _normalize_extracted_job_lines(payload)

        assert normalized["line_count"] == 55
        assert {
            "date": "2025-12-19",
            "description": "S2S*Citycorner541282XXXXXX6246",
            "amount": 300.0,
            "balance": 31542.24,
            "transaction_type": "debit",
        } in normalized["lines"]
        assert {
            "date": "2025-12-27",
            "description": "VAT 26/11-26/12 = R19.17",
            "amount": 0.0,
            "balance": 17590.66,
            "transaction_type": "debit",
        } in normalized["lines"]
        assert {
            "date": "2025-12-31",
            "description": "CAP JUSTINVEST 295270549996",
            "amount": 5000.0,
            "balance": 16269.84,
            "transaction_type": "credit",
        } in normalized["lines"]
        assert {
            "date": "2026-01-13",
            "description": "Koos",
            "amount": 7500.0,
            "balance": 12209.82,
            "transaction_type": "credit",
        } in normalized["lines"]
        assert {
            "date": "2026-01-14",
            "description": "SWELLENDAM MEC541282XXXXXX6246",
            "amount": 113.86,
            "balance": 10199.03,
            "transaction_type": "debit",
        } in normalized["lines"]
        assert _sort_normalized_lines(normalized) == _sort_normalized_lines(expected)

    server._session_factory = None
