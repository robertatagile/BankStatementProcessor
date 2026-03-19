# Documentation Index

This folder contains the current operational documentation for the Bank Statement Processor.

## Runtime Diagrams

| File | Description |
|---|---|
| `architecture.mmd` | End-to-end service architecture, including FastAPI, SQLite, frontend, and Document Processor integration |
| `job-lifecycle.mmd` | Upload, queue, processing, polling, and completion lifecycle for asynchronous jobs |
| `refinement-workflow.mmd` | AI classification proposal review flow and rule activation path |

## Guides

| File | Description |
|---|---|
| `INTEGRATION_TESTING.md` | Automated pytest coverage, real PDF validation flow, and Document Processor contract checks |

## Runtime Notes

- The backend API listens on port `8000` inside the container and is published on host port `8001` by `docker-compose.yml`.
- The frontend is served on host port `3000`.
- The current API surface covers job processing, rule management, refinement review, dashboard statistics, PDF retrieval, and file explorer integration.
- Registered bank profiles are exposed by `GET /api/banks` and currently include ABSA, ABSA Afrikaans, FNB, Nedbank, Standard Bank, Capitec, African Bank, TymeBank, Discovery Bank, Investec, and Old Mutual.