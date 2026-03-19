# Bank Statement Processor

A Python application that processes PDF bank statements through a 4-stage pipeline: PDF extraction, data cleansing, regex-based classification, and AI-powered classification using Claude. The system is self-improving — AI-generated regex patterns are saved and reused in future runs, reducing API calls over time.

## How It Works

```
PDF File
  │
  ▼
┌─────────────────────┐
│  1. PDF Extractor    │  Parse PDF → extract headers + transaction lines
└─────────┬───────────┘
          ▼
┌─────────────────────┐
│  2. Data Cleanser    │  Deduplicate → validate totals → insert into SQLite
└─────────┬───────────┘
          ▼
┌─────────────────────┐
│  3. Regex Classifier │  Match descriptions against ordered regex rules
└─────────┬───────────┘
          ▼
┌─────────────────────┐
│  4. AI Classifier    │  Send unmatched lines to Claude → classify + generate regex
└─────────────────────┘
```

### Stage 1: PDF Extraction

Parses bank statement PDFs using [pdfplumber](https://github.com/jsvine/pdfplumber). Extracts:

- **Statement headers**: bank name, account number, statement period, branch code, opening/closing balance
- **Transaction lines**: date, description, amount, balance, transaction type (debit/credit)
- **Personal/address info**: account holder name, street address, suburb, postal code, account type (stored in the `statement_info` table)

Supports both table-based and text-based PDF layouts, including FNB's merged-cell table format where amounts and balances are packed into a single cell. Handles multi-line descriptions, multiple date formats (`DD/MM/YYYY`, `DD-MM-YYYY`, `DD Mon YYYY`, `DDMon`, `YYYY-MM-DD`, etc.), and various currency symbols (`£`, `$`, `€`, `R`).

The extractor uses a **bank profile system** to handle bank-specific PDF formats. See [Bank Profiles](#bank-profiles) below.

### Stage 2: Data Cleansing

- **Deduplication**: Removes duplicate records based on the `(date, description, amount)` tuple, keeping the first occurrence
- **Total validation**: Verifies that `sum(credits) - sum(debits)` matches `closing_balance - opening_balance` (with a tolerance of `£0.01`). Mismatches are logged as warnings but do not halt processing
- **Database insertion**: Inserts the cleaned statement, its lines, and personal/address info into SQLite via SQLAlchemy
- **Classification rules seeding**: On first run, populates the `classification_rules` DB table from the JSON config file

### Stage 3: Regex Classification

Matches each transaction description against an ordered list of regex patterns from `config/classification_rules.json`. Rules are sorted by priority (lower number = higher priority). **First match wins.**

The rules file ships with 14 built-in categories tuned for South African bank statements:

| Priority | Category | Example matches |
|---|---|---|
| 1 | Groceries | Checkers, Pick n Pay, Shoprite, Spar, Woolworths Food, Food Lovers Market |
| 2 | Utilities | Eskom, City Power, Rand Water, Vodacom, MTN, Cell C, Telkom, municipalities |
| 3 | Rent/Mortgage | Rent, Mortgage, Bond Payment, Body Corporate, Levy |
| 4 | Salary | Salary, Wages, Payroll, Remuneration |
| 5 | Transfer | Transfer, EFT, Standing Order, Direct Debit, Instant Payment |
| 6 | Subscriptions | Netflix, Showmax, DStv, MultiChoice, Spotify, YouTube Premium |
| 7 | Transport | Engen, Sasol, Caltex, Total, Gautrain, e-toll, Bolt Ride/Trip/Taxi, Uber |
| 8 | Dining | Nando's, Spur, Steers, Wimpy, Ocean Basket, Debonairs, Chicken Licken, Mugg & Bean, KFC, Mr D Food, Bolt Food |
| 9 | Entertainment | Ster-Kinekor, Nu Metro, Computicket, Cinema, Theatre, Steam, PlayStation |
| 10 | Healthcare | Clicks, Dis-Chem, Mediclinic, Netcare, Life Healthcare, Discovery Health, Bonitas, Fedhealth, Medihelp, Virgin Active, Planet Fitness |
| 11 | Insurance | Sanlam, Old Mutual, Santam, Outsurance, King Price, Hollard, MiWay, Discovery Insure |
| 12 | Cash Withdrawal | ATM, Cash, Withdrawal |
| 13 | Clothing/Apparel | Mr Price, Truworths, Edgars, Jet, Ackermans, Pep Stores, Foschini, Totalsports, Markham |
| 14 | Electronics/Home | Game, Makro, Builders Warehouse, Hi-Fi Corp, Incredible Connection, iStore, Takealot |

Rules are evaluated in priority order — lower number wins. The first matching rule is applied and no further rules are checked.

### Stage 4: AI Classification

Transactions not matched by any regex rule are sent to the Claude API for classification. For each transaction, Claude returns:

1. A **category** from the predefined list
2. A **regex pattern** to match similar transactions in the future
3. A **confidence score** (0.0–1.0)

High-confidence patterns (>0.8) are automatically appended to `config/classification_rules.json` with `"source": "ai"`. This means subsequent runs will classify those transactions via the fast regex stage instead of calling the API again.

Transactions are batched (up to 20 per API call) to minimise costs. If the API is unavailable, lines default to the "Other" category.

## Prerequisites

- Python 3.9+
- An [Anthropic API key](https://console.anthropic.com/) (only required for Stage 4 — AI classification)

## Installation

```bash
git clone https://github.com/robertatagile/BankStatementProcessor.git
cd BankStatementProcessor
pip install -r requirements.txt
```

## Documentation and Diagrams

Detailed runtime documentation lives in `docs/`.

| File | Purpose |
|---|---|
| `docs/README.md` | Documentation index |
| `docs/architecture.mmd` | Runtime architecture and integration diagram |
| `docs/job-lifecycle.mmd` | Upload, queue, processing, and polling lifecycle |
| `docs/refinement-workflow.mmd` | AI proposal review and rule activation flow |
| `docs/INTEGRATION_TESTING.md` | Automated and real-statement validation guide |

## Usage

The repository supports three operating modes:

1. **CLI batch processing** through `main.py`
2. **FastAPI service** for asynchronous upload and polling
3. **Browser operations console** served by nginx on port `3000`

### CLI: process all PDFs in a directory

```bash
export ANTHROPIC_API_KEY=your-api-key-here
python3 main.py --pdf-dir data/
```

The CLI creates `processed/` and `failed/` folders inside the input directory and moves files after each run.

### CLI: process a single PDF

```bash
python3 main.py --pdf-file path/to/statement.pdf
```

### CLI: force a bank profile

```bash
python3 main.py --pdf-file statement.pdf --bank absa
```

If `--bank` is omitted, the extractor auto-detects the most likely profile from the PDF content.

### CLI: skip AI classification

```bash
python3 main.py --pdf-dir data/ --dry-run
```

### CLI: disable OCR fallback

```bash
python3 main.py --pdf-file scanned.pdf --no-ocr
```

### CLI options

| Option | Description |
|---|---|
| `--pdf-dir` | Directory containing PDF bank statements |
| `--pdf-file` | Process a single PDF file instead of a directory |
| `--db-path` | SQLite database path. Default: `data/statements.db` |
| `--rules-path` | Classification rules JSON path. Default: `config/classification_rules.json` |
| `--dry-run` | Skip the AI classification stage |
| `--bank` | Force a specific bank profile instead of auto-detecting |
| `--no-ocr` | Disable OCR fallback for scanned PDFs |

## Service Integration

The FastAPI backend in this repository is the bank statement extraction service consumed by the Document Processor workspace.

The active integration contract is:

- `POST /api/upload` uploads the original bank statement PDF as multipart form data and returns a `job_id`
- `GET /api/jobs/{job_id}/status` provides lightweight polling while the pipeline runs asynchronously
- `GET /api/jobs/{job_id}` returns the completed statement and transaction payload once processing finishes

In this workspace, the recommended host endpoint is `http://localhost:8001`.

- When Document Processor runs on the host machine, configure `BankStatementProcessor:BaseUrl` as `http://localhost:8001`
- When Document Processor runs in Docker, configure `BankStatementProcessor__BaseUrl` as `http://host.docker.internal:8001`

The backend container listens on port `8000` internally. Docker publishes it on host port `8001`.

## API Reference

### Core processing endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/health` | Liveness check. Returns `{ "status": "ok" }` |
| `POST` | `/api/upload` | Upload a PDF and enqueue a background job. Optional multipart field: `bank` |
| `GET` | `/api/jobs/{job_id}` | Full job detail plus extracted statement lines when complete |
| `GET` | `/api/jobs/{job_id}/status` | Lightweight status poll with stage and error fields |
| `GET` | `/api/jobs/{job_id}/pdf` | Stream the original uploaded PDF |
| `GET` | `/api/history` | List jobs with optional `status`, `bank`, and `search` filters |
| `GET` | `/api/banks` | List all registered bank profile keys |
| `POST` | `/api/jobs/{job_id}/reprocess` | Re-enqueue processing for the original stored PDF |
| `POST` | `/api/jobs/{job_id}/open-file` | Open the file location in the host operating system's file explorer |

### Rule management endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/rules` | List rules with optional `category`, `source`, and `enabled_only` filters |
| `POST` | `/api/rules` | Create a manual rule. Request body: `pattern`, `category`, optional `priority`, optional `description` |
| `PUT` | `/api/rules/{rule_id}` | Update a rule's pattern, category, priority, enabled flag, or description |
| `DELETE` | `/api/rules/{rule_id}` | Delete a rule and sync the JSON config |

### Refinement and dashboard endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/refinements` | List AI-generated rule proposals. Optional `status` filter |
| `POST` | `/api/refinements/{proposal_id}/review` | Approve or reject a pending proposal. Request body: `action`, optional `note`, optional edited `pattern`, optional edited `category` |
| `GET` | `/api/dashboard/stats` | Return aggregate counts for jobs, lines, rules, and pending refinements |

## Operations Console

The static frontend in `frontend/` runs on host port `3000` and talks to the FastAPI backend.

The UI currently exposes:

- **Dashboard**: job counts, classification totals, and recent activity
- **Jobs**: search, filter, inspect job details, stream the original PDF, and reprocess jobs
- **Rules**: create, edit, disable, and remove classification rules
- **Refinements**: review AI-proposed regex patterns before they become active rules
- **Training & Setup**: embedded operator guidance for improving coverage over time

### Run with Docker Compose

```bash
docker compose up --build
```

Open `http://localhost:3000` for the UI and `http://localhost:8001/api/health` for the backend health endpoint.

### Run without Docker

Start the backend:

```bash
uvicorn src.api.server:app --reload --port 8001
```

Then serve `frontend/index.html` with any static web server, or open it directly if you keep the default same-origin API setup.

## Project Structure

```
BankStatementProcessor/
├── main.py                          # CLI entry point
├── requirements.txt                 # Python dependencies
├── Dockerfile                       # Backend Docker image
├── docker-compose.yml               # Run frontend + backend together
├── config/
│   └── classification_rules.json    # Regex classification rules (manual + AI-generated)
├── src/
│   ├── api/
│   │   ├── server.py                # FastAPI application + endpoints
│   │   ├── schemas.py               # Pydantic request/response models
│   │   └── jobs.py                  # Background job runner
│   ├── pipeline/
│   │   ├── queue.py                 # Pipeline, Stage base class, PipelineContext
│   │   ├── pdf_extractor.py         # Stage 1: PDF parsing (profile-aware)
│   │   ├── data_cleanser.py         # Stage 2: Dedup, validation, DB insert
│   │   ├── regex_classifier.py      # Stage 3: Regex-based classification
│   │   └── ai_classifier.py         # Stage 4: Claude API classification
│   ├── profiles/
│   │   ├── base.py                  # BankProfile dataclass
│   │   ├── factory.py               # BankProfileFactory (registry + auto-detection)
│   │   └── south_africa.py          # ABSA, FNB, Nedbank, Standard Bank, Capitec
│   ├── models/
│   │   └── database.py              # SQLAlchemy models + DB initialisation
│   └── utils/
│       └── logger.py                # Logging configuration
├── frontend/
│   ├── index.html                   # Single-page upload + viewer UI
│   ├── nginx.conf                   # Nginx config (reverse-proxy to backend)
│   └── Dockerfile                   # Frontend Docker image
├── data/                            # SQLite DB (auto-created)
├── docs/
│   └── INTEGRATION_TESTING.md       # Integration testing guide
├── logs/                            # Pipeline log files (auto-created)
├── teststatement/                   # Real PDF test runner (not committed to git)
│   └── input/                       # Drop real PDFs here
│       ├── processed/               # Successfully processed PDFs
│       └── failed/                  # PDFs that failed processing
├── uploads/                         # Uploaded PDFs (auto-created)
└── tests/                           # Test suite
    ├── test_pipeline.py
    ├── test_pdf_extractor.py
    ├── test_data_cleanser.py
    ├── test_regex_classifier.py
    ├── test_ai_classifier.py
    ├── test_bank_profiles.py
    └── test_integration.py          # Integration tests (file management + end-to-end)
```

## Bank Profiles

The PDF extractor uses a **profile system** to handle the formatting differences between banks. Each profile encapsulates:

- **Header patterns** — regex patterns for extracting bank name, account number, branch/sort code, statement period, and balances
- **Date formats** — ordered list of date formats to try when parsing transaction dates
- **Column keywords** — keywords that identify table columns (date, description, debit, credit, balance)
- **Currency handling** — currency symbol and thousands separator for amount parsing
- **Text extraction pattern** — regex for extracting transactions from raw text (fallback when tables aren't detected)

### Supported South African Banks

| Bank | Profile key | Currency | Thousands separator | Key features |
|---|---|---|---|---|
| **ABSA** | `absa` | R (ZAR) | Space | "Cheque Account" label, period as "01 January 2024 to 31 January 2024", branch code |
| **ABSA Afrikaans** | `absa_afrikaans` | R (ZAR) | Space | Afrikaans headers, comma-decimal balances, `Kt`/`Dt`, text-only extraction |
| **FNB** | `fnb` | R (ZAR) | Space | Merged-cell table parsing, `DDMon` dates (no space), `Cr`/`Dr` suffixes, personal info extraction, "First National Bank" detection |
| **Nedbank** | `nedbank` | R (ZAR) | Space | "Account No" label, Greenbacks awareness, "Nedbank Ltd" detection |
| **Standard Bank** | `standard_bank` | R (ZAR) | Space | "Statement Period" label, "SBSA" detection |
| **Capitec** | `capitec` | R (ZAR) | Space | Single "Amount" column (not separate debit/credit), "Global One" branding, "Branch" without "Code" |
| **African Bank** | `african_bank` | R (ZAR) | Space | `YYYY/MM/DD` dates, negative amounts for debits, "Bank Charges" column, "Statement for:" personal info block, "MyWORLD Account" detection |
| **TymeBank** | `tymebank` | R (ZAR) | Space | TymeBank statement naming, local card purchase patterns, digital-wallet-oriented transaction text |
| **Discovery Bank** | `discovery_bank` | R (ZAR) | Space | Discovery branding, card-focused descriptions, modern app-driven statement layouts |
| **Investec** | `investec` | R (ZAR) | Space | Private banking statement formatting and high-balance account layouts |
| **Old Mutual** | `old_mutual` | R (ZAR) | Space | Old Mutual transaction wording and statement branding |

All South African profiles handle:
- **Rand amounts**: `R 1 234.56` (space thousands separator) and `R1,234.56` (comma fallback)
- **Branch codes**: 4–6 digit codes instead of UK sort codes
- **SA date formats**: `DD/MM/YYYY`, `DD Month YYYY`, `DD Mon YYYY`

### Auto-Detection

When no `--bank` argument is provided, the system automatically detects the bank by scanning the first page of the PDF for known keywords (e.g., "ABSA", "First National Bank", "Capitec"). The profile with the most keyword matches is selected. If no bank is detected, a generic profile is used that preserves the original UK-centric parsing behaviour.

### Manual Selection

Use the `--bank` flag to skip auto-detection and force a specific profile:

```bash
python3 main.py --pdf-file statement.pdf --bank fnb
```

### Adding a New Bank Profile

To add support for a new bank, create a factory function in `src/profiles/south_africa.py` (or a new region file) and register it with the factory:

```python
# In src/profiles/south_africa.py (or a new file)

def my_bank_profile() -> BankProfile:
    return _sa_base_profile(
        name="My Bank",
        detection_keywords=["my bank", "my bank ltd"],
        # Override any other fields as needed
    )

# Register in the register_all() function:
BankProfileFactory.register("my_bank", my_bank_profile)
```

The `_sa_base_profile()` helper provides shared South African defaults (ZAR currency, space thousands separator, branch code patterns, SA date formats). Override individual fields as needed for your bank.

For a non-SA bank, create a `BankProfile` directly:

```python
from src.profiles.base import BankProfile

def my_uk_bank_profile() -> BankProfile:
    return BankProfile(
        name="My UK Bank",
        detection_keywords=["my uk bank"],
        currency_symbol="£",
        thousands_separator=",",
        # ... other overrides
    )
```

## Database Schema

The SQLite database (`data/statements.db`) contains four tables:

### `statements`

| Column | Type | Description |
|---|---|---|
| id | INTEGER | Primary key |
| bank_name | VARCHAR(200) | Name of the bank |
| account_number | VARCHAR(50) | Account number |
| statement_date | DATE | Statement end date |
| opening_balance | NUMERIC(12,2) | Opening balance |
| closing_balance | NUMERIC(12,2) | Closing balance |
| file_path | VARCHAR(500) | Source PDF file path |
| created_at | DATETIME | Record creation timestamp |

### `statement_info`

| Column | Type | Description |
|---|---|---|
| id | INTEGER | Primary key |
| statement_id | INTEGER | Foreign key → `statements.id` (one-to-one) |
| account_number | VARCHAR(50) | Bank account number |
| account_holder | VARCHAR(300) | Account holder name (e.g. company or individual) |
| address_line1 | VARCHAR(300) | Street address |
| address_line2 | VARCHAR(300) | Suburb/city |
| address_line3 | VARCHAR(300) | Additional address line |
| postal_code | VARCHAR(20) | Postal code |
| account_type | VARCHAR(100) | Account type (e.g. "Gold Business Account") |
| branch_code | VARCHAR(20) | Branch code |
| created_at | DATETIME | Record creation timestamp |

### `statement_lines`

| Column | Type | Description |
|---|---|---|
| id | INTEGER | Primary key |
| statement_id | INTEGER | Foreign key → `statements.id` |
| date | DATE | Transaction date |
| description | VARCHAR(500) | Transaction description |
| amount | NUMERIC(12,2) | Transaction amount (always positive) |
| balance | NUMERIC(12,2) | Running balance (nullable) |
| transaction_type | VARCHAR(10) | `"debit"` or `"credit"` |
| category | VARCHAR(100) | Assigned category (nullable until classified) |
| classification_method | VARCHAR(20) | `"regex"`, `"ai"`, or null |
| created_at | DATETIME | Record creation timestamp |

### `classification_rules`

| Column | Type | Description |
|---|---|---|
| id | INTEGER | Primary key |
| pattern | VARCHAR(500) | Regex pattern |
| category | VARCHAR(100) | Target category |
| priority | INTEGER | Match priority (lower = higher priority) |
| source | VARCHAR(20) | `"manual"` or `"ai"` |
| created_at | DATETIME | Record creation timestamp |

## Adding Custom Classification Rules

Edit `config/classification_rules.json` to add your own regex patterns:

```json
{
  "rules": [
    {
      "pattern": "(?i)my-custom-merchant",
      "category": "Shopping",
      "priority": 15,
      "source": "manual"
    }
  ]
}
```

- **pattern**: A Python-compatible regex. Use `(?i)` for case-insensitive matching.
- **category**: The category to assign when matched. Can be any string.
- **priority**: Lower numbers are checked first. If a transaction matches multiple rules, the lowest priority number wins.
- **source**: Use `"manual"` for hand-written rules. AI-generated rules use `"ai"`.

### Persistent data

Docker Compose mounts these directories so data survives container restarts:

| Host path | Container path | Contents |
|---|---|---|
| `./data/` | `/app/data/` | SQLite database |
| `./uploads/` | `/app/uploads/` | Uploaded PDF files |
| `./logs/` | `/app/logs/` | Pipeline log files |
| `./config/` | `/app/config/` | Classification rules (manual + AI-generated) |

### AI classification

If `ANTHROPIC_API_KEY` is set, unmatched transactions are sent to Claude for classification. The service uses two persistence paths for learning:

- high-confidence matches can be written back into the rule set
- lower-trust suggestions can be queued as `RefinementProposal` records for manual approval through the UI or API

If the key is not set, the AI stage is skipped and only regex classification is used.

## Classification Categories

The active rule set is not limited to the original 14 seed categories. The repository currently uses a broader category set that includes manual and AI-generated coverage such as:

- `Groceries`
- `Utilities`
- `Rent/Mortgage`
- `Salary`
- `Transfer`
- `Subscriptions`
- `Transport`
- `Dining`
- `Entertainment`
- `Healthcare`
- `Insurance`
- `Cash Withdrawal`
- `Clothing/Apparel`
- `Electronics/Home`
- `Shopping`
- `Education`
- `Charity`
- `Fees`
- `Other`

Because rules can be created, approved, disabled, and deleted at runtime, treat `config/classification_rules.json` and `GET /api/rules` as the source of truth for the currently active catalogue.

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `ANTHROPIC_API_KEY` | unset | Enables Stage 4 AI classification |
| `DB_PATH` | `data/statements.db` | SQLite database path |
| `RULES_PATH` | `config/classification_rules.json` | Rule config path |
| `UPLOAD_DIR` | `uploads` | Directory for uploaded PDFs |

## Logging

Logs are written to both the console (INFO level) and `logs/pipeline.log` (DEBUG level). Each pipeline stage logs its entry, completion, and any warnings.

## Running Tests

```bash
# Python unit and integration tests
python3 -m pytest tests/ -v

# Integration tests only
python3 -m pytest tests/test_integration.py -v

# Browser UI smoke tests
npm install
npx playwright test
```

The Python test suite uses temporary databases and mocked external dependencies, so no API key or network access is needed for normal test execution. UI smoke tests use Playwright and default to `http://127.0.0.1:3000`.

For testing with real bank statement PDFs, see [docs/INTEGRATION_TESTING.md](docs/INTEGRATION_TESTING.md).

## Dependencies

| Package | Purpose |
|---|---|
| [pdfplumber](https://github.com/jsvine/pdfplumber) | PDF text and table extraction |
| [SQLAlchemy](https://www.sqlalchemy.org/) | ORM and database management |
| [anthropic](https://github.com/anthropics/anthropic-sdk-python) | Claude API client |
| [pydantic](https://docs.pydantic.dev/) | Data validation for AI responses |
| [FastAPI](https://fastapi.tiangolo.com/) | Web API framework |
| [uvicorn](https://www.uvicorn.org/) | ASGI server for FastAPI |
| [python-multipart](https://github.com/Kludex/python-multipart) | Multipart form parsing for file uploads |
| [pytest](https://docs.pytest.org/) | Test framework |
