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

- **Statement headers**: bank name, account number, statement period, sort code, opening/closing balance
- **Transaction lines**: date, description, amount, balance, transaction type (debit/credit)

Supports both table-based and text-based PDF layouts. Handles multi-line descriptions, multiple date formats (`DD/MM/YYYY`, `DD-MM-YYYY`, `DD Mon YYYY`, `YYYY-MM-DD`, etc.), and various currency symbols (`£`, `$`, `€`).

### Stage 2: Data Cleansing

- **Deduplication**: Removes duplicate records based on the `(date, description, amount)` tuple, keeping the first occurrence
- **Total validation**: Verifies that `sum(credits) - sum(debits)` matches `closing_balance - opening_balance` (with a tolerance of `£0.01`). Mismatches are logged as warnings but do not halt processing
- **Database insertion**: Inserts the cleaned statement and its lines into SQLite via SQLAlchemy

### Stage 3: Regex Classification

Matches each transaction description against an ordered list of regex patterns from `config/classification_rules.json`. Rules are sorted by priority (lower number = higher priority). **First match wins.**

The rules file ships with 12 built-in categories:

| Category | Example matches |
|---|---|
| Groceries | Tesco, Sainsbury, Lidl, Aldi, Asda, Waitrose |
| Utilities | Electric, Gas, Water, British Gas, EDF, Octopus Energy |
| Rent/Mortgage | Rent, Mortgage, Letting |
| Salary | Salary, Wages, Payroll |
| Transfer | Transfer, Standing Order, Direct Debit |
| Subscriptions | Netflix, Spotify, Amazon Prime, Disney |
| Transport | Uber, Taxi, Train, TfL, Petrol, Shell, BP |
| Dining | Restaurant, Starbucks, Costa, McDonald's, Deliveroo |
| Entertainment | Cinema, Theatre, Pub, Steam, PlayStation |
| Healthcare | Pharmacy, Doctor, Dentist, NHS, Gym |
| Insurance | Aviva, Admiral, Direct Line, AXA |
| Cash Withdrawal | ATM, Cash, Withdrawal |

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

## Usage

### Process all PDFs in a directory

```bash
export ANTHROPIC_API_KEY=your-api-key-here
python3 main.py --pdf-dir data/
```

### Process a single PDF

```bash
python3 main.py --pdf-file path/to/statement.pdf
```

### Skip AI classification (regex only)

```bash
python3 main.py --pdf-dir data/ --dry-run
```

### All CLI options

```
usage: main.py [-h] [--pdf-dir PDF_DIR] [--pdf-file PDF_FILE]
               [--db-path DB_PATH] [--rules-path RULES_PATH] [--dry-run]

options:
  --pdf-dir PDF_DIR       Directory containing PDF bank statements (default: data)
  --pdf-file PDF_FILE     Process a single PDF file instead of a directory
  --db-path DB_PATH       Path to the SQLite database file (default: data/statements.db)
  --rules-path RULES_PATH Path to the classification rules JSON file
                          (default: config/classification_rules.json)
  --dry-run               Skip the AI classification stage
```

## Project Structure

```
BankStatementProcessor/
├── main.py                          # CLI entry point
├── requirements.txt                 # Python dependencies
├── config/
│   └── classification_rules.json    # Regex classification rules (manual + AI-generated)
├── src/
│   ├── pipeline/
│   │   ├── queue.py                 # Pipeline, Stage base class, PipelineContext
│   │   ├── pdf_extractor.py         # Stage 1: PDF parsing
│   │   ├── data_cleanser.py         # Stage 2: Dedup, validation, DB insert
│   │   ├── regex_classifier.py      # Stage 3: Regex-based classification
│   │   └── ai_classifier.py         # Stage 4: Claude API classification
│   ├── models/
│   │   └── database.py              # SQLAlchemy models + DB initialisation
│   └── utils/
│       └── logger.py                # Logging configuration
├── data/                            # Place PDF files here (also stores SQLite DB)
├── logs/                            # Pipeline log files (auto-created)
└── tests/                           # Test suite (48 tests)
    ├── test_pipeline.py
    ├── test_pdf_extractor.py
    ├── test_data_cleanser.py
    ├── test_regex_classifier.py
    └── test_ai_classifier.py
```

## Database Schema

The SQLite database (`data/statements.db`) contains three tables:

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
      "priority": 13,
      "source": "manual"
    }
  ]
}
```

- **pattern**: A Python-compatible regex. Use `(?i)` for case-insensitive matching.
- **category**: The category to assign when matched. Can be any string.
- **priority**: Lower numbers are checked first. If a transaction matches multiple rules, the lowest priority number wins.
- **source**: Use `"manual"` for hand-written rules. AI-generated rules use `"ai"`.

## Logging

Logs are written to both the console (INFO level) and `logs/pipeline.log` (DEBUG level). Each pipeline stage logs its entry, completion, and any warnings.

## Running Tests

```bash
python3 -m pytest tests/ -v
```

All tests use temporary databases and mock external dependencies (Anthropic API), so no API key or network access is needed to run the test suite.

## Dependencies

| Package | Purpose |
|---|---|
| [pdfplumber](https://github.com/jsvine/pdfplumber) | PDF text and table extraction |
| [SQLAlchemy](https://www.sqlalchemy.org/) | ORM and database management |
| [anthropic](https://github.com/anthropics/anthropic-sdk-python) | Claude API client |
| [pydantic](https://docs.pydantic.dev/) | Data validation for AI responses |
| [pytest](https://docs.pytest.org/) | Test framework |
