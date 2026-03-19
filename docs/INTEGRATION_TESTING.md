# Integration Testing Guide

This document explains the integration testing strategy for the Bank Statement Processor, covering both automated pytest tests and the manual test runner for real bank statement PDFs.

---

## Part 1 — Real Statement Test Runner (`teststatement/`)

### Folder Structure

```
teststatement/
├── run_test.sh              ← launcher script
├── run_test.ps1             ← Windows PowerShell launcher
├── test_statements.db       ← SQLite DB (created per run, deleted on reset)
└── input/
    ├── April2025.pdf        ← PDFs to process (you add these)
    ├── Feb2025.pdf
    ├── ...
    ├── processed/           ← successfully processed PDFs land here
    │   └── April2025.pdf
    └── failed/              ← PDFs that error out land here
        └── corrupt.pdf
```

### Quick Start

```bash
# 1. Place PDF bank statements in the input folder
cp ~/statements/*.pdf teststatement/input/

# 2. Run the test (from project root OR inside teststatement/)
bash teststatement/run_test.sh
```

```powershell
# Windows PowerShell / pwsh
pwsh -File .\teststatement\run_test.ps1
```

### What the Script Does

1. **Resets** the previous run — moves any files in `processed/` and `failed/` back to `input/`
2. **Deletes** the previous test database (`test_statements.db`) for a clean run
3. **Counts** PDF files and prints a pre-flight summary
4. **Runs the pipeline** in `--dry-run` mode (skips AI classification, no API key needed)
5. **Reports results** — counts of processed vs failed, with file lists
6. **Prints a DB summary** — statement count, line count, classification percentage, and top categories

### File Movement

| Outcome | Destination |
|---------|-------------|
| Pipeline completes successfully | `input/processed/` |
| Pipeline throws an exception | `input/failed/` |
| Duplicate filename in destination | Timestamp suffix added (e.g. `April2025_20260318_094512.pdf`) |

### Adding Test Files

**Positive tests (should succeed):** Place real bank statement PDFs in `teststatement/input/`. The pipeline will extract headers, parse transactions, and classify them using regex rules.

**Negative tests (should fail gracefully):** Add files that will fail parsing:
- Text files renamed to `.pdf`
- Empty `.pdf` files (zero bytes)
- Corrupted PDFs (partial or garbled content)
- Non-statement PDFs (invoices, receipts, documents with no transaction data)

These files should end up in `input/failed/`.

### Interpreting Results

The script prints a colour-coded summary:

```
  Processed : 12       (green)
  Failed    : 1        (red)

Database summary:
  Statements      : 12
  Total lines     : 487
  Classified      : 423
  Unclassified    : 64
  Classification% : 86.9%

  Top categories:
    Transfer                  98
    Groceries                 67
    Utilities                 54
```

  ### Windows Notes

  - `run_test.ps1` is the Windows equivalent of `run_test.sh`
  - The PowerShell script auto-detects Python using `py -3`, `python`, or `python3`
  - If Python dependencies are not installed yet, install them first with `py -3 -m pip install -r requirements.txt` or `python -m pip install -r requirements.txt`

### Database Inspection

The test database is saved at `teststatement/test_statements.db`. Query it directly:

```bash
sqlite3 teststatement/tmp/test.db

-- View all statements
SELECT bank_name, account_number, statement_date, opening_balance, closing_balance FROM statements;

-- View personal/address info
SELECT account_holder, address_line1, address_line2, postal_code, account_type, branch_code FROM statement_info;

-- View unclassified lines (candidates for new regex rules)
SELECT description, amount FROM statement_lines WHERE category IS NULL;

-- Category breakdown
SELECT category, COUNT(*), SUM(amount)
FROM statement_lines GROUP BY category ORDER BY COUNT(*) DESC;

-- Check seeded classification rules
SELECT category, priority, source FROM classification_rules ORDER BY priority;
```

### Running with AI Classification

To enable the AI classification stage (Stage 4), set your API key and run directly:

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
python3 main.py \
    --pdf-dir teststatement/input \
    --db-path teststatement/test_statements.db \
    --rules-path config/classification_rules.json
```

---

## Part 2 — Automated Integration Tests (`tests/test_integration.py`)

These tests use programmatically generated PDFs (via `fpdf2`) and run entirely inside pytest with `tmp_path` fixtures. No real bank statements or API keys are needed.

The broader automated test surface in this repository also includes:

- API tests in `tests/test_api_server.py`
- bank profile coverage in `tests/test_bank_profiles.py`
- extractor, cleanser, regex, AI, and pipeline unit tests in the remaining `tests/*.py` modules
- Playwright UI smoke tests in `tests/ui/`

### File Management Flow

The `process_files()` function in `main.py` handles post-processing file management:

1. **Before processing** — `processed/` and `failed/` subdirectories are created if they don't exist.
2. **On success** — The PDF is moved from `input/` to `input/processed/`.
3. **On failure** — The PDF is moved from `input/` to `input/failed/`.
4. **Duplicate filenames** — A timestamp suffix is appended (e.g., `statement_20260318_143022.pdf`).

### Test Fixtures

All fixtures are generated programmatically — no sample PDFs are committed.

**Valid Statement PDF** — Generated by `_create_statement_pdf()` with:
- Header: Test Bank, account 12345678, period 01/01/2024–31/01/2024
- 4 SA transaction lines: SALARY, CHECKERS, NETFLIX, ESKOM

**Negative Fixtures:**

| Fixture | Contents | Expected behaviour |
|---|---|---|
| Plain text as PDF | `This is not a PDF file at all.` | Fails → moved to `failed/` |
| Empty file | Zero bytes | Fails → moved to `failed/` |
| Corrupted PDF | `%PDF-1.4` header + garbage | Fails → moved to `failed/` |

### Test Structure

#### TestFileManagement (7 tests)

| Test | Verifies |
|---|---|
| `test_valid_pdf_moved_to_processed` | Valid PDF ends up in `processed/` |
| `test_invalid_file_moved_to_failed` | Plain text `.pdf` ends up in `failed/` |
| `test_empty_file_moved_to_failed` | Zero-byte `.pdf` ends up in `failed/` |
| `test_corrupted_pdf_moved_to_failed` | Corrupted `.pdf` ends up in `failed/` |
| `test_mixed_valid_and_invalid` | 1 valid + 1 invalid → correct routing, results list has 1 entry |
| `test_dirs_created_automatically` | Directories are created if they don't exist |
| `test_duplicate_filename_gets_timestamp_suffix` | Second file with same name gets timestamp suffix |

#### TestPipelineEndToEnd (4 tests)

| Test | Verifies |
|---|---|
| `test_database_populated` | `statements` has 1 row, `statement_lines` has 4 rows |
| `test_regex_classification_applied` | CHECKERS→Groceries, NETFLIX→Subscriptions, ESKOM→Utilities, SALARY→Salary |
| `test_classification_method_is_regex` | All classified lines have `classification_method = "regex"` |
| `test_unclassified_line_remains_without_ai` | Unrecognisable merchants remain unclassified without AI stage |

#### TestSafeMove (2 tests)

| Test | Verifies |
|---|---|
| `test_moves_file` | File moved to destination, source removed |
| `test_adds_timestamp_on_conflict` | Timestamp suffix added when destination file exists |

### Running the Tests

```bash
# All Python tests
python3 -m pytest tests/ -v

# Integration tests only
python3 -m pytest tests/test_integration.py -v

# Specific class
python3 -m pytest tests/test_integration.py::TestFileManagement -v

# Single test
python3 -m pytest tests/test_integration.py::TestFileManagement::test_mixed_valid_and_invalid -v

# UI smoke tests (frontend running on http://127.0.0.1:3000)
npm install
npx playwright test
```

---

## Part 3 — Running Against Real Bank Statements

Place real bank statement PDFs in `teststatement/input/` and run:

```bash
# Dry run (regex classification only, no API key needed)
python3 main.py --pdf-dir teststatement/input --db-path teststatement/tmp/test.db --dry-run

# Full run with AI classification
export ANTHROPIC_API_KEY="sk-ant-..."
python3 main.py --pdf-dir teststatement/input --db-path teststatement/tmp/test.db
```

### Supported Banks

| Bank | Auto-detected by | Key features |
|------|------------------|--------------|
| **FNB** | "FNB", "First National Bank", "FirstRand" | Merged-cell tables, `DDMon` dates, `Cr`/`Dr` suffixes, `*COMPANY` personal info |
| **African Bank** | "African Bank", "MyWORLD" | `YYYY/MM/DD` dates, negative amounts for debits, Bank Charges column, "Statement for:" address block |
| **ABSA** | "ABSA", "ABSA Bank" | "Cheque Account" label, period as "01 January 2024 to 31 January 2024" |
| **ABSA Afrikaans** | "ABSA", "Tjekrekeningstaat", Afrikaans field labels | Afrikaans statement text, comma-decimal balances, `Kt`/`Dt` indicators |
| **Nedbank** | "Nedbank", "Nedbank Ltd" | "Account No" label, Greenbacks awareness |
| **Standard Bank** | "Standard Bank", "SBSA" | "Statement Period" label |
| **Capitec** | "Capitec", "Global One" | Single Amount column, "Branch" without "Code" |
| **TymeBank** | "TymeBank", TymeBank statement branding | Digital-first card/payment phrasing |
| **Discovery Bank** | "Discovery", Discovery Bank branding | Card-centric transaction descriptions |
| **Investec** | "Investec" | Private banking statement formatting |
| **Old Mutual** | "Old Mutual" | Old Mutual branding and statement wording |

The pipeline will:
1. Auto-detect the bank from PDF content (or use `--bank` flag)
2. Extract transactions using bank-specific table/text parsers
3. Extract personal info (account holder, address, postal code, account type) into `statement_info`
4. Seed classification rules into the DB on first run
5. Classify transactions via regex, then AI for any remaining unclassified lines
6. Move processed PDFs to `input/processed/`, failed ones to `input/failed/`

Verify results:

```bash
sqlite3 teststatement/tmp/test.db "SELECT bank_name, account_number FROM statements;"
sqlite3 teststatement/tmp/test.db "SELECT account_holder, address_line1, postal_code FROM statement_info;"
sqlite3 teststatement/tmp/test.db "SELECT count(*) as lines, sum(case when category is not null then 1 else 0 end) as classified FROM statement_lines;"
```

---

## Full Validation (both approaches)

```bash
# 1. Unit + automated integration tests
python3 -m pytest tests/ -v

# 2. Real PDF test (manually inspect DB results)
python3 main.py --pdf-dir teststatement/input --db-path teststatement/tmp/test.db --dry-run
```

## Document Processor Contract Check

When validating the .NET Document Processor integration against this service, use the same host port described in `docker-compose.yml` and the README service section.

Expected host endpoints:

- `http://localhost:8001/api/upload`
- `http://localhost:8001/api/jobs/{job_id}/status`
- `http://localhost:8001/api/jobs/{job_id}`

Quick manual validation flow:

1. Start the Bank Statement Processor backend on host port `8001`
2. Submit a PDF through the FastAPI upload endpoint or through Document Processor
3. Confirm the upload response returns a `job_id`
4. Poll the status endpoint until it reports `completed`
5. Fetch the job detail and verify the `result.lines` payload contains the extracted transaction rows expected by Document Processor

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `No PDF files in input` | Place PDF files in `teststatement/input/` |
| All files go to `failed/` | Check `logs/pipeline.log` — PDFs may not match any bank profile |
| Low classification % | Expected on first run; run with AI enabled to auto-generate regex rules |
| `ModuleNotFoundError` | Run `pip3 install -r requirements.txt` from project root |
