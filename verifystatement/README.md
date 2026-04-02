# Vision Verification Tool

Verifies bank statement PDF extraction accuracy by sending each page's image + extracted text to Claude for comparison. Optionally auto-fixes bank profiles when issues are found.

## Prerequisites

- Python 3.10+
- `ANTHROPIC_API_KEY` environment variable set
- Project dependencies installed (`pip install -r requirements.txt`)

## Quick Start

```bash
# Verify a single PDF
python verifystatement/verify.py --pdf-file path/to/statement.pdf

# Verify all PDFs in the default input directory
python verifystatement/verify.py --pdf-dir verifystatement/input

# Verify with auto-fix enabled (modifies bank profiles)
python verifystatement/verify.py --pdf-file path/to/statement.pdf --auto-fix
```

### Shell Wrappers

```powershell
# PowerShell
pwsh -File .\verifystatement\run_verify.ps1
pwsh -File .\verifystatement\run_verify.ps1 -PdfFile path\to\statement.pdf
pwsh -File .\verifystatement\run_verify.ps1 -AutoFix
```

```bash
# Bash
./verifystatement/run_verify.sh
./verifystatement/run_verify.sh --pdf-file path/to/statement.pdf
./verifystatement/run_verify.sh --auto-fix
```

## CLI Arguments

| Argument | Description | Default |
|---|---|---|
| `--pdf-file PATH` | Single PDF to verify | — |
| `--pdf-dir PATH` | Directory of PDFs to verify | — |
| `--auto-fix` | Enable auto-fix (legacy compatibility) | off |
| `--support-loop` | Run the task-driven support loop | off |
| `--resume` | Resume the most recent support-loop run | off |
| `--inspect-run PATH` | Inspect state of a run directory | — |
| `--archive-run PATH` | Archive a completed run | — |
| `--list-runs` | List all support-loop runs | — |
| `--report-dir PATH` | Directory for JSON reports | `verifystatement/reports` |
| `--model MODEL` | Claude model to use | `claude-sonnet-4-20250514` |
| `--max-attempts N` | Max fix iterations | `10` |

## How It Works

### Phase 1: Verification

1. Opens the PDF with pdfplumber
2. Auto-detects the bank profile (or falls back to "Generic")
3. For each page:
   - Extracts transaction lines using the bank profile (with OCR enabled to match the production extraction path, including FNB OCR fee-line supplementation, multiline description merging, and transaction-type inference)
   - Renders the page as a 300 DPI image
   - Sends image + extracted text to Claude
   - Claude compares and reports: missing, incorrect, or extra transactions
4. Aggregates discrepancies into a JSON report

### Phase 2: Auto-Fix (when `--auto-fix` is enabled)

**Unknown bank (Generic profile detected) → creates a new profile:**

1. Sends page images (up to 10 pages), raw text, the BankProfile dataclass, SA helpers, and an example profile to Claude
2. Claude generates a new profile module with detection keywords, date formats, patterns, and column mappings
3. Writes the module to `src/profiles/banks/` and registers it in `__init__.py`
4. Re-verifies with the new profile; iterates up to `--max-attempts` times
5. If all attempts fail, rolls back the generated profile and registration

**Known bank → fixes existing profile (regression-safe):**

1. Snapshots the current profile code as a backup
2. Runs existing regression tests to establish a passing baseline — **aborts if baseline tests fail or no tests exist**
3. Sends the profile code + discrepancies + raw text to Claude for a fix
4. Applies the fix, then runs ALL regression tests for that bank
5. If any test fails: reverts, feeds failure details back to Claude, retries
6. If tests pass: re-verifies the new PDF
7. If still issues: loops with remaining discrepancies
8. If max attempts exhausted: reverts to original profile

## Output

Reports are saved as JSON in `verifystatement/reports/` with the format:

```
{stem}_verify_{timestamp}.json
```

Each report contains per-page results and aggregated discrepancies (missing, incorrect, extra transactions).

## Support Loop Mode

The support loop (`--support-loop`) is the recommended way to fix extraction issues. It decomposes work into small deterministic tasks with persistent state:

```bash
# Run the support loop on a failing statement
python verifystatement/verify.py --pdf-file path/to/statement.pdf --support-loop

# Resume a failed run
python verifystatement/verify.py --pdf-file path/to/statement.pdf --support-loop --resume

# Inspect a run
python verifystatement/verify.py --inspect-run verifystatement/runs/my_statement_20260401_120000

# List all runs
python verifystatement/verify.py --list-runs

# Archive a completed run
python verifystatement/verify.py --archive-run verifystatement/runs/my_statement_20260401_120000
```

### Loop Stages

1. **Discover** — Collect evidence (page text, tables, layout signature) and verify via Claude
2. **Classify** — Select repair strategy: `profile_patch`, `extractor_patch`, `new_profile`, or `manual_review`
3. **Repair** — Execute the strategy with task-sized prompts and minimum context
4. **Validate** — Re-verify target PDF, run bank regression tests, optionally run cross-bank smoke tests
5. **Learn** — Persist resolution learnings for future support iterations

### Strategy Selection

| Condition | Strategy |
|---|---|
| Generic detection + stable branding | `new_profile` |
| Known bank + collapsed/no tables | `extractor_patch` |
| Known bank + similar layout | `profile_patch` |
| 3+ failed `profile_patch` attempts | escalate to `extractor_patch` |
| 5+ total failures / ambiguous | `manual_review` |

### Validation Gates

- **Target PDF verification** — zero discrepancies required
- **Bank regression tests** — existing tests must still pass
- **Smoke tests** (extractor changes only) — all bank fixtures tested

### Run Artifacts

Each run creates a directory under `verifystatement/runs/` with:

```
verifystatement/runs/{pdf_stem}_{timestamp}/
├── task.json                  # Persistent task state
├── evidence.json              # Raw evidence (page text, tables, layout signature)
├── verification_report.json   # Claude verification results
├── summary.json               # Final summary
└── attempts/
    ├── attempt-1.json         # Per-attempt record with gates
    ├── attempt-2.json
    └── ...
```

## Directory Structure

```
verifystatement/
├── verify.py          # CLI entrypoint (verification + auto-fix + support-loop)
├── support_loop.py    # Task-driven orchestrator
├── task_state.py      # Persistent state model and run directory management
├── evidence.py        # Evidence collection (page text, tables, layout signature)
├── strategy.py        # Strategy selection (profile_patch, extractor_patch, etc.)
├── repair.py          # Repair execution with task-sized prompts
├── validation.py      # Validation gates (target PDF, regressions, smoke tests)
├── learnings.py       # Learning persistence per resolved layout
├── run_verify.ps1     # PowerShell wrapper
├── run_verify.sh      # Bash wrapper
├── input/             # Drop PDFs here for batch verification
├── reports/           # JSON verification reports (git-ignored)
├── runs/              # Support-loop run artifacts (git-ignored)
└── learnings/         # Persisted resolution learnings
```
