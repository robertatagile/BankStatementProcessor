#!/usr/bin/env bash
# ============================================================
# Bulk Integration Test Runner — Parallel Processing
# ============================================================
# Processes PDFs in parallel batches (default: 10 workers).
# Each batch gets its own temp directory and database to avoid
# SQLite write contention. Results are merged at the end.
#
# Usage:
#   cd teststatement && ./run_bulk_test.sh
#   OR from project root:  bash teststatement/run_bulk_test.sh
#
# Environment variables:
#   WORKERS=10   Number of parallel batches (default: 10)
# ============================================================

set -euo pipefail

# ---- Configuration ----
WORKERS="${WORKERS:-10}"

# ---- Resolve paths relative to this script ----
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
INPUT_DIR="$SCRIPT_DIR/input"
PROCESSED_DIR="$INPUT_DIR/processed"
FAILED_DIR="$INPUT_DIR/failed"
DB_PATH="$SCRIPT_DIR/test_statements.db"
RULES_PATH="$PROJECT_ROOT/config/classification_rules.json"
TMP_DIR="$SCRIPT_DIR/tmp_bulk"

# ---- Colours ----
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${CYAN}============================================================${NC}"
echo -e "${CYAN}  Bank Statement Processor — Bulk Integration Test${NC}"
echo -e "${CYAN}  Workers: ${YELLOW}$WORKERS${CYAN} parallel batches${NC}"
echo -e "${CYAN}============================================================${NC}"
echo ""

# ---- Pre-flight checks ----
if [ ! -d "$INPUT_DIR" ]; then
    echo -e "${RED}ERROR: input directory not found at $INPUT_DIR${NC}"
    exit 1
fi

# ---- Reset previous run (move processed/failed back to input) ----
echo -e "${CYAN}Resetting previous run...${NC}"
if [ -d "$PROCESSED_DIR" ]; then
    for f in "$PROCESSED_DIR"/*.pdf; do
        [ -e "$f" ] && mv "$f" "$INPUT_DIR/"
    done
fi
if [ -d "$FAILED_DIR" ]; then
    for f in "$FAILED_DIR"/*.pdf; do
        [ -e "$f" ] && mv "$f" "$INPUT_DIR/"
    done
fi

# Remove old test database and temp directory
rm -f "$DB_PATH"
rm -rf "$TMP_DIR"

# Recount after reset
PDF_COUNT=$(find "$INPUT_DIR" -maxdepth 1 -name "*.pdf" | wc -l | tr -d ' ')

if [ "$PDF_COUNT" -eq 0 ]; then
    echo -e "${YELLOW}No PDF files in $INPUT_DIR — nothing to process.${NC}"
    exit 0
fi

echo -e "Input directory : ${YELLOW}$INPUT_DIR${NC}"
echo -e "PDF files found : ${YELLOW}$PDF_COUNT${NC}"
echo -e "Database        : ${YELLOW}$DB_PATH${NC}"
echo -e "Rules           : ${YELLOW}$RULES_PATH${NC}"
echo ""

# ---- Distribute PDFs round-robin into batch directories ----
echo -e "${CYAN}Distributing $PDF_COUNT PDFs across $WORKERS batches...${NC}"

# Create batch directories
for i in $(seq -w 1 "$WORKERS"); do
    mkdir -p "$TMP_DIR/batch_$i"
done

# Round-robin assignment via symlinks
batch_idx=1
for pdf in "$INPUT_DIR"/*.pdf; do
    [ -e "$pdf" ] || continue
    padded=$(printf "%0*d" "${#WORKERS}" "$batch_idx")
    ln -s "$pdf" "$TMP_DIR/batch_$padded/$(basename "$pdf")"
    batch_idx=$(( batch_idx % WORKERS + 1 ))
done

# Show batch sizes
for d in "$TMP_DIR"/batch_*; do
    count=$(find "$d" -maxdepth 1 -name "*.pdf" | wc -l | tr -d ' ')
    echo -e "  $(basename "$d"): ${YELLOW}$count${NC} PDFs"
done
echo ""

# ---- Parallel execution ----
echo -e "${CYAN}Running $WORKERS pipeline workers (dry-run, no AI, no OCR)...${NC}"
echo ""

START_TIME=$SECONDS
PIDS=()

cd "$PROJECT_ROOT"

for d in "$TMP_DIR"/batch_*; do
    batch_name=$(basename "$d")
    # Skip empty batches (fewer PDFs than workers)
    pdf_count=$(find "$d" -maxdepth 1 -name "*.pdf" | wc -l | tr -d ' ')
    if [ "$pdf_count" -eq 0 ]; then
        continue
    fi
    python3 main.py \
        --pdf-dir "$d" \
        --db-path "$TMP_DIR/${batch_name}.db" \
        --rules-path "$RULES_PATH" \
        --dry-run \
        --no-ocr \
        > "$TMP_DIR/${batch_name}.log" 2>&1 &
    PIDS+=($!)
done

echo -e "  Launched ${YELLOW}${#PIDS[@]}${NC} background workers (PIDs: ${PIDS[*]})"
echo -e "  Waiting for completion..."

# Show progress while waiting (disable set -e for the progress loop)
set +e
while true; do
    # Count how many workers are still running
    ALIVE=0
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            ALIVE=$((ALIVE + 1))
        fi
    done
    [ "$ALIVE" -eq 0 ] && break

    # Count processed + failed across all batches
    DONE=$(find "$TMP_DIR"/batch_*/processed "$TMP_DIR"/batch_*/failed -name "*.pdf" 2>/dev/null | wc -l | tr -d ' ')
    ELAPSED_SO_FAR=$(( SECONDS - START_TIME ))
    printf "\r  Progress: %d/%d PDFs | %d workers active | %ds elapsed" \
        "$DONE" "$PDF_COUNT" "$ALIVE" "$ELAPSED_SO_FAR"
    sleep 2
done

# Collect exit codes
FAILURES=0
for pid in "${PIDS[@]}"; do
    if ! wait "$pid"; then
        FAILURES=$((FAILURES + 1))
    fi
done
set -e

ELAPSED=$(( SECONDS - START_TIME ))
printf "\r%-80s\n" ""  # clear progress line
echo -e "  All workers finished in ${YELLOW}${ELAPSED}s${NC} (${FAILURES} worker failures)"
echo ""

# ---- Consolidate file movements ----
echo -e "${CYAN}Consolidating results...${NC}"
mkdir -p "$PROCESSED_DIR" "$FAILED_DIR"

for d in "$TMP_DIR"/batch_*; do
    [ -d "$d" ] || continue

    # Move originals for processed symlinks
    if [ -d "$d/processed" ]; then
        for link in "$d/processed"/*.pdf; do
            [ -e "$link" ] || continue
            # Resolve the symlink to find the original file
            original=$(readlink "$link" 2>/dev/null || true)
            if [ -n "$original" ] && [ -e "$original" ]; then
                mv "$original" "$PROCESSED_DIR/"
            fi
        done
    fi

    # Move originals for failed symlinks
    if [ -d "$d/failed" ]; then
        for link in "$d/failed"/*.pdf; do
            [ -e "$link" ] || continue
            original=$(readlink "$link" 2>/dev/null || true)
            if [ -n "$original" ] && [ -e "$original" ]; then
                mv "$original" "$FAILED_DIR/"
            fi
        done
    fi
done

# ---- Merge databases ----
echo -e "${CYAN}Merging batch databases...${NC}"

python3 -c "
import sqlite3
import glob
import sys
import os

sys.path.insert(0, '$PROJECT_ROOT')
from src.models.database import init_db
from src.pipeline.regex_classifier import seed_classification_rules

# Initialise final DB with schema + classification rules
db_path = '$DB_PATH'
rules_path = '$RULES_PATH'
session_factory = init_db(db_path)
seed_classification_rules(session_factory, rules_path)

final = sqlite3.connect(db_path)
final.execute('PRAGMA journal_mode=WAL')

batch_dbs = sorted(glob.glob('$TMP_DIR/batch_*.db'))
total_stmts = 0
total_lines = 0
total_info = 0

for batch_db in batch_dbs:
    if not os.path.exists(batch_db):
        continue
    src = sqlite3.connect(batch_db)

    # Read statements from this batch
    stmts = src.execute(
        'SELECT id, bank_name, account_number, statement_date, '
        'opening_balance, closing_balance, file_path, created_at '
        'FROM statements'
    ).fetchall()

    for old_row in stmts:
        old_id = old_row[0]
        # Insert statement with new auto-increment ID
        cur = final.execute(
            'INSERT INTO statements '
            '(bank_name, account_number, statement_date, '
            'opening_balance, closing_balance, file_path, created_at) '
            'VALUES (?, ?, ?, ?, ?, ?, ?)',
            old_row[1:],
        )
        new_id = cur.lastrowid
        total_stmts += 1

        # Copy statement_lines with remapped statement_id
        lines = src.execute(
            'SELECT date, description, amount, balance, transaction_type, '
            'category, classification_method, matched_rule_id, matched_pattern, '
            'confidence, classification_reason, created_at '
            'FROM statement_lines WHERE statement_id = ?',
            (old_id,),
        ).fetchall()
        for line in lines:
            final.execute(
                'INSERT INTO statement_lines '
                '(statement_id, date, description, amount, balance, transaction_type, '
                'category, classification_method, matched_rule_id, matched_pattern, '
                'confidence, classification_reason, created_at) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (new_id, *line),
            )
            total_lines += 1

        # Copy statement_info with remapped statement_id
        infos = src.execute(
            'SELECT account_number, account_holder, address_line1, address_line2, '
            'address_line3, postal_code, account_type, branch_code, created_at '
            'FROM statement_info WHERE statement_id = ?',
            (old_id,),
        ).fetchall()
        for info in infos:
            final.execute(
                'INSERT INTO statement_info '
                '(statement_id, account_number, account_holder, address_line1, '
                'address_line2, address_line3, postal_code, account_type, '
                'branch_code, created_at) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (new_id, *info),
            )
            total_info += 1

    src.close()

final.commit()
final.close()
print(f'  Merged: {total_stmts} statements, {total_lines} lines, {total_info} info records')
"

# ---- Cleanup temp directory ----
rm -rf "$TMP_DIR"

echo ""

# ---- Report results ----
echo -e "${CYAN}============================================================${NC}"
echo -e "${CYAN}  Results${NC}"
echo -e "${CYAN}============================================================${NC}"

PASS_COUNT=0
FAIL_COUNT=0

if [ -d "$PROCESSED_DIR" ]; then
    PASS_COUNT=$(find "$PROCESSED_DIR" -maxdepth 1 -name "*.pdf" | wc -l | tr -d ' ')
fi
if [ -d "$FAILED_DIR" ]; then
    FAIL_COUNT=$(find "$FAILED_DIR" -maxdepth 1 -name "*.pdf" | wc -l | tr -d ' ')
fi

echo -e "  ${GREEN}Processed : $PASS_COUNT${NC}"
echo -e "  ${RED}Failed    : $FAIL_COUNT${NC}"
echo -e "  ${YELLOW}Elapsed   : ${ELAPSED}s${NC}"
echo ""

if [ -d "$PROCESSED_DIR" ] && [ "$PASS_COUNT" -gt 0 ]; then
    echo -e "${GREEN}Processed files:${NC}"
    for f in "$PROCESSED_DIR"/*.pdf; do
        [ -e "$f" ] && echo -e "  ${GREEN}✓ $(basename "$f")${NC}"
    done
    echo ""
fi

if [ -d "$FAILED_DIR" ] && [ "$FAIL_COUNT" -gt 0 ]; then
    echo -e "${RED}Failed files:${NC}"
    for f in "$FAILED_DIR"/*.pdf; do
        [ -e "$f" ] && echo -e "  ${RED}✗ $(basename "$f")${NC}"
    done
    echo ""
fi

# ---- Quick DB summary ----
if [ -f "$DB_PATH" ]; then
    echo -e "${CYAN}Database summary:${NC}"
    python3 -c "
import sqlite3
db = sqlite3.connect('$DB_PATH')
cur = db.cursor()
stmts = cur.execute('SELECT COUNT(*) FROM statements').fetchone()[0]
lines = cur.execute('SELECT COUNT(*) FROM statement_lines').fetchone()[0]
classified = cur.execute(\"SELECT COUNT(*) FROM statement_lines WHERE category IS NOT NULL\").fetchone()[0]
unclassified = lines - classified
print(f'  Statements      : {stmts}')
print(f'  Total lines     : {lines}')
print(f'  Classified      : {classified}')
print(f'  Unclassified    : {unclassified}')
if lines > 0:
    print(f'  Classification% : {classified/lines*100:.1f}%')
print()
print('  Top categories:')
for cat, cnt in cur.execute(\"\"\"
    SELECT COALESCE(category, 'Unclassified'), COUNT(*)
    FROM statement_lines GROUP BY category ORDER BY COUNT(*) DESC LIMIT 10
\"\"\").fetchall():
    print(f'    {cat:<25} {cnt}')
db.close()
"
fi

echo ""
echo -e "${CYAN}============================================================${NC}"
echo -e "${CYAN}  Bulk test complete — ${YELLOW}${ELAPSED}s${CYAN} with ${YELLOW}$WORKERS${CYAN} workers${NC}"
echo -e "${CYAN}============================================================${NC}"
