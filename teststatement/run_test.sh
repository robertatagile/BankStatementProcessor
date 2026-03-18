#!/usr/bin/env bash
# ============================================================
# Integration Test Runner
# ============================================================
# Runs the bank statement processor against real PDFs in the
# teststatement/input folder.
#
# Usage:
#   cd teststatement && ./run_test.sh
#   OR from project root:  bash teststatement/run_test.sh
# ============================================================

set -euo pipefail

# ---- Resolve paths relative to this script ----
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
INPUT_DIR="$SCRIPT_DIR/input"
PROCESSED_DIR="$INPUT_DIR/processed"
FAILED_DIR="$INPUT_DIR/failed"
DB_PATH="$SCRIPT_DIR/test_statements.db"
RULES_PATH="$PROJECT_ROOT/config/classification_rules.json"

# ---- Colours ----
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${CYAN}============================================================${NC}"
echo -e "${CYAN}  Bank Statement Processor — Integration Test${NC}"
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

# Remove old test database so each run starts fresh
rm -f "$DB_PATH"

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

# ---- Run the processor ----
echo -e "${CYAN}Running pipeline (dry-run — no AI stage)...${NC}"
echo ""

cd "$PROJECT_ROOT"
python3 main.py \
    --pdf-dir "$INPUT_DIR" \
    --db-path "$DB_PATH" \
    --rules-path "$RULES_PATH" \
    --dry-run

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
echo -e "${CYAN}  Test complete${NC}"
echo -e "${CYAN}============================================================${NC}"
