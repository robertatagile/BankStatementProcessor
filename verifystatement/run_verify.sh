#!/usr/bin/env bash
# ============================================================
# Vision Verification Tool Runner (Bash)
# ============================================================
# Runs verify.py against PDFs in verifystatement/input or a
# specified path.
#
# Usage:
#   ./verifystatement/run_verify.sh
#   ./verifystatement/run_verify.sh --pdf-file path/to/file.pdf
#   ./verifystatement/run_verify.sh --auto-fix
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

section() {
    echo "============================================================"
    echo "  $1"
    echo "============================================================"
}

# Find Python
PYTHON=""
for candidate in python3 python py; do
    if command -v "$candidate" &>/dev/null; then
        PYTHON="$candidate"
        break
    fi
done

if [ -z "$PYTHON" ]; then
    echo "ERROR: Python not found in PATH" >&2
    exit 1
fi

# Check for API key
if [ -z "${ANTHROPIC_API_KEY:-}" ]; then
    echo "ERROR: ANTHROPIC_API_KEY environment variable not set" >&2
    exit 1
fi

section "Vision Verification Tool"
echo ""

# If no arguments given, default to verifystatement/input
if [ $# -eq 0 ]; then
    DEFAULT_INPUT="$SCRIPT_DIR/input"
    PDF_COUNT=$(find "$DEFAULT_INPUT" -maxdepth 1 -name '*.pdf' 2>/dev/null | wc -l)
    if [ "$PDF_COUNT" -eq 0 ]; then
        echo "No PDF files in $DEFAULT_INPUT - nothing to verify."
        exit 0
    fi
    echo "PDF dir  : $DEFAULT_INPUT ($PDF_COUNT files)"
    set -- --pdf-dir "$DEFAULT_INPUT"
fi

echo "Running verification..."
echo ""

cd "$PROJECT_ROOT"
"$PYTHON" verifystatement/verify.py "$@"

echo ""
section "Verification complete"
