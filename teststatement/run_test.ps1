#!/usr/bin/env pwsh
# ============================================================
# Integration Test Runner (Windows PowerShell)
# ============================================================
# Runs the bank statement processor against real PDFs in the
# teststatement/input folder.
#
# Usage:
#   pwsh -File .\teststatement\run_test.ps1
#   OR from inside teststatement\
#   .\run_test.ps1
# ============================================================

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Write-Section {
    param(
        [string]$Text
    )

    Write-Host '============================================================' -ForegroundColor Cyan
    Write-Host "  $Text" -ForegroundColor Cyan
    Write-Host '============================================================' -ForegroundColor Cyan
}

function Get-PythonCommand {
    foreach ($candidate in @(
        @{ Name = 'py'; Args = @('-3') },
        @{ Name = 'python'; Args = @() },
        @{ Name = 'python3'; Args = @() }
    )) {
        if (Get-Command $candidate.Name -ErrorAction SilentlyContinue) {
            return $candidate
        }
    }

    throw 'Python was not found in PATH. Install Python 3 or use the py launcher.'
}

function Invoke-Python {
    param(
        [hashtable]$Python,
        [string[]]$Arguments
    )

    & $Python.Name @($Python.Args + $Arguments)
    if ($LASTEXITCODE -ne 0) {
        throw "Python command failed with exit code $LASTEXITCODE"
    }
}

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
$InputDir = Join-Path $ScriptDir 'input'
$ProcessedDir = Join-Path $InputDir 'processed'
$FailedDir = Join-Path $InputDir 'failed'
$DbPath = Join-Path $ScriptDir 'test_statements.db'
$RulesPath = Join-Path $ProjectRoot 'config/classification_rules.json'
$Python = Get-PythonCommand

Write-Section 'Bank Statement Processor - Integration Test'
Write-Host ''

if (-not (Test-Path -LiteralPath $InputDir -PathType Container)) {
    Write-Host "ERROR: input directory not found at $InputDir" -ForegroundColor Red
    exit 1
}

Write-Host 'Resetting previous run...' -ForegroundColor Cyan

if (Test-Path -LiteralPath $ProcessedDir -PathType Container) {
    Get-ChildItem -LiteralPath $ProcessedDir -Filter '*.pdf' -File | ForEach-Object {
        Move-Item -LiteralPath $_.FullName -Destination $InputDir -Force
    }
}

if (Test-Path -LiteralPath $FailedDir -PathType Container) {
    Get-ChildItem -LiteralPath $FailedDir -Filter '*.pdf' -File | ForEach-Object {
        Move-Item -LiteralPath $_.FullName -Destination $InputDir -Force
    }
}

if (Test-Path -LiteralPath $DbPath -PathType Leaf) {
    Remove-Item -LiteralPath $DbPath -Force
}

$PdfCount = (Get-ChildItem -LiteralPath $InputDir -Filter '*.pdf' -File | Measure-Object).Count

if ($PdfCount -eq 0) {
    Write-Host "No PDF files in $InputDir - nothing to process." -ForegroundColor Yellow
    exit 0
}

Write-Host "Input directory : $InputDir" -ForegroundColor Yellow
Write-Host "PDF files found : $PdfCount" -ForegroundColor Yellow
Write-Host "Database        : $DbPath" -ForegroundColor Yellow
Write-Host "Rules           : $RulesPath" -ForegroundColor Yellow
Write-Host ''

Write-Host 'Running pipeline (dry-run - no AI stage)...' -ForegroundColor Cyan
Write-Host ''

Push-Location $ProjectRoot
try {
    Invoke-Python -Python $Python -Arguments @(
        'main.py',
        '--pdf-dir', $InputDir,
        '--db-path', $DbPath,
        '--rules-path', $RulesPath,
        '--dry-run'
    )
}
finally {
    Pop-Location
}

Write-Host ''
Write-Section 'Results'

$PassCount = 0
$FailCount = 0

if (Test-Path -LiteralPath $ProcessedDir -PathType Container) {
    $PassCount = (Get-ChildItem -LiteralPath $ProcessedDir -Filter '*.pdf' -File | Measure-Object).Count
}

if (Test-Path -LiteralPath $FailedDir -PathType Container) {
    $FailCount = (Get-ChildItem -LiteralPath $FailedDir -Filter '*.pdf' -File | Measure-Object).Count
}

Write-Host "  Processed : $PassCount" -ForegroundColor Green
Write-Host "  Failed    : $FailCount" -ForegroundColor Red
Write-Host ''

if ((Test-Path -LiteralPath $ProcessedDir -PathType Container) -and $PassCount -gt 0) {
    Write-Host 'Processed files:' -ForegroundColor Green
    Get-ChildItem -LiteralPath $ProcessedDir -Filter '*.pdf' -File | ForEach-Object {
        Write-Host "  [OK] $($_.Name)" -ForegroundColor Green
    }
    Write-Host ''
}

if ((Test-Path -LiteralPath $FailedDir -PathType Container) -and $FailCount -gt 0) {
    Write-Host 'Failed files:' -ForegroundColor Red
    Get-ChildItem -LiteralPath $FailedDir -Filter '*.pdf' -File | ForEach-Object {
        Write-Host "  [FAIL] $($_.Name)" -ForegroundColor Red
    }
    Write-Host ''
}

if (Test-Path -LiteralPath $DbPath -PathType Leaf) {
    Write-Host 'Database summary:' -ForegroundColor Cyan
    $DbSummaryScript = @"
import sqlite3
import sys

db = sqlite3.connect(sys.argv[1])
cur = db.cursor()
stmts = cur.execute('SELECT COUNT(*) FROM statements').fetchone()[0]
lines = cur.execute('SELECT COUNT(*) FROM statement_lines').fetchone()[0]
classified = cur.execute("SELECT COUNT(*) FROM statement_lines WHERE category IS NOT NULL").fetchone()[0]
unclassified = lines - classified
print(f'  Statements      : {stmts}')
print(f'  Total lines     : {lines}')
print(f'  Classified      : {classified}')
print(f'  Unclassified    : {unclassified}')
if lines > 0:
    print(f'  Classification% : {classified/lines*100:.1f}%')
print()
print('  Top categories:')
for cat, cnt in cur.execute("""
    SELECT COALESCE(category, 'Unclassified'), COUNT(*)
    FROM statement_lines GROUP BY category ORDER BY COUNT(*) DESC LIMIT 10
""").fetchall():
    print(f'    {cat:<25} {cnt}')
db.close()
"@

    Invoke-Python -Python $Python -Arguments @('-c', $DbSummaryScript, $DbPath)
}

Write-Host ''
Write-Section 'Test complete'