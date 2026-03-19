#!/usr/bin/env pwsh
# ============================================================
# Bulk Integration Test Runner (Windows PowerShell)
# ============================================================
# Processes PDFs in parallel batches (default: 10 workers).
# Each batch gets its own temp directory and database to avoid
# SQLite write contention. Results are merged at the end.
#
# Usage:
#   pwsh -File .\teststatement\run_bulk_test.ps1
#   OR from inside teststatement\
#   .\run_bulk_test.ps1
#
# Environment variables:
#   WORKERS=10   Number of parallel batches (default: 10)
# ============================================================

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Write-Section {
    param(
        [string]$Text
    )

    Write-Host '============================================================' -ForegroundColor Cyan
    Write-Host ('  {0}' -f $Text) -ForegroundColor Cyan
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
        throw ('Python command failed with exit code {0}' -f $LASTEXITCODE)
    }
}

function Get-PdfCount {
    param(
        [string]$Path
    )

    if (-not (Test-Path -LiteralPath $Path -PathType Container)) {
        return 0
    }

    return (Get-ChildItem -LiteralPath $Path -Filter '*.pdf' -File | Measure-Object).Count
}

$WorkerSetting = if ([string]::IsNullOrWhiteSpace($env:WORKERS)) { '10' } else { $env:WORKERS }
[int]$WorkerCount = 0
if (-not [int]::TryParse($WorkerSetting, [ref]$WorkerCount) -or $WorkerCount -lt 1) {
    throw ('WORKERS must be a positive integer. Current value: {0}' -f $WorkerSetting)
}

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
$InputDir = Join-Path $ScriptDir 'input'
$ProcessedDir = Join-Path $InputDir 'processed'
$FailedDir = Join-Path $InputDir 'failed'
$DbPath = Join-Path $ScriptDir 'test_statements.db'
$RulesPath = Join-Path $ProjectRoot 'config/classification_rules.json'
$TmpDir = Join-Path $ScriptDir 'tmp_bulk'
$Python = Get-PythonCommand
$BatchWidth = [Math]::Max(2, $WorkerCount.ToString().Length)

Write-Section 'Bank Statement Processor - Bulk Integration Test'
Write-Host ('  Workers: {0} parallel batches' -f $WorkerCount) -ForegroundColor Cyan
Write-Host ''

if (-not (Test-Path -LiteralPath $InputDir -PathType Container)) {
    Write-Host ('ERROR: input directory not found at {0}' -f $InputDir) -ForegroundColor Red
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

if (Test-Path -LiteralPath $TmpDir) {
    Remove-Item -LiteralPath $TmpDir -Recurse -Force
}

$PdfFiles = Get-ChildItem -LiteralPath $InputDir -Filter '*.pdf' -File | Sort-Object Name
$PdfCount = $PdfFiles.Count

if ($PdfCount -eq 0) {
    Write-Host ('No PDF files in {0} - nothing to process.' -f $InputDir) -ForegroundColor Yellow
    exit 0
}

Write-Host ('Input directory : {0}' -f $InputDir) -ForegroundColor Yellow
Write-Host ('PDF files found : {0}' -f $PdfCount) -ForegroundColor Yellow
Write-Host ('Database        : {0}' -f $DbPath) -ForegroundColor Yellow
Write-Host ('Rules           : {0}' -f $RulesPath) -ForegroundColor Yellow
Write-Host ''

Write-Host ('Distributing {0} PDFs across {1} batches...' -f $PdfCount, $WorkerCount) -ForegroundColor Cyan

$BatchDirs = @()
for ($index = 1; $index -le $WorkerCount; $index++) {
    $batchName = ('batch_{0:d' + $BatchWidth + '}') -f $index
    $batchDir = Join-Path $TmpDir $batchName
    New-Item -ItemType Directory -Path $batchDir -Force | Out-Null
    $BatchDirs += [PSCustomObject]@{
        Name = $batchName
        Path = $batchDir
    }
}

$batchIndex = 0
foreach ($pdf in $PdfFiles) {
    $targetBatch = $BatchDirs[$batchIndex]
    Copy-Item -LiteralPath $pdf.FullName -Destination (Join-Path $targetBatch.Path $pdf.Name) -Force
    $batchIndex = ($batchIndex + 1) % $BatchDirs.Count
}

foreach ($batch in $BatchDirs) {
    $count = Get-PdfCount -Path $batch.Path
    Write-Host ('  {0}: {1} PDFs' -f $batch.Name, $count) -ForegroundColor Yellow
}
Write-Host ''

Write-Host ('Running {0} pipeline workers (dry-run, no AI, no OCR)...' -f $WorkerCount) -ForegroundColor Cyan
Write-Host ''

$StartTime = Get-Date
$Jobs = @()

foreach ($batch in $BatchDirs) {
    $pdfCountInBatch = Get-PdfCount -Path $batch.Path
    if ($pdfCountInBatch -eq 0) {
        continue
    }

    $logPath = Join-Path $TmpDir ($batch.Name + '.log')
    $job = Start-Job -Name $batch.Name -ArgumentList @(
        $ProjectRoot,
        $Python.Name,
        [string[]]$Python.Args,
        [string[]]@(
            'main.py',
            '--pdf-dir', $batch.Path,
            '--db-path', (Join-Path $TmpDir ($batch.Name + '.db')),
            '--rules-path', $RulesPath,
            '--dry-run',
            '--no-ocr'
        ),
        $logPath
    ) -ScriptBlock {
        param(
            [string]$ProjectRoot,
            [string]$PythonName,
            [string[]]$PythonPrefixArgs,
            [string[]]$MainArgs,
            [string]$LogPath
        )

        Set-Location $ProjectRoot
        & $PythonName @($PythonPrefixArgs + $MainArgs) *> $LogPath
        if ($LASTEXITCODE -ne 0) {
            throw ('Python command failed with exit code {0}' -f $LASTEXITCODE)
        }
    }

    $Jobs += $job
}

$JobIds = if ($Jobs.Count -gt 0) { ($Jobs | ForEach-Object Id) -join ', ' } else { '' }
Write-Host ('  Launched {0} background workers (Job IDs: {1})' -f $Jobs.Count, $JobIds)
Write-Host '  Waiting for completion...'

while ($true) {
    $alive = @($Jobs | Where-Object { $_.State -in @('NotStarted', 'Running') }).Count
    if ($alive -eq 0) {
        break
    }

    $done = 0
    foreach ($batch in $BatchDirs) {
        $done += Get-PdfCount -Path (Join-Path $batch.Path 'processed')
        $done += Get-PdfCount -Path (Join-Path $batch.Path 'failed')
    }

    $elapsedSoFar = [int]((Get-Date) - $StartTime).TotalSeconds
    Write-Host ("`r  Progress: {0}/{1} PDFs | {2} workers active | {3}s elapsed" -f $done, $PdfCount, $alive, $elapsedSoFar) -NoNewline
    Start-Sleep -Seconds 2
}

$Failures = 0
$FailedJobs = @()
foreach ($job in $Jobs) {
    Wait-Job -Job $job | Out-Null
    if ($job.State -ne 'Completed') {
        $Failures++
        $FailedJobs += $job
    }

    try {
        Receive-Job -Job $job -ErrorAction SilentlyContinue | Out-Null
    }
    catch {
    }
}

$Elapsed = [int]((Get-Date) - $StartTime).TotalSeconds
Write-Host ("`r{0}`r" -f (' ' * 90)) -NoNewline
Write-Host ('  All workers finished in {0}s ({1} worker failures)' -f $Elapsed, $Failures)
Write-Host ''

if ($FailedJobs.Count -gt 0) {
    Write-Host 'Worker failures:' -ForegroundColor Red
    foreach ($job in $FailedJobs) {
        Write-Host ('  {0} -> {1}' -f $job.Name, (Join-Path $TmpDir ($job.Name + '.log'))) -ForegroundColor Red
    }
    Write-Host ''
}

Write-Host 'Consolidating results...' -ForegroundColor Cyan
New-Item -ItemType Directory -Path $ProcessedDir -Force | Out-Null
New-Item -ItemType Directory -Path $FailedDir -Force | Out-Null

foreach ($batch in $BatchDirs) {
    $batchProcessedDir = Join-Path $batch.Path 'processed'
    if (Test-Path -LiteralPath $batchProcessedDir -PathType Container) {
        Get-ChildItem -LiteralPath $batchProcessedDir -Filter '*.pdf' -File | ForEach-Object {
            $original = Join-Path $InputDir $_.Name
            if (Test-Path -LiteralPath $original -PathType Leaf) {
                Move-Item -LiteralPath $original -Destination $ProcessedDir -Force
            }
        }
    }

    $batchFailedDir = Join-Path $batch.Path 'failed'
    if (Test-Path -LiteralPath $batchFailedDir -PathType Container) {
        Get-ChildItem -LiteralPath $batchFailedDir -Filter '*.pdf' -File | ForEach-Object {
            $original = Join-Path $InputDir $_.Name
            if (Test-Path -LiteralPath $original -PathType Leaf) {
                Move-Item -LiteralPath $original -Destination $FailedDir -Force
            }
        }
    }
}

Write-Host 'Merging batch databases...' -ForegroundColor Cyan
$MergeScript = @'
import glob
import os
import sqlite3
import sys

project_root, db_path, rules_path, tmp_dir = sys.argv[1:5]
sys.path.insert(0, project_root)

from src.models.database import init_db
from src.pipeline.regex_classifier import seed_classification_rules

session_factory = init_db(db_path)
seed_classification_rules(session_factory, rules_path)

final = sqlite3.connect(db_path)
final.execute('PRAGMA journal_mode=WAL')

batch_dbs = sorted(glob.glob(os.path.join(tmp_dir, 'batch_*.db')))
total_stmts = 0
total_lines = 0
total_info = 0

for batch_db in batch_dbs:
    if not os.path.exists(batch_db):
        continue

    src = sqlite3.connect(batch_db)
    stmts = src.execute(
        'SELECT id, bank_name, account_number, statement_date, '
        'opening_balance, closing_balance, file_path, created_at '
        'FROM statements'
    ).fetchall()

    for old_row in stmts:
        old_id = old_row[0]
        cur = final.execute(
            'INSERT INTO statements '
            '(bank_name, account_number, statement_date, '
            'opening_balance, closing_balance, file_path, created_at) '
            'VALUES (?, ?, ?, ?, ?, ?, ?)',
            old_row[1:],
        )
        new_id = cur.lastrowid
        total_stmts += 1

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
'@
Invoke-Python -Python $Python -Arguments @('-c', $MergeScript, $ProjectRoot, $DbPath, $RulesPath, $TmpDir)

if (Test-Path -LiteralPath $TmpDir) {
    Remove-Item -LiteralPath $TmpDir -Recurse -Force
}

foreach ($job in $Jobs) {
    Remove-Job -Job $job -Force -ErrorAction SilentlyContinue
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

Write-Host ('  Processed : {0}' -f $PassCount) -ForegroundColor Green
Write-Host ('  Failed    : {0}' -f $FailCount) -ForegroundColor Red
Write-Host ('  Elapsed   : {0}s' -f $Elapsed) -ForegroundColor Yellow
Write-Host ''

if ((Test-Path -LiteralPath $ProcessedDir -PathType Container) -and $PassCount -gt 0) {
    Write-Host 'Processed files:' -ForegroundColor Green
    Get-ChildItem -LiteralPath $ProcessedDir -Filter '*.pdf' -File | Sort-Object Name | ForEach-Object {
        Write-Host ('  [OK] {0}' -f $_.Name) -ForegroundColor Green
    }
    Write-Host ''
}

if ((Test-Path -LiteralPath $FailedDir -PathType Container) -and $FailCount -gt 0) {
    Write-Host 'Failed files:' -ForegroundColor Red
    Get-ChildItem -LiteralPath $FailedDir -Filter '*.pdf' -File | Sort-Object Name | ForEach-Object {
        Write-Host ('  [FAIL] {0}' -f $_.Name) -ForegroundColor Red
    }
    Write-Host ''
}

if (Test-Path -LiteralPath $DbPath -PathType Leaf) {
    Write-Host 'Database summary:' -ForegroundColor Cyan
    $DbSummaryScript = @'
import sqlite3
import sys

db = sqlite3.connect(sys.argv[1])
cur = db.cursor()
stmts = cur.execute('SELECT COUNT(*) FROM statements').fetchone()[0]
lines = cur.execute('SELECT COUNT(*) FROM statement_lines').fetchone()[0]
classified = cur.execute('SELECT COUNT(*) FROM statement_lines WHERE category IS NOT NULL').fetchone()[0]
unclassified = lines - classified
print(f'  Statements      : {stmts}')
print(f'  Total lines     : {lines}')
print(f'  Classified      : {classified}')
print(f'  Unclassified    : {unclassified}')
if lines > 0:
    print(f'  Classification% : {classified/lines*100:.1f}%')
print()
print('  Top categories:')
for cat, cnt in cur.execute('''
    SELECT COALESCE(category, 'Unclassified'), COUNT(*)
    FROM statement_lines GROUP BY category ORDER BY COUNT(*) DESC LIMIT 10
''').fetchall():
    print(f'    {cat:<25} {cnt}')
db.close()
'@

    Invoke-Python -Python $Python -Arguments @('-c', $DbSummaryScript, $DbPath)
}

Write-Host ''
Write-Section ('Bulk test complete - {0}s with {1} workers' -f $Elapsed, $WorkerCount)