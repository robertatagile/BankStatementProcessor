#!/usr/bin/env pwsh
# ============================================================
# Vision Verification Tool Runner (Windows PowerShell)
# ============================================================
# Runs verify.py against PDFs in verifystatement/input or a
# specified path.
#
# Usage:
#   pwsh -File .\verifystatement\run_verify.ps1
#   pwsh -File .\verifystatement\run_verify.ps1 -PdfFile path\to\file.pdf
#   pwsh -File .\verifystatement\run_verify.ps1 -AutoFix
# ============================================================

param(
    [string]$PdfFile,
    [string]$PdfDir,
    [switch]$AutoFix,
    [string]$ReportDir,
    [string]$Model = "claude-sonnet-4-20250514",
    [int]$MaxAttempts = 3
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Write-Section {
    param([string]$Text)
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
$Python = Get-PythonCommand

Write-Section 'Vision Verification Tool'
Write-Host ''

# Check for API key
if (-not $env:ANTHROPIC_API_KEY) {
    Write-Host 'ERROR: ANTHROPIC_API_KEY environment variable not set' -ForegroundColor Red
    exit 1
}

# Build arguments
$PythonArgs = @('verifystatement/verify.py')

if ($PdfFile) {
    $PythonArgs += @('--pdf-file', $PdfFile)
    Write-Host "PDF file : $PdfFile" -ForegroundColor Yellow
}
elseif ($PdfDir) {
    $PythonArgs += @('--pdf-dir', $PdfDir)
    Write-Host "PDF dir  : $PdfDir" -ForegroundColor Yellow
}
else {
    $DefaultInput = Join-Path $ScriptDir 'input'
    $PdfCount = (Get-ChildItem -LiteralPath $DefaultInput -Filter '*.pdf' -File -ErrorAction SilentlyContinue | Measure-Object).Count
    if ($PdfCount -eq 0) {
        Write-Host "No PDF files in $DefaultInput - nothing to verify." -ForegroundColor Yellow
        exit 0
    }
    $PythonArgs += @('--pdf-dir', $DefaultInput)
    Write-Host "PDF dir  : $DefaultInput ($PdfCount files)" -ForegroundColor Yellow
}

$PythonArgs += @('--model', $Model, '--max-attempts', $MaxAttempts)

if ($ReportDir) {
    $PythonArgs += @('--report-dir', $ReportDir)
    Write-Host "Report   : $ReportDir" -ForegroundColor Yellow
}

if ($AutoFix) {
    $PythonArgs += '--auto-fix'
    Write-Host 'Auto-fix : ENABLED' -ForegroundColor Magenta
}

Write-Host ''
Write-Host 'Running verification...' -ForegroundColor Cyan
Write-Host ''

Push-Location $ProjectRoot
try {
    Invoke-Python -Python $Python -Arguments $PythonArgs
}
finally {
    Pop-Location
}

Write-Host ''
Write-Section 'Verification complete'
