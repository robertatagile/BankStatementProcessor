"""Repair execution for the support loop.

Provides task-sized repair prompts with explicit allowed files instead
of the monolithic auto-fix prompt. Each strategy gets the minimum required
context plus the last failure reason.
"""
from __future__ import annotations

import io
import json
import base64
import sys
import textwrap
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pdfplumber
from anthropic import Anthropic

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.profiles.factory import BankProfileFactory
from src.utils.logger import get_logger
from verifystatement.task_state import Strategy, TaskState

logger = get_logger(__name__)

MAX_PROFILE_CREATION_PAGES = 10


class _RepairEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, Decimal):
            return float(o)
        return super().default(o)


# ---------------------------------------------------------------------------
# Prompt templates — one per strategy role
# ---------------------------------------------------------------------------

TRIAGE_SYSTEM_PROMPT = textwrap.dedent("""\
    You are triaging a bank statement extraction issue.

    Given the verification discrepancies, raw page text, and layout evidence,
    summarize:
    1. Root cause (date parsing, column mapping, row splitting, missing pattern, etc.)
    2. Specific lines/patterns affected
    3. Whether the fix is in profile settings or requires extractor changes

    Return **only** valid JSON (no markdown fences):
    {
      "root_cause": "...",
      "affected_patterns": ["..."],
      "fix_scope": "profile_only" | "extractor_needed",
      "hypothesis": "concise description of what to change"
    }
""")

PROFILE_PATCH_SYSTEM_PROMPT = textwrap.dedent("""\
    You are fixing a bank statement PDF extractor profile.

    You receive:
    1. The current bank profile Python source code
    2. Extraction errors (missing/incorrect/extra transactions)
    3. Raw pdfplumber text from problematic pages
    4. A triage summary identifying the root cause
    5. [If retry] The previous fix attempt and why it failed

    Rules:
    - Only modify the profile — NOT the core extractor
    - Must be backward-compatible with existing statements
    - Focus on: regex patterns, date formats, column mappings, text_line_pattern,
      fee_line_pattern, header_patterns, prefer_text_extraction
    - If a previous attempt broke tests, adjust narrowly
    - Do NOT add features beyond what is needed to fix the issue

    Return **only** valid JSON (no markdown fences):
    {
      "explanation": "what was wrong and how you fixed it",
      "profile_code": "...full updated profile module source..."
    }
""")

EXTRACTOR_PATCH_SYSTEM_PROMPT = textwrap.dedent("""\
    You are fixing a bank statement PDF extractor to support a new layout variant.

    You receive:
    1. The relevant section of the extractor source
    2. The bank profile source
    3. Extraction errors and raw page text
    4. A triage summary
    5. [If retry] Previous attempt and failure reason

    Rules:
    - Make the MINIMUM change needed — do not refactor unrelated code
    - New extractor behavior must be guarded by a profile flag or bank check
      so existing banks are not affected
    - Both the profile AND extractor changes may be needed
    - Must pass regression tests for ALL banks, not just the target

    Return **only** valid JSON (no markdown fences):
    {
      "explanation": "what was wrong and how you fixed it",
      "profile_code": "...full updated profile module source...",
      "extractor_patch": "...full updated extractor module source or empty string if unchanged..."
    }
""")

NEW_PROFILE_SYSTEM_PROMPT = textwrap.dedent("""\
    You are creating a bank profile for a South African bank statement PDF extractor.

    You receive:
    1. Page images from the bank statement
    2. The raw text pdfplumber extracted from page 1
    3. The BankProfile dataclass (base.py)
    4. SA common helpers (_sa_common.py)
    5. An example bank profile module (nedbank.py)
    6. Corrected transaction data from verification

    Create a new profile module. It must:
    - Import from src.profiles.banks._sa_common and call sa_base_profile() with overrides
    - Set detection_keywords from bank branding visible in the text
    - Set appropriate date_formats for observed date patterns
    - Set text_line_pattern or default_column_map based on layout
    - Override header_patterns as needed for this bank's format
    - Follow the exact code style of the example profile
    - The factory function must be named {bank_key}_profile and return a BankProfile

    Return **only** valid JSON (no markdown fences):
    {
      "bank_key": "new_bank",
      "module_filename": "new_bank.py",
      "profile_code": "...full Python module source..."
    }
""")


def _read_source_file(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _parse_json_response(text: str) -> dict:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        start = 1
        end = len(lines)
        for i in range(len(lines) - 1, 0, -1):
            if lines[i].strip().startswith("```"):
                end = i
                break
        cleaned = "\n".join(lines[start:end])
    return json.loads(cleaned)


def _page_to_base64_png(page) -> str:
    page_image = page.to_image(resolution=300)
    pil_image = page_image.original
    buf = io.BytesIO()
    pil_image.save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode("utf-8")


# ---------------------------------------------------------------------------
# Triage
# ---------------------------------------------------------------------------

def run_triage(
    evidence: dict,
    verification_report: dict,
    client: Anthropic,
    model: str,
) -> dict:
    """Run issue triage to produce a root-cause hypothesis."""
    discrepancies = json.dumps(
        verification_report.get("aggregated", {}),
        indent=2,
        cls=_RepairEncoder,
    )
    # Include first few page text snippets
    snippets = evidence.get("page_text_snippets", {})
    text_sample = "\n\n".join(
        f"--- Page {k} ---\n{v}" for k, v in list(snippets.items())[:3]
    )
    layout = json.dumps(evidence.get("layout_signature", {}), indent=2)

    prompt = (
        f"Extraction errors:\n```json\n{discrepancies}\n```\n\n"
        f"Layout signature:\n```json\n{layout}\n```\n\n"
        f"Raw page text samples:\n```\n{text_sample}\n```"
    )

    response = client.messages.create(
        model=model,
        max_tokens=2048,
        system=TRIAGE_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )

    try:
        return _parse_json_response(response.content[0].text)
    except (json.JSONDecodeError, IndexError) as exc:
        logger.warning(f"Triage parse failed: {exc}")
        return {
            "root_cause": "unknown",
            "affected_patterns": [],
            "fix_scope": "profile_only",
            "hypothesis": "Could not determine — manual inspection needed",
        }


# ---------------------------------------------------------------------------
# Profile Patch
# ---------------------------------------------------------------------------

def execute_profile_patch(
    task: TaskState,
    evidence: dict,
    verification_report: dict,
    triage: dict,
    client: Anthropic,
    model: str,
) -> Dict[str, Any]:
    """Execute a profile-only patch attempt.

    Returns dict with keys: explanation, profile_code, changed_files, error.
    """
    profile_path = _find_profile_source_path(task.detected_bank)
    if not profile_path:
        return {"error": f"Profile source not found for '{task.detected_bank}'"}

    current_code = _read_source_file(profile_path)

    # Collect raw text from pages with issues
    page_texts = evidence.get("page_texts", {})
    raw_text_str = "\n\n".join(
        f"--- Page {pn} ---\n{txt}"
        for pn, txt in page_texts.items()
    )

    discrepancies = json.dumps(
        verification_report.get("aggregated", {}), indent=2, cls=_RepairEncoder
    )

    # Build prompt with last failure if retrying
    prompt = (
        f"Current profile source ({profile_path.name}):\n"
        f"```python\n{current_code}\n```\n\n"
        f"Extraction errors:\n```json\n{discrepancies}\n```\n\n"
        f"Triage summary:\n```json\n{json.dumps(triage, indent=2)}\n```\n\n"
        f"Raw pdfplumber text from statement:\n```\n{raw_text_str[:8000]}\n```"
    )

    if task.latest_failure_reason:
        prompt += (
            f"\n\nPrevious attempt failed:\n{task.latest_failure_reason}\n"
            f"Please provide a more conservative fix."
        )

    response = client.messages.create(
        model=model,
        max_tokens=8192,
        system=PROFILE_PATCH_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )

    try:
        result = _parse_json_response(response.content[0].text)
    except (json.JSONDecodeError, IndexError) as exc:
        return {"error": f"Failed to parse response: {exc}"}

    new_code = result.get("profile_code", "")
    if not new_code:
        return {"error": "Empty profile code returned"}

    # Backup and write
    original_code = current_code
    profile_path.write_text(new_code, encoding="utf-8")
    BankProfileFactory._registry.clear()

    return {
        "explanation": result.get("explanation", ""),
        "profile_code": new_code,
        "original_code": original_code,
        "profile_path": str(profile_path),
        "changed_files": [str(profile_path)],
        "error": "",
    }


# ---------------------------------------------------------------------------
# Extractor Patch
# ---------------------------------------------------------------------------

def execute_extractor_patch(
    task: TaskState,
    evidence: dict,
    verification_report: dict,
    triage: dict,
    client: Anthropic,
    model: str,
) -> Dict[str, Any]:
    """Execute an extractor + profile patch attempt."""
    profile_path = _find_profile_source_path(task.detected_bank)
    extractor_path = PROJECT_ROOT / "src" / "pipeline" / "pdf_extractor.py"

    if not profile_path:
        return {"error": f"Profile source not found for '{task.detected_bank}'"}

    profile_code = _read_source_file(profile_path)
    extractor_code = _read_source_file(extractor_path)

    page_texts = evidence.get("page_texts", {})
    raw_text_str = "\n\n".join(
        f"--- Page {pn} ---\n{txt}" for pn, txt in page_texts.items()
    )
    discrepancies = json.dumps(
        verification_report.get("aggregated", {}), indent=2, cls=_RepairEncoder
    )

    prompt = (
        f"Extractor source (pdf_extractor.py):\n"
        f"```python\n{extractor_code}\n```\n\n"
        f"Bank profile source ({profile_path.name}):\n"
        f"```python\n{profile_code}\n```\n\n"
        f"Extraction errors:\n```json\n{discrepancies}\n```\n\n"
        f"Triage summary:\n```json\n{json.dumps(triage, indent=2)}\n```\n\n"
        f"Raw pdfplumber text:\n```\n{raw_text_str[:6000]}\n```"
    )

    if task.latest_failure_reason:
        prompt += (
            f"\n\nPrevious attempt failed:\n{task.latest_failure_reason}\n"
            f"Adjust narrowly."
        )

    response = client.messages.create(
        model=model,
        max_tokens=16384,
        system=EXTRACTOR_PATCH_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )

    try:
        result = _parse_json_response(response.content[0].text)
    except (json.JSONDecodeError, IndexError) as exc:
        return {"error": f"Failed to parse response: {exc}"}

    original_profile = profile_code
    original_extractor = extractor_code
    changed: List[str] = []

    new_profile = result.get("profile_code", "")
    if new_profile:
        profile_path.write_text(new_profile, encoding="utf-8")
        changed.append(str(profile_path))

    new_extractor = result.get("extractor_patch", "")
    if new_extractor:
        extractor_path.write_text(new_extractor, encoding="utf-8")
        changed.append(str(extractor_path))

    BankProfileFactory._registry.clear()

    return {
        "explanation": result.get("explanation", ""),
        "profile_code": new_profile,
        "extractor_code": new_extractor,
        "original_profile": original_profile,
        "original_extractor": original_extractor,
        "profile_path": str(profile_path),
        "extractor_path": str(extractor_path),
        "changed_files": changed,
        "error": "",
    }


# ---------------------------------------------------------------------------
# New Profile Creation
# ---------------------------------------------------------------------------

def execute_new_profile(
    task: TaskState,
    evidence: dict,
    verification_report: dict,
    client: Anthropic,
    model: str,
) -> Dict[str, Any]:
    """Create a new bank profile from scratch."""
    pdf_path = Path(task.target_pdf)

    base_py = _read_source_file(PROJECT_ROOT / "src" / "profiles" / "base.py")
    sa_common = _read_source_file(
        PROJECT_ROOT / "src" / "profiles" / "banks" / "_sa_common.py"
    )
    nedbank_example = _read_source_file(
        PROJECT_ROOT / "src" / "profiles" / "banks" / "nedbank.py"
    )

    page1_text = evidence.get("page_texts", {}).get("1", "")

    with pdfplumber.open(str(pdf_path)) as pdf:
        pages_to_send = pdf.pages[:MAX_PROFILE_CREATION_PAGES]
        page_images = []
        for page in pages_to_send:
            page_images.append({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": "image/png",
                    "data": _page_to_base64_png(page),
                },
            })

    corrected_data = json.dumps(
        verification_report.get("aggregated", {}), indent=2, cls=_RepairEncoder
    )

    user_content = page_images + [{
        "type": "text",
        "text": (
            f"Raw text from page 1:\n```\n{page1_text}\n```\n\n"
            f"BankProfile dataclass (base.py):\n```python\n{base_py}\n```\n\n"
            f"SA common helpers (_sa_common.py):\n```python\n{sa_common}\n```\n\n"
            f"Example profile (nedbank.py):\n```python\n{nedbank_example}\n```\n\n"
            f"Corrected transaction data:\n```json\n{corrected_data}\n```"
        ),
    }]

    if task.latest_failure_reason:
        user_content[-1]["text"] += (
            f"\n\nPrevious attempt failed:\n{task.latest_failure_reason}"
        )

    response = client.messages.create(
        model=model,
        max_tokens=8192,
        system=NEW_PROFILE_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_content}],
    )

    try:
        result = _parse_json_response(response.content[0].text)
    except (json.JSONDecodeError, IndexError) as exc:
        return {"error": f"Failed to parse response: {exc}"}

    bank_key = result.get("bank_key", "unknown_bank")
    module_filename = result.get("module_filename", f"{bank_key}.py")
    profile_code = result.get("profile_code", "")

    if not profile_code:
        return {"error": "Empty profile code returned"}

    profile_path = PROJECT_ROOT / "src" / "profiles" / "banks" / module_filename
    init_path = PROJECT_ROOT / "src" / "profiles" / "banks" / "__init__.py"
    original_init = _read_source_file(init_path)

    try:
        profile_path.write_text(profile_code, encoding="utf-8")
        func_name = f"{bank_key}_profile"
        import_line = f"    from src.profiles.banks.{bank_key} import {func_name}"
        register_line = f'    BankProfileFactory.register("{bank_key}", {func_name})'

        if import_line not in original_init:
            new_init = original_init.rstrip() + f"\n\n{import_line}\n{register_line}\n"
            init_path.write_text(new_init, encoding="utf-8")

        BankProfileFactory._registry.clear()

        return {
            "explanation": f"Created new profile for {bank_key}",
            "bank_key": bank_key,
            "profile_code": profile_code,
            "profile_path": str(profile_path),
            "original_init": original_init,
            "changed_files": [str(profile_path), str(init_path)],
            "error": "",
        }
    except Exception as exc:
        # Rollback
        if profile_path.exists():
            profile_path.unlink()
        init_path.write_text(original_init, encoding="utf-8")
        BankProfileFactory._registry.clear()
        return {"error": f"Profile creation failed: {exc}"}


def revert_repair(repair_result: dict) -> None:
    """Revert changes made by a repair attempt."""
    if repair_result.get("original_code"):
        Path(repair_result["profile_path"]).write_text(
            repair_result["original_code"], encoding="utf-8"
        )
    if repair_result.get("original_profile"):
        Path(repair_result["profile_path"]).write_text(
            repair_result["original_profile"], encoding="utf-8"
        )
    if repair_result.get("original_extractor"):
        Path(repair_result["extractor_path"]).write_text(
            repair_result["original_extractor"], encoding="utf-8"
        )
    if repair_result.get("original_init"):
        init_path = PROJECT_ROOT / "src" / "profiles" / "banks" / "__init__.py"
        init_path.write_text(repair_result["original_init"], encoding="utf-8")
    # Clean up new profile file if it was created
    profile_path = repair_result.get("profile_path", "")
    if (
        repair_result.get("bank_key")
        and profile_path
        and Path(profile_path).exists()
    ):
        Path(profile_path).unlink(missing_ok=True)

    BankProfileFactory._registry.clear()


def _find_profile_source_path(bank_name: str) -> Optional[Path]:
    """Locate the profile source file for a given bank name."""
    banks_dir = PROJECT_ROOT / "src" / "profiles" / "banks"
    candidate = bank_name.lower().replace(" ", "_").replace("-", "_")
    path = banks_dir / f"{candidate}.py"
    if path.exists():
        return path
    for py_file in banks_dir.glob("*.py"):
        if py_file.name.startswith("_") or py_file.name == "__init__.py":
            continue
        content = py_file.read_text(encoding="utf-8")
        if f'name="{bank_name}"' in content or f"name='{bank_name}'" in content:
            return py_file
    return None
