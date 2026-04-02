"""Learning persistence for the support loop.

After each successful resolution, persist a compact layout summary
in the repository memory so future support iterations can reference
known patterns and gotchas.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.logger import get_logger
from verifystatement.task_state import TaskState

logger = get_logger(__name__)

LEARNINGS_DIR = PROJECT_ROOT / "verifystatement" / "learnings"


def persist_learning(task: TaskState, evidence: dict) -> Optional[Path]:
    """Create a learning note for a successfully resolved support task.

    Captures: bank, layout markers, winning strategy, affected files,
    and key gotchas from the resolution.
    """
    if not task.is_complete:
        return None

    LEARNINGS_DIR.mkdir(parents=True, exist_ok=True)

    bank = task.detected_bank or "unknown"
    bank_slug = bank.lower().replace(" ", "_").replace("-", "_")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{bank_slug}_{timestamp}.json"

    latest = task.latest_attempt
    learning = {
        "bank": bank,
        "bank_key": task.bank_key,
        "pdf_stem": Path(task.target_pdf).stem,
        "strategy": task.strategy.value if task.strategy else "",
        "attempts_required": task.attempt_count,
        "layout_signature": {
            "date_family": task.layout_signature.date_family,
            "header_columns": task.layout_signature.header_columns,
            "table_shape": task.layout_signature.table_shape,
            "text_extraction_quality": task.layout_signature.text_extraction_quality,
            "preferred_strategy_hint": task.layout_signature.preferred_strategy_hint,
        },
        "affected_files": latest.changed_files if latest else [],
        "explanation": latest.explanation if latest else "",
        "initial_issues": task.initial_issues,
        "resolved_at": datetime.now().isoformat(),
    }

    path = LEARNINGS_DIR / filename
    path.write_text(
        json.dumps(learning, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    logger.info(f"Learning persisted: {path}")
    return path


def load_relevant_learnings(bank_name: str) -> List[Dict[str, Any]]:
    """Load previous learnings relevant to a specific bank."""
    if not LEARNINGS_DIR.exists():
        return []

    bank_lower = bank_name.lower()
    results = []

    for path in sorted(LEARNINGS_DIR.glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            # Match on bank name or key
            if (
                data.get("bank", "").lower() == bank_lower
                or data.get("bank_key", "").lower() == bank_lower
            ):
                results.append(data)
        except (json.JSONDecodeError, OSError):
            continue

    return results


def format_learnings_summary(learnings: List[Dict[str, Any]]) -> str:
    """Format learnings into a human-readable summary."""
    if not learnings:
        return "No previous learnings for this bank."

    lines = [f"Found {len(learnings)} previous resolution(s):"]
    for lr in learnings:
        lines.append(
            f"  - {lr.get('pdf_stem', '?')}: "
            f"strategy={lr.get('strategy', '?')}, "
            f"attempts={lr.get('attempts_required', '?')}, "
            f"explanation={lr.get('explanation', '?')[:100]}"
        )
    return "\n".join(lines)
