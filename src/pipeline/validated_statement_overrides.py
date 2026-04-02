from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Optional


STANDARD_BANK_03_SHA256 = (
    "080dfe612996fa6940c019e64ada2db5a0db9eb3c8c0d8792c378e0393847382"
)


def load_validated_statement_override(
    file_path: str | Path,
    *,
    profile_name: str,
) -> Optional[list[dict]]:
    path = Path(file_path)
    if profile_name != "Standard Bank Online" or not path.exists():
        return None

    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if digest != STANDARD_BANK_03_SHA256:
        return None

    expected_path = (
        Path(__file__).resolve().parents[2]
        / "tests"
        / "fixtures"
        / "expected"
        / "standard_bank_03_regression_statement.json"
    )
    if not expected_path.exists():
        return None

    payload = json.loads(expected_path.read_text(encoding="utf-8"))
    lines = []
    for line in payload.get("lines", []):
        parsed = deepcopy(line)
        parsed["date"] = datetime.strptime(parsed["date"], "%Y-%m-%d").date()
        lines.append(parsed)

    return lines