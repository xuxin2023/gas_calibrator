from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..utils.app_info import APP_INFO


def load_build_info(path: str | Path | None) -> dict[str, Any]:
    payload = APP_INFO.as_dict()
    if path is None:
        return payload
    source = Path(path)
    if not source.exists():
        return payload
    try:
        raw = json.loads(source.read_text(encoding="utf-8"))
    except Exception:
        return payload
    if isinstance(raw, dict):
        payload.update({str(key): value for key, value in raw.items()})
    return payload
