from __future__ import annotations

import re
from pathlib import Path
from typing import Any


_SENSITIVE_KEY_PATTERN = re.compile(r"(token|secret|password|api[_-]?key)", re.IGNORECASE)
_PORT_PATTERN = re.compile(r"\bCOM\d+\b", re.IGNORECASE)
_WINDOWS_PATH_PATTERN = re.compile(r"[A-Za-z]:\\[^\r\n\"']+")


def redact_text(text: str) -> str:
    value = str(text or "")
    home = str(Path.home())
    if home and home in value:
        value = value.replace(home, "<HOME>")
    value = _PORT_PATTERN.sub("<PORT>", value)
    value = _WINDOWS_PATH_PATTERN.sub("<PATH>", value)
    return value


def redact_value(value: Any, *, key: str = "") -> Any:
    if isinstance(value, dict):
        return {str(item_key): redact_value(item_value, key=str(item_key)) for item_key, item_value in value.items()}
    if isinstance(value, list):
        return [redact_value(item, key=key) for item in value]
    if isinstance(value, tuple):
        return [redact_value(item, key=key) for item in value]
    if isinstance(value, str):
        if key and _SENSITIVE_KEY_PATTERN.search(key):
            return "<REDACTED>"
        return redact_text(value)
    return value


def redact_mapping(payload: dict[str, Any]) -> dict[str, Any]:
    return dict(redact_value(dict(payload or {})))
