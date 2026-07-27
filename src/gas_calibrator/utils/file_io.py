"""Small, product-neutral file helpers."""

from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
from typing import Any


def sha256_file(path: str | Path) -> str:
    digest = sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(
    path: str | Path,
    payload: Any,
    *,
    trailing_newline: bool = True,
) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    target.write_text(text + ("\n" if trailing_newline else ""), encoding="utf-8")
    return target


__all__ = ["sha256_file", "write_json"]
