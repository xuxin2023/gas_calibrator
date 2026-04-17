from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

from .engineering_isolation_gate_evaluator import (
    ENGINEERING_ISOLATION_BLOCKERS_FILENAME,
    ENGINEERING_ISOLATION_GATE_DIGEST_FILENAME,
    ENGINEERING_ISOLATION_GATE_RESULT_FILENAME,
    ENGINEERING_ISOLATION_WARNINGS_FILENAME,
    build_engineering_isolation_gate_evaluator,
)


ENGINEERING_ISOLATION_GATE_REPOSITORY_SCHEMA_VERSION = "engineering-isolation-gate-repository-v1"
ENGINEERING_ISOLATION_GATE_REPOSITORY_MODE = "file_artifact_first"
ENGINEERING_ISOLATION_GATE_GATEWAY_MODE = "file_backed_default"


class EngineeringIsolationGateRepository(Protocol):
    def load_snapshot(self) -> dict[str, Any]:
        """Return engineering-isolation gate payloads."""


class FileBackedEngineeringIsolationGateRepository:
    def __init__(
        self,
        run_dir: Path,
        **builder_kwargs: Any,
    ) -> None:
        self.run_dir = Path(run_dir)
        self.builder_kwargs = {
            "run_dir": str(self.run_dir),
            **{key: value for key, value in dict(builder_kwargs or {}).items()},
        }

    def load_snapshot(self) -> dict[str, Any]:
        built = build_engineering_isolation_gate_evaluator(**self.builder_kwargs)
        result_payload = {
            **dict(built.get("engineering_isolation_gate_result") or {}),
            **self._load_json(ENGINEERING_ISOLATION_GATE_RESULT_FILENAME),
        }
        blockers_payload = {
            **dict(built.get("engineering_isolation_blockers") or {}),
            **self._load_json(ENGINEERING_ISOLATION_BLOCKERS_FILENAME),
        }
        warnings_payload = {
            **dict(built.get("engineering_isolation_warnings") or {}),
            **self._load_json(ENGINEERING_ISOLATION_WARNINGS_FILENAME),
        }
        digest_markdown = self._load_markdown(ENGINEERING_ISOLATION_GATE_DIGEST_FILENAME) or str(
            built.get("engineering_isolation_gate_digest_markdown") or ""
        )
        compact_panel = {
            **dict(built.get("engineering_isolation_gate_compact_panel") or {}),
            **dict(result_payload.get("compact_panel") or {}),
        }

        for payload, filename in (
            (result_payload, ENGINEERING_ISOLATION_GATE_RESULT_FILENAME),
            (blockers_payload, ENGINEERING_ISOLATION_BLOCKERS_FILENAME),
            (warnings_payload, ENGINEERING_ISOLATION_WARNINGS_FILENAME),
        ):
            payload.setdefault("schema_version", ENGINEERING_ISOLATION_GATE_REPOSITORY_SCHEMA_VERSION)
            payload["repository_mode"] = ENGINEERING_ISOLATION_GATE_REPOSITORY_MODE
            payload["gateway_mode"] = ENGINEERING_ISOLATION_GATE_GATEWAY_MODE
            payload["artifact_present_on_disk"] = bool((self.run_dir / filename).exists())
            payload["file_artifact_first_preserved"] = True
            payload["main_chain_dependency"] = False

        compact_panel["repository_mode"] = ENGINEERING_ISOLATION_GATE_REPOSITORY_MODE
        compact_panel["gateway_mode"] = ENGINEERING_ISOLATION_GATE_GATEWAY_MODE
        compact_panel["file_artifact_first_preserved"] = True
        compact_panel["main_chain_dependency"] = False

        return {
            "engineering_isolation_gate_result": result_payload,
            "engineering_isolation_blockers": blockers_payload,
            "engineering_isolation_warnings": warnings_payload,
            "engineering_isolation_gate_digest_markdown": digest_markdown,
            "engineering_isolation_gate_compact_panel": compact_panel,
        }

    def _load_json(self, filename: str) -> dict[str, Any]:
        path = self.run_dir / filename
        if not path.exists():
            return {}
        try:
            import json

            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return dict(payload) if isinstance(payload, dict) else {}

    def _load_markdown(self, filename: str) -> str:
        path = self.run_dir / filename
        if not path.exists():
            return ""
        try:
            return path.read_text(encoding="utf-8")
        except Exception:
            return ""
