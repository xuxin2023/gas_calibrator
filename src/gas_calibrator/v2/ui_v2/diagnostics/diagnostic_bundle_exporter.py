from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from gas_calibrator.utils.file_io import write_json

from ..packaging.preflight_checks import run_preflight_checks
from ..packaging.runtime_manifest import build_runtime_manifest
from .redact_helpers import redact_mapping, redact_text


class DiagnosticBundleExporter:
    """Export a local, redacted diagnostic bundle for support use."""

    def __init__(self, runtime_paths: Any, *, ui_root: str | Path | None = None) -> None:
        self.runtime_paths = runtime_paths
        self.ui_root = Path(ui_root) if ui_root is not None else Path(__file__).resolve().parents[1]

    def export(self, facade: Any) -> dict[str, Any]:
        diagnostics_dir = Path(self.runtime_paths.base_dir) / "diagnostics"
        diagnostics_dir.mkdir(parents=True, exist_ok=True)
        bundle_dir = diagnostics_dir / f"bundle_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        bundle_dir.mkdir(parents=True, exist_ok=True)

        snapshot = redact_mapping(facade.build_snapshot())
        preferences = redact_mapping(facade.get_preferences())
        recent_runs = redact_mapping({"items": facade.get_recent_runs()})
        app_info = redact_mapping(facade.get_app_info())
        manifest = build_runtime_manifest(self.ui_root)
        preflight = run_preflight_checks(self.runtime_paths, ui_root=self.ui_root)
        logs_text = redact_text("\n".join(facade.get_recent_logs()))

        files = []
        files.append(write_json(bundle_dir / "app_info.json", app_info, trailing_newline=False))
        files.append(write_json(bundle_dir / "preferences.json", preferences, trailing_newline=False))
        files.append(write_json(bundle_dir / "recent_runs.json", recent_runs, trailing_newline=False))
        files.append(write_json(bundle_dir / "snapshot.json", snapshot, trailing_newline=False))
        files.append(write_json(bundle_dir / "runtime_manifest.json", manifest, trailing_newline=False))
        files.append(write_json(bundle_dir / "preflight.json", preflight, trailing_newline=False))
        files.append(self._write_text(bundle_dir / "logs.txt", logs_text))

        return {
            "ok": True,
            "bundle_dir": str(bundle_dir),
            "files": [str(item) for item in files],
        }
    @staticmethod
    def _write_text(path: Path, text: str) -> Path:
        path.write_text(str(text or ""), encoding="utf-8")
        return path
