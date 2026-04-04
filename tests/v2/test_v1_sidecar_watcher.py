from __future__ import annotations

import json
import os
from pathlib import Path
import time

import pytest

from gas_calibrator.v2.adapters import v1_sidecar_watcher


def _set_tree_age(path: Path, age_s: float) -> None:
    ts = time.time() - float(age_s)
    for child in path.rglob("*"):
        os.utime(child, (ts, ts))
    os.utime(path, (ts, ts))


def _write_run_dir(
    base_dir: Path,
    name: str,
    *,
    phase: str = "completed",
    age_s: float = 120.0,
    include_summary: bool = True,
    error: str | None = None,
) -> Path:
    run_dir = base_dir / name
    run_dir.mkdir(parents=True, exist_ok=True)

    if include_summary:
        (run_dir / "summary.json").write_text(
            json.dumps(
                {
                    "run_id": name,
                    "generated_at": "2026-03-21T10:00:00",
                    "status": {
                        "phase": phase,
                        "total_points": 1,
                        "completed_points": 1 if phase == "completed" else 0,
                        "progress": 1.0 if phase == "completed" else 0.5,
                        "error": error,
                    },
                    "stats": {"warning_count": 0, "error_count": 0},
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    (run_dir / "run.log").write_text("run log", encoding="utf-8")
    (run_dir / "points.csv").write_text("point_index,status\n1,completed\n", encoding="utf-8")
    _set_tree_age(run_dir, age_s)
    return run_dir


def test_sidecar_watcher_processes_completed_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "output"
    run_dir = _write_run_dir(root / "nested", "run_20260321_010000", age_s=180.0)
    calls: list[Path] = []

    def fake_invoke_postprocess(*, run_dir: Path, import_db: bool, skip_ai: bool) -> dict[str, str]:
        calls.append(run_dir)
        manifest = run_dir / "manifest.json"
        manifest.write_text("{}", encoding="utf-8")
        postprocess_summary = run_dir / "calibration_coefficients_postprocess_summary.json"
        postprocess_summary.write_text("{}", encoding="utf-8")
        return {"summary": str(postprocess_summary), "manifest": str(manifest)}

    monkeypatch.setattr(v1_sidecar_watcher, "_invoke_postprocess", fake_invoke_postprocess)

    result = v1_sidecar_watcher.run_sidecar_once(root=root, stable_seconds=30.0)

    assert len(result["processed"]) == 1
    assert not result["failed"]
    assert calls == [run_dir.resolve()]

    status_payload = json.loads((run_dir / "sidecar_status.json").read_text(encoding="utf-8"))
    assert status_payload["status"] == "completed"
    assert status_payload["run_id"] == "run_20260321_010000"
    assert status_payload["runner_options"] == {"import_db": False, "skip_ai": False, "download": False}
    assert status_payload["postprocess_summary"] == str(run_dir / "calibration_coefficients_postprocess_summary.json")


def test_sidecar_watcher_does_not_reprocess_completed_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "output"
    run_dir = _write_run_dir(root, "run_20260321_020000", age_s=180.0)
    call_count = {"value": 0}

    def fake_invoke_postprocess(*, run_dir: Path, import_db: bool, skip_ai: bool) -> dict[str, str]:
        call_count["value"] += 1
        (run_dir / "manifest.json").write_text("{}", encoding="utf-8")
        summary_path = run_dir / "calibration_coefficients_postprocess_summary.json"
        summary_path.write_text("{}", encoding="utf-8")
        return {"summary": str(summary_path)}

    monkeypatch.setattr(v1_sidecar_watcher, "_invoke_postprocess", fake_invoke_postprocess)

    first = v1_sidecar_watcher.run_sidecar_once(root=root, stable_seconds=30.0)
    second = v1_sidecar_watcher.run_sidecar_once(root=root, stable_seconds=30.0)

    assert len(first["processed"]) == 1
    assert call_count["value"] == 1
    assert not second["processed"]
    assert len(second["skipped"]) == 1
    assert "already processed" in second["skipped"][0]["reason"]


def test_sidecar_watcher_skips_unfinished_or_active_runs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "output"
    _write_run_dir(root, "run_20260321_030000", phase="running", age_s=180.0)
    _write_run_dir(root, "run_20260321_030100", phase="completed", age_s=5.0)
    _write_run_dir(root, "run_20260321_030200", include_summary=False, age_s=180.0)
    calls: list[Path] = []

    def fake_invoke_postprocess(*, run_dir: Path, import_db: bool, skip_ai: bool) -> dict[str, str]:
        calls.append(run_dir)
        return {"summary": str(run_dir / "calibration_coefficients_postprocess_summary.json")}

    monkeypatch.setattr(v1_sidecar_watcher, "_invoke_postprocess", fake_invoke_postprocess)

    result = v1_sidecar_watcher.run_sidecar_once(root=root, stable_seconds=30.0)

    assert not result["processed"]
    assert not result["failed"]
    assert not calls
    reasons = {item["run_dir"]: item["reason"] for item in result["skipped"]}
    assert any("summary phase not terminal" in reason for reason in reasons.values())
    assert any("still changing" in reason for reason in reasons.values())
    assert any("summary.json missing" in reason for reason in reasons.values())
    assert not (root / "run_20260321_030000" / "sidecar_status.json").exists()
    assert not (root / "run_20260321_030100" / "sidecar_status.json").exists()
    assert not (root / "run_20260321_030200" / "sidecar_status.json").exists()


def test_sidecar_watcher_failure_does_not_block_other_runs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "output"
    failed_run = _write_run_dir(root / "group_a", "run_20260321_040000", age_s=180.0)
    completed_run = _write_run_dir(root / "group_b", "run_20260321_040100", age_s=180.0)

    def fake_invoke_postprocess(*, run_dir: Path, import_db: bool, skip_ai: bool) -> dict[str, str]:
        if run_dir.name.endswith("040000"):
            raise RuntimeError("synthetic postprocess failure")
        summary_path = run_dir / "calibration_coefficients_postprocess_summary.json"
        summary_path.write_text("{}", encoding="utf-8")
        return {"summary": str(summary_path)}

    monkeypatch.setattr(v1_sidecar_watcher, "_invoke_postprocess", fake_invoke_postprocess)

    result = v1_sidecar_watcher.run_sidecar_once(root=root, stable_seconds=30.0)

    assert len(result["processed"]) == 1
    assert len(result["failed"]) == 1

    failed_status = json.loads((failed_run / "sidecar_status.json").read_text(encoding="utf-8"))
    completed_status = json.loads((completed_run / "sidecar_status.json").read_text(encoding="utf-8"))
    assert failed_status["status"] == "failed"
    assert "synthetic postprocess failure" in failed_status["error"]
    assert completed_status["status"] == "completed"
