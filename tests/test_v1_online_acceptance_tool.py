from __future__ import annotations

import json
from pathlib import Path

import pytest

from gas_calibrator.tools import run_v1_online_acceptance as module


def _write_config(path: Path) -> Path:
    payload = {
        "paths": {
            "output_dir": str((path.parent / "logs").resolve()),
        },
        "devices": {
            "gas_analyzer": {
                "enabled": True,
                "port": "COM11",
                "baud": 115200,
                "device_id": "086",
            }
        },
        "coefficients": {
            "h2o_zero_span": {
                "status": "not_supported",
                "require_supported_capability": False,
            }
        },
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


class _FakeAnalyzer:
    def __init__(self, *args, **kwargs) -> None:
        self.device_id = str(kwargs.get("device_id", "086"))
        self.mode = 1
        self.values = {1: [10.0, 20.0, 30.0, 40.0, 0.0, 0.0]}
        self.mode_calls: list[int] = []
        self.open_called = 0
        self.close_called = 0

    def open(self) -> None:
        self.open_called += 1

    def close(self) -> None:
        self.close_called += 1

    def read_current_mode_snapshot(self):
        return {"mode": self.mode, "id": self.device_id, "raw": f"mode={self.mode}"}

    def set_mode_with_ack(self, mode: int, *, require_ack: bool = True) -> bool:
        self.mode = int(mode)
        self.mode_calls.append(int(mode))
        return True

    def set_senco(self, group: int, *coeffs) -> bool:
        values = list(coeffs[0]) if len(coeffs) == 1 and isinstance(coeffs[0], (list, tuple)) else list(coeffs)
        self.values[int(group)] = [float(value) for value in values]
        return True

    def read_coefficient_group(self, group: int):
        return {f"C{idx}": float(value) for idx, value in enumerate(self.values[int(group)])}


class _FakeAnalyzerExitUnconfirmed(_FakeAnalyzer):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._snapshot_after_restore = None

    def read_current_mode_snapshot(self):
        if self.mode_calls and self.mode_calls[-1] == 1:
            return self._snapshot_after_restore
        return {"mode": self.mode, "id": self.device_id, "raw": f"mode={self.mode}"}


def test_online_acceptance_requires_dual_gate_for_real_device(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path / "config.json")
    output_dir = tmp_path / "online"
    analyzer_factory_calls = {"count": 0}

    def _forbidden_factory(*args, **kwargs):
        analyzer_factory_calls["count"] += 1
        raise AssertionError("dry-run should not instantiate analyzer")

    result = module.run_online_acceptance(
        config_path=str(config_path),
        output_dir=output_dir,
        real_device=True,
        env={module.REAL_DEVICE_ENV: "0"},
        analyzer_factory=_forbidden_factory,
        log_fn=lambda *_: None,
    )

    assert result["mode"] == "dry_run"
    assert result["status"] == "DRY_RUN_ONLY"
    assert analyzer_factory_calls["count"] == 0
    assert (output_dir / "01_online_acceptance_checklist.md").exists()
    assert (output_dir / "02_online_run_template.json").exists()
    assert (output_dir / "03_online_protocol_log_schema.md").exists()
    assert (output_dir / "04_online_evidence_summary.md").exists()
    assert not list(output_dir.glob("online_run_*.json"))
    assert not list(output_dir.glob("online_protocol_*.jsonl"))


def test_online_acceptance_dry_run_generates_templates_only(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path / "config.json")
    output_dir = tmp_path / "online"

    result = module.run_online_acceptance(
        config_path=str(config_path),
        output_dir=output_dir,
        real_device=False,
        analyzer_factory=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("no device in dry-run")),
        log_fn=lambda *_: None,
    )

    template = json.loads((output_dir / "02_online_run_template.json").read_text(encoding="utf-8"))
    schema_text = (output_dir / "03_online_protocol_log_schema.md").read_text(encoding="utf-8")
    assert result["mode"] == "dry_run"
    assert template["capability_boundary"]["h2o_zero"] == "NOT_SUPPORTED"
    assert template["capability_boundary"]["h2o_span"] == "NOT_SUPPORTED"
    assert "raw_command" in schema_text
    assert "raw_response" in schema_text
    assert not list(output_dir.glob("online_run_*.json"))
    assert not list(output_dir.glob("online_protocol_*.jsonl"))


def test_online_acceptance_real_mode_writes_summary_and_protocol_log(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path / "config.json")
    output_dir = tmp_path / "online"
    fake = _FakeAnalyzer()

    result = module.run_online_acceptance(
        config_path=str(config_path),
        output_dir=output_dir,
        real_device=True,
        env={module.REAL_DEVICE_ENV: "1"},
        analyzer_factory=lambda *args, **kwargs: fake,
        log_fn=lambda *_: None,
    )

    assert result["mode"] == "real_device"
    assert result["status"] == "SUCCESS"
    assert result["mode_exit_attempted"] is True
    assert result["mode_exit_confirmed"] is True
    assert Path(result["run_summary_path"]).exists()
    assert Path(result["protocol_log_path"]).exists()
    run_payload = json.loads(Path(result["run_summary_path"]).read_text(encoding="utf-8"))
    protocol_rows = [
        json.loads(line)
        for line in Path(result["protocol_log_path"]).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    for key in module.REQUIRED_RUN_FIELDS:
        assert key in run_payload
    assert protocol_rows
    for key in module.REQUIRED_PROTOCOL_FIELDS:
        assert key in protocol_rows[0]
    assert fake.open_called == 1
    assert fake.close_called == 1
    assert fake.mode_calls == [2, 1]
    assert {row["stage"] for row in protocol_rows} >= {
        "baseline-mode-snapshot",
        "mode-switch",
        "write-coefficients",
        "getco-readback",
    }


def test_online_acceptance_exit_unconfirmed_is_unsafe_and_failed(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path / "config.json")
    output_dir = tmp_path / "online"
    fake = _FakeAnalyzerExitUnconfirmed()

    result = module.run_online_acceptance(
        config_path=str(config_path),
        output_dir=output_dir,
        real_device=True,
        env={module.REAL_DEVICE_ENV: "1"},
        analyzer_factory=lambda *args, **kwargs: fake,
        log_fn=lambda *_: None,
    )

    assert result["mode"] == "real_device"
    assert result["status"] == "FAILED"
    assert result["unsafe"] is True
    assert result["mode_exit_attempted"] is True
    assert result["mode_exit_confirmed"] is False
    run_payload = json.loads(Path(result["run_summary_path"]).read_text(encoding="utf-8"))
    assert run_payload["unsafe"] is True
    assert run_payload["mode_exit_confirmed"] is False
    summary_text = (output_dir / "04_online_evidence_summary.md").read_text(encoding="utf-8")
    assert "ONLINE_EVIDENCE_REQUIRED" in summary_text
    assert "FAILED" in summary_text


def test_online_acceptance_h2o_request_fails_fast(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path / "config.json")

    with pytest.raises(RuntimeError, match="NOT_SUPPORTED"):
        module.run_online_acceptance(
            config_path=str(config_path),
            output_dir=tmp_path / "online",
            gas_type="h2o",
            real_device=False,
            log_fn=lambda *_: None,
        )

    with pytest.raises(RuntimeError, match="NOT_SUPPORTED"):
        module.run_online_acceptance(
            config_path=str(config_path),
            output_dir=tmp_path / "online_groups",
            groups=[2],
            real_device=False,
            log_fn=lambda *_: None,
        )
