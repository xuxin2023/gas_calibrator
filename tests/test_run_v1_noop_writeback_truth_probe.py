from __future__ import annotations

import json
from pathlib import Path

from gas_calibrator.tools import run_v1_noop_writeback_truth_probe as module


def _build_candidate_dir(tmp_path: Path) -> Path:
    candidate_dir = tmp_path / "candidate_079"
    candidate_dir.mkdir()
    (candidate_dir / "download_plan_no_500.csv").write_text(
        "\n".join(
            [
                "Analyzer,ActualDeviceId,Gas,PrimarySENCO,PrimaryCommand,SecondarySENCO,SecondaryCommand",
                "GA03,079,CO2,1,\"SENCO1,YGAS,FFF,1,2,3,4,0,0\",3,\"SENCO3,YGAS,FFF,5,6,7,8,0,0\"",
                "GA03,079,H2O,2,\"SENCO2,YGAS,FFF,9,10,11,12,0,0\",4,\"SENCO4,YGAS,FFF,13,14,15,16,0,0\"",
            ]
        )
        + "\n",
        encoding="utf-8-sig",
    )
    (candidate_dir / "temperature_coefficients_target.csv").write_text(
        "\n".join(
            [
                "analyzer_id,fit_type,senco_channel,command_string",
                "GA03,cell,SENCO7,\"SENCO7,YGAS,FFF,-1,0.9,0.01,-0.0001\"",
                "GA03,shell,SENCO8,\"SENCO8,YGAS,FFF,-2,0.8,0.02,-0.0002\"",
            ]
        )
        + "\n",
        encoding="utf-8-sig",
    )
    (candidate_dir / "write_readiness_summary.csv").write_text(
        "\n".join(
            [
                "corrected_fit_quality,delivery_recommendation,device_write_verify_quality,runtime_parity_quality,runtime_parity_verdict,legacy_stream_only,coefficient_source,final_write_ready,readiness_code,readiness_reason,readiness_summary",
                "pass,ok,not_requested,parity_inconclusive_missing_runtime_inputs,parity_inconclusive_missing_runtime_inputs,True,simplified,False,legacy_stream_insufficient_for_runtime_parity,legacy_stream_insufficient_for_runtime_parity,placeholder",
            ]
        )
        + "\n",
        encoding="utf-8-sig",
    )
    return candidate_dir


def test_noop_writeback_truth_probe_defaults_to_dry_run(tmp_path: Path, monkeypatch) -> None:
    candidate_dir = _build_candidate_dir(tmp_path)
    called = {"count": 0}

    def _unexpected_write(**_kwargs):
        called["count"] += 1
        raise AssertionError("dry-run should not invoke write_coefficients_to_live_devices")

    monkeypatch.setattr(module, "write_coefficients_to_live_devices", _unexpected_write)

    result = module.run_from_cli(
        candidate_dir=candidate_dir,
        device_id="079",
        port="COM39",
        output_dir=tmp_path / "noop_plan_out",
        execute=False,
    )

    plan = json.loads(Path(result["plan_json_path"]).read_text(encoding="utf-8"))
    markdown = Path(result["plan_markdown_path"]).read_text(encoding="utf-8")

    assert called["count"] == 0
    assert result["executed"] is False
    assert plan["dry_run"] is True
    assert plan["execute_requested"] is False
    assert plan["same_value_confirmed"] is False
    assert plan["allowed_to_execute"] is False
    assert plan["group_ids"] == [1, 2, 3, 4, 7, 8]
    assert "same-value is unconfirmed" in markdown or "same-value" in markdown


def test_noop_writeback_truth_probe_dry_run_flag_does_not_invoke_write(tmp_path: Path, monkeypatch) -> None:
    candidate_dir = _build_candidate_dir(tmp_path)
    called = {"count": 0}

    def _unexpected_write(**_kwargs):
        called["count"] += 1
        raise AssertionError("main --dry-run should not invoke write_coefficients_to_live_devices")

    monkeypatch.setattr(module, "write_coefficients_to_live_devices", _unexpected_write)

    exit_code = module.main(
        [
            "--candidate-dir",
            str(candidate_dir),
            "--device-id",
            "079",
            "--port",
            "COM39",
            "--output-dir",
            str(tmp_path / "noop_plan_cli_out"),
            "--dry-run",
        ]
    )

    assert exit_code == 0
    assert called["count"] == 0
    assert (tmp_path / "noop_plan_cli_out" / "noop_writeback_truth_plan.md").exists()


def test_noop_writeback_truth_probe_blocks_execute_without_same_value_confirmation(tmp_path: Path, monkeypatch) -> None:
    candidate_dir = _build_candidate_dir(tmp_path)
    called = {"count": 0}

    def _unexpected_write(**_kwargs):
        called["count"] += 1
        raise AssertionError("execute should remain blocked until same-value confirmation is explicit")

    monkeypatch.setattr(module, "write_coefficients_to_live_devices", _unexpected_write)

    result = module.run_from_cli(
        candidate_dir=candidate_dir,
        device_id="079",
        port="COM39",
        output_dir=tmp_path / "noop_plan_blocked",
        execute=True,
        assume_same_value_confirmed=False,
    )

    assert called["count"] == 0
    assert result["executed"] is False
    assert result["execution_blocked"] is True
    assert "same-value is unconfirmed" in result["execution_block_reason"]


def test_noop_writeback_truth_probe_allows_execute_when_operator_accepts_unconfirmed_same_value(tmp_path: Path, monkeypatch) -> None:
    candidate_dir = _build_candidate_dir(tmp_path)
    captured: dict[str, object] = {}

    def _fake_write(**kwargs):
        captured.update(kwargs)
        return {
            "summary_rows": [{"Analyzer": "GA03", "Status": "partial"}],
            "detail_rows": [],
            "writeback_raw_transcript_path": str(tmp_path / "writeback_raw_transcript.log"),
            "writeback_truth_summary_path": str(tmp_path / "writeback_truth_summary.json"),
            "writeback_truth_groups_path": str(tmp_path / "writeback_truth_groups.csv"),
        }

    monkeypatch.setattr(module, "write_coefficients_to_live_devices", _fake_write)

    result = module.run_from_cli(
        candidate_dir=candidate_dir,
        device_id="079",
        port="COM39",
        output_dir=tmp_path / "noop_plan_execute_allowed",
        execute=True,
        assume_same_value_confirmed=False,
        allow_unconfirmed_same_value=True,
    )

    assert result["executed"] is True
    assert result["plan"]["allow_unconfirmed_same_value"] is True
    assert result["plan"]["allowed_to_execute"] is True
    assert captured["write_pressure_rows"] is False
