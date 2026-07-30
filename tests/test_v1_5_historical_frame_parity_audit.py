from __future__ import annotations

import json
from pathlib import Path

import pytest

from gas_calibrator.validation.historical_frame_parity_audit import (
    DEFAULT_CATALOG_PATH,
    DEFAULT_OBSERVED_FIXTURE_PATH,
    audit_historical_frames,
    write_historical_frame_parity_artifacts,
)


def test_shared_historical_frame_audit_cli_preserves_arguments_and_pass_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from gas_calibrator.tools import audit_historical_frame_parity as cli

    captured: dict[str, Path] = {}

    def fake_audit(*, catalog_path: Path, observed_fixture_path: Path) -> dict:
        captured["catalog"] = catalog_path
        captured["fixture"] = observed_fixture_path
        return {"status": "PASS"}

    def fake_write(result: dict, output_dir: Path) -> dict[str, str]:
        captured["output_dir"] = output_dir
        return {"execution_summary": str(output_dir / "summary.json")}

    monkeypatch.setattr(cli, "audit_historical_frames", fake_audit)
    monkeypatch.setattr(cli, "write_historical_frame_parity_artifacts", fake_write)

    exit_code = cli.main(
        [
            "--catalog",
            str(tmp_path / "catalog.json"),
            "--fixture",
            str(tmp_path / "fixture.json"),
            "--output-dir",
            str(tmp_path / "output"),
        ]
    )

    assert exit_code == 0
    assert captured == {
        "catalog": tmp_path / "catalog.json",
        "fixture": tmp_path / "fixture.json",
        "output_dir": tmp_path / "output",
    }
    assert "execution_summary=" in capsys.readouterr().out


def test_shared_historical_frame_audit_cli_returns_two_on_failed_audit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from gas_calibrator.tools import audit_historical_frame_parity as cli

    monkeypatch.setattr(
        cli,
        "audit_historical_frames",
        lambda **_kwargs: {"status": "FAIL"},
    )
    monkeypatch.setattr(
        cli,
        "write_historical_frame_parity_artifacts",
        lambda _result, _output_dir: {},
    )

    assert cli.main(["--output-dir", str(tmp_path)]) == 2
def _historical_inputs_available() -> bool:
    if (
        not DEFAULT_CATALOG_PATH.is_file()
        or not DEFAULT_OBSERVED_FIXTURE_PATH.is_file()
    ):
        return False
    catalog = json.loads(DEFAULT_CATALOG_PATH.read_text(encoding="utf-8"))
    accepted = [
        Path(str(item.get("point_dir") or ""))
        for item in catalog.get("points", [])
        if item.get("accepted_manifest_member") and item.get("route_kind") == "co2"
    ]
    return len(accepted) == 45 and all(path.is_dir() for path in accepted)


@pytest.mark.skipif(
    not _historical_inputs_available(),
    reason="0620 historical frame evidence is not installed in this environment",
)
def test_0620_current_parser_is_frame_by_frame_equivalent_and_read_only(
    tmp_path: Path,
) -> None:
    result = audit_historical_frames()

    assert result["status"] == "PASS"
    assert result["summary"] == {
        "point_count": 45,
        "point_pass_count": 45,
        "raw_frame_count": 22197,
        "raw_frame_parser_identity_pass_count": 22197,
        "raw_frame_failure_count": 0,
        "sampled_frame_count": 2698,
        "sampled_value_mismatch_count": 0,
        "source_file_count": 182,
        "source_mutation_count": 0,
    }
    assert result["mismatch_reasons"] == {}
    assert result["boundary"]["opens_com_ports"] is False
    assert result["boundary"]["writes_devices"] is False
    assert result["boundary"]["writes_database"] is False
    assert result["method_invariants"]["co2_zero_separate_from_h2o_dry"] is True
    assert result["method_invariants"]["pressure_first_senco9_required"] is True

    artifacts = write_historical_frame_parity_artifacts(result, tmp_path)
    assert set(artifacts) == {
        "execution_rows",
        "execution_summary",
        "diagnostic_analysis",
    }
    assert all(Path(path).is_file() for path in artifacts.values())
