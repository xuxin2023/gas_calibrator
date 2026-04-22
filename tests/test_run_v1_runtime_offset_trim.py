from __future__ import annotations

import csv
import json
from pathlib import Path

from gas_calibrator.tools import run_v1_runtime_offset_trim as module


def _build_candidate_dir(tmp_path: Path) -> Path:
    candidate_dir = tmp_path / "candidate_079"
    candidate_dir.mkdir()
    (candidate_dir / "download_plan_no_500.csv").write_text(
        "\n".join(
            [
                "Analyzer,ActualDeviceId,Gas,PrimaryCommand,PrimaryValues,PrimaryC0,SecondaryCommand,a0,a1,a2,a3,a4,a5,a6,a7,a8",
                "GA03,079,CO2,\"SENCO1,YGAS,FFF,1.00000e02,2.00000e00,3.00000e00,4.00000e00,0.00000e00,0.00000e00\",\"1.00000e02,2.00000e00,3.00000e00,4.00000e00,0.00000e00,0.00000e00\",1.00000e02,\"SENCO3,YGAS,FFF,5.00000e00,6.00000e00,7.00000e00,0.00000e00,0.00000e00,0.00000e00\",100.0,2.0,3.0,4.0,5.0,6.0,7.0,0.0,0.0",
                "GA03,079,H2O,\"SENCO2,YGAS,FFF,9.00000e00,1.00000e01,1.10000e01,1.20000e01,0.00000e00,0.00000e00\",\"9.00000e00,1.00000e01,1.10000e01,1.20000e01,0.00000e00,0.00000e00\",9.00000e00,\"SENCO4,YGAS,FFF,1.30000e01,1.40000e01,1.50000e01,1.60000e01,0.00000e00,0.00000e00\",9.0,10.0,11.0,12.0,13.0,14.0,15.0,0.0,0.0",
                "GA04,012,CO2,\"SENCO1,YGAS,FFF,2.00000e02,2.10000e01,2.20000e01,2.30000e01,0.00000e00,0.00000e00\",\"2.00000e02,2.10000e01,2.20000e01,2.30000e01,0.00000e00,0.00000e00\",2.00000e02,\"SENCO3,YGAS,FFF,2.40000e01,2.50000e01,2.60000e01,0.00000e00,0.00000e00,0.00000e00\",200.0,21.0,22.0,23.0,24.0,25.0,26.0,0.0,0.0",
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
    return candidate_dir


def _make_readback_capture(group: int, coefficients: list[float], *, explicit: bool = True, error: str = "") -> dict[str, object]:
    coeff_map = {f"C{index}": float(value) for index, value in enumerate(coefficients)}
    line = ",".join(f"C{index}:{value}" for index, value in enumerate(coefficients))
    return {
        "command": f"GETCO,YGAS,079,{group}\r\n",
        "group": int(group),
        "attempts": 10,
        "attempt_index": 1,
        "source": (
            "parsed_from_explicit_c0_line"
            if explicit
            else "no_valid_coefficient_line"
        ),
        "coefficients": coeff_map if explicit else {},
        "source_line": line if explicit else "",
        "source_line_has_explicit_c0": bool(explicit),
        "raw_transcript_lines": [line] if explicit else [],
        "attempt_transcripts": [{"attempt": 1, "lines": [line]}] if explicit else [],
        "error": error if not explicit else "",
    }


class _FakeReadbackDevice:
    def __init__(self, payloads: dict[int, dict[str, object]]) -> None:
        self.payloads = payloads

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    def read_coefficient_group_capture(self, group: int, **_kwargs):
        return dict(self.payloads[int(group)])


class _FakeCaptureDevice:
    def __init__(self, frames: list[dict[str, object]]) -> None:
        self.frames = frames
        self.index = -1

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    def read_latest_data(self, **_kwargs) -> str:
        self.index = (self.index + 1) % len(self.frames)
        return str(self.frames[self.index]["raw"])

    def parse_line(self, _raw_line: str):
        return dict(self.frames[self.index]["parsed"])


class _FakeWriteDevice:
    def __init__(self, initial_group1: list[float]) -> None:
        self.group1 = list(initial_group1)
        self.mode_calls: list[int] = []
        self.set_senco_calls: list[tuple[int, list[float]]] = []

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    def set_mode_with_ack(self, mode: int, *, require_ack: bool = True) -> bool:
        self.mode_calls.append(int(mode))
        return True

    def set_senco(self, group: int, coefficients) -> bool:
        values = list(coefficients)
        self.set_senco_calls.append((int(group), values))
        self.group1 = list(values)
        return True

    def read_coefficient_group_capture(self, group: int, **_kwargs):
        return _make_readback_capture(int(group), self.group1, explicit=True)


class _DeviceFactory:
    def __init__(self, devices: list[object]) -> None:
        self.devices = list(devices)

    def __call__(self, *_args, **_kwargs):
        if not self.devices:
            raise AssertionError("device_factory exhausted unexpectedly")
        return self.devices.pop(0)


def _frames(*ppm_values: float) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for value in ppm_values:
        rows.append(
            {
                "raw": f"YGAS,079,{value:08.3f},00.000,0.99,0.99,028.74,104.28,0001,2786",
                "parsed": {
                    "mode": 1,
                    "id": "079",
                    "co2_ppm": float(value),
                    "h2o_mmol": 0.0,
                    "co2_sig": 0.99,
                    "h2o_sig": 0.99,
                    "temp_c": 28.74,
                    "pressure_kpa": 104.28,
                    "status": "0001",
                },
            }
        )
    return rows


def test_runtime_offset_trim_dry_run_generates_plan_without_write(tmp_path: Path) -> None:
    candidate_dir = _build_candidate_dir(tmp_path)
    factory = _DeviceFactory(
        [
            _FakeReadbackDevice(
                {
                    1: _make_readback_capture(1, [100.0, 2.0, 3.0, 4.0, 0.0, 0.0]),
                    3: _make_readback_capture(3, [5.0, 6.0, 7.0, 0.0, 0.0, 0.0]),
                }
            ),
            _FakeCaptureDevice(_frames(450.0, 450.0)),
        ]
    )

    result = module.run_from_cli(
        port="COM35",
        device_id="079",
        candidate_dir=candidate_dir,
        target_ppm=400.0,
        capture_duration_s=0.05,
        execute=False,
        device_factory=factory,
    )

    plan = json.loads(Path(result["plan_path"]).read_text(encoding="utf-8"))
    writeback = json.loads(Path(result["writeback_path"]).read_text(encoding="utf-8"))

    assert result["executed"] is False
    assert plan["whether_execute"] is False
    assert plan["new_a0"] == 50.0
    assert writeback["executed"] is False
    assert writeback["block_reason"] == "execute=false"


def test_runtime_offset_trim_execute_only_updates_senco1_c0(tmp_path: Path) -> None:
    candidate_dir = _build_candidate_dir(tmp_path)
    write_device = _FakeWriteDevice([100.0, 2.0, 3.0, 4.0, 0.0, 0.0])
    factory = _DeviceFactory(
        [
            _FakeReadbackDevice(
                {
                    1: _make_readback_capture(1, [100.0, 2.0, 3.0, 4.0, 0.0, 0.0]),
                    3: _make_readback_capture(3, [5.0, 6.0, 7.0, 0.0, 0.0, 0.0]),
                }
            ),
            _FakeCaptureDevice(_frames(450.0, 450.0)),
            write_device,
            _FakeCaptureDevice(_frames(405.0, 405.0)),
        ]
    )

    result = module.run_from_cli(
        port="COM35",
        device_id="079",
        candidate_dir=candidate_dir,
        target_ppm=400.0,
        capture_duration_s=0.05,
        response_slope=1.0,
        execute=True,
        device_factory=factory,
    )

    writeback = json.loads(Path(result["writeback_path"]).read_text(encoding="utf-8"))
    post_summary = json.loads(Path(result["post_summary_path"]).read_text(encoding="utf-8"))
    updated_plan = json.loads(
        Path(writeback["post_offset_candidate_dir"])  # type: ignore[index]
        .joinpath("post_offset_manifest.json")
        .read_text(encoding="utf-8")
    )

    assert write_device.set_senco_calls == [(1, [50.0, 2.0, 3.0, 4.0, 0.0, 0.0])]
    assert write_device.mode_calls == [2, 1]
    assert writeback["strict_explicit_c0_verified"] is True
    assert post_summary["verdict"] == "pass"
    assert updated_plan["new_a0"] == 50.0


def test_runtime_offset_trim_uses_response_slope_in_correction(tmp_path: Path) -> None:
    candidate_dir = _build_candidate_dir(tmp_path)
    factory = _DeviceFactory(
        [
            _FakeReadbackDevice(
                {
                    1: _make_readback_capture(1, [100.0, 2.0, 3.0, 4.0, 0.0, 0.0]),
                    3: _make_readback_capture(3, [5.0, 6.0, 7.0, 0.0, 0.0, 0.0]),
                }
            ),
            _FakeCaptureDevice(_frames(450.0, 450.0)),
        ]
    )

    result = module.run_from_cli(
        port="COM35",
        device_id="079",
        candidate_dir=candidate_dir,
        target_ppm=400.0,
        capture_duration_s=0.05,
        response_slope=0.5,
        execute=False,
        device_factory=factory,
    )

    plan = json.loads(Path(result["plan_path"]).read_text(encoding="utf-8"))
    assert plan["correction_delta_a0"] == -100.0
    assert plan["new_a0"] == 0.0


def test_runtime_offset_trim_blocks_execute_when_strict_explicit_c0_is_missing(tmp_path: Path) -> None:
    candidate_dir = _build_candidate_dir(tmp_path)
    factory = _DeviceFactory(
        [
            _FakeReadbackDevice(
                {
                    1: _make_readback_capture(1, [100.0, 2.0, 3.0, 4.0, 0.0, 0.0], explicit=False, error="NO_VALID_COEFFICIENT_LINE"),
                    3: _make_readback_capture(3, [5.0, 6.0, 7.0, 0.0, 0.0, 0.0]),
                }
            ),
            _FakeCaptureDevice(_frames(450.0, 450.0)),
        ]
    )

    result = module.run_from_cli(
        port="COM35",
        device_id="079",
        candidate_dir=candidate_dir,
        target_ppm=400.0,
        capture_duration_s=0.05,
        execute=True,
        device_factory=factory,
    )

    writeback = json.loads(Path(result["writeback_path"]).read_text(encoding="utf-8"))
    assert result["executed"] is False
    assert "strict explicit-C0 prewrite readback" in writeback["block_reason"]


def test_runtime_offset_trim_residual_classifier_covers_pass_review_and_fail() -> None:
    assert module._classify_residual(10.0)["code"] == "pass"
    assert module._classify_residual(40.0)["code"] == "review"
    assert module._classify_residual(120.0)["code"] == "fail"


def test_post_offset_candidate_only_updates_target_device_co2_a0(tmp_path: Path) -> None:
    candidate_dir = _build_candidate_dir(tmp_path)

    result = module._build_post_offset_candidate(
        candidate_dir=candidate_dir,
        source_output_dir=tmp_path / "trim_result",
        device_id="079",
        current_port="COM35",
        old_a0=100.0,
        new_a0=50.0,
        correction_delta_a0=-50.0,
        target_ppm=400.0,
        measured_after=405.0,
        residual_after=5.0,
        strict_explicit_c0_verified=True,
        high_point_checked=False,
    )

    rows = list(csv.DictReader(Path(result["download_plan_path"]).open("r", encoding="utf-8-sig", newline="")))
    co2_079 = next(row for row in rows if row["ActualDeviceId"] == "079" and row["Gas"] == "CO2")
    h2o_079 = next(row for row in rows if row["ActualDeviceId"] == "079" and row["Gas"] == "H2O")
    co2_012 = next(row for row in rows if row["ActualDeviceId"] == "012" and row["Gas"] == "CO2")

    assert co2_079["a0"] == "50.0"
    assert h2o_079["a0"] == "9.0"
    assert co2_012["a0"] == "200.0"


def test_runtime_offset_trim_high_point_check_is_read_only(tmp_path: Path) -> None:
    candidate_dir = _build_candidate_dir(tmp_path)
    capture_device = _FakeCaptureDevice(_frames(600.0, 602.0))
    factory = _DeviceFactory([capture_device])

    result = module.run_high_point_check(
        port="COM35",
        device_id="079",
        candidate_dir=candidate_dir,
        target_ppm=600.0,
        capture_duration_s=0.05,
        device_factory=factory,
    )

    summary = result["summary"]
    assert summary["high_point_verdict"] == "pass"
    assert summary["retain_a0_19707_1_recommended"] is True
