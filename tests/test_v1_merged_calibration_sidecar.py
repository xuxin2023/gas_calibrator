from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from gas_calibrator.tools import run_v1_merged_calibration_sidecar as sidecar


def _temperature_rows() -> list[dict[str, object]]:
    return [
        {
            "analyzer_id": "GA01",
            "fit_type": "cell",
            "availability": "available",
            "fit_ok": True,
            "A": 0.0,
            "B": 1.0,
            "C": 0.0,
            "D": 0.0,
        },
        {
            "analyzer_id": "GA01",
            "fit_type": "shell",
            "availability": "available",
            "fit_ok": True,
            "A": 0.1,
            "B": 1.1,
            "C": 0.0,
            "D": 0.0,
        },
    ]


def test_summary_phase_prefers_phasekey_fallback() -> None:
    assert sidecar._summary_phase({"PhaseKey": "co2", "PointPhase": "水路"}) == "co2"
    assert sidecar._summary_phase({"PointPhase": "气路"}) == "co2"
    assert sidecar._summary_phase({"流程阶段": "水路"}) == "h2o"
    assert sidecar._summary_phase({"PointPhase": "unknown"}) == "unknown"


def test_split_summary_rows_by_phase_keeps_gas_water_separate() -> None:
    rows = [
        {"Analyzer": "GA01", "PhaseKey": "co2", "PointPhase": "水路", "PointRow": 3},
        {"Analyzer": "GA01", "PointPhase": "水路", "PointRow": 9},
        {"Analyzer": "GA01", "PointPhase": "气路", "PointRow": 12},
        {"Analyzer": "GA01", "PointPhase": "mystery", "PointRow": 99},
    ]

    gas_rows, water_rows, unknown_rows = sidecar._split_summary_rows_by_phase(rows)
    counts = sidecar._count_summary_by_phase(rows)

    assert [row["PointRow"] for row in gas_rows] == [3, 12]
    assert [row["PointRow"] for row in water_rows] == [9]
    assert [row["PointRow"] for row in unknown_rows] == [99]
    assert counts == {"gas": 2, "water": 1, "unknown": 1, "total": 4}


def test_point_identity_aligns_points_readable_with_summary_rows() -> None:
    point_row = {
        "流程阶段": "气路",
        "温箱目标温度C": -20.0,
        "目标二氧化碳浓度ppm": 0.0,
        "湿度发生器_目标温度(℃)": "",
        "湿度发生器_目标湿度(%RH)": "",
        "目标压力hPa": "",
    }
    summary_row = {
        "PhaseKey": "co2",
        "PointPhase": "气路",
        "TempSet": float("nan"),
        "EnvTempC": -20.0,
        "ppm_CO2_Tank": 0.0,
        "HgenTempSet": float("nan"),
        "HgenRhSet": float("nan"),
        "PressureTarget": float("nan"),
    }

    assert sidecar._point_identity_from_row(point_row) == sidecar._point_identity_from_row(summary_row)


def test_merge_summary_rows_matches_points_when_summary_uses_env_temp() -> None:
    key = sidecar._point_identity_from_row(
        {
            "流程阶段": "气路",
            "温箱目标温度C": -20.0,
            "目标二氧化碳浓度ppm": 0.0,
        }
    )
    rows = sidecar._merge_summary_rows(
        {key: {"source_run": "run-a", "point_row": "3", "phase": "co2"}},
        {
            "run-a": [
                {
                    "Analyzer": "GA01",
                    "PhaseKey": "co2",
                    "PointPhase": "气路",
                    "TempSet": float("nan"),
                    "EnvTempC": -20.0,
                    "ppm_CO2_Tank": 0.0,
                    "PointRow": 3,
                }
            ]
        },
    )

    assert len(rows) == 1
    assert rows[0]["PointRow"] == 3


def test_merge_summary_rows_falls_back_to_point_row_for_h2o_rows_without_targets() -> None:
    key = sidecar._point_identity_from_row(
        {
            "流程阶段": "h2o",
            "温箱目标温度C": 0.0,
            "湿度发生器_目标温度(℃)": 0.0,
            "湿度发生器_目标湿度(%RH)": 50.0,
        }
    )
    rows = sidecar._merge_summary_rows(
        {key: {"source_run": "run-a", "point_row": "9", "phase": "h2o"}},
        {
            "run-a": [
                {
                    "Analyzer": "GA01",
                    "PointRow": 9,
                    "PointPhase": "水路",
                    "TempSet": float("nan"),
                    "EnvTempC": 0.0,
                    "HgenTempSet": float("nan"),
                    "HgenRhSet": float("nan"),
                }
            ]
        },
    )

    assert len(rows) == 1
    assert rows[0]["PointRow"] == 9
    assert rows[0]["PointPhase"] == "水路"


def test_sidecar_temperature_write_enters_mode_once(monkeypatch, tmp_path: Path) -> None:
    calls: list[tuple[str, object]] = []

    class _FakeAnalyzer:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def open(self) -> None:
            calls.append(("open", None))

        def set_mode(self, mode: int) -> bool:
            calls.append(("mode", int(mode)))
            return True

        def set_senco(self, index: int, *coeffs: float) -> bool:
            calls.append((f"senco{index}", tuple(coeffs)))
            return True

        def close(self) -> None:
            calls.append(("close", None))

    class _FakeLogger:
        def __init__(self, path: Path) -> None:
            self.path = path

        def close(self) -> None:
            return None

    monkeypatch.setattr(sidecar, "_read_coefficient_groups_for_targets", lambda **kwargs: [])
    monkeypatch.setattr(
        sidecar,
        "load_download_targets",
        lambda _config_path: [SimpleNamespace(analyzer="GA01", port="COM1", baudrate=115200, timeout=1.0, device_id="001")],
    )
    monkeypatch.setattr(sidecar, "_resolve_gas_analyzer_class", lambda: _FakeAnalyzer)
    monkeypatch.setattr(sidecar, "CsvIoLogger", _FakeLogger)

    payload = sidecar._write_temperature_compensation(
        config_path="dummy.json",
        temperature_rows=_temperature_rows(),
        output_dir=tmp_path,
    )

    assert payload["write_rows"][0]["WriteOk"] is True
    assert calls == [
        ("open", None),
        ("mode", 2),
        ("senco7", (0.0, 1.0, 0.0, 0.0)),
        ("senco8", (0.1, 1.1, 0.0, 0.0)),
        ("mode", 1),
        ("close", None),
    ]


def test_sidecar_temperature_write_attempts_mode_exit_after_failure(monkeypatch, tmp_path: Path) -> None:
    calls: list[tuple[str, object]] = []

    class _FakeAnalyzer:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def open(self) -> None:
            calls.append(("open", None))

        def set_mode(self, mode: int) -> bool:
            calls.append(("mode", int(mode)))
            return True

        def set_senco(self, index: int, *coeffs: float) -> bool:
            calls.append((f"senco{index}", tuple(coeffs)))
            raise RuntimeError("write failed")

        def close(self) -> None:
            calls.append(("close", None))

    class _FakeLogger:
        def __init__(self, path: Path) -> None:
            self.path = path

        def close(self) -> None:
            return None

    monkeypatch.setattr(sidecar, "_read_coefficient_groups_for_targets", lambda **kwargs: [])
    monkeypatch.setattr(
        sidecar,
        "load_download_targets",
        lambda _config_path: [SimpleNamespace(analyzer="GA01", port="COM1", baudrate=115200, timeout=1.0, device_id="001")],
    )
    monkeypatch.setattr(sidecar, "_resolve_gas_analyzer_class", lambda: _FakeAnalyzer)
    monkeypatch.setattr(sidecar, "CsvIoLogger", _FakeLogger)

    payload = sidecar._write_temperature_compensation(
        config_path="dummy.json",
        temperature_rows=_temperature_rows()[:1],
        output_dir=tmp_path,
    )

    assert payload["write_rows"][0]["Error"] == "write failed"
    assert calls == [
        ("open", None),
        ("mode", 2),
        ("senco7", (0.0, 1.0, 0.0, 0.0)),
        ("mode", 1),
        ("close", None),
    ]
