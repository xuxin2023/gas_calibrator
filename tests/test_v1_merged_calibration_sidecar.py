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
