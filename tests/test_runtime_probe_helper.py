from __future__ import annotations

import json
from pathlib import Path

from gas_calibrator.tools.runtime_probe_helper import capture_baseline_ygas_stream


class _StubGasAnalyzer:
    def __init__(self, *_args, **_kwargs) -> None:
        self._index = 0
        self._frames = [
            {
                "raw": "YGAS,079,0754.993,00.000,0.99,0.99,028.74,104.28,0001,2786",
                "parsed": {
                    "mode": 1,
                    "id": "079",
                    "co2_ppm": 754.993,
                    "h2o_mmol": 0.0,
                    "co2_sig": 0.99,
                    "h2o_sig": 0.99,
                    "temp_c": 28.74,
                    "pressure_kpa": 104.28,
                    "status": "0001",
                },
            },
            {
                "raw": "YGAS,079,0755.330,00.000,0.99,0.99,028.75,104.27,0001,2772",
                "parsed": {
                    "mode": 1,
                    "id": "079",
                    "co2_ppm": 755.330,
                    "h2o_mmol": 0.0,
                    "co2_sig": 0.99,
                    "h2o_sig": 0.99,
                    "temp_c": 28.75,
                    "pressure_kpa": 104.27,
                    "status": "0001",
                },
            },
        ]
        self._current = self._frames[0]

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    def read_latest_data(self, **_kwargs) -> str:
        frame = self._frames[min(self._index, len(self._frames) - 1)]
        self._current = frame
        self._index += 1
        return str(frame["raw"])

    def parse_line(self, _raw_line: str):
        return dict(self._current["parsed"])


def test_capture_baseline_ygas_stream_marks_legacy_probe_as_inconclusive(tmp_path: Path) -> None:
    out_dir = tmp_path / "probe"

    result = capture_baseline_ygas_stream(
        port="COM39",
        device_id="079",
        output_dir=out_dir,
        capture_seconds=1.0,
        max_frames=2,
        poll_interval_s=0.0,
        device_factory=_StubGasAnalyzer,
    )

    assert result["stream_formats_seen"] == ["legacy"]
    assert result["legacy_stream_only"] is True
    assert result["parity_verdict"] == "parity_inconclusive_missing_runtime_inputs"
    assert result["runtime_parity_quality"] == "parity_inconclusive_missing_runtime_inputs"
    assert result["final_write_ready"] is False
    assert result["readiness_reason"] == "legacy_stream_insufficient_for_runtime_parity"
    assert (out_dir / "baseline_stream_079.csv").exists()

    summary_payload = json.loads((out_dir / "baseline_stream_summary.json").read_text(encoding="utf-8"))
    assert summary_payload["visible_runtime_inputs_available"] == [
        "target_available",
        "legacy_signal_available",
        "temperature_available",
    ]
    assert "ratio_f_available" in summary_payload["visible_runtime_inputs_missing"]
