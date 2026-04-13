import csv
import json
import types
from pathlib import Path

import pytest

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger, _field_label
from gas_calibrator.workflow.runner import CalibrationRunner


def _point(index: int, *, co2_ppm: float) -> CalibrationPoint:
    return CalibrationPoint(
        index=index,
        temp_chamber_c=20.0,
        co2_ppm=co2_ppm,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )


def _h2o_point(index: int, *, h2o_mmol: float) -> CalibrationPoint:
    return CalibrationPoint(
        index=index,
        temp_chamber_c=20.0,
        co2_ppm=None,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1000.0,
        dewpoint_c=-5.0,
        h2o_mmol=h2o_mmol,
        raw_h2o="fixture",
    )


def _runner_cfg(*, sencos: dict | None = None) -> dict:
    return {
        "workflow": {
            "sampling": {
                "stable_count": 2,
                "interval_s": 0.0,
                "quality": {"enabled": False},
            }
        },
        "coefficients": {
            "enabled": True,
            "auto_fit": False,
            "sencos": sencos or {},
        },
    }


class _FakeWritebackAnalyzer:
    def __init__(
        self,
        *,
        before_by_group: dict[int, list[float]],
        mismatch_groups: set[int] | None = None,
        fail_on_target_groups: set[int] | None = None,
    ) -> None:
        self.before_by_group = {int(group): [float(v) for v in values] for group, values in before_by_group.items()}
        self.values = {int(group): list(values) for group, values in self.before_by_group.items()}
        self.mismatch_groups = {int(group) for group in set(mismatch_groups or set())}
        self.fail_on_target_groups = {int(group) for group in set(fail_on_target_groups or set())}
        self.mode = 1
        self.device_id = "086"
        self.mode_calls: list[int] = []
        self.write_calls: list[tuple[int, tuple[float, ...]]] = []

    def read_current_mode_snapshot(self):
        return {"mode": self.mode, "id": self.device_id, "raw": f"mode={self.mode}"}

    def set_mode_with_ack(self, mode: int, *, require_ack: bool = True) -> bool:
        self.mode = int(mode)
        self.mode_calls.append(int(mode))
        return True

    def set_senco(self, group: int, *coeffs) -> bool:
        values = list(coeffs[0]) if len(coeffs) == 1 and isinstance(coeffs[0], (list, tuple)) else list(coeffs)
        group = int(group)
        self.write_calls.append((group, tuple(float(value) for value in values)))
        if group in self.fail_on_target_groups and values != self.before_by_group.get(group):
            raise RuntimeError(f"boom-{group}")
        self.values[group] = [float(value) for value in values]
        return True

    def read_coefficient_group(self, group: int):
        group = int(group)
        values = list(self.values[group])
        if group in self.mismatch_groups and values != self.before_by_group.get(group):
            values = [float(value) + 0.5 for value in values]
        return {f"C{idx}": float(value) for idx, value in enumerate(values)}


def test_postrun_corrected_delivery_defaults_safe(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)

    resolved = runner._effective_postrun_corrected_delivery_cfg()

    logger.close()
    assert resolved["enabled"] is False
    assert resolved["write_devices"] is False
    assert resolved["_resolved_sources"]["enabled"] == "default"
    assert resolved["_resolved_sources"]["write_devices"] == "default"


def test_postrun_corrected_delivery_allows_explicit_env_enable(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("GAS_CAL_POSTRUN_CORRECTED_DELIVERY_ENABLED", "1")
    monkeypatch.setenv("GAS_CAL_ALLOW_REAL_DEVICE_WRITE", "1")
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)

    resolved = runner._effective_postrun_corrected_delivery_cfg()

    logger.close()
    assert resolved["enabled"] is True
    assert resolved["write_devices"] is True
    assert resolved["_resolved_sources"]["enabled"] == "ENV:GAS_CAL_POSTRUN_CORRECTED_DELIVERY_ENABLED"
    assert resolved["_resolved_sources"]["write_devices"] == "ENV:GAS_CAL_ALLOW_REAL_DEVICE_WRITE"


def test_h2o_zero_span_capability_defaults_not_supported(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)

    payload = runner._h2o_zero_span_capability_payload([_h2o_point(1, h2o_mmol=2.0)])

    logger.close()
    assert payload["status"] == "NOT_SUPPORTED"
    assert payload["has_h2o_points"] is True
    assert payload["require_supported_capability"] is False


def test_h2o_zero_span_requirement_fails_fast(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "coefficients": {
                "h2o_zero_span": {
                    "status": "not_supported",
                    "require_supported_capability": True,
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )

    with pytest.raises(RuntimeError, match="Current HEAD V1 only supports the CO2 main chain; H2O zero/span is NOT_SUPPORTED"):
        runner._require_supported_h2o_zero_span_if_requested([_h2o_point(1, h2o_mmol=2.0)])
    logger.close()


def test_runner_writeback_success_reads_before_and_after(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    analyzer = _FakeWritebackAnalyzer(
        before_by_group={
            1: [10.0, 20.0, 30.0, 40.0, 0.0, 0.0],
            7: [0.0, 1.0, 0.0, 0.0],
        }
    )
    runner = CalibrationRunner(
        _runner_cfg(
            sencos={
                "1": {"values": [1.0, 2.0, 3.0, 4.0, 0.0, 0.0]},
                "7": {"A": 0.1, "B": 1.1, "C": 0.0, "D": 0.0},
            }
        ),
        {"gas_analyzer": analyzer},
        logger,
        lambda *_: None,
        lambda *_: None,
    )

    runner._maybe_write_coefficients()
    logger.close()

    assert analyzer.mode_calls == [2, 1]
    with logger.coefficient_write_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 2
    assert {row["senco_group"] for row in rows} == {"1", "7"}
    assert all(row["write_status"] == "success" for row in rows)
    assert all(row["verify_status"] == "success" for row in rows)
    assert all(row["rollback_status"] == "not_needed" for row in rows)
    assert all(row["mode_before"] == "1" for row in rows)
    assert all(row["mode_after"] == "1" for row in rows)
    first = rows[0]
    assert json.loads(first["coeff_before"])
    assert json.loads(first["coeff_target"])
    assert json.loads(first["coeff_readback"])


def test_runner_writeback_mismatch_triggers_rollback_and_restore(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    analyzer = _FakeWritebackAnalyzer(
        before_by_group={1: [10.0, 20.0, 30.0, 40.0, 0.0, 0.0]},
        mismatch_groups={1},
    )
    runner = CalibrationRunner(
        _runner_cfg(sencos={"1": {"values": [1.0, 2.0, 3.0, 4.0, 0.0, 0.0]}}),
        {"gas_analyzer": analyzer},
        logger,
        lambda *_: None,
        lambda *_: None,
    )

    with pytest.raises(RuntimeError, match="Coefficient writeback failed"):
        runner._maybe_write_coefficients()
    logger.close()

    assert analyzer.mode_calls == [2, 1]
    assert analyzer.write_calls[0][0] == 1
    assert analyzer.write_calls[0][1] == (1.0, 2.0, 3.0, 4.0, 0.0, 0.0)
    assert analyzer.write_calls[-1][1] == (10.0, 20.0, 30.0, 40.0, 0.0, 0.0)
    with logger.coefficient_write_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    row = rows[0]
    assert row["verify_status"] == "failed"
    assert row["rollback_status"] == "success"
    assert row["mode_after"] == "1"
    assert "READBACK_MISMATCH" in row["failure_reason"]


def test_runner_write_exception_restores_mode_in_finally(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    analyzer = _FakeWritebackAnalyzer(
        before_by_group={1: [10.0, 20.0, 30.0, 40.0, 0.0, 0.0]},
        fail_on_target_groups={1},
    )
    runner = CalibrationRunner(
        _runner_cfg(sencos={"1": {"values": [1.0, 2.0, 3.0, 4.0, 0.0, 0.0]}}),
        {"gas_analyzer": analyzer},
        logger,
        lambda *_: None,
        lambda *_: None,
    )

    with pytest.raises(RuntimeError, match="Coefficient writeback failed"):
        runner._maybe_write_coefficients()
    logger.close()

    assert analyzer.mode_calls == [2, 1]
    with logger.coefficient_write_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    row = rows[0]
    assert row["mode_after"] == "1"
    assert row["rollback_status"] == "success"
    assert "boom-1" in row["failure_reason"]


def test_point_export_rows_include_traceability_fields_and_do_not_overwrite(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(_runner_cfg(), {}, logger, lambda *_: None, lambda *_: None)

    samples_by_point = {
        1: [
            {
                "point_title": "P1",
                "point_row": 1,
                "sample_ts": "2026-04-12T10:00:00.000",
                "sample_end_ts": "2026-04-12T10:00:00.100",
                "co2_ppm": 399.5,
                "pressure_hpa": 1000.1,
                "id": "086",
            },
            {
                "point_title": "P1",
                "point_row": 1,
                "sample_ts": "2026-04-12T10:00:01.000",
                "sample_end_ts": "2026-04-12T10:00:01.100",
                "co2_ppm": 400.5,
                "pressure_hpa": 1000.2,
                "id": "086",
            },
        ],
        2: [
            {
                "point_title": "P2",
                "point_row": 2,
                "sample_ts": "2026-04-12T10:01:00.000",
                "sample_end_ts": "2026-04-12T10:01:00.100",
                "co2_ppm": 499.5,
                "pressure_hpa": 1000.1,
                "id": "086",
            },
            {
                "point_title": "P2",
                "point_row": 2,
                "sample_ts": "2026-04-12T10:01:01.000",
                "sample_end_ts": "2026-04-12T10:01:01.100",
                "co2_ppm": 500.5,
                "pressure_hpa": 1000.2,
                "id": "086",
            },
        ],
    }

    runner._collect_samples = types.MethodType(
        lambda self, point, *_args, **_kwargs: [dict(row) for row in samples_by_point[point.index]],
        runner,
    )

    runner._sample_and_log(_point(1, co2_ppm=400.0), phase="co2")
    runner._sample_and_log(_point(2, co2_ppm=500.0), phase="co2")
    logger.close()

    with logger.points_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 2

    for row, expected_point, expected_target, expected_measured in (
        (rows[0], "1", "400.0", "400.0"),
        (rows[1], "2", "500.0", "500.0"),
    ):
        assert row[_field_label("point_no")] == expected_point
        assert row[_field_label("step")] == "co2"
        assert row[_field_label("gas_type")] == "CO2"
        assert row[_field_label("target_value")] == expected_target
        assert row[_field_label("measured_value")] == expected_measured
        assert row[_field_label("sample_ts")]
        assert row[_field_label("save_ts")]
        assert row[_field_label("device_id")] == "086"
        assert row[_field_label("sample_count")] == "2"
