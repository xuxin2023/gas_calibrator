from __future__ import annotations

import csv
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger, _field_label
from gas_calibrator.workflow.runner import CalibrationRunner


def _point_co2_low_pressure() -> CalibrationPoint:
    return CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=500.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=700.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
        co2_group="B",
    )


def _sampling_rows(values: list[float]) -> list[dict]:
    start = datetime(2026, 4, 4, 10, 0, 0)
    rows: list[dict] = []
    for idx, value in enumerate(values):
        ts = start + timedelta(seconds=idx)
        rows.append(
            {
                "sample_ts": ts.isoformat(timespec="milliseconds"),
                "sample_start_ts": ts.isoformat(timespec="milliseconds"),
                "dewpoint_live_c": value,
            }
        )
    return rows


def _co2_sampling_rows(values: list[float]) -> list[dict]:
    start = datetime(2026, 4, 4, 10, 0, 0)
    rows: list[dict] = []
    for idx, value in enumerate(values):
        ts = start + timedelta(seconds=idx)
        rows.append(
            {
                "sample_ts": ts.isoformat(timespec="milliseconds"),
                "sample_start_ts": ts.isoformat(timespec="milliseconds"),
                "sample_end_ts": (ts + timedelta(milliseconds=100)).isoformat(timespec="milliseconds"),
                "co2_ppm": value,
                "frame_usable": True,
                "id": "086",
            }
        )
    return rows


def _co2_sampling_rows_from_items(items: list[dict]) -> list[dict]:
    start = datetime(2026, 4, 4, 10, 0, 0)
    rows: list[dict] = []
    for idx, item in enumerate(items):
        ts_value = item.get("_sample_ts")
        include_ts = item.get("_include_ts", True)
        if isinstance(ts_value, datetime):
            ts = ts_value
        else:
            ts = start + timedelta(seconds=idx if ts_value is None else float(ts_value))
        row = {}
        if include_ts:
            row.update(
                {
                    "sample_ts": ts.isoformat(timespec="milliseconds"),
                    "sample_start_ts": ts.isoformat(timespec="milliseconds"),
                    "sample_end_ts": (ts + timedelta(milliseconds=100)).isoformat(timespec="milliseconds"),
                }
            )
        row.update(item)
        row.pop("_sample_ts", None)
        row.pop("_include_ts", None)
        rows.append(row)
    return rows


def test_evaluate_co2_sampling_window_qc_passes_for_stable_window(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "co2_sampling_window_qc_enabled": True,
                    "co2_sampling_window_qc_max_range_c": 0.20,
                    "co2_sampling_window_qc_max_rise_c": 0.12,
                    "co2_sampling_window_qc_max_abs_slope_c_per_s": 0.02,
                    "co2_sampling_window_qc_policy": "reject",
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2_low_pressure()

    result = runner._evaluate_co2_sampling_window_qc(
        point,
        phase="co2",
        samples=_sampling_rows(
            [-24.50, -24.49, -24.48, -24.47, -24.47, -24.46, -24.45, -24.45, -24.44, -24.43]
        ),
    )
    logger.close()

    assert result["sampling_window_dewpoint_first_c"] == -24.5
    assert result["sampling_window_dewpoint_last_c"] == -24.43
    assert result["sampling_window_dewpoint_range_c"] == 0.07
    assert result["sampling_window_dewpoint_rise_c"] == 0.07
    assert pytest.approx(result["sampling_window_dewpoint_slope_c_per_s"], abs=1e-6) == 0.07 / 9.0
    assert result["sampling_window_qc_status"] == "pass"
    assert result["sampling_window_qc_reason"] == ""


@pytest.mark.parametrize(
    ("policy", "expected_status"),
    [
        ("warn", "warn"),
        ("reject", "fail"),
    ],
)
def test_evaluate_co2_sampling_window_qc_warns_or_fails_for_drifting_window(
    tmp_path: Path,
    policy: str,
    expected_status: str,
) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "co2_sampling_window_qc_enabled": True,
                    "co2_sampling_window_qc_max_range_c": 0.20,
                    "co2_sampling_window_qc_max_rise_c": 0.12,
                    "co2_sampling_window_qc_max_abs_slope_c_per_s": 0.02,
                    "co2_sampling_window_qc_policy": policy,
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2_low_pressure()

    result = runner._evaluate_co2_sampling_window_qc(
        point,
        phase="co2",
        samples=_sampling_rows(
            [-24.50, -24.44, -24.38, -24.31, -24.25, -24.18, -24.11, -24.05, -23.99, -23.90]
        ),
    )
    logger.close()

    assert result["sampling_window_dewpoint_first_c"] == -24.5
    assert result["sampling_window_dewpoint_last_c"] == -23.9
    assert result["sampling_window_dewpoint_range_c"] == 0.6
    assert result["sampling_window_dewpoint_rise_c"] == 0.6
    assert pytest.approx(result["sampling_window_dewpoint_slope_c_per_s"], abs=1e-6) == 0.6 / 9.0
    assert result["sampling_window_qc_status"] == expected_status
    assert "range_c=0.600>max_range_c=0.200" in result["sampling_window_qc_reason"]
    assert "rise_c=0.600>max_rise_c=0.120" in result["sampling_window_qc_reason"]
    assert "abs_slope_c_per_s=0.0667>max_abs_slope_c_per_s=0.0200" in result["sampling_window_qc_reason"]
    assert f"policy={policy}" in result["sampling_window_qc_reason"]


def test_evaluate_co2_steady_state_window_qc_prefers_latest_qualified_window(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
            {
                "workflow": {
                    "sampling": {
                        "interval_s": 1.0,
                        "co2_interval_s": 1.0,
                        "quality": {
                            "co2_steady_state_enabled": True,
                            "co2_steady_state_policy": "warn",
                            "co2_steady_state_min_samples": 4,
                        "co2_steady_state_fallback_samples": 4,
                        "co2_steady_state_max_std_ppm": 0.2,
                        "co2_steady_state_max_range_ppm": 0.4,
                        "co2_steady_state_max_abs_slope_ppm_per_s": 0.2,
                    }
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2_low_pressure()

    result = runner._evaluate_co2_steady_state_window_qc(
        point,
        phase="co2",
        samples=_co2_sampling_rows([400.0, 400.1, 399.9, 400.0, 450.0, 480.0, 500.0, 500.2, 499.9, 500.1]),
    )
    logger.close()

    assert result["co2_steady_window_found"] is True
    assert result["co2_steady_window_status"] == "pass"
    assert result["co2_steady_window_start_sample_index"] == 7
    assert result["co2_steady_window_end_sample_index"] == 10
    assert result["co2_steady_window_candidate_count"] >= 2
    assert result["measured_value_source"] == "co2_steady_state_window"
    assert pytest.approx(result["co2_representative_value"], abs=1e-6) == 500.05


def test_evaluate_co2_steady_state_window_qc_rejects_obvious_drift(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
            {
                "workflow": {
                    "sampling": {
                        "interval_s": 1.0,
                        "co2_interval_s": 1.0,
                        "quality": {
                            "co2_steady_state_enabled": True,
                            "co2_steady_state_policy": "reject",
                            "co2_steady_state_min_samples": 4,
                        "co2_steady_state_fallback_samples": 4,
                        "co2_steady_state_max_std_ppm": 2.0,
                        "co2_steady_state_max_range_ppm": 4.0,
                        "co2_steady_state_max_abs_slope_ppm_per_s": 1.0,
                    }
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2_low_pressure()

    result = runner._evaluate_co2_steady_state_window_qc(
        point,
        phase="co2",
        samples=_co2_sampling_rows([100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0, 800.0, 900.0, 1000.0]),
    )
    logger.close()

    assert result["co2_steady_window_found"] is False
    assert result["co2_steady_window_status"] == "fail"
    assert "no_qualified_steady_state_window" in result["co2_steady_window_reason"]
    assert "fallback=trailing_window" in result["co2_steady_window_reason"]
    assert result["measured_value_source"] == "co2_trailing_window_fallback"


def test_evaluate_co2_steady_state_window_qc_keeps_clean_data_near_legacy_baseline(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    stable_rows = _co2_sampling_rows([500.0, 500.1, 499.9, 500.0, 500.1, 499.9])
    base_cfg = {
        "workflow": {
            "sampling": {
                "interval_s": 1.0,
                "co2_interval_s": 1.0,
                "quality": {
                    "co2_steady_state_enabled": True,
                    "co2_steady_state_policy": "warn",
                    "co2_steady_state_min_samples": 4,
                    "co2_steady_state_fallback_samples": 4,
                    "co2_steady_state_max_std_ppm": 0.2,
                    "co2_steady_state_max_range_ppm": 0.4,
                    "co2_steady_state_max_abs_slope_ppm_per_s": 0.2,
                }
            }
        }
    }
    legacy_runner = CalibrationRunner(
        {
            "workflow": {
                "sampling": {
                    "interval_s": 1.0,
                    "co2_interval_s": 1.0,
                    "quality": {
                        **base_cfg["workflow"]["sampling"]["quality"],
                        "co2_bad_frame_quarantine_enabled": False,
                        "co2_source_trust_enabled": False,
                    },
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2_low_pressure()
    legacy = legacy_runner._evaluate_co2_steady_state_window_qc(point, phase="co2", samples=stable_rows)

    runner = CalibrationRunner(base_cfg, {}, logger, lambda *_: None, lambda *_: None)
    current = runner._evaluate_co2_steady_state_window_qc(point, phase="co2", samples=stable_rows)
    logger.close()

    assert pytest.approx(current["co2_representative_value"], abs=1e-9) == legacy["co2_representative_value"]
    assert current["co2_rows_before_quarantine"] == 6
    assert current["co2_rows_after_quarantine"] == 6
    assert current["co2_bad_frame_count"] == 0
    assert current["co2_soft_warn_count"] == 0
    assert current["co2_source_selected"] == "primary"


def test_evaluate_co2_steady_state_window_qc_quarantines_bad_frame_before_window_selection(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    cfg = {
        "workflow": {
            "sampling": {
                "interval_s": 1.0,
                "co2_interval_s": 1.0,
                "quality": {
                    "co2_steady_state_enabled": True,
                    "co2_steady_state_policy": "warn",
                    "co2_steady_state_min_samples": 4,
                    "co2_steady_state_fallback_samples": 4,
                    "co2_steady_state_max_std_ppm": 0.2,
                    "co2_steady_state_max_range_ppm": 0.4,
                    "co2_steady_state_max_abs_slope_ppm_per_s": 0.2,
                }
            }
        }
    }
    legacy_runner = CalibrationRunner(
        {
            "workflow": {
                "sampling": {
                    "interval_s": 1.0,
                    "co2_interval_s": 1.0,
                    "quality": {
                        **cfg["workflow"]["sampling"]["quality"],
                        "co2_bad_frame_quarantine_enabled": False,
                        "co2_source_trust_enabled": False,
                    },
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)
    point = _point_co2_low_pressure()
    rows = _co2_sampling_rows([500.0, 500.1, 999999.0, 500.0, 499.9])

    legacy = legacy_runner._evaluate_co2_steady_state_window_qc(point, phase="co2", samples=rows)
    current = runner._evaluate_co2_steady_state_window_qc(point, phase="co2", samples=rows)
    logger.close()

    assert legacy["measured_value_source"] == "co2_trailing_window_fallback"
    assert pytest.approx(legacy["co2_representative_value"], rel=0.0, abs=1e-3) == 250374.75
    assert current["measured_value_source"] == "co2_steady_state_window"
    assert current["co2_bad_frame_count"] == 1
    assert current["co2_rows_before_quarantine"] == 5
    assert current["co2_rows_after_quarantine"] == 4
    assert "co2_value_sentinel" in current["co2_quarantine_reason_summary"]
    assert pytest.approx(current["co2_representative_value"], abs=1e-6) == 500.0
    assert rows[2]["co2_bad_frame"] is True
    assert "co2_value_sentinel" in rows[2]["co2_bad_frame_reason"]


def test_evaluate_co2_steady_state_window_qc_phase_gate_excludes_head_transition_before_window(
    tmp_path: Path,
) -> None:
    logger = RunLogger(tmp_path)
    cfg = {
        "workflow": {
            "sampling": {
                "interval_s": 1.0,
                "co2_interval_s": 1.0,
                "quality": {
                    "co2_steady_state_enabled": True,
                    "co2_steady_state_policy": "warn",
                    "co2_steady_state_min_samples": 4,
                    "co2_steady_state_fallback_samples": 4,
                    "co2_steady_state_max_std_ppm": 0.2,
                    "co2_steady_state_max_range_ppm": 0.4,
                    "co2_steady_state_max_abs_slope_ppm_per_s": 0.2,
                    "co2_phase_contract_enabled": True,
                    "co2_phase_head_exclusion_max_rows": 2,
                },
            }
        }
    }
    runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)
    point = _point_co2_low_pressure()
    rows = _co2_sampling_rows([900.0, 500.0, 500.1, 499.9, 500.0, 500.1])

    result = runner._evaluate_co2_steady_state_window_qc(point, phase="co2", samples=rows)
    logger.close()

    assert result["co2_candidate_rows_before_phase_gate"] == 6
    assert result["co2_candidate_rows_after_phase_gate"] == 5
    assert result["co2_candidate_rows_after_quarantine"] == 5
    assert result["co2_phase_excluded_count"] == 1
    assert "transition_head_delta_gt_threshold" in result["co2_phase_reason_summary"]
    assert rows[0]["co2_phase_label"] == "transition_excluded"
    assert rows[1]["co2_phase_label"] == "acquisition_selected"
    assert pytest.approx(result["co2_representative_value"], abs=1e-6) == 500.02


def test_evaluate_co2_steady_state_window_qc_keeps_clean_data_equal_to_hardened_baseline_with_phase_gate(
    tmp_path: Path,
) -> None:
    logger = RunLogger(tmp_path)
    stable_rows = _co2_sampling_rows([500.0, 500.1, 499.9, 500.0, 500.1, 499.9])
    cfg = {
        "workflow": {
            "sampling": {
                "interval_s": 1.0,
                "co2_interval_s": 1.0,
                "quality": {
                    "co2_steady_state_enabled": True,
                    "co2_steady_state_policy": "warn",
                    "co2_steady_state_min_samples": 4,
                    "co2_steady_state_fallback_samples": 4,
                    "co2_steady_state_max_std_ppm": 0.2,
                    "co2_steady_state_max_range_ppm": 0.4,
                    "co2_steady_state_max_abs_slope_ppm_per_s": 0.2,
                },
            }
        }
    }
    baseline_runner = CalibrationRunner(
        {
            "workflow": {
                "sampling": {
                    "interval_s": 1.0,
                    "co2_interval_s": 1.0,
                    "quality": {
                        **cfg["workflow"]["sampling"]["quality"],
                        "co2_phase_contract_enabled": False,
                    },
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    current_runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)
    point = _point_co2_low_pressure()

    baseline = baseline_runner._evaluate_co2_steady_state_window_qc(point, phase="co2", samples=list(stable_rows))
    current = current_runner._evaluate_co2_steady_state_window_qc(point, phase="co2", samples=list(stable_rows))
    logger.close()

    assert pytest.approx(current["co2_representative_value"], abs=1e-9) == baseline["co2_representative_value"]
    assert current["co2_phase_excluded_count"] == 0
    assert current["co2_candidate_rows_before_phase_gate"] == 6
    assert current["co2_candidate_rows_after_phase_gate"] == 6


def test_evaluate_co2_steady_state_window_qc_phase_gate_degrades_cleanly_when_no_usable_rows(
    tmp_path: Path,
) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "sampling": {
                    "interval_s": 1.0,
                    "co2_interval_s": 1.0,
                    "quality": {
                        "co2_steady_state_enabled": True,
                        "co2_steady_state_policy": "warn",
                        "co2_steady_state_min_samples": 4,
                        "co2_steady_state_fallback_samples": 4,
                        "co2_steady_state_max_std_ppm": 0.2,
                        "co2_steady_state_max_range_ppm": 0.4,
                        "co2_steady_state_max_abs_slope_ppm_per_s": 0.2,
                        "co2_phase_contract_enabled": True,
                    },
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2_low_pressure()
    rows = _co2_sampling_rows_from_items(
        [
            {"co2_ppm": None, "frame_usable": False, "frame_status": "NO_RESPONSE"},
            {"co2_ppm": None, "frame_usable": False, "frame_status": "NO_RESPONSE"},
            {"co2_ppm": None, "frame_usable": False, "frame_status": "NO_RESPONSE"},
            {"co2_ppm": None, "frame_usable": False, "frame_status": "NO_RESPONSE"},
        ]
    )

    result = runner._evaluate_co2_steady_state_window_qc(point, phase="co2", samples=rows)
    logger.close()

    assert result["measured_value_source"] == "co2_no_trusted_source"
    assert result["co2_candidate_rows_before_phase_gate"] == 4
    assert result["co2_candidate_rows_after_phase_gate"] == 0
    assert result["co2_phase_excluded_count"] == 4
    assert "no_usable_rows_before_phase_gate" in result["co2_phase_contract_reason"]
    assert "presample_not_usable_or_missing_value" in result["co2_phase_reason_summary"]


def test_evaluate_co2_steady_state_window_qc_phase_gate_can_force_primary_fallback(
    tmp_path: Path,
) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "sampling": {
                    "interval_s": 1.0,
                    "co2_interval_s": 1.0,
                    "quality": {
                        "co2_steady_state_enabled": True,
                        "co2_steady_state_policy": "warn",
                        "co2_steady_state_min_samples": 4,
                        "co2_steady_state_fallback_samples": 4,
                        "co2_steady_state_max_std_ppm": 0.2,
                        "co2_steady_state_max_range_ppm": 0.4,
                        "co2_steady_state_max_abs_slope_ppm_per_s": 0.2,
                        "co2_phase_contract_enabled": True,
                        "co2_phase_head_exclusion_max_rows": 2,
                    },
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    runner._all_gas_analyzers = lambda: [("ga01", None, {}), ("ga02", None, {})]
    point = _point_co2_low_pressure()
    rows = _co2_sampling_rows_from_items(
        [
            {
                "co2_ppm": 900.0,
                "frame_usable": True,
                "frame_status": "可用",
                "ga01_co2_ppm": 900.0,
                "ga01_frame_usable": True,
                "ga01_frame_status": "可用",
                "ga02_co2_ppm": 620.0,
                "ga02_frame_usable": True,
                "ga02_frame_status": "可用",
            },
            {
                "co2_ppm": 500.0,
                "frame_usable": True,
                "frame_status": "可用",
                "ga01_co2_ppm": 500.0,
                "ga01_frame_usable": True,
                "ga01_frame_status": "可用",
                "ga02_co2_ppm": 620.1,
                "ga02_frame_usable": True,
                "ga02_frame_status": "可用",
            },
            {
                "co2_ppm": 500.1,
                "frame_usable": True,
                "frame_status": "可用",
                "ga01_co2_ppm": 500.1,
                "ga01_frame_usable": True,
                "ga01_frame_status": "可用",
                "ga02_co2_ppm": 619.9,
                "ga02_frame_usable": True,
                "ga02_frame_status": "可用",
            },
            {
                "co2_ppm": 499.9,
                "frame_usable": True,
                "frame_status": "可用",
                "ga01_co2_ppm": 499.9,
                "ga01_frame_usable": True,
                "ga01_frame_status": "可用",
                "ga02_co2_ppm": 620.0,
                "ga02_frame_usable": True,
                "ga02_frame_status": "可用",
            },
        ]
    )

    result = runner._evaluate_co2_steady_state_window_qc(point, phase="co2", samples=rows)
    logger.close()

    assert result["co2_source_selected"] == "ga02"
    assert "primary_lost_to=ga02" in result["co2_source_switch_reason"]
    assert "rows_after_phase_gate=3" in result["co2_source_switch_reason"]
    assert "rows_after_quarantine=3<min_samples=4" in result["co2_source_switch_reason"]
    assert pytest.approx(result["co2_representative_value"], abs=1e-6) == 620.0


def test_evaluate_co2_steady_state_window_qc_keeps_clean_data_equal_to_hardened_baseline_with_segment_contract(
    tmp_path: Path,
) -> None:
    logger = RunLogger(tmp_path)
    stable_rows = _co2_sampling_rows([500.0, 500.1, 499.9, 500.0, 500.1, 499.9])
    cfg = {
        "workflow": {
            "sampling": {
                "interval_s": 1.0,
                "co2_interval_s": 1.0,
                "quality": {
                    "co2_steady_state_enabled": True,
                    "co2_steady_state_policy": "warn",
                    "co2_steady_state_min_samples": 4,
                    "co2_steady_state_fallback_samples": 4,
                    "co2_steady_state_max_std_ppm": 0.2,
                    "co2_steady_state_max_range_ppm": 0.4,
                    "co2_steady_state_max_abs_slope_ppm_per_s": 0.2,
                },
            }
        }
    }
    baseline_runner = CalibrationRunner(
        {
            "workflow": {
                "sampling": {
                    "interval_s": 1.0,
                    "co2_interval_s": 1.0,
                    "quality": {
                        **cfg["workflow"]["sampling"]["quality"],
                        "co2_source_segment_contract_enabled": False,
                    },
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    current_runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)
    point = _point_co2_low_pressure()

    baseline = baseline_runner._evaluate_co2_steady_state_window_qc(point, phase="co2", samples=list(stable_rows))
    current = current_runner._evaluate_co2_steady_state_window_qc(point, phase="co2", samples=list(stable_rows))
    logger.close()

    assert pytest.approx(current["co2_representative_value"], abs=1e-9) == baseline["co2_representative_value"]
    assert current["co2_source_selected"] == "primary"
    assert current["co2_candidate_rows_before_segment_gate"] == 6
    assert current["co2_candidate_rows_after_segment_gate"] == 6
    assert current["co2_source_segment_selected"] == "primary#1"
    assert current["co2_source_segment_settle_excluded_count"] == 0


def test_evaluate_co2_steady_state_window_qc_segment_contract_can_force_primary_segment_fallback(
    tmp_path: Path,
) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "sampling": {
                    "interval_s": 1.0,
                    "co2_interval_s": 1.0,
                    "quality": {
                        "co2_steady_state_enabled": True,
                        "co2_steady_state_policy": "warn",
                        "co2_steady_state_min_samples": 4,
                        "co2_steady_state_fallback_samples": 4,
                        "co2_steady_state_max_std_ppm": 0.2,
                        "co2_steady_state_max_range_ppm": 0.4,
                        "co2_steady_state_max_abs_slope_ppm_per_s": 0.2,
                        "co2_source_segment_contract_enabled": True,
                        "co2_source_segment_head_exclusion_rows": 1,
                        "co2_source_segment_min_rows_after_settle": 4,
                    }
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    runner._all_gas_analyzers = lambda: [("ga02", None, {})]
    point = _point_co2_low_pressure()
    rows = _co2_sampling_rows_from_items(
        [
            {"co2_ppm": 999999.0, "frame_usable": True, "frame_status": "可用", "ga02_co2_ppm": 620.0, "ga02_frame_usable": True, "ga02_frame_status": "可用"},
            {"co2_ppm": 999999.0, "frame_usable": True, "frame_status": "可用", "ga02_co2_ppm": 620.1, "ga02_frame_usable": True, "ga02_frame_status": "可用"},
            {"co2_ppm": 999999.0, "frame_usable": True, "frame_status": "可用", "ga02_co2_ppm": 619.9, "ga02_frame_usable": True, "ga02_frame_status": "可用"},
            {"co2_ppm": 999999.0, "frame_usable": True, "frame_status": "可用", "ga02_co2_ppm": 620.0, "ga02_frame_usable": True, "ga02_frame_status": "可用"},
            {"co2_ppm": None, "frame_usable": False, "frame_status": "NO_RESPONSE", "ga02_co2_ppm": 620.0, "ga02_frame_usable": True, "ga02_frame_status": "可用"},
            {"co2_ppm": 500.0, "frame_usable": True, "frame_status": "可用", "ga02_co2_ppm": 620.1, "ga02_frame_usable": True, "ga02_frame_status": "可用"},
            {"co2_ppm": 500.1, "frame_usable": True, "frame_status": "可用", "ga02_co2_ppm": 619.9, "ga02_frame_usable": True, "ga02_frame_status": "可用"},
            {"co2_ppm": 499.9, "frame_usable": True, "frame_status": "可用", "ga02_co2_ppm": 620.0, "ga02_frame_usable": True, "ga02_frame_status": "可用"},
            {"co2_ppm": 500.0, "frame_usable": True, "frame_status": "可用", "ga02_co2_ppm": 620.1, "ga02_frame_usable": True, "ga02_frame_status": "可用"},
        ]
    )

    result = runner._evaluate_co2_steady_state_window_qc(point, phase="co2", samples=rows)
    logger.close()

    assert result["co2_source_selected"] == "ga02"
    assert result["co2_source_segment_selected"] == "ga02#1"
    assert "primary_lost_to=ga02/ga02#1" in result["co2_source_switch_reason"]
    assert "primary_segment=primary#2" in result["co2_source_switch_reason"]
    assert "rows_after_segment_gate=3" in result["co2_source_switch_reason"]
    assert pytest.approx(result["co2_representative_value"], abs=1e-6) == 620.011111


def test_evaluate_co2_steady_state_window_qc_segment_contract_degrades_when_all_segments_untrusted(
    tmp_path: Path,
) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "sampling": {
                    "interval_s": 1.0,
                    "co2_interval_s": 1.0,
                    "quality": {
                        "co2_steady_state_enabled": True,
                        "co2_steady_state_policy": "warn",
                        "co2_steady_state_min_samples": 4,
                        "co2_steady_state_fallback_samples": 4,
                        "co2_steady_state_max_std_ppm": 0.2,
                        "co2_steady_state_max_range_ppm": 0.4,
                        "co2_steady_state_max_abs_slope_ppm_per_s": 0.2,
                        "co2_source_segment_contract_enabled": True,
                        "co2_source_segment_head_exclusion_rows": 1,
                        "co2_source_segment_min_rows_after_settle": 4,
                    }
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    runner._all_gas_analyzers = lambda: [("ga02", None, {})]
    point = _point_co2_low_pressure()
    rows = _co2_sampling_rows_from_items(
        [
            {"co2_ppm": 999999.0, "frame_usable": True, "frame_status": "可用", "ga02_co2_ppm": None, "ga02_frame_usable": False, "ga02_frame_status": "NO_RESPONSE"},
            {"co2_ppm": 999999.0, "frame_usable": True, "frame_status": "可用", "ga02_co2_ppm": None, "ga02_frame_usable": False, "ga02_frame_status": "NO_RESPONSE"},
            {"co2_ppm": None, "frame_usable": False, "frame_status": "NO_RESPONSE", "ga02_co2_ppm": None, "ga02_frame_usable": False, "ga02_frame_status": "NO_RESPONSE"},
            {"co2_ppm": 500.0, "frame_usable": True, "frame_status": "可用", "ga02_co2_ppm": 620.0, "ga02_frame_usable": True, "ga02_frame_status": "可用"},
            {"co2_ppm": 500.1, "frame_usable": True, "frame_status": "可用", "ga02_co2_ppm": 620.1, "ga02_frame_usable": True, "ga02_frame_status": "可用"},
            {"co2_ppm": 499.9, "frame_usable": True, "frame_status": "可用", "ga02_co2_ppm": 619.9, "ga02_frame_usable": True, "ga02_frame_status": "可用"},
        ]
    )

    result = runner._evaluate_co2_steady_state_window_qc(point, phase="co2", samples=rows)
    logger.close()

    assert result["measured_value_source"] == "co2_no_trusted_source"
    assert result["co2_steady_window_status"] == "warn"
    assert result["co2_source_selected"] is None
    assert result["co2_source_segment_selected"] == ""
    assert result["co2_source_trust_reason"] == "no_trusted_source_after_quarantine"


def test_evaluate_co2_steady_state_window_qc_keeps_clean_data_equal_to_hardened_baseline_with_temporal_contract(
    tmp_path: Path,
) -> None:
    logger = RunLogger(tmp_path)
    stable_rows = _co2_sampling_rows([500.0, 500.1, 499.9, 500.0, 500.1, 499.9])
    cfg = {
        "workflow": {
            "sampling": {
                "interval_s": 1.0,
                "co2_interval_s": 1.0,
                "quality": {
                    "co2_steady_state_enabled": True,
                    "co2_steady_state_policy": "warn",
                    "co2_steady_state_min_samples": 4,
                    "co2_steady_state_fallback_samples": 4,
                    "co2_steady_state_max_std_ppm": 0.2,
                    "co2_steady_state_max_range_ppm": 0.4,
                    "co2_steady_state_max_abs_slope_ppm_per_s": 0.2,
                    "co2_temporal_contract_enabled": True,
                    "co2_temporal_min_effective_dwell_seconds": 4.0,
                    "co2_temporal_min_cadence_coverage_ratio": 0.75,
                },
            }
        }
    }
    baseline_runner = CalibrationRunner(
        {
            "workflow": {
                "sampling": {
                    "interval_s": 1.0,
                    "co2_interval_s": 1.0,
                    "quality": {
                        **cfg["workflow"]["sampling"]["quality"],
                        "co2_temporal_contract_enabled": False,
                    },
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    current_runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)
    point = _point_co2_low_pressure()

    baseline = baseline_runner._evaluate_co2_steady_state_window_qc(point, phase="co2", samples=list(stable_rows))
    current = current_runner._evaluate_co2_steady_state_window_qc(point, phase="co2", samples=list(stable_rows))
    logger.close()

    assert pytest.approx(current["co2_representative_value"], abs=1e-9) == baseline["co2_representative_value"]
    assert current["co2_temporal_candidate_rows_before_gate"] == 6
    assert current["co2_temporal_candidate_rows_after_gate"] == 6
    assert current["co2_temporal_contract_status"] == "pass"
    assert current["co2_backward_timestamp_count"] == 0
    assert current["co2_large_gap_count"] == 0
    assert pytest.approx(float(current["co2_effective_dwell_seconds"]), abs=1e-9) == 5.0
    assert pytest.approx(float(current["co2_cadence_coverage_ratio"]), abs=1e-9) == 1.0
    assert current["co2_decision_waterfall_status"] == "pass"
    assert current["co2_decision_stage_count"] == 6
    assert (
        current["co2_decision_selected_stage_path"]
        == "phase:pass>source_segment:pass>temporal:pass>quarantine:pass>source_trust:pass>steady_state:pass"
    )
    lineage = current["co2_decision_lineage"]
    assert lineage["selected_source"] == "primary"
    assert lineage["selected_segment"].startswith("primary#")
    assert [stage["stage_name"] for stage in lineage["stages"]] == [
        "phase",
        "source_segment",
        "temporal",
        "quarantine",
        "source_trust",
        "steady_state",
    ]
    assert {stage["status"] for stage in lineage["stages"]} == {"pass"}
    assert current["co2_point_suitability_status"] == "fit"
    assert current["co2_calibration_candidate_recommended"] is True
    assert current["co2_calibration_candidate_hard_blocked"] is False
    assert pytest.approx(float(current["co2_calibration_weight_recommended"]), abs=1e-9) == 1.0
    assert pytest.approx(float(current["co2_evidence_score"]), abs=1e-9) == 100.0
    assert "waterfall=pass" in current["co2_point_suitability_reason_chain"]
    assert "fit_for_calibration" in current["co2_point_evidence_budget_reason"]


def test_evaluate_co2_steady_state_window_qc_temporal_contract_rejects_timestamp_rollback(
    tmp_path: Path,
) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "sampling": {
                    "interval_s": 1.0,
                    "co2_interval_s": 1.0,
                    "quality": {
                        "co2_steady_state_enabled": True,
                        "co2_steady_state_policy": "warn",
                        "co2_steady_state_min_samples": 4,
                        "co2_steady_state_fallback_samples": 4,
                        "co2_steady_state_max_std_ppm": 0.2,
                        "co2_steady_state_max_range_ppm": 0.4,
                        "co2_steady_state_max_abs_slope_ppm_per_s": 0.2,
                        "co2_temporal_contract_enabled": True,
                        "co2_temporal_min_effective_dwell_seconds": 4.0,
                    },
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2_low_pressure()
    rows = _co2_sampling_rows_from_items(
        [
            {"_sample_ts": 0.0, "co2_ppm": 500.0, "frame_usable": True, "frame_status": "可用"},
            {"_sample_ts": 1.0, "co2_ppm": 500.1, "frame_usable": True, "frame_status": "可用"},
            {"_sample_ts": 0.5, "co2_ppm": 499.9, "frame_usable": True, "frame_status": "可用"},
            {"_sample_ts": 2.0, "co2_ppm": 500.0, "frame_usable": True, "frame_status": "可用"},
            {"_sample_ts": 3.0, "co2_ppm": 500.1, "frame_usable": True, "frame_status": "可用"},
        ]
    )

    result = runner._evaluate_co2_steady_state_window_qc(point, phase="co2", samples=rows)
    logger.close()

    assert result["co2_temporal_contract_status"] == "fail"
    assert result["co2_backward_timestamp_count"] == 1
    assert result["co2_temporal_candidate_rows_before_gate"] == 5
    assert result["co2_temporal_candidate_rows_after_gate"] == 4
    assert rows[2]["co2_temporal_excluded"] is True
    assert "timestamp_rollback" in rows[2]["co2_temporal_reason"]
    assert result["measured_value_source"] == "co2_no_trusted_source"
    assert result["co2_representative_value"] is None
    assert "effective_dwell_seconds=3.000<min_effective_dwell_seconds=4.000" in result["co2_temporal_contract_reason"]
    assert result["co2_decision_waterfall_status"] == "fail"
    assert "source_trust=no_trusted_source_after_quarantine" in result["co2_decision_waterfall_reason"]
    assert "steady_state=no_trusted_source_after_quarantine;policy=warn;co2_no_trusted_source" in result[
        "co2_decision_waterfall_reason"
    ]
    lineage = result["co2_decision_lineage"]
    temporal_stage = next(stage for stage in lineage["stages"] if stage["stage_name"] == "temporal")
    assert temporal_stage["status"] == "fail"
    assert "timestamp_rollback" in temporal_stage["reason_summary"]
    assert "effective_dwell_seconds=3.000<min_effective_dwell_seconds=4.000" in temporal_stage["reason_summary"]
    source_stage = next(stage for stage in lineage["stages"] if stage["stage_name"] == "source_trust")
    assert source_stage["status"] == "fail"
    assert source_stage["reason_summary"] == "no_trusted_source_after_quarantine"
    assert result["co2_point_suitability_status"] == "unfit"
    assert result["co2_calibration_candidate_recommended"] is False
    assert result["co2_calibration_candidate_hard_blocked"] is True
    assert pytest.approx(float(result["co2_calibration_weight_recommended"]), abs=1e-9) == 0.0
    assert "temporal_failed" in result["co2_point_evidence_budget_reason"]
    assert "hard_blocked" in result["co2_point_evidence_budget_reason"]
    assert "waterfall=fail" in result["co2_point_suitability_reason_chain"]


def test_evaluate_co2_steady_state_window_qc_temporal_contract_rejects_large_gap_sparse_dwell(
    tmp_path: Path,
) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "sampling": {
                    "interval_s": 1.0,
                    "co2_interval_s": 1.0,
                    "quality": {
                        "co2_steady_state_enabled": True,
                        "co2_steady_state_policy": "warn",
                        "co2_steady_state_min_samples": 4,
                        "co2_steady_state_fallback_samples": 4,
                        "co2_steady_state_max_std_ppm": 0.2,
                        "co2_steady_state_max_range_ppm": 0.4,
                        "co2_steady_state_max_abs_slope_ppm_per_s": 0.2,
                        "co2_temporal_contract_enabled": True,
                        "co2_temporal_min_effective_dwell_seconds": 4.0,
                        "co2_temporal_max_gap_factor": 3.0,
                    },
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2_low_pressure()
    rows = _co2_sampling_rows_from_items(
        [
            {"_sample_ts": 0.0, "co2_ppm": 500.0, "frame_usable": True, "frame_status": "可用"},
            {"_sample_ts": 1.0, "co2_ppm": 500.1, "frame_usable": True, "frame_status": "可用"},
            {"_sample_ts": 2.0, "co2_ppm": 499.9, "frame_usable": True, "frame_status": "可用"},
            {"_sample_ts": 10.0, "co2_ppm": 500.0, "frame_usable": True, "frame_status": "可用"},
            {"_sample_ts": 11.0, "co2_ppm": 500.1, "frame_usable": True, "frame_status": "可用"},
            {"_sample_ts": 12.0, "co2_ppm": 499.9, "frame_usable": True, "frame_status": "可用"},
        ]
    )

    result = runner._evaluate_co2_steady_state_window_qc(point, phase="co2", samples=rows)
    logger.close()

    assert result["measured_value_source"] == "co2_no_trusted_source"
    assert result["co2_temporal_contract_status"] == "fail"
    assert result["co2_large_gap_count"] == 1
    assert "effective_dwell_seconds=2.000<min_effective_dwell_seconds=4.000" in result["co2_temporal_contract_reason"]
    assert result["co2_point_suitability_status"] == "unfit"
    assert result["co2_calibration_candidate_hard_blocked"] is True
    assert pytest.approx(float(result["co2_calibration_weight_recommended"]), abs=1e-9) == 0.0


def test_evaluate_co2_steady_state_window_qc_temporal_contract_falls_back_to_row_semantics_without_timestamp(
    tmp_path: Path,
) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "sampling": {
                    "interval_s": 1.0,
                    "co2_interval_s": 1.0,
                    "quality": {
                        "co2_steady_state_enabled": True,
                        "co2_steady_state_policy": "warn",
                        "co2_steady_state_min_samples": 4,
                        "co2_steady_state_fallback_samples": 4,
                        "co2_steady_state_max_std_ppm": 0.2,
                        "co2_steady_state_max_range_ppm": 0.4,
                        "co2_steady_state_max_abs_slope_ppm_per_s": 0.2,
                        "co2_temporal_contract_enabled": True,
                        "co2_temporal_allow_row_fallback_without_timestamp": True,
                    },
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2_low_pressure()
    rows = _co2_sampling_rows_from_items(
        [
            {"_include_ts": False, "co2_ppm": 500.0, "frame_usable": True, "frame_status": "可用"},
            {"_include_ts": False, "co2_ppm": 500.1, "frame_usable": True, "frame_status": "可用"},
            {"_include_ts": False, "co2_ppm": 499.9, "frame_usable": True, "frame_status": "可用"},
            {"_include_ts": False, "co2_ppm": 500.0, "frame_usable": True, "frame_status": "可用"},
        ]
    )

    result = runner._evaluate_co2_steady_state_window_qc(point, phase="co2", samples=rows)
    logger.close()

    assert result["co2_temporal_contract_status"] == "fallback_row_count"
    assert result["co2_temporal_fallback_reason"] == "timestamp_missing_row_fallback"
    assert result["co2_temporal_candidate_rows_before_gate"] == 4
    assert result["co2_temporal_candidate_rows_after_gate"] == 4
    assert pytest.approx(result["co2_representative_value"], abs=1e-6) == 500.0


def test_evaluate_co2_steady_state_window_qc_temporal_contract_can_fail_short_primary_dwell_and_fallback(
    tmp_path: Path,
) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "sampling": {
                    "interval_s": 1.0,
                    "co2_interval_s": 1.0,
                    "quality": {
                        "co2_steady_state_enabled": True,
                        "co2_steady_state_policy": "warn",
                        "co2_steady_state_min_samples": 4,
                        "co2_steady_state_fallback_samples": 4,
                        "co2_steady_state_max_std_ppm": 0.2,
                        "co2_steady_state_max_range_ppm": 0.4,
                        "co2_steady_state_max_abs_slope_ppm_per_s": 0.2,
                        "co2_source_segment_contract_enabled": True,
                        "co2_source_segment_min_rows_after_settle": 4,
                        "co2_temporal_contract_enabled": True,
                        "co2_temporal_min_effective_dwell_seconds": 4.0,
                    },
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    runner._all_gas_analyzers = lambda: [("ga02", None, {})]
    point = _point_co2_low_pressure()
    rows = _co2_sampling_rows_from_items(
        [
            {"_sample_ts": 0.0, "co2_ppm": None, "frame_usable": False, "frame_status": "NO_RESPONSE", "ga02_co2_ppm": 620.0, "ga02_frame_usable": True, "ga02_frame_status": "可用"},
            {"_sample_ts": 1.0, "co2_ppm": None, "frame_usable": False, "frame_status": "NO_RESPONSE", "ga02_co2_ppm": 620.1, "ga02_frame_usable": True, "ga02_frame_status": "可用"},
            {"_sample_ts": 2.0, "co2_ppm": None, "frame_usable": False, "frame_status": "NO_RESPONSE", "ga02_co2_ppm": 619.9, "ga02_frame_usable": True, "ga02_frame_status": "可用"},
            {"_sample_ts": 3.0, "co2_ppm": None, "frame_usable": False, "frame_status": "NO_RESPONSE", "ga02_co2_ppm": 620.0, "ga02_frame_usable": True, "ga02_frame_status": "可用"},
            {"_sample_ts": 5.0, "co2_ppm": 500.0, "frame_usable": True, "frame_status": "可用", "ga02_co2_ppm": 620.1, "ga02_frame_usable": True, "ga02_frame_status": "可用"},
            {"_sample_ts": 6.0, "co2_ppm": 500.1, "frame_usable": True, "frame_status": "可用", "ga02_co2_ppm": 619.9, "ga02_frame_usable": True, "ga02_frame_status": "可用"},
            {"_sample_ts": 7.0, "co2_ppm": 499.9, "frame_usable": True, "frame_status": "可用", "ga02_co2_ppm": 620.0, "ga02_frame_usable": True, "ga02_frame_status": "可用"},
            {"_sample_ts": 8.0, "co2_ppm": 500.0, "frame_usable": True, "frame_status": "可用", "ga02_co2_ppm": 620.1, "ga02_frame_usable": True, "ga02_frame_status": "可用"},
        ]
    )

    result = runner._evaluate_co2_steady_state_window_qc(point, phase="co2", samples=rows)
    logger.close()

    assert result["co2_source_selected"] == "ga02"
    assert result["co2_source_segment_selected"] == "ga02#1"
    assert "primary_lost_to=ga02/ga02#1" in result["co2_source_switch_reason"]
    assert "rows_after_segment_gate=3<min_rows_after_segment_gate=4" in result["co2_source_switch_reason"]
    assert "effective_dwell_seconds=2.000<min_effective_dwell_seconds=4.000" in result["co2_source_switch_reason"]
    assert pytest.approx(result["co2_representative_value"], abs=1e-6) == 620.0125
    assert result["co2_decision_waterfall_status"] == "warn"
    assert "source_trust=primary_lost_to=ga02/ga02#1" in result["co2_decision_waterfall_reason"]
    lineage = result["co2_decision_lineage"]
    assert lineage["selected_source"] == "ga02"
    assert lineage["selected_segment"] == "ga02#1"
    source_stage = next(stage for stage in lineage["stages"] if stage["stage_name"] == "source_trust")
    assert source_stage["status"] == "warn"
    assert "primary_lost_to=ga02/ga02#1" in source_stage["reason_summary"]
    assert "primary_segment=primary#1" in source_stage["reason_summary"]
    assert "effective_dwell_seconds=2.000<min_effective_dwell_seconds=4.000" in source_stage["reason_summary"]
    assert result["co2_point_suitability_status"] == "advisory"
    assert result["co2_calibration_candidate_recommended"] is True
    assert result["co2_calibration_candidate_hard_blocked"] is False
    assert 0.0 < float(result["co2_calibration_weight_recommended"]) < 1.0
    assert float(result["co2_evidence_score"]) < 100.0
    assert "source_fallback" in result["co2_point_evidence_budget_reason"]


def test_evaluate_co2_steady_state_window_qc_falls_back_to_next_usable_source(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "sampling": {
                    "interval_s": 1.0,
                    "co2_interval_s": 1.0,
                    "quality": {
                        "co2_steady_state_enabled": True,
                        "co2_steady_state_policy": "warn",
                        "co2_steady_state_min_samples": 4,
                        "co2_steady_state_fallback_samples": 4,
                        "co2_steady_state_max_std_ppm": 0.2,
                        "co2_steady_state_max_range_ppm": 0.4,
                        "co2_steady_state_max_abs_slope_ppm_per_s": 0.2,
                    }
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    runner._all_gas_analyzers = lambda: [("ga01", None, {}), ("ga02", None, {})]
    point = _point_co2_low_pressure()
    rows = _co2_sampling_rows_from_items(
        [
            {
                "co2_ppm": 999999.0,
                "frame_usable": True,
                "frame_status": "可用",
                "ga01_co2_ppm": 999999.0,
                "ga01_frame_usable": True,
                "ga01_frame_status": "可用",
                "ga02_co2_ppm": 620.0,
                "ga02_frame_usable": True,
                "ga02_frame_status": "可用",
            },
            {
                "co2_ppm": 999999.0,
                "frame_usable": True,
                "frame_status": "可用",
                "ga01_co2_ppm": 999999.0,
                "ga01_frame_usable": True,
                "ga01_frame_status": "可用",
                "ga02_co2_ppm": 620.1,
                "ga02_frame_usable": True,
                "ga02_frame_status": "可用",
            },
            {
                "co2_ppm": 999999.0,
                "frame_usable": True,
                "frame_status": "可用",
                "ga01_co2_ppm": 999999.0,
                "ga01_frame_usable": True,
                "ga01_frame_status": "可用",
                "ga02_co2_ppm": 619.9,
                "ga02_frame_usable": True,
                "ga02_frame_status": "可用",
            },
            {
                "co2_ppm": 999999.0,
                "frame_usable": True,
                "frame_status": "可用",
                "ga01_co2_ppm": 999999.0,
                "ga01_frame_usable": True,
                "ga01_frame_status": "可用",
                "ga02_co2_ppm": 620.0,
                "ga02_frame_usable": True,
                "ga02_frame_status": "可用",
            },
        ]
    )

    result = runner._evaluate_co2_steady_state_window_qc(point, phase="co2", samples=rows)
    logger.close()

    assert result["co2_source_selected"] == "ga02"
    assert result["co2_steady_window_analyzer_source"] == "ga02"
    assert "primary_lost_to=ga02" in result["co2_source_switch_reason"]
    assert pytest.approx(result["co2_representative_value"], abs=1e-6) == 620.0
    assert rows[0]["co2_source_selected_for_value"] == "ga02"
    assert rows[0]["co2_bad_frame"] is False
    assert result["co2_point_suitability_status"] == "advisory"
    assert result["co2_calibration_candidate_recommended"] is True
    assert result["co2_calibration_candidate_hard_blocked"] is False
    assert 0.0 < float(result["co2_calibration_weight_recommended"]) < 1.0
    assert "source_fallback" in result["co2_point_evidence_budget_reason"]


def test_sample_and_log_does_not_silently_emit_value_when_all_sources_untrusted(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "sampling": {
                    "interval_s": 1.0,
                    "co2_interval_s": 1.0,
                    "quality": {
                        "co2_steady_state_enabled": True,
                        "co2_steady_state_policy": "warn",
                        "co2_steady_state_min_samples": 4,
                        "co2_steady_state_fallback_samples": 4,
                        "co2_steady_state_max_std_ppm": 0.2,
                        "co2_steady_state_max_range_ppm": 0.4,
                        "co2_steady_state_max_abs_slope_ppm_per_s": 0.2,
                    }
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    runner._all_gas_analyzers = lambda: [("ga01", None, {}), ("ga02", None, {})]
    runner._collect_samples = lambda *_args, **_kwargs: _co2_sampling_rows_from_items(
        [
            {
                "co2_ppm": 999999.0,
                "frame_usable": True,
                "frame_status": "可用",
                "ga01_co2_ppm": 999999.0,
                "ga01_frame_usable": True,
                "ga01_frame_status": "可用",
                "ga02_co2_ppm": 999999.0,
                "ga02_frame_usable": True,
                "ga02_frame_status": "可用",
            },
            {
                "co2_ppm": 999999.0,
                "frame_usable": True,
                "frame_status": "可用",
                "ga01_co2_ppm": 999999.0,
                "ga01_frame_usable": True,
                "ga01_frame_status": "可用",
                "ga02_co2_ppm": 999999.0,
                "ga02_frame_usable": True,
                "ga02_frame_status": "可用",
            },
            {
                "co2_ppm": 999999.0,
                "frame_usable": True,
                "frame_status": "可用",
                "ga01_co2_ppm": 999999.0,
                "ga01_frame_usable": True,
                "ga01_frame_status": "可用",
                "ga02_co2_ppm": 999999.0,
                "ga02_frame_usable": True,
                "ga02_frame_status": "可用",
            },
            {
                "co2_ppm": 999999.0,
                "frame_usable": True,
                "frame_status": "可用",
                "ga01_co2_ppm": 999999.0,
                "ga01_frame_usable": True,
                "ga01_frame_status": "可用",
                "ga02_co2_ppm": 999999.0,
                "ga02_frame_usable": True,
                "ga02_frame_status": "可用",
            },
        ]
    )
    point = _point_co2_low_pressure()
    point.co2_ppm = 1000.0

    runner._sample_and_log(point, phase="co2")
    logger.close()

    with logger.points_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    row = rows[0]
    assert row[_field_label("measured_value")] == ""
    assert row[_field_label("measured_value_source")] == "co2_no_trusted_source"
    assert row[_field_label("co2_steady_window_reason")] == "no_trusted_source_after_quarantine;policy=warn"
    assert row[_field_label("co2_source_selected")] == ""
    assert row[_field_label("co2_decision_waterfall_status")] == "fail"
    assert row[_field_label("co2_decision_stage_count")] == "6"
    assert "source_trust=no_trusted_source_after_quarantine" in row[_field_label("co2_decision_waterfall_reason")]
    assert "source_trust:fail" in row[_field_label("co2_decision_selected_stage_path")]
    assert "steady_state:warn" in row[_field_label("co2_decision_selected_stage_path")]
    assert "source_trust[fail" in row[_field_label("co2_decision_stage_summary")]
    assert row[_field_label("co2_point_suitability_status")] == "unfit"
    assert row[_field_label("co2_calibration_candidate_recommended")] == "False"
    assert row[_field_label("co2_calibration_candidate_hard_blocked")] == "True"
    assert row[_field_label("co2_calibration_weight_recommended")] == "0.0"
    assert "hard_blocked" in row[_field_label("co2_point_evidence_budget_reason")]
    assert row[_field_label("point_quality_status")] == "warn"

    with logger.samples_path.open("r", encoding="utf-8", newline="") as handle:
        sample_rows = list(csv.DictReader(handle))
    assert sample_rows[0][_field_label("co2_source_selected_for_value")] == ""
    assert sample_rows[0][_field_label("co2_bad_frame")] == "True"
    assert _field_label("co2_bad_frame_count") == "气路坏帧数"


def test_sample_and_log_exports_co2_steady_state_measured_value_and_reasons(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
            {
                "workflow": {
                    "sampling": {
                        "interval_s": 1.0,
                        "co2_interval_s": 1.0,
                        "quality": {
                            "co2_steady_state_enabled": True,
                            "co2_steady_state_policy": "warn",
                            "co2_steady_state_min_samples": 4,
                        "co2_steady_state_fallback_samples": 4,
                        "co2_steady_state_max_std_ppm": 0.2,
                        "co2_steady_state_max_range_ppm": 0.4,
                        "co2_steady_state_max_abs_slope_ppm_per_s": 0.2,
                    }
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    runner._collect_samples = lambda *_args, **_kwargs: _co2_sampling_rows(
        [120.0, 200.0, 300.0, 420.0, 480.0, 495.0, 500.0, 500.2, 499.9, 500.1]
    )
    point = _point_co2_low_pressure()
    point.co2_ppm = 500.0

    runner._sample_and_log(point, phase="co2")
    logger.close()

    with logger.points_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    row = rows[0]

    assert pytest.approx(float(row[_field_label("measured_value")]), abs=1e-6) == 500.05
    assert row[_field_label("measured_value_source")] == "co2_steady_state_window"
    assert row[_field_label("co2_steady_window_found")] == "True"
    assert row[_field_label("co2_steady_window_start_sample_index")] == "7"
    assert row[_field_label("co2_steady_window_end_sample_index")] == "10"
    assert row[_field_label("co2_steady_window_status")] == "pass"


def test_sample_and_log_exports_phase_contract_fields(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "sampling": {
                    "interval_s": 1.0,
                    "co2_interval_s": 1.0,
                    "quality": {
                        "co2_steady_state_enabled": True,
                        "co2_steady_state_policy": "warn",
                        "co2_steady_state_min_samples": 4,
                        "co2_steady_state_fallback_samples": 4,
                        "co2_steady_state_max_std_ppm": 0.2,
                        "co2_steady_state_max_range_ppm": 0.4,
                        "co2_steady_state_max_abs_slope_ppm_per_s": 0.2,
                        "co2_phase_contract_enabled": True,
                        "co2_phase_head_exclusion_max_rows": 2,
                    },
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    runner._collect_samples = lambda *_args, **_kwargs: _co2_sampling_rows(
        [900.0, 500.0, 500.1, 499.9, 500.0, 500.1]
    )
    point = _point_co2_low_pressure()

    runner._sample_and_log(point, phase="co2")
    logger.close()

    with logger.points_path.open("r", encoding="utf-8", newline="") as handle:
        point_rows = list(csv.DictReader(handle))
    with logger.samples_path.open("r", encoding="utf-8", newline="") as handle:
        sample_rows = list(csv.DictReader(handle))

    point_row = point_rows[0]
    assert point_row[_field_label("co2_candidate_rows_before_phase_gate")] == "6"
    assert point_row[_field_label("co2_candidate_rows_after_phase_gate")] == "5"
    assert point_row[_field_label("co2_candidate_rows_after_quarantine")] == "5"
    assert point_row[_field_label("co2_phase_excluded_count")] == "1"
    assert "transition_head_delta_gt_threshold" in point_row[_field_label("co2_phase_reason_summary")]
    assert sample_rows[0][_field_label("co2_phase_label")] == "transition_excluded"
    assert sample_rows[1][_field_label("co2_phase_label")] == "acquisition_selected"
    assert _field_label("co2_phase_label") == "气路采样相位"


def test_sample_and_log_exports_source_segment_contract_fields(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "sampling": {
                    "interval_s": 1.0,
                    "co2_interval_s": 1.0,
                    "quality": {
                        "co2_steady_state_enabled": True,
                        "co2_steady_state_policy": "warn",
                        "co2_steady_state_min_samples": 4,
                        "co2_steady_state_fallback_samples": 4,
                        "co2_steady_state_max_std_ppm": 0.2,
                        "co2_steady_state_max_range_ppm": 0.4,
                        "co2_steady_state_max_abs_slope_ppm_per_s": 0.2,
                        "co2_source_segment_contract_enabled": True,
                        "co2_source_segment_head_exclusion_rows": 1,
                        "co2_source_segment_min_rows_after_settle": 4,
                        "co2_temporal_contract_enabled": True,
                    },
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    runner._all_gas_analyzers = lambda: [("ga02", None, {})]
    runner._collect_samples = lambda *_args, **_kwargs: _co2_sampling_rows_from_items(
        [
            {"co2_ppm": 999999.0, "frame_usable": True, "frame_status": "可用", "ga02_co2_ppm": 620.0, "ga02_frame_usable": True, "ga02_frame_status": "可用"},
            {"co2_ppm": 999999.0, "frame_usable": True, "frame_status": "可用", "ga02_co2_ppm": 620.1, "ga02_frame_usable": True, "ga02_frame_status": "可用"},
            {"co2_ppm": 999999.0, "frame_usable": True, "frame_status": "可用", "ga02_co2_ppm": 619.9, "ga02_frame_usable": True, "ga02_frame_status": "可用"},
            {"co2_ppm": 999999.0, "frame_usable": True, "frame_status": "可用", "ga02_co2_ppm": 620.0, "ga02_frame_usable": True, "ga02_frame_status": "可用"},
            {"co2_ppm": None, "frame_usable": False, "frame_status": "NO_RESPONSE", "ga02_co2_ppm": 620.0, "ga02_frame_usable": True, "ga02_frame_status": "可用"},
            {"co2_ppm": 500.0, "frame_usable": True, "frame_status": "可用", "ga02_co2_ppm": 620.1, "ga02_frame_usable": True, "ga02_frame_status": "可用"},
            {"co2_ppm": 500.1, "frame_usable": True, "frame_status": "可用", "ga02_co2_ppm": 619.9, "ga02_frame_usable": True, "ga02_frame_status": "可用"},
            {"co2_ppm": 499.9, "frame_usable": True, "frame_status": "可用", "ga02_co2_ppm": 620.0, "ga02_frame_usable": True, "ga02_frame_status": "可用"},
            {"co2_ppm": 500.0, "frame_usable": True, "frame_status": "可用", "ga02_co2_ppm": 620.1, "ga02_frame_usable": True, "ga02_frame_status": "可用"},
        ]
    )
    point = _point_co2_low_pressure()

    runner._sample_and_log(point, phase="co2")
    logger.close()

    with logger.points_path.open("r", encoding="utf-8", newline="") as handle:
        point_rows = list(csv.DictReader(handle))
    with logger.samples_path.open("r", encoding="utf-8", newline="") as handle:
        sample_rows = list(csv.DictReader(handle))

    point_row = point_rows[0]
    assert point_row[_field_label("co2_candidate_rows_before_segment_gate")] == "9"
    assert point_row[_field_label("co2_candidate_rows_after_segment_gate")] == "9"
    assert point_row[_field_label("co2_temporal_candidate_rows_before_gate")] == "9"
    assert point_row[_field_label("co2_temporal_candidate_rows_after_gate")] == "9"
    assert point_row[_field_label("co2_temporal_contract_status")] == "pass"
    assert point_row[_field_label("co2_source_segment_selected")] == "ga02#1"
    assert point_row[_field_label("co2_source_selected")] == "ga02"
    assert point_row[_field_label("co2_decision_waterfall_status")] == "warn"
    assert point_row[_field_label("co2_decision_stage_count")] == "6"
    assert "source_trust:warn" in point_row[_field_label("co2_decision_selected_stage_path")]
    assert "source_trust[warn" in point_row[_field_label("co2_decision_stage_summary")]
    assert point_row[_field_label("co2_point_suitability_status")] == "advisory"
    assert point_row[_field_label("co2_calibration_candidate_recommended")] == "True"
    assert point_row[_field_label("co2_calibration_candidate_hard_blocked")] == "False"
    assert 0.0 < float(point_row[_field_label("co2_calibration_weight_recommended")]) < 1.0
    assert sample_rows[0][_field_label("co2_source_segment_id")] == "ga02#1"
    assert sample_rows[0][_field_label("co2_source_segment_selected")] == "ga02#1"
    assert sample_rows[0][_field_label("co2_temporal_excluded")] == "False"
    assert sample_rows[0][_field_label("co2_point_suitability_status")] == "advisory"
    assert _field_label("co2_decision_waterfall_status") == "气路决策瀑布结果"
    assert _field_label("co2_decision_selected_stage_path") == "气路决策阶段路径"
    assert _field_label("co2_point_suitability_status") == "气路点适用性"
    assert _field_label("co2_calibration_weight_recommended") == "气路推荐校准权重"
    assert _field_label("co2_source_segment_id") == "气路来源分段ID"
    assert _field_label("co2_temporal_contract_status") == "气路时间契约结果"


def test_sample_and_log_warns_when_no_co2_steady_state_window(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
            {
                "workflow": {
                    "sampling": {
                        "interval_s": 1.0,
                        "co2_interval_s": 1.0,
                        "quality": {
                            "co2_steady_state_enabled": True,
                            "co2_steady_state_policy": "warn",
                            "co2_steady_state_min_samples": 4,
                        "co2_steady_state_fallback_samples": 4,
                        "co2_steady_state_max_std_ppm": 2.0,
                        "co2_steady_state_max_range_ppm": 4.0,
                        "co2_steady_state_max_abs_slope_ppm_per_s": 1.0,
                    }
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    runner._collect_samples = lambda *_args, **_kwargs: _co2_sampling_rows(
        [100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0, 800.0, 900.0, 1000.0]
    )
    point = _point_co2_low_pressure()
    point.co2_ppm = 1000.0

    runner._sample_and_log(point, phase="co2")
    logger.close()

    with logger.points_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    row = rows[0]

    assert pytest.approx(float(row[_field_label("measured_value")]), abs=1e-6) == 850.0
    assert row[_field_label("measured_value_source")] == "co2_trailing_window_fallback"
    assert row[_field_label("point_quality_status")] == "warn"
    assert "co2_steady_window" in row[_field_label("point_quality_flags")]
    assert "no_qualified_steady_state_window" in row[_field_label("co2_steady_window_reason")]
