from pathlib import Path

import pytest

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow.runner import CalibrationRunner


def _co2_points(*pressures: int) -> list[CalibrationPoint]:
    return [
        CalibrationPoint(
            index=idx + 1,
            temp_chamber_c=20.0,
            co2_ppm=float(idx * 200),
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=float(pressure),
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        )
        for idx, pressure in enumerate(pressures)
    ]


def _h2o_points(*pressures: int) -> list[CalibrationPoint]:
    return [
        CalibrationPoint(
            index=idx + 1,
            temp_chamber_c=20.0,
            co2_ppm=None,
            hgen_temp_c=20.0,
            hgen_rh_pct=50.0,
            target_pressure_hpa=float(pressure),
            dewpoint_c=5.0,
            h2o_mmol=10.0,
            raw_h2o=f"h2o-{idx}",
        )
        for idx, pressure in enumerate(pressures)
    ]


def _runner(tmp_path: Path, cfg: dict | None = None) -> CalibrationRunner:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg or {}, {}, logger, lambda *_: None, lambda *_: None)
    runner._test_logger = logger  # type: ignore[attr-defined]
    return runner


def _close_runner(runner: CalibrationRunner) -> None:
    logger = getattr(runner, "_test_logger", None)
    if logger is not None:
        logger.close()


def test_pressure_selection_defaults_to_full_pressure_points(tmp_path: Path) -> None:
    runner = _runner(tmp_path)
    try:
        points = _co2_points(700, 1100, 900, 500)
        selected = runner._co2_pressure_points_for_temperature(points)
    finally:
        _close_runner(runner)

    assert [int(point.target_pressure_hpa or 0) for point in selected] == [1100, 900, 700, 500]


def test_pressure_selection_single_point(tmp_path: Path) -> None:
    runner = _runner(tmp_path, {"workflow": {"selected_pressure_points": [900]}})
    try:
        points = _co2_points(700, 1100, 900, 500)
        selected = runner._co2_pressure_points_for_temperature(points)
    finally:
        _close_runner(runner)

    assert [int(point.target_pressure_hpa or 0) for point in selected] == [900]


def test_pressure_selection_normalizes_execution_order(tmp_path: Path) -> None:
    runner = _runner(tmp_path, {"workflow": {"selected_pressure_points": [700, 1100, 900]}})
    try:
        points = _co2_points(700, 1100, 900, 500)
        selected = runner._co2_pressure_points_for_temperature(points)
    finally:
        _close_runner(runner)

    assert [int(point.target_pressure_hpa or 0) for point in selected] == [1100, 900, 700]


def test_pressure_selection_deduplicates_values(tmp_path: Path) -> None:
    runner = _runner(tmp_path, {"workflow": {"selected_pressure_points": [900, 900, 700]}})
    try:
        points = _co2_points(700, 1100, 900, 500)
        selected = runner._co2_pressure_points_for_temperature(points)
    finally:
        _close_runner(runner)

    assert [int(point.target_pressure_hpa or 0) for point in selected] == [900, 700]


def test_pressure_selection_rejects_invalid_values(tmp_path: Path) -> None:
    runner = _runner(tmp_path, {"workflow": {"selected_pressure_points": [950]}})
    try:
        with pytest.raises(ValueError, match="invalid selected_pressure_points"):
            runner._co2_pressure_points_for_temperature(_co2_points(700, 1100, 900))
    finally:
        _close_runner(runner)


def test_pressure_selection_rejects_empty_result_after_filter(tmp_path: Path) -> None:
    runner = _runner(tmp_path, {"workflow": {"selected_pressure_points": [1100]}})
    try:
        with pytest.raises(ValueError, match="no valid pressure points selected after filtering"):
            runner._co2_pressure_points_for_temperature(_co2_points(900, 700, 500))
    finally:
        _close_runner(runner)


def test_pressure_selection_applies_to_h2o_route(tmp_path: Path) -> None:
    runner = _runner(tmp_path, {"workflow": {"selected_pressure_points": [1100, 700]}})
    try:
        points = _h2o_points(700, 1100, 900, 500)
        selected = runner._h2o_pressure_points_for_temperature(points)
    finally:
        _close_runner(runner)

    assert [int(point.target_pressure_hpa or 0) for point in selected] == [1100, 700]


def test_pressure_selection_does_not_change_existing_co2_skip_logic(tmp_path: Path) -> None:
    runner = _runner(
        tmp_path,
        {
            "workflow": {
                "skip_h2o": True,
                "skip_co2_ppm": [200],
                "selected_pressure_points": [1100, 900],
            }
        },
    )
    try:
        source_points = _co2_points(1100, 900, 700)
        for point in source_points:
            point.temp_chamber_c = 40.0
        pressures = runner._co2_pressure_points_for_temperature(source_points)
        runner._run_co2_point = lambda point, pressure_points=None, next_route_context=None: None  # type: ignore[method-assign]
        sources = runner._co2_source_points(source_points)
    finally:
        _close_runner(runner)

    assert [int(point.target_pressure_hpa or 0) for point in pressures] == [1100, 900]
    assert [int(point.co2_ppm or 0) for point in sources] == [0, 400]


def test_pressure_selection_supports_ambient_only(tmp_path: Path) -> None:
    runner = _runner(tmp_path, {"workflow": {"selected_pressure_points": ["ambient"]}})
    try:
        selected = runner._co2_pressure_points_for_temperature(_co2_points(700, 1100, 900))
    finally:
        _close_runner(runner)

    assert len(selected) == 1
    assert runner._pressure_mode_for_point(selected[0]) == "ambient_open"
    assert runner._pressure_target_label(selected[0]) == "当前大气压"


def test_pressure_selection_supports_ambient_then_numeric_order(tmp_path: Path) -> None:
    runner = _runner(tmp_path, {"workflow": {"selected_pressure_points": ["ambient", 900]}})
    try:
        selected = runner._co2_pressure_points_for_temperature(_co2_points(700, 1100, 900))
    finally:
        _close_runner(runner)

    assert [runner._pressure_target_label(point) for point in selected] == ["当前大气压", "900hPa"]


def test_pressure_selection_normalizes_ambient_and_numeric_mix(tmp_path: Path) -> None:
    runner = _runner(tmp_path, {"workflow": {"selected_pressure_points": [900, "ambient", 700]}})
    try:
        selected = runner._co2_pressure_points_for_temperature(_co2_points(700, 1100, 900))
    finally:
        _close_runner(runner)

    assert [runner._pressure_target_label(point) for point in selected] == ["当前大气压", "900hPa", "700hPa"]


def test_pressure_selection_ambient_point_uses_explicit_labels(tmp_path: Path) -> None:
    runner = _runner(tmp_path, {"workflow": {"selected_pressure_points": ["ambient"]}})
    try:
        point = _co2_points(900)[0]
        ambient_ref = runner._co2_pressure_points_for_temperature([point])[0]
        ambient_point = runner._build_co2_pressure_point(point, ambient_ref)
    finally:
        _close_runner(runner)

    assert runner._co2_point_tag(ambient_point) == "co2_groupa_0ppm_ambient"
    assert runner._stage_label_for_point(ambient_point, phase="co2") == "CO2 0ppm 当前大气压"
    assert runner._point_title(ambient_point, phase="co2") == "20°C环境，二氧化碳0ppm，当前大气压"


def test_route_requires_preseal_topoff_only_when_1100_selected(tmp_path: Path) -> None:
    runner = _runner(tmp_path)
    try:
        sealed_without_1100 = _co2_points(900, 500)
        sealed_with_1100 = _co2_points(1100, 500)
        ambient_ref = runner._ambient_pressure_reference_point(sealed_without_1100[0])

        assert runner._route_requires_preseal_topoff([ambient_ref, *sealed_without_1100]) is False
        assert runner._route_requires_preseal_topoff(sealed_with_1100) is True
    finally:
        _close_runner(runner)


def test_pressure_selection_rejects_invalid_ambient_token(tmp_path: Path) -> None:
    runner = _runner(tmp_path, {"workflow": {"selected_pressure_points": ["ambient_now"]}})
    try:
        with pytest.raises(ValueError, match="invalid selected_pressure_points"):
            runner._co2_pressure_points_for_temperature(_co2_points(700, 1100, 900))
    finally:
        _close_runner(runner)
