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


def test_synthesized_co2_source_points_do_not_leak_template_pressure_into_route_context(tmp_path: Path) -> None:
    runner = _runner(tmp_path, {"workflow": {"selected_pressure_points": ["ambient", 500]}})
    try:
        points = [
            CalibrationPoint(
                index=1,
                temp_chamber_c=20.0,
                co2_ppm=0.0,
                hgen_temp_c=None,
                hgen_rh_pct=None,
                target_pressure_hpa=1000.0,
                dewpoint_c=None,
                h2o_mmol=None,
                raw_h2o=None,
            ),
            CalibrationPoint(
                index=2,
                temp_chamber_c=20.0,
                co2_ppm=400.0,
                hgen_temp_c=None,
                hgen_rh_pct=None,
                target_pressure_hpa=500.0,
                dewpoint_c=None,
                h2o_mmol=None,
                raw_h2o=None,
            ),
            CalibrationPoint(
                index=3,
                temp_chamber_c=20.0,
                co2_ppm=600.0,
                hgen_temp_c=None,
                hgen_rh_pct=None,
                target_pressure_hpa=500.0,
                dewpoint_c=None,
                h2o_mmol=None,
                raw_h2o=None,
            ),
        ]
        pressure_points = runner._co2_pressure_points_for_temperature(points)
        synthesized_200 = next(
            point for point in runner._co2_source_points(points) if int(point.co2_ppm or 0) == 200
        )
        route_context = runner._route_entry_context_for_co2_source(
            synthesized_200,
            pressure_points=pressure_points,
        )
    finally:
        _close_runner(runner)

    assert synthesized_200.target_pressure_hpa is None
    assert route_context["point_tag"] == "co2_groupa_200ppm_ambient"
    assert runner._point_title(synthesized_200, phase="co2", point_tag=route_context["point_tag"]) == (
        "20°C环境，二氧化碳200ppm，气压未设"
    )


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


def test_explicit_tiny_matrix_keeps_repeated_500hpa_rows(tmp_path: Path) -> None:
    runner = _runner(
        tmp_path,
        {"workflow": {"preserve_explicit_point_matrix": True, "selected_pressure_points": ["ambient", 500]}},
    )
    try:
        points = [
            CalibrationPoint(
                index=idx + 1,
                temp_chamber_c=20.0,
                co2_ppm=500.0,
                hgen_temp_c=None,
                hgen_rh_pct=None,
                target_pressure_hpa=float(pressure),
                dewpoint_c=None,
                h2o_mmol=None,
                raw_h2o=None,
                co2_group="B",
            )
            for idx, pressure in enumerate([1100, 500, 500, 500])
        ]
        selected = runner._co2_pressure_points_for_temperature(points)
    finally:
        _close_runner(runner)

    assert [runner._pressure_target_label(point) for point in selected] == [
        "当前大气压",
        "500hPa",
        "500hPa",
        "500hPa",
    ]


def test_explicit_tiny_matrix_disables_20c_full_co2_sweep_expansion(tmp_path: Path) -> None:
    runner = _runner(tmp_path, {"workflow": {"preserve_explicit_point_matrix": True}})
    try:
        points = [
            CalibrationPoint(
                index=idx + 1,
                temp_chamber_c=20.0,
                co2_ppm=500.0,
                hgen_temp_c=None,
                hgen_rh_pct=None,
                target_pressure_hpa=float(pressure),
                dewpoint_c=None,
                h2o_mmol=None,
                raw_h2o=None,
                co2_group="B",
            )
            for idx, pressure in enumerate([1100, 500, 500, 500])
        ]
        selected = runner._co2_source_points(points)
    finally:
        _close_runner(runner)

    assert [int(point.co2_ppm or 0) for point in selected] == [500]
    assert [str(point.co2_group or "") for point in selected] == ["B"]


def test_pressure_selection_rejects_invalid_ambient_token(tmp_path: Path) -> None:
    runner = _runner(tmp_path, {"workflow": {"selected_pressure_points": ["ambient_now"]}})
    try:
        with pytest.raises(ValueError, match="invalid selected_pressure_points"):
            runner._co2_pressure_points_for_temperature(_co2_points(700, 1100, 900))
    finally:
        _close_runner(runner)
