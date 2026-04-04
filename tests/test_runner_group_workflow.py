from pathlib import Path

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow.runner import CalibrationRunner


def test_temperature_group_runs_h2o_groups_before_co2_points(tmp_path: Path) -> None:
    points = [
        CalibrationPoint(
            index=21,
            temp_chamber_c=40.0,
            co2_ppm=None,
            hgen_temp_c=20.0,
            hgen_rh_pct=30.0,
            target_pressure_hpa=1100.0,
            dewpoint_c=1.0,
            h2o_mmol=7.0,
            raw_h2o="h2o-a",
        ),
        CalibrationPoint(
            index=22,
            temp_chamber_c=40.0,
            co2_ppm=None,
            hgen_temp_c=20.0,
            hgen_rh_pct=30.0,
            target_pressure_hpa=1000.0,
            dewpoint_c=1.0,
            h2o_mmol=7.0,
            raw_h2o="h2o-a",
        ),
        CalibrationPoint(
            index=24,
            temp_chamber_c=40.0,
            co2_ppm=None,
            hgen_temp_c=20.0,
            hgen_rh_pct=50.0,
            target_pressure_hpa=800.0,
            dewpoint_c=5.0,
            h2o_mmol=11.0,
            raw_h2o="h2o-b",
        ),
        CalibrationPoint(
            index=27,
            temp_chamber_c=40.0,
            co2_ppm=None,
            hgen_temp_c=20.0,
            hgen_rh_pct=70.0,
            target_pressure_hpa=500.0,
            dewpoint_c=9.0,
            h2o_mmol=16.0,
            raw_h2o="h2o-c",
        ),
        CalibrationPoint(
            index=28,
            temp_chamber_c=40.0,
            co2_ppm=0.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=1100.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
        CalibrationPoint(
            index=29,
            temp_chamber_c=40.0,
            co2_ppm=200.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=1000.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
        CalibrationPoint(
            index=30,
            temp_chamber_c=40.0,
            co2_ppm=600.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=800.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
    ]
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    calls = []
    try:
        runner._run_h2o_group = lambda group, pressure_points=None, next_route_context=None: calls.append(  # type: ignore[method-assign]
            ("h2o", [p.index for p in group], [int(p.target_pressure_hpa or 0) for p in (pressure_points or [])])
        )
        runner._run_co2_point = lambda point, pressure_points=None, next_route_context=None: calls.append(  # type: ignore[method-assign]
            ("co2", point.index, [p.index for p in (pressure_points or [])])
        )
        runner._run_temperature_group(points)
    finally:
        logger.close()

    assert calls == [
        ("h2o", [21, 22], [1100, 1000, 800, 500]),
        ("h2o", [24], [1100, 1000, 800, 500]),
        ("h2o", [27], [1100, 1000, 800, 500]),
        ("co2", 28, [21, 22, 24, 27]),
        ("co2", 29, [21, 22, 24, 27]),
        ("co2", 30, [21, 22, 24, 27]),
    ]


def test_temperature_group_skips_configured_co2_ppm(tmp_path: Path) -> None:
    points = [
        CalibrationPoint(
            index=21,
            temp_chamber_c=40.0,
            co2_ppm=0.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=1100.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
        CalibrationPoint(
            index=22,
            temp_chamber_c=40.0,
            co2_ppm=200.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=1000.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
        CalibrationPoint(
            index=23,
            temp_chamber_c=40.0,
            co2_ppm=400.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=900.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
    ]
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({"workflow": {"skip_h2o": True, "skip_co2_ppm": [200]}}, {}, logger, lambda *_: None, lambda *_: None)
    calls = []
    try:
        runner._run_h2o_group = lambda group, pressure_points=None, next_route_context=None: calls.append(("h2o", [p.index for p in group]))  # type: ignore[method-assign]
        runner._run_co2_point = lambda point, pressure_points=None, next_route_context=None: calls.append(  # type: ignore[method-assign]
            ("co2", point.index, [p.index for p in (pressure_points or [])])
        )
        runner._run_temperature_group(points)
    finally:
        logger.close()

    assert calls == [
        ("co2", 21, [21, 22, 23]),
        ("co2", 23, [21, 22, 23]),
    ]


def test_temperature_group_co2_uses_all_pressures_for_each_source_sorted_desc(tmp_path: Path) -> None:
    points = [
        CalibrationPoint(
            index=31,
            temp_chamber_c=40.0,
            co2_ppm=0.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=700.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
        CalibrationPoint(
            index=32,
            temp_chamber_c=40.0,
            co2_ppm=400.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=1100.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
        CalibrationPoint(
            index=33,
            temp_chamber_c=40.0,
            co2_ppm=1000.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=500.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
        CalibrationPoint(
            index=34,
            temp_chamber_c=40.0,
            co2_ppm=800.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=900.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
    ]
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({"workflow": {"skip_h2o": True}}, {}, logger, lambda *_: None, lambda *_: None)
    calls = []
    try:
        runner._run_h2o_group = lambda group, pressure_points=None, next_route_context=None: calls.append(("h2o", [p.index for p in group]))  # type: ignore[method-assign]
        runner._run_co2_point = lambda point, pressure_points=None, next_route_context=None: calls.append(  # type: ignore[method-assign]
            (
                "co2",
                int(point.co2_ppm or 0),
                [int(p.target_pressure_hpa or 0) for p in (pressure_points or [])],
            )
        )
        runner._run_temperature_group(points)
    finally:
        logger.close()

    assert calls == [
        ("co2", 0, [1100, 900, 700, 500]),
        ("co2", 400, [1100, 900, 700, 500]),
        ("co2", 800, [1100, 900, 700, 500]),
        ("co2", 1000, [1100, 900, 700, 500]),
    ]


def test_temperature_group_h2o_uses_all_pressures_for_each_humidity_group_sorted_desc(tmp_path: Path) -> None:
    points = [
        CalibrationPoint(
            index=41,
            temp_chamber_c=40.0,
            co2_ppm=0.0,
            hgen_temp_c=20.0,
            hgen_rh_pct=30.0,
            target_pressure_hpa=700.0,
            dewpoint_c=1.0,
            h2o_mmol=7.0,
            raw_h2o="h2o-a",
        ),
        CalibrationPoint(
            index=42,
            temp_chamber_c=40.0,
            co2_ppm=200.0,
            hgen_temp_c=20.0,
            hgen_rh_pct=30.0,
            target_pressure_hpa=1100.0,
            dewpoint_c=1.0,
            h2o_mmol=7.0,
            raw_h2o="h2o-a",
        ),
        CalibrationPoint(
            index=43,
            temp_chamber_c=40.0,
            co2_ppm=400.0,
            hgen_temp_c=20.0,
            hgen_rh_pct=50.0,
            target_pressure_hpa=500.0,
            dewpoint_c=5.0,
            h2o_mmol=11.0,
            raw_h2o="h2o-b",
        ),
        CalibrationPoint(
            index=44,
            temp_chamber_c=40.0,
            co2_ppm=600.0,
            hgen_temp_c=20.0,
            hgen_rh_pct=50.0,
            target_pressure_hpa=900.0,
            dewpoint_c=5.0,
            h2o_mmol=11.0,
            raw_h2o="h2o-b",
        ),
    ]
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({"workflow": {"route_mode": "h2o_only"}}, {}, logger, lambda *_: None, lambda *_: None)
    calls = []
    try:
        runner._run_h2o_group = lambda group, pressure_points=None, next_route_context=None: calls.append(  # type: ignore[method-assign]
            (
                "h2o",
                [p.index for p in group],
                [int(p.target_pressure_hpa or 0) for p in (pressure_points or [])],
            )
        )
        runner._run_temperature_group(points)
    finally:
        logger.close()

    assert calls == [
        ("h2o", [41, 42], [1100, 900, 700, 500]),
        ("h2o", [43, 44], [1100, 900, 700, 500]),
    ]


def test_temperature_group_co2_expands_full_sweep_for_20c_sources(tmp_path: Path) -> None:
    points = [
        CalibrationPoint(
            index=21,
            temp_chamber_c=20.0,
            co2_ppm=0.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=1100.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
        CalibrationPoint(
            index=23,
            temp_chamber_c=20.0,
            co2_ppm=400.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=900.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
    ]
    cfg = {
        "workflow": {"skip_h2o": True, "skip_co2_ppm": [200]},
        "valves": {
            "co2_map": {"0": 1, "200": 2, "400": 3, "600": 4, "800": 5, "1000": 6},
            "co2_map_group2": {"0": 21, "100": 22, "300": 23, "500": 24, "700": 25, "900": 26},
        },
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)
    calls = []
    try:
        runner._run_h2o_group = lambda group, pressure_points=None, next_route_context=None: calls.append(("h2o", [p.index for p in group]))  # type: ignore[method-assign]
        runner._run_co2_point = lambda point, pressure_points=None, next_route_context=None: calls.append(  # type: ignore[method-assign]
            ("co2", int(point.co2_ppm or 0), getattr(point, "co2_group", None), [int(p.target_pressure_hpa or 0) for p in (pressure_points or [])])
        )
        runner._run_temperature_group(points)
    finally:
        logger.close()

    assert calls == [
        ("co2", 0, None, [1100, 900]),
        ("co2", 100, "B", [1100, 900]),
        ("co2", 300, "B", [1100, 900]),
        ("co2", 400, None, [1100, 900]),
        ("co2", 500, "B", [1100, 900]),
        ("co2", 600, None, [1100, 900]),
        ("co2", 700, "B", [1100, 900]),
        ("co2", 800, None, [1100, 900]),
        ("co2", 900, "B", [1100, 900]),
        ("co2", 1000, None, [1100, 900]),
    ]


def test_co2_source_points_sort_real_sources_by_ppm(tmp_path: Path) -> None:
    points = [
        CalibrationPoint(
            index=21,
            temp_chamber_c=40.0,
            co2_ppm=0.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=500.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
        CalibrationPoint(
            index=23,
            temp_chamber_c=40.0,
            co2_ppm=400.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=500.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
    ]
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({"workflow": {"skip_h2o": True}}, {}, logger, lambda *_: None, lambda *_: None)
    try:
        sources = runner._co2_source_points(points)
    finally:
        logger.close()

    assert [(int(point.co2_ppm or 0), getattr(point, "co2_group", None)) for point in sources] == [
        (0, None),
        (400, None),
    ]
    assert [int(point.target_pressure_hpa or 0) for point in sources] == [500, 500]


def test_co2_source_points_do_not_synthesize_when_group_has_no_real_co2_source(tmp_path: Path) -> None:
    points = [
        CalibrationPoint(
            index=27,
            temp_chamber_c=40.0,
            co2_ppm=None,
            hgen_temp_c=20.0,
            hgen_rh_pct=70.0,
            target_pressure_hpa=500.0,
            dewpoint_c=9.0,
            h2o_mmol=16.0,
            raw_h2o="h2o-c",
        ),
    ]
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    try:
        sources = runner._co2_source_points(points)
    finally:
        logger.close()

    assert sources == []


def test_co2_source_points_synthesize_full_sweep_for_10c_without_real_co2_rows(tmp_path: Path) -> None:
    points = [
        CalibrationPoint(
            index=12,
            temp_chamber_c=10.0,
            co2_ppm=None,
            hgen_temp_c=10.0,
            hgen_rh_pct=30.0,
            target_pressure_hpa=1100.0,
            dewpoint_c=1.0,
            h2o_mmol=7.0,
            raw_h2o="h2o-10c",
        ),
    ]
    cfg = {
        "workflow": {"skip_co2_ppm": [200]},
        "valves": {
            "co2_map": {"0": 1, "200": 2, "400": 3, "600": 4, "800": 5, "1000": 6},
            "co2_map_group2": {"0": 21, "100": 22, "300": 23, "500": 24, "700": 25, "900": 26},
        },
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)
    try:
        sources = runner._co2_source_points(points)
    finally:
        logger.close()

    assert [(int(point.co2_ppm or 0), getattr(point, "co2_group", None)) for point in sources] == [
        (0, None),
        (100, "B"),
        (300, "B"),
        (400, None),
        (500, "B"),
        (600, None),
        (700, "B"),
        (800, None),
        (900, "B"),
        (1000, None),
    ]
    assert [int(point.target_pressure_hpa or 0) for point in sources] == [1100] * 10


def test_co2_source_points_keep_real_group_b_order_without_duplicate_group_a_ppm(tmp_path: Path) -> None:
    points = [
        CalibrationPoint(
            index=21,
            temp_chamber_c=40.0,
            co2_ppm=300.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=900.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
            co2_group="B",
        ),
        CalibrationPoint(
            index=22,
            temp_chamber_c=40.0,
            co2_ppm=400.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=900.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
    ]
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({"workflow": {"skip_h2o": True}}, {}, logger, lambda *_: None, lambda *_: None)
    try:
        sources = runner._co2_source_points(points)
    finally:
        logger.close()

    assert [(int(point.co2_ppm or 0), getattr(point, "co2_group", None)) for point in sources] == [
        (300, "B"),
        (400, None),
    ]


def test_co2_source_points_ignore_config_only_ppm_keys(tmp_path: Path) -> None:
    points = [
        CalibrationPoint(
            index=21,
            temp_chamber_c=40.0,
            co2_ppm=400.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=1100.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
    ]
    cfg = {
        "workflow": {"skip_h2o": True},
        "valves": {
            "co2_map": {"600": 1, "bad": 2, "0": 3, "200": 4},
            "co2_map_group2": {"500": 1, "100": 2, "oops": 3},
        },
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)
    try:
        sources = runner._co2_source_points(points)
    finally:
        logger.close()

    assert [(int(point.co2_ppm or 0), getattr(point, "co2_group", None)) for point in sources] == [
        (400, None),
    ]


def test_co2_source_points_keep_real_only_outside_10_20_30c(tmp_path: Path) -> None:
    points = [
        CalibrationPoint(
            index=21,
            temp_chamber_c=40.0,
            co2_ppm=0.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=1100.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
        CalibrationPoint(
            index=23,
            temp_chamber_c=40.0,
            co2_ppm=400.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=900.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
    ]
    cfg = {
        "workflow": {"skip_h2o": True, "skip_co2_ppm": [200]},
        "valves": {
            "co2_map": {"0": 1, "200": 2, "400": 3, "600": 4, "800": 5, "1000": 6},
            "co2_map_group2": {"0": 21, "100": 22, "300": 23, "500": 24, "700": 25, "900": 26},
        },
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)
    try:
        sources = runner._co2_source_points(points)
    finally:
        logger.close()

    assert [(int(point.co2_ppm or 0), getattr(point, "co2_group", None)) for point in sources] == [
        (0, None),
        (400, None),
    ]


def test_co2_pressure_points_keep_distinct_500_and_550_sorted_desc(tmp_path: Path) -> None:
    points = [
        CalibrationPoint(
            index=21,
            temp_chamber_c=20.0,
            co2_ppm=0.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=500.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
        CalibrationPoint(
            index=22,
            temp_chamber_c=20.0,
            co2_ppm=400.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=550.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
        CalibrationPoint(
            index=23,
            temp_chamber_c=20.0,
            co2_ppm=800.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=900.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
    ]
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({"workflow": {"skip_h2o": True}}, {}, logger, lambda *_: None, lambda *_: None)
    try:
        pressure_points = runner._co2_pressure_points_for_temperature(points)
    finally:
        logger.close()

    assert [int(point.target_pressure_hpa or 0) for point in pressure_points] == [900, 550, 500]


def test_temperature_group_preconditions_next_group_humidity_by_default(tmp_path: Path) -> None:
    current_points = [
        CalibrationPoint(
            index=21,
            temp_chamber_c=40.0,
            co2_ppm=None,
            hgen_temp_c=20.0,
            hgen_rh_pct=30.0,
            target_pressure_hpa=1100.0,
            dewpoint_c=1.0,
            h2o_mmol=7.0,
            raw_h2o="h2o-a",
        ),
        CalibrationPoint(
            index=24,
            temp_chamber_c=40.0,
            co2_ppm=None,
            hgen_temp_c=20.0,
            hgen_rh_pct=50.0,
            target_pressure_hpa=900.0,
            dewpoint_c=5.0,
            h2o_mmol=11.0,
            raw_h2o="h2o-b",
        ),
        CalibrationPoint(
            index=25,
            temp_chamber_c=40.0,
            co2_ppm=0.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=1100.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
        CalibrationPoint(
            index=26,
            temp_chamber_c=40.0,
            co2_ppm=400.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=900.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
    ]
    next_points = [
        CalibrationPoint(
            index=31,
            temp_chamber_c=30.0,
            co2_ppm=0.0,
            hgen_temp_c=30.0,
            hgen_rh_pct=30.0,
            target_pressure_hpa=1100.0,
            dewpoint_c=7.0,
            h2o_mmol=15.0,
            raw_h2o="h2o-c",
        ),
        CalibrationPoint(
            index=34,
            temp_chamber_c=30.0,
            co2_ppm=400.0,
            hgen_temp_c=30.0,
            hgen_rh_pct=50.0,
            target_pressure_hpa=900.0,
            dewpoint_c=10.0,
            h2o_mmol=20.0,
            raw_h2o="h2o-d",
        ),
    ]
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({"workflow": {"route_mode": "h2o_then_co2"}}, {}, logger, lambda *_: None, lambda *_: None)
    calls = []
    try:
        runner._run_h2o_group = lambda group, pressure_points=None, next_route_context=None: None  # type: ignore[method-assign]
        runner._run_co2_point = lambda point, pressure_points=None, next_route_context=None: calls.append(("co2", int(point.co2_ppm or 0)))  # type: ignore[method-assign]
        runner._prepare_humidity_generator = lambda point: calls.append(  # type: ignore[method-assign]
            ("prep", point.temp_chamber_c, point.hgen_temp_c, point.hgen_rh_pct)
        )
        runner._run_temperature_group(current_points, next_group=next_points)
    finally:
        logger.close()

    assert calls[0] == ("prep", 30.0, 30.0, 30.0)
    assert calls[1:] == [
        ("co2", 0),
        ("co2", 400),
    ]


def test_temperature_group_does_not_precondition_next_group_humidity_by_default(tmp_path: Path) -> None:
    current_points = [
        CalibrationPoint(
            index=21,
            temp_chamber_c=40.0,
            co2_ppm=0.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=1100.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
    ]
    next_points = [
        CalibrationPoint(
            index=31,
            temp_chamber_c=30.0,
            co2_ppm=0.0,
            hgen_temp_c=30.0,
            hgen_rh_pct=30.0,
            target_pressure_hpa=1100.0,
            dewpoint_c=7.0,
            h2o_mmol=15.0,
            raw_h2o="h2o-c",
        ),
    ]
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "route_mode": "h2o_then_co2",
                "stability": {"humidity_generator": {"precondition_next_group_enabled": False}},
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    calls = []
    try:
        runner._run_h2o_group = lambda group, pressure_points=None, next_route_context=None: None  # type: ignore[method-assign]
        runner._run_co2_point = lambda point, pressure_points=None, next_route_context=None: calls.append(("co2", int(point.co2_ppm or 0)))  # type: ignore[method-assign]
        runner._prepare_humidity_generator = lambda point: calls.append(("prep", point.temp_chamber_c))  # type: ignore[method-assign]
        runner._run_temperature_group(current_points, next_group=next_points)
    finally:
        logger.close()

    assert calls == [
        ("co2", 0),
    ]


def test_temperature_group_does_not_precondition_next_group_humidity_for_co2_only(tmp_path: Path) -> None:
    current_points = [
        CalibrationPoint(
            index=21,
            temp_chamber_c=40.0,
            co2_ppm=0.0,
            hgen_temp_c=None,
            hgen_rh_pct=None,
            target_pressure_hpa=1100.0,
            dewpoint_c=None,
            h2o_mmol=None,
            raw_h2o=None,
        ),
    ]
    next_points = [
        CalibrationPoint(
            index=31,
            temp_chamber_c=30.0,
            co2_ppm=0.0,
            hgen_temp_c=30.0,
            hgen_rh_pct=30.0,
            target_pressure_hpa=1100.0,
            dewpoint_c=7.0,
            h2o_mmol=15.0,
            raw_h2o="h2o-c",
        ),
    ]
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "route_mode": "co2_only",
                "stability": {"humidity_generator": {"precondition_next_group_enabled": True}},
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    calls = []
    try:
        runner._run_h2o_group = lambda group, pressure_points=None, next_route_context=None: None  # type: ignore[method-assign]
        runner._run_co2_point = lambda point, pressure_points=None, next_route_context=None: calls.append(("co2", int(point.co2_ppm or 0)))  # type: ignore[method-assign]
        runner._prepare_humidity_generator = lambda point: calls.append(("prep", point.temp_chamber_c))  # type: ignore[method-assign]
        runner._run_temperature_group(current_points, next_group=next_points)
    finally:
        logger.close()

    assert calls == [
        ("co2", 0),
    ]


def test_temperature_group_does_not_precondition_next_group_chamber_even_when_enabled(tmp_path: Path) -> None:
    class _FakeChamber:
        def __init__(self) -> None:
            self.calls = []

        def set_temp_c(self, value: float) -> None:
            self.calls.append(("set", value))

        def start(self) -> None:
            self.calls.append(("start",))

    current_points = [
        CalibrationPoint(
            index=21,
            temp_chamber_c=20.0,
            co2_ppm=0.0,
            hgen_temp_c=20.0,
            hgen_rh_pct=30.0,
            target_pressure_hpa=1100.0,
            dewpoint_c=1.0,
            h2o_mmol=7.0,
            raw_h2o="h2o-a",
        ),
    ]
    next_points = [
        CalibrationPoint(
            index=31,
            temp_chamber_c=30.0,
            co2_ppm=0.0,
            hgen_temp_c=30.0,
            hgen_rh_pct=30.0,
            target_pressure_hpa=1100.0,
            dewpoint_c=7.0,
            h2o_mmol=15.0,
            raw_h2o="h2o-c",
        ),
    ]
    logger = RunLogger(tmp_path)
    chamber = _FakeChamber()
    runner = CalibrationRunner(
        {
            "workflow": {
                "route_mode": "h2o_then_co2",
                "stability": {"temperature": {"precondition_next_group_enabled": True, "command_offset_c": 0.5}},
            }
        },
        {"temp_chamber": chamber},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    try:
        runner._run_h2o_group = lambda group, pressure_points=None, next_route_context=None: None  # type: ignore[method-assign]
        runner._run_co2_point = lambda point, pressure_points=None, next_route_context=None: None  # type: ignore[method-assign]
        runner._prepare_humidity_generator = lambda point: None  # type: ignore[method-assign]
        runner._run_temperature_group(current_points, next_group=next_points)
    finally:
        logger.close()

    assert chamber.calls == []


def test_temperature_group_does_not_precondition_next_group_chamber_by_default(tmp_path: Path) -> None:
    class _FakeChamber:
        def __init__(self) -> None:
            self.calls = []

        def set_temp_c(self, value: float) -> None:
            self.calls.append(("set", value))

        def start(self) -> None:
            self.calls.append(("start",))

    current_points = [
        CalibrationPoint(
            index=21,
            temp_chamber_c=20.0,
            co2_ppm=0.0,
            hgen_temp_c=20.0,
            hgen_rh_pct=30.0,
            target_pressure_hpa=1100.0,
            dewpoint_c=1.0,
            h2o_mmol=7.0,
            raw_h2o="h2o-a",
        ),
    ]
    next_points = [
        CalibrationPoint(
            index=31,
            temp_chamber_c=30.0,
            co2_ppm=0.0,
            hgen_temp_c=30.0,
            hgen_rh_pct=30.0,
            target_pressure_hpa=1100.0,
            dewpoint_c=7.0,
            h2o_mmol=15.0,
            raw_h2o="h2o-c",
        ),
    ]
    logger = RunLogger(tmp_path)
    chamber = _FakeChamber()
    runner = CalibrationRunner(
        {"workflow": {"route_mode": "h2o_then_co2"}},
        {"temp_chamber": chamber},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    try:
        runner._run_h2o_group = lambda group, pressure_points=None, next_route_context=None: None  # type: ignore[method-assign]
        runner._run_co2_point = lambda point, pressure_points=None, next_route_context=None: None  # type: ignore[method-assign]
        runner._prepare_humidity_generator = lambda point: None  # type: ignore[method-assign]
        runner._run_temperature_group(current_points, next_group=next_points)
    finally:
        logger.close()

    assert chamber.calls == []
