from __future__ import annotations

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.models import CalibrationPoint
from gas_calibrator.v2.core.point_parser import PointParser
from gas_calibrator.v2.core.route_planner import RoutePlanner


def test_route_planner_groups_points_and_plans_h2o_and_co2_routes() -> None:
    config = AppConfig.from_dict({"workflow": {"route_mode": "h2o_then_co2"}})
    planner = RoutePlanner(config, PointParser())
    points = [
        CalibrationPoint(index=1, temperature_c=25.0, humidity_pct=30.0, pressure_hpa=900.0, route="h2o"),
        CalibrationPoint(index=2, temperature_c=25.0, humidity_pct=30.0, pressure_hpa=1000.0, route="h2o"),
        CalibrationPoint(index=3, temperature_c=25.0, co2_ppm=400.0, pressure_hpa=900.0, route="co2", co2_group="A"),
        CalibrationPoint(index=4, temperature_c=25.0, co2_ppm=800.0, pressure_hpa=1000.0, route="co2", co2_group="A"),
        CalibrationPoint(index=5, temperature_c=25.0, co2_ppm=800.0, pressure_hpa=1100.0, route="co2", co2_group="A"),
        CalibrationPoint(index=6, temperature_c=30.0, co2_ppm=1200.0, pressure_hpa=1000.0, route="co2", co2_group="B"),
    ]

    groups = planner.group_by_temperature(points)

    assert [group.temperature_c for group in groups] == [25.0, 30.0]
    assert planner.should_run_h2o(groups[0].points) is True
    assert [point.index for point in planner.h2o_pressure_points(groups[0].points)] == [5, 2, 1]
    assert [point.index for point in planner.co2_sources(groups[0].points)] == [3, 4]
    assert [point.index for point in planner.co2_pressure_points(groups[0].points[0], groups[0].points)] == [5, 2, 1]


def test_route_planner_skips_h2o_when_subzero_or_co2_only() -> None:
    planner = RoutePlanner(AppConfig.from_dict({"workflow": {"route_mode": "co2_only"}}), PointParser())
    points = [CalibrationPoint(index=1, temperature_c=-10.0, humidity_pct=30.0, pressure_hpa=900.0, route="h2o")]

    assert planner.should_run_h2o(points) is False


def test_route_planner_matches_v1_compare_pressure_expansion_for_sparse_carry_forward_points() -> None:
    planner = RoutePlanner(
        AppConfig.from_dict({"workflow": {"route_mode": "h2o_then_co2", "missing_pressure_policy": "carry_forward"}}),
        PointParser(),
    )
    points = [
        CalibrationPoint(index=9, temperature_c=0.0, humidity_pct=50.0, pressure_hpa=1100.0, route="h2o"),
        CalibrationPoint(index=10, temperature_c=0.0, co2_ppm=400.0, pressure_hpa=800.0, route="co2"),
        CalibrationPoint(index=11, temperature_c=0.0, co2_ppm=1000.0, pressure_hpa=500.0, route="co2"),
        CalibrationPoint(index=12, temperature_c=0.0, co2_ppm=0.0, pressure_hpa=1100.0, route="co2"),
    ]

    h2o_pressures = [int(point.target_pressure_hpa or 0) for point in planner.h2o_pressure_points(points)]
    sources = planner.co2_sources(points)
    source_ppm = [int(point.co2_ppm or 0) for point in sources]
    co2_pressures = {
        int(source.co2_ppm or 0): [int(point.target_pressure_hpa or 0) for point in planner.co2_pressure_points(source, points)]
        for source in sources
    }

    lead = points[0]
    planned_tags = {
        planner.h2o_point_tag(planner.build_h2o_pressure_point(lead, pressure_point))
        for pressure_point in planner.h2o_pressure_points(points)
    }
    for source in sources:
        for pressure_point in planner.co2_pressure_points(source, points):
            planned_tags.add(planner.co2_point_tag(planner.build_co2_pressure_point(source, pressure_point)))

    expected_tags = {
        "h2o_0c_50rh_1100hpa",
        "h2o_0c_50rh_800hpa",
        "h2o_0c_50rh_500hpa",
        "co2_groupa_0ppm_1100hpa",
        "co2_groupa_0ppm_800hpa",
        "co2_groupa_0ppm_500hpa",
        "co2_groupa_400ppm_1100hpa",
        "co2_groupa_400ppm_800hpa",
        "co2_groupa_400ppm_500hpa",
        "co2_groupa_1000ppm_1100hpa",
    }
    consistency_score = 100.0 * len(planned_tags & expected_tags) / len(planned_tags | expected_tags)

    assert h2o_pressures == [1100, 800, 500]
    assert source_ppm == [0, 400, 1000]
    assert co2_pressures[0] == [1100, 800, 500]
    assert co2_pressures[400] == [1100, 800, 500]
    assert co2_pressures[1000] == [1100]
    assert planned_tags == expected_tags
    assert consistency_score > 90.0


def test_route_planner_progress_point_keys_follow_logical_route_expansion() -> None:
    planner = RoutePlanner(
        AppConfig.from_dict({"workflow": {"route_mode": "h2o_then_co2"}}),
        PointParser(),
    )
    points = [
        CalibrationPoint(index=1, temperature_c=25.0, humidity_pct=30.0, pressure_hpa=900.0, route="h2o"),
        CalibrationPoint(index=2, temperature_c=25.0, humidity_pct=30.0, pressure_hpa=1000.0, route="h2o"),
        CalibrationPoint(index=3, temperature_c=25.0, co2_ppm=400.0, pressure_hpa=900.0, route="co2", co2_group="A"),
        CalibrationPoint(index=4, temperature_c=25.0, co2_ppm=800.0, pressure_hpa=1000.0, route="co2", co2_group="A"),
        CalibrationPoint(index=5, temperature_c=25.0, co2_ppm=800.0, pressure_hpa=1100.0, route="co2", co2_group="A"),
    ]

    keys = planner.progress_point_keys(points)

    assert keys == [
        "h2o:h2o_25c_30rh_1100hpa",
        "h2o:h2o_25c_30rh_1000hpa",
        "h2o:h2o_25c_30rh_900hpa",
        "co2:co2_groupa_400ppm_1100hpa",
        "co2:co2_groupa_400ppm_1000hpa",
        "co2:co2_groupa_400ppm_900hpa",
        "co2:co2_groupa_800ppm_1100hpa",
        "co2:co2_groupa_800ppm_1000hpa",
        "co2:co2_groupa_800ppm_900hpa",
    ]


def test_route_planner_uses_water_first_threshold_to_choose_route_sequence() -> None:
    planner = RoutePlanner(
        AppConfig.from_dict({"workflow": {"route_mode": "h2o_then_co2", "water_first_temp_gte": 30.0}}),
        PointParser(),
    )
    cool_group = [
        CalibrationPoint(index=1, temperature_c=25.0, humidity_pct=30.0, pressure_hpa=900.0, route="h2o"),
        CalibrationPoint(index=2, temperature_c=25.0, co2_ppm=400.0, pressure_hpa=900.0, route="co2", co2_group="A"),
    ]
    hot_group = [
        CalibrationPoint(index=3, temperature_c=35.0, humidity_pct=35.0, pressure_hpa=900.0, route="h2o"),
        CalibrationPoint(index=4, temperature_c=35.0, co2_ppm=800.0, pressure_hpa=900.0, route="co2", co2_group="A"),
    ]

    assert planner.water_first_temp_threshold() == 30.0
    assert planner.route_sequence(cool_group) == ["co2", "h2o"]
    assert planner.route_sequence(hot_group) == ["h2o", "co2"]


def test_route_planner_water_first_all_temps_forces_h2o_first_for_non_subzero_groups() -> None:
    planner = RoutePlanner(
        AppConfig.from_dict(
            {
                "workflow": {
                    "route_mode": "h2o_then_co2",
                    "water_first_all_temps": True,
                    "water_first_temp_gte": 30.0,
                }
            }
        ),
        PointParser(),
    )
    points = [
        CalibrationPoint(index=1, temperature_c=10.0, humidity_pct=30.0, pressure_hpa=900.0, route="h2o"),
        CalibrationPoint(index=2, temperature_c=10.0, co2_ppm=400.0, pressure_hpa=900.0, route="co2", co2_group="A"),
    ]

    assert planner.water_first_temp_threshold() == float("-inf")
    assert planner.route_sequence(points) == ["h2o", "co2"]
