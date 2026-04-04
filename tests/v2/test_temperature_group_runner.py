from __future__ import annotations

from types import SimpleNamespace

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.models import CalibrationPoint
from gas_calibrator.v2.core.point_parser import PointParser
from gas_calibrator.v2.core.route_context import RouteContext
from gas_calibrator.v2.core.route_planner import RoutePlanner
from gas_calibrator.v2.core.runners.route_run_result import RouteRunResult
from gas_calibrator.v2.core.runners.temperature_group_runner import TemperatureGroupRunner
from gas_calibrator.v2.exceptions import WorkflowValidationError


def test_temperature_group_runner_dispatches_h2o_then_co2(monkeypatch) -> None:
    calls: list[str] = []
    completed: list[int] = []

    def fake_h2o_execute(self):
        calls.append(f"h2o:{[point.index for point in self.points]}")
        point = self.points[0]
        return RouteRunResult(
            success=True,
            completed_points=[point],
            completed_point_indices=[point.index],
            sampled_points=[point],
            sampled_point_indices=[point.index],
        )

    def fake_co2_execute(self):
        calls.append(f"co2:{self.point.index}")
        completed_points = [
            CalibrationPoint(index=2, temperature_c=25.0, co2_ppm=800.0, pressure_hpa=900.0, route="co2", co2_group="A"),
            CalibrationPoint(index=3, temperature_c=25.0, co2_ppm=800.0, pressure_hpa=1000.0, route="co2", co2_group="A"),
        ]
        return RouteRunResult(
            success=True,
            completed_points=completed_points,
            completed_point_indices=[point.index for point in completed_points],
            sampled_points=completed_points,
            sampled_point_indices=[point.index for point in completed_points],
        )

    monkeypatch.setattr("gas_calibrator.v2.core.runners.temperature_group_runner.H2oRouteRunner.execute", fake_h2o_execute)
    monkeypatch.setattr("gas_calibrator.v2.core.runners.temperature_group_runner.Co2RouteRunner.execute", fake_co2_execute)

    service = SimpleNamespace(
        route_context=RouteContext(),
        route_planner=RoutePlanner(AppConfig.from_dict({"workflow": {"route_mode": "h2o_then_co2"}}), PointParser()),
        status_service=SimpleNamespace(
            check_stop=lambda: calls.append("check_stop"),
            update_status=lambda **kwargs: calls.append(f"update:{kwargs['message']}"),
            mark_point_completed=lambda point, **kwargs: completed.append(point.index),
        ),
        analyzer_fleet_service=SimpleNamespace(attempt_reenable_disabled_analyzers=lambda: calls.append("reenable")),
        _precondition_next_temperature_humidity=lambda points: calls.append("precondition_humidity"),
        _precondition_next_temperature_chamber=lambda points: calls.append("precondition_chamber"),
        _as_int=lambda value: None if value is None else int(value),
    )
    points = [
        CalibrationPoint(index=1, temperature_c=25.0, humidity_pct=30.0, pressure_hpa=900.0, route="h2o"),
        CalibrationPoint(index=2, temperature_c=25.0, co2_ppm=800.0, pressure_hpa=900.0, route="co2", co2_group="A"),
        CalibrationPoint(index=3, temperature_c=25.0, co2_ppm=800.0, pressure_hpa=1000.0, route="co2", co2_group="A"),
    ]
    next_group = [CalibrationPoint(index=4, temperature_c=30.0, humidity_pct=35.0, pressure_hpa=900.0, route="h2o")]

    TemperatureGroupRunner(service, points, next_group=next_group).execute()

    assert calls[:2] == ["check_stop", "update:Temperature group 25.00 C"]
    assert "reenable" in calls
    assert "h2o:[1]" in calls
    assert "co2:2" in calls
    assert "precondition_humidity" in calls
    assert "precondition_chamber" in calls
    assert completed == [1, 2, 3]
    assert service.route_context.current_route == ""


def test_temperature_group_runner_raises_when_route_returns_skipped_points(monkeypatch) -> None:
    completed: list[int] = []

    def fake_h2o_execute(self):
        point = self.points[0]
        return RouteRunResult(
            success=True,
            completed_points=[point],
            completed_point_indices=[point.index],
            sampled_points=[point],
            sampled_point_indices=[point.index],
        )

    def fake_co2_execute(self):
        completed_point = CalibrationPoint(
            index=2,
            temperature_c=25.0,
            co2_ppm=800.0,
            pressure_hpa=900.0,
            route="co2",
            co2_group="A",
        )
        return RouteRunResult(
            success=False,
            completed_points=[completed_point],
            completed_point_indices=[2],
            sampled_points=[completed_point],
            sampled_point_indices=[2],
            skipped_point_indices=[3],
            error="pressure did not stabilize",
        )

    monkeypatch.setattr("gas_calibrator.v2.core.runners.temperature_group_runner.H2oRouteRunner.execute", fake_h2o_execute)
    monkeypatch.setattr("gas_calibrator.v2.core.runners.temperature_group_runner.Co2RouteRunner.execute", fake_co2_execute)

    service = SimpleNamespace(
        route_context=RouteContext(),
        route_planner=RoutePlanner(AppConfig.from_dict({"workflow": {"route_mode": "h2o_then_co2"}}), PointParser()),
        status_service=SimpleNamespace(
            check_stop=lambda: None,
            update_status=lambda **kwargs: None,
            mark_point_completed=lambda point, **kwargs: completed.append(point.index),
        ),
        analyzer_fleet_service=SimpleNamespace(attempt_reenable_disabled_analyzers=lambda: None),
        _precondition_next_temperature_humidity=lambda points: None,
        _precondition_next_temperature_chamber=lambda points: None,
        _as_int=lambda value: None if value is None else int(value),
    )
    points = [
        CalibrationPoint(index=1, temperature_c=25.0, humidity_pct=30.0, pressure_hpa=900.0, route="h2o"),
        CalibrationPoint(index=2, temperature_c=25.0, co2_ppm=800.0, pressure_hpa=900.0, route="co2", co2_group="A"),
        CalibrationPoint(index=3, temperature_c=25.0, co2_ppm=800.0, pressure_hpa=1000.0, route="co2", co2_group="A"),
    ]

    try:
        TemperatureGroupRunner(service, points).execute()
    except WorkflowValidationError as exc:
        assert exc.context["route_failures"][0]["skipped_point_indices"] == [3]
    else:
        raise AssertionError("Expected WorkflowValidationError")

    assert completed == [1, 2]


def test_temperature_group_runner_counts_logical_co2_points_not_raw_pressure_indices(monkeypatch) -> None:
    completed: list[tuple[int, str]] = []

    def fake_co2_execute(self):
        completed_points = [
            CalibrationPoint(index=7, temperature_c=25.0, co2_ppm=0.0, pressure_hpa=1050.0, route="co2", co2_group="A"),
            CalibrationPoint(index=7, temperature_c=25.0, co2_ppm=600.0, pressure_hpa=1050.0, route="co2", co2_group="A"),
        ]
        return RouteRunResult(
            success=True,
            completed_points=completed_points,
            completed_point_indices=[point.index for point in completed_points],
            sampled_points=completed_points,
            sampled_point_indices=[point.index for point in completed_points],
        )

    monkeypatch.setattr("gas_calibrator.v2.core.runners.temperature_group_runner.Co2RouteRunner.execute", fake_co2_execute)

    service = SimpleNamespace(
        route_context=RouteContext(),
        route_planner=RoutePlanner(AppConfig.from_dict({"workflow": {"route_mode": "co2_only"}}), PointParser()),
        status_service=SimpleNamespace(
            check_stop=lambda: None,
            update_status=lambda **kwargs: None,
            mark_point_completed=lambda point, **kwargs: completed.append((point.index, kwargs.get("point_tag", ""))),
        ),
        analyzer_fleet_service=SimpleNamespace(attempt_reenable_disabled_analyzers=lambda: None),
        _precondition_next_temperature_humidity=lambda points: None,
        _precondition_next_temperature_chamber=lambda points: None,
        _as_int=lambda value: None if value is None else int(value),
    )
    points = [
        CalibrationPoint(index=1, temperature_c=25.0, co2_ppm=0.0, pressure_hpa=950.0, route="co2", co2_group="A"),
        CalibrationPoint(index=7, temperature_c=25.0, co2_ppm=600.0, pressure_hpa=1050.0, route="co2", co2_group="A"),
    ]

    TemperatureGroupRunner(service, points).execute()

    assert completed == [
        (7, "co2_groupa_0ppm_1050hpa"),
        (7, "co2_groupa_600ppm_1050hpa"),
    ]


def test_temperature_group_runner_dispatches_co2_before_h2o_when_threshold_not_met(monkeypatch) -> None:
    calls: list[str] = []

    def fake_h2o_execute(self):
        calls.append(f"h2o:{[point.index for point in self.points]}")
        point = self.points[0]
        return RouteRunResult(
            success=True,
            completed_points=[point],
            completed_point_indices=[point.index],
            sampled_points=[point],
            sampled_point_indices=[point.index],
        )

    def fake_co2_execute(self):
        calls.append(f"co2:{self.point.index}")
        point = CalibrationPoint(index=2, temperature_c=25.0, co2_ppm=800.0, pressure_hpa=900.0, route="co2", co2_group="A")
        return RouteRunResult(
            success=True,
            completed_points=[point],
            completed_point_indices=[point.index],
            sampled_points=[point],
            sampled_point_indices=[point.index],
        )

    monkeypatch.setattr("gas_calibrator.v2.core.runners.temperature_group_runner.H2oRouteRunner.execute", fake_h2o_execute)
    monkeypatch.setattr("gas_calibrator.v2.core.runners.temperature_group_runner.Co2RouteRunner.execute", fake_co2_execute)

    service = SimpleNamespace(
        route_context=RouteContext(),
        route_planner=RoutePlanner(
            AppConfig.from_dict(
                {"workflow": {"route_mode": "h2o_then_co2", "water_first_temp_gte": 30.0}}
            ),
            PointParser(),
        ),
        status_service=SimpleNamespace(
            check_stop=lambda: calls.append("check_stop"),
            update_status=lambda **kwargs: calls.append(f"update:{kwargs['message']}"),
            mark_point_completed=lambda point, **kwargs: calls.append(f"done:{point.index}"),
        ),
        analyzer_fleet_service=SimpleNamespace(attempt_reenable_disabled_analyzers=lambda: calls.append("reenable")),
        _precondition_next_temperature_humidity=lambda points: calls.append("precondition_humidity"),
        _precondition_next_temperature_chamber=lambda points: calls.append("precondition_chamber"),
        _as_int=lambda value: None if value is None else int(value),
    )
    points = [
        CalibrationPoint(index=1, temperature_c=25.0, humidity_pct=30.0, pressure_hpa=900.0, route="h2o"),
        CalibrationPoint(index=2, temperature_c=25.0, co2_ppm=800.0, pressure_hpa=900.0, route="co2", co2_group="A"),
    ]
    next_group = [CalibrationPoint(index=4, temperature_c=30.0, humidity_pct=35.0, pressure_hpa=900.0, route="h2o")]

    TemperatureGroupRunner(service, points, next_group=next_group).execute()

    assert calls.index("precondition_humidity") < calls.index("co2:2")
    assert calls.index("co2:2") < calls.index("h2o:[1]")
