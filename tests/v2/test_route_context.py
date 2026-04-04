from __future__ import annotations

from gas_calibrator.v2.core.models import CalibrationPhase, CalibrationPoint
from gas_calibrator.v2.core.route_context import RouteContext


def test_route_context_tracks_and_clears_route_runtime_state() -> None:
    point = CalibrationPoint(index=1, temperature_c=25.0, co2_ppm=400.0, route="co2")
    active = CalibrationPoint(index=2, temperature_c=25.0, co2_ppm=400.0, pressure_hpa=1000.0, route="co2")
    context = RouteContext()

    context.enter(
        current_route="co2",
        current_phase=CalibrationPhase.CO2_ROUTE,
        current_point=point,
        source_point=point,
        active_point=point,
        point_tag="co2_source",
        retry=0,
        route_state={"source_point_index": 1},
    )
    context.update(
        current_point=active,
        active_point=active,
        point_tag="co2_active",
        retry=1,
        route_state={"pressure_point_index": 2},
    )

    assert context.current_route == "co2"
    assert context.current_phase is CalibrationPhase.CO2_ROUTE
    assert context.current_point == active
    assert context.source_point == point
    assert context.active_point == active
    assert context.point_tag == "co2_active"
    assert context.retry == 1
    assert context.route_state == {"source_point_index": 1, "pressure_point_index": 2}

    context.clear()

    assert context.current_route == ""
    assert context.current_phase is None
    assert context.current_point is None
    assert context.source_point is None
    assert context.active_point is None
    assert context.point_tag == ""
    assert context.retry == 0
    assert context.route_state == {}
