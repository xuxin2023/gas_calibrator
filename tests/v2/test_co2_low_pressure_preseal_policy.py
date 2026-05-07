from __future__ import annotations

from types import SimpleNamespace

import pytest

from gas_calibrator.v2.core.services.pressure_control_service import (
    PressureControlService,
    PressureWaitResult,
)
from gas_calibrator.v2.core.models import CalibrationPoint


def _point(index=1, ppm=1000.0, pressure=800.0, route="co2"):
    return CalibrationPoint(
        index=index,
        temperature_c=20.0,
        co2_ppm=ppm,
        pressure_hpa=pressure,
        pressure_mode="sealed_controlled",
        pressure_selection_token="800hPa",
        route=route,
    )


def _make_host(overrides=None):
    overrides = dict(overrides or {})

    def cfg_get(path, default=None):
        return dict(overrides).get(path, default)

    host = SimpleNamespace(
        _cfg_get=cfg_get,
        _as_float=lambda v: None if v in (None, "") else float(v),
        _log=lambda msg: None,
        _check_stop=lambda: None,
        a2_hooks=SimpleNamespace(
            high_pressure_first_point_mode_enabled=True,
            co2_route_conditioning_completed=False,
            co2_route_conditioning_completed_at="",
            seal_allowed=False,
            co2_route_conditioning_at_atmosphere_context={},
        ),
        run_state=SimpleNamespace(humidity=SimpleNamespace(active_post_h2o_co2_zero_flush=False)),
        _capture_preseal_dewpoint_snapshot=lambda: None,
        _make_pressure_reader=lambda: None,
        _device=lambda name: None,
    )
    return host


class TestPositivePresealEnabledForSubAtmospheric:
    def test_800hPa_with_ambient_1015_returns_false(self):
        point = _point(pressure=800.0)
        host = _make_host({"workflow.pressure.co2_preseal_sub_atmospheric_margin_hpa": 50.0})
        svc = PressureControlService(SimpleNamespace(), SimpleNamespace(humidity=SimpleNamespace(active_post_h2o_co2_zero_flush=False)), host=host)

        result = svc._positive_preseal_enabled(
            point, route="co2", measured_atmospheric_pressure_hpa=1015.0
        )
        assert result is False

    def test_1100hPa_with_ambient_1015_returns_true(self):
        point = _point(pressure=1100.0)
        host = _make_host({"workflow.pressure.co2_preseal_sub_atmospheric_margin_hpa": 50.0})
        svc = PressureControlService(SimpleNamespace(), SimpleNamespace(humidity=SimpleNamespace(active_post_h2o_co2_zero_flush=False)), host=host)

        result = svc._positive_preseal_enabled(
            point, route="co2", measured_atmospheric_pressure_hpa=1015.0
        )
        assert result is True

    def test_700hPa_returns_false(self):
        point = _point(pressure=700.0)
        host = _make_host({"workflow.pressure.co2_preseal_sub_atmospheric_margin_hpa": 50.0})
        svc = PressureControlService(SimpleNamespace(), SimpleNamespace(humidity=SimpleNamespace(active_post_h2o_co2_zero_flush=False)), host=host)

        result = svc._positive_preseal_enabled(
            point, route="co2", measured_atmospheric_pressure_hpa=1015.0
        )
        assert result is False

    def test_800hPa_no_ambient_returns_false(self):
        point = _point(pressure=800.0)
        host = _make_host({"workflow.pressure.co2_preseal_sub_atmospheric_margin_hpa": 50.0})
        svc = PressureControlService(SimpleNamespace(), SimpleNamespace(humidity=SimpleNamespace(active_post_h2o_co2_zero_flush=False)), host=host)

        result = svc._positive_preseal_enabled(
            point, route="co2", measured_atmospheric_pressure_hpa=None
        )
        assert result is False

    def test_1100hPa_no_ambient_returns_true(self):
        point = _point(pressure=1100.0)
        host = _make_host({"workflow.pressure.co2_preseal_sub_atmospheric_margin_hpa": 50.0})
        svc = PressureControlService(SimpleNamespace(), SimpleNamespace(humidity=SimpleNamespace(active_post_h2o_co2_zero_flush=False)), host=host)

        result = svc._positive_preseal_enabled(
            point, route="co2", measured_atmospheric_pressure_hpa=None
        )
        assert result is True

    def test_h2o_always_returns_false(self):
        point = _point(pressure=800.0, route="h2o")
        host = _make_host({})
        svc = PressureControlService(SimpleNamespace(), SimpleNamespace(humidity=SimpleNamespace(active_post_h2o_co2_zero_flush=False)), host=host)

        result = svc._positive_preseal_enabled(
            point, route="h2o", measured_atmospheric_pressure_hpa=1015.0
        )
        assert result is False

    def test_800hPa_at_ambient_850_margin_30_returns_false(self):
        point = _point(pressure=800.0)
        host = _make_host({"workflow.pressure.co2_preseal_sub_atmospheric_margin_hpa": 30.0})
        svc = PressureControlService(SimpleNamespace(), SimpleNamespace(humidity=SimpleNamespace(active_post_h2o_co2_zero_flush=False)), host=host)

        result = svc._positive_preseal_enabled(
            point, route="co2", measured_atmospheric_pressure_hpa=825.0
        )
        assert result is False

    def test_800hPa_at_ambient_840_with_margin_30_returns_true(self):
        point = _point(pressure=800.0)
        host = _make_host({"workflow.pressure.co2_preseal_sub_atmospheric_margin_hpa": 30.0})
        svc = PressureControlService(SimpleNamespace(), SimpleNamespace(humidity=SimpleNamespace(active_post_h2o_co2_zero_flush=False)), host=host)

        result = svc._positive_preseal_enabled(
            point, route="co2", measured_atmospheric_pressure_hpa=840.0
        )
        assert result is True


class TestPositivePresealDisabledWhenHighPressureModeFalse:
    def _make_host_no_high_pressure(self, overrides=None):
        overrides = dict(overrides or {})

        def cfg_get(path, default=None):
            return dict(overrides).get(path, default)

        host = SimpleNamespace(
            _cfg_get=cfg_get,
            _as_float=lambda v: None if v in (None, "") else float(v),
            _log=lambda msg: None,
            _check_stop=lambda: None,
            a2_hooks=SimpleNamespace(
                high_pressure_first_point_mode_enabled=False,
                co2_route_conditioning_completed=False,
                co2_route_conditioning_completed_at="",
                seal_allowed=False,
                co2_route_conditioning_at_atmosphere_context={},
            ),
            run_state=SimpleNamespace(humidity=SimpleNamespace(active_post_h2o_co2_zero_flush=False)),
            _capture_preseal_dewpoint_snapshot=lambda: None,
            _make_pressure_reader=lambda: None,
            _device=lambda name: None,
        )
        return host

    def test_800hpa_with_high_pressure_mode_false_returns_false(self):
        point = _point(pressure=800.0)
        host = self._make_host_no_high_pressure({
            "workflow.pressure.positive_preseal_pressurization_enabled": False,
            "workflow.pressure.co2_preseal_sub_atmospheric_margin_hpa": 50.0,
        })
        svc = PressureControlService(
            SimpleNamespace(),
            SimpleNamespace(humidity=SimpleNamespace(active_post_h2o_co2_zero_flush=False)),
            host=host,
        )

        result = svc._positive_preseal_enabled(
            point, route="co2", measured_atmospheric_pressure_hpa=1015.0
        )
        assert result is False

    def test_1100hPa_with_high_pressure_mode_false_still_checks_config(self):
        point = _point(pressure=1100.0)
        host = self._make_host_no_high_pressure({
            "workflow.pressure.positive_preseal_pressurization_enabled": True,
        })
        svc = PressureControlService(
            SimpleNamespace(),
            SimpleNamespace(humidity=SimpleNamespace(active_post_h2o_co2_zero_flush=False)),
            host=host,
        )

        result = svc._positive_preseal_enabled(
            point, route="co2", measured_atmospheric_pressure_hpa=1015.0
        )
        assert result is True

    def test_800hPa_config_explicit_false_returns_false(self):
        point = _point(pressure=800.0)
        host = self._make_host_no_high_pressure({
            "workflow.pressure.positive_preseal_pressurization_enabled": False,
        })
        svc = PressureControlService(
            SimpleNamespace(),
            SimpleNamespace(humidity=SimpleNamespace(active_post_h2o_co2_zero_flush=False)),
            host=host,
        )

        result = svc._positive_preseal_enabled(
            point, route="co2", measured_atmospheric_pressure_hpa=1015.0
        )
        assert result is False

    def test_source_point_target_none_with_high_pressure_mode_false_returns_false(self):
        point = CalibrationPoint(
            index=99,
            temperature_c=20.0,
            co2_ppm=1000.0,
            pressure_hpa=None,
            pressure_mode="ambient_open",
            route="co2",
        )
        host = self._make_host_no_high_pressure({
            "workflow.pressure.positive_preseal_pressurization_enabled": False,
        })
        svc = PressureControlService(
            SimpleNamespace(),
            SimpleNamespace(humidity=SimpleNamespace(active_post_h2o_co2_zero_flush=False)),
            host=host,
        )

        result = svc._positive_preseal_enabled(
            point, route="co2", measured_atmospheric_pressure_hpa=1015.0
        )
        assert result is False
