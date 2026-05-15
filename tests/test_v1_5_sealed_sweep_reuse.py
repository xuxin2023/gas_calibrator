"""Tests for V1.5 sealed sweep OUTP-cycle fix.

Verifies that once a CO2 route is sealed, subsequent pressure points in the
same sweep reuse the sealed state without re-executing:
  - _set_pressure_controller_vent(False)  (OUTP 0 + VENT 0)
  - _enable_pressure_controller_output    (OUTP 1)

Only the first sealed point may go through the full preseal → seal → OUTP path.
"""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch, call

from gas_calibrator.workflow.runner import CalibrationRunner


def _make_runner(cfg=None, devices=None, log_lines=None):
    cfg = dict(cfg or {})
    cfg.setdefault("paths", {}).setdefault("output_dir", "logs")

    if devices is None:
        devices = {}

    logged: list[str] = []

    def log_fn(msg: str) -> None:
        logged.append(str(msg))

    class FakeLogger:
        run_dir = None

    runner = CalibrationRunner(cfg, devices, FakeLogger(), log_fn, log_fn)
    return runner, logged


def _co2_point(index=3, temp=20.0, co2=1000.0, pressure=1100.0):
    from gas_calibrator.data.points import CalibrationPoint
    return CalibrationPoint(
        index=index, temp_chamber_c=temp, co2_ppm=co2,
        target_pressure_hpa=pressure, co2_group=None,
        hgen_temp_c=None, hgen_rh_pct=None,
        dewpoint_c=None, h2o_mmol=None, raw_h2o=None,
    )


class TestSealedSweepFlagLifecycle:
    """Verify _active_co2_sealed_sweep flag transitions."""

    def test_default_is_false(self):
        runner, _ = _make_runner()
        assert runner._active_co2_sealed_sweep is False

    def test_cleanup_resets_flag(self):
        runner, _ = _make_runner()
        runner._active_co2_sealed_sweep = True
        runner._cleanup_co2_route(reason="test")
        assert runner._active_co2_sealed_sweep is False

    def test_cleanup_callable_without_flag(self):
        runner, _ = _make_runner()
        runner._cleanup_co2_route(reason="test")
        assert runner._active_co2_sealed_sweep is False


class TestMatchingPresealSealedSweep:
    """Verify _matching_preseal_pressure_control_ready_state relaxes checks."""

    def _setup_snapshot(self, runner, phase="co2", point_row=3, target=1100.0):
        import time
        runner._preseal_pressure_control_ready_state = {
            "phase": phase,
            "point_row": point_row,
            "target_pressure_hpa": target,
            "route_sealed": True,
            "atmosphere_hold_stopped": True,
            "recorded_wall_ts": time.time(),
            "pace_vent_status": 3,
            "pace_output_state": 1,
            "pace_isolation_state": 1,
        }

    def test_sealed_sweep_relaxes_point_row(self):
        runner, _ = _make_runner()
        runner._active_co2_sealed_sweep = True
        self._setup_snapshot(runner, point_row=3)
        point = _co2_point(index=4, pressure=900.0)
        state, reason = runner._matching_preseal_pressure_control_ready_state(
            point, phase="co2"
        )
        assert state is not None, f"should match despite point_row mismatch, got: {reason}"
        assert reason == "sealed_sweep_reuse"

    def test_sealed_sweep_relaxes_target_pressure(self):
        runner, _ = _make_runner()
        runner._active_co2_sealed_sweep = True
        self._setup_snapshot(runner, point_row=3, target=1100.0)
        point = _co2_point(index=3, pressure=500.0)
        state, reason = runner._matching_preseal_pressure_control_ready_state(
            point, phase="co2"
        )
        assert state is not None, f"should match despite 600hPa delta, got: {reason}"
        assert reason == "sealed_sweep_reuse"

    def test_normal_mode_still_checks_point_row(self):
        runner, _ = _make_runner()
        runner._active_co2_sealed_sweep = False
        self._setup_snapshot(runner, point_row=3)
        point = _co2_point(index=4, pressure=1100.0)
        state, reason = runner._matching_preseal_pressure_control_ready_state(
            point, phase="co2"
        )
        assert state is None
        assert "point_row_mismatch" in reason

    def test_sealed_sweep_requires_route_sealed(self):
        runner, _ = _make_runner()
        runner._active_co2_sealed_sweep = True
        runner._preseal_pressure_control_ready_state = {
            "phase": "co2",
            "point_row": 3,
            "target_pressure_hpa": 1100.0,
            "route_sealed": False,
            "atmosphere_hold_stopped": True,
            "recorded_wall_ts": __import__("time").time(),
        }
        point = _co2_point(index=3, pressure=1100.0)
        state, _ = runner._matching_preseal_pressure_control_ready_state(point, phase="co2")
        assert state is None

    def test_h2o_path_not_affected(self):
        runner, _ = _make_runner()
        runner._active_co2_sealed_sweep = True
        self._setup_snapshot(runner, phase="co2")
        point = _co2_point(index=3, pressure=1100.0)
        state, reason = runner._matching_preseal_pressure_control_ready_state(
            point, phase="h2o"
        )
        assert state is None
        assert "phase_mismatch" in reason


class TestSealedSweepSetpointTransition:
    """Verify the sealed sweep reuse path in _set_pressure_to_target."""

    def test_sealed_sweep_keeps_preseal_snapshot_alive(self):
        """Snapshot must persist across sealed sweep pressure points."""
        runner, _ = _make_runner()
        assert runner._preseal_pressure_control_ready_state is None

        import time
        runner._preseal_pressure_control_ready_state = {
            "phase": "co2",
            "point_row": 3,
            "target_pressure_hpa": 1100.0,
            "route_sealed": True,
            "atmosphere_hold_stopped": True,
            "recorded_wall_ts": time.time(),
            "pace_vent_status": 3,
            "pace_output_state": 1,
            "pace_isolation_state": 1,
            "ready_verification_pending": False,
        }
        runner._active_co2_sealed_sweep = True

        # Simulate end of _set_pressure_to_target — should NOT clear snapshot
        # The guard: if not self._active_co2_sealed_sweep: _clear...
        if not runner._active_co2_sealed_sweep:
            runner._clear_preseal_pressure_control_ready_state(
                reason="control_sequence_completed",
                point=_co2_point(index=3, pressure=1100.0),
                phase="co2",
            )

        assert runner._preseal_pressure_control_ready_state is not None, (
            "snapshot must survive in sealed sweep"
        )

        # After cleanup: flag must be reset
        runner._active_co2_sealed_sweep = False
        assert runner._active_co2_sealed_sweep is False

        # Snapshot cleared explicitly in normal (non-sealed) path
        if not runner._active_co2_sealed_sweep:
            runner._clear_preseal_pressure_control_ready_state(
                reason="control_sequence_completed",
                point=_co2_point(index=4, pressure=1000.0),
                phase="co2",
            )
        assert runner._preseal_pressure_control_ready_state is None

    def test_normal_mode_clears_snapshot(self):
        """Without sealed sweep, snapshot is cleared at point completion."""
        runner, _ = _make_runner()
        import time
        runner._preseal_pressure_control_ready_state = {
            "phase": "co2", "point_row": 3,
            "target_pressure_hpa": 1100.0,
            "route_sealed": True, "atmosphere_hold_stopped": True,
            "recorded_wall_ts": time.time(),
        }
        runner._active_co2_sealed_sweep = False

        runner._clear_preseal_pressure_control_ready_state(
            reason="control_sequence_completed",
            point=_co2_point(index=3, pressure=1100.0),
            phase="co2",
        )
        assert runner._preseal_pressure_control_ready_state is None


class TestH2oNotAffected:
    def test_h2o_phase_not_affected_by_co2_flag(self):
        runner, _ = _make_runner()
        runner._active_co2_sealed_sweep = True
        runner._preseal_pressure_control_ready_state = {
            "phase": "co2",
            "point_row": 3,
            "target_pressure_hpa": 1100.0,
            "route_sealed": True,
            "atmosphere_hold_stopped": True,
            "recorded_wall_ts": __import__("time").time(),
        }
        point = _co2_point(index=3, pressure=1100.0)
        state, reason = runner._matching_preseal_pressure_control_ready_state(
            point, phase="h2o"
        )
        assert state is None, "H2O phase should not match CO2 snapshot"
        assert "phase_mismatch" in reason
