"""Tests for V1.5 no-write guard semantics.

These tests lock the behaviour of the no-write guard added to CalibrationRunner:
- Blocked writes must NOT set *_command_sent flags.
- Blocked writes must set blocked-count fields.
- Offline report paths must not be blocked.
- Audit summary must emit in finally (tested indirectly via attribute state).
"""

from __future__ import annotations

import pytest

from gas_calibrator.workflow.runner import CalibrationRunner


def _make_runner(cfg=None, devices=None, log_lines=None):
    """Build a minimal runner with captured log."""
    cfg = dict(cfg or {})
    cfg.setdefault("paths", {}).setdefault("output_dir", "logs")

    if devices is None:
        devices = {}

    logged: list[str] = []

    def log_fn(msg: str) -> None:
        logged.append(str(msg))

    class FakeLogger:
        run_dir = None

    from unittest.mock import MagicMock
    runner = CalibrationRunner(cfg, devices, FakeLogger(), log_fn, log_fn)
    return runner, logged


class TestNoWriteGuardFieldSemantics:
    """Verify that blocked writes do NOT set command_sent flags."""

    def test_guard_defaults(self):
        runner, _ = _make_runner()
        assert runner._no_write_guard_enabled is True
        assert runner._attempted_write_count == 0
        assert runner._identity_write_command_sent is False
        assert runner._calibration_write_command_sent is False
        assert runner._coefficient_write_command_sent is False
        assert runner._coefficient_write_blocked is False
        assert runner._coefficient_write_blocked_count == 0
        assert runner._no_write_guard_blocked_count == 0
        assert runner._no_write_guard_blocked_reasons == []

    def test_blocked_does_not_mark_command_sent(self):
        runner, _ = _make_runner()
        result = runner._check_no_write_guard("coefficient_write")
        assert result is True
        assert runner._coefficient_write_command_sent is False
        assert runner._identity_write_command_sent is False
        assert runner._calibration_write_command_sent is False

    def test_blocked_does_not_increment_attempted_write_count(self):
        runner, _ = _make_runner()
        runner._check_no_write_guard("coefficient_write")
        assert runner._attempted_write_count == 0

    def test_blocked_increments_blocked_count(self):
        runner, _ = _make_runner()
        assert runner._no_write_guard_blocked_count == 0
        runner._check_no_write_guard("coefficient_write")
        assert runner._no_write_guard_blocked_count == 1
        runner._check_no_write_guard("coefficient_write")
        assert runner._no_write_guard_blocked_count == 2

    def test_blocked_records_reason(self):
        runner, _ = _make_runner()
        runner._check_no_write_guard("coefficient_write")
        runner._check_no_write_guard("postrun_write")
        assert len(runner._no_write_guard_blocked_reasons) == 2
        assert "BLOCKED coefficient_write" in runner._no_write_guard_blocked_reasons[0]
        assert "BLOCKED postrun_write" in runner._no_write_guard_blocked_reasons[1]

    def test_coefficient_write_blocked_sets_blocked_fields(self):
        runner, _ = _make_runner()
        cfg = {"coefficients": {"enabled": True}}
        runner.cfg = cfg
        runner._maybe_write_coefficients()
        assert runner._coefficient_write_blocked is True
        assert runner._coefficient_write_blocked_count == 1
        assert runner._coefficient_write_command_sent is False
        assert runner._attempted_write_count == 0

    def test_guard_logs_blocked_message(self):
        runner, logged = _make_runner()
        runner._check_no_write_guard("coefficient_write")
        combined = " ".join(logged)
        assert "no-write-guard" in combined.lower()
        assert "BLOCKED" in combined
        assert "coefficient_write" in combined
        assert "no_write_guard_blocked_count=1" in combined

    def test_guard_disabled_allows_through(self):
        runner, _ = _make_runner()
        runner._no_write_guard_enabled = False
        result = runner._check_no_write_guard("coefficient_write")
        assert result is False
        assert runner._no_write_guard_blocked_count == 0


class TestNoWriteGuardAuditSummary:
    """Verify audit summary emits correct fields regardless of path."""

    def test_audit_summary_includes_blocked_fields(self):
        runner, logged = _make_runner()
        cfg = {"coefficients": {"enabled": True}}
        runner.cfg = cfg
        runner._maybe_write_coefficients()
        runner._emit_no_write_audit_summary()
        combined = " ".join(logged)
        assert "no-write-audit" in combined.lower()
        assert "attempted_write_count=0" in combined
        assert "identity_write_command_sent=false" in combined
        assert "calibration_write_command_sent=false" in combined
        assert "coefficient_write_command_sent=false" in combined
        assert "coefficient_write_blocked=true" in combined
        assert "coefficient_write_blocked_count=1" in combined
        assert "no_write_guard_blocked_count=1" in combined
        assert "BLOCKED coefficient_write" in combined

    def test_audit_summary_when_no_blocks(self):
        runner, logged = _make_runner()
        runner._emit_no_write_audit_summary()
        combined = " ".join(logged)
        assert "coefficient_write_blocked=false" in combined
        assert "coefficient_write_blocked_count=0" in combined
        assert "no_write_guard_blocked_count=0" in combined
        assert "no_write_guard_blocked_reasons=none" in combined
