from pathlib import Path

from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.ui import app as app_mod


def _make_stub_app():
    app = app_mod.App.__new__(app_mod.App)
    app.statuses = []
    app.logs = []
    app.set_status = app.statuses.append
    app.log = app.logs.append
    return app


def _base_cfg():
    return {
        "devices": {
            "pressure_controller": {"enabled": True},
            "pressure_gauge": {"enabled": True},
        },
        "workflow": {"startup_connect_check": {"enabled": True}},
    }


def test_startup_connectivity_retry_then_pass(monkeypatch, tmp_path: Path) -> None:
    app = _make_stub_app()
    app.cfg = _base_cfg()
    logger = RunLogger(tmp_path)
    calls = []

    def fake_self_test(_cfg, **kwargs):
        only = kwargs.get("only_devices")
        calls.append(list(only) if only else None)
        if len(calls) == 1:
            return {
                "pressure_controller": {"ok": False, "err": "NO_RESPONSE"},
                "pressure_gauge": {"ok": False, "err": "NO_RESPONSE"},
            }
        return {
            "pressure_controller": {"ok": True},
            "pressure_gauge": {"ok": True},
        }

    monkeypatch.setattr(app_mod, "run_self_test", fake_self_test)
    monkeypatch.setattr(app_mod.messagebox, "askretrycancel", lambda *_args, **_kwargs: True)

    try:
        ok = app._startup_connectivity_check(logger)
    finally:
        logger.close()

    assert ok is True
    assert calls[0] is None
    assert sorted(calls[1]) == ["pressure_controller", "pressure_gauge"]


def test_startup_connectivity_cancel(monkeypatch, tmp_path: Path) -> None:
    app = _make_stub_app()
    app.cfg = _base_cfg()
    logger = RunLogger(tmp_path)

    monkeypatch.setattr(
        app_mod,
        "run_self_test",
        lambda *_args, **_kwargs: {"pressure_controller": {"ok": False, "err": "NO_RESPONSE"}},
    )
    monkeypatch.setattr(app_mod.messagebox, "askretrycancel", lambda *_args, **_kwargs: False)

    try:
        ok = app._startup_connectivity_check(logger)
    finally:
        logger.close()

    assert ok is False
    assert "空闲" in app.statuses
