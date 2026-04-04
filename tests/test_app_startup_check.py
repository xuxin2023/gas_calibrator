from gas_calibrator.ui.app import App


def test_enabled_failures_only_reports_enabled_failures() -> None:
    cfg = {
        "devices": {
            "gas_analyzer": {"enabled": True},
            "dewpoint_meter": {"enabled": False},
            "relay": {"enabled": True},
        }
    }
    results = {
        "gas_analyzer": {"ok": False, "err": "TIMEOUT"},
        "dewpoint_meter": {"ok": False, "err": "NO_PORT"},
        "relay": {"ok": True},
    }

    failures = App._enabled_failures(cfg, results)
    assert failures == [("gas_analyzer", "TIMEOUT")]


def test_enabled_failures_handles_non_dict_result() -> None:
    cfg = {"devices": {"gas_analyzer": {"enabled": True}}}
    results = {"gas_analyzer": "unexpected"}

    failures = App._enabled_failures(cfg, results)
    assert failures == [("gas_analyzer", "UNKNOWN")]


def test_enabled_failures_supports_multi_analyzers() -> None:
    cfg = {
        "devices": {
            "gas_analyzer": {"enabled": False},
            "gas_analyzers": [
                {"enabled": True},
                {"enabled": True},
            ],
        }
    }
    results = {"gas_analyzer": {"ok": False, "err": "MODE2_PARSE_FAILED"}}

    failures = App._enabled_failures(cfg, results)
    assert failures == [("gas_analyzer", "MODE2_PARSE_FAILED")]
