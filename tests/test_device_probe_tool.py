from gas_calibrator.tools.device_probe import _candidate_thermo_settings, parse_args


def test_candidate_thermo_settings_dedup() -> None:
    cfg = {
        "baud": 2400,
        "parity": "N",
        "bytesize": 8,
        "stopbits": 1,
        "timeout": 1.2,
    }
    rows = _candidate_thermo_settings(cfg, try_all=True)
    assert rows[0] == (2400, "N", 8, 1.0, 1.2)
    assert len(rows) >= 3
    assert len(rows) == len(set(rows))


def test_parse_args_pressure_scan() -> None:
    ns = parse_args(["--config", "configs/default_config.json", "pressure", "--scan-ids"])
    assert ns.mode == "pressure"
    assert ns.scan_ids is True

