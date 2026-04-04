from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from gas_calibrator.devices.serial_base import ReplaySerial
from gas_calibrator.v2.adapters.analyzer_coefficient_downloader import (
    download_coefficients_to_analyzers,
    load_download_plan,
)


def test_load_download_plan_reads_download_sheet(tmp_path: Path) -> None:
    report_path = tmp_path / "calibration_coefficients.xlsx"
    with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
        pd.DataFrame(
            [
                {
                    "Analyzer": "GA01",
                    "Gas": "CO2",
                    "ModeEnterCommand": "MODE,YGAS,FFF,2",
                    "ModeExitCommand": "MODE,YGAS,FFF,1",
                    "PrimaryCommand": "SENCO1,YGAS,FFF,1.00000e00,2.00000e00,3.00000e00,4.00000e00,0.00000e00,0.00000e00",
                    "SecondaryCommand": "SENCO3,YGAS,FFF,5.00000e00,6.00000e00,7.00000e00,8.00000e00,9.00000e00,0.00000e00",
                }
            ]
        ).to_excel(writer, sheet_name="download_plan", index=False)

    rows = load_download_plan(report_path)

    assert len(rows) == 1
    assert rows[0]["Analyzer"] == "GA01"
    assert rows[0]["PrimaryCommand"].startswith("SENCO1,YGAS,FFF,")


def test_download_coefficients_to_analyzers_sends_expected_commands(tmp_path: Path) -> None:
    report_path = tmp_path / "calibration_coefficients.xlsx"
    with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
        pd.DataFrame(
            [
                {
                    "Analyzer": "GA01",
                    "Gas": "CO2",
                    "ModeEnterCommand": "MODE,YGAS,FFF,2",
                    "ModeExitCommand": "MODE,YGAS,FFF,1",
                    "PrimaryCommand": "SENCO1,YGAS,FFF,1.00000e00,2.00000e00,3.00000e00,4.00000e00,0.00000e00,0.00000e00",
                    "SecondaryCommand": "SENCO3,YGAS,FFF,5.00000e00,6.00000e00,7.00000e00,8.00000e00,9.00000e00,0.00000e00",
                },
                {
                    "Analyzer": "GA01",
                    "Gas": "H2O",
                    "ModeEnterCommand": "MODE,YGAS,FFF,2",
                    "ModeExitCommand": "MODE,YGAS,FFF,1",
                    "PrimaryCommand": "SENCO2,YGAS,FFF,1.10000e00,2.10000e00,3.10000e00,4.10000e00,0.00000e00,0.00000e00",
                    "SecondaryCommand": "SENCO4,YGAS,FFF,5.10000e00,6.10000e00,7.10000e00,8.10000e00,9.10000e00,0.00000e00",
                },
            ]
        ).to_excel(writer, sheet_name="download_plan", index=False)

    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "devices": {
                    "gas_analyzers": [
                        {
                            "name": "ga01",
                            "enabled": True,
                            "port": "COM35",
                            "baud": 115200,
                            "timeout": 0.2,
                            "device_id": "001",
                        }
                    ]
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    serials: list[ReplaySerial] = []

    def serial_factory(**kwargs: object) -> ReplaySerial:
        def on_write(payload: bytes, serial_obj: ReplaySerial) -> None:
            command = payload.decode("ascii", errors="ignore").strip()
            if command.startswith("MODE,YGAS,FFF,") or command.startswith("SENCO"):
                serial_obj.queue_line("<YGAS,001,T>")

        serial_obj = ReplaySerial(on_write=on_write, **kwargs)
        serials.append(serial_obj)
        return serial_obj

    outputs = download_coefficients_to_analyzers(
        report_path=report_path,
        config_path=config_path,
        output_dir=tmp_path,
        serial_factory=serial_factory,
    )

    assert Path(outputs["download_summary"]).exists()
    assert Path(outputs["io_log"]).exists()
    assert len(serials) == 1

    writes = [payload.decode("ascii", errors="ignore").strip() for payload in serials[0].writes]
    assert writes == [
        "SETCOMWAY,YGAS,FFF,0",
        "MODE,YGAS,FFF,2",
        "SENCO1,YGAS,FFF,1.00000e00,2.00000e00,3.00000e00,4.00000e00,0.00000e00,0.00000e00",
        "SENCO3,YGAS,FFF,5.00000e00,6.00000e00,7.00000e00,8.00000e00,9.00000e00,0.00000e00",
        "SENCO2,YGAS,FFF,1.10000e00,2.10000e00,3.10000e00,4.10000e00,0.00000e00,0.00000e00",
        "SENCO4,YGAS,FFF,5.10000e00,6.10000e00,7.10000e00,8.10000e00,9.10000e00,0.00000e00",
        "MODE,YGAS,FFF,1",
    ]
