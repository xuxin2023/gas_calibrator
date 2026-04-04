from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from gas_calibrator.v2.adapters.offline_refit_runner import run_from_cli


def test_run_from_cli_exports_expected_files(tmp_path: Path) -> None:
    input_path = tmp_path / "ga01_co2.csv"
    rows = []
    for cycle in range(3):
        for target in [0.0, 180.0, 400.0, 800.0]:
            temperature = 20.0 + float(cycle)
            pressure = 110.0 + float(cycle)
            ratio = 1.0 + 0.001 * target + 0.002 * (temperature + 273.15) + 0.0003 * pressure
            rows.append(
                {
                    "Analyzer": "GA01",
                    "PointRow": len(rows) + 1,
                    "PointPhase": "气路",
                    "PointTag": f"co2_{len(rows)}",
                    "PointTitle": f"point-{len(rows)}",
                    "ppm_CO2_Tank": target,
                    "R_CO2": ratio,
                    "T1": temperature,
                    "BAR": pressure,
                }
            )
    pd.DataFrame(rows).to_csv(input_path, index=False, encoding="utf-8-sig")

    config_path = tmp_path / "refit.json"
    config_path.write_text(
        json.dumps(
            {
                "enabled": True,
                "gas_type": "co2",
                "filtering": {
                    "enable_refit_filtering": True,
                    "target_bins_co2": [0, 200, 500, 1000],
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    exported = run_from_cli(
        input_path=str(input_path),
        gas_type="co2",
        analyzer_id="GA01",
        config_path=str(config_path),
        output_dir=str(tmp_path / "out"),
    )
    assert Path(exported["excel"]).exists()
    assert Path(exported["audit_csv"]).exists()
    assert Path(exported["summary_csv"]).exists()
