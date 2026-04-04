import json
from pathlib import Path

import pandas as pd
import pytest

from gas_calibrator.modeling.offline_model_runner import run_offline_modeling_analysis


def test_run_offline_modeling_analysis_writes_outputs(tmp_path: Path) -> None:
    project_root = tmp_path / "proj"
    configs_dir = project_root / "configs"
    logs_dir = project_root / "logs"
    configs_dir.mkdir(parents=True)
    logs_dir.mkdir()

    rows = []
    for index in range(24):
        ratio = 0.7 + 0.03 * index
        temp = 10.0 + index * 0.5
        pressure = 98.0 + (index % 5)
        temp_k = temp + 273.15
        target = 20 + 30 * ratio - 2 * (ratio**2) + 0.1 * temp_k - 0.5 * pressure
        rows.append(
            {
                "Analyzer": "GA01",
                "PointRow": index,
                "PointPhase": "CO2",
                "PointTag": f"p{index}",
                "PointTitle": f"Point {index}",
                "ppm_CO2_Tank": target,
                "R_CO2": ratio,
                "T1": temp,
                "BAR": pressure,
            }
        )
    input_path = project_root / "summary.csv"
    pd.DataFrame(rows).to_csv(input_path, index=False)

    default_config = {
        "paths": {"output_dir": "logs"},
        "modeling": {"enabled": False},
    }
    (configs_dir / "default_config.json").write_text(json.dumps(default_config, ensure_ascii=False), encoding="utf-8")
    (configs_dir / "modeling_offline.json").write_text(
        json.dumps(
            {
                "modeling": {
                    "enabled": True,
                    "data_source": {
                        "path": str(input_path),
                        "gas": "co2",
                        "target_key": "ppm_CO2_Tank",
                        "ratio_keys": ["R_CO2"],
                        "temp_keys": ["T1"],
                        "pressure_keys": ["BAR"],
                    },
                    "export": {
                        "enabled": True,
                        "formats": ["json", "csv"],
                        "output_dir": str(project_root / "logs" / "modeling_offline"),
                    },
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    result = run_offline_modeling_analysis(
        base_config_path=configs_dir / "default_config.json",
        modeling_config_path=configs_dir / "modeling_offline.json",
    )

    assert result["run_dir"].exists()
    assert (result["run_dir"] / "summary.txt").exists()
    assert (result["run_dir"] / "summary.json").exists()
    assert result["recommended_model"]
    assert any(path.name.endswith(".xlsx") for key, path in result["exported_paths"].items() if key.startswith("prediction_"))
    assert any(path.name.endswith(".csv") for key, path in result["exported_paths"].items() if key.startswith("prediction_"))


def test_run_offline_modeling_analysis_reads_excel_sheet_from_config(tmp_path: Path) -> None:
    project_root = tmp_path / "proj"
    configs_dir = project_root / "configs"
    logs_dir = project_root / "logs"
    configs_dir.mkdir(parents=True)
    logs_dir.mkdir()

    rows = []
    for index in range(24):
        ratio = 0.8 + 0.02 * index
        temp = 15.0 + index * 0.2
        pressure = 99.0 + (index % 3)
        temp_k = temp + 273.15
        target = 12 + 18 * ratio + 0.05 * temp_k - 0.4 * pressure
        rows.append(
            {
                "Analyzer": "GA02",
                "PointRow": index,
                "PointPhase": "CO2",
                "PointTag": f"p{index}",
                "PointTitle": f"Point {index}",
                "ppm_CO2_Tank": target,
                "R_CO2": ratio,
                "T1": temp,
                "BAR": pressure,
            }
        )
    input_path = project_root / "summary.xlsx"
    pd.DataFrame(rows).to_excel(input_path, sheet_name="Data", index=False)

    (configs_dir / "default_config.json").write_text(
        json.dumps({"paths": {"output_dir": "logs"}, "modeling": {"enabled": False}}, ensure_ascii=False),
        encoding="utf-8",
    )
    (configs_dir / "modeling_offline.json").write_text(
        json.dumps(
            {
                "modeling": {
                    "enabled": True,
                    "data_source": {
                        "path": str(input_path),
                        "file_type": "xlsx",
                        "sheet_name": "Data",
                        "gas": "co2",
                        "target_key": "ppm_CO2_Tank",
                        "ratio_keys": ["R_CO2"],
                        "temp_keys": ["T1"],
                        "pressure_keys": ["BAR"],
                    },
                    "export": {
                        "enabled": True,
                        "formats": ["json"],
                        "output_dir": str(project_root / "logs" / "modeling_offline"),
                    },
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    result = run_offline_modeling_analysis(
        base_config_path=configs_dir / "default_config.json",
        modeling_config_path=configs_dir / "modeling_offline.json",
    )

    summary_payload = json.loads((result["run_dir"] / "summary.json").read_text(encoding="utf-8"))
    assert summary_payload["input_path"] == str(input_path)
    assert summary_payload["file_type"] == "xlsx"
    assert summary_payload["sheet_name"] == "Data"
    assert "prediction_analysis_paths" in summary_payload


def test_run_offline_modeling_analysis_reports_missing_input_path(tmp_path: Path) -> None:
    project_root = tmp_path / "proj"
    configs_dir = project_root / "configs"
    configs_dir.mkdir(parents=True)
    (project_root / "logs").mkdir()

    (configs_dir / "default_config.json").write_text(
        json.dumps({"paths": {"output_dir": "logs"}, "modeling": {"enabled": False}}, ensure_ascii=False),
        encoding="utf-8",
    )
    (configs_dir / "modeling_offline.json").write_text(
        json.dumps(
            {
                "modeling": {
                    "enabled": True,
                    "data_source": {
                        "path": "",
                        "gas": "co2",
                        "target_key": "ppm_CO2_Tank",
                        "ratio_keys": ["R_CO2"],
                        "temp_keys": ["T1"],
                        "pressure_keys": ["BAR"],
                    },
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="未选择"):
        run_offline_modeling_analysis(
            base_config_path=configs_dir / "default_config.json",
            modeling_config_path=configs_dir / "modeling_offline.json",
        )


def test_run_offline_modeling_analysis_exports_h2o_cross_coefficients(tmp_path: Path) -> None:
    project_root = tmp_path / "proj"
    configs_dir = project_root / "configs"
    logs_dir = project_root / "logs"
    configs_dir.mkdir(parents=True)
    logs_dir.mkdir()

    rows = []
    for index in range(32):
        ratio = 0.7 + 0.02 * index
        temp = 12.0 + index * 0.3
        pressure = 98.0 + (index % 4)
        h2o = 900.0 + (index % 5) * 140.0 + index * 8.0
        temp_k = temp + 273.15
        target = 18 + 26 * ratio - 3 * (ratio**2) + 0.05 * temp_k - 0.45 * pressure + 0.04 * h2o + 0.018 * ratio * h2o
        rows.append(
            {
                "Analyzer": "GA05",
                "PointRow": index,
                "PointPhase": "CO2",
                "PointTag": f"p{index}",
                "PointTitle": f"Point {index}",
                "ppm_CO2_Tank": target,
                "ppm_H2O_Dew": h2o,
                "R_CO2": ratio,
                "T1": temp,
                "BAR": pressure,
            }
        )
    input_path = project_root / "summary.csv"
    pd.DataFrame(rows).to_csv(input_path, index=False)

    (configs_dir / "default_config.json").write_text(
        json.dumps({"paths": {"output_dir": "logs"}, "modeling": {"enabled": False}}, ensure_ascii=False),
        encoding="utf-8",
    )
    (configs_dir / "modeling_offline.json").write_text(
        json.dumps(
            {
                "modeling": {
                    "enabled": True,
                    "data_source": {
                        "path": str(input_path),
                        "gas": "co2",
                        "target_key": "ppm_CO2_Tank",
                        "ratio_keys": ["R_CO2"],
                        "temp_keys": ["T1"],
                        "pressure_keys": ["BAR"],
                        "humidity_keys": ["ppm_H2O_Dew"],
                    },
                    "candidate_models": {
                        "Model_A": ["intercept", "R", "R2", "T", "P"],
                        "Model_E": ["intercept", "R", "R2", "R3", "T", "T2", "RT", "P", "H", "H2", "RH"],
                    },
                    "export": {
                        "enabled": True,
                        "formats": ["json"],
                        "output_dir": str(project_root / "logs" / "modeling_offline"),
                    },
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    result = run_offline_modeling_analysis(
        base_config_path=configs_dir / "default_config.json",
        modeling_config_path=configs_dir / "modeling_offline.json",
    )

    summary_payload = json.loads((result["run_dir"] / "summary.json").read_text(encoding="utf-8"))
    comparison_json = next(value for key, value in result["exported_paths"].items() if key == "comparison_json")
    comparison_payload = json.loads(comparison_json.read_text(encoding="utf-8"))
    assert summary_payload["recommended_model"] == "Model_E"
    assert set(comparison_payload["H2O_cross_coefficients"].keys()) == {"a_H", "a_H2", "a_RH"}
