import json
from pathlib import Path

import pandas as pd
import pytest

from gas_calibrator.modeling.config_loader import (
    find_latest_modeling_artifacts,
    load_modeling_config,
    save_modeling_config,
    summarize_modeling_config,
    validate_modeling_input_source,
)


def test_load_modeling_config_merges_defaults(tmp_path: Path) -> None:
    project_root = tmp_path / "proj"
    configs_dir = project_root / "configs"
    configs_dir.mkdir(parents=True)
    (project_root / "logs").mkdir()
    default_config = {
        "paths": {"output_dir": "logs"},
        "modeling": {
            "enabled": False,
            "fit_method": "ordinary_least_squares",
        },
    }
    (configs_dir / "default_config.json").write_text(json.dumps(default_config, ensure_ascii=False), encoding="utf-8")
    (configs_dir / "modeling_offline.json").write_text(
        json.dumps(
            {
                "modeling": {
                    "enabled": True,
                    "data_source": {"path": "sample.csv"},
                    "outlier_filter": {"enabled": True, "method": "residual", "threshold": 3.0},
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    loaded = load_modeling_config(
        base_config_path=configs_dir / "default_config.json",
        modeling_config_path=configs_dir / "modeling_offline.json",
    )

    assert loaded["modeling"]["enabled"] is True
    assert loaded["modeling"]["fit_method"] == "least_squares"
    assert loaded["modeling"]["outlier_filter"]["method"] == "residual_sigma"
    assert "离线建模分析功能" in summarize_modeling_config(loaded)


def test_find_latest_modeling_artifacts_returns_latest_files(tmp_path: Path) -> None:
    run_dir = tmp_path / "modeling_20260316_120000"
    run_dir.mkdir()
    (run_dir / "summary.txt").write_text("ok", encoding="utf-8")
    (run_dir / "summary.json").write_text("{}", encoding="utf-8")

    artifacts = find_latest_modeling_artifacts(tmp_path)

    assert artifacts["run_dir"] == run_dir
    assert artifacts["summary_txt"] == run_dir / "summary.txt"


def test_save_modeling_config_persists_input_source_and_reload(tmp_path: Path) -> None:
    project_root = tmp_path / "proj"
    configs_dir = project_root / "configs"
    configs_dir.mkdir(parents=True)
    (project_root / "logs").mkdir()
    default_config = {
        "paths": {"output_dir": "logs"},
        "modeling": {"enabled": False},
    }
    default_path = configs_dir / "default_config.json"
    modeling_path = configs_dir / "modeling_offline.json"
    default_path.write_text(json.dumps(default_config, ensure_ascii=False), encoding="utf-8")

    source_path = project_root / "summary.csv"
    pd.DataFrame([{"ppm_CO2_Tank": 1.0, "R_CO2": 2.0, "T1": 20.0, "BAR": 100.0}]).to_csv(source_path, index=False)

    save_modeling_config(
        modeling_config_path=modeling_path,
        base_config_path=default_path,
        path=source_path,
        file_type="auto",
        sheet_name="Sheet1",
    )

    loaded = load_modeling_config(
        base_config_path=default_path,
        modeling_config_path=modeling_path,
    )

    data_source = loaded["modeling"]["data_source"]
    assert Path(data_source["path"]) == source_path.resolve()
    assert data_source["file_type"] == "csv"
    assert data_source["format"] == "csv"
    assert data_source["sheet_name"] == 0


def test_validate_modeling_input_source_reports_clear_errors(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="未选择"):
        validate_modeling_input_source({}, project_root=tmp_path)

    with pytest.raises(FileNotFoundError, match="不存在"):
        validate_modeling_input_source({"path": "missing.csv"}, project_root=tmp_path)

    unsupported = tmp_path / "summary.txt"
    unsupported.write_text("demo", encoding="utf-8")
    with pytest.raises(ValueError, match="扩展名"):
        validate_modeling_input_source({"path": str(unsupported), "file_type": "auto"}, project_root=tmp_path)
