"""离线建模配置读取、保存与摘要工具。"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Dict, Mapping, MutableMapping, Optional, Sequence

from ..config import load_config


SUPPORTED_MODELING_FILE_TYPES = {"auto", "csv", "xlsx", "xls"}

DEFAULT_MODELING_CONFIG: Dict[str, Any] = {
    "enabled": False,
    "description": "离线建模分析功能，默认不参与当前自动校准运行流程。",
    "data_source": {
        "enabled": True,
        "path": "",
        "sheet_name": 0,
        "format": "auto",
        "file_type": "auto",
        "gas": "co2",
        "target_key": "ppm_CO2_Tank",
        "ratio_keys": ["R_CO2"],
        "temp_keys": ["T1"],
        "pressure_keys": ["BAR"],
        "humidity_keys": ["ppm_H2O_Dew", "H2O", "h2o_mmol"],
    },
    "candidate_models": [
        ["intercept", "R", "R2", "T", "P"],
        ["intercept", "R", "R2", "R3", "T", "T2", "RT", "P"],
        ["intercept", "R", "R2", "R3", "T", "T2", "RT", "P", "RTP"],
    ],
    "fit_method": "ordinary_least_squares",
    "ridge_lambda": 1e-6,
    "dataset_split": {
        "train_ratio": 0.7,
        "val_ratio": 0.15,
        "random_seed": 42,
        "shuffle": True,
    },
    "outlier_filter": {
        "enabled": False,
        "method": "iqr",
        "threshold": 1.5,
    },
    "simplification": {
        "enabled": True,
        "method": "column_norm",
        "selection_scope": "train",
        "auto_digits": True,
        "target_digits": 6,
        "rmse_tolerance": 0.0,
        "digit_candidates": [8, 7, 6, 5, 4],
    },
    "export": {
        "enabled": True,
        "formats": ["json", "csv"],
        "output_dir": "logs/modeling_offline",
    },
}


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _deep_merge(base: Dict[str, Any], overlay: Mapping[str, Any]) -> Dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in overlay.items():
        if isinstance(value, Mapping) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _normalize_candidate_models(raw_models: Any) -> Dict[str, list[str]]:
    if isinstance(raw_models, Mapping):
        normalized: Dict[str, list[str]] = {}
        for key, value in raw_models.items():
            normalized[str(key)] = [str(item) for item in list(value or [])]
        return normalized
    models = list(raw_models or [])
    return {f"Model_{index + 1}": [str(item) for item in list(features or [])] for index, features in enumerate(models)}


def _normalize_fit_method(value: Any) -> str:
    fit_method = str(value or "ordinary_least_squares").strip().lower()
    return "least_squares" if fit_method == "ordinary_least_squares" else fit_method


def _display_fit_method(value: Any) -> str:
    fit_method = str(value or "least_squares").strip().lower()
    return "ordinary_least_squares" if fit_method == "least_squares" else fit_method


def _normalize_outlier_method(value: Any) -> str:
    method = str(value or "iqr").strip().lower()
    return "residual_sigma" if method == "residual" else method


def _display_outlier_method(value: Any) -> str:
    method = str(value or "iqr").strip().lower()
    return "residual" if method == "residual_sigma" else method


def _to_storage_path(path: Path, *, project_root: Path) -> str:
    try:
        return str(path.resolve().relative_to(project_root))
    except Exception:
        return str(path.resolve())


def _normalize_sheet_name(value: Any) -> str | int:
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return 0
        if text.lstrip("+-").isdigit():
            return int(text)
        return text
    if value in (None, ""):
        return 0
    return value


def _normalize_modeling_cfg(modeling_cfg: Dict[str, Any], *, project_root: Path) -> Dict[str, Any]:
    cfg = copy.deepcopy(modeling_cfg)
    cfg["fit_method"] = _normalize_fit_method(cfg.get("fit_method"))

    outlier = cfg.setdefault("outlier_filter", {})
    outlier["method"] = _normalize_outlier_method(outlier.get("method"))

    cfg["candidate_models"] = _normalize_candidate_models(cfg.get("candidate_models"))

    export_cfg = cfg.setdefault("export", {})
    output_dir = Path(export_cfg.get("output_dir", DEFAULT_MODELING_CONFIG["export"]["output_dir"]))
    if not output_dir.is_absolute():
        output_dir = (project_root / output_dir).resolve()
    export_cfg["output_dir"] = str(output_dir)

    data_source = cfg.setdefault("data_source", {})
    file_type = str(data_source.get("file_type", data_source.get("format", "auto")) or "auto").strip().lower()
    if file_type not in SUPPORTED_MODELING_FILE_TYPES:
        file_type = "auto"
    data_source["file_type"] = file_type
    data_source["format"] = file_type

    source_path = str(data_source.get("path", "") or "").strip()
    if source_path:
        source = Path(source_path)
        if not source.is_absolute():
            source = (project_root / source).resolve()
        data_source["path"] = str(source)
    return cfg


def load_modeling_config(
    *,
    base_config_path: str | Path | None = None,
    modeling_config_path: str | Path | None = None,
) -> Dict[str, Any]:
    """读取离线建模配置，并补齐默认值。"""
    project_root = _project_root()
    base_path = Path(base_config_path or (project_root / "configs" / "default_config.json")).resolve()
    base_cfg = load_config(base_path)
    modeling_cfg = _deep_merge(DEFAULT_MODELING_CONFIG, base_cfg.get("modeling", {}))

    modeling_path = Path(modeling_config_path or (project_root / "configs" / "modeling_offline.json")).resolve()
    if modeling_path.exists():
        payload = json.loads(modeling_path.read_text(encoding="utf-8-sig"))
        if isinstance(payload, Mapping):
            modeling_cfg = _deep_merge(modeling_cfg, payload.get("modeling", payload))

    modeling_cfg = _normalize_modeling_cfg(modeling_cfg, project_root=project_root)
    return {
        "project_root": str(project_root),
        "base_config_path": str(base_path),
        "modeling_config_path": str(modeling_path),
        "modeling": modeling_cfg,
        "description": "本功能默认不参与在线自动校准流程，仅用于离线建模分析与系数生成。",
    }


def validate_modeling_input_source(
    data_source: Mapping[str, Any],
    *,
    project_root: str | Path | None = None,
) -> Dict[str, Any]:
    """校验离线建模输入文件配置。"""
    root = Path(project_root or _project_root()).resolve()
    raw_path = str(data_source.get("path", "") or "").strip()
    if not raw_path:
        raise ValueError("离线建模输入文件未选择。")

    file_path = Path(raw_path)
    if not file_path.is_absolute():
        file_path = (root / file_path).resolve()
    if not file_path.exists():
        raise FileNotFoundError(f"离线建模输入文件不存在：{file_path}")

    file_type = str(data_source.get("file_type", data_source.get("format", "auto")) or "auto").strip().lower()
    if file_type not in SUPPORTED_MODELING_FILE_TYPES:
        raise ValueError(f"不支持的离线建模文件类型：{file_type}")

    suffix = file_path.suffix.lower().lstrip(".")
    detected_type = suffix if suffix in {"csv", "xlsx", "xls"} else ""
    if file_type == "auto":
        file_type = detected_type
    if file_type not in {"csv", "xlsx", "xls"}:
        raise ValueError(f"不支持的离线建模输入文件扩展名：{file_path.suffix}")

    if file_type in {"xlsx", "xls"}:
        sheet_name = data_source.get("sheet_name", 0)
    else:
        sheet_name = 0

    return {
        "path": file_path,
        "file_type": file_type,
        "sheet_name": sheet_name,
        "display_path": str(file_path),
    }


def save_modeling_config(
    *,
    modeling_config_path: str | Path,
    base_config_path: str | Path | None = None,
    path: str | Path,
    file_type: str = "auto",
    sheet_name: str | int = 0,
) -> Path:
    """将离线建模输入文件配置保存到 modeling_offline.json。"""
    loaded = load_modeling_config(
        base_config_path=base_config_path,
        modeling_config_path=modeling_config_path,
    )
    project_root = Path(loaded["project_root"]).resolve()
    modeling_path = Path(modeling_config_path).resolve()

    source_path = Path(str(path).strip())
    if not source_path.is_absolute():
        source_path = (project_root / source_path).resolve()
    else:
        source_path = source_path.resolve()
    if not source_path.exists():
        raise FileNotFoundError(f"离线建模输入文件不存在：{source_path}")

    normalized_type = str(file_type or "auto").strip().lower()
    if normalized_type not in SUPPORTED_MODELING_FILE_TYPES:
        raise ValueError(f"不支持的离线建模文件类型：{normalized_type}")
    if normalized_type == "auto":
        suffix = source_path.suffix.lower().lstrip(".")
        normalized_type = suffix if suffix in {"csv", "xlsx", "xls"} else "auto"
    if normalized_type not in {"csv", "xlsx", "xls"}:
        raise ValueError(f"不支持的离线建模输入文件扩展名：{source_path.suffix}")

    payload: Dict[str, Any] = {"modeling": copy.deepcopy(loaded["modeling"])}
    data_source: MutableMapping[str, Any] = payload["modeling"].setdefault("data_source", {})
    data_source["path"] = _to_storage_path(source_path, project_root=project_root)
    data_source["file_type"] = normalized_type
    data_source["format"] = normalized_type
    data_source["sheet_name"] = _normalize_sheet_name(sheet_name) if normalized_type in {"xlsx", "xls"} else 0
    data_source["enabled"] = True

    modeling_path.parent.mkdir(parents=True, exist_ok=True)
    modeling_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return modeling_path


def summarize_modeling_config(loaded_cfg: Mapping[str, Any]) -> str:
    """输出面向 UI / CLI 的中文摘要。"""
    modeling = dict(loaded_cfg.get("modeling", {}))
    data_source = dict(modeling.get("data_source", {}))
    outlier = dict(modeling.get("outlier_filter", {}))
    simplification = dict(modeling.get("simplification", {}))
    export_cfg = dict(modeling.get("export", {}))
    candidate_models = modeling.get("candidate_models", {})

    source_path = str(data_source.get("path", "") or "").strip()
    file_type = str(data_source.get("file_type", data_source.get("format", "auto")) or "auto").strip().lower()
    sheet_name = data_source.get("sheet_name", 0)
    path_status = "未选择"
    if source_path:
        try:
            validated = validate_modeling_input_source(data_source, project_root=loaded_cfg.get("project_root"))
        except Exception as exc:
            path_status = f"无效：{exc}"
        else:
            path_status = f"可用：{validated['display_path']}"

    lines = [
        "离线建模分析功能",
        "默认不参与当前自动校准运行流程",
        f"启用状态：{'开启' if modeling.get('enabled') else '关闭'}",
        f"输入文件：{source_path or '未配置'}",
        f"文件状态：{path_status}",
        f"文件类型：{file_type}",
        f"Excel Sheet：{sheet_name if file_type in {'xlsx', 'xls'} else '--'}",
        f"H2O交叉输入列：{', '.join(str(item) for item in data_source.get('humidity_keys', [])) or '--'}",
        f"候选模型数量：{len(candidate_models)}",
        f"拟合方法：{_display_fit_method(modeling.get('fit_method', 'least_squares'))}",
        f"异常点筛选：{'开启' if outlier.get('enabled') else '关闭'}",
        f"异常点方法：{_display_outlier_method(outlier.get('method', 'iqr'))}",
        f"系数简化：{'开启' if simplification.get('enabled') else '关闭'}",
        f"简化方法：{simplification.get('method', 'column_norm')}",
        f"自动有效数字：{'开启' if simplification.get('auto_digits') else '关闭'}",
        f"目标有效数字：{simplification.get('target_digits', 6)}",
        f"导出目录：{export_cfg.get('output_dir', '')}",
        "该配置仅用于离线建模分析，不影响在线自动校准流程。",
    ]
    return "\n".join(lines)


def find_latest_modeling_artifacts(output_dir: str | Path) -> Dict[str, Path | None]:
    """查找最新一轮离线建模结果文件。"""
    root = Path(output_dir)
    if not root.exists():
        return {"run_dir": None, "summary_txt": None, "summary_json": None, "comparison_json": None, "comparison_csv": None}
    run_dirs = sorted([item for item in root.glob("modeling_*") if item.is_dir()], key=lambda path: path.stat().st_mtime)
    if not run_dirs:
        return {"run_dir": None, "summary_txt": None, "summary_json": None, "comparison_json": None, "comparison_csv": None}
    run_dir = run_dirs[-1]

    def latest(pattern: str) -> Path | None:
        candidates = sorted(run_dir.glob(pattern), key=lambda path: path.stat().st_mtime)
        return candidates[-1] if candidates else None

    return {
        "run_dir": run_dir,
        "summary_txt": latest("summary.txt"),
        "summary_json": latest("summary.json"),
        "comparison_json": latest("*_model_compare_*.json"),
        "comparison_csv": latest("*_model_compare_*.csv"),
    }
