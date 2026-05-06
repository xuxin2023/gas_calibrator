# src/gas_calibrator/v2/web/routes_api.py
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/api")

V2_CONFIG_DIR = Path(__file__).resolve().parents[1] / "configs" / "validation"
DEFAULT_CONFIG = "run001_h2o_only_1_point_no_write_real_machine.json"

PARAM_GROUPS: list[dict[str, Any]] = [
    {
        "id": "humidity_generator",
        "label": "湿度控制",
        "icon": "💧",
        "json_path": ["workflow", "stability", "humidity_generator"],
        "params": [
            {"key": "temp_tol_c", "label": "温度容差", "unit": "°C", "type": "float", "v1_default": 1.0},
            {"key": "rh_tol_pct", "label": "相对湿度容差", "unit": "%", "type": "float", "v1_default": 1.6},
            {"key": "rh_stable_window_s", "label": "湿度判稳窗口", "unit": "秒", "type": "float", "v1_default": 60.0},
            {"key": "rh_stable_span_pct", "label": "湿度判稳跨度", "unit": "%", "type": "float", "v1_default": 0.6},
            {"key": "timeout_s", "label": "超时时间", "unit": "秒", "type": "float", "v1_default": 1800.0},
            {"key": "poll_s", "label": "轮询间隔", "unit": "秒", "type": "float", "v1_default": 1.0},
        ],
    },
    {
        "id": "dewpoint",
        "label": "露点判稳",
        "icon": "🌡️",
        "json_path": ["workflow", "stability", "dewpoint"],
        "params": [
            {"key": "window_s", "label": "判稳窗口", "unit": "秒", "type": "float", "v1_default": 60.0},
            {"key": "timeout_s", "label": "超时时间", "unit": "秒", "type": "float", "v1_default": 1800.0},
            {"key": "poll_s", "label": "轮询间隔", "unit": "秒", "type": "float", "v1_default": 1.0},
            {"key": "temp_match_tol_c", "label": "温度匹配容差", "unit": "°C", "type": "float", "v1_default": 0.3},
            {"key": "rh_match_tol_pct", "label": "湿度匹配容差", "unit": "%", "type": "float", "v1_default": 4.0},
            {"key": "stability_tol_c", "label": "稳定性容差", "unit": "°C", "type": "float", "v1_default": 0.05},
        ],
    },
    {
        "id": "pressure",
        "label": "压力控制",
        "icon": "⚡",
        "json_path": ["workflow", "pressure"],
        "params": [
            {"key": "pressurize_high_hpa", "label": "增压上限", "unit": "hPa", "type": "float", "v1_default": 1100.0},
            {"key": "pressurize_timeout_s", "label": "增压超时", "unit": "秒", "type": "float", "v1_default": 120.0},
            {"key": "stabilize_timeout_s", "label": "稳定超时", "unit": "秒", "type": "float", "v1_default": 180.0},
            {"key": "restabilize_retries", "label": "重稳定重试", "unit": "次", "type": "int", "v1_default": 0},
            {"key": "post_stable_sample_delay_s", "label": "稳定后采样延迟", "unit": "秒", "type": "float", "v1_default": 0.0},
            {"key": "pressurize_wait_after_vent_off_s", "label": "排气后等待", "unit": "秒", "type": "float", "v1_default": 3.0},
        ],
    },
    {
        "id": "temperature",
        "label": "温度控制",
        "icon": "🔥",
        "json_path": ["workflow", "stability", "temperature"],
        "params": [
            {"key": "tol", "label": "温度容差", "unit": "°C", "type": "float", "v1_default": 0.2},
            {"key": "window_s", "label": "判稳窗口", "unit": "秒", "type": "float", "v1_default": 40.0},
            {"key": "timeout_s", "label": "超时时间", "unit": "秒", "type": "float", "v1_default": 1800.0},
        ],
    },
    {
        "id": "sampling",
        "label": "采样",
        "icon": "📊",
        "json_path": ["workflow", "sampling"],
        "params": [
            {"key": "count", "label": "采样次数", "unit": "次", "type": "int", "v1_default": 10},
            {"key": "stable_count", "label": "稳定采样次数", "unit": "次", "type": "int", "v1_default": 10},
            {"key": "interval_s", "label": "采样间隔", "unit": "秒", "type": "float", "v1_default": 1.0},
            {"key": "co2_interval_s", "label": "CO2 采样间隔", "unit": "秒", "type": "float", "v1_default": 2.0},
            {"key": "h2o_interval_s", "label": "H2O 采样间隔", "unit": "秒", "type": "float", "v1_default": 2.0},
        ],
    },
    {
        "id": "vent_keepalive",
        "label": "排气保持",
        "icon": "🔁",
        "json_path": ["workflow", "pressure"],
        "params": [
            {"key": "h2o_vent_keepalive_interval_s", "label": "排气保持间隔", "unit": "秒", "type": "float", "v1_default": 1.0},
            {"key": "h2o_pre_seal_natural_rise_wait_s", "label": "密封前回升等待", "unit": "秒", "type": "float", "v1_default": 1.5},
        ],
    },
]


def _resolve_config(filename: str | None = None) -> Path:
    name = filename or DEFAULT_CONFIG
    candidate = V2_CONFIG_DIR / name
    if candidate.exists():
        return candidate
    alt = V2_CONFIG_DIR / f"{name}.json"
    if alt.exists():
        return alt
    raise HTTPException(status_code=404, detail=f"配置文件未找到: {name}")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _save_json(path: Path, data: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _deep_get(data: dict[str, Any], path: list[str]) -> Any:
    cur: Any = data
    for key in path:
        if isinstance(cur, dict):
            cur = cur.get(key, {})
        else:
            return {}
    return cur if isinstance(cur, dict) else {}


def _deep_set(data: dict[str, Any], path: list[str], value: Any) -> None:
    cur = data
    for key in path[:-1]:
        if key not in cur or not isinstance(cur[key], dict):
            cur[key] = {}
        cur = cur[key]
    cur[path[-1]] = value


def _list_config_files() -> list[dict[str, Any]]:
    if not V2_CONFIG_DIR.exists():
        return []
    return sorted(
        [{"name": p.name, "path": str(p)} for p in V2_CONFIG_DIR.glob("*.json")],
        key=lambda x: x["name"],
    )


def _coerce(raw: str, type_name: str) -> Any:
    stripped = raw.strip()
    if not stripped:
        raise ValueError("不能为空")
    if type_name == "int":
        return int(float(stripped))
    if type_name == "float":
        return float(stripped)
    return stripped


@router.get("/overview")
def api_overview(filename: str = Query(default=DEFAULT_CONFIG)):
    config_path = _resolve_config(filename)
    raw = _load_json(config_path)
    governance = raw.get("run001_h2o_1_point", {})
    devices = raw.get("devices", {})
    enabled_devices = {k: v for k, v in devices.items() if isinstance(v, dict) and v.get("enabled")}
    analyzers = [a for a in devices.get("gas_analyzers", []) if isinstance(a, dict) and a.get("enabled")]
    workflow = raw.get("workflow", {})
    return {
        "config_file": config_path.name,
        "scenario": governance.get("scenario", ""),
        "mode": governance.get("mode", ""),
        "device_count": len(enabled_devices),
        "device_names": list(enabled_devices.keys()),
        "analyzer_count": len(analyzers),
        "run_mode": workflow.get("run_mode", "auto_calibration"),
        "route_mode": workflow.get("route_mode", "h2o_then_co2"),
        "selected_temps_c": workflow.get("selected_temps_c", []),
    }


@router.get("/configs")
def api_list_configs():
    return {"configs": _list_config_files()}


@router.get("/configs/{filename:path}")
def api_get_config(filename: str):
    config_path = _resolve_config(filename)
    return _load_json(config_path)


@router.put("/configs/{filename:path}")
def api_save_config(filename: str, payload: dict[str, Any]):
    config_path = _resolve_config(filename)
    if not payload.get("workflow") and not payload.get("devices"):
        raise HTTPException(status_code=400, detail="JSON 缺少 workflow 或 devices 字段")
    _save_json(config_path, payload)
    return {"ok": True, "file": config_path.name}


@router.get("/status")
def api_status(filename: str = Query(default=DEFAULT_CONFIG)):
    config_path = _resolve_config(filename)
    raw = _load_json(config_path)
    devices = raw.get("devices", {})
    result: list[dict[str, Any]] = []
    for name, cfg in devices.items():
        if isinstance(cfg, dict):
            result.append(
                {
                    "name": name,
                    "port": cfg.get("port", "—"),
                    "baud": cfg.get("baud", 9600),
                    "enabled": cfg.get("enabled", False),
                    "description": cfg.get("description", ""),
                }
            )
    return {"devices": result, "total": len(result)}


@router.get("/params")
def api_get_params(filename: str = Query(default=DEFAULT_CONFIG)):
    config_path = _resolve_config(filename)
    raw = _load_json(config_path)
    groups: list[dict[str, Any]] = []
    for group in PARAM_GROUPS:
        section = _deep_get(raw, group["json_path"])
        params: list[dict[str, Any]] = []
        for pdef in group["params"]:
            params.append(
                {
                    **pdef,
                    "current_value": section.get(pdef["key"], pdef["v1_default"]),
                }
            )
        groups.append({**group, "params": params})
    return {"groups": groups, "config_file": config_path.name}


@router.put("/params")
def api_save_params(
    filename: str = Query(default=DEFAULT_CONFIG),
    payload: dict[str, Any] | None = None,
):
    if not payload:
        raise HTTPException(status_code=400, detail="请求体为空")
    config_path = _resolve_config(filename)
    raw = _load_json(config_path)
    errors: list[str] = []
    for group_id, group_values in payload.items():
        group_def = next((g for g in PARAM_GROUPS if g["id"] == group_id), None)
        if not group_def:
            continue
        section = _deep_get(raw, group_def["json_path"])
        for pdef in group_def["params"]:
            if pdef["key"] not in group_values:
                continue
            try:
                val = group_values[pdef["key"]]
                coerced = _coerce(str(val), pdef["type"])
                section[pdef["key"]] = coerced
            except (ValueError, TypeError):
                errors.append(f"{group_def['label']}/{pdef['label']}")
            except Exception:
                errors.append(f"{group_def['label']}/{pdef['label']}: 无效值")
        _deep_set(raw, group_def["json_path"], section)
    if errors:
        raise HTTPException(status_code=422, detail=f"验证失败: {', '.join(errors[:5])}")
    _save_json(config_path, raw)
    return {"ok": True, "file": config_path.name}


@router.get("/history")
def api_history(limit: int = Query(default=50)):
    return {
        "runs": [],
        "total": 0,
        "note": "历史记录模块待数据库集成后可用",
    }


@router.get("/simulation/snapshot")
def api_simulation_snapshot():
    import random

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "pressure_hpa": round(random.uniform(990, 1010), 1),
        "temperature_c": round(random.uniform(22.5, 23.5), 1),
        "humidity_pct": round(random.uniform(45, 55), 1),
        "dewpoint_c": round(random.uniform(10, 14), 1),
        "co2_ppm": round(random.uniform(400, 420), 1),
        "phase": "idle",
        "point": 0,
        "progress_pct": 0.0,
    }
