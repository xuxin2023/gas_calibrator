# src/gas_calibrator/v2/web/routes_api.py
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

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
def api_history(request: Request, limit: int = Query(default=50)):
    db = getattr(request.app.state, "database_manager", None)
    if db is None:
        return {"runs": [], "total": 0, "note": "数据库未连接"}
    try:
        from ..storage.queries import HistoryQueryService

        service = HistoryQueryService(db)
        raw = service.runs_by_time_range(limit=limit)
        runs: list[dict[str, Any]] = []
        for r in raw:
            runs.append({
                "run_id": str(r.get("id", "")),
                "start_time": r.get("start_time"),
                "end_time": r.get("end_time"),
                "status": r.get("status", ""),
                "total_points": r.get("total_points", 0),
                "successful_points": r.get("successful_points", 0),
                "failed_points": r.get("failed_points", 0),
                "route_mode": r.get("route_mode", ""),
                "run_mode": r.get("run_mode", ""),
                "operator": r.get("operator", "") or "",
                "software_version": r.get("software_version", ""),
            })
        return {"runs": runs, "total": len(runs)}
    except Exception as exc:
        return {"runs": [], "total": 0, "note": f"查询失败: {exc}"}


@router.get("/runs/{run_id}")
def api_run_detail(request: Request, run_id: str):
    db = getattr(request.app.state, "database_manager", None)
    if db is None:
        raise HTTPException(status_code=503, detail="数据库未连接")
    try:
        from ..storage.database import resolve_run_uuid
        from ..storage.queries import HistoryQueryService

        service = HistoryQueryService(db)
        run_uuid = resolve_run_uuid(run_id)

        all_runs = service.runs_by_time_range(limit=10000)
        run_data = None
        for r in all_runs:
            if str(r.get("id", "")) == str(run_uuid):
                run_data = r
                break
        if run_data is None:
            raise HTTPException(status_code=404, detail=f"运行记录未找到: {run_id}")

        points: list[dict[str, Any]] = []
        try:
            raw_samples = service.samples_by_point(run_id=str(run_uuid))
            point_map: dict[str, dict[str, Any]] = {}
            for s in raw_samples:
                pid = str(s.get("point_id", ""))
                if pid not in point_map:
                    point_map[pid] = {
                        "point_id": pid,
                        "sample_count": 0,
                        "analyzer_ids": [],
                        "co2_values": [],
                    }
                entry = point_map[pid]
                entry["sample_count"] += 1
                aid = s.get("analyzer_id", "")
                if aid and aid not in entry["analyzer_ids"]:
                    entry["analyzer_ids"].append(aid)
                co2 = s.get("co2_ppm")
                if co2 is not None:
                    entry["co2_values"].append(co2)
            for pid, info in point_map.items():
                vals = info["co2_values"]
                info["co2_avg"] = round(sum(vals) / len(vals), 2) if vals else None
                del info["co2_values"]
            points = sorted(point_map.values(), key=lambda x: x["point_id"])
        except Exception:
            pass

        return {
            "run_id": run_data.get("id"),
            "start_time": run_data.get("start_time"),
            "end_time": run_data.get("end_time"),
            "status": run_data.get("status"),
            "total_points": run_data.get("total_points", 0),
            "successful_points": run_data.get("successful_points", 0),
            "failed_points": run_data.get("failed_points", 0),
            "route_mode": run_data.get("route_mode"),
            "run_mode": run_data.get("run_mode"),
            "operator": run_data.get("operator") or "",
            "software_version": run_data.get("software_version"),
            "points": points,
            "stats": service.statistics(),
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"查询运行详情失败: {exc}") from exc


@router.get("/devices/live")
def api_devices_live(request: Request):
    svc = getattr(request.app.state, "calibration_service", None)
    if svc is None:
        return {"devices": [], "total": 0, "note": "校准服务未初始化"}
    try:
        mgr = getattr(svc, "device_manager", None)
        if mgr is None:
            return {"devices": [], "total": 0, "note": "设备管理器不可用"}
        health = mgr.health_check()
        result: list[dict[str, Any]] = []
        for name, info in mgr.list_device_info().items():
            entry = {
                "name": name,
                "device_type": info.device_type,
                "port": info.port,
                "enabled": info.enabled,
                "status": info.status.value,
                "healthy": health.get(name, False),
                "error_message": info.error_message,
                "current_value": None,
            }
            if info.enabled and info.device is not None:
                try:
                    dev = info.device
                    if hasattr(dev, "status"):
                        st = dev.status()
                        if isinstance(st, dict):
                            entry["current_value"] = {k: v for k, v in st.items() if k != "ok" and not k.startswith("_")}
                except Exception:
                    pass
            result.append(entry)
        return {"devices": result, "total": len(result)}
    except Exception as exc:
        return {"devices": [], "total": 0, "note": f"设备状态查询失败: {exc}"}


@router.get("/analyzers/live")
def api_analyzers_live(request: Request):
    svc = getattr(request.app.state, "calibration_service", None)
    if svc is None:
        return {"analyzers": [], "total": 0, "note": "校准服务未初始化"}
    try:
        orchestrator = getattr(svc, "orchestrator", None)
        if orchestrator is None:
            return {"analyzers": [], "total": 0, "note": "编排器不可用"}
        fleet = getattr(orchestrator, "analyzer_fleet_service", None)
        if fleet is None:
            return {"analyzers": [], "total": 0, "note": "分析仪服务不可用"}
        analyzers = fleet.all_gas_analyzers()
        result: list[dict[str, Any]] = []
        for label, analyzer, _cfg in analyzers:
            entry: dict[str, Any] = {
                "label": label,
                "online": label not in getattr(orchestrator.run_state.analyzers, "disabled", set()),
                "co2_ppm": None,
                "h2o_mmol": None,
                "co2_ratio_f": None,
                "h2o_ratio_f": None,
                "chamber_temp_c": None,
                "pressure_kpa": None,
                "co2_signal": None,
                "h2o_signal": None,
            }
            try:
                snapshot = fleet._read_raw_sensor_frame_with_retry(
                    analyzer, label=label, log_failures=False
                )
                frame = snapshot[0] if snapshot else {}
                if isinstance(frame, dict) and frame:
                    entry["co2_ppm"] = frame.get("co2_ppm") or frame.get("co2")
                    entry["h2o_mmol"] = frame.get("h2o_mmol") or frame.get("h2o")
                    entry["co2_ratio_f"] = frame.get("co2_ratio_f")
                    entry["h2o_ratio_f"] = frame.get("h2o_ratio_f")
                    entry["co2_signal"] = frame.get("co2_signal")
                    entry["h2o_signal"] = frame.get("h2o_signal")
                    entry["chamber_temp_c"] = frame.get("chamber_temp_c") or frame.get("temp_c")
                    entry["pressure_kpa"] = frame.get("pressure_kpa") or frame.get("pressure_hpa")
            except Exception:
                pass
            result.append(entry)
        return {"analyzers": result, "total": len(result)}
    except Exception as exc:
        return {"analyzers": [], "total": 0, "note": f"分析仪查询失败: {exc}"}


@router.post("/run/start")
def api_run_start(request: Request, payload: dict[str, Any] | None = None):
    svc = getattr(request.app.state, "calibration_service", None)
    if svc is None:
        raise HTTPException(status_code=503, detail="校准服务未初始化，请先 POST /api/run/init")
    if svc.is_running:
        raise HTTPException(status_code=409, detail="校准运行已在进行中")
    try:
        points_path = (payload or {}).get("points_path")
        svc.start(points_path=points_path)
        status = svc.get_status()
        return {
            "ok": True,
            "run_id": svc.run_id,
            "phase": status.phase.value if hasattr(status.phase, "value") else str(status.phase),
            "total_points": status.total_points,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"启动失败: {exc}") from exc


@router.post("/run/stop")
def api_run_stop(request: Request):
    svc = getattr(request.app.state, "calibration_service", None)
    if svc is None:
        raise HTTPException(status_code=503, detail="校准服务未初始化")
    try:
        svc.stop(wait=False)
        return {"ok": True, "message": "停止信号已发送"}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"停止失败: {exc}") from exc


@router.post("/run/pause")
def api_run_pause(request: Request):
    svc = getattr(request.app.state, "calibration_service", None)
    if svc is None:
        raise HTTPException(status_code=503, detail="校准服务未初始化")
    try:
        svc.pause()
        return {"ok": True, "message": "已暂停"}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"暂停失败: {exc}") from exc


@router.post("/run/resume")
def api_run_resume(request: Request):
    svc = getattr(request.app.state, "calibration_service", None)
    if svc is None:
        raise HTTPException(status_code=503, detail="校准服务未初始化")
    try:
        svc.resume()
        return {"ok": True, "message": "已恢复"}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"恢复失败: {exc}") from exc


@router.get("/run/status")
def api_run_status(request: Request):
    svc = getattr(request.app.state, "calibration_service", None)
    if svc is None:
        return {
            "running": False,
            "phase": "idle",
            "current_point": None,
            "total_points": 0,
            "completed_points": 0,
            "progress_pct": 0.0,
            "message": "校准服务未初始化",
        }
    try:
        status = svc.get_status()
        return {
            "running": svc.is_running,
            "phase": status.phase.value if hasattr(status.phase, "value") else str(status.phase),
            "current_point": status.current_point,
            "total_points": status.total_points,
            "completed_points": status.completed_points,
            "progress_pct": round(status.progress * 100, 1) if status.progress else 0.0,
            "message": status.message or "",
        }
    except Exception as exc:
        return {
            "running": False,
            "phase": "error",
            "current_point": None,
            "total_points": 0,
            "completed_points": 0,
            "progress_pct": 0.0,
            "message": f"状态查询失败: {exc}",
        }


@router.post("/run/init")
def api_run_init(request: Request, payload: dict[str, Any]):
    config_path = payload.get("config_path", "")
    points_path = payload.get("points_path", "")
    if not config_path:
        raise HTTPException(status_code=400, detail="缺少 config_path")

    try:
        from ..config import AppConfig

        cfg = AppConfig.from_json(config_path)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"配置加载失败: {exc}") from exc

    try:
        from ..core.calibration_service import CalibrationService

        svc = CalibrationService(config=cfg)
        if points_path:
            svc.load_points(points_path)
        request.app.state.calibration_service = svc
        return {"ok": True, "run_id": svc.run_id, "points_loaded": len(svc._points)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"校准服务初始化失败: {exc}") from exc


@router.get("/simulation/snapshot")
def api_simulation_snapshot(request: Request):
    tele = request.app.state.telemetry_state
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "pressure_hpa": tele.get("pressure_hpa"),
        "temperature_c": tele.get("temperature_c"),
        "humidity_pct": tele.get("humidity_pct"),
        "dewpoint_c": tele.get("dewpoint_c"),
        "co2_ppm": tele.get("co2_ppm"),
        "phase": tele.get("phase", "idle"),
        "point": tele.get("point_index", 0),
        "progress_pct": tele.get("progress_pct", 0.0),
    }
