"""Offline V1.5 post-run coefficient-execution closure.

This module builds a reviewer-facing and machine-readable execution plan after
pressure, temperature, CO2, and H2O evidence has been collected. It never opens
COM ports, controls PACE/valves/routes, or writes SENCO values. Its purpose is
to make the coefficient workflow deterministic so later real-device runners can
execute only reviewed steps in the right physical order.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence


SCHEMA = "v1_5_post_run_coefficient_executor_v1"


@dataclass(frozen=True)
class ExecutorStage:
    stage_id: str
    title: str
    status: str
    reason: str
    required_roles: tuple[str, ...]
    artifact_count: int
    physical_meaning: str
    next_action: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DeviceClosure:
    device_id: str
    identity_status: str
    pressure_status: str
    temperature_status: str
    device_quality_status: str
    fit_input_quality_status: str
    co2_status: str
    h2o_status: str
    h2o_dry_anchor_bridge_status: str
    output_trim_status: str
    overall_status: str
    blockers: tuple[str, ...]
    next_action: str
    blocker_summary_zh: str = ""
    next_action_zh: str = ""

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClosureGap:
    scope: str
    item: str
    status: str
    reason: str
    next_action: str
    physical_meaning: str
    reason_zh: str = ""
    next_action_zh: str = ""

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _load_json(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {}
    source = Path(path)
    if not source.exists() or not source.is_file():
        return {}
    payload = json.loads(source.read_text(encoding="utf-8-sig"))
    return payload if isinstance(payload, dict) else {}


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists() or not path.is_file():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    keys: list[str] = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in keys})


def _normalize_device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.startswith("GA"):
        text = text[2:]
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _reason_to_zh(reason: Any) -> str:
    """Translate common internal blocker/status tokens into reviewer Chinese."""

    text = str(reason or "").strip()
    if not text:
        return "未给出具体原因，需要回到对应证据表核对。"
    lower = text.lower()
    mappings = [
        (
            ("missing_initial_identity_or_getco_snapshot", "identity_initialization"),
            "缺少初始化身份绑定或 GETCO1-9 旧系数快照，不能证明写入对象和旧值可追溯。",
        ),
        (
            ("needs_senco9_review_or_calibration", "senco9_write_not_verified", "senco9_write_not_applied"),
            "压力输入量 P 尚未完成 SENCO9 评审、写入回读或复验闭环；压力问题应先独立处理，不能混入 CO2/H2O 拟合。",
        ),
        (
            ("needs_senco78_review_or_temperature_gate", "temperature"),
            "温度输入量 T 尚未完成评审或修正；多温度组分拟合前必须确认腔体/壳体温度可信。",
        ),
        (
            ("needs_s5_s6_linear_trim_review", "output_layer_trim", "s5", "s6"),
            "S5/S6 输出层线性修正还未评审；它们应在 S1/S3、S2/S4 主链路之后单独处理。",
        ),
        (
            ("h2o_blocked:model_matrix_rank_deficient", "model_matrix_rank_deficient"),
            "H2O 拟合矩阵秩不足，通常意味着有效湿度点、干气锚点、温度覆盖或被剔除样本不足。",
        ),
        (
            ("fail_senco24_raw_state_transfer", "h2o_state_transfer_failed"),
            "H2O S2/S4 主链路状态转移失败，应检查 H2O ratio、露点、dry/wet ppmv 与 S6 是否误吸收主链路误差。",
        ),
        (
            ("ratio_stable_but_curve_inconsistent", "factory_signal_health", "device_rejected_or_unqualified"),
            "ratio 虽可能稳定，但曲线或光学健康不符合标气响应；应检查 ref_signal、CO2/H2O signal、SETCO2、SETPOW 和状态寄存器。",
        ),
        (
            ("candidate_missing", "candidate_not_ready", "formal_candidate_review_not_ready"),
            "候选系数证据不足，需补齐稳定样本、模型选择和残差评审后再进入写入评审。",
        ),
        (
            ("fit_samples", "sample_count"),
            "有效样本数不足，需回到采样窗口和 QC 分级，确认是否能补证据或明确拒绝原因。",
        ),
        (
            ("archive", "database", "report", "certificate"),
            "归档、数据库、报告或证书证据尚未闭环；需要补齐 hash、路径、数据库索引或报告重建证据。",
        ),
    ]
    for needles, zh in mappings:
        if any(needle in lower for needle in needles):
            return zh
    if "missing_roles=" in lower:
        return "必要角色证据缺失，需要先补齐对应 artifact，再进入下一阶段。"
    if "invalid_roles=" in lower:
        return "发现对应 artifact，但状态无效或失败；不能作为正式初始化/评审证据。"
    if "not_attempted" in lower:
        return "该阶段尚未执行，不影响前序离线评审，但不能作为最终发布证据。"
    return f"需要人工复核内部原因：{text}"


def _next_action_to_zh(action: Any, blockers: Sequence[Any] | None = None) -> str:
    text = str(action or "").strip()
    lower = text.lower()
    blocker_text = ";".join(str(item) for item in blockers or ())
    if "export_controlled_write_package" in lower:
        return "进入受控写入评审：先核对设备 ID 和旧系数快照，再由写入工具逐台执行并回读。"
    if "repair_missing_evidence" in lower or "exclude_bad_device" in lower:
        return "先修复缺失证据；若确认设备自身异常，应只阻断该设备并写明拒绝原因，不拖死其它设备。"
    if "pressure" in lower or "senco9" in lower or "senco9" in blocker_text.lower():
        return "先做压力通道 SENCO9 评审/校准/复验，合格后再释放 CO2/H2O 写入。"
    if "temperature" in lower or "senco78" in lower or "temperature" in blocker_text.lower():
        return "先做温度通道评审或 SENCO7/SENCO8 修复，避免温度错误被组分系数吸收。"
    if "candidate" in lower or "model" in lower:
        return "补齐候选系数、模型选择、残差和被拒绝样本理由，再重新运行采集后闭环。"
    if "archive" in lower or "database" in lower or "report" in lower:
        return "补齐归档索引、数据库 sidecar、报告和证书证据，并重新刷新 evidence status。"
    if text:
        return f"按内部动作执行并补充审计记录：{text}"
    return "回到对应阶段证据表，确认缺口后重新生成离线闭环。"


def _blocker_summary_zh(blockers: Sequence[Any]) -> str:
    unique = [str(item) for item in dict.fromkeys(str(item) for item in blockers if str(item))]
    if not unique:
        return "无阻断项，设备可进入受控写入评审。"
    return "；".join(_reason_to_zh(item) for item in unique)


def _existing_latest(root: Path, patterns: Sequence[str]) -> Path | None:
    matches: list[Path] = []
    for pattern in patterns:
        matches.extend(path for path in _safe_rglob(root, pattern) if path.is_file())
    if not matches:
        return None
    return max(matches, key=lambda item: item.stat().st_mtime)


def _existing_latest_matching(root: Path, pattern: str, predicate: Callable[[Path], bool]) -> Path | None:
    matches = [path for path in _safe_rglob(root, pattern) if path.is_file() and predicate(path)]
    if not matches:
        return None
    return max(matches, key=lambda item: item.stat().st_mtime)


def _safe_rglob(root: Path, pattern: str) -> tuple[Path, ...]:
    try:
        return tuple(root.rglob(pattern))
    except OSError:
        return ()


def _existing_dirs_with(root: Path, filename: str) -> tuple[Path, ...]:
    if not root.exists():
        return ()
    dirs = {path.parent.resolve() for path in _safe_rglob(root, filename) if path.is_file()}
    return tuple(sorted(dirs, key=lambda item: str(item).lower()))


def _existing_dirs_with_any(root: Path, filenames: Sequence[str]) -> tuple[Path, ...]:
    if not root.exists():
        return ()
    dirs: set[Path] = set()
    for filename in filenames:
        dirs.update(path.parent.resolve() for path in _safe_rglob(root, filename) if path.is_file())
    return tuple(sorted(dirs, key=lambda item: str(item).lower()))


def _is_reverification_review_file(path: Path) -> bool:
    name = path.name.lower()
    if "process_record" in name:
        return False
    return "reverif" in name or "postwrite" in name or "post_write" in name


def _discover_artifacts(run_dir: Path, explicit: Mapping[str, str | Path | None]) -> dict[str, tuple[Path, ...]]:
    def one(key: str, patterns: Sequence[str]) -> tuple[Path, ...]:
        value = explicit.get(key)
        if value:
            path = Path(value).resolve()
            if path.exists():
                return (path,)
        latest = _existing_latest(run_dir, patterns)
        return (latest.resolve(),) if latest else ()

    def one_reverification(key: str, patterns: Sequence[str]) -> tuple[Path, ...]:
        value = explicit.get(key)
        if value:
            path = Path(value).resolve()
            if path.exists() and path.is_file() and _is_reverification_review_file(path):
                return (path,)
            return ()
        matches = [
            path.resolve()
            for pattern in patterns
            for path in _safe_rglob(run_dir, pattern)
            if path.is_file() and _is_reverification_review_file(path)
        ]
        if not matches:
            return ()
        return (max(matches, key=lambda item: item.stat().st_mtime),)

    def one_initialization() -> tuple[Path, ...]:
        value = explicit.get("initialization_readiness_json")
        if value:
            path = Path(value).resolve()
            if path.exists():
                return (path,)
        readiness = _existing_latest(
            run_dir,
            ("initialization_readiness.json", "v1_5_initialization_readiness.json", "*initialization_archive_confirmation*.json"),
        )
        if readiness:
            return (readiness.resolve(),)
        prewrite_snapshot = _existing_latest_matching(
            run_dir,
            "getco_component_snapshot_identity.csv",
            lambda path: "before" in str(path).lower() and "after_main_senco_write" not in str(path).lower(),
        )
        return (prewrite_snapshot.resolve(),) if prewrite_snapshot else ()

    def one_gate_dir(key: str, filenames: Sequence[str]) -> tuple[Path, ...]:
        value = explicit.get(key)
        if value:
            path = Path(value).resolve()
            if path.is_file() and path.name in filenames:
                return (path.parent,)
            if path.is_dir() and any((path / filename).is_file() for filename in filenames):
                return (path,)
            return ()
        return _existing_dirs_with_any(run_dir, filenames)

    pressure_review_paths = list(
        one(
            "pressure_review_json",
            ("*pressure*review*.json", "*senco9*review*.json", "pressure_channel_completion_summary.csv"),
        )
    )
    for key in ("pressure_completion_summary_csv", "pressure_device_readiness_csv"):
        value = explicit.get(key)
        if not value:
            continue
        path = Path(value).resolve()
        if path.exists() and path.is_file() and path not in pressure_review_paths:
            pressure_review_paths.append(path)

    artifacts: dict[str, tuple[Path, ...]] = {
        "plan_snapshot": one("plan_json", ("formal_plan_snapshot.json", "v1_5_full_flow_plan.json")),
        "pressure_reference": one("pressure_reference_json", ("pressure_reference.json", "com22_pressure_reference.json")),
        "initialization_readiness": one_initialization(),
        "run_evidence_status": one("run_evidence_status_json", ("v1_5_run_evidence_status.json",)),
        "pressure_review": tuple(pressure_review_paths),
        "temperature_review": one(
            "temperature_review_csv",
            (
                "temperature_current_point_review.csv",
                "*temperature*senco78*review*.csv",
                "temperature_channel_review_summary.json",
                "temperature_channel_summary*.csv",
                "temperature_compensation_coefficients.csv",
            ),
        ),
        "main_precheck_pack": one("main_precheck_meta_json", ("main_senco_write_precheck_meta.json",)),
        "post_write_reverification": one_reverification(
            "post_write_reverification_json",
            ("post_write_reverification_review.json", "*postwrite*reverify*review*.json", "*post_write*reverification*summary*.json"),
        ),
        "archive_closure": one("archive_closure_json", ("v1_5_formal_archive_closure_index.json",)),
    }
    fit_input_quality_paths = list(
        one(
            "fit_input_quality_summary_csv",
            ("v1_5_fit_input_quality_summary.csv", "fit_input_quality_summary.csv"),
        )
    )
    explicit_fit_devices = explicit.get("fit_input_quality_devices_csv")
    if explicit_fit_devices:
        path = Path(explicit_fit_devices).resolve()
        if path.exists() and path.is_file():
            fit_input_quality_paths.append(path)
    else:
        devices_path = _existing_latest(
            run_dir,
            ("v1_5_fit_input_quality_devices.csv", "fit_input_quality_devices.csv"),
        )
        if devices_path:
            fit_input_quality_paths.append(devices_path.resolve())
    artifacts["fit_input_quality"] = tuple(dict.fromkeys(fit_input_quality_paths))
    candidate_dirs = _existing_dirs_with_any(
        run_dir,
        (
            "candidate_fit_residuals.csv",
            "candidate_policy_summary.csv",
            "co2_formal_senco13_ratio_only_summary.csv",
            "co2_recommended_writeable_summary.csv",
            "h2o_senco24_device_policy.csv",
            "h2o_senco24_review_summary.csv",
        ),
    )
    model_dirs = _existing_dirs_with_any(
        run_dir,
        (
            "model_selection_summary.csv",
            "co2_recommended_writeable_summary.csv",
            "co2_best_candidate_summary_all_models.csv",
            "co2_formal_senco13_ratio_only_summary.csv",
            "h2o_senco24_device_policy.csv",
        ),
    )
    trim_dirs = _existing_dirs_with_any(
        run_dir,
        (
            "linear_trim_review.csv",
            "co2_senco13_postwrite_s5_linear_trim_candidates.csv",
            "h2o_senco6_linear_trim_candidate_summary.csv",
        ),
    )
    write_events = tuple(
        sorted(
            (
                path.resolve()
                for pattern in ("*write_events.csv", "*senco*_write_events.csv")
                for path in _safe_rglob(run_dir, pattern)
                if path.is_file()
            ),
            key=lambda item: str(item).lower(),
        )
    )
    artifacts["candidate_dirs"] = candidate_dirs
    artifacts["model_selection_dirs"] = model_dirs
    artifacts["linear_trim_dirs"] = trim_dirs
    artifacts["h2o_dry_anchor_bridge"] = _existing_dirs_with_any(
        run_dir,
        (
            "h2o_dry_anchor_bridge_manifest.json",
            "h2o_dry_anchor_bridge_device_summary.csv",
            "h2o_dry_anchor_bridge_predictions.csv",
            "h2o_dry_anchor_bridge_strategy_comparison.csv",
        ),
    )
    artifacts["co2_source_state_gate"] = one_gate_dir(
        "co2_source_state_gate",
        (
            "co2_s13_source_state_run_summary.csv",
            "co2_s13_source_state_root_cause_decision.csv",
        ),
    )
    artifacts["write_events"] = write_events
    artifacts["database_sidecar"] = tuple(
        sorted(
            (
                path.resolve()
                for path in _safe_rglob(run_dir, "*database_sidecar.json")
                if path.is_file()
            ),
            key=lambda item: str(item).lower(),
        )
    )
    explicit_quality = explicit.get("device_quality_review_csv")
    if explicit_quality:
        path = Path(explicit_quality).resolve()
        artifacts["device_quality_review"] = (path,) if path.exists() and path.is_file() else ()
    else:
        artifacts["device_quality_review"] = tuple(
            sorted(
                (
                    path.resolve()
                    for pattern in ("*root_cause_flags.csv", "*device_quality*review*.csv")
                    for path in _safe_rglob(run_dir, pattern)
                    if path.is_file()
                ),
                key=lambda item: str(item).lower(),
            )
        )
    artifacts["formal_package_dirs"] = _existing_dirs_with_any(
        run_dir,
        (
            "candidate_coefficient_review.csv",
            "formal_calibration_package_meta.json",
            "package_summary.csv",
        ),
    )
    return artifacts


def _role_count(artifacts: Mapping[str, Sequence[Path]], roles: Iterable[str]) -> int:
    return sum(len(tuple(artifacts.get(role, ()))) for role in roles)


def _status_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _truthy_text(value: Any) -> bool:
    return _status_text(value) in {"true", "1", "yes", "y", "ok", "pass", "passed", "ready"}


def _csv_group_set(value: Any) -> set[str]:
    text = str(value or "").strip()
    if not text:
        return set()
    return {item.strip() for item in text.replace(";", ",").split(",") if item.strip()}


def _initialization_getco_snapshot_csv_ready(rows: Sequence[Mapping[str, str]]) -> bool | None:
    marker_keys = {
        "identity_verified",
        "requested_groups",
        "found_groups",
        "all_groups_found",
        "writes_senco",
        "writes_device_id",
        "controls_water_or_gas_routes",
        "controls_pace",
    }
    if not any(marker_keys & set(row) for row in rows):
        return None

    required_groups = {str(index) for index in range(1, 10)}
    unsafe_action_keys = (
        "writes_senco",
        "writes_device_id",
        "controls_water_or_gas_routes",
        "controls_pace",
    )
    for row in rows:
        if str(row.get("error") or "").strip():
            return False
        if not _truthy_text(row.get("identity_verified")):
            return False
        if any(_truthy_text(row.get(key)) for key in unsafe_action_keys):
            return False

        if "all_groups_found" in row and row.get("all_groups_found") not in (None, ""):
            if not _truthy_text(row.get("all_groups_found")):
                return False
            continue

        requested_groups = _csv_group_set(row.get("requested_groups"))
        found_groups = _csv_group_set(row.get("found_groups"))
        if not required_groups.issubset(requested_groups) or not required_groups.issubset(found_groups):
            return False
    return True


def _float_or_none(value: Any) -> float | None:
    try:
        return float(str(value or "").strip())
    except (TypeError, ValueError):
        return None


def _json_status_values(payload: Mapping[str, Any]) -> list[str]:
    keys = (
        "overall_status",
        "status",
        "readiness_status",
        "review_status",
        "package_status",
        "archive_status",
        "import_status",
        "db_import_status",
        "result",
    )
    return [_status_text(payload.get(key)) for key in keys if payload.get(key) not in (None, "")]


def _json_artifact_ready(role: str, payload: Mapping[str, Any]) -> bool:
    statuses = set(_json_status_values(payload))
    ready_statuses = {
        "ready",
        "pass",
        "passed",
        "complete",
        "completed",
        "archived",
        "imported",
        "written",
        "written_readback_verified",
        "success",
        "ok",
        "initialization_ready_with_warnings",
        "ready_with_warnings",
    }
    blocked_statuses = {
        "blocked",
        "failed",
        "fail",
        "error",
        "incomplete",
        "partial",
        "not_ready",
        "not ready",
    }
    if statuses & blocked_statuses:
        return False
    if statuses & ready_statuses:
        return True
    if role == "database_sidecar":
        tables = payload.get("database_target_tables")
        suggested_rows = payload.get("suggested_rows")
        return bool(tables) and isinstance(suggested_rows, list) and bool(suggested_rows)
    if role == "pressure_review":
        return bool(payload.get("pressure_review") or payload.get("senco9_review"))
    return not statuses


def _csv_artifact_ready(role: str, path: Path) -> bool:
    rows = _read_csv(path)
    if not rows:
        return False
    if role == "fit_input_quality":
        if "summary" not in path.name.lower():
            return False
        row = rows[0]
        run_status = _status_text(row.get("run_status") or row.get("overall_status") or row.get("status"))
        continuity_status = _status_text(row.get("fit_input_continuity_gate_status"))
        unsafe = any(
            _truthy_text(row.get(key))
            for key in ("opens_com_ports", "controls_water_or_gas_routes", "writes_coefficients")
        )
        return run_status == "pass" and continuity_status == "pass" and not unsafe
    if role == "initialization_readiness":
        snapshot_ready = _initialization_getco_snapshot_csv_ready(rows)
        if snapshot_ready is not None:
            return snapshot_ready
    status_keys = ("overall_status", "status", "readiness_status", "review_status", "candidate_status")
    ready_markers = {
        "ready",
        "pass",
        "passed",
        "complete",
        "completed",
        "review_ready",
        "written_readback_verified",
        "already_neutral",
        "ok",
        "initialization_ready_with_warnings",
        "ready_with_warnings",
    }
    blocked_markers = {"blocked", "failed", "fail", "error", "incomplete", "not_ready"}
    saw_status = False
    for row in rows:
        statuses = {_status_text(row.get(key)) for key in status_keys if row.get(key) not in (None, "")}
        statuses.discard("")
        if statuses:
            saw_status = True
        if statuses & blocked_markers:
            return False
        if statuses & ready_markers:
            return True
    return not saw_status


def _artifact_ready_for_role(role: str, path: Path) -> bool:
    if path.is_dir():
        return True
    suffix = path.suffix.lower()
    if suffix == ".json":
        return _json_artifact_ready(role, _load_json(path))
    if suffix == ".csv":
        return _csv_artifact_ready(role, path)
    return path.exists()


def _role_ready(artifacts: Mapping[str, Sequence[Path]], role: str) -> bool:
    return any(_artifact_ready_for_role(role, path) for path in artifacts.get(role, ()))


def _stage_status(
    artifacts: Mapping[str, Sequence[Path]],
    *,
    stage_id: str,
    title: str,
    required_roles: Sequence[str],
    physical_meaning: str,
    next_action: str,
    optional: bool = False,
) -> ExecutorStage:
    present = [role for role in required_roles if _role_ready(artifacts, role)]
    missing = [role for role in required_roles if not artifacts.get(role)]
    invalid = [
        role
        for role in required_roles
        if artifacts.get(role) and not _role_ready(artifacts, role)
    ]
    if not missing:
        if invalid:
            status = "partial"
            reason = "invalid_roles=" + ",".join(invalid)
        else:
            status = "ready"
            reason = "required_evidence_present"
    elif present:
        status = "partial"
        reason = "missing_roles=" + ",".join(missing)
    elif optional:
        status = "not_attempted"
        reason = "optional_stage_not_attempted"
    else:
        status = "blocked"
        reason = "missing_roles=" + ",".join(missing)
    return ExecutorStage(
        stage_id=stage_id,
        title=title,
        status=status,
        reason=reason,
        required_roles=tuple(required_roles),
        artifact_count=_role_count(artifacts, required_roles),
        physical_meaning=physical_meaning,
        next_action=next_action,
    )


def _device_ids_from_plan(plan: Mapping[str, Any]) -> set[str]:
    out: set[str] = set()
    devices = plan.get("devices")
    if isinstance(devices, Mapping):
        analyzers = devices.get("gas_analyzers")
        if isinstance(analyzers, Sequence) and not isinstance(analyzers, (str, bytes)):
            for item in analyzers:
                if not isinstance(item, Mapping):
                    continue
                device_id = _normalize_device_id(
                    item.get("runtime_device_id") or item.get("device_id") or item.get("id")
                )
                if device_id:
                    out.add(device_id)
    return out


def _device_ids_from_csvs(paths: Sequence[Path]) -> set[str]:
    out: set[str] = set()
    for path in paths:
        for row in _read_csv(path):
            for key in ("analyzer_device_id", "device_id", "runtime_device_id", "target_device_id"):
                device_id = _normalize_device_id(row.get(key))
                if device_id:
                    out.add(device_id)
                    break
    return out


def _device_ids_from_artifact_dirs(dirs: Sequence[Path]) -> set[str]:
    filenames = (
        "candidate_policy_summary.csv",
        "candidate_fit_residuals.csv",
        "model_selection_summary.csv",
        "co2_formal_senco13_ratio_only_summary.csv",
        "co2_recommended_writeable_summary.csv",
        "co2_best_candidate_summary_all_models.csv",
        "h2o_senco24_device_policy.csv",
        "h2o_senco6_linear_trim_candidate_summary.csv",
        "h2o_dry_anchor_bridge_device_summary.csv",
        "linear_trim_review.csv",
    )
    out: set[str] = set()
    for directory in dirs:
        out.update(_device_ids_from_csvs(tuple(directory / filename for filename in filenames)))
    return out


def _device_ids_from_snapshot(payload: Mapping[str, Any]) -> set[str]:
    out: set[str] = set()
    devices = payload.get("devices")
    if isinstance(devices, Mapping):
        iterable = devices.items()
    elif isinstance(devices, Sequence) and not isinstance(devices, (str, bytes)):
        iterable = enumerate(devices)
    else:
        iterable = payload.items()
    for key, value in iterable:
        if isinstance(value, Mapping):
            device_id = _normalize_device_id(value.get("analyzer_device_id") or value.get("device_id") or key)
            if device_id:
                out.add(device_id)
    return out


def _pressure_completion_status_by_device(artifacts: Mapping[str, Sequence[Path]]) -> dict[str, str]:
    """Return per-device pressure readiness when a completion package is available."""

    pressure_paths = artifacts.get("pressure_review", ())
    candidate_paths: list[Path] = []
    for path in pressure_paths:
        if path.name == "pressure_channel_device_readiness.csv":
            candidate_paths.append(path)
        if path.name == "pressure_channel_completion_summary.csv":
            sibling = path.with_name("pressure_channel_device_readiness.csv")
            if sibling.exists():
                candidate_paths.append(sibling)

    statuses: dict[str, str] = {}
    for path in candidate_paths:
        for row in _read_csv(path):
            device_id = _normalize_device_id(
                row.get("analyzer_device_id") or row.get("device_id") or row.get("runtime_device_id")
            )
            if not device_id:
                continue
            status = str(row.get("readiness_status") or row.get("status") or "").strip().lower()
            if status == "pass":
                statuses[device_id] = "ready"
                continue
            reasons = str(row.get("readiness_reasons") or row.get("blocked_reasons") or status).strip()
            statuses[device_id] = (
                f"needs_senco9_review_or_calibration:{reasons}"
                if reasons
                else "needs_senco9_review_or_calibration"
            )
    return statuses


def _device_quality_status_by_device(artifacts: Mapping[str, Sequence[Path]]) -> dict[str, str]:
    """Return per-device hard rejection findings from root-cause/quality review artifacts."""

    hard_statuses = {
        "blocked",
        "fail",
        "failed",
        "reject",
        "rejected",
        "unqualified",
        "device_rejected",
        "not_calibratable",
    }
    hard_reason_markers = (
        "curve_inconsistent_not_window_noise",
        "ratio_stable_but_curve_inconsistent",
        "not_window_noise",
        "ratio_saturated",
        "saturation",
        "optical_root_cause",
        "reference_signal_saturation",
        "ref_signal_saturation",
        "not_calibratable",
        "unqualified",
        "device_rejected",
        "不合格",
        "拒绝",
    )
    findings: dict[str, list[str]] = {}
    for path in artifacts.get("device_quality_review", ()):
        for row in _read_csv(path):
            device_id = _normalize_device_id(
                row.get("analyzer_device_id") or row.get("device_id") or row.get("runtime_device_id")
            )
            if not device_id:
                continue
            component = str(row.get("component") or "device").strip().lower() or "device"
            status = _status_text(
                row.get("status")
                or row.get("review_status")
                or row.get("verdict")
                or row.get("quality_status")
                or row.get("qualification_status")
            )
            reason = str(
                row.get("reason_short")
                or row.get("root_cause")
                or row.get("reason")
                or row.get("blocked_reasons")
                or status
                or ""
            ).strip()
            reason_l = reason.lower()
            rel = _float_or_none(row.get("max_abs_relative_error_pct") or row.get("max_relative_error_pct"))
            hard = status in hard_statuses or any(marker in reason_l for marker in hard_reason_markers)
            if not hard and rel is not None and abs(rel) >= 20.0 and reason:
                hard = True
            if not hard:
                continue
            detail = reason or "device_quality_review_rejected"
            if rel is not None:
                detail = f"{component}:{detail}:max_abs_relative_error_pct={rel:.3f}"
            else:
                detail = f"{component}:{detail}"
            findings.setdefault(device_id, []).append(detail)
    return {
        device_id: "device_rejected_or_unqualified:" + ";".join(dict.fromkeys(reasons))
        for device_id, reasons in findings.items()
    }


def _fit_input_quality_status_by_device(artifacts: Mapping[str, Sequence[Path]]) -> dict[str, str]:
    """Return per-device fit-input blockers from the offline fit-input audit."""

    findings: dict[str, list[str]] = {}
    for path in artifacts.get("fit_input_quality", ()):
        if not path.is_file() or "devices" not in path.name.lower():
            continue
        for row in _read_csv(path):
            device_id = _normalize_device_id(
                row.get("analyzer_device_id") or row.get("device_id") or row.get("runtime_device_id")
            )
            if not device_id:
                continue
            component = str(row.get("component") or "component").strip().lower() or "component"
            grade = str(row.get("fit_input_grade") or "").strip().upper()
            status = _status_text(row.get("fit_input_status"))
            if grade == "A" and status in {"", "usable_for_candidate_fit", "ready", "pass"}:
                continue
            reason = str(row.get("reject_reasons") or status or grade or "not_a_grade").strip()
            findings.setdefault(device_id, []).append(f"{component}:{reason}")
    return {
        device_id: "fit_input_quality_rejected:" + ";".join(dict.fromkeys(reasons))
        for device_id, reasons in findings.items()
    }


def _h2o_dry_anchor_bridge_status_by_device(artifacts: Mapping[str, Sequence[Path]]) -> dict[str, str]:
    """Return per-device H2O dry-anchor bridge status when bridge evidence exists.

    Dry gas points are not interchangeable with CO2 zero-gas anchors. They can
    constrain low-water behavior only after dewpoint, pressure, and temperature
    evidence prove that their residual H2O target is physically compatible with
    the wet-route H2O model.
    """

    ready_recommendations = {
        "dry_anchors_can_enter_low_end_fit_review",
        "compatible_dry_anchor_subset_can_enter_low_end_review",
        "wet_points_main_fit_keep_dry_anchors_as_qc",
        "no_dry_anchor_evidence",
    }
    blocked_recommendations = {
        "collect_new_formal_dry_h2o_anchor_evidence",
    }
    statuses: dict[str, str] = {}
    for directory in artifacts.get("h2o_dry_anchor_bridge", ()):
        summary = directory / "h2o_dry_anchor_bridge_device_summary.csv"
        if not summary.exists():
            continue
        for row in _read_csv(summary):
            device_id = _normalize_device_id(
                row.get("analyzer_device_id") or row.get("device_id") or row.get("runtime_device_id")
            )
            if not device_id:
                continue
            recommendation = str(row.get("recommendation") or "").strip()
            if recommendation in blocked_recommendations:
                statuses[device_id] = f"h2o_dry_anchor_bridge_blocked:{recommendation}"
                continue
            if recommendation in ready_recommendations:
                statuses[device_id] = f"ready:{recommendation}"
                continue
            if recommendation:
                statuses[device_id] = f"h2o_dry_anchor_bridge_review_required:{recommendation}"
    return statuses


def _co2_source_state_gate_status(artifacts: Mapping[str, Sequence[Path]]) -> tuple[str, str, Path | None]:
    """Return the latest run-level CO2 source-state write gate status.

    The source-state audit is optional for legacy runs. When present, it blocks
    CO2 S1/S3/S5 write review if the same gas-point targets cannot be reconciled
    across source states such as different purge/dryness/temperature segments.
    """

    records: list[tuple[float, str, int, str, Path]] = []
    for directory in artifacts.get("co2_source_state_gate", ()):
        summary = directory / "co2_s13_source_state_run_summary.csv"
        if not summary.exists():
            continue
        for row in _read_csv(summary):
            status = _status_text(row.get("write_gate_status") or row.get("status"))
            blocker_count_value = _float_or_none(row.get("write_gate_blocker_count"))
            blocker_count = int(blocker_count_value or 0)
            topics = str(row.get("write_gate_blocker_topics") or "").strip()
            records.append((summary.stat().st_mtime, status, blocker_count, topics, summary))

    if not records:
        return "not_attempted", "optional_stage_not_attempted", None

    _, status, blocker_count, topics, path = max(records, key=lambda item: item[0])
    blocked = status.startswith("blocked") or blocker_count > 0
    if blocked:
        reason = f"co2_source_state_blocked:{status or 'blocked'}"
        if topics:
            reason = f"{reason}:{topics}"
        return "blocked", reason, path

    return "ready", f"co2_source_state_gate_{status or 'present'}", path


def _co2_source_state_gate_stage(artifacts: Mapping[str, Sequence[Path]]) -> ExecutorStage:
    status, reason, _ = _co2_source_state_gate_status(artifacts)
    return ExecutorStage(
        stage_id="co2_source_state_write_gate",
        title="CO2 source-state write gate",
        status=status,
        reason=reason,
        required_roles=("co2_source_state_gate",),
        artifact_count=_role_count(artifacts, ("co2_source_state_gate",)),
        physical_meaning=(
            "CO2 writes are blocked when equal gas-point evidence cannot be explained "
            "by one stable open-flow source-state contract."
        ),
        next_action=(
            "repair_source_state_discontinuity_before_co2_write_package"
            if status == "blocked"
            else "export_v1_5_co2_s13_source_state_discontinuity_audit_if_co2_residuals_disagree_with_reverification"
        ),
    )


def _classify_devices(run_dir: Path, artifacts: Mapping[str, Sequence[Path]]) -> tuple[DeviceClosure, ...]:
    plan = _load_json(artifacts.get("plan_snapshot", [None])[0] if artifacts.get("plan_snapshot") else None)
    ids = _device_ids_from_plan(plan)
    ids.update(_device_ids_from_csvs([p for role in ("write_events",) for p in artifacts.get(role, ())]))
    ids.update(_device_ids_from_artifact_dirs(artifacts.get("candidate_dirs", ())))
    ids.update(_device_ids_from_artifact_dirs(artifacts.get("model_selection_dirs", ())))
    ids.update(_device_ids_from_artifact_dirs(artifacts.get("linear_trim_dirs", ())))
    ids.update(_device_ids_from_artifact_dirs(artifacts.get("h2o_dry_anchor_bridge", ())))
    snapshot = _existing_latest(run_dir, ("old_component_coefficients_snapshot.json",))
    if snapshot:
        ids.update(_device_ids_from_snapshot(_load_json(snapshot)))

    if not ids:
        return ()

    has_init = _role_ready(artifacts, "initialization_readiness")
    has_pressure = _role_ready(artifacts, "pressure_review")
    pressure_status_by_device = _pressure_completion_status_by_device(artifacts)
    device_quality_by_device = _device_quality_status_by_device(artifacts)
    fit_input_quality_by_device = _fit_input_quality_status_by_device(artifacts)
    dry_anchor_bridge_by_device = _h2o_dry_anchor_bridge_status_by_device(artifacts)
    co2_source_state_status, co2_source_state_reason, _ = _co2_source_state_gate_status(artifacts)
    has_temp = _role_ready(artifacts, "temperature_review")
    has_fit_input_quality = _role_ready(artifacts, "fit_input_quality")
    candidate_dirs = artifacts.get("candidate_dirs", ())
    model_dirs = artifacts.get("model_selection_dirs", ())
    trim_dirs = artifacts.get("linear_trim_dirs", ())

    closures: list[DeviceClosure] = []
    for device_id in sorted(ids):
        blockers: list[str] = []
        identity_status = "ready" if has_init or snapshot else "missing_initial_identity_or_getco_snapshot"
        if identity_status != "ready":
            blockers.append(identity_status)
        pressure_status = pressure_status_by_device.get(
            device_id, "ready" if has_pressure else "needs_senco9_review_or_calibration"
        )
        if pressure_status != "ready":
            blockers.append(pressure_status)
        temperature_status = "ready" if has_temp else "needs_senco78_review_or_temperature_gate"
        if temperature_status != "ready":
            blockers.append(temperature_status)
        device_quality_status = device_quality_by_device.get(device_id, "ready")
        if device_quality_status != "ready":
            blockers.append(device_quality_status)
        fit_input_quality_status = fit_input_quality_by_device.get(
            device_id,
            "ready" if has_fit_input_quality else "fit_input_quality_review_not_ready",
        )
        if fit_input_quality_status != "ready":
            blockers.append(fit_input_quality_status)
        co2_status = _component_status_for_device(device_id, candidate_dirs, model_dirs, "co2")
        h2o_status = _component_status_for_device(device_id, candidate_dirs, model_dirs, "h2o")
        if co2_source_state_status == "blocked":
            co2_status = f"co2_blocked:{co2_source_state_reason}"
            blockers.append(co2_source_state_reason)
        elif not co2_status.startswith("candidate_ready"):
            blockers.append(co2_status)
        if not h2o_status.startswith("candidate_ready"):
            blockers.append(h2o_status)
        h2o_dry_anchor_bridge_status = dry_anchor_bridge_by_device.get(device_id, "not_attempted")
        if h2o_dry_anchor_bridge_status.startswith("h2o_dry_anchor_bridge_blocked"):
            blockers.append(h2o_dry_anchor_bridge_status)
        output_trim_status = "review_ready" if trim_dirs else "needs_s5_s6_linear_trim_review_after_main_fit"
        if output_trim_status != "review_ready":
            blockers.append(output_trim_status)
        overall = "ready_for_controlled_write_review" if not blockers else "blocked_or_partial"
        next_action = (
            "export_controlled_write_package_then_execute_reviewed_writers"
            if overall == "ready_for_controlled_write_review"
            else "repair_missing_evidence_or_exclude_bad_device_with_reason"
        )
        unique_blockers = tuple(dict.fromkeys(blockers))
        closures.append(
            DeviceClosure(
                device_id=device_id,
                identity_status=identity_status,
                pressure_status=pressure_status,
                temperature_status=temperature_status,
                device_quality_status=device_quality_status,
                fit_input_quality_status=fit_input_quality_status,
                co2_status=co2_status,
                h2o_status=h2o_status,
                h2o_dry_anchor_bridge_status=h2o_dry_anchor_bridge_status,
                output_trim_status=output_trim_status,
                overall_status=overall,
                blockers=unique_blockers,
                next_action=next_action,
                blocker_summary_zh=_blocker_summary_zh(unique_blockers),
                next_action_zh=_next_action_to_zh(next_action, unique_blockers),
            )
        )
    return tuple(closures)


def _component_status_for_device(
    device_id: str,
    candidate_dirs: Sequence[Path],
    model_dirs: Sequence[Path],
    component: str,
) -> str:
    candidate_records: list[tuple[float, str, str]] = []
    model_records: list[tuple[float, str, str]] = []

    def row_device(row: Mapping[str, str]) -> str:
        for key in ("analyzer_device_id", "device_id", "runtime_device_id", "target_device_id"):
            normalized = _normalize_device_id(row.get(key))
            if normalized:
                return normalized
        return ""

    def row_component(row: Mapping[str, str], filename: str) -> str:
        explicit = str(row.get("component") or "").strip().lower()
        if explicit:
            return explicit
        lowered = filename.lower()
        if lowered.startswith("co2_"):
            return "co2"
        if lowered.startswith("h2o_"):
            return "h2o"
        return ""

    def row_blocker(row: Mapping[str, str]) -> str:
        reason = str(row.get("blocked_reasons") or "").strip()
        if reason:
            return reason
        status = str(row.get("candidate_status") or row.get("status") or "").strip().lower()
        if status in {"blocked", "block", "failed", "fail"} or status.startswith("blocked"):
            return status or "candidate_policy_blocked"
        note = str(row.get("review_note") or "").strip()
        if note.startswith("blocked"):
            return note
        return ""

    def row_readiness(row: Mapping[str, str]) -> str:
        status = str(
            row.get("candidate_status")
            or row.get("review_status")
            or row.get("status")
            or ""
        ).strip().lower()
        if status in {
            "ready",
            "review_ready",
            "ready_for_reviewer",
            "fit_ready_requires_verification",
            "candidate_fit_review_required",
        }:
            return "ready"
        for key in ("allowed_for_review", "allowed_to_fit", "candidate_fit_may_be_reviewed"):
            value = str(row.get(key) or "").strip().lower()
            if value in {"true", "1", "yes"}:
                return "ready"
        if any(str(row.get(key) or "").strip() for key in ("recommended_model_id", "model_id", "selected_model_terms")):
            return "ready"
        if row_blocker(row):
            return "blocked"
        if status in {"blocked", "block", "failed", "fail"} or status.startswith("blocked"):
            return "blocked"
        return "present"

    def scan_rows(directory: Path, filenames: Sequence[str]) -> Iterable[tuple[str, dict[str, str]]]:
        for filename in filenames:
            for row in _read_csv(directory / filename):
                yield filename, row

    for candidate_dir in candidate_dirs:
        for filename, row in scan_rows(
            candidate_dir,
            (
                "candidate_policy_summary.csv",
                "candidate_fit_residuals.csv",
                "co2_formal_senco13_ratio_only_summary.csv",
                "co2_recommended_writeable_summary.csv",
                "h2o_senco24_device_policy.csv",
            ),
        ):
            if row_device(row) != device_id:
                continue
            if row_component(row, filename) != component:
                continue
            candidate_records.append((candidate_dir.stat().st_mtime, row_readiness(row), row_blocker(row)))
    for model_dir in model_dirs:
        for filename, row in scan_rows(
            model_dir,
            (
                "model_selection_summary.csv",
                "co2_recommended_writeable_summary.csv",
                "co2_best_candidate_summary_all_models.csv",
                "co2_formal_senco13_ratio_only_summary.csv",
                "h2o_senco24_device_policy.csv",
            ),
        ):
            if row_device(row) != device_id:
                continue
            if row_component(row, filename) != component:
                continue
            marker = (
                row.get("recommended_model")
                or row.get("recommended_model_id")
                or row.get("model_id")
                or row.get("selected_model_terms")
            )
            if str(marker or "").strip().lower() in {"true", "1", "yes"} or str(marker or "").strip():
                model_records.append((model_dir.stat().st_mtime, "ready", row_blocker(row)))
    latest_candidate = max(candidate_records, default=None, key=lambda item: item[0])
    latest_model = max(model_records, default=None, key=lambda item: item[0])
    if latest_candidate and latest_candidate[1] == "blocked":
        return f"{component}_blocked:{latest_candidate[2] or 'candidate_policy_blocked'}"
    if latest_model and latest_model[2]:
        return f"{component}_blocked:{latest_model[2]}"
    if latest_candidate and latest_model and latest_candidate[1] == "ready":
        return f"candidate_ready_{component}"
    if latest_candidate:
        return f"{component}_candidate_present_model_selection_missing"
    return f"{component}_candidate_missing"


def _execution_commands() -> list[dict[str, Any]]:
    return [
        {
            "order": 10,
            "action": "initialization_readiness",
            "tool": "python -m gas_calibrator.tools.export_v1_5_initialization_readiness",
            "writes_senco": False,
            "physical_gate": "device identity and GETCO1-9 epoch-0 snapshot are frozen before any coefficient math",
        },
        {
            "order": 20,
            "action": "pressure_senco9_review_or_calibration",
            "tool": "python -m gas_calibrator.tools.validate_pressure_only",
            "writes_senco": "controlled_writer_only",
            "physical_gate": "internal pressure P is an input to firmware concentration calculations and must be trustworthy first",
        },
        {
            "order": 30,
            "action": "temperature_senco78_review_or_repair",
            "tool": "temperature review / controlled SENCO7-SENCO8 tools",
            "writes_senco": "controlled_writer_only",
            "physical_gate": "temperature input errors must not be absorbed into CO2 or H2O coefficients",
        },
        {
            "order": 40,
            "action": "co2_candidate_coefficients",
            "tool": "python -m gas_calibrator.tools.export_v1_5_candidate_coefficients --component co2 --fit-all-eligible-samples",
            "writes_senco": False,
            "physical_gate": "open-flow CO2 fit uses eligible stable samples and freezes pressure terms under current-atmosphere contract",
        },
        {
            "order": 45,
            "action": "co2_source_state_write_gate",
            "tool": "python -m gas_calibrator.tools.export_v1_5_co2_s13_source_state_discontinuity_audit",
            "writes_senco": False,
            "physical_gate": "CO2 write review is blocked when same gas-point source states cannot be reconciled.",
        },
        {
            "order": 50,
            "action": "h2o_candidate_coefficients",
            "tool": "python -m gas_calibrator.tools.export_v1_5_candidate_coefficients --component h2o --fit-all-eligible-samples",
            "writes_senco": False,
            "physical_gate": "H2O fit uses dewpoint-backed water evidence and keeps dry-gas anchor distinct from CO2 zero gas",
        },
        {
            "order": 55,
            "action": "h2o_dry_anchor_bridge_review",
            "tool": "python -m gas_calibrator.tools.export_v1_5_h2o_dry_anchor_bridge_review",
            "writes_senco": False,
            "physical_gate": (
                "gas-route dry anchors may constrain the H2O low end only after "
                "dewpoint, pressure, and temperature bridge review"
            ),
        },
        {
            "order": 58,
            "action": "fit_input_quality_review",
            "tool": "python -m gas_calibrator.tools.export_v1_5_fit_input_quality",
            "writes_senco": False,
            "physical_gate": (
                "controlled writes require route-continuity-aware A-grade fit inputs; "
                "segmented or migration evidence stays blocked"
            ),
        },
        {
            "order": 60,
            "action": "model_selection_and_s5_s6_review",
            "tool": "python -m gas_calibrator.tools.export_v1_5_candidate_model_selection_review",
            "writes_senco": False,
            "physical_gate": "S5/S6 are final displayed-output affine trims and are reviewed after main-chain fit",
        },
        {
            "order": 70,
            "action": "main_senco_write_precheck_pack",
            "tool": "python -m gas_calibrator.tools.export_v1_5_main_senco_write_precheck_pack",
            "writes_senco": False,
            "physical_gate": "S1/S3 and S2/S4 write payloads are reviewed before any controlled writer is unlocked",
        },
        {
            "order": 80,
            "action": "controlled_main_writes",
            "tool": "controlled writers for SENCO1/SENCO3 and SENCO2/SENCO4",
            "writes_senco": "requires explicit writer unlock and identity check",
            "physical_gate": "main chain is written before S5/S6 trim so trims do not hide model errors",
        },
        {
            "order": 90,
            "action": "post_main_open_flow_reverification",
            "tool": "formal open-flow verification queue",
            "writes_senco": False,
            "physical_gate": "verification must use fresh open-flow samples with valves open during sampling",
        },
        {
            "order": 100,
            "action": "controlled_s5_s6_writes_if_needed",
            "tool": "controlled SENCO5/SENCO6 affine trim writers",
            "writes_senco": "requires explicit writer unlock and identity check",
            "physical_gate": "output-layer trim is allowed only when main-chain residuals show stable affine error",
        },
        {
            "order": 110,
            "action": "final_reverification_archive_database_reports",
            "tool": "post-write reverification and formal archive closure exporters",
            "writes_senco": False,
            "physical_gate": "final certificates and database import are generated from evidence, not hand-edited results",
        },
    ]


def _closure_gaps(stages: Sequence[ExecutorStage], devices: Sequence[DeviceClosure]) -> list[ClosureGap]:
    gaps: list[ClosureGap] = []
    for stage in stages:
        if stage.status in {"ready"}:
            continue
        gaps.append(
            ClosureGap(
                scope="stage",
                item=stage.stage_id,
                status=stage.status,
                reason=stage.reason,
                next_action=stage.next_action,
                physical_meaning=stage.physical_meaning,
                reason_zh=_reason_to_zh(stage.reason or stage.stage_id),
                next_action_zh=_next_action_to_zh(stage.next_action),
            )
        )
    for device in devices:
        if device.overall_status == "ready_for_controlled_write_review":
            continue
        for blocker in device.blockers:
            gaps.append(
                ClosureGap(
                    scope=f"device:{device.device_id}",
                    item=blocker.split(":", 1)[0],
                    status=device.overall_status,
                    reason=blocker,
                    next_action=device.next_action,
                    physical_meaning=(
                        "This device must not inherit another analyzer's evidence; "
                        "repair the missing input or exclude the device with an auditable reason."
                    ),
                    reason_zh=_reason_to_zh(blocker),
                    next_action_zh=_next_action_to_zh(device.next_action, device.blockers),
                )
            )
    return gaps


def _controlled_write_package_rows(devices: Sequence[DeviceClosure]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for device in devices:
        if device.overall_status != "ready_for_controlled_write_review":
            rows.append(
                {
                    "device_id": device.device_id,
                    "order": 0,
                    "phase": "blocked",
                    "component": "device",
                    "plan_status": "blocked_until_evidence_repaired",
                    "requires_explicit_authorization": "false",
                    "opens_com_ports": "false",
                    "writes_senco": "false",
                    "source_status": device.overall_status,
                    "physical_gate": "do not write a device whose own evidence is blocked or partial",
                    "next_action": device.next_action,
                    "blockers": ";".join(device.blockers),
                }
            )
            continue

        base = {
            "device_id": device.device_id,
            "plan_status": "ready_for_controlled_write_review",
            "blockers": "",
        }
        rows.extend(
            [
                {
                    **base,
                    "order": 10,
                    "phase": "identity",
                    "component": "device_id_getco_snapshot",
                    "requires_explicit_authorization": "false",
                    "opens_com_ports": "controlled_writer_precheck_only",
                    "writes_senco": "false",
                    "source_status": device.identity_status,
                    "physical_gate": "verify current device ID before using any COM label",
                    "next_action": "read device ID and confirm old GETCO1-9 snapshot",
                },
                {
                    **base,
                    "order": 20,
                    "phase": "input_quantity",
                    "component": "pressure_temperature",
                    "requires_explicit_authorization": "false",
                    "opens_com_ports": "false",
                    "writes_senco": "false",
                    "source_status": f"{device.pressure_status};{device.temperature_status}",
                    "physical_gate": "SENCO9 and SENCO7/8 must be reviewed before component writes",
                    "next_action": "confirm pressure and temperature reviews remain valid",
                },
                {
                    **base,
                    "order": 30,
                    "phase": "main_component_write",
                    "component": "co2_senco1_senco3",
                    "requires_explicit_authorization": "true",
                    "opens_com_ports": "controlled_writer_only",
                    "writes_senco": "reviewed_payload_only",
                    "source_status": device.co2_status,
                    "physical_gate": "CO2 main-chain coefficients come from eligible open-flow ratio samples",
                    "next_action": "write reviewed SENCO1/SENCO3 candidate, then read back",
                },
                {
                    **base,
                    "order": 40,
                    "phase": "main_component_write",
                    "component": "h2o_senco2_senco4",
                    "requires_explicit_authorization": "true",
                    "opens_com_ports": "controlled_writer_only",
                    "writes_senco": "reviewed_payload_only",
                    "source_status": f"{device.h2o_status};{device.h2o_dry_anchor_bridge_status}",
                    "physical_gate": (
                        "H2O main-chain coefficients come from eligible open-flow humidity samples; "
                        "dry-gas low-water anchors must pass dewpoint/pressure/temperature bridge review before use"
                    ),
                    "next_action": "write reviewed SENCO2/SENCO4 candidate, then read back",
                },
                {
                    **base,
                    "order": 50,
                    "phase": "post_main_reverification",
                    "component": "co2_h2o_open_flow",
                    "requires_explicit_authorization": "operator_run_authorization",
                    "opens_com_ports": "reverification_runner_only",
                    "writes_senco": "false",
                    "source_status": "required_after_main_write",
                    "physical_gate": "sample only while the gas or water route remains open-flow",
                    "next_action": "run low/mid/high CO2 and representative H2O reverification",
                },
                {
                    **base,
                    "order": 60,
                    "phase": "output_trim_write",
                    "component": "senco5_senco6",
                    "requires_explicit_authorization": "true_if_trim_review_selected",
                    "opens_com_ports": "controlled_writer_only",
                    "writes_senco": "reviewed_payload_only_if_needed",
                    "source_status": device.output_trim_status,
                    "physical_gate": "S5/S6 are final display-layer affine trims, after main-chain validation",
                    "next_action": "write S5/S6 only when trim review proves stable affine residuals",
                },
                {
                    **base,
                    "order": 70,
                    "phase": "final_reverification",
                    "component": "co2_h2o_open_flow",
                    "requires_explicit_authorization": "operator_run_authorization",
                    "opens_com_ports": "reverification_runner_only",
                    "writes_senco": "false",
                    "source_status": "required_after_any_trim_write",
                    "physical_gate": "final acceptance is based on independent post-write open-flow data",
                    "next_action": "run final post-write reverification and classify residual errors",
                },
                {
                    **base,
                    "order": 80,
                    "phase": "archive",
                    "component": "database_report_certificate",
                    "requires_explicit_authorization": "false",
                    "opens_com_ports": "false",
                    "writes_senco": "false",
                    "source_status": "required_for_traceability",
                    "physical_gate": "certificates are regenerated from raw evidence, hashes, and readbacks",
                    "next_action": "import audit rows, refresh evidence status, and generate certificates",
                },
            ]
        )
    return rows


def _post_write_reverification_plan_rows(devices: Sequence[DeviceClosure]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for device in devices:
        if device.overall_status != "ready_for_controlled_write_review":
            rows.append(
                {
                    "device_id": device.device_id,
                    "component": "device",
                    "plan_status": "blocked_until_device_ready",
                    "route_contract": "",
                    "suggested_points": "",
                    "acceptance_basis": "",
                    "blockers": ";".join(device.blockers),
                }
            )
            continue
        rows.extend(
            [
                {
                    "device_id": device.device_id,
                    "component": "co2",
                    "plan_status": "required_after_senco1_senco3_or_senco5_write",
                    "route_contract": "gas route must remain open until the sample window is complete",
                    "suggested_points": "low/mid/high or reviewed worst-case CO2 points",
                    "acceptance_basis": "certificate value, filtered ratio stability, dewpoint/dryness evidence, and final displayed CO2 error",
                    "blockers": "",
                },
                {
                    "device_id": device.device_id,
                    "component": "h2o",
                    "plan_status": "required_after_senco2_senco4_or_senco6_write",
                    "route_contract": "water route must remain open until the sample window is complete",
                    "suggested_points": "dry anchor plus representative mid/high H2O points",
                    "acceptance_basis": "dewpoint reference, H2O ratio stability, dry/wet conversion evidence, and final displayed H2O error",
                    "blockers": "",
                },
            ]
        )
    return rows


def build_post_run_coefficient_executor_model(
    *,
    run_dir: str | Path,
    plan_json: str | Path | None = None,
    pressure_reference_json: str | Path | None = None,
    initialization_readiness_json: str | Path | None = None,
    run_evidence_status_json: str | Path | None = None,
    pressure_review_json: str | Path | None = None,
    pressure_completion_summary_csv: str | Path | None = None,
    pressure_device_readiness_csv: str | Path | None = None,
    temperature_review_csv: str | Path | None = None,
    device_quality_review_csv: str | Path | None = None,
    main_precheck_meta_json: str | Path | None = None,
    post_write_reverification_json: str | Path | None = None,
    archive_closure_json: str | Path | None = None,
    co2_source_state_gate: str | Path | None = None,
    fit_input_quality_summary_csv: str | Path | None = None,
    fit_input_quality_devices_csv: str | Path | None = None,
) -> dict[str, Any]:
    root = Path(run_dir).resolve()
    explicit = {
        "plan_json": plan_json,
        "pressure_reference_json": pressure_reference_json,
        "initialization_readiness_json": initialization_readiness_json,
        "run_evidence_status_json": run_evidence_status_json,
        "pressure_review_json": pressure_review_json,
        "pressure_completion_summary_csv": pressure_completion_summary_csv,
        "pressure_device_readiness_csv": pressure_device_readiness_csv,
        "temperature_review_csv": temperature_review_csv,
        "device_quality_review_csv": device_quality_review_csv,
        "main_precheck_meta_json": main_precheck_meta_json,
        "post_write_reverification_json": post_write_reverification_json,
        "archive_closure_json": archive_closure_json,
        "co2_source_state_gate": co2_source_state_gate,
        "fit_input_quality_summary_csv": fit_input_quality_summary_csv,
        "fit_input_quality_devices_csv": fit_input_quality_devices_csv,
    }
    artifacts = _discover_artifacts(root, explicit)
    stages = (
        _stage_status(
            artifacts,
            stage_id="identity_initialization",
            title="Identity and epoch-0 GETCO snapshot",
            required_roles=("plan_snapshot", "initialization_readiness"),
            physical_meaning="Device ID, serial mapping, and GETCO1-9 must be frozen before coefficient calculation.",
            next_action="repair_initialization_readiness_or_bind_runtime_identity",
        ),
        _stage_status(
            artifacts,
            stage_id="pressure_input_quantity",
            title="Pressure input quantity SENCO9",
            required_roles=("pressure_reference", "pressure_review"),
            physical_meaning="Analyzer pressure P is a firmware input. It is calibrated or verified separately from CO2/H2O.",
            next_action="run_pressure_senco9_review_or_controlled_calibration",
        ),
        _stage_status(
            artifacts,
            stage_id="temperature_input_quantity",
            title="Temperature input quantity SENCO7/SENCO8",
            required_roles=("temperature_review",),
            physical_meaning="Temperature errors must be reviewed before multi-temperature CO2/H2O fitting.",
            next_action="run_temperature_review_or_controlled_senco78_repair",
        ),
        _stage_status(
            artifacts,
            stage_id="component_candidate_fit",
            title="CO2/H2O main candidate fit",
            required_roles=("candidate_dirs", "model_selection_dirs"),
            physical_meaning="Open-flow stable component data are converted into S1/S3 and S2/S4 candidates.",
            next_action="export_candidate_coefficients_and_model_selection",
        ),
        _stage_status(
            artifacts,
            stage_id="fit_input_quality_review",
            title="Route-continuity-aware fit-input quality review",
            required_roles=("fit_input_quality",),
            physical_meaning=(
                "Candidate coefficient rows may feed controlled write review only after the fit-input audit "
                "confirms A-grade device/component inputs from a continuous mature route run."
            ),
            next_action="export_v1_5_fit_input_quality_after_mature_route_continuity_gate_passes",
        ),
        _co2_source_state_gate_stage(artifacts),
        _stage_status(
            artifacts,
            stage_id="h2o_dry_anchor_bridge_review",
            title="H2O dry-anchor bridge review",
            required_roles=("h2o_dry_anchor_bridge",),
            physical_meaning=(
                "Gas-route dry points are low-water bridge evidence only when "
                "dewpoint, pressure, and temperature prove compatibility with the wet-route H2O model."
            ),
            next_action="export_h2o_dry_anchor_bridge_review_if_dry_anchors_are_used",
            optional=True,
        ),
        _stage_status(
            artifacts,
            stage_id="output_layer_trim_review",
            title="S5/S6 output-layer trim review",
            required_roles=("linear_trim_dirs",),
            physical_meaning="S5/S6 are final displayed-concentration affine trims, after main-chain fit.",
            next_action="export_s5_s6_trim_review_after_main_candidate",
        ),
        _stage_status(
            artifacts,
            stage_id="main_write_precheck",
            title="Main SENCO write precheck package",
            required_roles=("main_precheck_pack",),
            physical_meaning="Controlled writers can only consume reviewed payloads and old snapshots.",
            next_action="export_main_senco_write_precheck_pack",
        ),
        _stage_status(
            artifacts,
            stage_id="controlled_write_events",
            title="Controlled coefficient write events",
            required_roles=("write_events",),
            physical_meaning="Every write must be linked to device ID, old value, new value, readback, actor, and reason.",
            next_action="execute_or_import_controlled_write_events",
            optional=True,
        ),
        _stage_status(
            artifacts,
            stage_id="post_write_reverification",
            title="Post-write open-flow reverification",
            required_roles=("post_write_reverification",),
            physical_meaning="Final acceptance requires independent open-flow verification after coefficient writes.",
            next_action="run_post_write_reverification_and_export_review",
            optional=True,
        ),
        _stage_status(
            artifacts,
            stage_id="archive_database_reports",
            title="Archive, database, reports, and certificates",
            required_roles=("run_evidence_status", "database_sidecar", "archive_closure"),
            physical_meaning="Reports and certificates are regenerated from evidence and hashes, not manual edits.",
            next_action="run_formal_archive_closure_and_database_import",
            optional=True,
        ),
    )
    devices = _classify_devices(root, artifacts)
    blockers = [stage.stage_id for stage in stages if stage.status == "blocked"]
    device_blockers = [
        device.device_id
        for device in devices
        if device.overall_status != "ready_for_controlled_write_review"
    ]
    if blockers:
        overall = "blocked"
    elif any(stage.status == "partial" for stage in stages) or device_blockers:
        overall = "partial"
    elif any(stage.status == "not_attempted" for stage in stages):
        overall = "ready_for_next_automatic_step"
    else:
        overall = "complete"
    gaps = _closure_gaps(stages, devices)
    controlled_write_package = _controlled_write_package_rows(devices)
    post_write_reverification_plan = _post_write_reverification_plan_rows(devices)
    return {
        "schema": SCHEMA,
        "created_at": _now(),
        "run_dir": str(root),
        "overall_status": overall,
        "physical_boundaries": {
            "offline_only": True,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "controls_valves_or_pace": False,
            "writes_coefficients": False,
            "writes_device_id": False,
            "not_real_acceptance_evidence": True,
        },
        "artifact_paths": {role: [str(path) for path in paths] for role, paths in artifacts.items()},
        "stages": [stage.to_json() for stage in stages],
        "devices": [device.to_json() for device in devices],
        "closure_gaps": [gap.to_json() for gap in gaps],
        "execution_order": _execution_commands(),
        "controlled_write_package": controlled_write_package,
        "post_write_reverification_plan": post_write_reverification_plan,
        "archive_gap_list": [gap.to_json() for gap in gaps],
        "workflow_contract": {
            "pressure_before_components": True,
            "temperature_before_components": True,
            "fit_all_eligible_stable_points": True,
            "fit_input_quality_review_before_controlled_write": True,
            "fit_verification_labels_do_not_exclude_samples_by_default": True,
            "co2_source_state_gate_blocks_writes": True,
            "co2_zero_anchor_distinct_from_h2o_dry_anchor": True,
            "h2o_dry_anchor_requires_dewpoint_pressure_temperature_bridge": True,
            "current_atmosphere_component_fit_freezes_pressure_terms": True,
            "s5_s6_after_main_fit": True,
            "sampling_window_requires_route_open": True,
            "open_flow_pressure_is_diagnostic_not_hard_block": True,
            "bad_device_does_not_block_other_devices": True,
        },
    }


def render_post_run_coefficient_executor_markdown(model: Mapping[str, Any]) -> str:
    """Render a readable Chinese reviewer summary for the post-run workflow."""

    lines = [
        "# V1.5 采集后系数闭环执行计划",
        "",
        f"- 运行目录：`{model.get('run_dir')}`",
        f"- 总体状态：`{model.get('overall_status')}`",
        "- 性质：离线 no-write 计划；不打开串口、不控制阀门或 PACE、不写 SENCO、不修改设备 ID。",
        "",
        "## 默认离线收口入口",
        "",
        "采集完成后默认先运行 post-acquisition closure。它只整理证据链：生成候选系数评审、受控写入包、写后复验计划、归档缺口清单和总 readiness，不执行真实设备动作。",
        "",
        "## 物理顺序",
        "",
        (
            "压力 P 和温度 T 是分析仪内部 CO2/H2O 算法的输入量，必须先独立验证或修正；"
            "CO2/H2O 主链路只使用开放流通、组分稳定、可追溯的样本；"
            "S5/S6 是最终显示层线性修正，必须放在 S1/S3 与 S2/S4 主链路之后。"
        ),
        "",
        "## 阶段门禁",
        "",
        "| 阶段 | 状态 | 原因 | 证据数 | 中文建议 |",
        "| --- | --- | --- | ---: | --- |",
    ]
    for row in model.get("stages") or []:
        lines.append(
            "| {stage} | `{status}` | {reason} | {count} | {next_action} |".format(
                stage=row.get("title", row.get("stage_id", "")),
                status=row.get("status", ""),
                reason=row.get("reason", ""),
                count=row.get("artifact_count", 0),
                next_action=_next_action_to_zh(row.get("next_action")),
            )
        )

    lines.extend(
        [
            "",
            "## 逐台设备状态",
            "",
            "| 设备 ID | 压力 | 温度 | 质量 | CO2 | H2O | S5/S6 | 总体 | 中文阻断解释 | 下一步 |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for row in model.get("devices") or []:
        blockers = row.get("blockers") or []
        if isinstance(blockers, str):
            blockers = [item for item in blockers.split(";") if item]
        lines.append(
            "| {device} | {pressure} | {temperature} | {quality} | {co2} | {h2o} | {trim} | {overall} | {blockers_zh} | {next_action_zh} |".format(
                device=row.get("device_id", ""),
                pressure=row.get("pressure_status", ""),
                temperature=row.get("temperature_status", ""),
                quality=row.get("device_quality_status", ""),
                co2=row.get("co2_status", ""),
                h2o=row.get("h2o_status", ""),
                trim=row.get("output_trim_status", ""),
                overall=row.get("overall_status", ""),
                blockers_zh=row.get("blocker_summary_zh") or _blocker_summary_zh(blockers),
                next_action_zh=row.get("next_action_zh") or _next_action_to_zh(row.get("next_action"), blockers),
            )
        )

    lines.extend(
        [
            "",
            "## 闭环缺口清单",
            "",
            "| 范围 | 项目 | 状态 | 内部原因 | 中文原因 | 中文下一步 |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
    )
    gaps = list(model.get("closure_gaps") or [])
    if not gaps:
        lines.append("| - | - | ready | 无缺口 | 暂未发现闭环缺口。 | 可进入下一阶段评审。 |")
    for row in gaps:
        lines.append(
            "| {scope} | {item} | `{status}` | {reason} | {reason_zh} | {next_action_zh} |".format(
                scope=row.get("scope", ""),
                item=row.get("item", ""),
                status=row.get("status", ""),
                reason=row.get("reason", ""),
                reason_zh=row.get("reason_zh") or _reason_to_zh(row.get("reason")),
                next_action_zh=row.get("next_action_zh") or _next_action_to_zh(row.get("next_action")),
            )
        )

    lines.extend(
        [
            "",
            "## 自动生成的后续计划",
            "",
            "- `controlled_write_package.csv`：逐台设备的受控写入包，要求先核对设备 ID 和旧系数快照，再写入已评审 payload。",
            "- `post_write_reverification_plan.csv`：写入后的开放流通复验计划，要求采样窗口必须在气路/水路保持开放流通时取得。",
            "- `archive_gap_list.csv`：归档、数据库、报告、证书仍缺的证据项。",
            "",
            "## 固化原则",
            "",
            "- 使用设备自身 ID 作为身份，不按 COM 或 GA 标签写系数。",
            "- CO2 零气低端锚点和 H2O 干气低水锚点物理意义不同，不能互相替代。",
            "- 当前大气开放流通 CO2/H2O 主拟合不引入压力项，压力由 SENCO9 独立处理。",
            "- fit / verification 标签不默认排除样本；只要样本满足稳定、证书、状态寄存器和物理门禁，就可以进入拟合。",
            "- 某台设备异常只阻断该设备，不拖死其它设备；异常点必须保留拒绝原因。",
            "- 采样窗口必须在气路/水路保持开放流通时取得，采样完成后才允许关阀。",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def write_post_run_coefficient_executor_outputs(
    model: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    manifest = root / "executor_manifest.json"
    summary = root / "executor_summary.md"
    stages = root / "executor_stages.csv"
    devices = root / "device_eligibility.csv"
    gaps = root / "closure_gap_list.csv"
    actions = root / "coefficient_execution_plan.csv"
    controlled_writes = root / "controlled_write_package.csv"
    reverification_plan = root / "post_write_reverification_plan.csv"
    archive_gaps = root / "archive_gap_list.csv"
    manifest.write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    summary.write_text(render_post_run_coefficient_executor_markdown(model), encoding="utf-8-sig")
    _write_csv(stages, model.get("stages") or [])
    _write_csv(devices, model.get("devices") or [])
    _write_csv(gaps, model.get("closure_gaps") or [])
    _write_csv(actions, model.get("execution_order") or [])
    _write_csv(controlled_writes, model.get("controlled_write_package") or [])
    _write_csv(reverification_plan, model.get("post_write_reverification_plan") or [])
    _write_csv(archive_gaps, model.get("archive_gap_list") or [])
    return {
        "manifest": manifest,
        "summary": summary,
        "stages": stages,
        "devices": devices,
        "closure_gaps": gaps,
        "execution_plan": actions,
        "controlled_write_package": controlled_writes,
        "post_write_reverification_plan": reverification_plan,
        "archive_gap_list": archive_gaps,
    }
