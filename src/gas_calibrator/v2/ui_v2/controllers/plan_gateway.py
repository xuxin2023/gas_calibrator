from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable, Optional

from ...config import (
    AppConfig,
    build_step2_config_governance_handoff,
    build_step2_config_safety_review,
    hydrate_step2_config_safety_summary,
    summarize_step2_config_safety,
)
from ...core.plan_compiler import CompiledPlan, PlanCompiler
from ...domain.mode_models import ModeProfile
from ...domain.plan_models import AnalyzerSetupSpec, CalibrationPlanProfile, PlanOrderingOptions
from ...domain.pressure_selection import pressure_target_label
from ...storage import ProfileStore


class PlanGateway:
    """UI-safe gateway for editable calibration plan profiles and compile preview."""

    def __init__(
        self,
        *,
        profile_store: ProfileStore,
        config_provider: Callable[[], AppConfig],
        compiled_points_dir: Path | None = None,
    ) -> None:
        self.profile_store = profile_store
        self._config_provider = config_provider
        self.compiled_points_dir = None if compiled_points_dir is None else Path(compiled_points_dir)

    def create_empty_profile(self, *, name: str = "", description: str = "") -> dict[str, Any]:
        config = deepcopy(self._config_provider())
        workflow = config.workflow
        profile = CalibrationPlanProfile(
            name=str(name or ""),
            profile_version="1.0",
            description=str(description or ""),
            is_default=False,
            mode_profile=ModeProfile.from_value(getattr(workflow, "run_mode", "auto_calibration")),
            analyzer_setup=AnalyzerSetupSpec.from_dict(getattr(workflow, "analyzer_setup", {})),
            ordering=PlanOrderingOptions(
                water_first=bool(getattr(workflow, "water_first_all_temps", False)),
                water_first_temp_gte=getattr(workflow, "water_first_temp_gte", None),
                selected_temps_c=list(getattr(workflow, "selected_temps_c", []) or []),
                selected_pressure_points=list(getattr(workflow, "selected_pressure_points", []) or []),
                skip_co2_ppm=list(getattr(workflow, "skip_co2_ppm", []) or []),
                temperature_descending=bool(getattr(workflow, "temperature_descending", True)),
                water_first_explicit=True,
                water_first_temp_gte_explicit=True,
            ),
        )
        return profile.to_dict()

    def list_profiles(self) -> list[dict[str, Any]]:
        return [self._decorate_profile_summary(item.to_dict()) for item in self.profile_store.list_profiles()]

    def load_profile(self, name: str) -> dict[str, Any] | None:
        profile = self.profile_store.load_profile(name)
        return None if profile is None else self._decorate_profile_payload(profile.to_dict())

    def get_default_profile(self) -> dict[str, Any] | None:
        profile = self.profile_store.get_default_profile()
        return None if profile is None else self._decorate_profile_payload(profile.to_dict())

    def save_profile(
        self,
        payload: dict[str, Any] | CalibrationPlanProfile,
        *,
        name_override: Optional[str] = None,
        set_default: Optional[bool] = None,
    ) -> dict[str, Any]:
        profile = self._coerce_profile(payload)
        if name_override is not None:
            profile.name = str(name_override)
        if set_default is not None:
            profile.is_default = bool(set_default)
        saved = self.profile_store.save_profile(profile)
        return self._decorate_profile_payload(saved.to_dict())

    def delete_profile(self, name: str) -> bool:
        return bool(self.profile_store.delete_profile(name))

    def set_default_profile(self, name: str) -> dict[str, Any]:
        profile = self.profile_store.set_default_profile(name)
        return self._decorate_profile_payload(profile.to_dict())

    def duplicate_profile(self, source_name: str, new_name: str) -> dict[str, Any]:
        source = self.profile_store.load_profile(source_name)
        if source is None:
            raise ValueError(f"profile not found: {source_name}")
        target_name = str(new_name or "").strip()
        if not target_name:
            raise ValueError("profile name is required")
        if target_name == source.name:
            raise ValueError("duplicate profile name must be different")
        if self.profile_store.load_profile(target_name) is not None:
            raise ValueError(f"profile already exists: {target_name}")
        source.name = target_name
        source.is_default = False
        saved = self.profile_store.save_profile(source)
        return self._decorate_profile_payload(saved.to_dict())

    def rename_profile(self, source_name: str, new_name: str) -> dict[str, Any]:
        source = self.profile_store.load_profile(source_name)
        if source is None:
            raise ValueError(f"profile not found: {source_name}")
        target_name = str(new_name or "").strip()
        if not target_name:
            raise ValueError("profile name is required")
        if target_name == source.name:
            return source.to_dict()
        if self.profile_store.load_profile(target_name) is not None:
            raise ValueError(f"profile already exists: {target_name}")
        was_default = bool(source.is_default)
        source.name = target_name
        source.is_default = was_default
        saved = self.profile_store.save_profile(source)
        self.profile_store.delete_profile(source_name)
        if was_default:
            saved = self.profile_store.set_default_profile(target_name)
        return self._decorate_profile_payload(saved.to_dict())

    def export_profile(self, name: str, destination: str | Path) -> str:
        profile = self.profile_store.load_profile(name)
        if profile is None:
            raise ValueError(f"profile not found: {name}")
        config_safety = self._config_safety_summary()
        config_safety_review = self._config_safety_review_payload(config_safety)
        config_governance_handoff = build_step2_config_governance_handoff(config_safety_review)
        path = Path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = profile.to_dict()
        payload["step2_config_governance"] = {
            "artifact_type": "plan_profile_step2_governance",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": "export_profile",
            **dict(config_governance_handoff),
            "config_safety": config_safety,
            "config_safety_review": config_safety_review,
            "config_governance_handoff": config_governance_handoff,
            "inventory_summary": str(config_safety_review.get("inventory_summary") or "--"),
            "boundary_note": "仅供 Step 2 simulation/offline/headless 入口治理与库存治理参考，不代表 real acceptance evidence。",
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return str(path)

    def import_profile(self, source: str | Path, *, set_default: Optional[bool] = None) -> dict[str, Any]:
        source_path = Path(source)
        source_payload = self._load_json_dict(source_path)
        imported_governance = self._imported_config_governance(source_payload)
        profile = self.profile_store.import_profile(source_path, set_default=set_default)
        decorated = self._decorate_profile_payload(profile.to_dict())
        decorated["import_governance_handoff"] = {
            "source_path": str(source_path),
            "has_imported_governance": bool(imported_governance),
            "imported_file_governance": imported_governance,
            "current_runtime_governance": dict(decorated.get("config_governance_handoff") or {}),
        }
        return decorated

    def validate_profile(
        self,
        profile_or_name: str | dict[str, Any] | CalibrationPlanProfile,
    ) -> dict[str, Any]:
        try:
            profile = self._resolve_profile(profile_or_name)
        except Exception as exc:
            message = f"配置结构无效：{exc}"
            return {
                "ok": False,
                "profile_name": "--",
                "profile_version": "--",
                "errors": [message],
                "warnings": [],
                "summary": message,
            }

        errors: list[str] = []
        warnings: list[str] = []
        enabled_temps = [item for item in list(profile.temperatures or []) if bool(getattr(item, "enabled", True))]
        enabled_humidity = [item for item in list(profile.humidities or []) if bool(getattr(item, "enabled", True))]
        enabled_gas = [item for item in list(profile.gas_points or []) if bool(getattr(item, "enabled", True))]
        enabled_pressure = [item for item in list(profile.pressures or []) if bool(getattr(item, "enabled", True))]
        run_mode = str(getattr(profile.mode_profile, "run_mode", "auto_calibration").value)
        analyzer_setup = getattr(profile, "analyzer_setup", None)
        manual_ids = list(getattr(analyzer_setup, "manual_device_ids", []) or [])
        duplicate_manual_ids = self._duplicate_labels(manual_ids)
        config_safety = self._config_safety_summary()
        selected_pressure_points = list(getattr(profile.ordering, "selected_pressure_points", []) or [])
        ambient_selected = any(str(item) == "ambient" for item in selected_pressure_points)

        if not str(profile.name or "").strip():
            errors.append("配置名称不能为空。")
        if any(ch.isspace() for ch in str(profile.profile_version or "")):
            errors.append("配置版本不能包含空白字符。")
        if not enabled_temps:
            errors.append("至少需要一个启用的温度点。")
        if run_mode == "co2_measurement" and not enabled_gas:
            errors.append("CO2 测量模式至少需要一个启用的 CO2 点。")
        if run_mode == "h2o_measurement" and not enabled_humidity:
            errors.append("H2O 测量模式至少需要一个启用的湿度点。")
        if not enabled_pressure and not ambient_selected:
            warnings.append("未配置压力点，将沿用运行时默认压力。")
        if str(getattr(analyzer_setup, "device_id_assignment_mode", "") or "") == "manual" and not manual_ids:
            warnings.append("手动设备编号模式未提供设备编号列表。")
        if duplicate_manual_ids:
            warnings.append(f"手动设备编号存在重复值：{', '.join(duplicate_manual_ids)}。")

        duplicate_temps = self._duplicate_labels(f"{float(getattr(item, 'temperature_c', 0.0)):g}C" for item in enabled_temps)
        duplicate_humidity = self._duplicate_labels(
            f"{float(getattr(item, 'hgen_temp_c', 0.0)):g}C/{float(getattr(item, 'hgen_rh_pct', 0.0)):g}%RH"
            for item in enabled_humidity
        )
        duplicate_gas = self._duplicate_labels(
            f"{float(getattr(item, 'co2_ppm', 0.0)):g}ppm/{str(getattr(item, 'co2_group', 'A') or 'A').strip().upper() or 'A'}"
            for item in enabled_gas
        )
        duplicate_pressures = self._duplicate_labels(
            pressure_target_label(
                pressure_hpa=getattr(item, "pressure_hpa", None),
                pressure_mode=getattr(item, "pressure_mode", ""),
                pressure_selection_token=getattr(item, "pressure_selection_token", ""),
                explicit_label=getattr(item, "pressure_target_label", None),
            )
            for item in enabled_pressure
        )
        if duplicate_temps:
            warnings.append(f"温度点存在重复值：{', '.join(duplicate_temps)}。")
        if duplicate_humidity:
            warnings.append(f"湿度点存在重复值：{', '.join(duplicate_humidity)}。")
        if duplicate_gas:
            warnings.append(f"CO2 点存在重复值：{', '.join(duplicate_gas)}。")
        if duplicate_pressures:
            warnings.append(f"压力点存在重复值：{', '.join(duplicate_pressures)}。")
        for warning in list(config_safety.get("warnings") or []):
            if warning not in warnings:
                warnings.append(str(warning))

        if errors:
            summary = f"配置校验失败，共 {len(errors)} 项错误。"
        elif warnings:
            summary = f"配置校验通过，但有 {len(warnings)} 项提醒。"
        else:
            summary = "配置校验通过，可用于仿真/离线编译。"
        return {
            "ok": not errors,
            "profile_name": profile.name,
            "profile_version": str(profile.profile_version or "1.0"),
            "run_mode": run_mode,
            "errors": errors,
            "warnings": warnings,
            "summary": summary,
            "config_safety": config_safety,
            "counts": {
                "temperatures": len(enabled_temps),
                "humidities": len(enabled_humidity),
                "gas_points": len(enabled_gas),
                "pressures": len(enabled_pressure),
            },
        }

    def diff_profiles(
        self,
        baseline: str | dict[str, Any] | CalibrationPlanProfile,
        candidate: str | dict[str, Any] | CalibrationPlanProfile,
    ) -> dict[str, Any]:
        baseline_profile = self._resolve_profile(baseline)
        candidate_profile = self._resolve_profile(candidate)
        before = baseline_profile.to_dict()
        after = candidate_profile.to_dict()
        changes = self._diff_payload(before, after)
        return {
            "baseline_profile": baseline_profile.name,
            "candidate_profile": candidate_profile.name,
            "change_count": len(changes),
            "changes": changes,
            "summary": (
                f"配置差异 {len(changes)} 项："
                f"{baseline_profile.name or '--'} -> {candidate_profile.name or '--'}"
            ),
        }

    def list_simulation_profile_library(self) -> list[dict[str, Any]]:
        return list(self._simulation_profile_library())

    def build_operator_safe_default_profile(self) -> dict[str, Any]:
        library = self._simulation_profile_library()
        if not library:
            return self.create_empty_profile(name="simulation_operator_safe")
        return dict(library[0].get("profile") or {})

    def build_runtime_snapshot(
        self,
        profile_or_name: str | dict[str, Any] | CalibrationPlanProfile,
    ) -> dict[str, Any]:
        profile = self._resolve_profile(profile_or_name)
        compiled = PlanCompiler(self._config_provider()).compile(profile)
        validation = self.validate_profile(profile)
        return self._runtime_snapshot_from_compiled(profile, compiled, validation=validation)

    def compile_profile_preview(
        self,
        profile_or_name: str | dict[str, Any] | CalibrationPlanProfile,
    ) -> dict[str, Any]:
        profile = self._resolve_profile(profile_or_name)
        compiled = PlanCompiler(self._config_provider()).compile(profile)
        validation = self.validate_profile(profile)
        config_safety = dict(validation.get("config_safety", {}) or {})
        return {
            "ok": True,
            "profile_name": profile.name,
            "profile_version": str(compiled.metadata.get("profile_version", "1.0") or "1.0"),
            "run_mode": compiled.metadata.get("run_mode", "auto_calibration"),
            "route_mode": compiled.metadata.get("route_mode", "h2o_then_co2"),
            "formal_calibration_report": bool(compiled.metadata.get("formal_calibration_report", True)),
            "report_family": compiled.metadata.get("report_family", ""),
            "report_templates": dict(compiled.metadata.get("report_templates") or {}),
            "analyzer_setup": dict(compiled.metadata.get("analyzer_setup") or {}),
            "summary": self._summary_text(compiled),
            "rows": [self._preview_row(row) for row in compiled.preview_rows()],
            "metadata": dict(compiled.metadata),
            "runtime_payload": compiled.to_runtime_payload(),
            "validation": validation,
            "config_safety": config_safety,
            "config_safety_review": self._config_safety_review_payload(config_safety),
        }

    def compile_preview(self, payload: dict[str, Any] | CalibrationPlanProfile) -> dict[str, Any]:
        return self.compile_profile_preview(payload)

    def compile_default_profile_preview(self) -> dict[str, Any]:
        return self.compile_profile_preview(self._get_default_profile())

    def build_runtime_points_file(
        self,
        profile_or_name: str | dict[str, Any] | CalibrationPlanProfile,
        *,
        destination: str | Path | None = None,
    ) -> dict[str, Any]:
        profile = self._resolve_profile(profile_or_name)
        compiled = PlanCompiler(self._config_provider()).compile(profile)
        target = Path(destination) if destination is not None else self._default_runtime_points_path(profile.name)
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = compiled.to_runtime_payload()
        target.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        validation = self.validate_profile(profile)
        runtime_snapshot = self._runtime_snapshot_from_compiled(profile, compiled, validation=validation)
        snapshot_path = target.with_name(f"{target.stem}.runtime_snapshot.json")
        audit_path = target.with_name(f"{target.stem}.audit_trail.json")
        snapshot_path.write_text(json.dumps(runtime_snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        audit_path.write_text(
            json.dumps(
                {
                    "artifact_type": "plan_compile_audit_trail",
                    "generated_at": runtime_snapshot.get("generated_at"),
                    "profile_name": runtime_snapshot.get("profile_name"),
                    "profile_version": runtime_snapshot.get("profile_version"),
                    "steps": list(runtime_snapshot.get("audit_trail") or []),
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        return {
            "profile_name": profile.name,
            "profile_version": str(compiled.metadata.get("profile_version", "1.0") or "1.0"),
            "path": str(target),
            "runtime_snapshot_path": str(snapshot_path),
            "audit_trail_path": str(audit_path),
            "run_mode": compiled.metadata.get("run_mode", "auto_calibration"),
            "route_mode": compiled.metadata.get("route_mode", "h2o_then_co2"),
            "formal_calibration_report": bool(compiled.metadata.get("formal_calibration_report", True)),
            "report_family": compiled.metadata.get("report_family", ""),
            "report_templates": dict(compiled.metadata.get("report_templates") or {}),
            "analyzer_setup": dict(compiled.metadata.get("analyzer_setup") or {}),
            "summary": self._summary_text(compiled),
            "metadata": dict(compiled.metadata),
            "runtime_payload": payload,
            "validation": validation,
            "config_safety": dict(runtime_snapshot.get("config_safety", {}) or {}),
            "runtime_snapshot": runtime_snapshot,
            "audit_trail": list(runtime_snapshot.get("audit_trail") or []),
        }

    def build_default_runtime_points_file(
        self,
        *,
        destination: str | Path | None = None,
    ) -> dict[str, Any]:
        return self.build_runtime_points_file(self._get_default_profile(), destination=destination)

    def _resolve_profile(
        self,
        profile_or_name: str | dict[str, Any] | CalibrationPlanProfile,
    ) -> CalibrationPlanProfile:
        if isinstance(profile_or_name, str):
            loaded = self.profile_store.load_profile(profile_or_name)
            if loaded is None:
                raise ValueError(f"profile not found: {profile_or_name}")
            return loaded
        return self._coerce_profile(profile_or_name)

    @staticmethod
    def _coerce_profile(payload: dict[str, Any] | CalibrationPlanProfile) -> CalibrationPlanProfile:
        if isinstance(payload, CalibrationPlanProfile):
            return deepcopy(payload)
        return CalibrationPlanProfile.from_dict(payload)

    def _get_default_profile(self) -> CalibrationPlanProfile:
        profile = self.profile_store.get_default_profile()
        if profile is None:
            raise ValueError("no default calibration profile configured")
        return profile

    def _default_runtime_points_path(self, profile_name: str) -> Path:
        if self.compiled_points_dir is None:
            raise ValueError("compiled points directory is not configured")
        safe_name = "".join(ch if ch.isalnum() or ch in "._-" else "-" for ch in str(profile_name or "").strip()).strip("._-")
        filename = f"default-profile-{safe_name or 'profile'}.json"
        return self.compiled_points_dir / filename

    def _runtime_snapshot_from_compiled(
        self,
        profile: CalibrationPlanProfile,
        compiled: CompiledPlan,
        *,
        validation: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        resolved_validation = dict(validation or self.validate_profile(profile))
        config_safety = hydrate_step2_config_safety_summary(
            dict(resolved_validation.get("config_safety") or self._config_safety_summary())
        )
        config_safety_review = self._config_safety_review_payload(config_safety)
        config_governance_handoff = build_step2_config_governance_handoff(config_safety_review)
        execution_gate = dict(config_safety.get("execution_gate") or {})
        gate_status = str(execution_gate.get("status") or "open")
        if gate_status == "blocked":
            config_safety_audit_status = "blocked"
        elif gate_status in {"unlocked_override", "warn"} or str(config_safety.get("status") or "ok") != "ok":
            config_safety_audit_status = "warn"
        else:
            config_safety_audit_status = "ok"
        route_breakdown: dict[str, int] = {}
        temperature_breakdown: dict[str, int] = {}
        pressure_breakdown: dict[str, int] = {}
        for row in list(compiled.runtime_rows or []):
            route = str(row.get("route") or "--")
            route_breakdown[route] = route_breakdown.get(route, 0) + 1
            temp = str(row.get("temperature") if row.get("temperature") is not None else "--")
            temperature_breakdown[temp] = temperature_breakdown.get(temp, 0) + 1
            pressure = str(
                pressure_target_label(
                    pressure_hpa=row.get("pressure_hpa"),
                    pressure_mode=row.get("pressure_mode"),
                    pressure_selection_token=row.get("pressure_selection_token"),
                    explicit_label=row.get("pressure_target_label"),
                )
                or "--"
            )
            pressure_breakdown[pressure] = pressure_breakdown.get(pressure, 0) + 1
        generated_at = datetime.now(timezone.utc).isoformat()
        run_mode = str(compiled.metadata.get("run_mode", "auto_calibration") or "auto_calibration")
        return {
            "artifact_type": "plan_runtime_snapshot",
            "generated_at": generated_at,
            "profile_name": profile.name,
            "profile_version": str(compiled.metadata.get("profile_version", "1.0") or "1.0"),
            "simulation_only": bool(getattr(self._config_provider().features, "simulation_mode", False)),
            "run_mode": run_mode,
            "route_mode": str(compiled.metadata.get("route_mode", "h2o_then_co2") or "h2o_then_co2"),
            "summary": (
                f"运行快照：profile={profile.name or '--'} | mode={run_mode} | "
                f"runtime={len(compiled.runtime_rows)} | preview={len(compiled.preview_points)}"
            ),
            "counts": {
                "source_rows": len(compiled.source_rows),
                "runtime_rows": len(compiled.runtime_rows),
                "prepared_points": len(compiled.points),
                "preview_points": len(compiled.preview_points),
            },
            "route_breakdown": route_breakdown,
            "temperature_breakdown": temperature_breakdown,
            "pressure_breakdown": pressure_breakdown,
            "metadata": dict(compiled.metadata),
            "analyzer_setup": dict(compiled.metadata.get("analyzer_setup") or {}),
            "validation": resolved_validation,
            "config_safety": config_safety,
            "config_safety_review": config_safety_review,
            "config_governance_handoff": config_governance_handoff,
            "audit_trail": [
                {
                    "step": "validate_profile",
                    "status": "ok" if bool(resolved_validation.get("ok", True)) else "warn",
                    "summary": str(resolved_validation.get("summary") or "配置校验通过"),
                },
                {
                    "step": "config_safety_review",
                    "status": config_safety_audit_status,
                    "summary": str(config_safety_review.get("summary") or "--"),
                    "classification": str(config_safety_review.get("classification") or ""),
                    "badge_ids": list(config_safety_review.get("badge_ids") or []),
                    "inventory_summary": str(config_safety_review.get("inventory_summary") or "--"),
                    "blocked_reasons": list(config_safety_review.get("blocked_reasons") or []),
                    "warnings": [str(item) for item in list(config_safety_review.get("warnings") or []) if str(item).strip()],
                    "real_port_device_count": int(config_safety_review.get("real_port_device_count", 0) or 0),
                    "engineering_only_flag_count": int(
                        config_safety_review.get("engineering_only_flag_count", 0) or 0
                    ),
                    "devices_with_real_ports": [
                        dict(item) for item in list(config_safety_review.get("devices_with_real_ports") or [])
                    ],
                    "enabled_engineering_flags": [
                        dict(item) for item in list(config_safety_review.get("enabled_engineering_flags") or [])
                    ],
                    "execution_gate": dict(config_safety_review.get("execution_gate") or {}),
                    "blocked_reason_details": [
                        dict(item) for item in list(config_safety_review.get("blocked_reason_details") or [])
                    ],
                    "requires_explicit_unlock": bool(config_safety_review.get("requires_explicit_unlock", False)),
                    "step2_default_workflow_allowed": bool(
                        config_safety_review.get("step2_default_workflow_allowed", True)
                    ),
                },
                {
                    "step": "compile_profile",
                    "status": "ok",
                    "summary": self._summary_text(compiled),
                },
            ],
        }

    @staticmethod
    def _summary_text(compiled: CompiledPlan) -> str:
        metadata = dict(compiled.metadata)
        return (
            f"profile={compiled.profile_name or '--'} | "
            f"version={metadata.get('profile_version', '1.0')} | "
            f"mode={metadata.get('run_mode', 'auto_calibration')} | "
            f"source={metadata.get('source_row_count', 0)} | "
            f"runtime={metadata.get('runtime_row_count', 0)} | "
            f"prepared={metadata.get('prepared_point_count', 0)} | "
            f"preview={metadata.get('preview_point_count', 0)}"
        )

    @staticmethod
    def _preview_row(row: dict[str, Any]) -> dict[str, str]:
        is_h2o = str(row.get("route", "")).strip().lower() == "h2o"
        hgen_temp = row.get("humidity_generator_temp_c")
        hgen_rh = row.get("humidity_pct")
        if hgen_temp is not None or hgen_rh is not None:
            temp_text = "--" if hgen_temp is None else f"{float(hgen_temp):g}C"
            rh_text = "--" if hgen_rh is None else f"{float(hgen_rh):g}%RH"
            hgen_text = f"{temp_text} / {rh_text}"
        else:
            hgen_text = "--"
        co2_text = "--" if is_h2o or row.get("co2_ppm") is None else f"{float(row['co2_ppm']):g}ppm"
        pressure_text = str(
            pressure_target_label(
                pressure_hpa=row.get("pressure_hpa"),
                pressure_mode=row.get("pressure_mode"),
                pressure_selection_token=row.get("pressure_selection_token"),
                explicit_label=row.get("pressure_target_label"),
            )
            or "--"
        )
        cylinder_nominal = row.get("cylinder_nominal_ppm")
        cylinder_text = "--" if cylinder_nominal is None else f"{float(cylinder_nominal):g}ppm"
        return {
            "seq": str(row.get("sequence", "")),
            "row": str(row.get("index", "")),
            "temp": f"{float(row.get('temperature_c', 0.0)):g}C",
            "route": "H2O" if is_h2o else "CO2",
            "hgen": hgen_text,
            "co2": co2_text,
            "pressure": pressure_text,
            "group": "--" if is_h2o else str(row.get("co2_group", "") or "--"),
            "cylinder": "--" if is_h2o else cylinder_text,
            "status": "compiled",
        }

    @staticmethod
    def _duplicate_labels(values: Any) -> list[str]:
        counts: dict[str, int] = {}
        for item in list(values or []):
            text = str(item or "").strip()
            if not text:
                continue
            counts[text] = counts.get(text, 0) + 1
        return [label for label, count in sorted(counts.items()) if count > 1]

    def _config_safety_summary(self) -> dict[str, Any]:
        config = self._config_provider()
        return summarize_step2_config_safety(
            config,
            unsafe_config_cli_flag="--allow-unsafe-step2-config",
            unsafe_config_env_var="GAS_CALIBRATOR_V2_ALLOW_UNSAFE_CONFIG",
        )

    @staticmethod
    def _config_safety_review_payload(config_safety: dict[str, Any] | None) -> dict[str, Any]:
        return build_step2_config_safety_review(config_safety)

    def _decorate_profile_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        decorated = dict(payload or {})
        config_safety = self._config_safety_summary()
        config_safety_review = self._config_safety_review_payload(config_safety)
        config_governance_handoff = build_step2_config_governance_handoff(config_safety_review)
        decorated["config_safety"] = config_safety
        decorated["config_safety_review"] = config_safety_review
        decorated["config_governance_handoff"] = config_governance_handoff
        decorated["config_classification"] = str(config_safety.get("classification") or "")
        decorated["config_badge_ids"] = list(config_safety.get("badge_ids") or [])
        decorated["config_inventory_summary"] = str(config_safety_review.get("inventory_summary") or "--")
        return decorated

    def _decorate_profile_summary(self, payload: dict[str, Any]) -> dict[str, Any]:
        decorated = dict(payload or {})
        config_safety = self._config_safety_summary()
        config_safety_review = self._config_safety_review_payload(config_safety)
        config_governance_handoff = build_step2_config_governance_handoff(config_safety_review)
        decorated["config_safety"] = config_safety
        decorated["config_safety_review"] = config_safety_review
        decorated["config_governance_handoff"] = config_governance_handoff
        decorated["config_classification"] = str(config_safety.get("classification") or "")
        decorated["config_badge_ids"] = list(config_safety.get("badge_ids") or [])
        decorated["config_inventory_summary"] = str(config_safety_review.get("inventory_summary") or "--")
        return decorated

    @staticmethod
    def _load_json_dict(path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return dict(payload) if isinstance(payload, dict) else {}

    @staticmethod
    def _imported_config_governance(payload: dict[str, Any] | None) -> dict[str, Any]:
        source_payload = dict(payload or {})
        imported = dict(source_payload.get("step2_config_governance") or {})
        handoff = dict(imported.get("config_governance_handoff") or {})
        if handoff:
            return handoff
        review = dict(imported.get("config_safety_review") or {})
        if review:
            return build_step2_config_governance_handoff(review)
        summary = dict(imported.get("config_safety") or {})
        if summary:
            return build_step2_config_governance_handoff(summary)
        return {}

    @classmethod
    def _enabled_engineering_only_flags(cls, config: AppConfig) -> list[dict[str, Any]]:
        workflow = getattr(config, "workflow", None)
        pressure = dict(getattr(workflow, "pressure", {}) or {})
        enabled_flags: list[dict[str, Any]] = []
        for flag_name, config_path, label in cls._engineering_only_pressure_flag_specs():
            if not bool(pressure.get(flag_name, False)):
                continue
            enabled_flags.append(
                {
                    "flag": flag_name,
                    "config_path": config_path,
                    "label": label,
                    "category": "engineering_only",
                    "default_enabled": False,
                }
            )
        return enabled_flags

    @staticmethod
    def _engineering_only_pressure_flag_specs() -> tuple[tuple[str, str, str], ...]:
        return (
            (
                "capture_then_hold_enabled",
                "workflow.pressure.capture_then_hold_enabled",
                "capture_then_hold",
            ),
            (
                "adaptive_pressure_sampling_enabled",
                "workflow.pressure.adaptive_pressure_sampling_enabled",
                "adaptive_pressure_sampling",
            ),
            (
                "soft_control_enabled",
                "workflow.pressure.soft_control_enabled",
                "soft_control",
            ),
        )

    @staticmethod
    def _iter_device_ports(config: AppConfig) -> list[tuple[str, str]]:
        devices = getattr(config, "devices", None)
        if devices is None:
            return []
        rows: list[tuple[str, str]] = []
        for name in (
            "pressure_controller",
            "pressure_meter",
            "dewpoint_meter",
            "humidity_generator",
            "temperature_chamber",
            "thermometer",
            "relay_a",
            "relay_b",
        ):
            payload = getattr(devices, name, None)
            if payload is None or not bool(getattr(payload, "enabled", True)):
                continue
            rows.append((name, str(getattr(payload, "port", "") or "").strip()))
        for index, payload in enumerate(list(getattr(devices, "gas_analyzers", []) or [])):
            if payload is None or not bool(getattr(payload, "enabled", True)):
                continue
            device_name = str(getattr(payload, "name", "") or "").strip() or f"gas_analyzer_{index}"
            rows.append((device_name, str(getattr(payload, "port", "") or "").strip()))
        return rows

    @staticmethod
    def _port_requires_real_device_review(port: str) -> bool:
        normalized = str(port or "").strip().upper()
        if not normalized:
            return False
        if normalized.startswith("SIM-") or normalized in {"SIM", "SIMULATED", "REPLAY"}:
            return False
        if normalized.startswith("COM"):
            return True
        if normalized.startswith("/DEV/") or normalized.startswith("TTY") or "TTYUSB" in normalized or "TTYACM" in normalized:
            return True
        return False

    @classmethod
    def _simulation_profile_library(cls) -> list[dict[str, Any]]:
        return [
            cls._simulation_library_item(
                profile=CalibrationPlanProfile.from_dict(
                    {
                        "name": "simulation_operator_safe",
                        "profile_version": "sim-1.0",
                        "description": "默认仿真操作员安全预设",
                        "temperatures": [{"temperature_c": 25.0}],
                        "humidities": [{"hgen_temp_c": 25.0, "hgen_rh_pct": 40.0}],
                        "gas_points": [{"co2_ppm": 400.0, "co2_group": "A", "cylinder_nominal_ppm": 405.0}],
                        "pressures": [{"pressure_hpa": 1000.0}],
                    }
                ),
                library_id="operator_safe_default",
                title="默认仿真预设",
                description="推荐的 simulation-only 操作员安全默认预设。",
                recommended=True,
            ),
            cls._simulation_library_item(
                profile=CalibrationPlanProfile.from_dict(
                    {
                        "name": "simulation_co2_quickcheck",
                        "profile_version": "sim-1.0",
                        "description": "CO2 仿真快速检查预设",
                        "run_mode": "co2_measurement",
                        "temperatures": [{"temperature_c": 25.0}],
                        "gas_points": [
                            {"co2_ppm": 400.0, "co2_group": "A", "cylinder_nominal_ppm": 405.0},
                            {"co2_ppm": 800.0, "co2_group": "B", "cylinder_nominal_ppm": 810.0},
                        ],
                        "pressures": [{"pressure_hpa": 1000.0}],
                    }
                ),
                library_id="co2_quickcheck",
                title="CO2 快检预设",
                description="用于 CO2 simulation/replay 快速回归。",
            ),
            cls._simulation_library_item(
                profile=CalibrationPlanProfile.from_dict(
                    {
                        "name": "simulation_h2o_quickcheck",
                        "profile_version": "sim-1.0",
                        "description": "H2O 仿真快速检查预设",
                        "run_mode": "h2o_measurement",
                        "temperatures": [{"temperature_c": 25.0}],
                        "humidities": [
                            {"hgen_temp_c": 25.0, "hgen_rh_pct": 40.0},
                            {"hgen_temp_c": 25.0, "hgen_rh_pct": 70.0},
                        ],
                        "pressures": [{"pressure_hpa": 1000.0}],
                    }
                ),
                library_id="h2o_quickcheck",
                title="H2O 快检预设",
                description="用于 H2O simulation/replay 快速回归。",
            ),
        ]

    @staticmethod
    def _simulation_library_item(
        *,
        profile: CalibrationPlanProfile,
        library_id: str,
        title: str,
        description: str,
        recommended: bool = False,
    ) -> dict[str, Any]:
        normalized_profile = profile if isinstance(profile, CalibrationPlanProfile) else CalibrationPlanProfile.from_dict(profile)
        payload = normalized_profile.to_dict()
        return {
            "id": library_id,
            "title": title,
            "description": description,
            "recommended": bool(recommended),
            "simulation_only": True,
            "profile": payload,
        }

    @classmethod
    def _diff_payload(cls, before: dict[str, Any], after: dict[str, Any]) -> list[dict[str, Any]]:
        before_flat = cls._flatten_payload(before)
        after_flat = cls._flatten_payload(after)
        rows: list[dict[str, Any]] = []
        for key in sorted(set(before_flat) | set(after_flat)):
            old_value = before_flat.get(key)
            new_value = after_flat.get(key)
            if old_value == new_value:
                continue
            section = key.split(".", 1)[0] if "." in key else key.split("[", 1)[0]
            rows.append(
                {
                    "field": key,
                    "section": section,
                    "before": old_value,
                    "after": new_value,
                }
            )
        return rows

    @classmethod
    def _flatten_payload(cls, payload: Any, *, prefix: str = "") -> dict[str, Any]:
        flattened: dict[str, Any] = {}
        if isinstance(payload, dict):
            for key, value in sorted(payload.items()):
                child = f"{prefix}.{key}" if prefix else str(key)
                flattened.update(cls._flatten_payload(value, prefix=child))
            return flattened
        if isinstance(payload, list):
            for index, value in enumerate(payload):
                child = f"{prefix}[{index}]"
                flattened.update(cls._flatten_payload(value, prefix=child))
            return flattened
        flattened[prefix or "value"] = payload
        return flattened
