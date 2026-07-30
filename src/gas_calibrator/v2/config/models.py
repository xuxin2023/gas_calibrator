"""
配置数据模型

本模块使用 dataclass 定义类型安全的配置模型，替代原有的字典访问方式。
提供配置验证、默认值和便捷的访问方法。

使用示例：
    from gas_calibrator.v2.config.models import AppConfig

    # 从字典加载
    config = AppConfig.from_dict(raw_config_dict)

    # 类型安全的访问
    timeout = config.workflow.stability.temperature.timeout_s
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ...storage.settings import StorageConfig
from ...validation.simulation.pressure_selection import normalize_selected_pressure_points
from ..exceptions import ConfigurationMissingError
from ...validation.simulation.config import (
    AIConfig,
    AIFeaturesConfig as AIFeaturesConfig,
    CoefficientSummaryColumnConfig as CoefficientSummaryColumnConfig,
    CoefficientsConfig as CoefficientsConfig,
    FeaturesConfig,
    H2OSummarySelectionConfig as H2OSummarySelectionConfig,
    HumidityStabilityConfig as HumidityStabilityConfig,
    PathsConfig,
    PrecheckConfig as PrecheckConfig,
    PressureControlConfig as PressureControlConfig,
    PressureStabilityConfig as PressureStabilityConfig,
    QCConfig as QCConfig,
    QCRuleConfig as QCRuleConfig,
    SamplingConfig as SamplingConfig,
    SignalStabilityConfig as SignalStabilityConfig,
    StabilityConfig,
    STEP2_ENGINEERING_ONLY_PRESSURE_FLAG_SPECS as STEP2_ENGINEERING_ONLY_PRESSURE_FLAG_SPECS,
    TemperatureStabilityConfig as TemperatureStabilityConfig,
    ValveConfig as ValveConfig,
    _build_shared_pressure_flag_inventory as _build_shared_pressure_flag_inventory,
    _build_step2_blocked_reason_details as _build_step2_blocked_reason_details,
    _build_step2_config_safety_badges as _build_step2_config_safety_badges,
    _build_step2_config_safety_inventory as _build_step2_config_safety_inventory,
    _normalize_run_mode as _normalize_run_mode,
    _normalize_sensor_precheck_config as _normalize_sensor_precheck_config,
    _step2_config_safety_badge_spec as _step2_config_safety_badge_spec,
    _step2_config_safety_classification as _step2_config_safety_classification,
    _step2_config_safety_classification_display as _step2_config_safety_classification_display,
    build_step2_config_governance_handoff as build_step2_config_governance_handoff,
    build_step2_config_safety_review as build_step2_config_safety_review,
    enabled_engineering_only_flags as enabled_engineering_only_flags,
    hydrate_step2_config_safety_summary as hydrate_step2_config_safety_summary,
    iter_config_device_ports as iter_config_device_ports,
    port_requires_real_device_review as port_requires_real_device_review,
)


def _normalize_analyzer_mode2_init_config(value: Any) -> Dict[str, Any]:
    if value is None:
        payload: Dict[str, Any] = {"enabled": False}
    elif value is True:
        payload: Dict[str, Any] = {}
    elif value is False:
        payload = {"enabled": False}
    elif isinstance(value, dict) and not value:
        payload = {"enabled": False}
    elif isinstance(value, dict):
        payload = dict(value)
    else:
        payload = {}

    numeric_defaults: Dict[str, tuple[type, float | int]] = {
        "reapply_attempts": (int, 4),
        "stream_attempts": (int, 10),
        "passive_attempts": (int, 4),
        "retry_delay_s": (float, 0.2),
        "reapply_delay_s": (float, 0.35),
        "command_gap_s": (float, 0.15),
        "post_enable_stream_wait_s": (float, 2.0),
        "post_enable_stream_ack_wait_s": (float, 8.0),
    }
    normalized = dict(payload)
    normalized["enabled"] = bool(payload.get("enabled", True))
    for key, (coerce, default) in numeric_defaults.items():
        raw = payload.get(key, default)
        try:
            normalized[key] = coerce(raw)
        except Exception:
            normalized[key] = coerce(default)
    return normalized


def _normalize_analyzer_setup_config(value: Any) -> Dict[str, Any]:
    payload = dict(value or {}) if isinstance(value, dict) else {}
    software_version = str(
        payload.get("software_version", payload.get("analyzer_version", "v5_plus"))
    ).strip().lower()
    if software_version in {"pre-v5", "pre_v5", "legacy", "v4"}:
        software_version = "pre_v5"
    else:
        software_version = "v5_plus"

    assignment_mode = str(
        payload.get("device_id_assignment_mode", payload.get("id_assignment_mode", "automatic"))
    ).strip().lower()
    if assignment_mode in {"manual", "manual_list", "fixed"}:
        assignment_mode = "manual"
    else:
        assignment_mode = "automatic"

    start_device_id = str(
        payload.get("start_device_id", payload.get("starting_device_id", "001"))
    ).strip()
    if start_device_id.isdigit():
        start_device_id = f"{int(start_device_id):03d}"
    elif not start_device_id:
        start_device_id = "001"

    raw_manual_ids = payload.get("manual_device_ids", payload.get("device_ids", payload.get("manual_ids", [])))
    if isinstance(raw_manual_ids, str):
        manual_tokens = raw_manual_ids.replace(";", ",").replace("\n", ",").split(",")
    else:
        manual_tokens = list(raw_manual_ids or [])

    manual_device_ids: List[str] = []
    for item in manual_tokens:
        text = str(item or "").strip()
        if not text:
            continue
        if text.isdigit():
            text = f"{int(text):03d}"
        else:
            text = text.upper()
        manual_device_ids.append(text)

    raw_apply_device_id = payload.get(
        "apply_device_id",
        payload.get("write_device_id", payload.get("enable_device_id_assignment", True)),
    )
    if isinstance(raw_apply_device_id, bool):
        apply_device_id = raw_apply_device_id
    elif raw_apply_device_id is None:
        apply_device_id = True
    else:
        apply_device_id = str(raw_apply_device_id).strip().lower() not in {"0", "false", "no", "off"}

    return {
        "software_version": software_version,
        "device_id_assignment_mode": assignment_mode,
        "start_device_id": start_device_id,
        "manual_device_ids": manual_device_ids,
        "apply_device_id": apply_device_id,
    }


# =============================================================================
# 工作流配置
# =============================================================================

@dataclass
class WorkflowConfig:
    """工作流配置"""
    missing_pressure_policy: str = "require"  # 缺失压力策略: require, skip, warn, carry_forward
    run_mode: str = "auto_calibration"
    profile_name: Optional[str] = None
    profile_version: Optional[str] = None
    analyzer_mode2_init: Dict[str, Any] = field(default_factory=dict)
    analyzer_setup: Dict[str, Any] = field(default_factory=dict)
    collect_only: bool = False
    collect_only_fast_path: bool = True
    route_mode: str = "h2o_then_co2"
    report_family: Optional[str] = None
    report_templates: Dict[str, Any] = field(default_factory=dict)
    skip_co2_ppm: List[int] = field(default_factory=list)
    selected_temps_c: List[float] = field(default_factory=list)
    selected_pressure_points: List[Any] = field(default_factory=list)
    temperature_descending: bool = True
    h2o_carry_forward: bool = False
    restore_baseline_on_finish: bool = True
    water_first_all_temps: bool = False
    water_first_temp_gte: Optional[float] = None
    pressure: Dict[str, Any] = field(default_factory=dict)
    humidity_generator: Dict[str, Any] = field(default_factory=dict)
    analyzer_live_snapshot: Dict[str, Any] = field(default_factory=dict)
    sensor_precheck: Dict[str, Any] = field(default_factory=dict)
    startup_pressure_precheck: Dict[str, Any] = field(default_factory=dict)
    sensor_read_retry: Dict[str, Any] = field(default_factory=dict)
    analyzer_reprobe: Dict[str, Any] = field(default_factory=dict)
    summary_alignment: Dict[str, Any] = field(default_factory=dict)
    reporting: Dict[str, Any] = field(default_factory=dict)
    stability: StabilityConfig = field(default_factory=StabilityConfig)
    sampling: SamplingConfig = field(default_factory=SamplingConfig)
    pressure_control: PressureControlConfig = field(default_factory=PressureControlConfig)
    precheck: PrecheckConfig = field(default_factory=PrecheckConfig)

    @classmethod
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> "WorkflowConfig":
        if not d:
            return cls()
        return cls(
            missing_pressure_policy=d.get("missing_pressure_policy", "require"),
            run_mode=_normalize_run_mode(d.get("run_mode")),
            profile_name=(str(d.get("profile_name")).strip() if d.get("profile_name") not in (None, "") else None),
            profile_version=(str(d.get("profile_version")).strip() if d.get("profile_version") not in (None, "") else None),
            analyzer_mode2_init=_normalize_analyzer_mode2_init_config(d.get("analyzer_mode2_init")),
            analyzer_setup=_normalize_analyzer_setup_config(d.get("analyzer_setup")),
            collect_only=bool(d.get("collect_only", False)),
            collect_only_fast_path=bool(d.get("collect_only_fast_path", True)),
            route_mode=str(d.get("route_mode", "h2o_then_co2")),
            report_family=(str(d.get("report_family")).strip() if d.get("report_family") not in (None, "") else None),
            report_templates=dict(d.get("report_templates", {})),
            skip_co2_ppm=[int(value) for value in d.get("skip_co2_ppm", [])],
            selected_temps_c=[float(value) for value in d.get("selected_temps_c", [])],
            selected_pressure_points=list(
                normalize_selected_pressure_points(
                    d.get("selected_pressure_points", d.get("selected_pressures", []))
                )
            ),
            temperature_descending=bool(d.get("temperature_descending", True)),
            h2o_carry_forward=bool(d.get("h2o_carry_forward", False)),
            restore_baseline_on_finish=bool(d.get("restore_baseline_on_finish", True)),
            water_first_all_temps=bool(d.get("water_first_all_temps", False)),
            water_first_temp_gte=None if d.get("water_first_temp_gte") is None else float(d.get("water_first_temp_gte")),
            pressure=dict(d.get("pressure", {})),
            humidity_generator=dict(d.get("humidity_generator", {})),
            analyzer_live_snapshot=dict(d.get("analyzer_live_snapshot", {})),
            sensor_precheck=_normalize_sensor_precheck_config(d.get("sensor_precheck", {})),
            startup_pressure_precheck=dict(d.get("startup_pressure_precheck", {})),
            sensor_read_retry=dict(d.get("sensor_read_retry", {})),
            analyzer_reprobe=dict(d.get("analyzer_reprobe", {})),
            summary_alignment=dict(d.get("summary_alignment", {})),
            reporting=dict(d.get("reporting", {})),
            stability=StabilityConfig.from_dict(d.get("stability")),
            sampling=SamplingConfig.from_dict(d.get("sampling")),
            pressure_control=PressureControlConfig.from_dict(d.get("pressure_control")),
            precheck=PrecheckConfig.from_dict(d.get("precheck")),
        )


# =============================================================================
# 设备配置
# =============================================================================

@dataclass
class SingleDeviceConfig:
    """单个设备配置"""
    port: str                           # 串口
    baud: int = 9600                    # 波特率
    enabled: bool = True                # 是否启用
    timeout: float = 1.0                # 超时时间 (秒)
    description: str = ""               # 设备描述
    name: str = ""
    line_ending: str = ""
    query_line_endings: List[str] = field(default_factory=list)
    pressure_queries: List[str] = field(default_factory=list)
    response_timeout_s: Optional[float] = None
    dest_id: str = ""
    station: str = ""
    addr: Optional[int] = None
    bytesize: Optional[int] = None
    parity: str = ""
    stopbits: Optional[float] = None
    device_id: str = ""
    mode: Optional[int] = None
    active_send: Optional[bool] = None
    ftd_hz: Optional[int] = None
    average_filter: Optional[int] = None
    average_co2: Optional[int] = None
    average_h2o: Optional[int] = None

    @classmethod
    def from_dict(cls, name: str, d: Optional[Dict[str, Any]]) -> "SingleDeviceConfig":
        if not d:
            raise ConfigurationMissingError(f"devices.{name}")
        return cls(
            port=d.get("port", ""),
            baud=d.get("baud", 9600),
            enabled=d.get("enabled", True),
            timeout=d.get("timeout", 1.0),
            description=d.get("description", ""),
            name=str(d.get("name", name)),
            line_ending=str(d.get("line_ending", "")),
            query_line_endings=list(d.get("query_line_endings", [])),
            pressure_queries=list(d.get("pressure_queries", [])),
            response_timeout_s=None if d.get("response_timeout_s") is None else float(d.get("response_timeout_s")),
            dest_id=str(d.get("dest_id", "")),
            station=str(d.get("station", "")),
            addr=None if d.get("addr") is None else int(d.get("addr")),
            bytesize=None if d.get("bytesize") is None else int(d.get("bytesize")),
            parity=str(d.get("parity", "")),
            stopbits=None if d.get("stopbits") is None else float(d.get("stopbits")),
            device_id=str(d.get("device_id", "")),
            mode=None if d.get("mode") is None else int(d.get("mode")),
            active_send=None if d.get("active_send") is None else bool(d.get("active_send")),
            ftd_hz=None if d.get("ftd_hz") is None else int(d.get("ftd_hz")),
            average_filter=None if d.get("average_filter") is None else int(d.get("average_filter")),
            average_co2=None if d.get("average_co2") is None else int(d.get("average_co2")),
            average_h2o=None if d.get("average_h2o") is None else int(d.get("average_h2o")),
        )


@dataclass
class DeviceConfig:
    """设备配置集合"""
    pressure_controller: Optional[SingleDeviceConfig] = None
    pressure_meter: Optional[SingleDeviceConfig] = None
    dewpoint_meter: Optional[SingleDeviceConfig] = None
    humidity_generator: Optional[SingleDeviceConfig] = None
    temperature_chamber: Optional[SingleDeviceConfig] = None
    thermometer: Optional[SingleDeviceConfig] = None
    relay_a: Optional[SingleDeviceConfig] = None
    relay_b: Optional[SingleDeviceConfig] = None
    gas_analyzers: List[SingleDeviceConfig] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> "DeviceConfig":
        if not d:
            return cls()

        config = cls()

        # 压力控制器
        if "pressure_controller" in d:
            config.pressure_controller = SingleDeviceConfig.from_dict("pressure_controller", d["pressure_controller"])

        # 压力计
        pressure_meter_cfg = d.get("pressure_meter") or d.get("pressure_gauge")
        if isinstance(pressure_meter_cfg, dict):
            config.pressure_meter = SingleDeviceConfig.from_dict("pressure_meter", pressure_meter_cfg)

        # 露点仪
        if "dewpoint_meter" in d:
            config.dewpoint_meter = SingleDeviceConfig.from_dict("dewpoint_meter", d["dewpoint_meter"])

        # 湿度发生器
        if "humidity_generator" in d:
            config.humidity_generator = SingleDeviceConfig.from_dict("humidity_generator", d["humidity_generator"])

        # 温箱
        if "temperature_chamber" in d:
            config.temperature_chamber = SingleDeviceConfig.from_dict("temperature_chamber", d["temperature_chamber"])
        if "thermometer" in d:
            config.thermometer = SingleDeviceConfig.from_dict("thermometer", d["thermometer"])

        # 继电器
        if "relay_a" in d:
            config.relay_a = SingleDeviceConfig.from_dict("relay_a", d["relay_a"])
        if "relay_b" in d:
            config.relay_b = SingleDeviceConfig.from_dict("relay_b", d["relay_b"])
        if config.relay_a is None and "relay" in d:
            config.relay_a = SingleDeviceConfig.from_dict("relay", d["relay"])
        if config.relay_b is None and "relay_8" in d:
            config.relay_b = SingleDeviceConfig.from_dict("relay_8", d["relay_8"])

        # 气体分析仪（支持多个）
        if "gas_analyzers" in d:
            ga_list = d["gas_analyzers"]
            if isinstance(ga_list, list):
                for i, ga_config in enumerate(ga_list):
                    config.gas_analyzers.append(
                        SingleDeviceConfig.from_dict(f"gas_analyzer_{i}", ga_config)
                    )
        elif "gas_analyzer" in d and isinstance(d["gas_analyzer"], dict):
            config.gas_analyzers.append(SingleDeviceConfig.from_dict("gas_analyzer", d["gas_analyzer"]))

        return config


# =============================================================================
# 应用配置（顶层）
# =============================================================================

@dataclass
class AppConfig:
    """应用配置（顶层配置类）"""
    devices: DeviceConfig = field(default_factory=DeviceConfig)
    workflow: WorkflowConfig = field(default_factory=WorkflowConfig)
    valves: ValveConfig = field(default_factory=ValveConfig)
    coefficients: CoefficientsConfig = field(default_factory=CoefficientsConfig)
    paths: PathsConfig = field(default_factory=PathsConfig)
    features: FeaturesConfig = field(default_factory=FeaturesConfig)
    qc: QCConfig = field(default_factory=QCConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    ai: AIConfig = field(default_factory=AIConfig)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "AppConfig":
        """
        从字典创建配置对象

        Args:
            d: 配置字典（通常从 JSON 文件加载）

        Returns:
            AppConfig 实例
        """
        return cls(
            devices=DeviceConfig.from_dict(d.get("devices")),
            workflow=WorkflowConfig.from_dict(d.get("workflow")),
            valves=ValveConfig.from_dict(d.get("valves")),
            coefficients=CoefficientsConfig.from_dict(d.get("coefficients")),
            paths=PathsConfig.from_dict(d.get("paths")),
            features=FeaturesConfig.from_dict(d.get("features")),
            qc=QCConfig.from_dict(d.get("qc")),
            storage=StorageConfig.from_dict(d.get("storage")),
            ai=AIConfig.from_dict(d.get("ai")),
        )

    @classmethod
    def from_json_file(cls, path: str) -> "AppConfig":
        """
        从 JSON 文件加载配置

        Args:
            path: JSON 文件路径

        Returns:
            AppConfig 实例
        """
        import json
        from pathlib import Path

        file_path = Path(path)
        if not file_path.exists():
            raise ConfigurationMissingError(f"配置文件: {path}")

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        return cls.from_dict(data)

    def validate(self) -> List[str]:
        """
        验证配置

        Returns:
            错误信息列表，空列表表示验证通过
        """
        errors = []

        # 验证设备配置
        if self.devices.pressure_controller:
            if not self.devices.pressure_controller.port:
                errors.append("pressure_controller.port 未配置")

        # 验证工作流配置
        if self.workflow.missing_pressure_policy not in ("require", "skip", "warn", "carry_forward"):
            errors.append(f"无效的 missing_pressure_policy: {self.workflow.missing_pressure_policy}")

        # 验证稳定性配置
        if self.workflow.stability.temperature.tol <= 0:
            errors.append("temperature.tol 必须大于 0")

        return errors


def summarize_step2_config_safety(
    config: AppConfig,
    *,
    allow_unsafe_step2_config: bool = False,
    unsafe_config_env_enabled: bool = False,
    unsafe_config_cli_flag: str = "--allow-unsafe-step2-config",
    unsafe_config_env_var: str = "GAS_CALIBRATOR_V2_ALLOW_UNSAFE_CONFIG",
) -> dict[str, Any]:
    simulation_only = bool(getattr(getattr(config, "features", None), "simulation_mode", False))
    device_ports = [
        {
            "device": name,
            "port": port,
            "requires_real_device_review": port_requires_real_device_review(port),
            "classification": "real_port" if port_requires_real_device_review(port) else "simulated_port",
        }
        for name, port in iter_config_device_ports(config)
    ]
    real_ports = [
        {"device": str(item["device"]), "port": str(item["port"])}
        for item in device_ports
        if bool(item.get("requires_real_device_review", False))
    ]
    engineering_flags = enabled_engineering_only_flags(config)
    warnings: list[str] = []
    risk_markers: list[str] = []
    if not simulation_only:
        risk_markers.append("simulation_mode_disabled")
        warnings.append("当前配置未启用 simulation_mode；Step 2 仅允许 simulation/offline/headless 验证。")
    if real_ports:
        risk_markers.append("real_ports_detected")
        warnings.append(
            "检测到非仿真设备端口："
            + ", ".join(f"{item['device']}={item['port']}" for item in real_ports)
            + "。Step 2 默认工作流不接受 real-COM 配置。"
        )
    if engineering_flags:
        risk_markers.append("engineering_only_flags_enabled")
        warnings.append(
            "检测到 engineering-only 实验开关已启用："
            + ", ".join(str(item["config_path"]) for item in engineering_flags)
            + "。Step 2 默认工作流要求这些能力保持 non-default 且默认关闭。"
        )

    requires_explicit_unlock = bool(risk_markers)
    dual_unlock_ready = bool(allow_unsafe_step2_config and unsafe_config_env_enabled)
    if requires_explicit_unlock and not dual_unlock_ready:
        execution_gate_status = "blocked"
        execution_gate_summary = (
            "Step 2 默认工作流已拦截当前配置；如需进入仅限工程排查的 non-default 路径，"
            f"必须同时提供 {unsafe_config_cli_flag} 与 {unsafe_config_env_var}=1。"
        )
    elif requires_explicit_unlock:
        execution_gate_status = "unlocked_override"
        execution_gate_summary = (
            "当前配置包含 non-default Step 2 风险项，但已收到 CLI flag + 环境变量双重显式解锁；"
            "该解锁仅用于工程隔离排查，不代表允许真实设备联调或 real acceptance。"
        )
    else:
        execution_gate_status = "open"
        execution_gate_summary = "当前配置属于 Step 2 simulation-only 安全边界，可进入默认工作流。"

    if warnings:
        summary = f"配置安全提醒 {len(warnings)} 项：{warnings[0]}"
        status = "warn"
    else:
        summary = "当前配置为 simulation-only，未发现真实串口风险。"
        status = "ok"

    return hydrate_step2_config_safety_summary(
        {
        "status": status,
        "summary": summary,
        "simulation_only": simulation_only,
        "operator_safe": not warnings,
        "risk_markers": risk_markers,
        "real_port_device_count": len(real_ports),
        "devices_with_real_ports": real_ports,
        "engineering_only_flag_count": len(engineering_flags),
        "enabled_engineering_flags": engineering_flags,
        "warnings": warnings,
        "requires_explicit_unlock": requires_explicit_unlock,
        "step2_default_workflow_allowed": not requires_explicit_unlock or dual_unlock_ready,
        "inventory": _build_step2_config_safety_inventory(device_ports, engineering_flags),
        "execution_gate": {
            "status": execution_gate_status,
            "summary": execution_gate_summary,
            "requires_dual_unlock": requires_explicit_unlock,
            "allow_unsafe_step2_config_flag": bool(allow_unsafe_step2_config),
            "allow_unsafe_step2_config_env": bool(unsafe_config_env_enabled),
            "unlock_cli_flag": unsafe_config_cli_flag,
            "unlock_env_var": unsafe_config_env_var,
            "blocked_reasons": list(risk_markers),
        },
        }
    )
