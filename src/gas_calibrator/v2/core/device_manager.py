"""
设备管理器。

本模块提供 V2 设备生命周期管理、状态监控与简单故障恢复能力。
设备实例可由上层注入，管理器仅负责统一调度与状态维护。
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional

from ..config.models import DeviceConfig, SingleDeviceConfig
from ..utils import as_bool
from .device_factory import DeviceFactory, DeviceType


class DeviceStatus(Enum):
    """设备状态。"""

    UNKNOWN = "unknown"
    OFFLINE = "offline"
    ONLINE = "online"
    ERROR = "error"
    DISABLED = "disabled"


@dataclass
class DeviceInfo:
    """设备元信息与当前状态。"""

    name: str
    device_type: str
    port: str = ""
    status: DeviceStatus = DeviceStatus.UNKNOWN
    enabled: bool = True
    error_message: Optional[str] = None
    device: Any = None


class DeviceManager:
    """
    V2 设备管理器。

    管理器根据 `DeviceConfig` 建立设备注册表，并支持后续注入具体设备实例。
    这样既能适配真实硬件，也方便在测试中使用模拟设备。
    """

    def __init__(
        self,
        config: DeviceConfig,
        device_factory: Optional[DeviceFactory] = None,
    ) -> None:
        """
        初始化设备管理器。

        Args:
            config: V2 设备配置模型。
        """
        self.config = config
        self.device_factory = device_factory or DeviceFactory()
        self._devices: Dict[str, Any] = {}
        self._device_info: Dict[str, DeviceInfo] = {}
        self._build_registry()

    def register_device(
        self,
        name: str,
        device: Any,
        *,
        device_type: Optional[str] = None,
    ) -> None:
        """
        注册或替换设备实例。

        Args:
            name: 设备名称。
            device: 设备对象。
            device_type: 可选的设备类型覆盖值。
        """
        info = self._device_info.get(name)
        if info is None:
            info = DeviceInfo(
                name=name,
                device_type=device_type or name,
                port=str(getattr(device, "port", "") or ""),
            )
            self._device_info[name] = info

        info.device = device
        info.device_type = device_type or info.device_type
        info.port = str(getattr(device, "port", info.port) or info.port)
        self._devices[name] = device

        if not info.enabled:
            info.status = DeviceStatus.DISABLED
        elif info.status is DeviceStatus.DISABLED:
            info.status = DeviceStatus.UNKNOWN

    def create_device(
        self,
        name: str,
        device_type: DeviceType | str,
        config: Any,
    ) -> Any:
        """
        使用设备工厂创建设备并注册。

        Args:
            name: 设备名称。
            device_type: 设备类型。
            config: 设备配置。

        Returns:
            创建出的设备实例。
        """
        normalized_type = DeviceType.from_value(device_type)
        device = self.device_factory.create(normalized_type, config)
        self.register_device(name, device, device_type=normalized_type.value)

        info = self._device_info.get(name)
        if info is not None:
            port = getattr(config, "port", None)
            if port is None and isinstance(config, dict):
                port = config.get("port", "")
            info.port = str(port or info.port)

        return device

    def open_all(self) -> Dict[str, bool]:
        """
        打开所有已启用设备。

        Returns:
            设备名到打开结果的映射。
        """
        results: Dict[str, bool] = {}
        for name, info in self._device_info.items():
            if not info.enabled:
                info.status = DeviceStatus.DISABLED
                results[name] = False
                continue

            device = self._devices.get(name)
            if device is None:
                info.status = DeviceStatus.OFFLINE
                info.error_message = "device not registered"
                results[name] = False
                continue

            try:
                results[name] = self._open_device(device)
            except Exception as exc:
                info.status = DeviceStatus.ERROR
                info.error_message = str(exc)
                results[name] = False
                continue

            if results[name]:
                info.status = DeviceStatus.ONLINE
                info.error_message = None
            else:
                info.status = DeviceStatus.OFFLINE
                info.error_message = "device open returned false"

        return results

    def close_all(self) -> None:
        """关闭所有已注册设备。"""
        for name, info in self._device_info.items():
            device = self._devices.get(name)
            if device is None:
                if info.enabled and info.status is not DeviceStatus.DISABLED:
                    info.status = DeviceStatus.OFFLINE
                continue

            try:
                self._close_device(device)
            except Exception as exc:
                info.status = DeviceStatus.ERROR
                info.error_message = str(exc)
                continue

            if info.enabled:
                info.status = DeviceStatus.OFFLINE
                info.error_message = None
            else:
                info.status = DeviceStatus.DISABLED

    def get_device(self, name: str) -> Any:
        """返回指定设备实例，不存在时返回 `None`。"""
        return self._devices.get(name)

    def get_status(self, name: str) -> DeviceStatus:
        """返回指定设备状态，不存在时返回 `UNKNOWN`。"""
        info = self._device_info.get(name)
        if info is None:
            return DeviceStatus.UNKNOWN
        return info.status

    def get_info(self, name: str) -> Optional[DeviceInfo]:
        """返回指定设备信息。"""
        return self._device_info.get(name)

    def list_device_info(self) -> Dict[str, DeviceInfo]:
        """返回设备信息快照。"""
        return dict(self._device_info)

    def get_devices_by_type(self, device_type: DeviceType | str) -> Dict[str, Any]:
        """按设备类型返回已注册设备。"""
        normalized_type = DeviceType.from_value(device_type)
        matched: Dict[str, Any] = {}
        for name, info in self._device_info.items():
            if self._normalize_device_type_name(info.device_type) == normalized_type.value:
                device = self._devices.get(name)
                if device is not None:
                    matched[name] = device
        return matched

    def disable_device(self, name: str, reason: str) -> None:
        """
        禁用设备。

        Args:
            name: 设备名称。
            reason: 禁用原因。
        """
        info = self._device_info.get(name)
        if info is None:
            return

        info.enabled = False
        info.error_message = reason

        device = self._devices.get(name)
        if device is not None:
            try:
                self._close_device(device)
            except Exception as exc:
                info.error_message = f"{reason}; close failed: {exc}"

        info.status = DeviceStatus.DISABLED

    def enable_device(self, name: str) -> bool:
        """
        重新启用设备并尝试打开。

        Args:
            name: 设备名称。

        Returns:
            是否重新启用成功。
        """
        info = self._device_info.get(name)
        if info is None:
            return False

        info.enabled = True
        info.error_message = None
        device = self._devices.get(name)
        if device is None:
            info.status = DeviceStatus.OFFLINE
            info.error_message = "device not registered"
            return False

        try:
            opened = self._open_device(device)
        except Exception as exc:
            info.status = DeviceStatus.ERROR
            info.error_message = str(exc)
            return False

        if opened:
            info.status = DeviceStatus.ONLINE
            return True

        info.status = DeviceStatus.OFFLINE
        info.error_message = "device open returned false"
        return False

    def health_check(self) -> Dict[str, bool]:
        """
        执行设备健康检查。

        Returns:
            设备名到健康状态的映射。
        """
        results: Dict[str, bool] = {}
        for name, info in self._device_info.items():
            if not info.enabled:
                info.status = DeviceStatus.DISABLED
                results[name] = False
                continue

            device = self._devices.get(name)
            if device is None:
                info.status = DeviceStatus.OFFLINE
                info.error_message = "device not registered"
                results[name] = False
                continue

            try:
                healthy = self._health_check_device(device)
            except Exception as exc:
                info.status = DeviceStatus.ERROR
                info.error_message = str(exc)
                results[name] = False
                continue

            results[name] = healthy
            if healthy:
                info.status = DeviceStatus.ONLINE
                info.error_message = None
            else:
                info.status = DeviceStatus.OFFLINE
                info.error_message = "health check reported offline"

        return results

    def __enter__(self) -> "DeviceManager":
        """进入上下文时自动打开设备。"""
        self.open_all()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """退出上下文时自动关闭设备。"""
        self.close_all()

    def _build_registry(self) -> None:
        self._register_from_config("pressure_controller", self.config.pressure_controller)
        self._register_from_config("pressure_meter", self.config.pressure_meter)
        self._register_from_config("dewpoint_meter", self.config.dewpoint_meter)
        self._register_from_config("humidity_generator", self.config.humidity_generator)
        self._register_from_config("temperature_chamber", self.config.temperature_chamber)
        self._register_from_config("relay_a", self.config.relay_a, device_type=DeviceType.RELAY)
        self._register_from_config("relay_b", self.config.relay_b, device_type=DeviceType.RELAY)

        for index, analyzer_config in enumerate(self.config.gas_analyzers):
            self._register_from_config(
                f"gas_analyzer_{index}",
                analyzer_config,
                device_type=DeviceType.GAS_ANALYZER,
            )

    def _register_from_config(
        self,
        name: str,
        config: Optional[SingleDeviceConfig],
        *,
        device_type: Optional[DeviceType] = None,
    ) -> None:
        if config is None:
            return

        resolved_type = device_type or self._infer_device_type_from_name(name)
        self._device_info[name] = DeviceInfo(
            name=name,
            device_type=resolved_type.value,
            port=config.port,
            status=DeviceStatus.UNKNOWN if config.enabled else DeviceStatus.DISABLED,
            enabled=bool(config.enabled),
        )

    @staticmethod
    def _infer_device_type_from_name(name: str) -> DeviceType:
        text = str(name or "").strip().lower()
        if text.startswith("gas_analyzer"):
            return DeviceType.GAS_ANALYZER
        if text.startswith("relay"):
            return DeviceType.RELAY
        return DeviceType.from_value(text)

    @staticmethod
    def _normalize_device_type_name(value: str) -> str:
        try:
            return DeviceType.from_value(value).value
        except ValueError:
            return str(value or "").strip().lower()

    @staticmethod
    def _open_device(device: Any) -> bool:
        if hasattr(device, "open"):
            device.open()
            return True
        if hasattr(device, "connect"):
            result = device.connect()
            return True if result is None else bool(result)
        return True

    @staticmethod
    def _close_device(device: Any) -> None:
        if hasattr(device, "close"):
            device.close()

    def _health_check_device(self, device: Any) -> bool:
        if hasattr(device, "selftest"):
            result = device.selftest()
            healthy = self._interpret_health_result(result)
            if not healthy and isinstance(result, dict):
                mode_val = result.get("mode")
                if mode_val is not None and int(mode_val or 0) != 2:
                    recovered = self._recover_gas_analyzer_mode2(device, result)
                    if recovered:
                        retry_result = device.selftest()
                        return self._interpret_health_result(retry_result)
                if mode_val is None and hasattr(device, "read_latest_data"):
                    for _ in range(3):
                        time.sleep(0.5)
                        retry_result = device.selftest()
                        if self._interpret_health_result(retry_result):
                            return True
            return healthy
        if hasattr(device, "status"):
            return self._interpret_health_result(device.status())
        return True

    @staticmethod
    def _recover_gas_analyzer_mode2(device: Any, selftest_result: dict[str, Any]) -> bool:
        if not (hasattr(device, "read_latest_data") or hasattr(device, "set_mode_with_ack")):
            return False
        mode_val = int(selftest_result.get("mode") or 0)
        if mode_val == 2:
            return False
        try:
            set_mode = getattr(device, "set_mode_with_ack", None)
            if callable(set_mode):
                set_mode(2)
                return True
            set_mode_simple = getattr(device, "set_mode", None)
            if callable(set_mode_simple):
                set_mode_simple(2)
                return True
            return False
        except Exception:
            return False

    def _interpret_health_result(self, result: Any) -> bool:
        if isinstance(result, bool):
            return result

        if isinstance(result, dict):
            for key in ("ok", "healthy", "passed", "success", "connected", "is_open", "online"):
                if key in result:
                    return as_bool(result[key], default=False)

            status_text = str(result.get("status", "")).strip().lower()
            if status_text in {"ok", "online", "connected", "ready", "open", "pass"}:
                return True
            if status_text in {"offline", "error", "closed", "fail", "failed"}:
                return False

            return bool(result)

        return bool(result)
