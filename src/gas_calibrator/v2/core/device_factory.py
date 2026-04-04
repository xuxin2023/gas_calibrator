"""
设备工厂。

本模块复用旧版 `src/gas_calibrator/devices/` 下的驱动实现，
通过统一的工厂接口在 V2 中创建设备实例。
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from enum import Enum
from importlib import import_module
import inspect
from typing import Any, Dict, Mapping, MutableMapping, Optional, Type

from ..sim.devices import (
    AnalyzerFake,
    GRZ5013Fake,
    PACE5000Fake,
    ParoscientificPressureGaugeFake,
    RelayFake,
    TemperatureChamberFake,
    ThermometerFake,
)
from .simulated_devices import (
    SimulationPlantState,
    SimulatedDewpointMeter,
)


class DeviceType(Enum):
    """设备类型枚举。"""

    PRESSURE_CONTROLLER = "pressure_controller"
    PRESSURE_METER = "pressure_meter"
    DEWPOINT_METER = "dewpoint_meter"
    HUMIDITY_GENERATOR = "humidity_generator"
    TEMPERATURE_CHAMBER = "temperature_chamber"
    RELAY = "relay"
    GAS_ANALYZER = "gas_analyzer"
    THERMOMETER = "thermometer"

    @classmethod
    def from_value(cls, value: "DeviceType | str") -> "DeviceType":
        """将枚举或字符串标准化为 `DeviceType`。"""
        if isinstance(value, cls):
            return value
        text = str(value or "").strip().lower()
        for member in cls:
            if member.value == text or member.name.lower() == text:
                return member
        raise ValueError(f"Unsupported device type: {value}")


class DeviceDriverImportError(ImportError):
    """Raised when a real device driver cannot be imported on demand."""

    def __init__(
        self,
        *,
        device_type: DeviceType,
        module_name: str,
        class_name: str,
        dependency_name: str,
    ) -> None:
        super().__init__(
            "Cannot create real "
            f"{device_type.value} device because driver dependency "
            f"'{dependency_name}' is unavailable while loading "
            f"{module_name}.{class_name}. "
            "Use simulation_mode/offline helpers or install the missing driver dependency."
        )
        self.device_type = device_type
        self.module_name = module_name
        self.class_name = class_name
        self.dependency_name = dependency_name


@dataclass
class _LazyDeviceClassRef:
    device_type: DeviceType
    module_name: str
    class_name: str
    _resolved_class: Optional[Type[Any]] = None

    @property
    def __name__(self) -> str:
        return self.class_name

    def resolve(self) -> Type[Any]:
        if self._resolved_class is not None:
            return self._resolved_class
        try:
            module = import_module(self.module_name)
        except ModuleNotFoundError as exc:
            dependency_name = str(getattr(exc, "name", "") or self.module_name)
            raise DeviceDriverImportError(
                device_type=self.device_type,
                module_name=self.module_name,
                class_name=self.class_name,
                dependency_name=dependency_name,
            ) from exc
        try:
            resolved = getattr(module, self.class_name)
        except AttributeError as exc:
            raise ImportError(
                f"Real driver class {self.module_name}.{self.class_name} is not available."
            ) from exc
        self._resolved_class = resolved
        return resolved


class DeviceFactory:
    """V2 设备工厂。"""

    def __init__(
        self,
        simulation_mode: bool = False,
        io_logger: Optional[Any] = None,
        simulation_context: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self.simulation_mode = bool(simulation_mode)
        self.io_logger = io_logger
        self.simulation_context: dict[str, Any] = (
            dict(simulation_context) if isinstance(simulation_context, Mapping) else {}
        )
        self._registry: Dict[DeviceType, Type[Any] | _LazyDeviceClassRef] = {}
        self._simulation_registry: Dict[DeviceType, Type[Any]] = {}
        self._simulation_plant_state = SimulationPlantState()
        self._register_defaults()

    def register(self, device_type: DeviceType | str, device_class: Type[Any]) -> None:
        """注册设备类。"""
        normalized_type = DeviceType.from_value(device_type)
        self._registry[normalized_type] = device_class

    def register_lazy(
        self,
        device_type: DeviceType | str,
        module_name: str,
        class_name: str,
    ) -> None:
        """注册按需解析的真实驱动。"""
        normalized_type = DeviceType.from_value(device_type)
        self._registry[normalized_type] = _LazyDeviceClassRef(
            device_type=normalized_type,
            module_name=module_name,
            class_name=class_name,
        )

    def create(self, device_type: DeviceType | str, config: Any) -> Any:
        """
        创建设备实例。

        Args:
            device_type: 设备类型。
            config: 配置对象，可为 dataclass、映射或普通对象。

        Returns:
            设备实例。
        """
        normalized_type = DeviceType.from_value(device_type)
        registry = self._simulation_registry if self.simulation_mode else self._registry
        registry_entry = registry.get(normalized_type) or self._registry.get(normalized_type)
        if registry_entry is None:
            raise ValueError(f"Device type not registered: {normalized_type.value}")
        device_class = self._resolve_device_class(registry_entry)

        kwargs = self._normalize_config(config)
        kwargs = self._apply_common_kwargs(kwargs)
        kwargs = self._apply_type_specific_kwargs(normalized_type, kwargs)
        if self.simulation_mode and normalized_type in self._simulation_registry:
            kwargs.setdefault("plant_state", self._simulation_plant_state)
            kwargs = self._apply_simulation_kwargs(normalized_type, kwargs)
        kwargs = self._filter_constructor_kwargs(device_class, kwargs)
        return device_class(**kwargs)

    @property
    def simulation_plant_state(self) -> SimulationPlantState:
        return self._simulation_plant_state

    def _register_defaults(self) -> None:
        if not self.simulation_mode:
            self.register_lazy(DeviceType.PRESSURE_CONTROLLER, "gas_calibrator.devices.pace5000", "Pace5000")
            self.register_lazy(
                DeviceType.PRESSURE_METER,
                "gas_calibrator.devices.paroscientific",
                "ParoscientificGauge",
            )
            self.register_lazy(DeviceType.DEWPOINT_METER, "gas_calibrator.devices.dewpoint_meter", "DewpointMeter")
            self.register_lazy(
                DeviceType.HUMIDITY_GENERATOR,
                "gas_calibrator.devices.humidity_generator",
                "HumidityGenerator",
            )
            self.register_lazy(
                DeviceType.TEMPERATURE_CHAMBER,
                "gas_calibrator.devices.temperature_chamber",
                "TemperatureChamber",
            )
            self.register_lazy(DeviceType.RELAY, "gas_calibrator.devices.relay", "RelayController")
            self.register_lazy(DeviceType.GAS_ANALYZER, "gas_calibrator.devices.gas_analyzer", "GasAnalyzer")
            self.register_lazy(DeviceType.THERMOMETER, "gas_calibrator.devices.thermometer", "Thermometer")
        self._simulation_registry.update(
            {
                DeviceType.PRESSURE_CONTROLLER: PACE5000Fake,
                DeviceType.PRESSURE_METER: ParoscientificPressureGaugeFake,
                DeviceType.DEWPOINT_METER: SimulatedDewpointMeter,
                DeviceType.HUMIDITY_GENERATOR: GRZ5013Fake,
                DeviceType.TEMPERATURE_CHAMBER: TemperatureChamberFake,
                DeviceType.RELAY: RelayFake,
                DeviceType.GAS_ANALYZER: AnalyzerFake,
                DeviceType.THERMOMETER: ThermometerFake,
            }
        )

    @staticmethod
    def _import_class(module_name: str, class_name: str) -> Type[Any]:
        module = import_module(module_name)
        return getattr(module, class_name)

    @staticmethod
    def _resolve_device_class(
        registry_entry: Type[Any] | _LazyDeviceClassRef,
    ) -> Type[Any]:
        if isinstance(registry_entry, _LazyDeviceClassRef):
            return registry_entry.resolve()
        return registry_entry

    @staticmethod
    def _normalize_config(config: Any) -> Dict[str, Any]:
        if config is None:
            return {}
        if is_dataclass(config):
            return dict(asdict(config))
        if isinstance(config, Mapping):
            return dict(config)

        values: Dict[str, Any] = {}
        for key in dir(config):
            if key.startswith("_"):
                continue
            value = getattr(config, key)
            if callable(value):
                continue
            values[key] = value
        return values

    def _apply_common_kwargs(self, config: MutableMapping[str, Any]) -> Dict[str, Any]:
        kwargs = dict(config)
        if "baud" in kwargs and "baudrate" not in kwargs:
            kwargs["baudrate"] = kwargs.pop("baud")
        if self.io_logger is not None and "io_logger" not in kwargs:
            kwargs["io_logger"] = self.io_logger
        return kwargs

    def _apply_type_specific_kwargs(
        self,
        device_type: DeviceType,
        config: MutableMapping[str, Any],
    ) -> Dict[str, Any]:
        kwargs = dict(config)

        return kwargs

    def _apply_simulation_kwargs(
        self,
        device_type: DeviceType,
        config: MutableMapping[str, Any],
    ) -> Dict[str, Any]:
        kwargs = dict(config)
        matrix = self._simulation_device_matrix()
        spec_key = self._simulation_spec_key(device_type, kwargs)
        spec = matrix.get(spec_key, {})
        if isinstance(spec, Mapping):
            for key, value in dict(spec).items():
                kwargs.setdefault(str(key), value)
        if device_type is DeviceType.GAS_ANALYZER:
            kwargs = self._apply_analyzer_simulation_kwargs(kwargs, spec)
        kwargs = self._apply_device_override_kwargs(kwargs, matrix)
        kwargs.setdefault("simulation_context", self.simulation_context)
        return kwargs

    @staticmethod
    def _simulation_spec_key(device_type: DeviceType, config: Mapping[str, Any]) -> str:
        if device_type is DeviceType.RELAY:
            name = str(config.get("name") or "").strip().lower()
            if name in {"relay_b", "relay_8"}:
                return "relay_8"
            return "relay"
        if device_type is DeviceType.THERMOMETER:
            return "thermometer"
        spec_key_map = {
            DeviceType.GAS_ANALYZER: "analyzers",
            DeviceType.HUMIDITY_GENERATOR: "humidity_generator",
            DeviceType.TEMPERATURE_CHAMBER: "temperature_chamber",
            DeviceType.DEWPOINT_METER: "dewpoint_meter",
            DeviceType.PRESSURE_CONTROLLER: "pressure_controller",
            DeviceType.PRESSURE_METER: "pressure_gauge",
        }
        return spec_key_map.get(device_type, "")

    def _simulation_device_matrix(self) -> dict[str, Any]:
        matrix = self.simulation_context.get("device_matrix")
        return dict(matrix) if isinstance(matrix, Mapping) else {}

    @staticmethod
    def _apply_device_override_kwargs(
        config: MutableMapping[str, Any],
        matrix: Mapping[str, Any],
    ) -> Dict[str, Any]:
        kwargs = dict(config)
        overrides = matrix.get("device_overrides")
        if not isinstance(overrides, Mapping):
            return kwargs
        candidates = {
            str(kwargs.get("name") or "").strip(),
            str(kwargs.get("name") or "").strip().lower(),
        }
        device_id = str(kwargs.get("device_id") or "").strip()
        if device_id:
            candidates.update(
                {
                    device_id,
                    device_id.lower(),
                    f"gas_analyzer_{max(0, int(device_id) - 1)}" if device_id.isdigit() else "",
                }
            )
        for candidate in candidates:
            if not candidate:
                continue
            payload = overrides.get(candidate)
            if not isinstance(payload, Mapping):
                continue
            for key, value in dict(payload).items():
                kwargs[str(key)] = value
        return kwargs

    @staticmethod
    def _apply_analyzer_simulation_kwargs(
        config: MutableMapping[str, Any],
        spec: Mapping[str, Any],
    ) -> Dict[str, Any]:
        kwargs = dict(config)
        name = str(kwargs.get("name") or "")
        index = DeviceFactory._simulation_analyzer_index(name=name, device_id=kwargs.get("device_id"))
        versions = spec.get("versions")
        if isinstance(versions, list) and versions:
            kwargs.setdefault("software_version", versions[min(index, len(versions) - 1)])
        status_bits = spec.get("status_bits")
        if isinstance(status_bits, list) and status_bits:
            kwargs.setdefault("status_bits", status_bits[min(index, len(status_bits) - 1)])
        if bool(spec.get("mode_switch_not_applied", False)):
            kwargs.setdefault("mode2_stream", "mode_switch_not_applied")
        return kwargs

    @staticmethod
    def _simulation_analyzer_index(*, name: str, device_id: Any) -> int:
        if str(device_id or "").strip().isdigit():
            return max(0, int(str(device_id).strip()) - 1)
        digits = "".join(ch for ch in str(name or "") if ch.isdigit())
        if digits:
            return max(0, int(digits) - 1)
        return 0

    @staticmethod
    def _filter_constructor_kwargs(
        device_class: Type[Any],
        config: MutableMapping[str, Any],
    ) -> Dict[str, Any]:
        try:
            parameters = inspect.signature(device_class.__init__).parameters
        except (TypeError, ValueError):
            return dict(config)

        accepts_var_kwargs = any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD
            for parameter in parameters.values()
        )
        if accepts_var_kwargs:
            return dict(config)

        allowed = {
            name
            for name, parameter in parameters.items()
            if name != "self"
            and parameter.kind in (
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            )
        }
        return {
            key: value
            for key, value in config.items()
            if key in allowed
        }
