from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from .mode_models import ModeProfile
from .pressure_selection import (
    AMBIENT_PRESSURE_TOKEN,
    effective_pressure_mode,
    is_ambient_pressure_selection_value,
    normalize_pressure_mode,
    normalize_pressure_selection_token,
    normalize_selected_pressure_points,
    pressure_selection_key,
    pressure_target_label,
)


def _as_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    return bool(value)


def _as_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    if value is None or value == "":
        return default
    return int(value)


def _as_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value is None or value == "":
        return default
    return float(value)


def _as_co2_group(value: Any, default: str = "A") -> str:
    text = str(value or "").strip().upper()
    return text or str(default or "A").strip().upper() or "A"


def _normalize_analyzer_software_version(value: Any, default: str = "v5_plus") -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"pre_v5", "pre-v5", "before_v5", "legacy", "v4"}:
        return "pre_v5"
    if normalized in {"v5_plus", "v5+", "v5", "post_v5", "after_v5", ""}:
        return "v5_plus"
    return str(default or "v5_plus").strip().lower() or "v5_plus"


def _normalize_device_id_assignment_mode(value: Any, default: str = "automatic") -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"manual", "manual_list", "fixed"}:
        return "manual"
    if normalized in {"automatic", "auto", "auto_assign", ""}:
        return "automatic"
    return str(default or "automatic").strip().lower() or "automatic"


def _normalize_device_id(value: Any, default: str = "") -> str:
    text = str(value or "").strip()
    if not text:
        text = str(default or "").strip()
    if not text:
        return ""
    if text.isdigit():
        return f"{int(text):03d}"
    return text.upper()


def _normalize_device_id_list(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        raw_items = value.replace(";", ",").replace("\n", ",").split(",")
    else:
        raw_items = list(value)
    normalized: list[str] = []
    for item in raw_items:
        device_id = _normalize_device_id(item)
        if device_id:
            normalized.append(device_id)
    return normalized


def _normalize_profile_version(value: Any, default: str = "1.0") -> str:
    text = str(value or "").strip()
    return text or str(default or "1.0").strip() or "1.0"


@dataclass
class AnalyzerSetupSpec:
    software_version: str = "v5_plus"
    device_id_assignment_mode: str = "automatic"
    start_device_id: str = "001"
    manual_device_ids: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "software_version": _normalize_analyzer_software_version(self.software_version),
            "device_id_assignment_mode": _normalize_device_id_assignment_mode(self.device_id_assignment_mode),
            "start_device_id": _normalize_device_id(self.start_device_id, default="001") or "001",
            "manual_device_ids": _normalize_device_id_list(self.manual_device_ids),
        }

    @classmethod
    def from_dict(cls, payload: Optional[dict[str, Any]]) -> "AnalyzerSetupSpec":
        data = dict(payload or {})
        manual_ids = data.get("manual_device_ids", data.get("device_ids", data.get("manual_ids", [])))
        return cls(
            software_version=_normalize_analyzer_software_version(
                data.get("software_version", data.get("analyzer_version"))
            ),
            device_id_assignment_mode=_normalize_device_id_assignment_mode(
                data.get("device_id_assignment_mode", data.get("id_assignment_mode", data.get("device_id_mode")))
            ),
            start_device_id=_normalize_device_id(
                data.get("start_device_id", data.get("starting_device_id", data.get("id_start"))),
                default="001",
            )
            or "001",
            manual_device_ids=_normalize_device_id_list(manual_ids),
        )


@dataclass
class TemperatureSpec:
    temperature_c: float
    enabled: bool = True
    order: Optional[int] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "temperature_c": float(self.temperature_c),
            "enabled": bool(self.enabled),
            "order": self.order,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "TemperatureSpec":
        return cls(
            temperature_c=float(payload.get("temperature_c", payload.get("value_c"))),
            enabled=_as_bool(payload.get("enabled"), True),
            order=_as_int(payload.get("order")),
        )


@dataclass
class HumiditySpec:
    hgen_temp_c: Optional[float] = None
    hgen_rh_pct: Optional[float] = None
    dewpoint_c: Optional[float] = None
    enabled: bool = True
    order: Optional[int] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "hgen_temp_c": self.hgen_temp_c,
            "hgen_rh_pct": self.hgen_rh_pct,
            "dewpoint_c": self.dewpoint_c,
            "enabled": bool(self.enabled),
            "order": self.order,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "HumiditySpec":
        return cls(
            hgen_temp_c=_as_float(payload.get("hgen_temp_c", payload.get("generator_temp_c"))),
            hgen_rh_pct=_as_float(payload.get("hgen_rh_pct", payload.get("rh_pct"))),
            dewpoint_c=_as_float(payload.get("dewpoint_c")),
            enabled=_as_bool(payload.get("enabled"), True),
            order=_as_int(payload.get("order")),
        )


@dataclass
class GasPointSpec:
    co2_ppm: float
    co2_group: str = "A"
    cylinder_nominal_ppm: Optional[float] = None
    enabled: bool = True
    order: Optional[int] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "co2_ppm": float(self.co2_ppm),
            "co2_group": _as_co2_group(self.co2_group),
            "cylinder_nominal_ppm": self.cylinder_nominal_ppm,
            "enabled": bool(self.enabled),
            "order": self.order,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "GasPointSpec":
        return cls(
            co2_ppm=float(payload.get("co2_ppm", payload.get("ppm"))),
            co2_group=_as_co2_group(payload.get("co2_group", payload.get("group"))),
            cylinder_nominal_ppm=_as_float(
                payload.get("cylinder_nominal_ppm", payload.get("nominal_ppm", payload.get("bottle_ppm")))
            ),
            enabled=_as_bool(payload.get("enabled"), True),
            order=_as_int(payload.get("order")),
        )


@dataclass
class PressureSpec:
    pressure_hpa: Optional[float]
    pressure_mode: str = ""
    pressure_target_label: Optional[str] = None
    pressure_selection_token: str = ""
    enabled: bool = True
    order: Optional[int] = None

    @property
    def effective_pressure_mode(self) -> str:
        return effective_pressure_mode(
            pressure_hpa=self.pressure_hpa,
            pressure_mode=self.pressure_mode,
            pressure_selection_token=self.pressure_selection_token,
        )

    @property
    def is_ambient_pressure_point(self) -> bool:
        return self.effective_pressure_mode == "ambient_open"

    def pressure_label(self) -> Optional[str]:
        return pressure_target_label(
            pressure_hpa=self.pressure_hpa,
            pressure_mode=self.pressure_mode,
            pressure_selection_token=self.pressure_selection_token,
            explicit_label=self.pressure_target_label,
        )

    def selection_key(self) -> Optional[float | str]:
        return pressure_selection_key(
            pressure_hpa=self.pressure_hpa,
            pressure_mode=self.pressure_mode,
            pressure_selection_token=self.pressure_selection_token,
        )

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "pressure_hpa": None if self.pressure_hpa is None else float(self.pressure_hpa),
            "enabled": bool(self.enabled),
            "order": self.order,
        }
        effective_mode = self.effective_pressure_mode
        if effective_mode:
            payload["pressure_mode"] = effective_mode
        token = normalize_pressure_selection_token(self.pressure_selection_token)
        if token:
            payload["pressure_selection_token"] = token
        label = self.pressure_label()
        if label:
            payload["pressure_target_label"] = label
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "PressureSpec":
        raw_pressure = payload.get("pressure_hpa", payload.get("value_hpa"))
        raw_mode = normalize_pressure_mode(payload.get("pressure_mode", payload.get("mode")))
        raw_token = normalize_pressure_selection_token(
            payload.get(
                "pressure_selection_token",
                payload.get("pressure_token", payload.get("selected_pressure_point")),
            )
        )
        if not raw_token and is_ambient_pressure_selection_value(raw_pressure):
            raw_token = AMBIENT_PRESSURE_TOKEN
        effective_mode = effective_pressure_mode(
            pressure_hpa=raw_pressure,
            pressure_mode=raw_mode,
            pressure_selection_token=raw_token,
        )
        pressure_hpa = None if effective_mode == "ambient_open" else _as_float(raw_pressure)
        return cls(
            pressure_hpa=pressure_hpa,
            pressure_mode=effective_mode,
            pressure_target_label=pressure_target_label(
                pressure_hpa=pressure_hpa,
                pressure_mode=effective_mode,
                pressure_selection_token=raw_token,
                explicit_label=payload.get("pressure_target_label", payload.get("pressure_label")),
            ),
            pressure_selection_token=raw_token,
            enabled=_as_bool(payload.get("enabled"), True),
            order=_as_int(payload.get("order")),
        )


@dataclass
class PlanOrderingOptions:
    water_first: bool = False
    water_first_temp_gte: Optional[float] = None
    selected_temps_c: list[float] = field(default_factory=list)
    selected_pressure_points: list[Any] = field(default_factory=list)
    skip_co2_ppm: list[int] = field(default_factory=list)
    temperature_descending: bool = True
    water_first_explicit: bool = field(default=False, repr=False, compare=False)
    water_first_temp_gte_explicit: bool = field(default=False, repr=False, compare=False)

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "water_first": bool(self.water_first),
            "selected_temps_c": [float(value) for value in self.selected_temps_c],
            "selected_pressure_points": list(normalize_selected_pressure_points(self.selected_pressure_points)),
            "skip_co2_ppm": [int(value) for value in self.skip_co2_ppm],
            "temperature_descending": bool(self.temperature_descending),
        }
        if self.water_first_temp_gte is not None or self.water_first_temp_gte_explicit:
            payload["water_first_temp_gte"] = self.water_first_temp_gte
        return payload

    @classmethod
    def from_dict(cls, payload: Optional[dict[str, Any]]) -> "PlanOrderingOptions":
        data = dict(payload or {})
        raw_selected = data.get("selected_temps_c", data.get("selected_temps", []))
        return cls(
            water_first=_as_bool(data.get("water_first"), False),
            water_first_temp_gte=_as_float(data.get("water_first_temp_gte")),
            selected_temps_c=[float(value) for value in raw_selected or []],
            selected_pressure_points=list(
                normalize_selected_pressure_points(
                    data.get("selected_pressure_points", data.get("selected_pressures", []))
                )
            ),
            skip_co2_ppm=[int(value) for value in data.get("skip_co2_ppm", [])],
            temperature_descending=_as_bool(data.get("temperature_descending"), True),
            water_first_explicit="water_first" in data,
            water_first_temp_gte_explicit="water_first_temp_gte" in data,
        )


@dataclass
class CalibrationPlanProfile:
    name: str
    profile_version: str = "1.0"
    description: str = ""
    is_default: bool = False
    mode_profile: ModeProfile = field(default_factory=ModeProfile)
    analyzer_setup: AnalyzerSetupSpec = field(default_factory=AnalyzerSetupSpec)
    temperatures: list[TemperatureSpec] = field(default_factory=list)
    humidities: list[HumiditySpec] = field(default_factory=list)
    gas_points: list[GasPointSpec] = field(default_factory=list)
    pressures: list[PressureSpec] = field(default_factory=list)
    ordering: PlanOrderingOptions = field(default_factory=PlanOrderingOptions)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "profile_version": _normalize_profile_version(self.profile_version),
            "description": self.description,
            "is_default": bool(self.is_default),
            "run_mode": self.mode_profile.run_mode.value,
            "mode_profile": self.mode_profile.to_dict(),
            "analyzer_setup": self.analyzer_setup.to_dict(),
            "temperatures": [item.to_dict() for item in self.temperatures],
            "humidities": [item.to_dict() for item in self.humidities],
            "gas_points": [item.to_dict() for item in self.gas_points],
            "pressures": [item.to_dict() for item in self.pressures],
            "ordering": self.ordering.to_dict(),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "CalibrationPlanProfile":
        ordering_payload = dict(payload.get("ordering", {}))
        for key in (
            "water_first",
            "water_first_temp_gte",
            "selected_temps",
            "selected_temps_c",
            "selected_pressure_points",
            "selected_pressures",
            "skip_co2_ppm",
            "temperature_descending",
        ):
            if key in payload and key not in ordering_payload:
                ordering_payload[key] = payload.get(key)
        mode_payload = payload.get("mode_profile")
        if not isinstance(mode_payload, dict):
            mode_payload = {"run_mode": payload.get("run_mode")}
        return cls(
            name=str(payload.get("name", "")),
            profile_version=_normalize_profile_version(
                payload.get("profile_version", payload.get("version", payload.get("plan_version")))
            ),
            description=str(payload.get("description", "")),
            is_default=_as_bool(payload.get("is_default", payload.get("default")), False),
            mode_profile=ModeProfile.from_value(mode_payload),
            analyzer_setup=AnalyzerSetupSpec.from_dict(payload.get("analyzer_setup")),
            temperatures=[
                TemperatureSpec.from_dict(item)
                for item in payload.get("temperatures", payload.get("temperature_points", []))
            ],
            humidities=[
                HumiditySpec.from_dict(item)
                for item in payload.get("humidities", payload.get("humidity_points", []))
            ],
            gas_points=[
                GasPointSpec.from_dict(item)
                for item in payload.get("gas_points", payload.get("co2_points", []))
            ],
            pressures=[
                PressureSpec.from_dict(item)
                for item in payload.get("pressures", payload.get("pressure_points", []))
            ],
            ordering=PlanOrderingOptions.from_dict(ordering_payload),
        )
