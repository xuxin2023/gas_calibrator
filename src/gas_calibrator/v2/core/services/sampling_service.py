from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from datetime import datetime, timezone
from statistics import mean, stdev
import time
from typing import Any, Callable, Dict, Optional

from ...utils import as_float, safe_get
from ..device_manager import DeviceStatus
from ..models import CalibrationPoint, SamplingResult
from ..orchestration_context import OrchestrationContext
from ..run_state import RunState


def sensor_read_retry_settings_from_host(host: Any) -> tuple[int, float]:
    retries_raw = host._cfg_get("workflow.sensor_read_retry.retries", None)
    delay_raw = host._cfg_get("workflow.sensor_read_retry.delay_s", None)
    if retries_raw is None or delay_raw is None:
        cfg = host._cfg_get("workflow.sensor_read_retry", {})
        if isinstance(cfg, dict):
            if retries_raw is None:
                retries_raw = cfg.get("retries")
            if delay_raw is None:
                delay_raw = cfg.get("delay_s")
    try:
        retries = max(0, int(1 if retries_raw is None else retries_raw))
    except Exception:
        retries = 1
    try:
        delay_s = max(0.0, float(0.05 if delay_raw is None else delay_raw))
    except Exception:
        delay_s = 0.05
    return retries, delay_s


def read_numeric_with_retry(
    read_func: Callable[[], Any],
    *,
    host: Any,
    context: str,
    transform: Optional[Callable[[Any], Optional[float]]] = None,
    log_failures: bool = True,
) -> Optional[float]:
    retries, delay_s = sensor_read_retry_settings_from_host(host)
    attempts = 1 + max(0, retries)
    transform_func = transform or as_float
    last_reason = ""
    for attempt in range(attempts):
        try:
            value = transform_func(read_func())
        except Exception as exc:
            last_reason = f"error={exc}"
        else:
            if value is not None:
                return float(value)
            last_reason = "missing numeric value"
        if attempt + 1 < attempts:
            if log_failures:
                host._log(f"Sensor read retry ({context}) {attempt + 1}/{retries}: {last_reason}")
            if delay_s > 0:
                time.sleep(max(0.0, delay_s))
        elif log_failures:
            host._log(f"Sensor read failed ({context}) after {attempts} attempts: {last_reason}")
    return None


def read_device_snapshot_with_retry(
    device: Any,
    *,
    host: Any,
    context: str = "",
    required_keys: tuple[str, ...] = (),
    retry_on_empty: bool = False,
    log_failures: bool = True,
) -> Any:
    method = host._first_method(device, ("fetch_all", "get_current", "read", "status"))
    if method is None:
        return {}
    retries, delay_s = sensor_read_retry_settings_from_host(host)
    attempts = 1 + max(0, retries)
    context_text = str(context or getattr(device, "__class__", type(device)).__name__).strip()
    last_snapshot: Any = {}
    last_reason = ""
    for attempt in range(attempts):
        try:
            snapshot = method()
            last_snapshot = snapshot
            retry_reason = SamplingService.snapshot_retry_reason(
                snapshot,
                required_keys=required_keys,
                retry_on_empty=retry_on_empty,
            )
        except Exception as exc:
            retry_reason = f"error={exc}"
            last_reason = retry_reason
            if attempt + 1 < attempts:
                if log_failures:
                    host._log(f"Sensor read retry ({context_text}) {attempt + 1}/{retries}: {retry_reason}")
                if delay_s > 0:
                    time.sleep(max(0.0, delay_s))
                continue
            if log_failures:
                host._log(f"Sensor read failed ({context_text}) after {attempts} attempts: {retry_reason}")
            return last_snapshot

        if retry_reason is None:
            return snapshot
        last_reason = retry_reason
        if attempt + 1 < attempts:
            if log_failures:
                host._log(f"Sensor read retry ({context_text}) {attempt + 1}/{retries}: {retry_reason}")
            if delay_s > 0:
                time.sleep(max(0.0, delay_s))
        elif log_failures:
            host._log(f"Sensor read failed ({context_text}) after {attempts} attempts: {retry_reason}")
    return last_snapshot


class SamplingService:
    """Sampling, runtime row shaping, and point-level batch collection helpers."""

    ANALYZER_FRAME_KEYS = (
        "co2_ratio_f",
        "co2_ppm",
        "co2",
        "h2o_ratio_f",
        "h2o_mmol",
        "h2o",
        "co2_signal",
        "h2o_signal",
    )
    STANDARD_ANALYZER_ROW_FIELDS = (
        ("co2_ppm", "co2_ppm"),
        ("h2o_mmol", "h2o_mmol"),
        ("co2_ratio_f", "co2_ratio_f"),
        ("h2o_ratio_f", "h2o_ratio_f"),
        ("co2_signal", "co2_signal"),
        ("h2o_signal", "h2o_signal"),
        ("ref_signal", "ref_signal"),
        ("analyzer_chamber_temp_c", "analyzer_chamber_temp_c"),
        ("case_temp_c", "case_temp_c"),
    )

    def __init__(self, context: OrchestrationContext, run_state: RunState, *, host: Any) -> None:
        self.context = context
        self.run_state = run_state
        self.host = host

    def collect_sampling_result(
        self,
        point: CalibrationPoint,
        analyzer_id: str,
        analyzer: Any,
        *,
        phase: str = "",
        point_tag: str = "",
        snapshot: Any = None,
        sample_index: int = 0,
    ) -> SamplingResult:
        raw_snapshot = snapshot if snapshot is not None else self.read_analyzer_snapshot(
            analyzer,
            label=analyzer_id,
            context=f"analyzer {analyzer_id} sampling result",
        )
        snapshot = self.normalize_snapshot(raw_snapshot)
        timing = self.host._point_timing(point, phase=phase, point_tag=point_tag)
        return SamplingResult(
            point=point,
            analyzer_id=analyzer_id,
            timestamp=datetime.now(timezone.utc),
            co2_ppm=self.pick_numeric(snapshot, "co2_ppm", "co2"),
            h2o_mmol=self.pick_numeric(snapshot, "h2o_mmol", "h2o"),
            h2o_signal=self.pick_numeric(snapshot, "h2o_signal", "h2o_sig"),
            co2_signal=self.pick_numeric(snapshot, "co2_signal", "co2_sig"),
            co2_ratio_f=self.pick_numeric(snapshot, "co2_ratio_f"),
            co2_ratio_raw=self.pick_numeric(snapshot, "co2_ratio_raw"),
            h2o_ratio_f=self.pick_numeric(snapshot, "h2o_ratio_f"),
            h2o_ratio_raw=self.pick_numeric(snapshot, "h2o_ratio_raw"),
            ref_signal=self.pick_numeric(snapshot, "ref_signal"),
            temperature_c=self.read_temperature_for_sampling(snapshot),
            pressure_hpa=self.read_pressure_for_sampling(snapshot),
            pressure_gauge_hpa=self.pick_numeric(snapshot, "pressure_gauge_hpa"),
            pressure_reference_status=self.pick_text(snapshot, "pressure_reference_status"),
            thermometer_temp_c=self.pick_numeric(snapshot, "thermometer_temp_c"),
            thermometer_reference_status=self.pick_text(snapshot, "thermometer_reference_status"),
            dew_point_c=self.read_dew_point_for_sampling(snapshot),
            analyzer_pressure_kpa=self.pick_numeric(snapshot, "pressure_kpa"),
            analyzer_chamber_temp_c=self.pick_numeric(snapshot, "chamber_temp_c", "temp_c"),
            case_temp_c=self.pick_numeric(snapshot, "case_temp_c"),
            frame_has_data=True,
            frame_usable=True,
            frame_status="ok",
            point_phase=str(phase or point.route or "").strip().lower(),
            point_tag=str(point_tag or ""),
            sample_index=int(sample_index),
            stability_time_s=self.host._as_float(timing.get("stability_time_s")),
        )

    def read_temperature_for_sampling(self, snapshot: Dict[str, Any]) -> Optional[float]:
        value = self.pick_numeric(snapshot, "temperature_c", "temp_c", "chamber_temp_c")
        if value is not None:
            return value
        chamber = self.context.device_manager.get_device("temperature_chamber")
        reader = self.make_temperature_reader(chamber)
        return None if reader is None else as_float(reader())

    def read_pressure_for_sampling(self, snapshot: Dict[str, Any]) -> Optional[float]:
        value = self.pick_numeric(snapshot, "pressure_hpa", "pressure", "target_pressure_hpa")
        if value is not None:
            return value
        reader = self.make_pressure_reader()
        return None if reader is None else as_float(reader())

    def read_dew_point_for_sampling(self, snapshot: Dict[str, Any]) -> Optional[float]:
        value = self.pick_numeric(snapshot, "dew_point_c", "dewpoint_c", "dew_point", "Td")
        if value is not None:
            return value
        dewpoint_meter = self.context.device_manager.get_device("dewpoint_meter")
        if dewpoint_meter is None:
            return None
        if self.context.device_manager.get_status("dewpoint_meter") is DeviceStatus.DISABLED:
            return None
        current = self.normalize_snapshot(
            self.read_device_snapshot(
                dewpoint_meter,
                context="dewpoint sampling snapshot",
                required_keys=("dew_point_c", "dewpoint_c", "dew_point", "Td"),
                retry_on_empty=True,
            )
        )
        return self.pick_numeric(current, "dew_point_c", "dewpoint_c", "dew_point", "Td")

    def make_temperature_reader(self, chamber: Any) -> Optional[Callable[[], Optional[float]]]:
        if chamber is None:
            return None
        method = self.host._first_method(chamber, ("read_temp_c", "read_temperature_c", "get_temperature"))
        if method is not None:
            return lambda method=method: read_numeric_with_retry(
                method,
                host=self.host,
                context="temperature chamber read",
                log_failures=False,
            )
        if self.host._first_method(chamber, ("fetch_all", "status", "read")) is None:
            return None
        return lambda chamber=chamber: self.pick_numeric(
            self.normalize_snapshot(
                read_device_snapshot_with_retry(
                    chamber,
                    host=self.host,
                    context="temperature chamber snapshot",
                    required_keys=("temperature_c", "temp_c", "chamber_temp_c"),
                    retry_on_empty=True,
                    log_failures=False,
                )
            ),
            "temperature_c",
            "temp_c",
            "chamber_temp_c",
        )

    def make_humidity_reader(self, humidity_generator: Any) -> Optional[Callable[[], Optional[float]]]:
        def read_dewpoint_meter_rh() -> Optional[float]:
            dewpoint_meter = self.context.device_manager.get_device("dewpoint_meter")
            if dewpoint_meter is None:
                return None
            snapshot = self.normalize_snapshot(
                read_device_snapshot_with_retry(
                    dewpoint_meter,
                    host=self.host,
                    context="dewpoint humidity read",
                    required_keys=("humidity_pct", "rh_pct", "humidity", "Uw", "Ui"),
                    retry_on_empty=True,
                    log_failures=False,
                )
            )
            return self.pick_humidity_value(snapshot)

        def with_fallback(value: Optional[float]) -> Optional[float]:
            return value if value is not None else read_dewpoint_meter_rh()

        if humidity_generator is not None:
            method = self.host._first_method(humidity_generator, ("read_humidity_pct", "read_rh_pct", "get_humidity_pct"))
            if method is not None:
                return lambda method=method: with_fallback(
                    read_numeric_with_retry(
                        method,
                        host=self.host,
                        context="humidity generator humidity read",
                        transform=lambda raw: self.sanitize_humidity_value(as_float(raw)),
                        log_failures=False,
                    )
                )
            if self.host._first_method(humidity_generator, ("fetch_all", "status", "read")) is not None:
                return lambda humidity_generator=humidity_generator: with_fallback(
                    self.pick_humidity_value(
                        self.normalize_snapshot(
                            read_device_snapshot_with_retry(
                                humidity_generator,
                                host=self.host,
                                context="humidity generator humidity snapshot",
                                required_keys=("humidity_pct", "rh_pct", "humidity", "Uw", "Ui"),
                                retry_on_empty=True,
                                log_failures=False,
                            )
                        )
                    )
                )
        chamber = self.context.device_manager.get_device("temperature_chamber")
        if chamber is not None:
            method = self.host._first_method(chamber, ("read_rh_pct", "read_humidity_pct"))
            if method is not None:
                return lambda method=method: with_fallback(
                    read_numeric_with_retry(
                        method,
                        host=self.host,
                        context="temperature chamber humidity read",
                        transform=lambda raw: self.sanitize_humidity_value(as_float(raw)),
                        log_failures=False,
                    )
                )
        dewpoint_reader = read_dewpoint_meter_rh
        return dewpoint_reader if self.context.device_manager.get_device("dewpoint_meter") is not None else None

    def make_pressure_reader(self) -> Optional[Callable[[], Optional[float]]]:
        for name in ("pressure_meter", "pressure_controller"):
            device = self.context.device_manager.get_device(name)
            if device is None:
                continue
            method = self.host._first_method(device, ("read_pressure", "read_pressure_hpa", "get_pressure"))
            if method is not None:
                return lambda method=method, name=name: read_numeric_with_retry(
                    method,
                    host=self.host,
                    context=f"{name} pressure read",
                    log_failures=False,
                )
            fetch_method = self.host._first_method(device, ("status",))
            if fetch_method is not None:
                return lambda device=device, name=name: self.pick_numeric(
                    self.normalize_snapshot(
                        read_device_snapshot_with_retry(
                            device,
                            host=self.host,
                            context=f"{name} pressure snapshot",
                            required_keys=("pressure_hpa", "pressure"),
                            retry_on_empty=True,
                            log_failures=False,
                        )
                    ),
                    "pressure_hpa",
                    "pressure",
                )
        return None

    def make_pressure_gauge_reader(self) -> Optional[Callable[[], Optional[float]]]:
        device = self.context.device_manager.get_device("pressure_meter")
        if device is None:
            return None
        method = self.host._first_method(device, ("read_pressure", "read_pressure_hpa", "get_pressure"))
        if method is not None:
            return lambda method=method: read_numeric_with_retry(
                method,
                host=self.host,
                context="pressure gauge read",
                log_failures=False,
            )
        if self.host._first_method(device, ("status",)) is not None:
            return lambda device=device: self.pick_numeric(
                self.normalize_snapshot(
                    read_device_snapshot_with_retry(
                        device,
                        host=self.host,
                        context="pressure gauge snapshot",
                        required_keys=("pressure_gauge_hpa", "pressure_hpa", "pressure"),
                        retry_on_empty=True,
                        log_failures=False,
                    )
                ),
                "pressure_gauge_hpa",
                "pressure_hpa",
                "pressure",
            )
        return None

    def make_pressure_gauge_snapshot_reader(self) -> Optional[Callable[[], Dict[str, Any]]]:
        device = self.context.device_manager.get_device("pressure_meter")
        if device is None:
            return None
        method = self.host._first_method(device, ("status", "read_current", "read", "fetch_all"))
        if method is None:
            return None
        return lambda method=method: self._safe_snapshot_read(method)

    def make_thermometer_reader(self) -> Optional[Callable[[], Optional[float]]]:
        device = self.context.device_manager.get_device("thermometer")
        if device is None:
            return None
        method = self.host._first_method(device, ("read_temp_c",))
        if method is not None:
            return lambda method=method: read_numeric_with_retry(
                method,
                host=self.host,
                context="thermometer read",
                log_failures=False,
            )
        if self.host._first_method(device, ("read_current", "status", "read")) is not None:
            return lambda device=device: self.pick_numeric(
                self.normalize_snapshot(
                    read_device_snapshot_with_retry(
                        device,
                        host=self.host,
                        context="thermometer snapshot",
                        required_keys=("thermometer_temp_c", "temp_c", "temperature_c"),
                        retry_on_empty=True,
                        log_failures=False,
                    )
                ),
                "thermometer_temp_c",
                "temp_c",
                "temperature_c",
            )
        return None

    def make_thermometer_snapshot_reader(self) -> Optional[Callable[[], Dict[str, Any]]]:
        device = self.context.device_manager.get_device("thermometer")
        if device is None:
            return None
        method = self.host._first_method(device, ("read_current", "status", "read", "fetch_all"))
        if method is None:
            return None
        return lambda method=method: self._safe_snapshot_read(method)

    def make_signal_reader(self, analyzer: Any) -> Optional[Callable[[], Optional[float]]]:
        if analyzer is None:
            return None

        def ordered_keys() -> tuple[str, ...]:
            point = self.context.session.current_point
            route = str(getattr(point, "route", "") or "").strip().lower()
            if route == "h2o":
                return (
                    "h2o_ratio_f",
                    "h2o_ratio_raw",
                    "h2o_signal",
                    "h2o",
                    "h2o_mmol",
                    "co2_ratio_f",
                    "co2_ratio_raw",
                    "co2_signal",
                    "co2",
                    "co2_ppm",
                )
            return (
                "co2_ratio_f",
                "co2_ratio_raw",
                "co2_signal",
                "co2",
                "co2_ppm",
                "h2o_ratio_f",
                "h2o_ratio_raw",
                "h2o_signal",
                "h2o",
                "h2o_mmol",
            )

        return lambda: self.pick_numeric(
            self.normalize_snapshot(
                self.read_device_snapshot(
                    analyzer,
                    context="analyzer signal read",
                    required_keys=ordered_keys(),
                    retry_on_empty=True,
                    log_failures=False,
                )
            ),
            *ordered_keys(),
        )

    def read_analyzer_snapshot(self, analyzer: Any, *, label: str, context: str = "") -> Any:
        raw_snapshot = self.read_device_snapshot(
            analyzer,
            context=context or f"analyzer {label} read",
            required_keys=self.ANALYZER_FRAME_KEYS,
            retry_on_empty=True,
        )
        snapshot = self.normalize_snapshot(raw_snapshot)
        if self.pick_numeric(snapshot, *self.ANALYZER_FRAME_KEYS) is None:
            raise RuntimeError(f"Analyzer read failed ({label}): no usable frame after retry")
        return raw_snapshot

    def read_device_snapshot(
        self,
        device: Any,
        *,
        context: str = "",
        required_keys: tuple[str, ...] = (),
        retry_on_empty: bool = False,
        log_failures: bool = True,
    ) -> Any:
        return read_device_snapshot_with_retry(
            device,
            host=self.host,
            context=context,
            required_keys=required_keys,
            retry_on_empty=retry_on_empty,
            log_failures=log_failures,
        )

    def sensor_read_retry_settings(self) -> tuple[int, float]:
        return sensor_read_retry_settings_from_host(self.host)

    @staticmethod
    def snapshot_retry_reason(
        snapshot: Any,
        *,
        required_keys: tuple[str, ...],
        retry_on_empty: bool,
    ) -> Optional[str]:
        normalized = SamplingService.normalize_snapshot(snapshot)
        if required_keys and SamplingService.pick_numeric(normalized, *required_keys) is None:
            return f"missing numeric data for keys={','.join(required_keys)}"
        if retry_on_empty and not SamplingService._snapshot_has_data(normalized):
            return "empty snapshot"
        return None

    @staticmethod
    def _snapshot_has_data(snapshot: Dict[str, Any]) -> bool:
        for value in snapshot.values():
            if value is None:
                continue
            if isinstance(value, dict) and not value:
                continue
            if isinstance(value, (list, tuple, set)) and not value:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            return True
        return False

    @staticmethod
    def normalize_snapshot(snapshot: Any) -> Dict[str, Any]:
        if not isinstance(snapshot, dict):
            return {}
        normalized: Dict[str, Any] = {}
        data = safe_get(snapshot, "data", default={})
        if isinstance(data, dict):
            normalized.update(data)
        normalized.update(snapshot)
        return normalized

    @staticmethod
    def pick_numeric(snapshot: Dict[str, Any], *keys: str) -> Optional[float]:
        for key in keys:
            value = as_float(snapshot.get(key))
            if value is not None:
                return float(value)
        return None

    def _safe_snapshot_read(self, method: Callable[[], Any]) -> Dict[str, Any]:
        try:
            return self.normalize_snapshot(method())
        except Exception:
            return {}

    @staticmethod
    def pick_text(snapshot: Dict[str, Any], *keys: str) -> str:
        for key in keys:
            value = snapshot.get(key)
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text
        return ""

    @classmethod
    def pick_humidity_value(cls, snapshot: Dict[str, Any]) -> Optional[float]:
        value = cls.pick_numeric(snapshot, "humidity_pct", "rh_pct", "humidity", "Uw", "Ui")
        return cls.sanitize_humidity_value(value)

    @staticmethod
    def sanitize_humidity_value(value: Optional[float]) -> Optional[float]:
        if value is None:
            return None
        numeric = float(value)
        if numeric < 0.0 or numeric > 100.0:
            return None
        return numeric

    @classmethod
    def standard_analyzer_row_values(cls, result: SamplingResult) -> Dict[str, Any]:
        row: Dict[str, Any] = {}
        for row_key, attr_name in cls.STANDARD_ANALYZER_ROW_FIELDS:
            row[row_key] = getattr(result, attr_name, None)
        return row

    @staticmethod
    def sampling_result_to_row(result: SamplingResult) -> Dict[str, Any]:
        return {
            "timestamp": result.timestamp.isoformat(),
            "point_index": result.point.index,
            "temperature_c": result.point.temperature_c,
            "co2_ppm": result.point.co2_ppm,
            "co2_group": result.point.co2_group,
            "cylinder_nominal_ppm": result.point.cylinder_nominal_ppm,
            "humidity_pct": result.point.humidity_pct,
            "route": result.point.route,
            "analyzer_id": result.analyzer_id,
            "sample_co2_ppm": result.co2_ppm,
            "sample_h2o_mmol": result.h2o_mmol,
            "h2o_signal": result.h2o_signal,
            "co2_signal": result.co2_signal,
            "co2_ratio_f": result.co2_ratio_f,
            "co2_ratio_raw": result.co2_ratio_raw,
            "h2o_ratio_f": result.h2o_ratio_f,
            "h2o_ratio_raw": result.h2o_ratio_raw,
            "ref_signal": result.ref_signal,
            "pressure_hpa": result.pressure_hpa,
            "pressure_gauge_hpa": result.pressure_gauge_hpa,
            "pressure_reference_status": result.pressure_reference_status,
            "thermometer_temp_c": result.thermometer_temp_c,
            "thermometer_reference_status": result.thermometer_reference_status,
            "dew_point_c": result.dew_point_c,
            "analyzer_pressure_kpa": result.analyzer_pressure_kpa,
            "analyzer_chamber_temp_c": result.analyzer_chamber_temp_c,
            "case_temp_c": result.case_temp_c,
            "frame_has_data": result.frame_has_data,
            "frame_usable": result.frame_usable,
            "frame_status": result.frame_status,
            "sample_index": result.sample_index,
        }

    def samples_for_point(
        self,
        point: CalibrationPoint,
        *,
        phase: str = "",
        point_tag: str = "",
    ) -> list[SamplingResult]:
        resolved_tag = str(point_tag or "").strip()
        resolved_phase = str(phase or "").strip().lower()
        samples: list[SamplingResult] = []
        for result in self.context.result_store.get_samples():
            if result.point.index != point.index or result.point.route != point.route:
                continue
            if resolved_tag and str(getattr(result, "point_tag", "") or "").strip() != resolved_tag:
                continue
            if resolved_phase and str(getattr(result, "point_phase", "") or "").strip().lower() not in {"", resolved_phase}:
                continue
            samples.append(result)
        return samples

    @staticmethod
    def span(values: list[float]) -> float:
        if len(values) < 2:
            return 0.0
        return float(max(values) - min(values))

    def evaluate_sample_quality(self, rows: list[dict[str, Any]]) -> tuple[bool, dict[str, float]]:
        qcfg = self.host._cfg_get("workflow.sampling.quality", {})
        if not isinstance(qcfg, dict) or not qcfg.get("enabled", False):
            return True, {}
        limits = {
            "co2_ppm": qcfg.get("max_span_co2_ppm"),
            "h2o_mmol": qcfg.get("max_span_h2o_mmol"),
            "pressure_hpa": qcfg.get("max_span_pressure_hpa"),
            "dewpoint_c": qcfg.get("max_span_dewpoint_c"),
        }
        spans: dict[str, float] = {}
        ok = True
        for key, raw_limit in limits.items():
            if raw_limit is None:
                continue
            values = [float(row[key]) for row in rows if row.get(key) is not None]
            if not values:
                continue
            span = self.span(values)
            spans[key] = span
            if span > float(raw_limit):
                ok = False
        return ok, spans

    def sampling_params(self, phase: str = "") -> tuple[int, float]:
        count = int(self.host._cfg_get("workflow.sampling.stable_count", self.host._cfg_get("workflow.sampling.count", 10)))
        count = max(1, count)
        interval = float(self.host._cfg_get("workflow.sampling.interval_s", 2.0))
        if phase == "co2":
            interval = float(self.host._cfg_get("workflow.sampling.co2_interval_s", interval))
        elif phase == "h2o":
            interval = float(self.host._cfg_get("workflow.sampling.h2o_interval_s", interval))
        if self.host._collect_only_fast_path_enabled():
            return 1, 0.0
        return count, interval

    def summarize_analyzer_integrity(self, rows: list[dict[str, Any]], *, analyzer_labels: list[str]) -> dict[str, Any]:
        expected = len(analyzer_labels)
        with_frame: list[str] = []
        usable: list[str] = []
        missing: list[str] = []
        unusable: list[str] = []
        for label in analyzer_labels:
            prefix = str(label or "").lower().replace(" ", "_")
            has_frame = any(bool(row.get(f"{prefix}_frame_has_data")) for row in rows)
            has_usable = any(bool(row.get(f"{prefix}_frame_usable")) for row in rows)
            display = str(label or "").upper()
            if has_frame:
                with_frame.append(display)
            else:
                missing.append(display)
            if has_usable:
                usable.append(display)
            elif has_frame:
                unusable.append(display)
        usable_count = len(usable)
        with_frame_count = len(with_frame)
        coverage_text = f"{usable_count}/{expected}" if expected else "0/0"
        integrity = "完整" if expected and usable_count == expected else "部分可用"
        if expected == 0:
            integrity = "无分析仪"
        elif usable_count == 0 and with_frame_count == 0:
            integrity = "无帧"
        elif usable_count == 0:
            integrity = "仅异常帧"
        elif missing and unusable:
            integrity = "部分缺失且含异常帧"
        elif missing:
            integrity = "部分缺失"
        elif unusable:
            integrity = "含异常帧"
        return {
            "analyzer_expected_count": expected,
            "analyzer_with_frame_count": with_frame_count,
            "analyzer_usable_count": usable_count,
            "analyzer_coverage_text": coverage_text,
            "analyzer_integrity": integrity,
            "analyzer_missing_labels": ",".join(missing),
            "analyzer_unusable_labels": ",".join(unusable),
        }

    def _read_single_analyzer_snapshot(
        self,
        point: CalibrationPoint,
        label: str,
        analyzer: Any,
        phase: str,
        point_tag: str,
        sample_index: int,
    ) -> tuple[Optional[SamplingResult], Any]:
        raw_snapshot = self.read_analyzer_snapshot(
            analyzer,
            label=label,
            context=f"analyzer {label} batch read",
        )
        result = self.collect_sampling_result(
            point,
            label,
            analyzer,
            phase=phase,
            point_tag=point_tag,
            snapshot=raw_snapshot,
            sample_index=sample_index,
        )
        return result, raw_snapshot

    def collect_sample_batch(
        self,
        point: CalibrationPoint,
        *,
        count: int,
        interval_s: float,
        phase: str,
        point_tag: str,
    ) -> tuple[list[dict[str, Any]], list[SamplingResult]]:
        analyzers = self.host._active_gas_analyzers()
        pressure_reader = self.make_pressure_reader()
        pressure_gauge_reader = self.make_pressure_gauge_reader()
        pressure_gauge_snapshot_reader = self.make_pressure_gauge_snapshot_reader()
        thermometer_reader = self.make_thermometer_reader()
        thermometer_snapshot_reader = self.make_thermometer_snapshot_reader()
        dewpoint = self.host._device("dewpoint_meter")
        chamber = self.host._device("temperature_chamber")
        generator = self.host._device("humidity_generator")
        rows: list[dict[str, Any]] = []
        results: list[SamplingResult] = []
        for sample_index in range(count):
            self.host._check_stop()
            row: dict[str, Any] = {
                "point_index": point.index,
                "point_phase": phase,
                "point_tag": point_tag,
                "sample_index": sample_index + 1,
                "sample_ts": datetime.now(timezone.utc).isoformat(),
                "temp_set_c": point.temp_chamber_c,
                "pressure_target_hpa": point.target_pressure_hpa,
                "co2_ppm_target": point.co2_ppm,
                "co2_group": point.co2_group,
                "cylinder_nominal_ppm": point.cylinder_nominal_ppm,
                "h2o_mmol_target": point.h2o_mmol,
                "point_is_h2o": point.is_h2o_point,
            }
            batch_failures: list[str] = []
            batch_results: list[SamplingResult] = []
            preferred_result: Optional[SamplingResult] = None
            first_usable_result: Optional[SamplingResult] = None
            preferred_analyzer_label = analyzers[0][0] if analyzers else ""
            sample_idx = sample_index + 1
            total_workers = len(analyzers) + 4  # analyzers + pressure + thermometer + chamber_temp + chamber_rh
            with ThreadPoolExecutor(max_workers=total_workers) as executor:
                analyzer_futures: dict[Any, str] = {}
                for label, analyzer, _ in analyzers:
                    future = executor.submit(
                        self._read_single_analyzer_snapshot,
                        point, label, analyzer, phase, point_tag, sample_idx,
                    )
                    analyzer_futures[future] = label
                pressure_snapshot_future = executor.submit(pressure_gauge_snapshot_reader) if pressure_gauge_snapshot_reader is not None else None
                thermometer_snapshot_future = executor.submit(thermometer_snapshot_reader) if thermometer_snapshot_reader is not None else None
                chamber_temp_future = executor.submit(self.make_temperature_reader(chamber)) if chamber is not None and self.make_temperature_reader(chamber) is not None else None
                chamber_rh_method = self.host._first_method(chamber, ("read_rh_pct", "read_humidity_pct")) if chamber is not None else None
                chamber_rh_future = executor.submit(chamber_rh_method) if chamber_rh_method is not None else None

                for future in as_completed(analyzer_futures):
                    label = analyzer_futures[future]
                    prefix = str(label or "").lower().replace(" ", "_")
                    try:
                        result, raw_snapshot = future.result()
                        if result is None:
                            raise RuntimeError("analyzer read returned None")
                        batch_results.append(result)
                        snapshot = self.normalize_snapshot(raw_snapshot)
                        row[f"{prefix}_frame_has_data"] = True
                        row[f"{prefix}_frame_usable"] = True
                        row[f"{prefix}_frame_status"] = "ok"
                        row[f"{prefix}_co2_ppm"] = result.co2_ppm
                        row[f"{prefix}_h2o_mmol"] = result.h2o_mmol
                        row[f"{prefix}_co2_ratio_f"] = result.co2_ratio_f
                        row[f"{prefix}_h2o_ratio_f"] = result.h2o_ratio_f
                        if label == preferred_analyzer_label:
                            preferred_result = result
                        if first_usable_result is None:
                            first_usable_result = result
                        for key, value in snapshot.items():
                            row[f"{prefix}_{key}"] = value
                    except Exception as exc:
                        row[f"{prefix}_frame_has_data"] = False
                        row[f"{prefix}_frame_usable"] = False
                        row[f"{prefix}_frame_status"] = "read_error"
                        row[f"{prefix}_error"] = str(exc)
                        batch_failures.append(label)

            if preferred_result is None:
                preferred_result = first_usable_result
            if preferred_result is not None:
                row.update(self.standard_analyzer_row_values(preferred_result))
            if batch_failures and len(batch_failures) < len(analyzers):
                self.host._disable_analyzers(batch_failures, reason="sample_timeout")

            pressure_hpa = None
            if pressure_reader is not None:
                pressure_hpa = pressure_reader()
            pressure_snapshot = pressure_snapshot_future.result() if pressure_snapshot_future is not None else {}
            pressure_gauge_hpa = self.pick_numeric(pressure_snapshot, "pressure_gauge_hpa", "pressure_hpa", "pressure")
            if pressure_gauge_hpa is None and pressure_hpa is not None:
                pressure_gauge_hpa = pressure_hpa
            if pressure_gauge_hpa is None and pressure_gauge_reader is not None:
                pressure_gauge_hpa = pressure_gauge_reader()
            pressure_reference_status = self.pick_text(
                pressure_snapshot,
                "pressure_reference_status",
                "reference_status",
            )
            thermometer_snapshot = thermometer_snapshot_future.result() if thermometer_snapshot_future is not None else {}
            thermometer_temp_c = self.pick_numeric(thermometer_snapshot, "thermometer_temp_c", "temp_c", "temperature_c")
            if thermometer_temp_c is None and thermometer_reader is not None:
                thermometer_temp_c = thermometer_reader()
            thermometer_reference_status = self.pick_text(
                thermometer_snapshot,
                "thermometer_reference_status",
                "reference_status",
            )
            if pressure_hpa is not None:
                row["pressure_hpa"] = pressure_hpa
            if pressure_gauge_hpa is not None:
                row["pressure_gauge_hpa"] = pressure_gauge_hpa
            if pressure_reference_status:
                row["pressure_reference_status"] = pressure_reference_status
            if thermometer_temp_c is not None:
                row["thermometer_temp_c"] = thermometer_temp_c
            if thermometer_reference_status:
                row["thermometer_reference_status"] = thermometer_reference_status
            if pressure_reader is not None:
                row.setdefault("pressure_hpa", pressure_hpa)

            if dewpoint is not None and self.context.device_manager.get_status("dewpoint_meter") is not DeviceStatus.DISABLED:
                if phase == "h2o" and self.run_state.humidity.preseal_dewpoint_snapshot:
                    row["dewpoint_c"] = self.run_state.humidity.preseal_dewpoint_snapshot.get("dewpoint_c")
                    row["dew_temp_c"] = self.run_state.humidity.preseal_dewpoint_snapshot.get("temp_c")
                    row["dew_rh_pct"] = self.run_state.humidity.preseal_dewpoint_snapshot.get("rh_pct")
                    row["dew_pressure_hpa"] = self.run_state.humidity.preseal_dewpoint_snapshot.get("pressure_hpa")
                    row["dewpoint_sample_ts"] = self.run_state.humidity.preseal_dewpoint_snapshot.get("sample_ts")
                else:
                    snapshot = self.normalize_snapshot(
                        self.read_device_snapshot(
                            dewpoint,
                            context="dewpoint batch snapshot",
                            required_keys=("dewpoint_c", "dew_point_c", "dew_point", "Td"),
                            retry_on_empty=True,
                        )
                    )
                    row["dewpoint_c"] = snapshot.get("dewpoint_c")
                    row["dew_temp_c"] = snapshot.get("temp_c")
                    row["dew_rh_pct"] = snapshot.get("rh_pct")

            if chamber_temp_future is not None:
                row["chamber_temp_c"] = chamber_temp_future.result()
            if chamber_rh_future is not None:
                row["chamber_rh_pct"] = self.host._as_float(chamber_rh_future.result())

            if generator is not None and self.context.device_manager.get_status("humidity_generator") is not DeviceStatus.DISABLED:
                snapshot = self.normalize_snapshot(
                    self.read_device_snapshot(
                        generator,
                        context="humidity generator batch snapshot",
                        retry_on_empty=True,
                    )
                )
                data = safe_get(snapshot, "data", default=snapshot)
                if isinstance(data, dict):
                    for key, value in data.items():
                        row[f"hgen_{key}"] = value

            for result in batch_results:
                results.append(
                    replace(
                        result,
                        sample_index=sample_index + 1,
                        pressure_gauge_hpa=pressure_gauge_hpa,
                        pressure_reference_status=pressure_reference_status,
                        thermometer_temp_c=thermometer_temp_c,
                        thermometer_reference_status=thermometer_reference_status,
                    )
                )
            rows.append(row)
        return rows, results

    def sample_point(self, point: CalibrationPoint, *, phase: str, point_tag: str = "") -> list[SamplingResult]:
        count, interval_s = self.sampling_params(phase=phase)
        qcfg = self.host._cfg_get("workflow.sampling.quality", {})
        retries = int(qcfg.get("retries", 0)) if isinstance(qcfg, dict) and qcfg.get("enabled", False) else 0
        final_rows: list[dict[str, Any]] = []
        final_results: list[SamplingResult] = []
        for attempt in range(retries + 1):
            rows, results = self.collect_sample_batch(
                point,
                count=count,
                interval_s=interval_s,
                phase=phase,
                point_tag=point_tag,
            )
            ok, spans = self.evaluate_sample_quality(rows)
            final_rows = rows
            final_results = results
            if ok:
                break
            if attempt < retries:
                self.host._log(f"Sample quality not met, retry {attempt + 1}/{retries}: spans={spans}")
            else:
                self.host._log(f"Sample quality not met, using last batch: spans={spans}")
        timing = self.host._finish_point_timing(point, phase=phase, point_tag=point_tag)
        stability_time_s = self.host._as_float(timing.get("stability_time_s"))
        total_time_s = self.host._as_float(timing.get("total_time_s"))
        analyzer_labels = [label for label, _, _ in self.host._all_gas_analyzers()]
        integrity = self.summarize_analyzer_integrity(final_rows, analyzer_labels=analyzer_labels)
        for row in final_rows:
            row.update(integrity)
            row["stability_time_s"] = stability_time_s
            row["total_time_s"] = total_time_s
            self.context.run_logger.log_sample(row)
        timed_results = [
            replace(
                result,
                point_phase=str(phase or point.route or "").strip().lower(),
                point_tag=str(point_tag or ""),
                stability_time_s=stability_time_s,
                total_time_s=total_time_s,
            )
            for result in final_results
        ]
        for result in timed_results:
            self.host._append_result(result)
        co2_values = [float(row["co2_ppm"]) for row in final_rows if row.get("co2_ppm") is not None]
        h2o_values = [float(row["h2o_mmol"]) for row in final_rows if row.get("h2o_mmol") is not None]
        pressure_values = [float(row["pressure_hpa"]) for row in final_rows if row.get("pressure_hpa") is not None]
        self.host._log(
            f"Point {point.index} sampled: "
            f"co2_mean={mean(co2_values) if co2_values else None} "
            f"co2_std={stdev(co2_values) if len(co2_values) > 1 else None} "
            f"h2o_mean={mean(h2o_values) if h2o_values else None} "
            f"h2o_std={stdev(h2o_values) if len(h2o_values) > 1 else None} "
            f"pressure_mean={mean(pressure_values) if pressure_values else None} "
            f"stability_time_s={stability_time_s} total_time_s={total_time_s}"
        )
        return timed_results
