"""
稳定性检测器。

本模块提供统一的稳定性检测逻辑，用于温度、湿度、压力和信号四类数据。
稳定性判定规则为：在配置指定的窗口时间内，采样值极差严格小于容差。
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from statistics import pstdev
import time
from typing import Callable, Deque, Iterable, Optional, Protocol, Sequence

from ..config import (
    HumidityStabilityConfig,
    PressureStabilityConfig,
    SignalStabilityConfig,
    StabilityConfig,
    TemperatureStabilityConfig,
)
from ..exceptions import StabilityTimeoutError
from ..utils import as_float


class StopEventProtocol(Protocol):
    """可用于中止等待过程的事件协议。"""

    def is_set(self) -> bool:
        """返回是否已收到停止信号。"""


class StabilityType(Enum):
    """稳定性检测类型。"""

    TEMPERATURE = "temperature"
    HUMIDITY = "humidity"
    PRESSURE = "pressure"
    SIGNAL = "signal"


@dataclass(frozen=True)
class StabilityResult:
    """稳定性检测结果。"""

    stability_type: StabilityType
    stable: bool
    readings: list[float] = field(default_factory=list)
    range_value: Optional[float] = None
    tolerance: float = 0.0
    elapsed_s: float = 0.0
    window_s: float = 0.0
    timeout_s: float = 0.0
    sample_count: int = 0
    last_value: Optional[float] = None
    std_dev: Optional[float] = None
    min_wait_s: float = 0.0
    timed_out: bool = False
    stopped: bool = False


@dataclass(frozen=True)
class _StabilityRule:
    """单类稳定性检测规则。"""

    tolerance: float
    window_s: float
    timeout_s: float


class StabilityChecker:
    """
    稳定性检测器。

    该类负责将 `StabilityConfig` 中的配置映射为统一的稳定性检测逻辑，
    并提供同步等待接口以便上层流程在设备达到稳定后继续执行。
    """

    def __init__(self, config: StabilityConfig) -> None:
        """
        初始化稳定性检测器。

        Args:
            config: V2 配置模型中的稳定性配置。
        """
        self.config = config
        self._debug_callback: Optional[Callable[[str], None]] = None

    def set_debug_callback(
        self,
        callback: Optional[Callable[[str], None]],
    ) -> None:
        """Register a callback for stability diagnostics."""

        self._debug_callback = callback

    def check_temperature(
        self,
        readings: Sequence[Optional[float]],
        elapsed_s: float,
    ) -> StabilityResult:
        """检查温度是否稳定。"""
        return self._evaluate(
            StabilityType.TEMPERATURE,
            readings,
            elapsed_s,
            self._temperature_rule(),
        )

    def check_humidity(
        self,
        readings: Sequence[Optional[float]],
        elapsed_s: float,
    ) -> StabilityResult:
        """检查湿度是否稳定。"""
        return self._evaluate(
            StabilityType.HUMIDITY,
            readings,
            elapsed_s,
            self._humidity_rule(),
        )

    def check_pressure(
        self,
        readings: Sequence[Optional[float]],
        elapsed_s: float,
    ) -> StabilityResult:
        """检查压力是否稳定。"""
        return self._evaluate(
            StabilityType.PRESSURE,
            readings,
            elapsed_s,
            self._pressure_rule(),
        )

    def check_signal(
        self,
        readings: Sequence[Optional[float]],
        elapsed_s: float,
    ) -> StabilityResult:
        """检查信号是否稳定。"""
        return self._evaluate(
            StabilityType.SIGNAL,
            readings,
            elapsed_s,
            self._signal_rule(),
        )

    def wait_for_stability(
        self,
        stability_type: StabilityType,
        read_func: Callable[[], Optional[float]],
        stop_event: StopEventProtocol,
        *,
        min_wait_s: float = 0.0,
        max_wait_s: Optional[float] = None,
        window_s: Optional[float] = None,
        tolerance: Optional[float] = None,
    ) -> StabilityResult:
        """
        持续采样并等待指定类型达到稳定。

        Args:
            stability_type: 待检测的稳定性类型。
            read_func: 读取单个采样值的回调，返回 `float` 或 `None`。
            stop_event: 用于中断等待过程的事件对象。

        Returns:
            检测结果；若已收到停止信号，则返回 `stopped=True` 的结果。

        Raises:
            StabilityTimeoutError: 在超时时间内未达到稳定时抛出。
        """
        rule = self._override_rule(
            self._rule_for(stability_type),
            timeout_s=max_wait_s,
            window_s=window_s,
            tolerance=tolerance,
        )
        samples: Deque[tuple[float, float]] = deque()
        start_time = time.monotonic()
        poll_interval_s = self._poll_interval(rule.window_s)
        effective_min_wait_s = max(0.0, float(min_wait_s))

        while True:
            if stop_event.is_set():
                result = self._result_from_samples(
                    stability_type=stability_type,
                    samples=samples,
                    elapsed_s=time.monotonic() - start_time,
                    rule=rule,
                    min_wait_s=effective_min_wait_s,
                    stopped=True,
                )
                self._emit_debug(stability_type, "stopped", result)
                return result

            now = time.monotonic()
            elapsed_s = now - start_time
            if elapsed_s > rule.timeout_s:
                break

            value = as_float(read_func())
            if value is not None:
                samples.append((now, float(value)))
                self._trim_samples(samples, now, rule.window_s)

            result = self._result_from_samples(
                stability_type=stability_type,
                samples=samples,
                elapsed_s=elapsed_s,
                rule=rule,
                min_wait_s=effective_min_wait_s,
            )
            phase = "warmup" if elapsed_s < effective_min_wait_s else "evaluate"
            self._emit_debug(stability_type, phase, result)
            if elapsed_s < effective_min_wait_s:
                time.sleep(poll_interval_s)
                continue
            if result.stable:
                self._emit_debug(stability_type, "passed", result)
                return result

            time.sleep(poll_interval_s)

        timeout_elapsed_s = time.monotonic() - start_time
        result = self._result_from_samples(
            stability_type=stability_type,
            samples=samples,
            elapsed_s=timeout_elapsed_s,
            rule=rule,
            min_wait_s=effective_min_wait_s,
            timed_out=True,
        )
        self._emit_debug(stability_type, "timeout", result)
        raise StabilityTimeoutError(
            parameter=stability_type.value,
            actual=result.last_value,
            tolerance=result.tolerance,
            timeout_s=rule.timeout_s,
        )

    def _evaluate(
        self,
        stability_type: StabilityType,
        readings: Sequence[Optional[float]],
        elapsed_s: float,
        rule: _StabilityRule,
        min_wait_s: float = 0.0,
        *,
        timed_out: bool = False,
        stopped: bool = False,
    ) -> StabilityResult:
        values = self._normalize_readings(readings)
        range_value = self._range_of(values)
        std_dev = self._std_of(values)
        stable = (
            len(values) >= 2
            and elapsed_s >= max(rule.window_s, min_wait_s)
            and range_value is not None
            and range_value < rule.tolerance
            and (std_dev is None or std_dev <= rule.tolerance)
        )
        return StabilityResult(
            stability_type=stability_type,
            stable=stable,
            readings=values,
            range_value=range_value,
            tolerance=rule.tolerance,
            elapsed_s=elapsed_s,
            window_s=rule.window_s,
            timeout_s=rule.timeout_s,
            sample_count=len(values),
            last_value=values[-1] if values else None,
            std_dev=std_dev,
            min_wait_s=min_wait_s,
            timed_out=timed_out,
            stopped=stopped,
        )

    def _result_from_samples(
        self,
        stability_type: StabilityType,
        samples: Iterable[tuple[float, float]],
        elapsed_s: float,
        rule: _StabilityRule,
        min_wait_s: float = 0.0,
        *,
        timed_out: bool = False,
        stopped: bool = False,
    ) -> StabilityResult:
        readings = [value for _, value in samples]
        return self._evaluate(
            stability_type,
            readings,
            elapsed_s,
            rule,
            min_wait_s=min_wait_s,
            timed_out=timed_out,
            stopped=stopped,
        )

    @staticmethod
    def _override_rule(
        rule: _StabilityRule,
        *,
        timeout_s: Optional[float] = None,
        window_s: Optional[float] = None,
        tolerance: Optional[float] = None,
    ) -> _StabilityRule:
        return _StabilityRule(
            tolerance=float(rule.tolerance if tolerance is None else tolerance),
            window_s=float(rule.window_s if window_s is None else window_s),
            timeout_s=float(rule.timeout_s if timeout_s is None else timeout_s),
        )

    def _rule_for(self, stability_type: StabilityType) -> _StabilityRule:
        if stability_type is StabilityType.TEMPERATURE:
            return self._temperature_rule()
        if stability_type is StabilityType.HUMIDITY:
            return self._humidity_rule()
        if stability_type is StabilityType.PRESSURE:
            return self._pressure_rule()
        return self._signal_rule()

    def _temperature_rule(self) -> _StabilityRule:
        return self._rule_from_temperature_config(self.config.temperature)

    def _humidity_rule(self) -> _StabilityRule:
        return self._rule_from_humidity_config(self.config.humidity)

    def _pressure_rule(self) -> _StabilityRule:
        return self._rule_from_pressure_config(self.config.pressure)

    def _signal_rule(self) -> _StabilityRule:
        return self._rule_from_signal_config(self.config.signal)

    @staticmethod
    def _rule_from_temperature_config(
        config: TemperatureStabilityConfig,
    ) -> _StabilityRule:
        return _StabilityRule(
            tolerance=float(config.tol),
            window_s=float(config.window_s),
            timeout_s=float(config.timeout_s),
        )

    @staticmethod
    def _rule_from_humidity_config(
        config: HumidityStabilityConfig,
    ) -> _StabilityRule:
        return _StabilityRule(
            tolerance=float(config.tol_dp),
            window_s=float(config.window_s),
            timeout_s=float(config.timeout_s),
        )

    @staticmethod
    def _rule_from_pressure_config(
        config: PressureStabilityConfig,
    ) -> _StabilityRule:
        return _StabilityRule(
            tolerance=float(config.tol_hpa),
            window_s=float(config.window_s),
            timeout_s=float(config.timeout_s),
        )

    @staticmethod
    def _rule_from_signal_config(
        config: SignalStabilityConfig,
    ) -> _StabilityRule:
        return _StabilityRule(
            tolerance=float(config.tol_pct),
            window_s=float(config.window_s),
            timeout_s=float(config.timeout_s),
        )

    @staticmethod
    def _normalize_readings(
        readings: Sequence[Optional[float]],
    ) -> list[float]:
        values: list[float] = []
        for reading in readings:
            value = as_float(reading)
            if value is not None:
                values.append(float(value))
        return values

    @staticmethod
    def _range_of(readings: Sequence[float]) -> Optional[float]:
        if not readings:
            return None
        return max(readings) - min(readings)

    @staticmethod
    def _std_of(readings: Sequence[float]) -> Optional[float]:
        if len(readings) < 2:
            return None
        return float(pstdev(readings))

    @staticmethod
    def _trim_samples(
        samples: Deque[tuple[float, float]],
        now: float,
        window_s: float,
    ) -> None:
        while samples and (now - samples[0][0]) > window_s:
            samples.popleft()

    @staticmethod
    def _poll_interval(window_s: float) -> float:
        return max(0.1, min(1.0, window_s / 10.0))

    def _emit_debug(
        self,
        stability_type: StabilityType,
        phase: str,
        result: StabilityResult,
    ) -> None:
        if self._debug_callback is None:
            return
        message = (
            f"[stability] type={stability_type.value} phase={phase} "
            f"elapsed={result.elapsed_s:.1f}s min_wait={result.min_wait_s:.1f}s "
            f"window={result.window_s:.1f}s value={self._format_optional(result.last_value)} "
            f"range={self._format_optional(result.range_value)} "
            f"std={self._format_optional(result.std_dev)} "
            f"tol={result.tolerance:.3f} samples={result.sample_count} stable={result.stable}"
        )
        self._debug_callback(message)

    @staticmethod
    def _format_optional(value: Optional[float]) -> str:
        if value is None:
            return "n/a"
        return f"{value:.3f}"
