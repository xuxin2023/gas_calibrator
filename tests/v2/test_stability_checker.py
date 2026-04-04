import threading

import pytest

from gas_calibrator.v2.config import StabilityConfig
from gas_calibrator.v2.core.stability_checker import (
    StabilityChecker,
    StabilityResult,
    StabilityType,
)
from gas_calibrator.v2.exceptions import StabilityTimeoutError


class FakeClock:
    def __init__(self) -> None:
        self.current = 0.0

    def monotonic(self) -> float:
        return self.current

    def sleep(self, seconds: float) -> None:
        self.current += seconds


def _make_checker() -> StabilityChecker:
    return StabilityChecker(
        StabilityConfig.from_dict(
            {
                "temperature": {"tol": 0.2, "window_s": 4.0, "timeout_s": 20.0},
                "humidity": {"tol_dp": 0.3, "window_s": 3.0, "timeout_s": 10.0},
                "pressure": {"tol_hpa": 0.5, "window_s": 2.0, "timeout_s": 8.0},
                "signal": {"tol_pct": 0.1, "window_s": 2.0, "timeout_s": 6.0},
            }
        )
    )


def test_check_temperature_returns_stable_result() -> None:
    checker = _make_checker()

    result = checker.check_temperature([20.0, 20.05, 20.1], elapsed_s=4.0)

    assert isinstance(result, StabilityResult)
    assert result.stable is True
    assert result.range_value == pytest.approx(0.1)
    assert result.tolerance == pytest.approx(0.2)
    assert result.sample_count == 3


def test_check_temperature_requires_full_window_elapsed() -> None:
    checker = _make_checker()

    result = checker.check_temperature([20.0, 20.02, 20.03], elapsed_s=3.9)

    assert result.stable is False
    assert result.range_value == pytest.approx(0.03)


def test_check_humidity_uses_strict_less_than_tolerance() -> None:
    checker = _make_checker()

    result = checker.check_humidity([10.0, 10.3], elapsed_s=3.0)

    assert result.stable is False
    assert result.range_value == pytest.approx(0.3)


def test_check_pressure_filters_invalid_readings() -> None:
    checker = _make_checker()

    result = checker.check_pressure([1000.0, None, "1000.2", "bad"], elapsed_s=2.0)

    assert result.stable is True
    assert result.readings == [1000.0, 1000.2]
    assert result.range_value == pytest.approx(0.2)


def test_check_signal_requires_at_least_two_samples() -> None:
    checker = _make_checker()

    result = checker.check_signal([1.0], elapsed_s=2.0)

    assert result.stable is False
    assert result.sample_count == 1


def test_wait_for_stability_returns_when_temperature_becomes_stable(monkeypatch: pytest.MonkeyPatch) -> None:
    checker = _make_checker()
    clock = FakeClock()
    values = iter([20.0, 20.6, 20.1, 20.12, 20.09, 20.11, 20.1])

    monkeypatch.setattr(
        "gas_calibrator.v2.core.stability_checker.time.monotonic",
        clock.monotonic,
    )
    monkeypatch.setattr(
        "gas_calibrator.v2.core.stability_checker.time.sleep",
        clock.sleep,
    )

    result = checker.wait_for_stability(
        StabilityType.TEMPERATURE,
        lambda: next(values, 20.1),
        threading.Event(),
    )

    assert result.stable is True
    assert result.elapsed_s >= result.window_s
    assert result.range_value is not None
    assert result.range_value < result.tolerance


def test_wait_for_stability_raises_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    checker = _make_checker()
    clock = FakeClock()

    monkeypatch.setattr(
        "gas_calibrator.v2.core.stability_checker.time.monotonic",
        clock.monotonic,
    )
    monkeypatch.setattr(
        "gas_calibrator.v2.core.stability_checker.time.sleep",
        clock.sleep,
    )

    with pytest.raises(StabilityTimeoutError) as exc_info:
        checker.wait_for_stability(
            StabilityType.SIGNAL,
            lambda: 1.0 if int(clock.current) % 2 == 0 else 1.5,
            threading.Event(),
        )

    assert exc_info.value.context["parameter"] == "signal"
    assert exc_info.value.context["timeout_s"] == pytest.approx(6.0)


def test_wait_for_stability_returns_stopped_result(monkeypatch: pytest.MonkeyPatch) -> None:
    checker = _make_checker()
    clock = FakeClock()
    stop_event = threading.Event()

    def read_func() -> float:
        stop_event.set()
        return 20.0

    monkeypatch.setattr(
        "gas_calibrator.v2.core.stability_checker.time.monotonic",
        clock.monotonic,
    )
    monkeypatch.setattr(
        "gas_calibrator.v2.core.stability_checker.time.sleep",
        clock.sleep,
    )

    result = checker.wait_for_stability(
        StabilityType.TEMPERATURE,
        read_func,
        stop_event,
    )

    assert result.stable is False
    assert result.stopped is True
    assert result.timed_out is False
