import pytest

from gas_calibrator.tools.run_v1_corrected_autodelivery import write_senco_groups_with_full_verification


def _coeffs(*values: float) -> list[float]:
    return [float(value) for value in values]


class _FaultInjectionAnalyzerBase:
    def __init__(
        self,
        *,
        before_values: list[float] | None = None,
        read_behaviors: list[object] | None = None,
        mode2_result: bool = True,
        mode2_exception: Exception | None = None,
        mode1_result: bool = True,
        mode1_exception: Exception | None = None,
        target_write_exception: Exception | None = None,
        rollback_write_exception: Exception | None = None,
        snapshot_after_restore: object = "CURRENT_MODE",
    ) -> None:
        self.before_values = list(before_values or _coeffs(10.0, 20.0, 30.0, 40.0, 0.0, 0.0))
        self.current_values = list(self.before_values)
        self.read_behaviors = list(read_behaviors or ["BEFORE", "CURRENT"])
        self.mode2_result = bool(mode2_result)
        self.mode2_exception = mode2_exception
        self.mode1_result = bool(mode1_result)
        self.mode1_exception = mode1_exception
        self.target_write_exception = target_write_exception
        self.rollback_write_exception = rollback_write_exception
        self.snapshot_after_restore = snapshot_after_restore
        self.mode = 1
        self.mode_calls: list[int] = []
        self.write_calls: list[tuple[int, tuple[float, ...]]] = []

    def set_mode_with_ack(self, mode: int, *, require_ack: bool = True) -> bool:
        self.mode_calls.append(int(mode))
        if int(mode) == 2:
            if self.mode2_exception is not None:
                raise self.mode2_exception
            if self.mode2_result:
                self.mode = 2
            return self.mode2_result
        if self.mode1_exception is not None:
            raise self.mode1_exception
        if self.mode1_result:
            self.mode = 1
        return self.mode1_result

    def set_senco(self, group: int, *coeffs) -> bool:
        values = list(coeffs[0]) if len(coeffs) == 1 and isinstance(coeffs[0], (list, tuple)) else list(coeffs)
        normalized = tuple(float(value) for value in values)
        self.write_calls.append((int(group), normalized))
        is_rollback = list(normalized) == list(self.before_values) and list(self.current_values) != list(self.before_values)
        if is_rollback and self.rollback_write_exception is not None:
            raise self.rollback_write_exception
        if list(normalized) != list(self.before_values) and self.target_write_exception is not None:
            raise self.target_write_exception
        self.current_values = list(normalized)
        return True

    def read_coefficient_group(self, group: int):
        behavior = self.read_behaviors.pop(0) if self.read_behaviors else "CURRENT"
        if isinstance(behavior, Exception):
            raise behavior
        if behavior == "EMPTY":
            return None
        if behavior == "PARSE_BAD":
            return {"C0": "bad", "C1": 2.0, "C2": 3.0, "C3": 4.0, "C4": 0.0, "C5": 0.0}
        if behavior == "BEFORE":
            values = list(self.before_values)
        elif behavior == "MISMATCH":
            values = [float(value) + 0.5 for value in self.current_values]
        elif isinstance(behavior, (list, tuple)):
            values = [float(value) for value in behavior]
        else:
            values = list(self.current_values)
        return {f"C{idx}": float(value) for idx, value in enumerate(values)}


class _FaultInjectionAnalyzer(_FaultInjectionAnalyzerBase):
    def read_current_mode_snapshot(self):
        if self.mode_calls and self.mode_calls[-1] == 1:
            if self.snapshot_after_restore is None:
                return None
            if self.snapshot_after_restore == "UNKNOWN":
                return {"mode": None}
            if self.snapshot_after_restore == "MODE2":
                return {"mode": 2}
        return {"mode": self.mode}


class _FaultInjectionAnalyzerNoSnapshot(_FaultInjectionAnalyzerBase):
    pass


def _run_helper(analyzer) -> dict:
    return write_senco_groups_with_full_verification(
        analyzer,
        expected_groups={1: _coeffs(1.0, 2.0, 3.0, 4.0, 0.0, 0.0)},
        readback_attempts=1,
        retry_delay_s=0.0,
    )


def test_fault_set_mode2_failure_still_attempts_restore() -> None:
    analyzer = _FaultInjectionAnalyzer(mode2_result=False, read_behaviors=["BEFORE"])

    result = _run_helper(analyzer)

    assert result["ok"] is False
    assert result["unsafe"] is False
    assert result["mode_exit_attempted"] is True
    assert result["mode_exit_confirmed"] is True
    assert result["write_status"] == "failed"
    assert result["verify_status"] == "failed"
    assert analyzer.mode_calls == [2, 1]


def test_fault_set_senco_exception_rolls_back_and_restores() -> None:
    analyzer = _FaultInjectionAnalyzer(
        read_behaviors=["BEFORE", "BEFORE"],
        target_write_exception=RuntimeError("boom-write"),
    )

    result = _run_helper(analyzer)

    assert result["ok"] is False
    assert result["unsafe"] is False
    assert result["rollback_attempted"] is True
    assert result["rollback_confirmed"] is True
    assert result["rollback_status"] == "success"
    assert result["mode_exit_attempted"] is True
    assert result["mode_exit_confirmed"] is True
    assert "boom-write" in result["failure_reason"]
    assert analyzer.mode_calls == [2, 1]


@pytest.mark.parametrize(
    ("read_behaviors", "expected_error"),
    [
        ([ "BEFORE", TimeoutError("GETCO_TIMEOUT"), "BEFORE"], "GETCO_TIMEOUT"),
        ([ "BEFORE", "EMPTY", "BEFORE"], "READBACK_EMPTY"),
        ([ "BEFORE", "PARSE_BAD", "BEFORE"], "READBACK_PARSE_ERROR"),
        ([ "BEFORE", "MISMATCH", "BEFORE"], "READBACK_MISMATCH"),
    ],
)
def test_fault_readback_failures_roll_back_and_restore(read_behaviors, expected_error: str) -> None:
    analyzer = _FaultInjectionAnalyzer(read_behaviors=list(read_behaviors))

    result = _run_helper(analyzer)

    assert result["ok"] is False
    assert result["unsafe"] is False
    assert result["write_status"] == "success"
    assert result["verify_status"] == "failed"
    assert result["rollback_attempted"] is True
    assert result["rollback_confirmed"] is True
    assert result["rollback_status"] == "success"
    assert result["mode_exit_attempted"] is True
    assert result["mode_exit_confirmed"] is True
    assert expected_error in result["failure_reason"]
    assert analyzer.mode_calls == [2, 1]


def test_fault_rollback_write_failure_marks_unsafe() -> None:
    analyzer = _FaultInjectionAnalyzer(
        read_behaviors=["BEFORE", "MISMATCH"],
        rollback_write_exception=RuntimeError("rollback-boom"),
    )

    result = _run_helper(analyzer)

    assert result["ok"] is False
    assert result["unsafe"] is True
    assert result["rollback_attempted"] is True
    assert result["rollback_confirmed"] is False
    assert result["rollback_status"] == "failed"
    assert "rollback-boom" in result["failure_reason"]
    assert analyzer.mode_calls == [2, 1]


def test_fault_set_mode1_exit_failure_is_unsafe() -> None:
    analyzer = _FaultInjectionAnalyzer(
        read_behaviors=["BEFORE", "CURRENT"],
        mode1_result=False,
        snapshot_after_restore="MODE2",
    )

    result = _run_helper(analyzer)

    assert result["ok"] is False
    assert result["unsafe"] is True
    assert result["mode_exit_attempted"] is True
    assert result["mode_exit_confirmed"] is False
    assert result["mode_after"] == 2
    assert "MODE=1 not acknowledged during restore" in result["failure_reason"]
    assert analyzer.mode_calls == [2, 1]


def test_fault_missing_mode_snapshot_marks_exit_unconfirmed_unsafe() -> None:
    analyzer = _FaultInjectionAnalyzerNoSnapshot(read_behaviors=["BEFORE", "CURRENT"])

    result = _run_helper(analyzer)

    assert result["ok"] is False
    assert result["unsafe"] is True
    assert result["mode_before"] == "UNKNOWN"
    assert result["mode_after"] == "UNKNOWN"
    assert result["mode_exit_attempted"] is True
    assert result["mode_exit_confirmed"] is False
    assert "mode_exit_unconfirmed" in result["failure_reason"]
    assert analyzer.mode_calls == [2, 1]


def test_fault_exit_attempt_without_confirmation_is_unsafe() -> None:
    analyzer = _FaultInjectionAnalyzer(
        read_behaviors=["BEFORE", "CURRENT"],
        snapshot_after_restore=None,
    )

    result = _run_helper(analyzer)

    assert result["ok"] is False
    assert result["unsafe"] is True
    assert result["mode_exit_attempted"] is True
    assert result["mode_exit_confirmed"] is False
    assert "mode_exit_unconfirmed" in result["failure_reason"]
    assert analyzer.mode_calls == [2, 1]
