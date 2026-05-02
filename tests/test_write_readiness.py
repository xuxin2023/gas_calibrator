from __future__ import annotations

from gas_calibrator.coefficients.write_readiness import (
    build_write_readiness_decision,
    summarize_runtime_standard_validation,
)


def test_write_readiness_requires_runtime_parity_audit() -> None:
    result = build_write_readiness_decision(
        fit_quality="pass",
        delivery_recommendation="ok",
        coefficient_source="simplified",
        writeback_status="pass",
        runtime_parity_verdict="not_audited",
        legacy_stream_only=False,
    )

    assert result["final_write_ready"] is False
    assert result["readiness_code"] == "runtime_parity_not_audited"


def test_write_readiness_blocks_runtime_parity_fail() -> None:
    result = build_write_readiness_decision(
        fit_quality="pass",
        delivery_recommendation="ok",
        coefficient_source="simplified",
        writeback_status="pass",
        runtime_parity_verdict="parity_fail",
        legacy_stream_only=False,
    )

    assert result["final_write_ready"] is False
    assert result["readiness_code"] == "runtime_parity_fail"


def test_write_readiness_allows_only_full_three_gate_pass() -> None:
    result = build_write_readiness_decision(
        fit_quality="pass",
        delivery_recommendation="ok",
        coefficient_source="simplified",
        writeback_status="pass",
        runtime_parity_verdict="parity_pass",
        legacy_stream_only=False,
    )

    assert result["final_write_ready"] is True
    assert result["readiness_code"] == "all_gates_passed"


def test_write_readiness_blocks_legacy_stream_even_if_other_gates_pass() -> None:
    result = build_write_readiness_decision(
        fit_quality="pass",
        delivery_recommendation="ok",
        coefficient_source="simplified",
        writeback_status="pass",
        runtime_parity_verdict="parity_inconclusive_missing_runtime_inputs",
        legacy_stream_only=True,
    )

    assert result["final_write_ready"] is False
    assert result["readiness_code"] == "legacy_stream_insufficient_for_runtime_parity"
    assert result["readiness_reason"] == "legacy_stream_insufficient_for_runtime_parity"


def test_runtime_standard_validation_summarizes_low_mid_pass_with_high_end_review_flag() -> None:
    result = summarize_runtime_standard_validation(
        {
            "offset_trim_status": "pass",
            "high_point_rows": [
                {"target_ppm": 400, "verdict": "pass"},
                {"target_ppm": 600, "verdict": "pass"},
                {"target_ppm": 1000, "verdict": "review"},
            ],
        }
    )

    assert result["status"] == "runtime_standard_validation_pass_low_mid"
    assert result["quality"] == "pass"
    assert result["high_end_review_needed"] is True
    assert result["passed_targets_ppm"] == [400, 600]
    assert result["review_targets_ppm"] == [1000]


def test_write_readiness_can_block_single_point_only_runtime_validation() -> None:
    result = build_write_readiness_decision(
        fit_quality="pass",
        delivery_recommendation="ok",
        coefficient_source="simplified",
        writeback_status="pass",
        runtime_parity_verdict="parity_pass",
        legacy_stream_only=False,
        runtime_standard_validation_status="offset_trim_pass_single_point",
    )

    assert result["final_write_ready"] is False
    assert result["readiness_code"] == "runtime_standard_validation_single_point_only"
