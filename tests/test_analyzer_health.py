from __future__ import annotations

from gas_calibrator.validation.analyzer_health import (
    build_analyzer_health,
    build_instrument_health,
)


def test_analyzer_health_orders_evaluated_rows_by_score() -> None:
    features = {
        "analyzers": [
            {
                "analyzer_id": "ga_bad",
                "analyzer_serial": "SN02",
                "run_count": 2,
                "sample_count": 10,
                "fit_result_count": 1,
                "mean_rmse": 0.08,
                "mean_r_squared": 0.96,
                "qc_fail_count": 3,
                "alarm_count": 4,
            },
            {
                "analyzer_id": "ga_good",
                "analyzer_serial": "SN01",
                "run_count": 3,
                "sample_count": 30,
                "fit_result_count": 3,
                "mean_rmse": 0.02,
                "mean_r_squared": 0.998,
                "qc_fail_count": 1,
                "alarm_count": 0,
            },
        ]
    }

    report = build_analyzer_health(features)

    assert report["analyzer_count"] == 2
    assert report["evaluated_count"] == 2
    assert report["not_evaluated_count"] == 0
    assert report["analyzers"][0]["analyzer_id"] == "ga_bad"
    assert (
        report["analyzers"][0]["health_score"]
        < report["analyzers"][1]["health_score"]
    )
    assert report["analyzers"][1]["status"] == "healthy"
    assert report["evaluation_scope"] == "offline_features_only"
    assert report["not_real_acceptance_evidence"] is True


def test_analyzer_health_does_not_score_missing_measurement_evidence() -> None:
    report = build_analyzer_health(
        {
            "analyzers": [
                {
                    "analyzer_id": "ga_missing",
                    "run_count": 0,
                    "sample_count": 0,
                    "fit_result_count": 0,
                }
            ]
        }
    )

    row = report["analyzers"][0]
    assert row["health_score"] is None
    assert row["status"] == "not_evaluated"
    assert row["qc_fail_rate"] is None
    assert row["alarm_density"] is None
    assert report["evaluated_count"] == 0
    assert report["not_evaluated_count"] == 1


def test_analyzer_health_penalizes_missing_fit_result() -> None:
    common = {
        "run_count": 2,
        "sample_count": 10,
        "mean_rmse": 0.01,
        "qc_fail_count": 0,
        "alarm_count": 0,
    }
    report = build_analyzer_health(
        {
            "analyzers": [
                {"analyzer_id": "without_fit", "fit_result_count": 0, **common},
                {"analyzer_id": "with_fit", "fit_result_count": 1, **common},
            ]
        }
    )
    rows = {row["analyzer_id"]: row for row in report["analyzers"]}

    assert (
        rows["with_fit"]["health_score"] - rows["without_fit"]["health_score"]
        == 10.0
    )


def test_instrument_health_scores_complete_offline_evidence() -> None:
    features = {
        "analyzer_features": [
            {
                "analyzer_label": "ga01",
                "frame_count": 10,
                "run_count": 2,
                "usable_rate": 0.95,
                "abnormal_status_count": 0,
                "point_count": 4,
                "qc_fail_count": 0,
                "history": [
                    {
                        "mean_co2_ratio_f": 1.00,
                        "mean_h2o_ratio_f": 0.70,
                        "mean_rmse": 0.02,
                    },
                    {
                        "mean_co2_ratio_f": 1.01,
                        "mean_h2o_ratio_f": 0.71,
                        "mean_rmse": 0.02,
                    },
                ],
            },
            {
                "analyzer_label": "ga02",
                "frame_count": 10,
                "run_count": 2,
                "usable_rate": 0.40,
                "abnormal_status_count": 4,
                "point_count": 4,
                "qc_fail_count": 2,
                "history": [
                    {
                        "mean_co2_ratio_f": 1.00,
                        "mean_h2o_ratio_f": 0.70,
                        "mean_rmse": 0.02,
                    },
                    {
                        "mean_co2_ratio_f": 1.05,
                        "mean_h2o_ratio_f": 0.76,
                        "mean_rmse": 0.06,
                    },
                ],
            },
        ]
    }

    report = build_instrument_health(features)

    assert report["analyzer_count"] == 2
    assert report["evaluated_count"] == 2
    assert (
        report["analyzers"][0]["health_score"]
        > report["analyzers"][1]["health_score"]
    )
    assert report["average_health_score"] is not None
    assert report["evaluation_scope"] == "offline_frames_and_qc_only"
    assert report["not_real_acceptance_evidence"] is True


def test_instrument_health_does_not_score_missing_frames_or_points() -> None:
    report = build_instrument_health(
        {
            "analyzer_features": [
                {
                    "analyzer_label": "ga_missing",
                    "frame_count": 0,
                    "point_count": 0,
                    "run_count": 0,
                    "usable_rate": 1.0,
                }
            ]
        }
    )

    row = report["analyzers"][0]
    assert row["health_score"] is None
    assert row["health_band"] == "not_evaluated"
    assert row["abnormal_status_rate"] is None
    assert row["qc_fail_rate"] is None
    assert report["average_health_score"] is None
    assert report["not_evaluated_count"] == 1


def test_instrument_health_keeps_short_history_as_unknown_drift() -> None:
    report = build_instrument_health(
        {
            "analyzer_features": [
                {
                    "analyzer_label": "ga_short_history",
                    "frame_count": 10,
                    "point_count": 2,
                    "run_count": 1,
                    "usable_rate": 0.9,
                    "history": [{"mean_co2_ratio_f": 1.0}],
                }
            ]
        }
    )

    row = report["analyzers"][0]
    assert row["health_score"] is not None
    assert row["drift_penalty"] is None
    assert row["drift_status"] == "not_evaluated"
    assert report["evaluated_count"] == 1
