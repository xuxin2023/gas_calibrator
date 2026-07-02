from gas_calibrator.validation.v1_ratio_poly_algorithm_audit import (
    build_v1_ratio_poly_algorithm_audit_tables,
    write_v1_ratio_poly_algorithm_audit,
)


def test_v1_ratio_poly_audit_detects_legacy_feature_contract():
    tables = build_v1_ratio_poly_algorithm_audit_tables()

    summary = tables["v1_ratio_poly_algorithm_summary"][0]
    assert summary["feature_count"] == 9
    assert "a8*R*T_k*P" in summary["formula"]
    assert summary["opens_com_ports"] is False
    assert summary["writes_coefficients"] is False

    feature_checks = {row["check"]: row for row in tables["v1_ratio_poly_feature_contract"]}
    assert feature_checks["legacy_default_features"]["status"] == "pass_legacy_v1_formula_terms_detected"
    assert "intercept;R;R2;R3;T;T2;RT;P;RTP" == feature_checks["legacy_default_features"]["observed"]
    assert (
        feature_checks["original_main_intercept_contract"]["status"]
        == "same_as_original_reference_add_intercept_by_default"
    )
    assert (
        feature_checks["v1_v2_intercept_contract"]["status"]
        == "same_intercept_token_default_as_original_reference"
    )
    assert (
        feature_checks["v1_5_formal_intercept_contract"]["status"]
        == "intercept_preserved_and_transformed_to_absolute_firmware_terms"
    )
    assert (
        summary["intercept_contract"]
        == "original_reference_and_v1_v2_and_v1_5_all_keep_intercept"
    )


def test_v1_ratio_poly_audit_flags_senco5_and_pressure_boundaries():
    tables = build_v1_ratio_poly_algorithm_audit_tables()
    mapping = {row["check"]: row for row in tables["v1_ratio_poly_mapping_contract"]}

    assert mapping["legacy_co2_senco_mapping"]["status"] == "conditional_reuse_for_senco1_senco3_only"
    assert (
        mapping["senco5_scope"]["status"]
        == "legacy_v1_mapping_requires_integrated_senco5_output_layer_review"
    )
    assert mapping["pressure_terms"]["status"] == "blocked_for_v1_5_formal_open_flow_fit"
    assert (
        mapping["zero_gas_use"]["status"]
        == "usable_as_CO2_zero_anchor_with_certificate_uncertainty_not_as_O2_percent_CO2"
    )
    decisions = {row["item"]: row for row in tables["v1_ratio_poly_reuse_decisions"]}
    assert decisions["intercept_term"]["decision"] == "keep_but_require_zero_or_low_end_anchor_review"
    assert (
        decisions["low_end_intercept_dispute"]["decision"]
        == "resolve_with_current_state_low_point_and_certified_zero_evidence"
    )


def test_v1_ratio_poly_audit_writes_artifacts(tmp_path):
    outputs = write_v1_ratio_poly_algorithm_audit(output_dir=tmp_path / "audit")

    assert outputs["markdown"].exists()
    assert outputs["meta_json"].exists()
    assert outputs["v1_ratio_poly_algorithm_summary_csv"].exists()
    assert "V1 feature order" in outputs["markdown"].read_text(encoding="utf-8")
