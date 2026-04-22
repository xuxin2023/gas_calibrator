-- Migration 003: AI-ready旁路对象 + 治理专用表
-- Per V1.3 sections 8.1 and 8.2
-- Adds: stability, shadow, health, anomaly, feature, model, review, governance tables

-- ============================================================================
-- 8.1 判稳与状态机数据库对象
-- ============================================================================

CREATE TABLE IF NOT EXISTS stability_policy_versions (
    policy_id       TEXT NOT NULL,
    version         INTEGER NOT NULL,
    policy_name     TEXT NOT NULL,
    route_type      TEXT NOT NULL DEFAULT 'both',
    config_json     JSON_VARIANT NOT NULL DEFAULT '{}',
    created_at      TIMESTAMP WITH TIME ZONE,
    created_by      TEXT,
    active          BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (policy_id, version)
);

CREATE TABLE IF NOT EXISTS stability_windows (
    window_id       GUID NOT NULL PRIMARY KEY,
    run_id          GUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    point_id        GUID NOT NULL REFERENCES points(id) ON DELETE CASCADE,
    policy_id       TEXT NOT NULL,
    policy_version  INTEGER NOT NULL,
    window_start_ts TIMESTAMP WITH TIME ZONE,
    window_end_ts   TIMESTAMP WITH TIME ZONE,
    window_duration_s REAL,
    sample_count    INTEGER NOT NULL DEFAULT 0,
    statistics_json JSON_VARIANT NOT NULL DEFAULT '{}',
    decision        TEXT NOT NULL DEFAULT 'pending',
    decision_at     TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS stability_decisions (
    decision_id     GUID NOT NULL PRIMARY KEY,
    window_id       GUID NOT NULL REFERENCES stability_windows(window_id) ON DELETE CASCADE,
    rule_name       TEXT NOT NULL,
    passed          BOOLEAN NOT NULL DEFAULT FALSE,
    value           REAL,
    threshold       REAL,
    message         TEXT,
    decided_at      TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS raw_signal_snapshots (
    snapshot_id     GUID NOT NULL PRIMARY KEY,
    run_id          GUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    point_id        GUID NOT NULL REFERENCES points(id) ON DELETE CASCADE,
    sensor_id       GUID REFERENCES sensors(sensor_id),
    snapshot_ts     TIMESTAMP WITH TIME ZONE,
    signal_type     TEXT NOT NULL,
    signal_value    REAL,
    metadata_json   JSON_VARIANT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS state_transition_logs (
    transition_id   GUID NOT NULL PRIMARY KEY,
    run_id          GUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    from_state      TEXT NOT NULL,
    to_state        TEXT NOT NULL,
    trigger_reason  TEXT NOT NULL,
    trigger_data    JSON_VARIANT NOT NULL DEFAULT '{}',
    transition_ts   TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE IF NOT EXISTS shadow_evaluation_results (
    evaluation_id   GUID NOT NULL PRIMARY KEY,
    run_id          GUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    point_id        GUID NOT NULL REFERENCES points(id) ON DELETE CASCADE,
    main_decision   TEXT NOT NULL,
    shadow_decision TEXT NOT NULL,
    decision_diff   TEXT,
    shadow_score    REAL,
    feature_set_version TEXT,
    model_version   TEXT,
    evaluation_ts   TIMESTAMP WITH TIME ZONE,
    details_json    JSON_VARIANT NOT NULL DEFAULT '{}'
);

-- ============================================================================
-- 8.2 AI-ready 旁路对象
-- ============================================================================

CREATE TABLE IF NOT EXISTS analyzer_health_profiles (
    profile_id      GUID NOT NULL PRIMARY KEY,
    sensor_id       GUID NOT NULL REFERENCES sensors(sensor_id),
    analyzer_id     TEXT NOT NULL,
    analyzer_serial TEXT,
    profile_date    DATE NOT NULL,
    health_score    REAL,
    avg_stability_time_s REAL,
    avg_drift_rate  REAL,
    anomaly_count_30d INTEGER NOT NULL DEFAULT 0,
    last_calibration_at TIMESTAMP WITH TIME ZONE,
    summary_json    JSON_VARIANT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS anomaly_cases (
    case_id         GUID NOT NULL PRIMARY KEY,
    run_id          GUID REFERENCES runs(id) ON DELETE CASCADE,
    sensor_id       GUID REFERENCES sensors(sensor_id),
    anomaly_type    TEXT NOT NULL,
    severity        TEXT NOT NULL DEFAULT 'info',
    state           TEXT NOT NULL DEFAULT 'open',
    device          TEXT,
    window_refs     JSON_VARIANT NOT NULL DEFAULT '[]',
    root_cause_candidates JSON_VARIANT NOT NULL DEFAULT '[]',
    reviewer_conclusion TEXT,
    detected_at     TIMESTAMP WITH TIME ZONE,
    resolved_at     TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS anomaly_labels (
    label_id        GUID NOT NULL PRIMARY KEY,
    case_id         GUID NOT NULL REFERENCES anomaly_cases(case_id) ON DELETE CASCADE,
    tag             TEXT NOT NULL,
    severity        TEXT NOT NULL DEFAULT 'info',
    state           TEXT NOT NULL DEFAULT 'active',
    device          TEXT,
    labeled_by      TEXT,
    labeled_at      TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS feature_snapshots (
    snapshot_id     GUID NOT NULL PRIMARY KEY,
    run_id          GUID REFERENCES runs(id) ON DELETE CASCADE,
    sensor_id       GUID REFERENCES sensors(sensor_id),
    feature_version TEXT NOT NULL,
    window_refs     JSON_VARIANT NOT NULL DEFAULT '[]',
    signal_family   TEXT,
    values_json     JSON_VARIANT NOT NULL DEFAULT '{}',
    linked_decision_diff TEXT,
    snapshot_ts     TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS model_registry (
    model_id        GUID NOT NULL PRIMARY KEY,
    model_name      TEXT NOT NULL,
    model_version   TEXT NOT NULL,
    feature_version TEXT,
    label_version   TEXT,
    model_type      TEXT NOT NULL DEFAULT 'shadow',
    evaluation_metrics JSON_VARIANT NOT NULL DEFAULT '{}',
    release_status  TEXT NOT NULL DEFAULT 'development',
    rollback_target TEXT,
    human_review_required BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP WITH TIME ZONE,
    released_at     TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS model_evaluations (
    evaluation_id   GUID NOT NULL PRIMARY KEY,
    model_id        GUID NOT NULL REFERENCES model_registry(model_id) ON DELETE CASCADE,
    evaluation_type TEXT NOT NULL,
    metrics_json    JSON_VARIANT NOT NULL DEFAULT '{}',
    dataset_scope   TEXT,
    passed          BOOLEAN NOT NULL DEFAULT FALSE,
    evaluated_at    TIMESTAMP WITH TIME ZONE,
    evaluated_by    TEXT
);

CREATE TABLE IF NOT EXISTS review_digests (
    digest_id       GUID NOT NULL PRIMARY KEY,
    run_id          GUID REFERENCES runs(id) ON DELETE CASCADE,
    risk_summary    TEXT,
    evidence_gaps   JSON_VARIANT NOT NULL DEFAULT '[]',
    revalidation_suggestions JSON_VARIANT NOT NULL DEFAULT '[]',
    standards_gap_navigation JSON_VARIANT NOT NULL DEFAULT '{}',
    generated_at    TIMESTAMP WITH TIME ZONE,
    generated_by    TEXT
);

CREATE TABLE IF NOT EXISTS run_risk_scores (
    score_id        GUID NOT NULL PRIMARY KEY,
    run_id          GUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    risk_score      REAL NOT NULL,
    risk_level      TEXT NOT NULL DEFAULT 'low',
    risk_summary    TEXT,
    scored_at       TIMESTAMP WITH TIME ZONE
);

-- ============================================================================
-- 治理专用表 (scope, uncertainty, method confirmation, software validation, WP6)
-- ============================================================================

CREATE TABLE IF NOT EXISTS scope_definitions (
    scope_id        TEXT NOT NULL PRIMARY KEY,
    scope_name      TEXT NOT NULL,
    scope_version   TEXT NOT NULL,
    measurand       TEXT NOT NULL,
    route_type      TEXT NOT NULL,
    environment_mode TEXT NOT NULL DEFAULT 'simulation',
    analyzer_model  TEXT,
    temperature_range_json JSON_VARIANT NOT NULL DEFAULT '{}',
    pressure_range_json    JSON_VARIANT NOT NULL DEFAULT '{}',
    gas_or_humidity_range_json JSON_VARIANT NOT NULL DEFAULT '{}',
    method_version  TEXT,
    algorithm_version TEXT,
    uncertainty_profile_id TEXT,
    decision_rule_id TEXT,
    readiness_status TEXT NOT NULL DEFAULT 'readiness_mapping_only',
    limitation_note  TEXT,
    non_claim_note   TEXT,
    created_at      TIMESTAMP WITH TIME ZONE,
    updated_at      TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS decision_rule_profiles (
    rule_id         TEXT NOT NULL PRIMARY KEY,
    rule_name       TEXT NOT NULL,
    source_standard TEXT,
    acceptance_limit REAL,
    guard_band_policy TEXT NOT NULL DEFAULT 'simple',
    uncertainty_source_scope TEXT,
    pass_fail_rule  TEXT NOT NULL DEFAULT 'simple_guard_band',
    statement_template_id TEXT,
    applicability_scope_id TEXT REFERENCES scope_definitions(scope_id),
    reviewer_gate_required BOOLEAN NOT NULL DEFAULT TRUE,
    exception_clause TEXT,
    created_at      TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS uncertainty_budget_cases (
    case_id         TEXT NOT NULL PRIMARY KEY,
    scope_id        TEXT REFERENCES scope_definitions(scope_id),
    decision_rule_id TEXT REFERENCES decision_rule_profiles(rule_id),
    measurand       TEXT NOT NULL,
    route_type      TEXT NOT NULL,
    budget_level    TEXT NOT NULL DEFAULT 'point',
    combined_standard_uncertainty REAL,
    coverage_factor REAL NOT NULL DEFAULT 2.0,
    expanded_uncertainty REAL,
    effective_degrees_of_freedom REAL,
    golden_case_status TEXT NOT NULL DEFAULT 'not_checked',
    report_rule     TEXT,
    created_at      TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS method_confirmation_protocols (
    protocol_id     TEXT NOT NULL PRIMARY KEY,
    scope_id        TEXT REFERENCES scope_definitions(scope_id),
    protocol_name   TEXT NOT NULL,
    validation_matrix_version TEXT,
    total_items     INTEGER NOT NULL DEFAULT 0,
    passed          INTEGER NOT NULL DEFAULT 0,
    failed          INTEGER NOT NULL DEFAULT 0,
    required_for_scope_passed BOOLEAN NOT NULL DEFAULT FALSE,
    coverage_fraction REAL NOT NULL DEFAULT 0.0,
    created_at      TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS method_validation_results (
    result_id       GUID NOT NULL PRIMARY KEY,
    protocol_id     TEXT NOT NULL REFERENCES method_confirmation_protocols(protocol_id) ON DELETE CASCADE,
    item_id         TEXT NOT NULL,
    category        TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'not_run',
    measured_value  REAL,
    deviation       REAL,
    acceptance_limit REAL,
    evidence_ref    TEXT,
    reviewer_note   TEXT,
    executed_at     TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS software_validation_records (
    record_id       GUID NOT NULL PRIMARY KEY,
    run_id          GUID REFERENCES runs(id) ON DELETE CASCADE,
    artifact_type   TEXT NOT NULL,
    artifact_key    TEXT NOT NULL,
    content_hash    TEXT NOT NULL,
    validation_status TEXT NOT NULL DEFAULT 'pending',
    validated_at    TIMESTAMP WITH TIME ZONE,
    validated_by    TEXT
);

CREATE TABLE IF NOT EXISTS pt_ilc_registry (
    entry_id        GUID NOT NULL PRIMARY KEY,
    scope_id        TEXT REFERENCES scope_definitions(scope_id),
    program_name    TEXT NOT NULL,
    program_type    TEXT NOT NULL DEFAULT 'pt',
    provider        TEXT,
    round_identifier TEXT,
    participation_date DATE,
    result_status   TEXT NOT NULL DEFAULT 'pending',
    performance_evaluation TEXT,
    details_json    JSON_VARIANT NOT NULL DEFAULT '{}'
);

-- ============================================================================
-- 电子签名与审批链表
-- ============================================================================

CREATE TABLE IF NOT EXISTS electronic_signatures (
    signature_id    TEXT NOT NULL PRIMARY KEY,
    person_id       TEXT NOT NULL,
    person_name     TEXT NOT NULL,
    person_role     TEXT NOT NULL,
    intent          TEXT NOT NULL,
    content_hash    TEXT NOT NULL,
    content_description TEXT NOT NULL,
    signed_at       TIMESTAMP WITH TIME ZONE NOT NULL,
    reason          TEXT,
    is_electronic   BOOLEAN NOT NULL DEFAULT TRUE,
    metadata_json   JSON_VARIANT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS approval_chains (
    chain_id        TEXT NOT NULL PRIMARY KEY,
    chain_name      TEXT NOT NULL,
    chain_type      TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at    TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS approval_chain_steps (
    step_id         TEXT NOT NULL,
    chain_id        TEXT NOT NULL REFERENCES approval_chains(chain_id) ON DELETE CASCADE,
    step_name       TEXT NOT NULL,
    required_role   TEXT NOT NULL,
    required_intent TEXT NOT NULL DEFAULT 'approve',
    is_dual_review  BOOLEAN NOT NULL DEFAULT FALSE,
    status          TEXT NOT NULL DEFAULT 'pending',
    signature_ids   JSON_VARIANT NOT NULL DEFAULT '[]',
    PRIMARY KEY (chain_id, step_id)
);

-- ============================================================================
-- 数据保留与归档策略表
-- ============================================================================

CREATE TABLE IF NOT EXISTS retention_policies (
    policy_id       TEXT NOT NULL PRIMARY KEY,
    artifact_category TEXT NOT NULL,
    retention_days  INTEGER NOT NULL,
    archive_after_days INTEGER,
    delete_after_days INTEGER,
    hold_override   BOOLEAN NOT NULL DEFAULT TRUE,
    regulatory_basis TEXT,
    created_at      TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS integrity_seals (
    seal_id         TEXT NOT NULL PRIMARY KEY,
    store_id        TEXT NOT NULL,
    content_hash    TEXT NOT NULL,
    chain_hash      TEXT NOT NULL,
    sealed_at       TIMESTAMP WITH TIME ZONE NOT NULL,
    sealed_by       TEXT NOT NULL,
    content_description TEXT NOT NULL,
    algorithm       TEXT NOT NULL DEFAULT 'sha256'
);

-- ============================================================================
-- 索引
-- ============================================================================

CREATE INDEX IF NOT EXISTS ix_stability_windows_run ON stability_windows(run_id);
CREATE INDEX IF NOT EXISTS ix_stability_windows_point ON stability_windows(point_id);
CREATE INDEX IF NOT EXISTS ix_stability_decisions_window ON stability_decisions(window_id);
CREATE INDEX IF NOT EXISTS ix_raw_signal_run ON raw_signal_snapshots(run_id);
CREATE INDEX IF NOT EXISTS ix_state_transitions_run ON state_transition_logs(run_id);
CREATE INDEX IF NOT EXISTS ix_shadow_eval_run ON shadow_evaluation_results(run_id);
CREATE INDEX IF NOT EXISTS ix_anomaly_cases_run ON anomaly_cases(run_id);
CREATE INDEX IF NOT EXISTS ix_anomaly_cases_sensor ON anomaly_cases(sensor_id);
CREATE INDEX IF NOT EXISTS ix_feature_snapshots_run ON feature_snapshots(run_id);
CREATE INDEX IF NOT EXISTS ix_model_registry_name_version ON model_registry(model_name, model_version);
CREATE INDEX IF NOT EXISTS ix_review_digests_run ON review_digests(run_id);
CREATE INDEX IF NOT EXISTS ix_risk_scores_run ON run_risk_scores(run_id);
CREATE INDEX IF NOT EXISTS ix_method_results_protocol ON method_validation_results(protocol_id);
CREATE INDEX IF NOT EXISTS ix_electronic_sigs_person ON electronic_signatures(person_id);
CREATE INDEX IF NOT EXISTS ix_electronic_sigs_signed_at ON electronic_signatures(signed_at);
CREATE INDEX IF NOT EXISTS ix_approval_chains_status ON approval_chains(status);
CREATE INDEX IF NOT EXISTS ix_integrity_seals_store ON integrity_seals(store_id);
CREATE INDEX IF NOT EXISTS ix_integrity_seals_sealed_at ON integrity_seals(sealed_at);
