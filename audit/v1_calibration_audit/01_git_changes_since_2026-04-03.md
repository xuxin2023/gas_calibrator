# 2026-04-03 以来可能影响 V1 校准的改动

- 当前分支: `main`
- 当前 HEAD: `f41b7b20c35a5051943fecd35bdaf62c05ae8d34`
- 筛选起点: `2026-04-03 00:00:00`
- 纳入 commit 数: `371`

## Commit 列表

### `f41b7b20c35a5051943fecd35bdaf62c05ae8d34`
- 时间: 2026-04-12 23:38:39 +0800
- 标题: chore: sync 2026-04-12 23:38:38
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3588,0 +3589,2 @@ class AppFacade: +            family_key="analytics",
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3588,0 +3589,2 @@ class AppFacade: +            family_budget=_family_budgets.get("analytics"),
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3633,0 +3636,2 @@ class AppFacade: +                family_key="offline_diagnostic",
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3633,0 +3636,2 @@ class AppFacade: +                family_budget=_family_budgets.get("offline_diagnostic"),
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3647,0 +3652,6 @@ class AppFacade: +        # Build per-family budget summary for diagnostics

### `bef6db703cf0c33db4731734a606db033cc598dc`
- 时间: 2026-04-12 22:03:30 +0800
- 标题: chore: sync 2026-04-12 22:03:29
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, db
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3374,0 +3375,2 @@ class AppFacade: +            family_key="stability",
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3374,0 +3375,2 @@ class AppFacade: +            family_budget=_family_budgets.get("stability"),
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3395,0 +3398,2 @@ class AppFacade: +            family_key="state_transition",
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3395,0 +3398,2 @@ class AppFacade: +            family_budget=_family_budgets.get("state_transition"),
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3416,0 +3421,2 @@ class AppFacade: +            family_key="measurement_phase_coverage",

### `42c1e838aa5bc30a37fa035902967ae86ca7bb95`
- 时间: 2026-04-12 21:58:30 +0800
- 标题: chore: sync 2026-04-12 21:58:30
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -105,0 +106,7 @@ from ..review_scope_export_index import ( +from ..review_center_scan_contracts import (
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -105,0 +106,7 @@ from ..review_scope_export_index import ( +    REVIEW_CENTER_SCAN_CONTRACTS_VERSION as _SCAN_CONTRACTS_VERSION,
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -105,0 +106,7 @@ from ..review_scope_export_index import ( +    FAMILY_KEY_TO_BUDGET as _FAMILY_KEY_TO_BUDGET,
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -105,0 +106,7 @@ from ..review_scope_export_index import ( +    FAMILY_SCAN_ORDER as _FAMILY_SCAN_ORDER,
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -105,0 +106,7 @@ from ..review_scope_export_index import ( +    allocate_family_budgets as _allocate_family_budgets,

### `fb152b44491920315c86b692ec44a50b9e033a58`
- 时间: 2026-04-12 21:53:30 +0800
- 标题: chore: sync 2026-04-12 21:53:30
- 涉及文件: `src/gas_calibrator/v2/ui_v2/review_center_scan_contracts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Point, STEP, Step, V1, cali, mode, point, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/review_center_scan_contracts.py @@ -0,0 +1,213 @@ +"""Review center evidence scan contracts — family-aware budget, priority, and ordering.
  - src/gas_calibrator/v2/ui_v2/review_center_scan_contracts.py @@ -0,0 +1,213 @@ +
  - src/gas_calibrator/v2/ui_v2/review_center_scan_contracts.py @@ -0,0 +1,213 @@ +This module is the single source of truth for:
  - src/gas_calibrator/v2/ui_v2/review_center_scan_contracts.py @@ -0,0 +1,213 @@ +- artifact family ordering (scan sequence)
  - src/gas_calibrator/v2/ui_v2/review_center_scan_contracts.py @@ -0,0 +1,213 @@ +- per-family scan budget allocation

### `b9894d22c4754ca681b73bf43d2e1036e520b57b`
- 时间: 2026-04-12 21:43:32 +0800
- 标题: chore: sync 2026-04-12 21:43:30
- 涉及文件: `audit/v1_calibration_audit/01_git_changes_since_2026-04-03.md`, `audit/v1_calibration_audit/02_v1_flow_map.md`, `audit/v1_calibration_audit/03_point_storage_map.md`, `audit/v1_calibration_audit/04_risk_checklist.md`, `audit/v1_calibration_audit/05_evidence.json`, `audit/v1_calibration_audit/06_trace_check.md`, `audit/v1_calibration_audit/README.md`, `audit/v1_calibration_audit/raw/git_log_since_2026-04-03.txt`, `audit/v1_calibration_audit/raw/rg_hits.txt`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CALI, CALIBRATION, CO2, COEFFICIENT, Cali, Calibration, Co2, Coefficient
- 关键 diff hunk 摘要:
  - audit/v1_calibration_audit/01_git_changes_since_2026-04-03.md @@ -0,0 +1,4300 @@ +# 2026-04-03 以来可能影响 V1 校准的改动
  - audit/v1_calibration_audit/01_git_changes_since_2026-04-03.md @@ -0,0 +1,4300 @@ +
  - audit/v1_calibration_audit/01_git_changes_since_2026-04-03.md @@ -0,0 +1,4300 @@ +- 当前分支: `main`
  - audit/v1_calibration_audit/01_git_changes_since_2026-04-03.md @@ -0,0 +1,4300 @@ +- 当前 HEAD: `2b472cb8949ea97546ab856b181379ff0ca9edee`
  - audit/v1_calibration_audit/01_git_changes_since_2026-04-03.md @@ -0,0 +1,4300 @@ +- 筛选起点: `2026-04-03 00:00:00`

### `2b472cb8949ea97546ab856b181379ff0ca9edee`
- 时间: 2026-04-12 21:38:30 +0800
- 标题: chore: sync 2026-04-12 21:38:30
- 涉及文件: `audit/v1_calibration_audit/raw/git_status.txt`, `tests/test_audit_v1_trace_check.py`, `tools/audit_v1_calibration.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, Calibration, GETCO, H2O, MODE, Point, READDATA, SENCO
- 关键 diff hunk 摘要:
  - audit/v1_calibration_audit/raw/git_status.txt @@ -0,0 +1,5 @@ +# git status --short --branch
  - audit/v1_calibration_audit/raw/git_status.txt @@ -0,0 +1,5 @@ +## main...origin/main
  - audit/v1_calibration_audit/raw/git_status.txt @@ -0,0 +1,5 @@ +
  - audit/v1_calibration_audit/raw/git_status.txt @@ -0,0 +1,5 @@ +# git status --porcelain=v1 -uall
  - audit/v1_calibration_audit/raw/git_status.txt @@ -0,0 +1,5 @@ +(clean)

### `f4e6b0a6efc38cb4f2ea046d78603d29df7d18ae`
- 时间: 2026-04-12 21:33:32 +0800
- 标题: chore: sync 2026-04-12 21:33:30
- 涉及文件: `tests/test_audit_v1_trace_check.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, Point, cali, co2, h2o, mode, point, report
- 关键 diff hunk 摘要:
  - tests/test_audit_v1_trace_check.py @@ -0,0 +1,176 @@ +import csv
  - tests/test_audit_v1_trace_check.py @@ -0,0 +1,176 @@ +import inspect
  - tests/test_audit_v1_trace_check.py @@ -0,0 +1,176 @@ +import types
  - tests/test_audit_v1_trace_check.py @@ -0,0 +1,176 @@ +from pathlib import Path
  - tests/test_audit_v1_trace_check.py @@ -0,0 +1,176 @@ +

### `0b0238add2bfa0723d42effa5b0b9148f7f443f1`
- 时间: 2026-04-12 21:13:31 +0800
- 标题: chore: sync 2026-04-12 21:13:31
- 涉及文件: `src/gas_calibrator/export/corrected_water_points_report.py`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py`, `tests/test_corrected_water_points_report.py`, `tests/test_run_v1_corrected_autodelivery.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, H2O, Point, cali, calibration, co2, delivery, h2o
- 关键 diff hunk 摘要:
  - src/gas_calibrator/export/corrected_water_points_report.py @@ -398,0 +399,34 @@ def build_corrected_water_points_report( +    h2o_selected_frames: List[pd.DataFrame] = []
  - src/gas_calibrator/export/corrected_water_points_report.py @@ -398,0 +399,34 @@ def build_corrected_water_points_report( +    for bundle in bundles:
  - src/gas_calibrator/export/corrected_water_points_report.py @@ -398,0 +399,34 @@ def build_corrected_water_points_report( +        if str(bundle.gas or "").strip().lower() != "h2o":
  - src/gas_calibrator/export/corrected_water_points_report.py @@ -398,0 +399,34 @@ def build_corrected_water_points_report( +            continue
  - src/gas_calibrator/export/corrected_water_points_report.py @@ -398,0 +399,34 @@ def build_corrected_water_points_report( +        selected = bundle.selected_frame.copy()

### `dac8104b44e9a4772b8827684c79d8ab0f59430f`
- 时间: 2026-04-12 21:03:31 +0800
- 标题: chore: sync 2026-04-12 21:03:30
- 涉及文件: `src/gas_calibrator/config.py`, `src/gas_calibrator/export/corrected_water_points_report.py`, `src/gas_calibrator/export/temperature_compensation_export.py`, `src/gas_calibrator/h2o_summary_selection.py`, `src/gas_calibrator/v2/config/models.py`, `src/gas_calibrator/workflow/runner.py`, `tests/test_config_runtime_defaults.py`, `tests/test_runner_quality.py`, `tests/test_temperature_compensation.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, Calibration, H2O, Point, ZERO, cali, calibration, co2
- 关键 diff hunk 摘要:
  - src/gas_calibrator/config.py @@ -15,0 +16,2 @@ from typing import Any, Dict +from .h2o_summary_selection import default_h2o_summary_selection
  - src/gas_calibrator/config.py @@ -15,0 +16,2 @@ from typing import Any, Dict +
  - src/gas_calibrator/config.py @@ -235,0 +238,21 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { +    "temperature_calibration": {
  - src/gas_calibrator/config.py @@ -235,0 +238,21 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { +        "enabled": True,
  - src/gas_calibrator/config.py @@ -235,0 +238,21 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { +        "snapshot_window_s": 60.0,

### `65d5c7c74e8f5e9d30a3032b2c3a3e2dc2250055`
- 时间: 2026-04-12 20:23:50 +0800
- 标题: chore: sync 2026-04-12 20:23:47
- 涉及文件: `configs/default_config.json`, `src/gas_calibrator/export/corrected_water_points_report.py`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py`, `src/gas_calibrator/v2/config/models.py`, `tests/test_corrected_water_points_report.py`, `tests/test_run_v1_corrected_autodelivery.py`, `tests/v2/test_ratio_poly_report.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, H2O, Point, cali, calibration, co2, coefficient, delivery
- 关键 diff hunk 摘要:
  - configs/default_config.json @@ -686,5 +686 @@ -      "include_co2_temp_groups_c": [
  - configs/default_config.json @@ -686,5 +686 @@ +      "include_co2_temp_groups_c": [],
  - src/gas_calibrator/export/corrected_water_points_report.py @@ -388,0 +389 @@ def build_corrected_water_points_report( +    h2o_selection = _resolve_h2o_selection(cfg.get("h2o_summary_selection"))
  - src/gas_calibrator/export/corrected_water_points_report.py @@ -402,0 +404 @@ def build_corrected_water_points_report( +                selection=h2o_selection,
  - src/gas_calibrator/export/corrected_water_points_report.py @@ -437,0 +440,4 @@ def build_corrected_water_points_report( +    note_rows[1] = {

### `81cdb3bdc6844c931490ac0b62432823b9c8e182`
- 时间: 2026-04-12 20:18:41 +0800
- 标题: chore: sync 2026-04-12 20:18:41
- 涉及文件: `src/gas_calibrator/export/corrected_water_points_report.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, H2O, cali, co2, db, h2o, point, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/export/corrected_water_points_report.py @@ -62,0 +63,44 @@ def _env_temp_from_row(row: Mapping[str, Any]) -> float | None: +def _selection_float_list(raw: Any, default: Sequence[float]) -> List[float]:
  - src/gas_calibrator/export/corrected_water_points_report.py @@ -62,0 +63,44 @@ def _env_temp_from_row(row: Mapping[str, Any]) -> float | None: +    if not isinstance(raw, (list, tuple)):
  - src/gas_calibrator/export/corrected_water_points_report.py @@ -62,0 +63,44 @@ def _env_temp_from_row(row: Mapping[str, Any]) -> float | None: +        return [float(value) for value in default]
  - src/gas_calibrator/export/corrected_water_points_report.py @@ -62,0 +63,44 @@ def _env_temp_from_row(row: Mapping[str, Any]) -> float | None: +    values: List[float] = []
  - src/gas_calibrator/export/corrected_water_points_report.py @@ -62,0 +63,44 @@ def _env_temp_from_row(row: Mapping[str, Any]) -> float | None: +    for item in raw:

### `b5401b5b2fc1e557fcf4e1d4245b9b6269194eb1`
- 时间: 2026-04-12 20:13:46 +0800
- 标题: chore: sync 2026-04-12 20:13:43
- 涉及文件: `tests/v2/test_governance_handoff_contracts.py`
- 判定原因: diff 内容命中关键词: Step, cali, calibration, serial, step
- 关键 diff hunk 摘要:
  - tests/v2/test_governance_handoff_contracts.py @@ -349,0 +350,336 @@ class TestGovernanceHandoffStep2Boundary: +        from gas_calibrator.v2.core.stage_admission_review_pack import build_stage_admission_review_pack
  - tests/v2/test_governance_handoff_contracts.py @@ -349,0 +350,336 @@ class TestGovernanceHandoffStep2Boundary: +        from gas_calibrator.v2.core.governance_handoff_contracts import GOVERNANCE_HANDOFF_TITLE_TEXTS
  - tests/v2/test_governance_handoff_contracts.py @@ -349,0 +350,336 @@ class TestGovernanceHandoffStep2Boundary: +            step2_readiness_summary={},
  - tests/v2/test_governance_handoff_contracts.py @@ -349,0 +350,336 @@ class TestGovernanceHandoffStep2Boundary: +            metrology_calibration_contract={},
  - tests/v2/test_governance_handoff_contracts.py @@ -349,0 +350,336 @@ class TestGovernanceHandoffStep2Boundary: +        from gas_calibrator.v2.core.governance_handoff_contracts import GOVERNANCE_HANDOFF_I18N_KEYS

### `0b898f573562809bf67f79a96f9bd1d73b430285`
- 时间: 2026-04-12 19:48:44 +0800
- 标题: chore: sync 2026-04-12 19:48:43
- 涉及文件: `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: diff 内容命中关键词: report
- 关键 diff hunk 摘要:
  - tests/v2/test_ui_v2_review_center_index.py @@ -105 +107 @@ def test_review_center_builds_cross_run_index_from_recent_runs(tmp_path: Path, m -        run_b / "summary_parity_report.json",
  - tests/v2/test_ui_v2_review_center_index.py @@ -105 +107 @@ def test_review_center_builds_cross_run_index_from_recent_runs(tmp_path: Path, m +        run_a / "summary_parity_report.json",
  - tests/v2/test_ui_v2_review_center_index.py @@ -116 +118 @@ def test_review_center_builds_cross_run_index_from_recent_runs(tmp_path: Path, m -        run_b / "export_resilience_report.json",
  - tests/v2/test_ui_v2_review_center_index.py @@ -116 +118 @@ def test_review_center_builds_cross_run_index_from_recent_runs(tmp_path: Path, m +        run_a / "export_resilience_report.json",
  - tests/v2/test_ui_v2_review_center_index.py @@ -172,20 +167,0 @@ def test_review_center_builds_cross_run_index_from_recent_runs(tmp_path: Path, m -    print(f"DEBUG: parity_file_exists={(run_b / 'summary_parity_report.json').exists()}")

### `95cca5a9ae42258e477064d606c0f8f52a23a5ce`
- 时间: 2026-04-12 19:23:30 +0800
- 标题: chore: sync 2026-04-12 19:23:30
- 涉及文件: `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: diff 内容命中关键词: report
- 关键 diff hunk 摘要:
  - tests/v2/test_ui_v2_review_center_index.py @@ -169,0 +170,14 @@ def test_review_center_builds_cross_run_index_from_recent_runs(tmp_path: Path, m +    print(f"DEBUG: parity_file_exists={(run_b / 'summary_parity_report.json').exists()}")
  - tests/v2/test_ui_v2_review_center_index.py @@ -169,0 +170,14 @@ def test_review_center_builds_cross_run_index_from_recent_runs(tmp_path: Path, m +        "summary_parity_report.json",

### `18ca560637f82c5f8f360f2e6e7d5ad62c315c2c`
- 时间: 2026-04-12 19:13:32 +0800
- 标题: chore: sync 2026-04-12 19:13:30
- 涉及文件: `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: diff 内容命中关键词: cali
- 关键 diff hunk 摘要:
  - tests/v2/test_ui_v2_review_center_index.py @@ -62 +62,6 @@ def _write_json(path: Path, payload: dict) -> None: +    import gas_calibrator.v2.ui_v2.controllers.app_facade as _facade_mod

### `d4b31f47084523079f090dad45cbbdb89cbf99a0`
- 时间: 2026-04-12 18:58:32 +0800
- 标题: chore: sync 2026-04-12 18:58:31
- 涉及文件: `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: diff 内容命中关键词: db
- 关键 diff hunk 摘要:
  - 未提取到关键词 hunk，建议结合 raw git log 复核。

### `5971675a2fdea403c68a40750f25678a70473f9e`
- 时间: 2026-04-12 18:53:37 +0800
- 标题: chore: sync 2026-04-12 18:53:30
- 涉及文件: `_debug_test/points.json`, `_debug_test/review_run_a/suite_summary.json`, `_debug_test/review_run_b/summary_parity_report.json`, `_debug_test/run_20260412_185117/acceptance_plan.json`, `_debug_test/run_20260412_185117/ai_run_summary.md`, `_debug_test/run_20260412_185117/analytics_summary.json`, `_debug_test/run_20260412_185117/artifact_contract_catalog.json`, `_debug_test/run_20260412_185117/artifact_contract_catalog.md`, `_debug_test/run_20260412_185117/artifact_hash_registry.json`, `_debug_test/run_20260412_185117/artifact_hash_registry.md`, `_debug_test/run_20260412_185117/audit_event_store.json`, `_debug_test/run_20260412_185117/audit_event_store.md`, `_debug_test/run_20260412_185117/audit_readiness_digest.json`, `_debug_test/run_20260412_185117/audit_readiness_digest.md`, `_debug_test/run_20260412_185117/budget_case.json`, `_debug_test/run_20260412_185117/budget_case.md`, `_debug_test/run_20260412_185117/calibration_coefficients.xlsx`, `_debug_test/run_20260412_185117/certificate_lifecycle_summary.json`, `_debug_test/run_20260412_185117/certificate_lifecycle_summary.md`, `_debug_test/run_20260412_185117/certificate_readiness_summary.json`, `_debug_test/run_20260412_185117/certificate_readiness_summary.md`, `_debug_test/run_20260412_185117/change_impact_summary.json`, `_debug_test/run_20260412_185117/change_impact_summary.md`, `_debug_test/run_20260412_185117/coefficient_registry.json`, `_debug_test/run_20260412_185117/comparison_digest.json`, `_debug_test/run_20260412_185117/comparison_digest.md`, `_debug_test/run_20260412_185117/comparison_evidence_pack.json`, `_debug_test/run_20260412_185117/comparison_evidence_pack.md`, `_debug_test/run_20260412_185117/comparison_rollup.json`, `_debug_test/run_20260412_185117/comparison_rollup.md`, `_debug_test/run_20260412_185117/compatibility_scan_summary.json`, `_debug_test/run_20260412_185117/compatibility_scan_summary.md`, `_debug_test/run_20260412_185117/config_fingerprint.json`, `_debug_test/run_20260412_185117/config_fingerprint.md`, `_debug_test/run_20260412_185117/decision_rule_profile.json`, `_debug_test/run_20260412_185117/decision_rule_profile.md`, `_debug_test/run_20260412_185117/environment_fingerprint.json`, `_debug_test/run_20260412_185117/environment_fingerprint.md`, `_debug_test/run_20260412_185117/evidence_registry.json`, `_debug_test/run_20260412_185117/external_comparison_importer.json`, `_debug_test/run_20260412_185117/external_comparison_importer.md`, `_debug_test/run_20260412_185117/lineage_summary.json`, `_debug_test/run_20260412_185117/manifest.json`, `_debug_test/run_20260412_185117/measurement_phase_coverage_report.json`, `_debug_test/run_20260412_185117/measurement_phase_coverage_report.md`, `_debug_test/run_20260412_185117/method_confirmation_matrix.json`, `_debug_test/run_20260412_185117/method_confirmation_matrix.md`, `_debug_test/run_20260412_185117/method_confirmation_protocol.json`, `_debug_test/run_20260412_185117/method_confirmation_protocol.md`, `_debug_test/run_20260412_185117/metrology_traceability_stub.json`, `_debug_test/run_20260412_185117/metrology_traceability_stub.md`, `_debug_test/run_20260412_185117/multi_source_stability_evidence.json`, `_debug_test/run_20260412_185117/multi_source_stability_evidence.md`, `_debug_test/run_20260412_185117/point_summaries.json`, `_debug_test/run_20260412_185117/points_readable.csv`, `_debug_test/run_20260412_185117/pre_run_readiness_gate.json`, `_debug_test/run_20260412_185117/pre_run_readiness_gate.md`, `_debug_test/run_20260412_185117/pt_ilc_registry.json`, `_debug_test/run_20260412_185117/pt_ilc_registry.md`, `_debug_test/run_20260412_185117/qc_report.csv`, `_debug_test/run_20260412_185117/qc_report.json`, `_debug_test/run_20260412_185117/reference_asset_registry.json`, `_debug_test/run_20260412_185117/reference_asset_registry.md`, `_debug_test/run_20260412_185117/reindex_manifest.json`, `_debug_test/run_20260412_185117/reindex_manifest.md`, `_debug_test/run_20260412_185117/release_boundary_digest.json`, `_debug_test/run_20260412_185117/release_boundary_digest.md`, `_debug_test/run_20260412_185117/release_evidence_pack_index.json`, `_debug_test/run_20260412_185117/release_evidence_pack_index.md`, `_debug_test/run_20260412_185117/release_input_digest.json`, `_debug_test/run_20260412_185117/release_input_digest.md`, `_debug_test/run_20260412_185117/release_manifest.json`, `_debug_test/run_20260412_185117/release_manifest.md`, `_debug_test/run_20260412_185117/release_scope_summary.json`, `_debug_test/run_20260412_185117/release_scope_summary.md`, `_debug_test/run_20260412_185117/release_validation_manifest.json`, `_debug_test/run_20260412_185117/release_validation_manifest.md`, `_debug_test/run_20260412_185117/requirement_design_code_test_links.json`, `_debug_test/run_20260412_185117/requirement_design_code_test_links.md`, `_debug_test/run_20260412_185117/results.json`, `_debug_test/run_20260412_185117/rollback_readiness_summary.json`, `_debug_test/run_20260412_185117/rollback_readiness_summary.md`, `_debug_test/run_20260412_185117/route_specific_validation_matrix.json`, `_debug_test/run_20260412_185117/route_specific_validation_matrix.md`, `_debug_test/run_20260412_185117/run_artifact_index.json`, `_debug_test/run_20260412_185117/run_artifact_index.md`, `_debug_test/run_20260412_185117/scope_comparison_view.json`, `_debug_test/run_20260412_185117/scope_comparison_view.md`, `_debug_test/run_20260412_185117/scope_definition_pack.json`, `_debug_test/run_20260412_185117/scope_definition_pack.md`, `_debug_test/run_20260412_185117/scope_readiness_summary.json`, `_debug_test/run_20260412_185117/scope_readiness_summary.md`, `_debug_test/run_20260412_185117/sensitivity_coefficient_set.json`, `_debug_test/run_20260412_185117/sensitivity_coefficient_set.md`, `_debug_test/run_20260412_185117/simulation_evidence_sidecar_bundle.json`, `_debug_test/run_20260412_185117/software_validation_traceability_matrix.json`, `_debug_test/run_20260412_185117/software_validation_traceability_matrix.md`, `_debug_test/run_20260412_185117/state_transition_evidence.json`, `_debug_test/run_20260412_185117/state_transition_evidence.md`, `_debug_test/run_20260412_185117/step2_closeout_digest.json`, `_debug_test/run_20260412_185117/step2_closeout_digest.md`, `_debug_test/run_20260412_185117/summary.json`, `_debug_test/run_20260412_185117/trend_registry.json`, `_debug_test/run_20260412_185117/uncertainty_budget_stub.json`, `_debug_test/run_20260412_185117/uncertainty_budget_stub.md`, `_debug_test/run_20260412_185117/uncertainty_digest.json`, `_debug_test/run_20260412_185117/uncertainty_digest.md`, `_debug_test/run_20260412_185117/uncertainty_golden_cases.json`, `_debug_test/run_20260412_185117/uncertainty_golden_cases.md`, `_debug_test/run_20260412_185117/uncertainty_input_set.json`, `_debug_test/run_20260412_185117/uncertainty_input_set.md`, `_debug_test/run_20260412_185117/uncertainty_method_readiness_summary.json`, `_debug_test/run_20260412_185117/uncertainty_method_readiness_summary.md`, `_debug_test/run_20260412_185117/uncertainty_model.json`, `_debug_test/run_20260412_185117/uncertainty_model.md`, `_debug_test/run_20260412_185117/uncertainty_report_pack.json`, `_debug_test/run_20260412_185117/uncertainty_report_pack.md`, `_debug_test/run_20260412_185117/uncertainty_rollup.json`, `_debug_test/run_20260412_185117/uncertainty_rollup.md`, `_debug_test/run_20260412_185117/validation_evidence_index.json`, `_debug_test/run_20260412_185117/validation_evidence_index.md`, `_debug_test/run_20260412_185117/validation_run_set.json`, `_debug_test/run_20260412_185117/validation_run_set.md`, `_debug_test/run_20260412_185117/verification_digest.json`, `_debug_test/run_20260412_185117/verification_digest.md`, `_debug_test/run_20260412_185117/verification_rollup.json`, `_debug_test/run_20260412_185117/verification_rollup.md`, `_debug_test/ui_v2_state/config/recent_runs.json`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CALI, CO2, Calibration, Coefficient, DB, H2O, POINT, Protocol
- 关键 diff hunk 摘要:
  - _debug_test/points.json @@ -0,0 +1 @@ +{"points": [{"index": 1, "temperature_c": 25.0, "humidity_pct": 30.0, "pressure_hpa": 1000.0, "route": "h2o"}, {"index": 2, "temperature_c": 25.0, "co2_ppm": 400.0, "pressure_hpa"
  - _debug_test/review_run_a/suite_summary.json @@ -0,0 +1,12 @@ +  "evidence_source": "simulated_protocol",
  - _debug_test/review_run_b/summary_parity_report.json @@ -0,0 +1,12 @@ +{
  - _debug_test/review_run_b/summary_parity_report.json @@ -0,0 +1,12 @@ +  "generated_at": "2026-04-10T18:51:18",
  - _debug_test/review_run_b/summary_parity_report.json @@ -0,0 +1,12 @@ +  "status": "MATCH",

### `a28dbe65f258dcb67f8a2a91657154aff8cfbe98`
- 时间: 2026-04-12 18:38:30 +0800
- 标题: chore: sync 2026-04-12 18:38:30
- 涉及文件: `src/gas_calibrator/v2/core/engineering_isolation_admission_checklist.py`, `src/gas_calibrator/v2/core/engineering_isolation_admission_checklist_artifact_entry.py`, `src/gas_calibrator/v2/core/stage_admission_review_pack.py`, `tests/v2/test_governance_handoff_contracts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Serial, Step, cali, db, serial, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/engineering_isolation_admission_checklist.py @@ -20,0 +21,3 @@ from .governance_handoff_contracts import ( +    GOVERNANCE_HANDOFF_TITLE_TEXTS as _GOV_TITLE_TEXTS,
  - src/gas_calibrator/v2/core/engineering_isolation_admission_checklist.py @@ -20,0 +21,3 @@ from .governance_handoff_contracts import ( +    GOVERNANCE_HANDOFF_SUMMARY_TEXTS as _GOV_SUMMARY_TEXTS,
  - src/gas_calibrator/v2/core/engineering_isolation_admission_checklist.py @@ -20,0 +21,3 @@ from .governance_handoff_contracts import ( +    GOVERNANCE_HANDOFF_PHASES as _GOV_PHASES,
  - src/gas_calibrator/v2/core/engineering_isolation_admission_checklist.py @@ -121,5 +124,2 @@ def build_engineering_isolation_admission_checklist( -        "title_text": "工程隔离准入清单 / Engineering Isolation Admission Checklist",
  - src/gas_calibrator/v2/core/engineering_isolation_admission_checklist.py @@ -121,5 +124,2 @@ def build_engineering_isolation_admission_checklist( -        "summary_text": (

### `7522a84d085c9d01505451b169f5e2ee35d7ae99`
- 时间: 2026-04-12 18:33:30 +0800
- 标题: chore: sync 2026-04-12 18:33:30
- 涉及文件: `src/gas_calibrator/v2/core/governance_handoff_contracts.py`, `src/gas_calibrator/v2/core/stage_admission_review_pack.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Step, cali, calibration, report, step, 校准
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/governance_handoff_contracts.py @@ -20 +20 @@ from __future__ import annotations -GOVERNANCE_HANDOFF_CONTRACTS_VERSION: str = "2.6.0"
  - src/gas_calibrator/v2/core/governance_handoff_contracts.py @@ -20 +20 @@ from __future__ import annotations +GOVERNANCE_HANDOFF_CONTRACTS_VERSION: str = "2.6.1"
  - src/gas_calibrator/v2/core/governance_handoff_contracts.py @@ -171,0 +172,68 @@ GOVERNANCE_HANDOFF_REVIEWER_PAIRING: dict[str, str] = { +# ---------------------------------------------------------------------------
  - src/gas_calibrator/v2/core/governance_handoff_contracts.py @@ -171,0 +172,68 @@ GOVERNANCE_HANDOFF_REVIEWER_PAIRING: dict[str, str] = { +# Summary texts (Chinese default)
  - src/gas_calibrator/v2/core/governance_handoff_contracts.py @@ -171,0 +172,68 @@ GOVERNANCE_HANDOFF_REVIEWER_PAIRING: dict[str, str] = { +

### `511200275e2b8b43766cb7a0b05e9b0a36370ca7`
- 时间: 2026-04-12 17:18:30 +0800
- 标题: chore: sync 2026-04-12 17:18:30
- 涉及文件: `tests/v2/test_ui_v2_reports_page.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: report
- 关键 diff hunk 摘要:
  - tests/v2/test_ui_v2_reports_page.py @@ -816,2 +816,2 @@ def test_reports_page_artifact_list_surfaces_engineering_isolation_admission_che -        assert "Engineering Isolation Admission Checklist / 工程隔离准入清单 (JSON)" in rows_by_name
  - tests/v2/test_ui_v2_reports_page.py @@ -816,2 +816,2 @@ def test_reports_page_artifact_list_surfaces_engineering_isolation_admission_che -        assert "Engineering Isolation Admission Checklist / 工程隔离准入清单 (Markdown)" in rows_by_name
  - tests/v2/test_ui_v2_reports_page.py @@ -816,2 +816,2 @@ def test_reports_page_artifact_list_surfaces_engineering_isolation_admission_che +        assert "工程隔离准入清单 / Engineering Isolation Admission Checklist (JSON)" in rows_by_name
  - tests/v2/test_ui_v2_reports_page.py @@ -816,2 +816,2 @@ def test_reports_page_artifact_list_surfaces_engineering_isolation_admission_che +        assert "工程隔离准入清单 / Engineering Isolation Admission Checklist (Markdown)" in rows_by_name
  - tests/v2/test_ui_v2_reports_page.py @@ -819 +819 @@ def test_reports_page_artifact_list_surfaces_engineering_isolation_admission_che -            "Engineering Isolation Admission Checklist / 工程隔离准入清单 (JSON)"

### `72f176c58360b3893c2eb9da39241597d0052609`
- 时间: 2026-04-12 17:03:31 +0800
- 标题: chore: sync 2026-04-12 17:03:30
- 涉及文件: `src/gas_calibrator/v2/ui_v2/locales/en_US.json`, `tests/v2/test_governance_handoff_contracts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CALIBRATION, Calibration, STEP, Step, cali, calibration, mode, serial
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/locales/en_US.json @@ -2350,0 +2351,10 @@ +  },
  - src/gas_calibrator/v2/ui_v2/locales/en_US.json @@ -2350,0 +2351,10 @@ +  "governance_handoff": {
  - src/gas_calibrator/v2/ui_v2/locales/en_US.json @@ -2350,0 +2351,10 @@ +    "step2_readiness_summary": "Step 2 Readiness Summary",
  - src/gas_calibrator/v2/ui_v2/locales/en_US.json @@ -2350,0 +2351,10 @@ +    "metrology_calibration_contract": "Metrology Calibration Contract",
  - src/gas_calibrator/v2/ui_v2/locales/en_US.json @@ -2350,0 +2351,10 @@ +    "phase_transition_bridge": "Phase Transition Bridge",

### `81f8ce0c1d4f0305b61a36c79eaa06a289c86061`
- 时间: 2026-04-12 16:53:32 +0800
- 标题: chore: sync 2026-04-12 16:53:31
- 涉及文件: `src/gas_calibrator/v2/ui_v2/locales/zh_CN.json`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Step, cali, calibration, step, 校准
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/locales/zh_CN.json @@ -2345,0 +2346,10 @@ +  },
  - src/gas_calibrator/v2/ui_v2/locales/zh_CN.json @@ -2345,0 +2346,10 @@ +  "governance_handoff": {
  - src/gas_calibrator/v2/ui_v2/locales/zh_CN.json @@ -2345,0 +2346,10 @@ +    "step2_readiness_summary": "Step 2 就绪度摘要",
  - src/gas_calibrator/v2/ui_v2/locales/zh_CN.json @@ -2345,0 +2346,10 @@ +    "metrology_calibration_contract": "计量校准合同",
  - src/gas_calibrator/v2/ui_v2/locales/zh_CN.json @@ -2345,0 +2346,10 @@ +    "phase_transition_bridge": "阶段过渡桥接",

### `581f7acb45d2694ef7b608024b372fb6030fdc82`
- 时间: 2026-04-12 16:23:31 +0800
- 标题: chore: sync 2026-04-12 16:23:30
- 涉及文件: `src/gas_calibrator/v2/core/engineering_isolation_admission_checklist.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/engineering_isolation_admission_checklist.py @@ -121 +121 @@ def build_engineering_isolation_admission_checklist( -        "title_text": "Engineering Isolation Admission Checklist / 工程隔离准入清单",
  - src/gas_calibrator/v2/core/engineering_isolation_admission_checklist.py @@ -121 +121 @@ def build_engineering_isolation_admission_checklist( +        "title_text": "工程隔离准入清单 / Engineering Isolation Admission Checklist",
  - src/gas_calibrator/v2/core/engineering_isolation_admission_checklist.py @@ -441 +441 @@ def _render_engineering_isolation_admission_checklist_markdown(display: dict[str -        f"# {display.get('title_text') or 'Engineering Isolation Admission Checklist / 工程隔离准入清单'}",
  - src/gas_calibrator/v2/core/engineering_isolation_admission_checklist.py @@ -441 +441 @@ def _render_engineering_isolation_admission_checklist_markdown(display: dict[str +        f"# {display.get('title_text') or '工程隔离准入清单 / Engineering Isolation Admission Checklist'}",

### `29b63cb919fd15094edd9df704a05c4db0677ed0`
- 时间: 2026-04-12 16:18:31 +0800
- 标题: chore: sync 2026-04-12 16:18:30
- 涉及文件: `src/gas_calibrator/v2/core/engineering_isolation_admission_checklist_artifact_entry.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, db
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/engineering_isolation_admission_checklist_artifact_entry.py @@ -30 +30 @@ def build_engineering_isolation_admission_checklist_artifact_entry( -    title_text = "Engineering Isolation Admission Checklist / 工程隔离准入清单"
  - src/gas_calibrator/v2/core/engineering_isolation_admission_checklist_artifact_entry.py @@ -30 +30 @@ def build_engineering_isolation_admission_checklist_artifact_entry( +    title_text = "工程隔离准入清单 / Engineering Isolation Admission Checklist"

### `5920841c97bc258c6d8728bb45e976fdea0b6239`
- 时间: 2026-04-12 16:08:31 +0800
- 标题: chore: sync 2026-04-12 16:08:30
- 涉及文件: `src/gas_calibrator/v2/core/metrology_calibration_contract.py`, `src/gas_calibrator/v2/core/phase_transition_bridge_reviewer_artifact.py`, `src/gas_calibrator/v2/core/step2_readiness.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CALIBRATION, STEP, cali, calibration, mode, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/metrology_calibration_contract.py @@ -6,0 +7 @@ from .acceptance_model import build_user_visible_evidence_boundary +from .governance_handoff_contracts import GOVERNANCE_HANDOFF_FILENAMES as _GOV_FILENAMES
  - src/gas_calibrator/v2/core/metrology_calibration_contract.py @@ -9 +10 @@ from .acceptance_model import build_user_visible_evidence_boundary -METROLOGY_CALIBRATION_CONTRACT_FILENAME = "metrology_calibration_contract.json"
  - src/gas_calibrator/v2/core/metrology_calibration_contract.py @@ -9 +10 @@ from .acceptance_model import build_user_visible_evidence_boundary +METROLOGY_CALIBRATION_CONTRACT_FILENAME = _GOV_FILENAMES["metrology_calibration_contract"]
  - src/gas_calibrator/v2/core/phase_transition_bridge_reviewer_artifact.py @@ -5,0 +6 @@ from .phase_transition_bridge_presenter import build_phase_transition_bridge_pan +from .governance_handoff_contracts import GOVERNANCE_HANDOFF_FILENAMES as _GOV_FILENAMES
  - src/gas_calibrator/v2/core/phase_transition_bridge_reviewer_artifact.py @@ -8 +9 @@ from .phase_transition_bridge_presenter import build_phase_transition_bridge_pan -PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME = "phase_transition_bridge_reviewer.md"

### `4dc2f69bb4a30e6c165c8cbd8a9215d426c945e6`
- 时间: 2026-04-12 16:03:34 +0800
- 标题: chore: sync 2026-04-12 16:03:33
- 涉及文件: `src/gas_calibrator/v2/core/engineering_isolation_admission_checklist.py`, `src/gas_calibrator/v2/core/phase_transition_bridge.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: STEP, cali, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/engineering_isolation_admission_checklist.py @@ -14,0 +15,7 @@ from .step2_readiness import STEP2_READINESS_SUMMARY_FILENAME +from .governance_handoff_contracts import (
  - src/gas_calibrator/v2/core/engineering_isolation_admission_checklist.py @@ -14,0 +15,7 @@ from .step2_readiness import STEP2_READINESS_SUMMARY_FILENAME +    GOVERNANCE_HANDOFF_FILENAMES as _GOV_FILENAMES,
  - src/gas_calibrator/v2/core/engineering_isolation_admission_checklist.py @@ -14,0 +15,7 @@ from .step2_readiness import STEP2_READINESS_SUMMARY_FILENAME +    GOVERNANCE_HANDOFF_DISPLAY_LABELS as _GOV_LABELS,
  - src/gas_calibrator/v2/core/engineering_isolation_admission_checklist.py @@ -14,0 +15,7 @@ from .step2_readiness import STEP2_READINESS_SUMMARY_FILENAME +    GOVERNANCE_HANDOFF_DISPLAY_LABELS_EN as _GOV_LABELS_EN,
  - src/gas_calibrator/v2/core/engineering_isolation_admission_checklist.py @@ -14,0 +15,7 @@ from .step2_readiness import STEP2_READINESS_SUMMARY_FILENAME +    GOVERNANCE_HANDOFF_I18N_KEYS as _GOV_I18N_KEYS,

### `d50d55ac1f96b53fb04dc7b190639f26dd0bde2d`
- 时间: 2026-04-12 15:58:32 +0800
- 标题: chore: sync 2026-04-12 15:58:31
- 涉及文件: `src/gas_calibrator/v2/core/stage_admission_review_pack.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: STEP, cali, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/stage_admission_review_pack.py @@ -13,0 +14,7 @@ from .step2_readiness import STEP2_READINESS_SUMMARY_FILENAME +from .governance_handoff_contracts import (
  - src/gas_calibrator/v2/core/stage_admission_review_pack.py @@ -13,0 +14,7 @@ from .step2_readiness import STEP2_READINESS_SUMMARY_FILENAME +    GOVERNANCE_HANDOFF_FILENAMES as _GOV_FILENAMES,
  - src/gas_calibrator/v2/core/stage_admission_review_pack.py @@ -13,0 +14,7 @@ from .step2_readiness import STEP2_READINESS_SUMMARY_FILENAME +    GOVERNANCE_HANDOFF_DISPLAY_LABELS as _GOV_LABELS,
  - src/gas_calibrator/v2/core/stage_admission_review_pack.py @@ -13,0 +14,7 @@ from .step2_readiness import STEP2_READINESS_SUMMARY_FILENAME +    GOVERNANCE_HANDOFF_DISPLAY_LABELS_EN as _GOV_LABELS_EN,
  - src/gas_calibrator/v2/core/stage_admission_review_pack.py @@ -13,0 +14,7 @@ from .step2_readiness import STEP2_READINESS_SUMMARY_FILENAME +    GOVERNANCE_HANDOFF_I18N_KEYS as _GOV_I18N_KEYS,

### `0f2722844d1b1196ce8fe314e7c3fc6c0a606caa`
- 时间: 2026-04-12 15:53:30 +0800
- 标题: chore: sync 2026-04-12 15:53:30
- 涉及文件: `src/gas_calibrator/v2/core/governance_handoff_contracts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, db
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/governance_handoff_contracts.py @@ -164 +164 @@ GOVERNANCE_HANDOFF_REVIEWER_PAIRING: dict[str, str] = { -    "engineering_isolation_admission_checklist": "engineering_isolation_admission_checklist_reviewer_arteract",
  - src/gas_calibrator/v2/core/governance_handoff_contracts.py @@ -164 +164 @@ GOVERNANCE_HANDOFF_REVIEWER_PAIRING: dict[str, str] = { +    "engineering_isolation_admission_checklist": "engineering_isolation_admission_checklist_reviewer_artifact",

### `28f23beaaa367f7a6b058348e2b69005899b7c83`
- 时间: 2026-04-12 15:48:31 +0800
- 标题: chore: sync 2026-04-12 15:48:30
- 涉及文件: `src/gas_calibrator/v2/core/governance_handoff_contracts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, STEP, Step, cali, calibration, db, mode, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/governance_handoff_contracts.py @@ -0,0 +1,188 @@ +"""Shared contract constants for Step 2 tail governance handoff artifacts.
  - src/gas_calibrator/v2/core/governance_handoff_contracts.py @@ -0,0 +1,188 @@ +
  - src/gas_calibrator/v2/core/governance_handoff_contracts.py @@ -0,0 +1,188 @@ +Single source of truth for artifact keys, filenames, roles, display labels,
  - src/gas_calibrator/v2/core/governance_handoff_contracts.py @@ -0,0 +1,188 @@ +i18n keys, and canonical ordering of the governance handoff chain.
  - src/gas_calibrator/v2/core/governance_handoff_contracts.py @@ -0,0 +1,188 @@ +Step 2 boundary:

### `ae35d84e78c49b39ca428f4f5f60220cf8025522`
- 时间: 2026-04-12 15:09:11 +0800
- 标题: chore: sync 2026-04-12 15:09:10
- 涉及文件: `src/gas_calibrator/devices/humidity_generator.py`, `src/gas_calibrator/humidity_math.py`, `src/gas_calibrator/tools/validate_verification_doc.py`, `src/gas_calibrator/ui/humidity_page.py`, `tests/test_humidity_generator_driver.py`, `tests/test_ui_pages.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Serial, cali, h2o, mode, point, readback, serial, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/devices/humidity_generator.py @@ -8,0 +9 @@ from typing import Any, Dict, List, Optional +from ..humidity_math import derive_humidity_generator_setpoint
  - src/gas_calibrator/devices/humidity_generator.py @@ -102,0 +104,4 @@ class HumidityGenerator: +    @staticmethod
  - src/gas_calibrator/devices/humidity_generator.py @@ -102,0 +104,4 @@ class HumidityGenerator: +    def derive_setpoint_from_dewpoint(dewpoint_c: float) -> Dict[str, float]:
  - src/gas_calibrator/devices/humidity_generator.py @@ -102,0 +104,4 @@ class HumidityGenerator: +        return derive_humidity_generator_setpoint(dewpoint_c)
  - src/gas_calibrator/devices/humidity_generator.py @@ -102,0 +104,4 @@ class HumidityGenerator: +

### `471a64a488ef5a08895ddc66f6e7f3b5d13bff74`
- 时间: 2026-04-12 14:08:34 +0800
- 标题: chore: sync 2026-04-12 14:08:32
- 涉及文件: `src/gas_calibrator/tools/runtime_temp_fix_bundle.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, cali, co2, coefficient, mode, point, step, store
- 关键 diff hunk 摘要:
  - src/gas_calibrator/tools/runtime_temp_fix_bundle.py @@ -0,0 +1,663 @@ +"""Apply the step-25 runtime temperature normalization fix to a production bundle.
  - src/gas_calibrator/tools/runtime_temp_fix_bundle.py @@ -0,0 +1,663 @@ +
  - src/gas_calibrator/tools/runtime_temp_fix_bundle.py @@ -0,0 +1,663 @@ +This tool keeps the scope intentionally narrow:
  - src/gas_calibrator/tools/runtime_temp_fix_bundle.py @@ -0,0 +1,663 @@ +- reuse the production bundle's existing ratio-poly base model
  - src/gas_calibrator/tools/runtime_temp_fix_bundle.py @@ -0,0 +1,663 @@ +- reuse debugger-handoff temperature coefficients and mapping metadata

### `0842d6b8201cf5579521b7c74f986b087b21fbbb`
- 时间: 2026-04-12 13:33:34 +0800
- 标题: chore: sync 2026-04-12 13:33:31
- 涉及文件: `tests/v2/test_pt_ilc_wp6_contracts.py`
- 判定原因: diff 内容命中关键词: Step, step
- 关键 diff hunk 摘要:
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -510,2 +510,2 @@ class TestWp6AppFacadeIntegration: +        # After Step 2.5, WP6 keys are accessed via bundle, not as local vars
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -673,4 +673,2 @@ class TestStep2SurfaceConsistency: +        # After Step 2.5, WP6 keys are accessed via bundle, not as local vars
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -1058 +1056,2 @@ class TestCloseoutDigestReviewerSurfaceVisibility: -        assert "step2_closeout_digest" in source
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -1058 +1056,2 @@ class TestCloseoutDigestReviewerSurfaceVisibility: +        # After Step 2.5, closeout_digest is accessed via bundle, not as local var

### `b5d8d3cfd8a02b43ef680a63b2fb26f2b4186b2e`
- 时间: 2026-04-12 13:28:31 +0800
- 标题: chore: sync 2026-04-12 13:28:30
- 涉及文件: `tests/v2/test_pt_ilc_wp6_contracts.py`
- 判定原因: diff 内容命中关键词: Step, cali, serial, step
- 关键 diff hunk 摘要:
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -1445,0 +1446,164 @@ class TestBundleStep2Boundary: +# Step 2.5: Reviewer bundle end-to-end cleanup tests
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -1445,0 +1446,164 @@ class TestBundleStep2Boundary: +        from gas_calibrator.v2.core.reviewer_surface_payloads import build_wp6_closeout_bundle
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -1445,0 +1446,164 @@ class TestBundleStep2Boundary: +        from gas_calibrator.v2.core.reviewer_surface_contracts import WP6_CLOSEOUT_ARTIFACT_KEYS
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -1445,0 +1446,164 @@ class TestBundleStep2Boundary: +        import gas_calibrator.v2.ui_v2.controllers.app_facade as af
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -1445,0 +1446,164 @@ class TestBundleStep2Boundary: +        assert 'step2_closeout_digest = _wp6_closeout_bundle[' not in source

### `cc889b69ab19c192611d7611b45ea0071ad3a39e`
- 时间: 2026-04-12 13:23:34 +0800
- 标题: chore: sync 2026-04-12 13:23:32
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -533,7 +533,7 @@ class DeviceWorkbenchController: -            "pt_ilc_registry": pt_ilc_registry,
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -533,7 +533,7 @@ class DeviceWorkbenchController: -            "external_comparison_importer": external_comparison_importer,
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -533,7 +533,7 @@ class DeviceWorkbenchController: -            "comparison_evidence_pack": comparison_evidence_pack,
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -533,7 +533,7 @@ class DeviceWorkbenchController: -            "scope_comparison_view": scope_comparison_view,
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -533,7 +533,7 @@ class DeviceWorkbenchController: -            "comparison_digest": comparison_digest_payload,

### `568cd02644c75d54a422ee72747bbd6ae508b096`
- 时间: 2026-04-12 13:18:31 +0800
- 标题: chore: sync 2026-04-12 13:18:31
- 涉及文件: `src/gas_calibrator/v2/scripts/historical_artifacts.py`, `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, report, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/scripts/historical_artifacts.py @@ -213,7 +212,0 @@ def _build_run_report( -    pt_ilc_registry = _wp6_closeout["pt_ilc_registry"]
  - src/gas_calibrator/v2/scripts/historical_artifacts.py @@ -213,7 +212,0 @@ def _build_run_report( -    external_comparison_importer = _wp6_closeout["external_comparison_importer"]
  - src/gas_calibrator/v2/scripts/historical_artifacts.py @@ -213,7 +212,0 @@ def _build_run_report( -    comparison_evidence_pack = _wp6_closeout["comparison_evidence_pack"]
  - src/gas_calibrator/v2/scripts/historical_artifacts.py @@ -213,7 +212,0 @@ def _build_run_report( -    scope_comparison_view = _wp6_closeout["scope_comparison_view"]
  - src/gas_calibrator/v2/scripts/historical_artifacts.py @@ -213,7 +212,0 @@ def _build_run_report( -    comparison_digest_payload = _wp6_closeout["comparison_digest"]

### `ff6cc4cf04eb44646735f6e0ec2c777e035ca9e6`
- 时间: 2026-04-12 12:58:32 +0800
- 标题: chore: sync 2026-04-12 12:58:30
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1539,7 +1538,0 @@ class AppFacade: -        pt_ilc_registry = _wp6_closeout_bundle["pt_ilc_registry"]
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1539,7 +1538,0 @@ class AppFacade: -        external_comparison_importer = _wp6_closeout_bundle["external_comparison_importer"]
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1539,7 +1538,0 @@ class AppFacade: -        comparison_evidence_pack = _wp6_closeout_bundle["comparison_evidence_pack"]
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1539,7 +1538,0 @@ class AppFacade: -        scope_comparison_view = _wp6_closeout_bundle["scope_comparison_view"]
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1539,7 +1538,0 @@ class AppFacade: -        comparison_digest = _wp6_closeout_bundle["comparison_digest"]

### `1ebff243fdcf907d1b254add4d1ab05f9cb9d421`
- 时间: 2026-04-12 12:53:34 +0800
- 标题: chore: sync 2026-04-12 12:53:34
- 涉及文件: `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py`, `src/gas_calibrator/ui/app.py`, `src/gas_calibrator/v2/core/reviewer_surface_payloads.py`, `src/gas_calibrator/workflow/runner.py`, `tests/test_config_runtime_defaults.py`, `tests/test_run_v1_corrected_autodelivery.py`, `tests/test_runner_corrected_delivery_hooks.py`, `tests/test_ui_app.py`, `tests/test_verify_short_run_tool.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, cali, calibration, co2, coefficient, delivery, h2o, insert
- 关键 diff hunk 摘要:
  - src/gas_calibrator/tools/run_v1_corrected_autodelivery.py @@ -112 +112,14 @@ def _annotate_rows_with_actual_device_ids( -        analyzer = _normalize_analyzer(payload.get(analyzer_key))
  - src/gas_calibrator/tools/run_v1_corrected_autodelivery.py @@ -112 +112,14 @@ def _annotate_rows_with_actual_device_ids( +        resolved_analyzer_key = analyzer_key if analyzer_key in payload else ""
  - src/gas_calibrator/tools/run_v1_corrected_autodelivery.py @@ -112 +112,14 @@ def _annotate_rows_with_actual_device_ids( +        if not resolved_analyzer_key:
  - src/gas_calibrator/tools/run_v1_corrected_autodelivery.py @@ -112 +112,14 @@ def _annotate_rows_with_actual_device_ids( +            for key in payload.keys():
  - src/gas_calibrator/tools/run_v1_corrected_autodelivery.py @@ -112 +112,14 @@ def _annotate_rows_with_actual_device_ids( +                header = str(key or "").strip()

### `248d0ac69942415ac136b17e0aaa24e116e52db4`
- 时间: 2026-04-12 12:48:40 +0800
- 标题: chore: sync 2026-04-12 12:48:36
- 涉及文件: `src/gas_calibrator/config.py`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py`, `src/gas_calibrator/tools/verify_short_run.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, cali, calibration, co2, coefficient, db, delivery, h2o
- 关键 diff hunk 摘要:
  - src/gas_calibrator/config.py @@ -217,0 +218,17 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { +        "postrun_corrected_delivery": {
  - src/gas_calibrator/config.py @@ -217,0 +218,17 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { +            "enabled": True,
  - src/gas_calibrator/config.py @@ -217,0 +218,17 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { +            "strict": False,
  - src/gas_calibrator/config.py @@ -217,0 +218,17 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { +            "write_devices": True,
  - src/gas_calibrator/config.py @@ -217,0 +218,17 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { +            "verify_report": False,

### `f6ad14b38ad58ea8d777d57b0d3c8272798f25a2`
- 时间: 2026-04-12 11:53:34 +0800
- 标题: chore: sync 2026-04-12 11:53:32
- 涉及文件: `tests/v2/test_pt_ilc_wp6_contracts.py`
- 判定原因: diff 内容命中关键词: Step, cali, dB, serial, step
- 关键 diff hunk 摘要:
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -1305,0 +1306,140 @@ class TestPayloadExtractionStep2Boundary: +# Step 2.4: Reviewer bundle handoff consolidation tests
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -1305,0 +1306,140 @@ class TestPayloadExtractionStep2Boundary: +        from gas_calibrator.v2.core.reviewer_surface_payloads import build_wp6_closeout_bundle
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -1305,0 +1306,140 @@ class TestPayloadExtractionStep2Boundary: +        from gas_calibrator.v2.core.reviewer_surface_contracts import WP6_CLOSEOUT_ARTIFACT_KEYS
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -1305,0 +1306,140 @@ class TestPayloadExtractionStep2Boundary: +        from gas_calibrator.v2.core import recognition_readiness_artifacts as rr
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -1305,0 +1306,140 @@ class TestPayloadExtractionStep2Boundary: +            {"step2_closeout_digest": {"title": "收口", "non_claim": True}}

### `18f45eae07472ea3d8372a8abc2f24706d48e24d`
- 时间: 2026-04-12 11:43:33 +0800
- 标题: chore: sync 2026-04-12 11:43:33
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3278,7 +3278 @@ class AppFacade: -        pt_ilc_registry: dict[str, Any],
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3278,7 +3278 @@ class AppFacade: -        external_comparison_importer: dict[str, Any],
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3278,7 +3278 @@ class AppFacade: -        comparison_evidence_pack: dict[str, Any],
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3278,7 +3278 @@ class AppFacade: -        scope_comparison_view: dict[str, Any],
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3278,7 +3278 @@ class AppFacade: -        comparison_digest: dict[str, Any],

### `240b1321d81b20cc72c9b546759d5e0942ad07ac`
- 时间: 2026-04-12 11:38:33 +0800
- 标题: chore: sync 2026-04-12 11:38:33
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1964,7 +1964 @@ class AppFacade: -            pt_ilc_registry=pt_ilc_registry,
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1964,7 +1964 @@ class AppFacade: -            external_comparison_importer=external_comparison_importer,
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1964,7 +1964 @@ class AppFacade: -            comparison_evidence_pack=comparison_evidence_pack,
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1964,7 +1964 @@ class AppFacade: -            scope_comparison_view=scope_comparison_view,
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1964,7 +1964 @@ class AppFacade: -            comparison_digest=comparison_digest,

### `410dcfb1e6a76094aae22ada32c241f365ab7f6b`
- 时间: 2026-04-12 11:33:47 +0800
- 标题: chore: sync 2026-04-12 11:33:45
- 涉及文件: `src/gas_calibrator/v2/core/reviewer_surface_payloads.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, point, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/reviewer_surface_payloads.py @@ -132,0 +133,72 @@ def build_wp6_closeout_readiness_pairs( +
  - src/gas_calibrator/v2/core/reviewer_surface_payloads.py @@ -132,0 +133,72 @@ def build_wp6_closeout_readiness_pairs( +# ---------------------------------------------------------------------------
  - src/gas_calibrator/v2/core/reviewer_surface_payloads.py @@ -132,0 +133,72 @@ def build_wp6_closeout_readiness_pairs( +# Unified bundle: single object for WP6+closeout handoff
  - src/gas_calibrator/v2/core/reviewer_surface_payloads.py @@ -132,0 +133,72 @@ def build_wp6_closeout_readiness_pairs( +class Wp6CloseoutBundle:
  - src/gas_calibrator/v2/core/reviewer_surface_payloads.py @@ -132,0 +133,72 @@ def build_wp6_closeout_readiness_pairs( +    """Unified bundle for WP6 + step2_closeout_digest reviewer surface handoff.

### `3672ff03188816194680f1a6a9dfc73baf418785`
- 时间: 2026-04-11 21:38:34 +0800
- 标题: chore: sync 2026-04-11 21:38:32
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3558 +3558,9 @@ class AppFacade: -            _wp6_closeout,
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3558 +3558,9 @@ class AppFacade: +            {
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3558 +3558,9 @@ class AppFacade: +                "pt_ilc_registry": pt_ilc_registry,
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3558 +3558,9 @@ class AppFacade: +                "external_comparison_importer": external_comparison_importer,
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3558 +3558,9 @@ class AppFacade: +                "comparison_evidence_pack": comparison_evidence_pack,

### `51bbc87184b6590781d77eda506383c35ecb357b`
- 时间: 2026-04-11 21:28:36 +0800
- 标题: chore: sync 2026-04-11 21:28:36
- 涉及文件: `tests/v2/test_pt_ilc_wp6_contracts.py`
- 判定原因: diff 内容命中关键词: Step, cali, dB, serial, step
- 关键 diff hunk 摘要:
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -1122,0 +1123,183 @@ class TestReviewerSurfaceStep2Boundary: +# Step 2.3: Reviewer payload extraction unification tests
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -1122,0 +1123,183 @@ class TestReviewerSurfaceStep2Boundary: +        from gas_calibrator.v2.core.reviewer_surface_payloads import extract_wp6_closeout_payloads
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -1122,0 +1123,183 @@ class TestReviewerSurfaceStep2Boundary: +        from gas_calibrator.v2.core.reviewer_surface_contracts import WP6_CLOSEOUT_ARTIFACT_KEYS
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -1122,0 +1123,183 @@ class TestReviewerSurfaceStep2Boundary: +        from gas_calibrator.v2.core.reviewer_surface_payloads import extract_wp6_closeout_enriched
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -1122,0 +1123,183 @@ class TestReviewerSurfaceStep2Boundary: +        # Last item is step2_closeout_digest

### `e78c5af4dcc2c68e3052d4e2565ba53930880988`
- 时间: 2026-04-11 21:23:36 +0800
- 标题: chore: sync 2026-04-11 21:23:34
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: STEP, cali, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3557,29 +3557,4 @@ class AppFacade: -            (
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3557,29 +3557,4 @@ class AppFacade: -                recognition_readiness.PT_ILC_REGISTRY_FILENAME,
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3557,29 +3557,4 @@ class AppFacade: -                dict(pt_ilc_registry or {}),
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3557,29 +3557,4 @@ class AppFacade: -            ),
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3557,29 +3557,4 @@ class AppFacade: -                recognition_readiness.EXTERNAL_COMPARISON_IMPORTER_FILENAME,

### `ec69153b672e4e1ac997c85d2a0ba8108192be32`
- 时间: 2026-04-11 21:18:43 +0800
- 标题: chore: sync 2026-04-11 21:18:42
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -48,0 +49,4 @@ from ...core.reviewer_surface_contracts import ( +from ...core.reviewer_surface_payloads import (
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -48,0 +49,4 @@ from ...core.reviewer_surface_contracts import ( +    extract_wp6_closeout_payloads as _extract_wp6_closeout_payloads,
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -48,0 +49,4 @@ from ...core.reviewer_surface_contracts import ( +    build_wp6_closeout_readiness_pairs as _build_wp6_closeout_readiness_pairs,
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -48,0 +49,4 @@ from ...core.reviewer_surface_contracts import ( +)
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1530,7 +1534,8 @@ class AppFacade: -        pt_ilc_registry = dict(payload.get("pt_ilc_registry", {}) or {})

### `33b82f378def6a3cd9680243e1aba6c93cf91aa6`
- 时间: 2026-04-11 21:13:42 +0800
- 标题: chore: sync 2026-04-11 21:13:38
- 涉及文件: `src/gas_calibrator/v2/core/reviewer_surface_payloads.py`, `src/gas_calibrator/v2/scripts/historical_artifacts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Step, cali, mode, report, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/reviewer_surface_payloads.py @@ -0,0 +1,132 @@ +"""Shared payload extraction helpers for WP6 + step2_closeout_digest reviewer surfaces.
  - src/gas_calibrator/v2/core/reviewer_surface_payloads.py @@ -0,0 +1,132 @@ +
  - src/gas_calibrator/v2/core/reviewer_surface_payloads.py @@ -0,0 +1,132 @@ +All modules that extract WP6+closeout payloads from a results payload dict must
  - src/gas_calibrator/v2/core/reviewer_surface_payloads.py @@ -0,0 +1,132 @@ +use these helpers instead of hand-writing payload.get("pt_ilc_registry") etc.
  - src/gas_calibrator/v2/core/reviewer_surface_payloads.py @@ -0,0 +1,132 @@ +Step 2 boundary:

### `7041c96c528a7cd9718a3be1ab7f27b82ec2a79c`
- 时间: 2026-04-11 20:13:36 +0800
- 标题: chore: sync 2026-04-11 20:13:33
- 涉及文件: `configs/default_config.json`, `src/gas_calibrator/config.py`, `src/gas_calibrator/workflow/runner.py`, `tests/test_config_runtime_defaults.py`, `tests/test_humidity_generator_driver.py`, `tests/test_runner_h2o_sequence.py`, `tests/v2/test_pt_ilc_wp6_contracts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, Serial, Step, cali, h2o, point, readback, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/config.py @@ -98,0 +99,7 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { +            "activation_verify_enabled": True,
  - src/gas_calibrator/config.py @@ -98,0 +99,7 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { +            "activation_verify_min_flow_lpm": 0.5,
  - src/gas_calibrator/config.py @@ -98,0 +99,7 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { +            "activation_verify_timeout_s": 30.0,
  - src/gas_calibrator/config.py @@ -98,0 +99,7 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { +            "activation_verify_poll_s": 1.0,
  - src/gas_calibrator/config.py @@ -98,0 +99,7 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { +            "activation_verify_expect_cooling_margin_c": 1.0,

### `1bbf7cdd7af9c84d50ab274ed3b7456c616cd986`
- 时间: 2026-04-11 20:08:35 +0800
- 标题: chore: sync 2026-04-11 20:08:34
- 涉及文件: `src/gas_calibrator/devices/humidity_generator.py`, `tests/v2/test_pt_ilc_wp6_contracts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Step, cali, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/devices/humidity_generator.py @@ -76,0 +77,10 @@ class HumidityGenerator: +    @classmethod
  - src/gas_calibrator/devices/humidity_generator.py @@ -76,0 +77,10 @@ class HumidityGenerator: +    def _pick_numeric(cls, data: Dict[str, Any], keys: List[str]) -> Optional[float]:
  - src/gas_calibrator/devices/humidity_generator.py @@ -76,0 +77,10 @@ class HumidityGenerator: +        if not isinstance(data, dict):
  - src/gas_calibrator/devices/humidity_generator.py @@ -76,0 +77,10 @@ class HumidityGenerator: +            return None
  - src/gas_calibrator/devices/humidity_generator.py @@ -76,0 +77,10 @@ class HumidityGenerator: +        for key in keys:

### `d236414692432781a2f3f6c57fe726ac3f43f867`
- 时间: 2026-04-11 20:03:34 +0800
- 标题: chore: sync 2026-04-11 20:03:34
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`, `src/gas_calibrator/v2/ui_v2/locales/en_US.json`, `src/gas_calibrator/v2/ui_v2/locales/zh_CN.json`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: STEP, Step, cali, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -44,0 +45,4 @@ from ...core import recognition_readiness_artifacts as recognition_readiness +from ...core.reviewer_surface_contracts import (
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -44,0 +45,4 @@ from ...core import recognition_readiness_artifacts as recognition_readiness +    WP6_CLOSEOUT_ARTIFACT_KEYS as _SHARED_WP6_CLOSEOUT_KEYS,
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -44,0 +45,4 @@ from ...core import recognition_readiness_artifacts as recognition_readiness +    WP6_CLOSEOUT_DISPLAY_LABELS as _SHARED_WP6_CLOSEOUT_DISPLAY_LABELS,
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -44,0 +45,4 @@ from ...core import recognition_readiness_artifacts as recognition_readiness +)
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3571,0 +3576,4 @@ class AppFacade: +            (

### `80586d21e42773a0c79c9eab75a64802c10dcf19`
- 时间: 2026-04-11 19:58:38 +0800
- 标题: chore: sync 2026-04-11 19:58:34
- 涉及文件: `src/gas_calibrator/v2/scripts/historical_artifacts.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, report, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/scripts/historical_artifacts.py @@ -21,0 +22,3 @@ from ..adapters.wp6_gateway import Wp6Gateway +from ..core.reviewer_surface_contracts import (
  - src/gas_calibrator/v2/scripts/historical_artifacts.py @@ -21,0 +22,3 @@ from ..adapters.wp6_gateway import Wp6Gateway +    WP6_CLOSEOUT_ARTIFACT_KEYS as _SHARED_WP6_CLOSEOUT_KEYS,
  - src/gas_calibrator/v2/scripts/historical_artifacts.py @@ -21,0 +22,3 @@ from ..adapters.wp6_gateway import Wp6Gateway +)
  - src/gas_calibrator/v2/scripts/historical_artifacts.py @@ -211,0 +215 @@ def _build_run_report( +    step2_closeout_digest = dict(wp6_payload.get("step2_closeout_digest") or {})
  - src/gas_calibrator/v2/scripts/historical_artifacts.py @@ -465,0 +470 @@ def _build_run_report( +        "step2_closeout_digest": step2_closeout_digest,

### `33e7e5789a54b167e2f42ab4d97c6bed2ccc691b`
- 时间: 2026-04-11 19:53:33 +0800
- 标题: chore: sync 2026-04-11 19:53:33
- 涉及文件: `src/gas_calibrator/v2/core/artifact_compatibility.py`, `src/gas_calibrator/v2/core/offline_artifacts.py`, `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: STEP, Step, cali, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -22,0 +23,5 @@ from . import recognition_readiness_artifacts as recognition_readiness +from .reviewer_surface_contracts import (
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -22,0 +23,5 @@ from . import recognition_readiness_artifacts as recognition_readiness +    WP6_CLOSEOUT_ARTIFACT_KEYS as _SHARED_WP6_CLOSEOUT_KEYS,
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -22,0 +23,5 @@ from . import recognition_readiness_artifacts as recognition_readiness +    WP6_CLOSEOUT_ARTIFACT_ROLES as _SHARED_WP6_CLOSEOUT_ROLES,
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -22,0 +23,5 @@ from . import recognition_readiness_artifacts as recognition_readiness +    WP6_CLOSEOUT_FILENAME_MAP as _SHARED_WP6_CLOSEOUT_FILENAME_MAP,
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -22,0 +23,5 @@ from . import recognition_readiness_artifacts as recognition_readiness +)

### `c56b9ca6eccae24ae0fecdab9d040aa83ce543a6`
- 时间: 2026-04-11 19:48:36 +0800
- 标题: chore: sync 2026-04-11 19:48:33
- 涉及文件: `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`, `src/gas_calibrator/v2/core/reviewer_surface_contracts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: STEP, Step, cali, mode, step, v1
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -30,0 +31,26 @@ from .reviewer_fragments_contract import ( +from .reviewer_surface_contracts import (
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -30,0 +31,26 @@ from .reviewer_fragments_contract import ( +    WP6_CLOSEOUT_ARTIFACT_KEYS as WP6_CLOSEOUT_ARTIFACT_KEYS,
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -30,0 +31,26 @@ from .reviewer_fragments_contract import ( +    WP6_CLOSEOUT_DISPLAY_LABELS as WP6_CLOSEOUT_DISPLAY_LABELS,
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -30,0 +31,26 @@ from .reviewer_fragments_contract import ( +    WP6_CLOSEOUT_DISPLAY_LABELS_EN as WP6_CLOSEOUT_DISPLAY_LABELS_EN,
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -30,0 +31,26 @@ from .reviewer_fragments_contract import ( +    WP6_CLOSEOUT_ANCHOR_DEFAULTS as _SHARED_ANCHOR_DEFAULTS,

### `cbedb952ffdd53aceadfacaee703db951088c83b`
- 时间: 2026-04-11 18:58:34 +0800
- 标题: chore: sync 2026-04-11 18:58:32
- 涉及文件: `src/gas_calibrator/workflow/runner.py`, `tests/test_runner_h2o_sequence.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, Point, cali, co2, db, h2o, mode, point
- 关键 diff hunk 摘要:
  - src/gas_calibrator/workflow/runner.py @@ -5266,4 +5266 @@ class CalibrationRunner: -        h2o_points = self._filter_execution_points_by_selected_pressure(
  - src/gas_calibrator/workflow/runner.py @@ -5266,4 +5266 @@ class CalibrationRunner: -            [point for point in next_group if point.is_h2o_point]
  - src/gas_calibrator/workflow/runner.py @@ -5266,4 +5266 @@ class CalibrationRunner: -        )
  - src/gas_calibrator/workflow/runner.py @@ -5266,4 +5266 @@ class CalibrationRunner: -        groups = self._group_h2o_points(h2o_points)
  - src/gas_calibrator/workflow/runner.py @@ -5266,4 +5266 @@ class CalibrationRunner: +        groups = self._h2o_source_groups_for_temperature(next_group)

### `02f319b0e5d4ef79f4751535296a7254000a265e`
- 时间: 2026-04-11 18:33:41 +0800
- 标题: chore: sync 2026-04-11 18:33:40
- 涉及文件: `configs/user_tuning.json`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py`, `tests/test_run_v1_corrected_autodelivery.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Readback, SPAN, Span, cali, calibration, delivery, span, v1
- 关键 diff hunk 摘要:
  - src/gas_calibrator/tools/run_v1_corrected_autodelivery.py @@ -23,0 +24,2 @@ _PRESSURE_WRITE_MAX_GAUGE_CONTROLLER_MAX_ABS_HPA = 8.0 +_STARTUP_PRESSURE_WRITE_MIN_SAMPLES = 3
  - src/gas_calibrator/tools/run_v1_corrected_autodelivery.py @@ -23,0 +24,2 @@ _PRESSURE_WRITE_MAX_GAUGE_CONTROLLER_MAX_ABS_HPA = 8.0 +_STARTUP_PRESSURE_WRITE_MAX_REFERENCE_SPAN_HPA = 2.0
  - src/gas_calibrator/tools/run_v1_corrected_autodelivery.py @@ -241,0 +244,5 @@ def load_startup_pressure_calibration_rows(run_dir: str | Path) -> List[Dict[str +    detail_path = summary_path.with_name("detail.csv")
  - src/gas_calibrator/tools/run_v1_corrected_autodelivery.py @@ -241,0 +244,5 @@ def load_startup_pressure_calibration_rows(run_dir: str | Path) -> List[Dict[str +    detail_rows: List[Dict[str, Any]] = []
  - src/gas_calibrator/tools/run_v1_corrected_autodelivery.py @@ -241,0 +244,5 @@ def load_startup_pressure_calibration_rows(run_dir: str | Path) -> List[Dict[str +    if detail_path.exists():

### `7532e172729e17d921f670d0bf5376762a9e59ef`
- 时间: 2026-04-11 18:23:42 +0800
- 标题: chore: sync 2026-04-11 18:23:41
- 涉及文件: `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py`, `tests/test_run_v1_corrected_autodelivery.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Mode, cali, coefficient, delivery, mode, senco, v1
- 关键 diff hunk 摘要:
  - src/gas_calibrator/tools/run_v1_corrected_autodelivery.py @@ -284 +284 @@ def compute_pressure_offset_rows( -    controller_candidates = ["鍘嬪姏鎺у埗鍣ㄥ帇鍔沨Pa", "pressure_controller_hpa", "PressureControllerHpa"]
  - src/gas_calibrator/tools/run_v1_corrected_autodelivery.py @@ -284 +284 @@ def compute_pressure_offset_rows( +    controller_candidates = ["压力控制器压力hPa", "pressure_controller_hpa", "PressureControllerHpa"]
  - tests/test_run_v1_corrected_autodelivery.py @@ -88,0 +89,32 @@ def test_compute_pressure_offset_rows_uses_ambient_pressure_gauge_samples(tmp_pa +def test_compute_pressure_offset_rows_marks_large_gauge_controller_gap_as_not_recommended(tmp_path: Path) -> None:
  - tests/test_run_v1_corrected_autodelivery.py @@ -88,0 +89,32 @@ def test_compute_pressure_offset_rows_uses_ambient_pressure_gauge_samples(tmp_pa +    run_dir = tmp_path / "run_2_backpressure"
  - tests/test_run_v1_corrected_autodelivery.py @@ -88,0 +89,32 @@ def test_compute_pressure_offset_rows_uses_ambient_pressure_gauge_samples(tmp_pa +    run_dir.mkdir()

### `68403f3f2a051f2775d5ecf6b729aca5965bfe6e`
- 时间: 2026-04-11 18:18:41 +0800
- 标题: chore: sync 2026-04-11 18:18:40
- 涉及文件: `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Readback, cali, coefficient, delivery, v1
- 关键 diff hunk 摘要:
  - src/gas_calibrator/tools/run_v1_corrected_autodelivery.py @@ -20,0 +21,4 @@ from .run_v1_no500_postprocess import _filter_no_500_frame +_PRESSURE_WRITE_MIN_GAUGE_CONTROLLER_OVERLAP = 5
  - src/gas_calibrator/tools/run_v1_corrected_autodelivery.py @@ -20,0 +21,4 @@ from .run_v1_no500_postprocess import _filter_no_500_frame +_PRESSURE_WRITE_MAX_GAUGE_CONTROLLER_MEAN_ABS_HPA = 3.0
  - src/gas_calibrator/tools/run_v1_corrected_autodelivery.py @@ -20,0 +21,4 @@ from .run_v1_no500_postprocess import _filter_no_500_frame +_PRESSURE_WRITE_MAX_GAUGE_CONTROLLER_MAX_ABS_HPA = 8.0
  - src/gas_calibrator/tools/run_v1_corrected_autodelivery.py @@ -20,0 +21,4 @@ from .run_v1_no500_postprocess import _filter_no_500_frame +
  - src/gas_calibrator/tools/run_v1_corrected_autodelivery.py @@ -275 +279 @@ def compute_pressure_offset_rows( -    ref_candidates = [

### `440dd0ae30c6a01b6393a15733403358617a08d2`
- 时间: 2026-04-11 18:15:03 +0800
- 标题: chore: sync 2026-04-11 18:15:02
- 涉及文件: `tools/absorbance_debugger/analysis/candidate_tournament.py`, `tools/absorbance_debugger/tests/test_absorbance_debugger.py`
- 判定原因: diff 内容命中关键词: h2o, mode, point, report, step, zero
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/analysis/candidate_tournament.py @@ -686,0 +687,8 @@ def _custom_family_frame( +        "delta_h2o_ratio_vs_legacy_summary_anchor",
  - tools/absorbance_debugger/analysis/candidate_tournament.py @@ -686,0 +687,8 @@ def _custom_family_frame( +        "delta_h2o_ratio_vs_legacy_zero_ppm_anchor",
  - tools/absorbance_debugger/analysis/candidate_tournament.py @@ -686,0 +687,8 @@ def _custom_family_frame( +        "delta_h2o_ratio_vs_subzero_anchor",
  - tools/absorbance_debugger/analysis/candidate_tournament.py @@ -686,0 +687,8 @@ def _custom_family_frame( +        "delta_h2o_ratio_vs_zeroC_anchor",
  - tools/absorbance_debugger/tests/test_absorbance_debugger.py @@ -2283,0 +2284,4 @@ def test_cross_run_batch_writes_scoped_old_vs_new_outputs(monkeypatch, tmp_path: +    assert (output_dir / "step_11_candidate_tournament_detail.csv").exists()

### `62a8a5649d72defe6d7c7f4417988e16ec547592`
- 时间: 2026-04-11 18:08:54 +0800
- 标题: chore: sync 2026-04-11 18:08:52
- 涉及文件: `tests/v2/test_pt_ilc_wp6_contracts.py`, `tools/absorbance_debugger/analysis/candidate_tournament.py`, `tools/absorbance_debugger/reports/renderers.py`, `tools/absorbance_debugger/tests/test_absorbance_debugger.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: co2, h2o, mode, point, report, zero
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/analysis/candidate_tournament.py @@ -231,0 +232,19 @@ def _point_feature_summary(filtered: pd.DataFrame) -> pd.DataFrame: +        "target_ppm": "target_co2_ppm",
  - tools/absorbance_debugger/analysis/candidate_tournament.py @@ -231,0 +232,19 @@ def _point_feature_summary(filtered: pd.DataFrame) -> pd.DataFrame: +        "ratio_co2_raw_mean": "ratio_co2_raw",
  - tools/absorbance_debugger/analysis/candidate_tournament.py @@ -231,0 +232,19 @@ def _point_feature_summary(filtered: pd.DataFrame) -> pd.DataFrame: +        "ratio_co2_filt_mean": "ratio_co2_filt",
  - tools/absorbance_debugger/analysis/candidate_tournament.py @@ -231,0 +232,19 @@ def _point_feature_summary(filtered: pd.DataFrame) -> pd.DataFrame: +        "h2o_ratio_raw_mean": "ratio_h2o_raw",
  - tools/absorbance_debugger/analysis/candidate_tournament.py @@ -231,0 +232,19 @@ def _point_feature_summary(filtered: pd.DataFrame) -> pd.DataFrame: +        "h2o_ratio_filt_mean": "ratio_h2o_filt",

### `47e0ef4869b4a1f8c0a0d45f805a46a3246c15f7`
- 时间: 2026-04-11 18:03:34 +0800
- 标题: chore: sync 2026-04-11 18:03:33
- 涉及文件: `tools/absorbance_debugger/analysis/candidate_tournament.py`
- 判定原因: diff 内容命中关键词: MODE, co2, h2o, mode, point, zero
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/analysis/candidate_tournament.py @@ -0,0 +1,1296 @@ +from .absorbance_models import _fit_one_candidate, active_model_specs
  - tools/absorbance_debugger/analysis/candidate_tournament.py @@ -0,0 +1,1296 @@ +SOURCE_MODES: tuple[str, ...] = (
  - tools/absorbance_debugger/analysis/candidate_tournament.py @@ -0,0 +1,1296 @@ +MODEL_FAMILIES: tuple[str, ...] = (
  - tools/absorbance_debugger/analysis/candidate_tournament.py @@ -0,0 +1,1296 @@ +    "source_mode",
  - tools/absorbance_debugger/analysis/candidate_tournament.py @@ -0,0 +1,1296 @@ +    "model_family",

### `8567df9570466b10932bf3653316312dde027d11`
- 时间: 2026-04-11 17:58:40 +0800
- 标题: chore: sync 2026-04-11 17:58:33
- 涉及文件: `tests/v2/test_pt_ilc_wp6_contracts.py`, `tools/absorbance_debugger/analysis/pipeline.py`, `tools/absorbance_debugger/app.py`, `tools/absorbance_debugger/plots/charts.py`, `tools/absorbance_debugger/reports/renderers.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Report, Step, cali, co2, mode, point, protocol, report
- 关键 diff hunk 摘要:
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -676,0 +677,185 @@ class TestStep2SurfaceConsistency: +# 7) Step 2.1 reviewer evidence chain hardening tests
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -676,0 +677,185 @@ class TestStep2SurfaceConsistency: +class TestStep2PayloadClassification:
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -676,0 +677,185 @@ class TestStep2SurfaceConsistency: +    """_classify_step2_payload_status must correctly handle nested boundary flags."""
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -676,0 +677,185 @@ class TestStep2SurfaceConsistency: +        from gas_calibrator.v2.core.wp6_builder import _classify_step2_payload_status
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -676,0 +677,185 @@ class TestStep2SurfaceConsistency: +        assert _classify_step2_payload_status(payload) == "simulated_readiness_only"

### `b963454cf1caead71098249ffbad793514a486b6`
- 时间: 2026-04-11 17:38:37 +0800
- 标题: chore: sync 2026-04-11 17:38:36
- 涉及文件: `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: STEP, Step, cali, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -126,0 +127,31 @@ STEP2_CLOSEOUT_DIGEST_MARKDOWN_FILENAME = "step2_closeout_digest.md" +# Unified WP6 + closeout artifact key list and display order
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -126,0 +127,31 @@ STEP2_CLOSEOUT_DIGEST_MARKDOWN_FILENAME = "step2_closeout_digest.md" +WP6_CLOSEOUT_ARTIFACT_KEYS: tuple[str, ...] = (
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -126,0 +127,31 @@ STEP2_CLOSEOUT_DIGEST_MARKDOWN_FILENAME = "step2_closeout_digest.md" +    "pt_ilc_registry",
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -126,0 +127,31 @@ STEP2_CLOSEOUT_DIGEST_MARKDOWN_FILENAME = "step2_closeout_digest.md" +    "external_comparison_importer",
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -126,0 +127,31 @@ STEP2_CLOSEOUT_DIGEST_MARKDOWN_FILENAME = "step2_closeout_digest.md" +    "comparison_evidence_pack",

### `0208fa72840258445633702676b72c14ebead5c2`
- 时间: 2026-04-11 17:33:34 +0800
- 标题: chore: sync 2026-04-11 17:33:32
- 涉及文件: `src/gas_calibrator/v2/core/artifact_compatibility.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: STEP, cali, mode, step, v1
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -779,0 +780,71 @@ def _normalize_artifact_compatibility_payloads( +# ---------------------------------------------------------------------------
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -779,0 +780,71 @@ def _normalize_artifact_compatibility_payloads( +# WP6 + step2_closeout_digest explicit contract entries
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -779,0 +780,71 @@ def _normalize_artifact_compatibility_payloads( +
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -779,0 +780,71 @@ def _normalize_artifact_compatibility_payloads( +_WP6_CLOSEOUT_ARTifact_KEYS = (
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -779,0 +780,71 @@ def _normalize_artifact_compatibility_payloads( +    "pt_ilc_registry",

### `89f16bf3cae8b208d09929e29e8fac6c4a2b0a9b`
- 时间: 2026-04-11 17:28:36 +0800
- 标题: chore: sync 2026-04-11 17:28:35
- 涉及文件: `src/gas_calibrator/v2/core/artifact_compatibility.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -837,0 +838,2 @@ def build_artifact_compatibility_bundle( +    # Ensure WP6 + step2_closeout_digest have explicit contract entries even if not on disk
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -837,0 +838,2 @@ def build_artifact_compatibility_bundle( +    entries = _ensure_wp6_closeout_contract_entries(entries, run_dir=run_dir, run_id=run_id, role_catalog=merged_role_catalog)

### `d85d85616c421f68ab69d43cde08fdd196b75aea`
- 时间: 2026-04-11 17:13:38 +0800
- 标题: chore: sync 2026-04-11 17:13:33
- 涉及文件: `src/gas_calibrator/v2/core/wp6_builder.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: STEP, Step, cali, step, v1
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/wp6_builder.py @@ -947,0 +948,96 @@ STEP2_CLOSEOUT_SCHEMA_VERSION = "step2-closeout-digest-v1" +# ---------------------------------------------------------------------------
  - src/gas_calibrator/v2/core/wp6_builder.py @@ -947,0 +948,96 @@ STEP2_CLOSEOUT_SCHEMA_VERSION = "step2-closeout-digest-v1" +# Shared helpers for Step 2 boundary classification
  - src/gas_calibrator/v2/core/wp6_builder.py @@ -947,0 +948,96 @@ STEP2_CLOSEOUT_SCHEMA_VERSION = "step2-closeout-digest-v1" +
  - src/gas_calibrator/v2/core/wp6_builder.py @@ -947,0 +948,96 @@ STEP2_CLOSEOUT_SCHEMA_VERSION = "step2-closeout-digest-v1" +_SIMULATED_ONLY_SIGNALS = (
  - src/gas_calibrator/v2/core/wp6_builder.py @@ -947,0 +948,96 @@ STEP2_CLOSEOUT_SCHEMA_VERSION = "step2-closeout-digest-v1" +    "evidence_source",       # == "simulated"

### `69a03ec0b8a2a8c685e30fb211d23d800d72b430`
- 时间: 2026-04-11 17:08:36 +0800
- 标题: chore: sync 2026-04-11 17:08:35
- 涉及文件: `src/gas_calibrator/v2/core/wp6_builder.py`, `tools/absorbance_debugger/analysis/comparison.py`, `tools/absorbance_debugger/app.py`, `tools/absorbance_debugger/plots/charts.py`, `tools/absorbance_debugger/reports/renderers.py`, `tools/absorbance_debugger/tests/test_absorbance_debugger.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, cali, co2, point, report, step, zero
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/wp6_builder.py @@ -982,7 +982 @@ def build_step2_closeout_digest( -        raw = payload.get("raw", payload)
  - src/gas_calibrator/v2/core/wp6_builder.py @@ -982,7 +982 @@ def build_step2_closeout_digest( -        if raw.get("not_real_acceptance_evidence") is True:
  - src/gas_calibrator/v2/core/wp6_builder.py @@ -982,7 +982 @@ def build_step2_closeout_digest( -            wp_status[label] = "simulated_readiness_only"
  - src/gas_calibrator/v2/core/wp6_builder.py @@ -982,7 +982 @@ def build_step2_closeout_digest( -        elif raw.get("available") is False:
  - src/gas_calibrator/v2/core/wp6_builder.py @@ -982,7 +982 @@ def build_step2_closeout_digest( -            wp_status[label] = "not_available"

### `f3f94cc580f14c6dd6e57e99c639eb8071982baa`
- 时间: 2026-04-11 16:58:40 +0800
- 标题: chore: sync 2026-04-11 16:58:39
- 涉及文件: `configs/user_tuning.json`
- 判定原因: diff 内容命中关键词: co2, point, span
- 关键 diff hunk 摘要:
  - configs/user_tuning.json @@ -17,0 +29,7 @@ +                                       "co2_route":  {
  - configs/user_tuning.json @@ -17,0 +29,7 @@ +                                                          "first_point_preseal_soak_s":  600
  - configs/user_tuning.json @@ -17,0 +29,7 @@ +                                       "gas_route_dewpoint_gate_policy":  "warn",
  - configs/user_tuning.json @@ -17,0 +29,7 @@ +                                       "gas_route_dewpoint_gate_tail_span_max_c":  0.45,
  - configs/user_tuning.json @@ -17,0 +29,7 @@ +                                       "gas_route_dewpoint_gate_tail_slope_abs_max_c_per_s":  0.005,

### `4d85cbbeb9f6dbc104658cf90f63ce38fc5a6243`
- 时间: 2026-04-11 16:38:47 +0800
- 标题: chore: sync 2026-04-11 16:38:46
- 涉及文件: `tools/absorbance_debugger/analysis/comparison.py`, `tools/absorbance_debugger/app.py`, `tools/absorbance_debugger/plots/charts.py`, `tools/absorbance_debugger/reports/renderers.py`, `tools/absorbance_debugger/tests/test_absorbance_debugger.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: db, mode, point, report, step, zero
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/analysis/comparison.py @@ -109,0 +110,22 @@ SCOPED_SEGMENT_COLUMNS: tuple[str, ...] = ( +    "point_count_used",
  - tools/absorbance_debugger/analysis/comparison.py @@ -109,0 +110,22 @@ SCOPED_SEGMENT_COLUMNS: tuple[str, ...] = ( +    "whether_point_table_mean_was_used",
  - tools/absorbance_debugger/analysis/comparison.py @@ -109,0 +110,22 @@ SCOPED_SEGMENT_COLUMNS: tuple[str, ...] = ( +    "future_external_reporting_rule",
  - tools/absorbance_debugger/analysis/comparison.py @@ -1521,0 +1544,122 @@ def build_scoped_old_vs_new_outputs( +def _scope_point_count(scope_output: dict[str, pd.DataFrame | str]) -> int:
  - tools/absorbance_debugger/analysis/comparison.py @@ -1521,0 +1544,122 @@ def build_scoped_old_vs_new_outputs( +            return int(pd.to_numeric(overall_row.iloc[0].get("point_count"), errors="coerce") or 0)

### `a10e5bf6c2b789dc503c06281c31af89159c8432`
- 时间: 2026-04-11 16:08:32 +0800
- 标题: chore: sync 2026-04-11 16:08:30
- 涉及文件: `tools/absorbance_debugger/app.py`, `tools/absorbance_debugger/reports/renderers.py`, `tools/absorbance_debugger/tests/test_absorbance_debugger.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Report, co2, mode, point, report, step, zero
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/app.py @@ -26,0 +28 @@ from .options import ( +from .reports.renderers import render_scoped_old_vs_new_report_markdown
  - tools/absorbance_debugger/app.py @@ -217,0 +220,50 @@ def run_debugger_batch( +        resolved_output / "step_09c_historical_ga02_ga03_old_vs_new_detail.csv",
  - tools/absorbance_debugger/app.py @@ -217,0 +220,50 @@ def run_debugger_batch( +        resolved_output / "step_09c_historical_ga02_ga03_old_vs_new_summary.csv",
  - tools/absorbance_debugger/app.py @@ -217,0 +220,50 @@ def run_debugger_batch( +        resolved_output / "step_09c_historical_ga02_ga03_local_wins.csv",
  - tools/absorbance_debugger/app.py @@ -217,0 +220,50 @@ def run_debugger_batch( +        resolved_output / "step_09c_historical_ga02_ga03_plot.png",

### `9cd3b3276b2b4cdee470d73c8fffab72d2ae39de`
- 时间: 2026-04-11 16:03:33 +0800
- 标题: chore: sync 2026-04-11 16:03:32
- 涉及文件: `tools/absorbance_debugger/analysis/comparison.py`, `tools/absorbance_debugger/plots/charts.py`
- 判定原因: diff 内容命中关键词: db, point, zero
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/analysis/comparison.py @@ -22,0 +23,87 @@ CONCENTRATION_BUCKETS: tuple[tuple[str, float | None, float | None], ...] = ( +    "old_chain_zero_rmse",
  - tools/absorbance_debugger/analysis/comparison.py @@ -22,0 +23,87 @@ CONCENTRATION_BUCKETS: tuple[tuple[str, float | None, float | None], ...] = ( +    "new_chain_zero_rmse",
  - tools/absorbance_debugger/analysis/comparison.py @@ -22,0 +23,87 @@ CONCENTRATION_BUCKETS: tuple[tuple[str, float | None, float | None], ...] = ( +    "delta_zero_rmse",
  - tools/absorbance_debugger/analysis/comparison.py @@ -22,0 +23,87 @@ CONCENTRATION_BUCKETS: tuple[tuple[str, float | None, float | None], ...] = ( +    "improvement_pct_zero",
  - tools/absorbance_debugger/analysis/comparison.py @@ -22,0 +23,87 @@ CONCENTRATION_BUCKETS: tuple[tuple[str, float | None, float | None], ...] = ( +    "point_count",

### `e965cb791044841f43bda35400d38f4ef8c123aa`
- 时间: 2026-04-11 15:53:32 +0800
- 标题: chore: sync 2026-04-11 15:53:30
- 涉及文件: `tools/absorbance_debugger/analysis/analyzer_sidecar.py`
- 判定原因: diff 内容命中关键词: mode, point
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/analysis/analyzer_sidecar.py @@ -328,0 +330,4 @@ def _detail_row( +    win_rows = mode_frame[sidecar_vs_current > 1.0e-12].copy()
  - tools/absorbance_debugger/analysis/analyzer_sidecar.py @@ -328,0 +330,4 @@ def _detail_row( +    loss_rows = mode_frame[sidecar_vs_current_loss > 1.0e-12].copy()
  - tools/absorbance_debugger/analysis/analyzer_sidecar.py @@ -349,2 +354,2 @@ def _detail_row( -        "local_win_examples": _format_example_series(mode_frame[sidecar_vs_current > 1.0e-12].assign(local_gain=sidecar_vs_current), "local_gain"),
  - tools/absorbance_debugger/analysis/analyzer_sidecar.py @@ -349,2 +354,2 @@ def _detail_row( -        "local_loss_examples": _format_example_series(mode_frame[sidecar_vs_current_loss > 1.0e-12].assign(local_loss=sidecar_vs_current_loss), "local_loss"),
  - tools/absorbance_debugger/analysis/analyzer_sidecar.py @@ -532,6 +537,6 @@ def build_analyzer_sidecar_challenge( -        replacement = frame[["analyzer_id", "point_title", "point_row", "sidecar_pred_ppm", "sidecar_error_ppm"]].copy()

### `dc8a870790329323ca77120cef7bdb9b05571678`
- 时间: 2026-04-11 15:43:34 +0800
- 标题: chore: sync 2026-04-11 15:43:33
- 涉及文件: `tools/absorbance_debugger/analysis/analyzer_sidecar.py`
- 判定原因: diff 内容命中关键词: db, mode, point
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/analysis/analyzer_sidecar.py @@ -245 +245,3 @@ def _same_family_refit_frame( -    return scope_frame[["point_title", "point_row", "target_ppm", "sidecar_pred_ppm", "sidecar_error_ppm"]].copy(), chosen_model
  - tools/absorbance_debugger/analysis/analyzer_sidecar.py @@ -245 +245,3 @@ def _same_family_refit_frame( +    output = scope_frame[["point_title", "point_row", "target_ppm", "sidecar_pred_ppm", "sidecar_error_ppm"]].copy()
  - tools/absorbance_debugger/analysis/analyzer_sidecar.py @@ -245 +245,3 @@ def _same_family_refit_frame( +    return output[["analyzer_id", "point_title", "point_row", "target_ppm", "sidecar_pred_ppm", "sidecar_error_ppm"]], chosen_model

### `f7b714af8e08fab2faa9b2e509e0d64bf7ed7d36`
- 时间: 2026-04-11 15:38:36 +0800
- 标题: chore: sync 2026-04-11 15:38:36
- 涉及文件: `tools/absorbance_debugger/analysis/analyzer_sidecar.py`, `tools/absorbance_debugger/analysis/pipeline.py`, `tools/absorbance_debugger/plots/charts.py`, `tools/absorbance_debugger/tests/test_absorbance_debugger.py`
- 判定原因: diff 内容命中关键词: co2, h2o, insert, mode, point, report, step, zero
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/analysis/analyzer_sidecar.py @@ -60,0 +61,19 @@ def _ratio_source_from_pair(source_pair: str) -> str: +def _format_point_label(row: pd.Series) -> str:
  - tools/absorbance_debugger/analysis/analyzer_sidecar.py @@ -60,0 +61,19 @@ def _ratio_source_from_pair(source_pair: str) -> str: +    return "|".join(_format_point_label(row) for _, row in ranked.iterrows())
  - tools/absorbance_debugger/analysis/analyzer_sidecar.py @@ -305,0 +325,2 @@ def _detail_row( +    sidecar_vs_current = mode_frame["abs_error_current"] - mode_frame["abs_error_sidecar"]
  - tools/absorbance_debugger/analysis/analyzer_sidecar.py @@ -305,0 +325,2 @@ def _detail_row( +    sidecar_vs_current_loss = mode_frame["abs_error_sidecar"] - mode_frame["abs_error_current"]
  - tools/absorbance_debugger/analysis/analyzer_sidecar.py @@ -325,0 +347,386 @@ def _detail_row( +        "local_win_examples": _format_example_series(mode_frame[sidecar_vs_current > 1.0e-12].assign(local_gain=sidecar_vs_current), "local_gain"),

### `9958f1a64a976ba74bff353c61104bcd354b7031`
- 时间: 2026-04-11 15:33:35 +0800
- 标题: chore: sync 2026-04-11 15:33:31
- 涉及文件: `tools/absorbance_debugger/analysis/analyzer_sidecar.py`
- 判定原因: diff 内容命中关键词: co2, mode, point, zero
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/analysis/analyzer_sidecar.py @@ -159,0 +160,167 @@ def _prediction_frame_for_scope(residual_df: pd.DataFrame, requested_scope: str) +    absorbance_point_variants: pd.DataFrame,
  - tools/absorbance_debugger/analysis/analyzer_sidecar.py @@ -159,0 +160,167 @@ def _prediction_frame_for_scope(residual_df: pd.DataFrame, requested_scope: str) +    fixed_model_family: str,
  - tools/absorbance_debugger/analysis/analyzer_sidecar.py @@ -159,0 +160,167 @@ def _prediction_frame_for_scope(residual_df: pd.DataFrame, requested_scope: str) +    fixed_zero_residual_mode: str,
  - tools/absorbance_debugger/analysis/analyzer_sidecar.py @@ -159,0 +160,167 @@ def _prediction_frame_for_scope(residual_df: pd.DataFrame, requested_scope: str) +    subset = absorbance_point_variants[
  - tools/absorbance_debugger/analysis/analyzer_sidecar.py @@ -159,0 +160,167 @@ def _prediction_frame_for_scope(residual_df: pd.DataFrame, requested_scope: str) +        (absorbance_point_variants["analyzer"].astype(str) == analyzer_id)

### `2e992ebe1840d1250cde6a22f2b30a05cd9204b0`
- 时间: 2026-04-11 15:28:36 +0800
- 标题: chore: sync 2026-04-11 15:28:36
- 涉及文件: `tools/absorbance_debugger/analysis/analyzer_sidecar.py`, `tools/absorbance_debugger/analysis/remaining_gap.py`
- 判定原因: diff 内容命中关键词: co2, h2o, mode, point, zero
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/analysis/analyzer_sidecar.py @@ -0,0 +1,159 @@ +from .absorbance_models import _fit_one_candidate, active_model_specs
  - tools/absorbance_debugger/analysis/analyzer_sidecar.py @@ -0,0 +1,159 @@ +    if segment_tag == "zero":
  - tools/absorbance_debugger/analysis/analyzer_sidecar.py @@ -0,0 +1,159 @@ +    return "ratio_co2_raw" if str(source_pair) == "raw/raw" else "ratio_co2_filt"
  - tools/absorbance_debugger/analysis/analyzer_sidecar.py @@ -0,0 +1,159 @@ +def _point_feature_summary(filtered_samples: pd.DataFrame, analyzer_id: str, selected_source_pair: str) -> pd.DataFrame:
  - tools/absorbance_debugger/analysis/analyzer_sidecar.py @@ -0,0 +1,159 @@ +        subset.groupby(["analyzer", "point_title", "point_row"], dropna=False)

### `035e24f1995c9c27fc5323b162d35b7d86375874`
- 时间: 2026-04-11 15:18:35 +0800
- 标题: chore: sync 2026-04-11 15:18:35
- 涉及文件: `src/gas_calibrator/v2/scripts/historical_artifacts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, db, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/scripts/historical_artifacts.py @@ -206,0 +207 @@ def _build_run_report( +    external_comparison_importer = dict(wp6_payload.get("external_comparison_importer") or {})

### `57ce05472adb03dbbd249d111f9eec530d96ee82`
- 时间: 2026-04-11 15:08:33 +0800
- 标题: chore: sync 2026-04-11 15:08:31
- 涉及文件: `tests/v2/test_pt_ilc_wp6_contracts.py`
- 判定原因: diff 内容命中关键词: Step, db
- 关键 diff hunk 摘要:
  - 未提取到关键词 hunk，建议结合 raw git log 复核。

### `7439e6388b98b18e79e3d8376f0e6f91541aad66`
- 时间: 2026-04-11 15:03:37 +0800
- 标题: chore: sync 2026-04-11 15:03:36
- 涉及文件: `tests/v2/test_pt_ilc_wp6_contracts.py`
- 判定原因: diff 内容命中关键词: REPORT, STEP, Step, cali, point, protocol, report, step
- 关键 diff hunk 摘要:
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -532,0 +533,141 @@ class TestWp6ReviewerSurfaceBoundary: +# 6) Step 2 总收口测试: role catalog / closeout digest / consistency
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -532,0 +533,141 @@ class TestWp6ReviewerSurfaceBoundary: +class TestStep2RoleCatalogConsistency:
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -532,0 +533,141 @@ class TestWp6ReviewerSurfaceBoundary: +        from gas_calibrator.v2.core.artifact_catalog import DEFAULT_ROLE_CATALOG
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -532,0 +533,141 @@ class TestWp6ReviewerSurfaceBoundary: +                     "step2_closeout_digest"):
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -532,0 +533,141 @@ class TestWp6ReviewerSurfaceBoundary: +                     "step2_closeout_digest_markdown"):

### `bcc0839bd18832ae92887b81204c19a2576e7c10`
- 时间: 2026-04-11 14:53:36 +0800
- 标题: chore: sync 2026-04-11 14:53:35
- 涉及文件: `src/gas_calibrator/config.py`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py`, `src/gas_calibrator/tools/safe_stop.py`, `tests/test_config_runtime_defaults.py`, `tests/test_run_v1_corrected_autodelivery.py`, `tests/test_safe_stop_tool.py`, `tools/absorbance_debugger/tests/test_absorbance_debugger.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: GETCO, READBACK, Readback, cali, co2, coefficient, delivery, mode
- 关键 diff hunk 摘要:
  - src/gas_calibrator/config.py @@ -97 +97 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { -            "safe_stop_timeout_s": 5.0,
  - src/gas_calibrator/config.py @@ -97 +97 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { +            "safe_stop_timeout_s": 15.0,
  - src/gas_calibrator/config.py @@ -99,0 +100,4 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { +        "safe_stop": {
  - src/gas_calibrator/config.py @@ -99,0 +100,4 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { +            "perform_attempts": 4,
  - src/gas_calibrator/config.py @@ -99,0 +100,4 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { +            "retry_delay_s": 2.0,

### `efa71524e6843b2861d5691295ede4be7df395a3`
- 时间: 2026-04-11 14:48:34 +0800
- 标题: chore: sync 2026-04-11 14:48:33
- 涉及文件: `src/gas_calibrator/v2/core/artifact_compatibility.py`, `src/gas_calibrator/v2/core/offline_artifacts.py`, `tools/absorbance_debugger/analysis/pipeline.py`, `tools/absorbance_debugger/analysis/source_policy.py`, `tools/absorbance_debugger/tests/test_absorbance_debugger.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: STEP, cali, mode, point, report, step, zero
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -171,0 +172,2 @@ CANONICAL_SURFACE_FILENAMES = frozenset( +        recognition_readiness.STEP2_CLOSEOUT_DIGEST_FILENAME,
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -171,0 +172,2 @@ CANONICAL_SURFACE_FILENAMES = frozenset( +        recognition_readiness.STEP2_CLOSEOUT_DIGEST_MARKDOWN_FILENAME,
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -1561,0 +1564 @@ def _surface_visibility(*, artifact_key: str, artifact_role: str) -> list[str]: +        "step2_closeout_digest",
  - src/gas_calibrator/v2/core/offline_artifacts.py @@ -1638,0 +1639,2 @@ def export_run_offline_artifacts( +        "step2_closeout_digest": str(run_dir / recognition_readiness.STEP2_CLOSEOUT_DIGEST_FILENAME),
  - src/gas_calibrator/v2/core/offline_artifacts.py @@ -1638,0 +1639,2 @@ def export_run_offline_artifacts( +        "step2_closeout_digest_markdown": str(run_dir / recognition_readiness.STEP2_CLOSEOUT_DIGEST_MARKDOWN_FILENAME),

### `09a46a465b61e6d0d848316fdb41cd0f91422ed7`
- 时间: 2026-04-11 14:43:34 +0800
- 标题: chore: sync 2026-04-11 14:43:32
- 涉及文件: `src/gas_calibrator/v2/core/artifact_catalog.py`, `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`, `tools/absorbance_debugger/analysis/pipeline.py`, `tools/absorbance_debugger/analysis/source_policy.py`, `tools/absorbance_debugger/plots/charts.py`, `tools/absorbance_debugger/reports/renderers.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: REPORT, cali, db, insert, mode, point, protocol, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -137,0 +138,2 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "step2_closeout_digest",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -137,0 +138,2 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "step2_closeout_digest_markdown",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -195,0 +198 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "step2_closeout_digest_markdown",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -347,0 +351,2 @@ KNOWN_ARTIFACT_KEYS_BY_FILENAME: dict[str, str] = { +    "step2_closeout_digest.json": "step2_closeout_digest",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -347,0 +351,2 @@ KNOWN_ARTIFACT_KEYS_BY_FILENAME: dict[str, str] = { +    "step2_closeout_digest.md": "step2_closeout_digest_markdown",

### `b5663fb6597cb50696f5c0593a398e24901890fa`
- 时间: 2026-04-11 14:38:36 +0800
- 标题: chore: sync 2026-04-11 14:38:33
- 涉及文件: `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`, `src/gas_calibrator/v2/core/wp6_builder.py`, `tools/absorbance_debugger/analysis/source_policy.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: STEP, Step, cali, co2, mode, point, protocol, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -123,0 +124,2 @@ COMPARISON_ROLLUP_MARKDOWN_FILENAME = "comparison_rollup.md" +STEP2_CLOSEOUT_DIGEST_FILENAME = "step2_closeout_digest.json"
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -123,0 +124,2 @@ COMPARISON_ROLLUP_MARKDOWN_FILENAME = "comparison_rollup.md" +STEP2_CLOSEOUT_DIGEST_MARKDOWN_FILENAME = "step2_closeout_digest.md"
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -140,0 +143 @@ RECOGNITION_READINESS_SUMMARY_FILENAMES = ( +    STEP2_CLOSEOUT_DIGEST_FILENAME,
  - src/gas_calibrator/v2/core/wp6_builder.py @@ -938,0 +939,118 @@ def build_wp6_artifacts( +
  - src/gas_calibrator/v2/core/wp6_builder.py @@ -938,0 +939,118 @@ def build_wp6_artifacts( +# ---------------------------------------------------------------------------

### `0058d7296ba551262079659fc0db67270c3d88a5`
- 时间: 2026-04-11 14:28:35 +0800
- 标题: chore: sync 2026-04-11 14:28:34
- 涉及文件: `src/gas_calibrator/v2/scripts/historical_artifacts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, db, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/scripts/historical_artifacts.py @@ -459,0 +460 @@ def _build_run_report( +        "external_comparison_importer": external_comparison_importer,

### `4ddfdc4de1285a6c959ab481697ef7309e98f862`
- 时间: 2026-04-11 14:23:34 +0800
- 标题: chore: sync 2026-04-11 14:23:33
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, db
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1950,0 +1951 @@ class AppFacade: +            external_comparison_importer=external_comparison_importer,
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1951,0 +1953 @@ class AppFacade: +            scope_comparison_view=scope_comparison_view,
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -2772,0 +2775 @@ class AppFacade: +            external_comparison_importer=external_comparison_importer,
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -2773,0 +2777 @@ class AppFacade: +            scope_comparison_view=scope_comparison_view,
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -487,0 +488 @@ class DeviceWorkbenchController: +        external_comparison_importer = dict(payload.get("external_comparison_importer") or {})

### `123acf41c6eff3495c5d60f4ac4fb45c16173548`
- 时间: 2026-04-11 14:18:34 +0800
- 标题: chore: sync 2026-04-11 14:18:33
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, db
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -2722,0 +2723 @@ class AppFacade: +        external_comparison_importer: dict[str, Any],
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -2723,0 +2725 @@ class AppFacade: +        scope_comparison_view: dict[str, Any],
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3273,0 +3276 @@ class AppFacade: +        external_comparison_importer: dict[str, Any],
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3274,0 +3278 @@ class AppFacade: +        scope_comparison_view: dict[str, Any],
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3538,0 +3543,4 @@ class AppFacade: +            (

### `64377924ba4d93009aecfa41bb9acb67873e29a0`
- 时间: 2026-04-11 14:13:36 +0800
- 标题: chore: sync 2026-04-11 14:13:36
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1526,0 +1527 @@ class AppFacade: +        external_comparison_importer = dict(payload.get("external_comparison_importer", {}) or {})

### `7e4371e39c86f70b3f6f2bcc887ec9433899d77d`
- 时间: 2026-04-11 14:08:36 +0800
- 标题: chore: sync 2026-04-11 14:08:35
- 涉及文件: `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`, `tools/absorbance_debugger/analysis/comparison.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -352,0 +353,6 @@ _RECOGNITION_NEXT_ARTIFACT_DEFAULTS: dict[str, list[str]] = { +    "pt_ilc_registry": ["external_comparison_importer", "comparison_evidence_pack"],
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -352,0 +353,6 @@ _RECOGNITION_NEXT_ARTIFACT_DEFAULTS: dict[str, list[str]] = { +    "external_comparison_importer": ["comparison_evidence_pack", "scope_comparison_view"],
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -352,0 +353,6 @@ _RECOGNITION_NEXT_ARTIFACT_DEFAULTS: dict[str, list[str]] = { +    "comparison_evidence_pack": ["scope_comparison_view", "comparison_digest"],
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -352,0 +353,6 @@ _RECOGNITION_NEXT_ARTIFACT_DEFAULTS: dict[str, list[str]] = { +    "scope_comparison_view": ["comparison_digest", "comparison_rollup"],
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -352,0 +353,6 @@ _RECOGNITION_NEXT_ARTIFACT_DEFAULTS: dict[str, list[str]] = { +    "comparison_digest": ["comparison_rollup", "audit_readiness_digest"],

### `cbbb146891ab2071f78d78dba0ae7924e49df8c0`
- 时间: 2026-04-11 14:03:39 +0800
- 标题: chore: sync 2026-04-11 14:03:39
- 涉及文件: `src/gas_calibrator/v2/core/artifact_catalog.py`, `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`, `tools/absorbance_debugger/analysis/comparison.py`, `tools/absorbance_debugger/analysis/pipeline.py`, `tools/absorbance_debugger/plots/charts.py`, `tools/absorbance_debugger/reports/renderers.py`, `tools/absorbance_debugger/tests/test_absorbance_debugger.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Point, Report, cali, co2, db, mode, point, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -189,0 +190,6 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "pt_ilc_registry_markdown",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -189,0 +190,6 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "external_comparison_importer_markdown",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -189,0 +190,6 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "comparison_evidence_pack_markdown",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -189,0 +190,6 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "scope_comparison_view_markdown",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -189,0 +190,6 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "comparison_digest_markdown",

### `37867ad2a09878f65085062e1bde58f45a947cdc`
- 时间: 2026-04-11 13:58:33 +0800
- 标题: chore: sync 2026-04-11 13:58:31
- 涉及文件: `src/gas_calibrator/v2/core/artifact_catalog.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -125,0 +126,12 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "pt_ilc_registry",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -125,0 +126,12 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "external_comparison_importer",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -125,0 +126,12 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "comparison_evidence_pack",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -125,0 +126,12 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "scope_comparison_view",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -125,0 +126,12 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "comparison_digest",

### `4734041828c0eb60ceed8a39fca57f3ec0a01a32`
- 时间: 2026-04-11 01:38:32 +0800
- 标题: chore: sync 2026-04-11 01:38:32
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1948,0 +1949,4 @@ class AppFacade: +            pt_ilc_registry=pt_ilc_registry,
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1948,0 +1949,4 @@ class AppFacade: +            comparison_evidence_pack=comparison_evidence_pack,
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1948,0 +1949,4 @@ class AppFacade: +            comparison_digest=comparison_digest,
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1948,0 +1949,4 @@ class AppFacade: +            comparison_rollup=comparison_rollup,
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -2716,0 +2721,4 @@ class AppFacade: +        pt_ilc_registry: dict[str, Any],

### `b5064d5508b0b4f531369f11c6380a1478f8a5e0`
- 时间: 2026-04-11 01:33:37 +0800
- 标题: chore: sync 2026-04-11 01:33:35
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, db
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -2760,0 +2761,4 @@ class AppFacade: +            pt_ilc_registry=pt_ilc_registry,
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -2760,0 +2761,4 @@ class AppFacade: +            comparison_evidence_pack=comparison_evidence_pack,
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -2760,0 +2761,4 @@ class AppFacade: +            comparison_digest=comparison_digest,
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -2760,0 +2761,4 @@ class AppFacade: +            comparison_rollup=comparison_rollup,
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3259,0 +3264,4 @@ class AppFacade: +        pt_ilc_registry: dict[str, Any],

### `522cfb62bc7aa785c4dc61d9fd16dd7e2b77c7f6`
- 时间: 2026-04-11 01:13:32 +0800
- 标题: chore: sync 2026-04-11 01:13:30
- 涉及文件: `tests/v2/test_pt_ilc_wp6_contracts.py`
- 判定原因: diff 内容命中关键词: cali
- 关键 diff hunk 摘要:
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -421,3 +420,0 @@ class TestWp6OfflineArtifactsIntegration: -        from gas_calibrator.v2.core.offline_artifacts import build_offline_governance_artifacts

### `a710efbb527d8507eaab6c2ee8541058deb1645f`
- 时间: 2026-04-11 01:08:36 +0800
- 标题: chore: sync 2026-04-11 01:08:36
- 涉及文件: `tests/v2/test_pt_ilc_wp6_contracts.py`
- 判定原因: diff 内容命中关键词: REPORT, Step, cali, db, point, report, step
- 关键 diff hunk 摘要:
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -351,0 +352,182 @@ class TestWp6ResultsGatewayVisibility: +        from gas_calibrator.v2.core.artifact_catalog import KNOWN_ARTIFACT_KEYS_BY_FILENAME
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -351,0 +352,182 @@ class TestWp6ResultsGatewayVisibility: +    def test_wp6_artifacts_in_known_report_artifacts(self) -> None:
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -351,0 +352,182 @@ class TestWp6ResultsGatewayVisibility: +        from gas_calibrator.v2.core.artifact_catalog import KNOWN_REPORT_ARTIFACTS
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -351,0 +352,182 @@ class TestWp6ResultsGatewayVisibility: +            assert fn in KNOWN_REPORT_ARTIFACTS, f"KNOWN_REPORT_ARTIFACTS missing WP6 filename: {fn}"
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -351,0 +352,182 @@ class TestWp6ResultsGatewayVisibility: +        from gas_calibrator.v2.core.artifact_compatibility import CANONICAL_SURFACE_FILENAMES

### `ce8cf785dd841e8edc473c4f4bb02e30d434400b`
- 时间: 2026-04-11 01:03:32 +0800
- 标题: chore: sync 2026-04-11 01:03:32
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -486,0 +487,5 @@ class DeviceWorkbenchController: +        pt_ilc_registry = dict(payload.get("pt_ilc_registry") or {})
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -486,0 +487,5 @@ class DeviceWorkbenchController: +        comparison_evidence_pack = dict(payload.get("comparison_evidence_pack") or {})
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -486,0 +487,5 @@ class DeviceWorkbenchController: +        scope_comparison_view = dict(payload.get("scope_comparison_view") or {})
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -486,0 +487,5 @@ class DeviceWorkbenchController: +        comparison_digest_payload = dict(payload.get("comparison_digest") or {})
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -486,0 +487,5 @@ class DeviceWorkbenchController: +        comparison_rollup = dict(payload.get("comparison_rollup") or {})

### `7f21a8ced2d9d0ec5d08ab3fc43409ad7ea60466`
- 时间: 2026-04-11 00:58:31 +0800
- 标题: chore: sync 2026-04-11 00:58:30
- 涉及文件: `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, db, protocol, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -876,0 +877,30 @@ def build_recognition_readiness_artifacts( +    wp6_artifacts = build_wp6_artifacts(
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -876,0 +877,30 @@ def build_recognition_readiness_artifacts( +        run_id=run_id,
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -876,0 +877,30 @@ def build_recognition_readiness_artifacts( +        scope_definition_pack=scope_definition_pack,
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -876,0 +877,30 @@ def build_recognition_readiness_artifacts( +        decision_rule_profile=decision_rule_profile,
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -876,0 +877,30 @@ def build_recognition_readiness_artifacts( +        reference_asset_registry=reference_asset_registry,

### `0ce7c8d49f5e2f385fb9fc8eaec627a3fd7041a4`
- 时间: 2026-04-11 00:53:33 +0800
- 标题: chore: sync 2026-04-11 00:53:32
- 涉及文件: `src/gas_calibrator/v2/core/offline_artifacts.py`, `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, db
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/offline_artifacts.py @@ -1775,0 +1776,6 @@ def export_run_offline_artifacts( +        "pt_ilc_registry": "execution_summary",
  - src/gas_calibrator/v2/core/offline_artifacts.py @@ -1775,0 +1776,6 @@ def export_run_offline_artifacts( +        "external_comparison_importer": "execution_summary",
  - src/gas_calibrator/v2/core/offline_artifacts.py @@ -1775,0 +1776,6 @@ def export_run_offline_artifacts( +        "comparison_evidence_pack": "diagnostic_analysis",
  - src/gas_calibrator/v2/core/offline_artifacts.py @@ -1775,0 +1776,6 @@ def export_run_offline_artifacts( +        "scope_comparison_view": "diagnostic_analysis",
  - src/gas_calibrator/v2/core/offline_artifacts.py @@ -1775,0 +1776,6 @@ def export_run_offline_artifacts( +        "comparison_digest": "diagnostic_analysis",

### `9e062c9dc1aa95e0326409c55800dc3abcbd85b5`
- 时间: 2026-04-11 00:48:32 +0800
- 标题: chore: sync 2026-04-11 00:48:32
- 涉及文件: `src/gas_calibrator/v2/core/artifact_catalog.py`, `src/gas_calibrator/v2/core/artifact_compatibility.py`, `src/gas_calibrator/v2/core/offline_artifacts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: REPORT, cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -477,0 +478,12 @@ KNOWN_REPORT_ARTIFACTS = [ +    "pt_ilc_registry.json",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -477,0 +478,12 @@ KNOWN_REPORT_ARTIFACTS = [ +    "pt_ilc_registry.md",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -477,0 +478,12 @@ KNOWN_REPORT_ARTIFACTS = [ +    "external_comparison_importer.json",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -477,0 +478,12 @@ KNOWN_REPORT_ARTIFACTS = [ +    "external_comparison_importer.md",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -477,0 +478,12 @@ KNOWN_REPORT_ARTIFACTS = [ +    "comparison_evidence_pack.json",

### `06b37385b4caba22c1524104859770afa477b7bf`
- 时间: 2026-04-11 00:43:31 +0800
- 标题: chore: sync 2026-04-11 00:43:31
- 涉及文件: `src/gas_calibrator/v2/core/artifact_catalog.py`, `src/gas_calibrator/v2/scripts/historical_artifacts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -317,0 +318,12 @@ KNOWN_ARTIFACT_KEYS_BY_FILENAME: dict[str, str] = { +    "pt_ilc_registry.json": "pt_ilc_registry",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -317,0 +318,12 @@ KNOWN_ARTIFACT_KEYS_BY_FILENAME: dict[str, str] = { +    "pt_ilc_registry.md": "pt_ilc_registry_markdown",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -317,0 +318,12 @@ KNOWN_ARTIFACT_KEYS_BY_FILENAME: dict[str, str] = { +    "external_comparison_importer.json": "external_comparison_importer",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -317,0 +318,12 @@ KNOWN_ARTIFACT_KEYS_BY_FILENAME: dict[str, str] = { +    "external_comparison_importer.md": "external_comparison_importer_markdown",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -317,0 +318,12 @@ KNOWN_ARTIFACT_KEYS_BY_FILENAME: dict[str, str] = { +    "comparison_evidence_pack.json": "comparison_evidence_pack",

### `90944b258f03c9dae5f27711ae6144b33d039a3b`
- 时间: 2026-04-10 23:48:54 +0800
- 标题: chore: sync 2026-04-10 23:48:52
- 涉及文件: `tools/absorbance_debugger/analysis/ppm_family_challenge.py`
- 判定原因: diff 内容命中关键词: point
- 关键 diff hunk 摘要:
  - 未提取到关键词 hunk，建议结合 raw git log 复核。

### `91a2cb3c150ae1154f085b1816250fbed139fa66`
- 时间: 2026-04-10 23:39:39 +0800
- 标题: chore: sync 2026-04-10 23:39:37
- 涉及文件: `tools/absorbance_debugger/analysis/absorbance_models.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: mode
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/analysis/absorbance_models.py @@ -225,0 +226,2 @@ def _design_matrix( +        elif term == "A*K":
  - tools/absorbance_debugger/analysis/absorbance_models.py @@ -225,0 +226,2 @@ def _design_matrix( +            columns.append(absorbance * _feature_column(frame, "K_feature"))

### `82ae5a46c1f46f90a5e196688bd3c7c0af836a83`
- 时间: 2026-04-10 23:34:47 +0800
- 标题: chore: sync 2026-04-10 23:34:39
- 涉及文件: `tools/absorbance_debugger/analysis/legacy_water_replay.py`, `tools/absorbance_debugger/analysis/ppm_family_challenge.py`
- 判定原因: diff 内容命中关键词: mode, zero
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/analysis/ppm_family_challenge.py @@ -778,0 +779,3 @@ def run_fixed_chain_ppm_family_challenge( +                            "fixed_best_model": str(fixed_row.get("best_absorbance_model") or ""),
  - tools/absorbance_debugger/analysis/ppm_family_challenge.py @@ -778,0 +779,3 @@ def run_fixed_chain_ppm_family_challenge( +                            "fixed_model_family": str(fixed_row.get("best_model_family") or ""),
  - tools/absorbance_debugger/analysis/ppm_family_challenge.py @@ -778,0 +779,3 @@ def run_fixed_chain_ppm_family_challenge( +                            "fixed_zero_residual_mode": str(fixed_row.get("zero_residual_mode") or "none"),

### `ed5ae499919e3e5b944722452de94c9bdc6e39b4`
- 时间: 2026-04-10 23:27:07 +0800
- 标题: chore: sync 2026-04-10 23:27:06
- 涉及文件: `tools/absorbance_debugger/analysis/pipeline.py`, `tools/absorbance_debugger/analysis/ppm_family_challenge.py`, `tools/absorbance_debugger/tests/test_absorbance_debugger.py`
- 判定原因: diff 内容命中关键词: co2, h2o, mode, point, step, zero
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/analysis/pipeline.py @@ -2136,0 +2137,12 @@ def execute_pipeline(config: DebuggerConfig) -> dict[str, Any]: +        fixed_selection=model_results["selection"],
  - tools/absorbance_debugger/analysis/pipeline.py @@ -2136,0 +2137,12 @@ def execute_pipeline(config: DebuggerConfig) -> dict[str, Any]: +    _frame_to_csv(config.output_dir / "step_06y_ppm_family_challenge_detail.csv", ppm_family_challenge["detail"])
  - tools/absorbance_debugger/analysis/pipeline.py @@ -2136,0 +2137,12 @@ def execute_pipeline(config: DebuggerConfig) -> dict[str, Any]: +    _frame_to_csv(config.output_dir / "step_06y_ppm_family_challenge_summary.csv", ppm_family_challenge["summary"])
  - tools/absorbance_debugger/analysis/pipeline.py @@ -2136,0 +2137,12 @@ def execute_pipeline(config: DebuggerConfig) -> dict[str, Any]: +    plot_ppm_family_challenge(ppm_family_challenge["detail"], config.output_dir / "step_06y_ppm_family_challenge_plot.png")
  - tools/absorbance_debugger/analysis/pipeline.py @@ -2136,0 +2137,12 @@ def execute_pipeline(config: DebuggerConfig) -> dict[str, Any]: +    _frame_to_csv(config.output_dir / "step_08y_ppm_family_challenge_conclusions.csv", ppm_family_challenge["conclusions"])

### `ef2322d6c1ec242e7f4d9e89cfb114e670cdfac1`
- 时间: 2026-04-10 23:19:07 +0800
- 标题: chore: sync 2026-04-10 23:19:05
- 涉及文件: `tools/absorbance_debugger/analysis/legacy_water_replay.py`, `tools/absorbance_debugger/analysis/pipeline.py`, `tools/absorbance_debugger/analysis/ppm_family_challenge.py`, `tools/absorbance_debugger/plots/charts.py`
- 判定原因: diff 内容命中关键词: Zero, co2, h2o, mode, point, report, zero
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/analysis/legacy_water_replay.py @@ -893,0 +894,3 @@ def _detail_rows(compare: pd.DataFrame, fixed_selection: pd.DataFrame) -> pd.Dat +            "mode2_semantic_profile": str(subset.get("mode2_semantic_profile", pd.Series(["mode2_semantics_unknown"])).fillna("mode2_semantics_unknown").iloc[0]),
  - tools/absorbance_debugger/analysis/legacy_water_replay.py @@ -893,0 +894,3 @@ def _detail_rows(compare: pd.DataFrame, fixed_selection: pd.DataFrame) -> pd.Dat +            "mode2_legacy_raw_compare_safe": bool(subset.get("mode2_legacy_raw_compare_safe", pd.Series([False])).fillna(False).iloc[0]),
  - tools/absorbance_debugger/analysis/legacy_water_replay.py @@ -893,0 +894,3 @@ def _detail_rows(compare: pd.DataFrame, fixed_selection: pd.DataFrame) -> pd.Dat +            "mode2_is_baseline_bearing_profile": bool(subset.get("mode2_is_baseline_bearing_profile", pd.Series([False])).fillna(False).iloc[0]),
  - tools/absorbance_debugger/analysis/legacy_water_replay.py @@ -1020,0 +1035,3 @@ def _stage_rows(compare: pd.DataFrame, detail_df: pd.DataFrame) -> pd.DataFrame: +                    "mode2_semantic_profile": detail.get("mode2_semantic_profile"),
  - tools/absorbance_debugger/analysis/legacy_water_replay.py @@ -1020,0 +1035,3 @@ def _stage_rows(compare: pd.DataFrame, detail_df: pd.DataFrame) -> pd.DataFrame: +                    "mode2_legacy_raw_compare_safe": detail.get("mode2_legacy_raw_compare_safe"),

### `9422b66dc4e14ef8100063746ca13aaee01f72f5`
- 时间: 2026-04-10 23:13:37 +0800
- 标题: chore: sync 2026-04-10 23:13:36
- 涉及文件: `tools/absorbance_debugger/analysis/absorbance_models.py`, `tools/absorbance_debugger/analysis/diagnostics.py`, `tools/absorbance_debugger/analysis/ppm_family_challenge.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: MODE, Mode, h2o, mode, point, zero
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/analysis/absorbance_models.py @@ -163,0 +164,30 @@ def _resolve_piecewise_breakpoint_absorbance( +def _term_required_columns(term: str, absorbance_column: str) -> tuple[str, ...]:
  - tools/absorbance_debugger/analysis/absorbance_models.py @@ -163,0 +164,30 @@ def _resolve_piecewise_breakpoint_absorbance( +    if term == "intercept":
  - tools/absorbance_debugger/analysis/absorbance_models.py @@ -163,0 +164,30 @@ def _resolve_piecewise_breakpoint_absorbance( +        return ()
  - tools/absorbance_debugger/analysis/absorbance_models.py @@ -163,0 +164,30 @@ def _resolve_piecewise_breakpoint_absorbance( +    if term in {"A", "A^2", "A^3", "H(A-A_break)", "H(A-A_break)^2"}:
  - tools/absorbance_debugger/analysis/absorbance_models.py @@ -163,0 +164,30 @@ def _resolve_piecewise_breakpoint_absorbance( +        return (absorbance_column,)

### `18b9cda80ca6106437cd914fd9bec479024362c6`
- 时间: 2026-04-10 22:18:31 +0800
- 标题: chore: sync 2026-04-10 22:18:31
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -598,0 +599,6 @@ class ResultsGateway: +        pt_ilc_registry = dict(payload.get("pt_ilc_registry", {}) or {})
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -598,0 +599,6 @@ class ResultsGateway: +        external_comparison_importer = dict(payload.get("external_comparison_importer", {}) or {})
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -598,0 +599,6 @@ class ResultsGateway: +        comparison_evidence_pack = dict(payload.get("comparison_evidence_pack", {}) or {})
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -598,0 +599,6 @@ class ResultsGateway: +        scope_comparison_view = dict(payload.get("scope_comparison_view", {}) or {})
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -598,0 +599,6 @@ class ResultsGateway: +        comparison_digest = dict(payload.get("comparison_digest", {}) or {})

### `9e68110e9b0aeb3b0e3cb77e55186706f9801b39`
- 时间: 2026-04-10 22:13:32 +0800
- 标题: chore: sync 2026-04-10 22:13:30
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `tests/v2/test_pt_ilc_wp6_contracts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: DB, MODE, Mode, Step, cali, db, mode, protocol
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -371 +371 @@ class ResultsGateway: -            workbench_action_snapshot=workbench_action_snapshot if isinstance(workbench_action_snapshot, dict) else None
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -371 +371 @@ class ResultsGateway: +            workbench_action_snapshot=workbench_action_snapshot if isinstance(workbench_action_snapshot, dict) else None,
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -0,0 +1,351 @@ +Step 2 only — reviewer-facing / readiness-mapping-only.
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -0,0 +1,351 @@ +from gas_calibrator.v2.adapters.wp6_gateway import Wp6Gateway
  - tests/v2/test_pt_ilc_wp6_contracts.py @@ -0,0 +1,351 @@ +from gas_calibrator.v2.core.wp6_builder import (

### `63e0a7f67409c7c9cdc7e861119e128423d9ef61`
- 时间: 2026-04-10 22:08:32 +0800
- 标题: chore: sync 2026-04-10 22:08:30
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -80,0 +81 @@ from .uncertainty_gateway import UncertaintyGateway +from .wp6_gateway import Wp6Gateway
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -363,0 +365,16 @@ class ResultsGateway: +        wp6_payload = Wp6Gateway(
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -363,0 +365,16 @@ class ResultsGateway: +            self.run_dir,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -363,0 +365,16 @@ class ResultsGateway: +            summary=summary if isinstance(summary, dict) else None,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -363,0 +365,16 @@ class ResultsGateway: +            analytics_summary=analytics_summary if isinstance(analytics_summary, dict) else None,

### `6e155e64bc6d3bd1ac313f2e52bbdf174660d874`
- 时间: 2026-04-10 22:03:33 +0800
- 标题: chore: sync 2026-04-10 22:03:33
- 涉及文件: `src/gas_calibrator/v2/adapters/wp6_gateway.py`, `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`, `src/gas_calibrator/v2/core/wp6_repository.py`, `tools/absorbance_debugger/analysis/legacy_water_replay.py`, `tools/absorbance_debugger/tests/test_absorbance_debugger.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: DB, MODE, PROTOCOL, Protocol, REPORT, Step, cali, co2
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/wp6_gateway.py @@ -0,0 +1,38 @@ +"""WP6 gateway: read-only gateway for Step 2 PT/ILC / comparison reviewer payloads."""
  - src/gas_calibrator/v2/adapters/wp6_gateway.py @@ -0,0 +1,38 @@ +from __future__ import annotations
  - src/gas_calibrator/v2/adapters/wp6_gateway.py @@ -0,0 +1,38 @@ +
  - src/gas_calibrator/v2/adapters/wp6_gateway.py @@ -0,0 +1,38 @@ +from pathlib import Path
  - src/gas_calibrator/v2/adapters/wp6_gateway.py @@ -0,0 +1,38 @@ +from typing import Any

### `65e1a3aa9040d54a473d53bfc5b82b20ee2f0326`
- 时间: 2026-04-10 21:58:33 +0800
- 标题: chore: sync 2026-04-10 21:58:30
- 涉及文件: `src/gas_calibrator/v2/core/wp6_builder.py`, `tools/absorbance_debugger/analysis/legacy_water_replay.py`, `tools/absorbance_debugger/tests/test_absorbance_debugger.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: MODE, Step, cali, co2, db, h2o, mode, point
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/wp6_builder.py @@ -0,0 +1,938 @@ +"""WP6 builder: PT/ILC importer + comparison evidence pack + reviewer navigation.
  - src/gas_calibrator/v2/core/wp6_builder.py @@ -0,0 +1,938 @@ +
  - src/gas_calibrator/v2/core/wp6_builder.py @@ -0,0 +1,938 @@ +Step 2 only — reviewer-facing / readiness-mapping-only.
  - src/gas_calibrator/v2/core/wp6_builder.py @@ -0,0 +1,938 @@ +No real external comparison, no formal compliance claim, no accreditation claim.
  - src/gas_calibrator/v2/core/wp6_builder.py @@ -0,0 +1,938 @@ +"""

### `cf091627b307093f50d7d2e5bc088fda43b8b3a9`
- 时间: 2026-04-10 21:53:33 +0800
- 标题: chore: sync 2026-04-10 21:53:31
- 涉及文件: `tools/absorbance_debugger/analysis/legacy_water_replay.py`, `tools/absorbance_debugger/analysis/pipeline.py`
- 判定原因: diff 内容命中关键词: co2, point, zero
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/analysis/legacy_water_replay.py @@ -352 +352,4 @@ def build_legacy_water_replay_features( -            & pd.to_numeric(anchor_base["target_co2_ppm"], errors="coerce").sub(0.0).abs().le(float(rules.co2_zero_ppm_tolerance))
  - tools/absorbance_debugger/analysis/legacy_water_replay.py @@ -352 +352,4 @@ def build_legacy_water_replay_features( +            & pd.to_numeric(anchor_base["target_co2_ppm"], errors="coerce")
  - tools/absorbance_debugger/analysis/legacy_water_replay.py @@ -352 +352,4 @@ def build_legacy_water_replay_features( +            .sub(float(rules.co2_zero_ppm_target))
  - tools/absorbance_debugger/analysis/legacy_water_replay.py @@ -352 +352,4 @@ def build_legacy_water_replay_features( +            .le(float(rules.co2_zero_ppm_tolerance))
  - tools/absorbance_debugger/analysis/legacy_water_replay.py @@ -1191,3 +1195 @@ def run_legacy_water_replay_diagnostic( -    point_raw = build_point_raw_summary(

### `b9975e573227be7dcc14a064d779ebcd3e572ead`
- 时间: 2026-04-10 21:48:36 +0800
- 标题: chore: sync 2026-04-10 21:48:36
- 涉及文件: `tools/absorbance_debugger/analysis/legacy_water_replay.py`, `tools/absorbance_debugger/analysis/pipeline.py`, `tools/absorbance_debugger/plots/charts.py`, `tools/absorbance_debugger/reports/renderers.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: MODE, Zero, cali, co2, coefficient, db, h2o, mode
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/analysis/legacy_water_replay.py @@ -0,0 +1,1216 @@ +from .absorbance_models import _fit_one_candidate, active_model_specs
  - tools/absorbance_debugger/analysis/legacy_water_replay.py @@ -0,0 +1,1216 @@ +from .diagnostics import build_point_raw_summary
  - tools/absorbance_debugger/analysis/legacy_water_replay.py @@ -0,0 +1,1216 @@ +WATER_LINEAGE_MODES: tuple[str, ...] = (
  - tools/absorbance_debugger/analysis/legacy_water_replay.py @@ -0,0 +1,1216 @@ +    "simplified_subzero_anchor",
  - tools/absorbance_debugger/analysis/legacy_water_replay.py @@ -0,0 +1,1216 @@ +    "legacy_h2o_summary_selection",

### `ad131e24aa10ca01893bf5b8cac692a03ab42efb`
- 时间: 2026-04-10 21:43:39 +0800
- 标题: chore: sync 2026-04-10 21:43:36
- 涉及文件: `tools/absorbance_debugger/parsers/schema.py`
- 判定原因: diff 内容命中关键词: MODE, mode
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/parsers/schema.py @@ -143,0 +144,85 @@ def analyzer_slot_from_label(label: str) -> int: +def parse_mode2_payload(raw_message: Any) -> list[str]:
  - tools/absorbance_debugger/parsers/schema.py @@ -143,0 +144,85 @@ def analyzer_slot_from_label(label: str) -> int: +    """Split one MODE2 raw message into cleaned tokens."""
  - tools/absorbance_debugger/parsers/schema.py @@ -143,0 +144,85 @@ def analyzer_slot_from_label(label: str) -> int: +def classify_mode2_semantics(
  - tools/absorbance_debugger/parsers/schema.py @@ -143,0 +144,85 @@ def analyzer_slot_from_label(label: str) -> int: +    mode2_field_count: Any,
  - tools/absorbance_debugger/parsers/schema.py @@ -143,0 +144,85 @@ def analyzer_slot_from_label(label: str) -> int: +    """Classify whether one MODE2 frame should be treated as legacy or baseline-bearing."""

### `cc45c60fb86f5170af6e159f4a33c22a802b02d1`
- 时间: 2026-04-10 21:18:36 +0800
- 标题: chore: sync 2026-04-10 21:18:35
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`, `src/gas_calibrator/v2/ui_v2/locales/zh_CN.json`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -656 +656 @@ class DeviceWorkbenchController: -                default="软件验证边界：仅供审阅 / 仅限仿真 / 不是真实验收证据 / 非 formal claim",
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -656 +656 @@ class DeviceWorkbenchController: +                default="软件验证边界：仅供审阅 / 仅限仿真 / 不是真实验收证据 / 非 claim",
  - src/gas_calibrator/v2/ui_v2/locales/zh_CN.json @@ -1612,0 +1613 @@ +        "software_validation_boundary": "软件验证边界：仅供审阅 / 仅限仿真 / 不是真实验收证据 / 非 claim",

### `15623fc83274fb459a77683b67ffdf5587a68a89`
- 时间: 2026-04-10 21:08:41 +0800
- 标题: chore: sync 2026-04-10 21:08:37
- 涉及文件: `src/gas_calibrator/v2/core/software_validation_builder.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Step, cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/software_validation_builder.py @@ -1216 +1216 @@ def build_software_validation_wp5_artifacts( -        summary_text="Software validation and audit readiness remain reviewer-only in Step 2.",
  - src/gas_calibrator/v2/core/software_validation_builder.py @@ -1216 +1216 @@ def build_software_validation_wp5_artifacts( +        summary_text="software validation / audit readiness remain reviewer-only in Step 2.",
  - src/gas_calibrator/v2/core/software_validation_builder.py @@ -1248 +1248 @@ def build_software_validation_wp5_artifacts( -            summary="Software validation and audit readiness remain reviewer-only in Step 2.",
  - src/gas_calibrator/v2/core/software_validation_builder.py @@ -1248 +1248 @@ def build_software_validation_wp5_artifacts( +            summary="software validation / audit readiness remain reviewer-only in Step 2.",

### `3f2fcebab5dcf7d915985adc9cddd1f411f262af`
- 时间: 2026-04-10 20:58:35 +0800
- 标题: chore: sync 2026-04-10 20:58:33
- 涉及文件: `src/gas_calibrator/v2/core/software_validation_builder.py`, `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`, `src/gas_calibrator/v2/ui_v2/locales/en_US.json`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Step, cali, db
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/software_validation_builder.py @@ -360,0 +361,8 @@ def build_software_validation_wp5_artifacts( +    release_manifest_reviewer_note = (
  - src/gas_calibrator/v2/core/software_validation_builder.py @@ -360,0 +361,8 @@ def build_software_validation_wp5_artifacts( +        "Release manifest stays reviewer-facing and Step 2 only. It summarizes release scope, linked validation, "
  - src/gas_calibrator/v2/core/software_validation_builder.py @@ -360,0 +361,8 @@ def build_software_validation_wp5_artifacts( +        "and current boundaries, but it is not formal release approval, not real acceptance evidence, and not a formal compliance claim."
  - src/gas_calibrator/v2/core/software_validation_builder.py @@ -360,0 +361,8 @@ def build_software_validation_wp5_artifacts( +    )
  - src/gas_calibrator/v2/core/software_validation_builder.py @@ -360,0 +361,8 @@ def build_software_validation_wp5_artifacts( +    audit_readiness_reviewer_note = (

### `1fb7e18ba1f92c5d628215123a3fc5b33ffdc3bb`
- 时间: 2026-04-10 20:48:38 +0800
- 标题: chore: sync 2026-04-10 20:48:35
- 涉及文件: `tests/v2/test_build_offline_governance_artifacts.py`
- 判定原因: diff 内容命中关键词: protocol, step, store
- 关键 diff hunk 摘要:
  - tests/v2/test_build_offline_governance_artifacts.py @@ -1285,0 +1286,14 @@ def test_rebuild_run_generates_recognition_readiness_artifacts(tmp_path: Path) - +    assert audit_event_store["artifact_type"] == "audit_event_store"
  - tests/v2/test_build_offline_governance_artifacts.py @@ -1287,5 +1301,43 @@ def test_rebuild_run_generates_recognition_readiness_artifacts(tmp_path: Path) - +    assert software_matrix["method_confirmation_protocol_id"]
  - tests/v2/test_build_offline_governance_artifacts.py @@ -1287,5 +1301,43 @@ def test_rebuild_run_generates_recognition_readiness_artifacts(tmp_path: Path) - +    assert release_manifest["linked_method_confirmation_protocols"]
  - tests/v2/test_build_offline_governance_artifacts.py @@ -1298 +1350 @@ def test_rebuild_run_generates_recognition_readiness_artifacts(tmp_path: Path) - -    assert audit_digest["reviewer_next_step_digest"]
  - tests/v2/test_build_offline_governance_artifacts.py @@ -1298 +1350 @@ def test_rebuild_run_generates_recognition_readiness_artifacts(tmp_path: Path) - +    assert audit_digest["digest"]["reviewer_next_step_digest"]

### `b75b03abde78a0ac89f4041fda6f1bafead9d437`
- 时间: 2026-04-10 20:43:39 +0800
- 标题: chore: sync 2026-04-10 20:43:38
- 涉及文件: `src/gas_calibrator/v2/core/artifact_compatibility.py`, `src/gas_calibrator/v2/core/measurement_phase_coverage.py`, `src/gas_calibrator/v2/ui_v2/locales/en_US.json`, `src/gas_calibrator/v2/ui_v2/locales/zh_CN.json`, `tests/v2/test_build_offline_governance_artifacts.py`, `tests/v2/test_software_validation_wp5_contracts.py`, `tools/absorbance_debugger/analysis/water_zero_anchor.py`, `tools/absorbance_debugger/tests/test_absorbance_debugger.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: STORE, cali, co2, db, h2o, insert, mode, point
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -127,0 +128,30 @@ CANONICAL_SURFACE_FILENAMES = frozenset( +        recognition_readiness.SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_FILENAME,
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -127,0 +128,30 @@ CANONICAL_SURFACE_FILENAMES = frozenset( +        recognition_readiness.SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_MARKDOWN_FILENAME,
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -127,0 +128,30 @@ CANONICAL_SURFACE_FILENAMES = frozenset( +        recognition_readiness.REQUIREMENT_DESIGN_CODE_TEST_LINKS_FILENAME,
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -127,0 +128,30 @@ CANONICAL_SURFACE_FILENAMES = frozenset( +        recognition_readiness.REQUIREMENT_DESIGN_CODE_TEST_LINKS_MARKDOWN_FILENAME,
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -127,0 +128,30 @@ CANONICAL_SURFACE_FILENAMES = frozenset( +        recognition_readiness.VALIDATION_EVIDENCE_INDEX_FILENAME,

### `8391b0fc1ef0f676cf9bc6b11647c811431a1de3`
- 时间: 2026-04-10 20:38:36 +0800
- 标题: chore: sync 2026-04-10 20:38:35
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/artifact_catalog.py`, `src/gas_calibrator/v2/scripts/historical_artifacts.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`, `tools/absorbance_debugger/analysis/pipeline.py`, `tools/absorbance_debugger/cli.py`, `tools/absorbance_debugger/gui.py`, `tools/absorbance_debugger/plots/charts.py`, `tools/absorbance_debugger/reports/renderers.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: REPORT, STORE, Zero, cali, co2, coefficient, db, mode
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -555,0 +556,20 @@ class ResultsGateway: +        software_validation_traceability_matrix = dict(
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -555,0 +556,20 @@ class ResultsGateway: +            payload.get("software_validation_traceability_matrix", {}) or {}
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -555,0 +556,20 @@ class ResultsGateway: +        )
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -555,0 +556,20 @@ class ResultsGateway: +        requirement_design_code_test_links = dict(
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -555,0 +556,20 @@ class ResultsGateway: +            payload.get("requirement_design_code_test_links", {}) or {}

### `7c81aac9570c67f723e267f54f2641a4a453b9ca`
- 时间: 2026-04-10 20:33:36 +0800
- 标题: chore: sync 2026-04-10 20:33:34
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/adapters/software_validation_gateway.py`, `tools/absorbance_debugger/analysis/absorbance_models.py`, `tools/absorbance_debugger/analysis/pipeline.py`, `tools/absorbance_debugger/analysis/water_zero_anchor.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Step, cali, co2, mode, point, report, store, zero
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -78,0 +79 @@ from .recognition_scope_gateway import RecognitionScopeGateway +from .software_validation_gateway import SoftwareValidationGateway
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -324,0 +326,38 @@ class ResultsGateway: +        software_validation_payload = SoftwareValidationGateway(
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -324,0 +326,38 @@ class ResultsGateway: +            self.run_dir,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -324,0 +326,38 @@ class ResultsGateway: +            summary=summary if isinstance(summary, dict) else None,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -324,0 +326,38 @@ class ResultsGateway: +            analytics_summary=analytics_summary if isinstance(analytics_summary, dict) else None,

### `837d708001c4746f3ae84abb8e683e36f02ac9b3`
- 时间: 2026-04-10 20:28:34 +0800
- 标题: chore: sync 2026-04-10 20:28:33
- 涉及文件: `src/gas_calibrator/v2/core/offline_artifacts.py`, `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`, `src/gas_calibrator/v2/core/software_validation_repository.py`, `tools/absorbance_debugger/analysis/lineage_audit.py`, `tools/absorbance_debugger/analysis/pressure_assessment.py`, `tools/absorbance_debugger/analysis/water_zero_anchor.py`, `tools/absorbance_debugger/app.py`, `tools/absorbance_debugger/models/config.py`, `tools/absorbance_debugger/options.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, DB, H2O, H2o, MODE, Mode, Protocol, STORE
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/offline_artifacts.py @@ -1564,0 +1565,56 @@ def export_run_offline_artifacts( +        "requirement_design_code_test_links": str(
  - src/gas_calibrator/v2/core/offline_artifacts.py @@ -1564,0 +1565,56 @@ def export_run_offline_artifacts( +            run_dir / recognition_readiness.REQUIREMENT_DESIGN_CODE_TEST_LINKS_FILENAME
  - src/gas_calibrator/v2/core/offline_artifacts.py @@ -1564,0 +1565,56 @@ def export_run_offline_artifacts( +        ),
  - src/gas_calibrator/v2/core/offline_artifacts.py @@ -1564,0 +1565,56 @@ def export_run_offline_artifacts( +        "requirement_design_code_test_links_markdown": str(
  - src/gas_calibrator/v2/core/offline_artifacts.py @@ -1564,0 +1565,56 @@ def export_run_offline_artifacts( +            run_dir / recognition_readiness.REQUIREMENT_DESIGN_CODE_TEST_LINKS_MARKDOWN_FILENAME

### `b9ac40bd278253c16661f6c780ec3ef57064943d`
- 时间: 2026-04-10 20:23:36 +0800
- 标题: chore: sync 2026-04-10 20:23:33
- 涉及文件: `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`, `src/gas_calibrator/v2/core/software_validation_builder.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: DB, MODE, STORE, Step, Store, cali, db, mode
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -3,0 +4 @@ from datetime import datetime, timezone +from pathlib import Path
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -80,0 +82,26 @@ SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_MARKDOWN_FILENAME = "software_validation +REQUIREMENT_DESIGN_CODE_TEST_LINKS_FILENAME = "requirement_design_code_test_links.json"
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -80,0 +82,26 @@ SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_MARKDOWN_FILENAME = "software_validation +REQUIREMENT_DESIGN_CODE_TEST_LINKS_MARKDOWN_FILENAME = "requirement_design_code_test_links.md"
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -80,0 +82,26 @@ SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_MARKDOWN_FILENAME = "software_validation +VALIDATION_EVIDENCE_INDEX_FILENAME = "validation_evidence_index.json"
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -80,0 +82,26 @@ SOFTWARE_VALIDATION_TRACEABILITY_MATRIX_MARKDOWN_FILENAME = "software_validation +VALIDATION_EVIDENCE_INDEX_MARKDOWN_FILENAME = "validation_evidence_index.md"

### `41fc21312086821c419b7c61beefb6b5c1fc6fa1`
- 时间: 2026-04-10 19:53:40 +0800
- 标题: chore: sync 2026-04-10 19:53:32
- 涉及文件: `src/gas_calibrator/v2/core/software_validation_builder.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Step, cali, mode, protocol, report, step, v1
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/software_validation_builder.py @@ -0,0 +1,441 @@ +from __future__ import annotations
  - src/gas_calibrator/v2/core/software_validation_builder.py @@ -0,0 +1,441 @@ +
  - src/gas_calibrator/v2/core/software_validation_builder.py @@ -0,0 +1,441 @@ +import hashlib
  - src/gas_calibrator/v2/core/software_validation_builder.py @@ -0,0 +1,441 @@ +import json
  - src/gas_calibrator/v2/core/software_validation_builder.py @@ -0,0 +1,441 @@ +import platform

### `e90412414ffd6c825fdcf74e46319c3d1117be86`
- 时间: 2026-04-10 19:43:44 +0800
- 标题: chore: sync 2026-04-10 19:43:43
- 涉及文件: `tools/absorbance_debugger/analysis/pipeline.py`, `tools/absorbance_debugger/tests/test_absorbance_debugger.py`
- 判定原因: diff 内容命中关键词: point, report, step, zero
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/tests/test_absorbance_debugger.py @@ -272,3 +273,3 @@ def test_cross_run_batch_interface_can_run(monkeypatch, tmp_path: Path) -> None: -                {"analyzer_id": "GA01", "old_chain_rmse": 5.0, "new_chain_rmse": 7.0, "winner_overall": "old_chain", "winner_zero": "old_chain", "winner_temp_stability": "old_chai
  - tools/absorbance_debugger/tests/test_absorbance_debugger.py @@ -272,3 +273,3 @@ def test_cross_run_batch_interface_can_run(monkeypatch, tmp_path: Path) -> None: -                {"analyzer_id": "GA02", "old_chain_rmse": 6.0, "new_chain_rmse": 4.0 if stem == "run_a" else 5.0, "winner_overall": "new_chain", "winner_zero": "new_chain", "winne
  - tools/absorbance_debugger/tests/test_absorbance_debugger.py @@ -272,3 +273,3 @@ def test_cross_run_batch_interface_can_run(monkeypatch, tmp_path: Path) -> None: -                {"analyzer_id": "GA03", "old_chain_rmse": 5.5, "new_chain_rmse": 4.5, "winner_overall": "new_chain", "winner_zero": "new_chain", "winner_temp_stability": "new_chai
  - tools/absorbance_debugger/tests/test_absorbance_debugger.py @@ -272,3 +273,3 @@ def test_cross_run_batch_interface_can_run(monkeypatch, tmp_path: Path) -> None: +                {"analyzer_id": "GA01", "old_chain_rmse": 5.0, "new_chain_rmse": 7.0, "old_zero_rmse": 2.0, "new_zero_rmse": 3.0, "old_temp_stability_metric": 1.0, "new_temp_stabi
  - tools/absorbance_debugger/tests/test_absorbance_debugger.py @@ -272,3 +273,3 @@ def test_cross_run_batch_interface_can_run(monkeypatch, tmp_path: Path) -> None: +                {"analyzer_id": "GA02", "old_chain_rmse": 6.0, "new_chain_rmse": 4.0 if stem == "run_a" else 5.0, "old_zero_rmse": 2.5, "new_zero_rmse": 1.7, "old_temp_stability_m

### `0bf0e020ae7bcae71667be52625be6fafdf37bd5`
- 时间: 2026-04-10 19:38:35 +0800
- 标题: chore: sync 2026-04-10 19:38:35
- 涉及文件: `tools/absorbance_debugger/analysis/comparison.py`, `tools/absorbance_debugger/analysis/merged_zero_anchor.py`, `tools/absorbance_debugger/analysis/pipeline.py`, `tools/absorbance_debugger/app.py`, `tools/absorbance_debugger/gui.py`, `tools/absorbance_debugger/reports/renderers.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, mode, point, report, step, zero
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/analysis/comparison.py @@ -281,5 +285,5 @@ def build_comparison_outputs( -    zero_df = pd.DataFrame(zero_rows).sort_values(["analyzer_id", "temp_c"], ignore_index=True)
  - tools/absorbance_debugger/analysis/comparison.py @@ -281,5 +285,5 @@ def build_comparison_outputs( +    zero_df = _sorted_frame(zero_rows, ["analyzer_id", "temp_c"])
  - tools/absorbance_debugger/analysis/merged_zero_anchor.py @@ -16,0 +17,6 @@ from ..plots.charts import plot_merged_zero_anchor_compare +def _analyzer_slice(frame: pd.DataFrame, analyzer_id: str) -> pd.DataFrame:
  - tools/absorbance_debugger/analysis/merged_zero_anchor.py @@ -16,0 +17,6 @@ from ..plots.charts import plot_merged_zero_anchor_compare +    if frame.empty or "analyzer_id" not in frame.columns:
  - tools/absorbance_debugger/analysis/merged_zero_anchor.py @@ -16,0 +17,6 @@ from ..plots.charts import plot_merged_zero_anchor_compare +        return frame.iloc[0:0].copy()

### `54ceced1feae088319334469e301eb658ce53347`
- 时间: 2026-04-10 19:33:34 +0800
- 标题: chore: sync 2026-04-10 19:33:31
- 涉及文件: `tools/absorbance_debugger/analysis/comparison.py`, `tools/absorbance_debugger/analysis/cross_run.py`, `tools/absorbance_debugger/analysis/merged_zero_anchor.py`, `tools/absorbance_debugger/analysis/pipeline.py`, `tools/absorbance_debugger/analysis/run_assessment.py`, `tools/absorbance_debugger/plots/charts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, Zero, co2, mode, point, step, zero
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/analysis/comparison.py @@ -240,0 +241,4 @@ def build_comparison_outputs( +                "old_zero_rmse": old_zero_metrics["rmse"],
  - tools/absorbance_debugger/analysis/comparison.py @@ -240,0 +241,4 @@ def build_comparison_outputs( +                "new_zero_rmse": new_zero_metrics["rmse"],
  - tools/absorbance_debugger/analysis/cross_run.py @@ -23,0 +28,16 @@ def build_cross_run_summary( +        invalid_point_count = 0
  - tools/absorbance_debugger/analysis/cross_run.py @@ -23,0 +28,16 @@ def build_cross_run_summary( +                invalid_point_count = int(pd.to_numeric(overall_invalid.iloc[0].get("invalid_point_count"), errors="coerce") or 0)
  - tools/absorbance_debugger/analysis/cross_run.py @@ -23,0 +28,16 @@ def build_cross_run_summary( +        has_high_temp_zero_anchor_candidate = False

### `e928f6b0a2437f07d174d0cb6222d4697a614256`
- 时间: 2026-04-10 19:08:39 +0800
- 标题: chore: sync 2026-04-10 19:08:35
- 涉及文件: `tools/absorbance_debugger/analysis/pipeline.py`
- 判定原因: diff 内容命中关键词: coefficient
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/analysis/pipeline.py @@ -1777,0 +1781,5 @@ def execute_pipeline(config: DebuggerConfig) -> dict[str, Any]: +        temp_coeffs.assign(coefficients_desc=temp_coeffs["coefficients_desc"].map(json.loads))
  - tools/absorbance_debugger/analysis/pipeline.py @@ -1777,0 +1781,5 @@ def execute_pipeline(config: DebuggerConfig) -> dict[str, Any]: +        if "coefficients_desc" in temp_coeffs.columns
  - tools/absorbance_debugger/analysis/pipeline.py @@ -1780 +1788 @@ def execute_pipeline(config: DebuggerConfig) -> dict[str, Any]: -        temp_coeffs.assign(coefficients_desc=temp_coeffs["coefficients_desc"].map(json.loads)),
  - tools/absorbance_debugger/analysis/pipeline.py @@ -1818,0 +1827,5 @@ def execute_pipeline(config: DebuggerConfig) -> dict[str, Any]: +        r0_coeffs.assign(coefficients_desc=r0_coeffs["coefficients_desc"].map(json.loads))
  - tools/absorbance_debugger/analysis/pipeline.py @@ -1818,0 +1827,5 @@ def execute_pipeline(config: DebuggerConfig) -> dict[str, Any]: +        if "coefficients_desc" in r0_coeffs.columns

### `d43a7a4825d49cf41944e5b578231f1057e8686e`
- 时间: 2026-04-10 19:03:39 +0800
- 标题: chore: sync 2026-04-10 19:03:39
- 涉及文件: `tools/absorbance_debugger/analysis/pipeline.py`, `tools/absorbance_debugger/reports/renderers.py`, `tools/absorbance_debugger/tests/test_absorbance_debugger.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: MODE, co2, coefficient, mode, point, report, step, v1
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/reports/renderers.py @@ -160 +160 @@ def render_report_markdown(report: Mapping[str, object]) -> str: -    lines.append(_table_to_markdown(report["auto_conclusions"], max_rows=10))
  - tools/absorbance_debugger/reports/renderers.py @@ -160 +160 @@ def render_report_markdown(report: Mapping[str, object]) -> str: +    lines.append(_table_to_markdown(report["auto_conclusions"], max_rows=16))
  - tools/absorbance_debugger/tests/test_absorbance_debugger.py @@ -9,0 +11,6 @@ from tools.absorbance_debugger import gui as gui_module +from tools.absorbance_debugger.analysis.absorbance_models import (
  - tools/absorbance_debugger/tests/test_absorbance_debugger.py @@ -9,0 +11,6 @@ from tools.absorbance_debugger import gui as gui_module +    PIECEWISE_MODEL_SPECS,
  - tools/absorbance_debugger/tests/test_absorbance_debugger.py @@ -9,0 +11,6 @@ from tools.absorbance_debugger import gui as gui_module +    active_model_specs,

### `3c7a346487feb38a49f345fb08dc4ac93a020315`
- 时间: 2026-04-10 18:58:49 +0800
- 标题: chore: sync 2026-04-10 18:58:48
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `tests/v2/test_results_gateway.py`, `tools/absorbance_debugger/__init__.py`, `tools/absorbance_debugger/analysis/pipeline.py`, `tools/absorbance_debugger/app.py`, `tools/absorbance_debugger/cli.py`, `tools/absorbance_debugger/gui.py`, `tools/absorbance_debugger/reports/renderers.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, Report, Zero, cali, mode, point, report, span
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3511 +3511 @@ class AppFacade: -            sorted(items, key=lambda item: float(item.get("sort_key", 0.0) or 0.0), reverse=True)[:20],
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -3511 +3511 @@ class AppFacade: +            sorted(items, key=lambda item: float(item.get("sort_key", 0.0) or 0.0), reverse=True)[:40],
  - tools/absorbance_debugger/analysis/pipeline.py @@ -995 +995,10 @@ def _select_best_matched_source( +            "best_model_family",
  - tools/absorbance_debugger/analysis/pipeline.py @@ -995 +995,10 @@ def _select_best_matched_source( +            "zero_residual_mode",
  - tools/absorbance_debugger/analysis/pipeline.py @@ -995 +995,10 @@ def _select_best_matched_source( +            "zero_residual_model_label",

### `afb05d9158884e0df0cb39ca8edef13d52844cbb`
- 时间: 2026-04-10 18:53:42 +0800
- 标题: chore: sync 2026-04-10 18:53:40
- 涉及文件: `tools/absorbance_debugger/analysis/absorbance_models.py`, `tools/absorbance_debugger/analysis/cross_run.py`, `tools/absorbance_debugger/analysis/diagnostics.py`, `tools/absorbance_debugger/analysis/pipeline.py`, `tools/absorbance_debugger/plots/charts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Zero, co2, coefficient, mode, point, step, zero
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/analysis/absorbance_models.py @@ -407,0 +408,3 @@ def _fit_one_candidate( +            "A_from_mean": row.get("A_from_mean"),
  - tools/absorbance_debugger/analysis/absorbance_models.py @@ -407,0 +408,3 @@ def _fit_one_candidate( +            "A_alt_mean": row.get("A_alt_mean"),
  - tools/absorbance_debugger/analysis/absorbance_models.py @@ -407,0 +408,3 @@ def _fit_one_candidate( +            "R0_T_mean": row.get("R0_T_mean"),
  - tools/absorbance_debugger/analysis/absorbance_models.py @@ -646,0 +650,3 @@ def evaluate_absorbance_models( +            "A_from_mean",
  - tools/absorbance_debugger/analysis/absorbance_models.py @@ -646,0 +650,3 @@ def evaluate_absorbance_models( +            "A_alt_mean",

### `765790db9beeff6122db727a31097db82594609c`
- 时间: 2026-04-10 18:49:30 +0800
- 标题: chore: sync 2026-04-10 18:48:57
- 涉及文件: `tests/v2/test_historical_artifacts_cli.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_review_center.py`, `tests/v2/test_ui_v2_workbench_evidence.py`, `tools/absorbance_debugger/analysis/absorbance_models.py`, `tools/absorbance_debugger/analysis/comparison.py`, `tools/absorbance_debugger/analysis/zero_residual.py`, `tools/absorbance_debugger/models/config.py`, `tools/absorbance_debugger/options.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: MODE, Mode, ZERO, Zero, co2, coefficient, db, mode
- 关键 diff hunk 摘要:
  - tests/v2/test_historical_artifacts_cli.py @@ -122,0 +123,11 @@ def test_historical_scan_supports_single_run_dir_and_root_dir(tmp_path: Path, ca +    assert single_report["runs"][0]["method_confirmation_overview"]
  - tests/v2/test_historical_artifacts_cli.py @@ -122,0 +123,11 @@ def test_historical_scan_supports_single_run_dir_and_root_dir(tmp_path: Path, ca +    assert single_report["runs"][0]["validation_matrix_completeness"]
  - tests/v2/test_historical_artifacts_cli.py @@ -122,0 +123,11 @@ def test_historical_scan_supports_single_run_dir_and_root_dir(tmp_path: Path, ca +    assert single_report["runs"][0]["current_evidence_coverage"]
  - tests/v2/test_historical_artifacts_cli.py @@ -122,0 +123,11 @@ def test_historical_scan_supports_single_run_dir_and_root_dir(tmp_path: Path, ca +    assert single_report["runs"][0]["top_gaps"]
  - tests/v2/test_historical_artifacts_cli.py @@ -122,0 +123,11 @@ def test_historical_scan_supports_single_run_dir_and_root_dir(tmp_path: Path, ca +    assert single_report["runs"][0]["reviewer_actions"]

### `a8c3fc5333b85994a58a46ae360ea1ca1597f75c`
- 时间: 2026-04-10 18:43:39 +0800
- 标题: chore: sync 2026-04-10 18:43:34
- 涉及文件: `src/gas_calibrator/v2/core/measurement_phase_coverage.py`, `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`, `src/gas_calibrator/v2/review_surface_formatter.py`, `src/gas_calibrator/v2/scripts/historical_artifacts.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`, `src/gas_calibrator/v2/ui_v2/locales/en_US.json`, `src/gas_calibrator/v2/ui_v2/locales/zh_CN.json`, `tests/v2/test_build_offline_governance_artifacts.py`, `tests/v2/test_method_confirmation_wp4_contracts.py`, `tests/v2/test_results_gateway.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, H2O, PROTOCOL, cali, db, insert, mode, point
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -226,0 +227,20 @@ _READINESS_ARTIFACT_ANCHORS: dict[str, dict[str, str]] = { +    "route_specific_validation_matrix": {
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -226,0 +227,20 @@ _READINESS_ARTIFACT_ANCHORS: dict[str, dict[str, str]] = { +        "artifact_type": "route_specific_validation_matrix",
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -226,0 +227,20 @@ _READINESS_ARTIFACT_ANCHORS: dict[str, dict[str, str]] = { +        "anchor_id": "route-specific-validation-matrix",
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -226,0 +227,20 @@ _READINESS_ARTIFACT_ANCHORS: dict[str, dict[str, str]] = { +        "anchor_label": "Route specific validation matrix",
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -226,0 +227,20 @@ _READINESS_ARTIFACT_ANCHORS: dict[str, dict[str, str]] = { +    },

### `140dbef783773859b97fefd04c38f3cbd6df7f7a`
- 时间: 2026-04-10 18:38:35 +0800
- 标题: chore: sync 2026-04-10 18:38:31
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/artifact_catalog.py`, `src/gas_calibrator/v2/core/offline_artifacts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: PROTOCOL, cali, db, protocol, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -76,0 +77 @@ from ..core.stage3_standards_alignment_matrix_artifact_entry import ( +from .method_confirmation_gateway import MethodConfirmationGateway
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -305,0 +307,18 @@ class ResultsGateway: +        method_confirmation_payload = MethodConfirmationGateway(
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -305,0 +307,18 @@ class ResultsGateway: +            self.run_dir,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -305,0 +307,18 @@ class ResultsGateway: +            summary=summary if isinstance(summary, dict) else None,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -305,0 +307,18 @@ class ResultsGateway: +            analytics_summary=analytics_summary if isinstance(analytics_summary, dict) else None,

### `e0e27c7a049ab0b0ee8d27c1a20150a67292aa92`
- 时间: 2026-04-10 18:33:33 +0800
- 标题: chore: sync 2026-04-10 18:33:31
- 涉及文件: `src/gas_calibrator/v2/adapters/method_confirmation_gateway.py`, `src/gas_calibrator/v2/core/artifact_catalog.py`, `src/gas_calibrator/v2/core/method_confirmation_repository.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: DB, MODE, PROTOCOL, Protocol, REPORT, Step, cali, db
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/method_confirmation_gateway.py @@ -0,0 +1,37 @@ +from __future__ import annotations
  - src/gas_calibrator/v2/adapters/method_confirmation_gateway.py @@ -0,0 +1,37 @@ +
  - src/gas_calibrator/v2/adapters/method_confirmation_gateway.py @@ -0,0 +1,37 @@ +from pathlib import Path
  - src/gas_calibrator/v2/adapters/method_confirmation_gateway.py @@ -0,0 +1,37 @@ +from typing import Any
  - src/gas_calibrator/v2/adapters/method_confirmation_gateway.py @@ -0,0 +1,37 @@ +from ..core.method_confirmation_repository import (

### `081385cc45c855f8b10588d3fb941976f3fbccba`
- 时间: 2026-04-10 18:28:33 +0800
- 标题: chore: sync 2026-04-10 18:28:32
- 涉及文件: `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, H2O, PROTOCOL, Protocol, Step, cali, mode, point
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -2703,0 +2704,577 @@ def _build_method_confirmation_protocol( +def build_method_confirmation_wp4_artifacts(
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -2703,0 +2704,577 @@ def _build_method_confirmation_protocol( +    *,
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -2703,0 +2704,577 @@ def _build_method_confirmation_protocol( +    run_id: str,
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -2703,0 +2704,577 @@ def _build_method_confirmation_protocol( +    scope_definition_pack: dict[str, Any],
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -2703,0 +2704,577 @@ def _build_method_confirmation_protocol( +    decision_rule_profile: dict[str, Any],

### `08d80116428e263bce097385d061fcaf66c68427`
- 时间: 2026-04-10 18:23:33 +0800
- 标题: chore: sync 2026-04-10 18:23:30
- 涉及文件: `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Step, cali, protocol, report, writeback
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -68,0 +69,8 @@ METHOD_CONFIRMATION_MATRIX_MARKDOWN_FILENAME = "method_confirmation_matrix.md" +ROUTE_SPECIFIC_VALIDATION_MATRIX_FILENAME = "route_specific_validation_matrix.json"
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -68,0 +69,8 @@ METHOD_CONFIRMATION_MATRIX_MARKDOWN_FILENAME = "method_confirmation_matrix.md" +ROUTE_SPECIFIC_VALIDATION_MATRIX_MARKDOWN_FILENAME = "route_specific_validation_matrix.md"
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -68,0 +69,8 @@ METHOD_CONFIRMATION_MATRIX_MARKDOWN_FILENAME = "method_confirmation_matrix.md" +VALIDATION_RUN_SET_FILENAME = "validation_run_set.json"
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -68,0 +69,8 @@ METHOD_CONFIRMATION_MATRIX_MARKDOWN_FILENAME = "method_confirmation_matrix.md" +VALIDATION_RUN_SET_MARKDOWN_FILENAME = "validation_run_set.md"
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -68,0 +69,8 @@ METHOD_CONFIRMATION_MATRIX_MARKDOWN_FILENAME = "method_confirmation_matrix.md" +VERIFICATION_DIGEST_FILENAME = "verification_digest.json"

### `319cd457980a3c94ce45a621e4d760ead44ff350`
- 时间: 2026-04-10 18:18:32 +0800
- 标题: chore: sync 2026-04-10 18:18:31
- 涉及文件: `tools/absorbance_debugger/analysis/pipeline.py`, `tools/absorbance_debugger/reports/renderers.py`, `tools/absorbance_debugger/tests/test_absorbance_debugger.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: co2, coefficient, mode, point, report, step, zero
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/analysis/pipeline.py @@ -1581,2 +1581,9 @@ def execute_pipeline(config: DebuggerConfig) -> dict[str, Any]: -    _frame_to_csv(config.output_dir / "step_02_samples_filtered.csv", filtered)
  - tools/absorbance_debugger/analysis/pipeline.py @@ -1581,2 +1581,9 @@ def execute_pipeline(config: DebuggerConfig) -> dict[str, Any]: +    _frame_to_csv(config.output_dir / "step_02_samples_filtered.csv", filtered_pre_invalid)
  - tools/absorbance_debugger/analysis/pipeline.py @@ -1581,2 +1581,9 @@ def execute_pipeline(config: DebuggerConfig) -> dict[str, Any]: +    invalid_pressure_points, invalid_pressure_summary, filtered_valid_candidates, excluded_invalid = _identify_invalid_pressure_points(
  - tools/absorbance_debugger/analysis/pipeline.py @@ -1584 +1591,3 @@ def execute_pipeline(config: DebuggerConfig) -> dict[str, Any]: -    validation_table = _build_validation_table(points, filtered)
  - tools/absorbance_debugger/analysis/pipeline.py @@ -1584 +1591,3 @@ def execute_pipeline(config: DebuggerConfig) -> dict[str, Any]: +    _frame_to_csv(config.output_dir / "step_02x_invalid_pressure_points.csv", invalid_pressure_points)

### `69b8e57864cf23e6dd9b94867c9c220d38d28d6b`
- 时间: 2026-04-10 18:13:37 +0800
- 标题: chore: sync 2026-04-10 18:13:32
- 涉及文件: `tools/absorbance_debugger/analysis/diagnostics.py`, `tools/absorbance_debugger/analysis/pipeline.py`, `tools/absorbance_debugger/plots/__init__.py`, `tools/absorbance_debugger/plots/charts.py`
- 判定原因: diff 内容命中关键词: co2, coefficient, mode, point, report
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/analysis/diagnostics.py @@ -335,0 +336,8 @@ def evaluate_scenario_groups( +def _selected_source_subset(branch_points: pd.DataFrame, selected_source_map: dict[str, str] | None) -> pd.DataFrame:
  - tools/absorbance_debugger/analysis/diagnostics.py @@ -335,0 +336,8 @@ def evaluate_scenario_groups( +        return branch_points.copy()
  - tools/absorbance_debugger/analysis/diagnostics.py @@ -335,0 +336,8 @@ def evaluate_scenario_groups( +    subset = branch_points.copy()
  - tools/absorbance_debugger/analysis/diagnostics.py @@ -342,5 +351,5 @@ def build_order_compare( -    subset = branch_points[
  - tools/absorbance_debugger/analysis/diagnostics.py @@ -342,5 +351,5 @@ def build_order_compare( -        (branch_points["ratio_in_source"] == config.default_ratio_source)

### `5b778ba997cbf613030e333044b1dcfde24c7c7b`
- 时间: 2026-04-10 18:08:37 +0800
- 标题: chore: sync 2026-04-10 18:08:34
- 涉及文件: `tools/absorbance_debugger/analysis/pipeline.py`, `tools/absorbance_debugger/app.py`, `tools/absorbance_debugger/cli.py`, `tools/absorbance_debugger/gui.py`, `tools/absorbance_debugger/models/config.py`, `tools/absorbance_debugger/options.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: MODE, co2, mode, point, report, span, store
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/analysis/pipeline.py @@ -48,0 +51 @@ from ..plots.charts import ( +    plot_invalid_pressure_points,
  - tools/absorbance_debugger/analysis/pipeline.py @@ -327,0 +331,160 @@ def _filter_samples(samples: pd.DataFrame, config: DebuggerConfig) -> tuple[pd.D +            "point_title",
  - tools/absorbance_debugger/analysis/pipeline.py @@ -327,0 +331,160 @@ def _filter_samples(samples: pd.DataFrame, config: DebuggerConfig) -> tuple[pd.D +            "point_row",
  - tools/absorbance_debugger/analysis/pipeline.py @@ -327,0 +331,160 @@ def _filter_samples(samples: pd.DataFrame, config: DebuggerConfig) -> tuple[pd.D +def _identify_invalid_pressure_points(
  - tools/absorbance_debugger/analysis/pipeline.py @@ -327,0 +331,160 @@ def _filter_samples(samples: pd.DataFrame, config: DebuggerConfig) -> tuple[pd.D +    point_summary = (

### `42b99d8e45ee6f25d0aa497e3526a51ff4ff238d`
- 时间: 2026-04-10 17:48:37 +0800
- 标题: chore: sync 2026-04-10 17:48:33
- 涉及文件: `tests/v2/test_build_offline_governance_artifacts.py`, `tests/v2/test_ui_v2_review_center.py`, `tools/absorbance_debugger/analysis/diagnostics.py`, `tools/absorbance_debugger/analysis/pipeline.py`, `tools/absorbance_debugger/tests/test_absorbance_debugger.py`
- 判定原因: diff 内容命中关键词: mode, report, step
- 关键 diff hunk 摘要:
  - tests/v2/test_build_offline_governance_artifacts.py @@ -1153 +1153 @@ def test_rebuild_run_generates_recognition_readiness_artifacts(tmp_path: Path) - -    assert "formal uncertainty declaration" in uncertainty_report_pack["gap_note"].lower()
  - tests/v2/test_build_offline_governance_artifacts.py @@ -1153 +1153 @@ def test_rebuild_run_generates_recognition_readiness_artifacts(tmp_path: Path) - +    assert "formal uncertainty" in uncertainty_report_pack["gap_note"].lower()
  - tools/absorbance_debugger/analysis/diagnostics.py @@ -419,0 +422 @@ def build_pressure_branch_compare( +    summary["pressure_branch_report"] = summary["pressure_branch_label"]
  - tools/absorbance_debugger/analysis/diagnostics.py @@ -462,0 +476,8 @@ def build_upper_bound_vs_deployable_compare( +            else "absorbance_ppm_model_gap"
  - tools/absorbance_debugger/analysis/pipeline.py @@ -728 +728 @@ def _run_loss_diagnostics( +        "pressure_branch_report",

### `c5b6935fdb48a635ca1acd3c5af61d42ef5664f5`
- 时间: 2026-04-10 17:43:35 +0800
- 标题: chore: sync 2026-04-10 17:43:33
- 涉及文件: `src/gas_calibrator/v2/review_surface_formatter.py`, `tools/absorbance_debugger/analysis/diagnostics.py`, `tools/absorbance_debugger/analysis/pipeline.py`, `tools/absorbance_debugger/gui.py`, `tools/absorbance_debugger/plots/__init__.py`, `tools/absorbance_debugger/plots/charts.py`, `tools/absorbance_debugger/reports/renderers.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, Mode, cali, calibration, co2, db, mode, point
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -543,0 +544,7 @@ def _display_non_claim_summary(payload: dict[str, Any]) -> str: +    raw_non_claim = payload.get("non_claim")
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -543,0 +544,7 @@ def _display_non_claim_summary(payload: dict[str, Any]) -> str: +    if isinstance(raw_non_claim, str):
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -543,0 +544,7 @@ def _display_non_claim_summary(payload: dict[str, Any]) -> str: +        text_values = [raw_non_claim]
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -543,0 +544,7 @@ def _display_non_claim_summary(payload: dict[str, Any]) -> str: +    elif isinstance(raw_non_claim, (list, tuple, set)):
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -543,0 +544,7 @@ def _display_non_claim_summary(payload: dict[str, Any]) -> str: +        text_values = list(raw_non_claim)

### `3834b7ee5feaafc2f0664e374d1b3c22c080dc7b`
- 时间: 2026-04-10 17:38:50 +0800
- 标题: chore: sync 2026-04-10 17:38:48
- 涉及文件: `tests/v2/test_results_gateway.py`, `tests/v2/test_uncertainty_wp3_contracts.py`, `tools/absorbance_debugger/analysis/absorbance_models.py`, `tools/absorbance_debugger/app.py`, `tools/absorbance_debugger/cli.py`, `tools/absorbance_debugger/models/config.py`, `tools/absorbance_debugger/options.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: MODE, Mode, db, mode, point
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/analysis/absorbance_models.py @@ -103 +103 @@ def _design_matrix(frame: pd.DataFrame, spec: AbsorbanceModelSpec) -> np.ndarray -    absorbance = frame["A_mean"].to_numpy(dtype=float)
  - tools/absorbance_debugger/analysis/absorbance_models.py @@ -103 +103 @@ def _design_matrix(frame: pd.DataFrame, spec: AbsorbanceModelSpec) -> np.ndarray +    absorbance = frame["absorbance_input"].to_numpy(dtype=float)
  - tools/absorbance_debugger/analysis/absorbance_models.py @@ -154,0 +155 @@ def _fit_one_candidate( +    absorbance_column: str,
  - tools/absorbance_debugger/analysis/absorbance_models.py @@ -156 +157,3 @@ def _fit_one_candidate( -    candidate_df = analyzer_df.dropna(subset=["A_mean", "temp_model_c", "target_ppm"]).copy()
  - tools/absorbance_debugger/analysis/absorbance_models.py @@ -156 +157,3 @@ def _fit_one_candidate( +    candidate_df = analyzer_df.dropna(subset=[absorbance_column, "temp_model_c", "target_ppm"]).copy()

### `1bc21a9afa3b910636e38b5f7d90dc96ee572c91`
- 时间: 2026-04-10 17:33:35 +0800
- 标题: chore: sync 2026-04-10 17:33:32
- 涉及文件: `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`, `src/gas_calibrator/v2/core/uncertainty_repository.py`, `tests/v2/test_historical_artifacts_cli.py`, `tests/v2/test_uncertainty_wp3_contracts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, COEFFICIENT, H2O, MODE, REPORT, cali, coefficient, db
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -507,0 +508,21 @@ def build_recognition_readiness_artifacts( +        filenames={
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -507,0 +508,21 @@ def build_recognition_readiness_artifacts( +            "uncertainty_model": UNCERTAINTY_MODEL_FILENAME,
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -507,0 +508,21 @@ def build_recognition_readiness_artifacts( +            "uncertainty_model_markdown": UNCERTAINTY_MODEL_MARKDOWN_FILENAME,
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -507,0 +508,21 @@ def build_recognition_readiness_artifacts( +            "uncertainty_input_set": UNCERTAINTY_INPUT_SET_FILENAME,
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -507,0 +508,21 @@ def build_recognition_readiness_artifacts( +            "uncertainty_input_set_markdown": UNCERTAINTY_INPUT_SET_MARKDOWN_FILENAME,

### `102289b7e72f63acffafdc586f60045703fb7a99`
- 时间: 2026-04-10 17:28:35 +0800
- 标题: chore: sync 2026-04-10 17:28:34
- 涉及文件: `tests/v2/test_build_offline_governance_artifacts.py`, `tests/v2/test_historical_artifacts_cli.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_review_center.py`, `tests/v2/test_ui_v2_workbench_evidence.py`
- 判定原因: diff 内容命中关键词: CO2, COEFFICIENT, H2O, MODE, REPORT, coefficient, db, mode
- 关键 diff hunk 摘要:
  - tests/v2/test_build_offline_governance_artifacts.py @@ -933,0 +934,16 @@ def test_rebuild_run_generates_recognition_readiness_artifacts(tmp_path: Path) - +        recognition_readiness.UNCERTAINTY_MODEL_FILENAME,
  - tests/v2/test_build_offline_governance_artifacts.py @@ -933,0 +934,16 @@ def test_rebuild_run_generates_recognition_readiness_artifacts(tmp_path: Path) - +        recognition_readiness.UNCERTAINTY_MODEL_MARKDOWN_FILENAME,
  - tests/v2/test_build_offline_governance_artifacts.py @@ -933,0 +934,16 @@ def test_rebuild_run_generates_recognition_readiness_artifacts(tmp_path: Path) - +        recognition_readiness.SENSITIVITY_COEFFICIENT_SET_FILENAME,
  - tests/v2/test_build_offline_governance_artifacts.py @@ -933,0 +934,16 @@ def test_rebuild_run_generates_recognition_readiness_artifacts(tmp_path: Path) - +        recognition_readiness.SENSITIVITY_COEFFICIENT_SET_MARKDOWN_FILENAME,
  - tests/v2/test_build_offline_governance_artifacts.py @@ -933,0 +934,16 @@ def test_rebuild_run_generates_recognition_readiness_artifacts(tmp_path: Path) - +        recognition_readiness.UNCERTAINTY_REPORT_PACK_FILENAME,

### `a3c417f3c1df8915c66cd81a7410a3eb537485a6`
- 时间: 2026-04-10 17:23:46 +0800
- 标题: chore: sync 2026-04-10 17:23:46
- 涉及文件: `src/gas_calibrator/v2/scripts/historical_artifacts.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/locales/en_US.json`, `src/gas_calibrator/v2/ui_v2/locales/zh_CN.json`, `tools/absorbance_debugger/analysis/absorbance_models.py`, `tools/absorbance_debugger/analysis/comparison.py`, `tools/absorbance_debugger/analysis/pipeline.py`, `tools/absorbance_debugger/plots/__init__.py`, `tools/absorbance_debugger/reports/renderers.py`, `tools/absorbance_debugger/tests/test_absorbance_debugger.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: REPORT, cali, calibration, co2, coefficient, mode, point, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/scripts/historical_artifacts.py @@ -17,0 +18 @@ from ..adapters.recognition_scope_gateway import RecognitionScopeGateway +from ..adapters.uncertainty_gateway import UncertaintyGateway
  - src/gas_calibrator/v2/scripts/historical_artifacts.py @@ -146,0 +148,12 @@ def _build_run_report( +    uncertainty_payload = UncertaintyGateway(
  - src/gas_calibrator/v2/scripts/historical_artifacts.py @@ -146,0 +148,12 @@ def _build_run_report( +        run_dir,
  - src/gas_calibrator/v2/scripts/historical_artifacts.py @@ -146,0 +148,12 @@ def _build_run_report( +        summary=summary_payload,
  - src/gas_calibrator/v2/scripts/historical_artifacts.py @@ -146,0 +148,12 @@ def _build_run_report( +        scope_readiness_summary=(

### `41f9877514c971bd5abb0f5421e25d458b7b459a`
- 时间: 2026-04-10 17:18:38 +0800
- 标题: chore: sync 2026-04-10 17:18:31
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/artifact_compatibility.py`, `src/gas_calibrator/v2/review_surface_formatter.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`, `tools/absorbance_debugger/analysis/absorbance_models.py`, `tools/absorbance_debugger/analysis/pipeline.py`, `tools/absorbance_debugger/app.py`, `tools/absorbance_debugger/cli.py`, `tools/absorbance_debugger/gui.py`, `tools/absorbance_debugger/models/config.py`, `tools/absorbance_debugger/options.py`, `tools/absorbance_debugger/plots/charts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: COEFFICIENT, MODE, Mode, REPORT, cali, calibration, co2, coefficient
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -77,0 +78 @@ from .recognition_scope_gateway import RecognitionScopeGateway +from .uncertainty_gateway import UncertaintyGateway
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -286,0 +288,18 @@ class ResultsGateway: +        uncertainty_payload = UncertaintyGateway(
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -286,0 +288,18 @@ class ResultsGateway: +            self.run_dir,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -286,0 +288,18 @@ class ResultsGateway: +            summary=summary if isinstance(summary, dict) else None,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -286,0 +288,18 @@ class ResultsGateway: +            analytics_summary=analytics_summary if isinstance(analytics_summary, dict) else None,

### `b78d5d128f6db85be5f45d8369678bbd48f65628`
- 时间: 2026-04-10 17:13:33 +0800
- 标题: chore: sync 2026-04-10 17:13:32
- 涉及文件: `src/gas_calibrator/v2/adapters/uncertainty_gateway.py`, `src/gas_calibrator/v2/core/uncertainty_repository.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: COEFFICIENT, DB, MODE, Protocol, REPORT, Step, cali, coefficient
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/uncertainty_gateway.py @@ -0,0 +1,39 @@ +from __future__ import annotations
  - src/gas_calibrator/v2/adapters/uncertainty_gateway.py @@ -0,0 +1,39 @@ +
  - src/gas_calibrator/v2/adapters/uncertainty_gateway.py @@ -0,0 +1,39 @@ +from pathlib import Path
  - src/gas_calibrator/v2/adapters/uncertainty_gateway.py @@ -0,0 +1,39 @@ +from typing import Any
  - src/gas_calibrator/v2/adapters/uncertainty_gateway.py @@ -0,0 +1,39 @@ +from ..core.uncertainty_repository import (

### `1adc5fb65688068a8ed31206a1b4e6c91a19753f`
- 时间: 2026-04-10 17:08:33 +0800
- 标题: chore: sync 2026-04-10 17:08:30
- 涉及文件: `src/gas_calibrator/v2/core/artifact_catalog.py`, `src/gas_calibrator/v2/core/measurement_phase_coverage.py`, `src/gas_calibrator/v2/core/offline_artifacts.py`, `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`, `tools/absorbance_debugger/analysis/comparison.py`, `tools/absorbance_debugger/analysis/pipeline.py`, `tools/absorbance_debugger/gui.py`, `tools/absorbance_debugger/reports/renderers.py`, `tools/absorbance_debugger/tests/test_absorbance_debugger.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: COEFFICIENT, MODE, REPORT, Report, Zero, cali, co2, coefficient
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -43,0 +44,4 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "uncertainty_model",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -43,0 +44,4 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "uncertainty_input_set",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -43,0 +44,4 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "sensitivity_coefficient_set",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -43,0 +44,4 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "budget_case",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -84,0 +89,8 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "uncertainty_golden_cases",

### `9fdc03d0a4aab2c59a791931ab309522d25c91b1`
- 时间: 2026-04-10 17:03:34 +0800
- 标题: chore: sync 2026-04-10 17:03:31
- 涉及文件: `src/gas_calibrator/v2/core/uncertainty_builder.py`, `tools/absorbance_debugger/analysis/comparison.py`, `tools/absorbance_debugger/analysis/pipeline.py`, `tools/absorbance_debugger/app.py`, `tools/absorbance_debugger/cli.py`, `tools/absorbance_debugger/models/config.py`, `tools/absorbance_debugger/options.py`, `tools/absorbance_debugger/plots/__init__.py`, `tools/absorbance_debugger/plots/charts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, Coefficient, H2O, Step, Writeback, Zero, cali, calibration
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/uncertainty_builder.py @@ -0,0 +1,1176 @@ +from __future__ import annotations
  - src/gas_calibrator/v2/core/uncertainty_builder.py @@ -0,0 +1,1176 @@ +
  - src/gas_calibrator/v2/core/uncertainty_builder.py @@ -0,0 +1,1176 @@ +from datetime import datetime, timezone
  - src/gas_calibrator/v2/core/uncertainty_builder.py @@ -0,0 +1,1176 @@ +from typing import Any
  - src/gas_calibrator/v2/core/uncertainty_builder.py @@ -0,0 +1,1176 @@ +def _now_iso() -> str:

### `25c595846ae1b8549f85644e92d3fa82bcf680a4`
- 时间: 2026-04-10 16:58:33 +0800
- 标题: chore: sync 2026-04-10 16:58:32
- 涉及文件: `requirements.txt`, `tests/test_absorbance_debugger.py`, `tools/absorbance_debugger/requirements.txt`, `tools/absorbance_debugger/tests/test_absorbance_debugger.py`
- 判定原因: diff 内容命中关键词: db, mode
- 关键 diff hunk 摘要:
  - 未提取到关键词 hunk，建议结合 raw git log 复核。

### `01686460701849e73e5c4b88175dfbe11c9c4022`
- 时间: 2026-04-10 16:53:32 +0800
- 标题: chore: sync 2026-04-10 16:53:30
- 涉及文件: `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: COEFFICIENT, MODE, REPORT, Step, cali, coefficient, mode, protocol
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -48,0 +49,16 @@ UNCERTAINTY_BUDGET_STUB_MARKDOWN_FILENAME = "uncertainty_budget_stub.md" +UNCERTAINTY_MODEL_FILENAME = "uncertainty_model.json"
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -48,0 +49,16 @@ UNCERTAINTY_BUDGET_STUB_MARKDOWN_FILENAME = "uncertainty_budget_stub.md" +UNCERTAINTY_MODEL_MARKDOWN_FILENAME = "uncertainty_model.md"
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -48,0 +49,16 @@ UNCERTAINTY_BUDGET_STUB_MARKDOWN_FILENAME = "uncertainty_budget_stub.md" +UNCERTAINTY_INPUT_SET_FILENAME = "uncertainty_input_set.json"
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -48,0 +49,16 @@ UNCERTAINTY_BUDGET_STUB_MARKDOWN_FILENAME = "uncertainty_budget_stub.md" +UNCERTAINTY_INPUT_SET_MARKDOWN_FILENAME = "uncertainty_input_set.md"
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -48,0 +49,16 @@ UNCERTAINTY_BUDGET_STUB_MARKDOWN_FILENAME = "uncertainty_budget_stub.md" +SENSITIVITY_COEFFICIENT_SET_FILENAME = "sensitivity_coefficient_set.json"

### `dd61f9cd831180ac77df20a0ef7590dbe0fb5cbb`
- 时间: 2026-04-10 16:48:33 +0800
- 标题: chore: sync 2026-04-10 16:48:33
- 涉及文件: `tools/absorbance_debugger/analysis/pipeline.py`, `tools/absorbance_debugger/reports/renderers.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: calibration, db, mode, point, report
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/analysis/pipeline.py @@ -716,0 +717,19 @@ def _report_tables( +        old_row = subset[subset["model_name"] == "old_ratio_poly_simplified"]
  - tools/absorbance_debugger/analysis/pipeline.py @@ -716,0 +717,19 @@ def _report_tables( +        new_row = subset[subset["model_name"] == "new_abs_linear"]
  - tools/absorbance_debugger/analysis/pipeline.py @@ -716,0 +717,19 @@ def _report_tables( +                f"{analyzer}: the current diagnostic absorbance-linear branch outperformed the archived ratio model "
  - tools/absorbance_debugger/analysis/pipeline.py @@ -716,0 +717,19 @@ def _report_tables( +                f"{analyzer}: the archived ratio model still fits better than the current diagnostic absorbance-linear branch "
  - tools/absorbance_debugger/analysis/pipeline.py @@ -748,0 +769 @@ def _report_tables( +            "The new-chain ppm comparison currently uses a simple linear diagnostic fit on A_mean, not a production-grade absorbance calibration model.",

### `bbef150e11886df5a4499d57deb1f7923cf22021`
- 时间: 2026-04-10 16:38:37 +0800
- 标题: chore: sync 2026-04-10 16:38:34
- 涉及文件: `requirements.txt`, `tests/test_absorbance_debugger.py`, `tools/absorbance_debugger/analysis/pipeline.py`, `tools/absorbance_debugger/app.py`, `tools/absorbance_debugger/cli.py`, `tools/absorbance_debugger/plots/charts.py`, `tools/absorbance_debugger/reports/renderers.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, POINT, Point, V1, calibration, co2, coefficient, h2o
- 关键 diff hunk 摘要:
  - tests/test_absorbance_debugger.py @@ -0,0 +1,53 @@ +    assert (output_dir / "step_01_samples_core.csv").exists()
  - tests/test_absorbance_debugger.py @@ -0,0 +1,53 @@ +    assert (output_dir / "step_05_r0_fit_coefficients.csv").exists()
  - tests/test_absorbance_debugger.py @@ -0,0 +1,53 @@ +    assert (output_dir / "step_08_old_vs_new_compare.xlsx").exists()
  - tests/test_absorbance_debugger.py @@ -0,0 +1,53 @@ +    assert (output_dir / "report.md").exists()
  - tests/test_absorbance_debugger.py @@ -0,0 +1,53 @@ +    assert (output_dir / "report.html").exists()

### `b792974defe76e6126954ab34e5a8d0430ab6063`
- 时间: 2026-04-10 16:28:38 +0800
- 标题: chore: sync 2026-04-10 16:28:32
- 涉及文件: `tools/__init__.py`, `tools/absorbance_debugger/__init__.py`, `tools/absorbance_debugger/__main__.py`, `tools/absorbance_debugger/analysis/__init__.py`, `tools/absorbance_debugger/analysis/fits.py`, `tools/absorbance_debugger/io/__init__.py`, `tools/absorbance_debugger/io/run_bundle.py`, `tools/absorbance_debugger/models/__init__.py`, `tools/absorbance_debugger/models/config.py`, `tools/absorbance_debugger/parsers/__init__.py`, `tools/absorbance_debugger/parsers/schema.py`, `tools/absorbance_debugger/plots/__init__.py`, `tools/absorbance_debugger/plots/charts.py`, `tools/absorbance_debugger/reports/__init__.py`, `tools/absorbance_debugger/reports/renderers.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, H2O, MODE, POINT, Report, Zero, co2, coefficient
- 关键 diff hunk 摘要:
  - tools/absorbance_debugger/__main__.py @@ -0,0 +1,7 @@ +"""Module entrypoint for the offline absorbance debugger."""
  - tools/absorbance_debugger/analysis/fits.py @@ -0,0 +1,138 @@ +    model_name: str
  - tools/absorbance_debugger/analysis/fits.py @@ -0,0 +1,138 @@ +    coefficients_desc: tuple[float, ...]
  - tools/absorbance_debugger/analysis/fits.py @@ -0,0 +1,138 @@ +        return np.polyval(self.coefficients_desc, values)
  - tools/absorbance_debugger/analysis/fits.py @@ -0,0 +1,138 @@ +        for idx, coeff in enumerate(self.coefficients_desc):

### `9c6004333665f493bcfae3ad8faf2f8e49e3770b`
- 时间: 2026-04-10 16:13:34 +0800
- 标题: chore: sync 2026-04-10 16:13:34
- 涉及文件: `tests/v2/test_historical_artifacts_cli.py`, `tests/v2/test_recognition_readiness_wp2_contracts.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_review_center.py`, `tests/v2/test_ui_v2_workbench_evidence.py`
- 判定原因: diff 内容命中关键词: cali, insert, mode, point, report, serial, step, store
- 关键 diff hunk 摘要:
  - tests/v2/test_historical_artifacts_cli.py @@ -98,0 +99,14 @@ def test_historical_scan_supports_single_run_dir_and_root_dir(tmp_path: Path, ca +    assert single_report["runs"][0]["asset_readiness_overview"]
  - tests/v2/test_historical_artifacts_cli.py @@ -98,0 +99,14 @@ def test_historical_scan_supports_single_run_dir_and_root_dir(tmp_path: Path, ca +    assert single_report["runs"][0]["certificate_lifecycle_overview"]
  - tests/v2/test_historical_artifacts_cli.py @@ -98,0 +99,14 @@ def test_historical_scan_supports_single_run_dir_and_root_dir(tmp_path: Path, ca +    assert single_report["runs"][0]["pre_run_gate_status"] in {
  - tests/v2/test_historical_artifacts_cli.py @@ -98,0 +99,14 @@ def test_historical_scan_supports_single_run_dir_and_root_dir(tmp_path: Path, ca +    assert single_report["runs"][0]["pre_run_gate_summary"]
  - tests/v2/test_historical_artifacts_cli.py @@ -98,0 +99,14 @@ def test_historical_scan_supports_single_run_dir_and_root_dir(tmp_path: Path, ca +    assert single_report["runs"][0]["blocking_digest"]

### `c4235814f6686bff7df9c53d4fc61a025bd7b8c9`
- 时间: 2026-04-10 16:08:33 +0800
- 标题: chore: sync 2026-04-10 16:08:33
- 涉及文件: `tests/v2/test_build_offline_governance_artifacts.py`, `tests/v2/test_recognition_scope_repository.py`, `tests/v2/test_results_gateway.py`
- 判定原因: diff 内容命中关键词: mode, point, serial
- 关键 diff hunk 摘要:
  - tests/v2/test_build_offline_governance_artifacts.py @@ -991,0 +1002,76 @@ def test_rebuild_run_generates_recognition_readiness_artifacts(tmp_path: Path) - +        "dewpoint_meter",
  - tests/v2/test_build_offline_governance_artifacts.py @@ -991,0 +1002,76 @@ def test_rebuild_run_generates_recognition_readiness_artifacts(tmp_path: Path) - +        "model",
  - tests/v2/test_build_offline_governance_artifacts.py @@ -991,0 +1002,76 @@ def test_rebuild_run_generates_recognition_readiness_artifacts(tmp_path: Path) - +        "serial_or_lot",

### `d79a3820b8f3beb1d3f4c1ed64ba05dc278dfa99`
- 时间: 2026-04-10 16:03:33 +0800
- 标题: chore: sync 2026-04-10 16:03:33
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/scripts/historical_artifacts.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/locales/en_US.json`, `src/gas_calibrator/v2/ui_v2/locales/zh_CN.json`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, point, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1188 +1188,7 @@ class ResultsGateway: -                lines.append(f"asset readiness overview: {reference_asset_text}")
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1188 +1188,7 @@ class ResultsGateway: +                lines.append(
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1188 +1188,7 @@ class ResultsGateway: +                    t(
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1188 +1188,7 @@ class ResultsGateway: +                        "facade.results.result_summary.asset_readiness_overview",
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1188 +1188,7 @@ class ResultsGateway: +                        value=reference_asset_text,

### `3d870ddcf6a77288e2930f24331d05c7937ea8d1`
- 时间: 2026-04-10 15:58:31 +0800
- 标题: chore: sync 2026-04-10 15:58:30
- 涉及文件: `src/gas_calibrator/v2/review_surface_formatter.py`, `src/gas_calibrator/v2/scripts/historical_artifacts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -1454,0 +1455,5 @@ def build_readiness_review_digest_lines(payload: dict[str, Any]) -> dict[str, li +        t(
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -1454,0 +1455,5 @@ def build_readiness_review_digest_lines(payload: dict[str, Any]) -> dict[str, li +            "results.review_center.detail.readiness.reviewer_action_summary_line",
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -1454,0 +1455,5 @@ def build_readiness_review_digest_lines(payload: dict[str, Any]) -> dict[str, li +            value=reviewer_action_summary,
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -1454,0 +1455,5 @@ def build_readiness_review_digest_lines(payload: dict[str, Any]) -> dict[str, li +            default=f"Reviewer actions: {reviewer_action_summary}",
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -1454,0 +1455,5 @@ def build_readiness_review_digest_lines(payload: dict[str, Any]) -> dict[str, li +        ),

### `181178061d83d21c73c59a311472bdbd971269fd`
- 时间: 2026-04-10 15:53:36 +0800
- 标题: chore: sync 2026-04-10 15:53:33
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, db
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -455,0 +456,2 @@ class DeviceWorkbenchController: +        reference_asset_registry = dict(payload.get("reference_asset_registry") or {})
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -455,0 +456,2 @@ class DeviceWorkbenchController: +        certificate_lifecycle_summary = dict(payload.get("certificate_lifecycle_summary") or {})
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -457,0 +460 @@ class DeviceWorkbenchController: +        pre_run_readiness_gate = dict(payload.get("pre_run_readiness_gate") or {})
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -470,0 +474,2 @@ class DeviceWorkbenchController: +            "reference_asset_registry": reference_asset_registry,
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -470,0 +474,2 @@ class DeviceWorkbenchController: +            "certificate_lifecycle_summary": certificate_lifecycle_summary,

### `73c2022c20062981477bec108bd5a016e43a14de`
- 时间: 2026-04-10 15:48:34 +0800
- 标题: chore: sync 2026-04-10 15:48:32
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -411,0 +412,2 @@ class ResultsGateway: +        reference_asset_registry = dict(payload.get("reference_asset_registry", {}) or {})
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -411,0 +412,2 @@ class ResultsGateway: +        certificate_lifecycle_summary = dict(payload.get("certificate_lifecycle_summary", {}) or {})
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -413,0 +416 @@ class ResultsGateway: +        pre_run_readiness_gate = dict(payload.get("pre_run_readiness_gate", {}) or {})
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -654,0 +658,8 @@ class ResultsGateway: +            row = self._decorate_reference_asset_registry_row(
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -654,0 +658,8 @@ class ResultsGateway: +                row,

### `f50ccf63e4a4a28dc0331323ce3a30577144ba93`
- 时间: 2026-04-10 15:43:31 +0800
- 标题: chore: sync 2026-04-10 15:43:31
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/review_surface_formatter.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -282,0 +283,3 @@ class ResultsGateway: +        reference_asset_registry = dict(recognition_scope_payload.get("reference_asset_registry") or {})
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -282,0 +283,3 @@ class ResultsGateway: +        certificate_lifecycle_summary = dict(recognition_scope_payload.get("certificate_lifecycle_summary") or {})
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -282,0 +283,3 @@ class ResultsGateway: +        pre_run_readiness_gate = dict(recognition_scope_payload.get("pre_run_readiness_gate") or {})
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -320,0 +324,2 @@ class ResultsGateway: +            reference_asset_registry=reference_asset_registry,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -320,0 +324,2 @@ class ResultsGateway: +            certificate_lifecycle_summary=certificate_lifecycle_summary,

### `06e6e9225465df3cd2211cc3cf7da2407eae4956`
- 时间: 2026-04-10 15:38:33 +0800
- 标题: chore: sync 2026-04-10 15:38:32
- 涉及文件: `src/gas_calibrator/v2/adapters/recognition_scope_gateway.py`, `src/gas_calibrator/v2/core/artifact_catalog.py`, `src/gas_calibrator/v2/core/measurement_phase_coverage.py`, `src/gas_calibrator/v2/core/offline_artifacts.py`, `src/gas_calibrator/v2/core/recognition_scope_repository.py`, `src/gas_calibrator/v2/review_surface_formatter.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: MODE, Protocol, REPORT, Step, cali, mode
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/recognition_scope_gateway.py @@ -32,0 +33,3 @@ class RecognitionScopeGateway: +            "reference_asset_registry": dict(snapshot.get("reference_asset_registry") or {}),
  - src/gas_calibrator/v2/adapters/recognition_scope_gateway.py @@ -32,0 +33,3 @@ class RecognitionScopeGateway: +            "certificate_lifecycle_summary": dict(snapshot.get("certificate_lifecycle_summary") or {}),
  - src/gas_calibrator/v2/adapters/recognition_scope_gateway.py @@ -32,0 +33,3 @@ class RecognitionScopeGateway: +            "pre_run_readiness_gate": dict(snapshot.get("pre_run_readiness_gate") or {}),
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -41,0 +42 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "certificate_lifecycle_summary",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -77,0 +79,2 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "certificate_lifecycle_summary",

### `677997192f5126ff026ad1a52acd7e213c4c0540`
- 时间: 2026-04-10 15:33:31 +0800
- 标题: chore: sync 2026-04-10 15:33:31
- 涉及文件: `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`, `src/gas_calibrator/v2/core/recognition_scope_repository.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, DB, Step, cali, calibration, mode, point, serial
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -1585,0 +1586,257 @@ def _build_reference_asset_registry( +def _build_certificate_lifecycle_summary(
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -1585,0 +1586,257 @@ def _build_reference_asset_registry( +    *,
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -1585,0 +1586,257 @@ def _build_reference_asset_registry( +    run_id: str,
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -1585,0 +1586,257 @@ def _build_reference_asset_registry( +    scope_definition_pack: dict[str, Any],
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -1585,0 +1586,257 @@ def _build_reference_asset_registry( +    decision_rule_profile: dict[str, Any],

### `fd7223005d499b4b1631a95eec62ab1d56ab5923`
- 时间: 2026-04-10 15:28:33 +0800
- 标题: chore: sync 2026-04-10 15:28:30
- 涉及文件: `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, DB, H2O, STEP, Step, cali, mode, point
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -1170,0 +1171,2 @@ def _build_reference_asset_registry( +    scope_definition_pack: dict[str, Any],
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -1170,0 +1171,2 @@ def _build_reference_asset_registry( +    decision_rule_profile: dict[str, Any],
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -1175 +1177,12 @@ def _build_reference_asset_registry( -    analyzer_scope = " | ".join(sample_digest["analyzers"]) or "simulation_analyzer_population"
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -1175 +1177,12 @@ def _build_reference_asset_registry( +    scope_raw = dict(scope_definition_pack.get("raw") or {})
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -1175 +1177,12 @@ def _build_reference_asset_registry( +    decision_raw = dict(decision_rule_profile.get("raw") or {})

### `efdcf118dd3e361777d00e067f95fb37498d3fc9`
- 时间: 2026-04-10 15:23:32 +0800
- 标题: chore: sync 2026-04-10 15:23:31
- 涉及文件: `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Step, cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -38,0 +39,2 @@ REFERENCE_ASSET_REGISTRY_MARKDOWN_FILENAME = "reference_asset_registry.md" +CERTIFICATE_LIFECYCLE_SUMMARY_FILENAME = "certificate_lifecycle_summary.json"
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -38,0 +39,2 @@ REFERENCE_ASSET_REGISTRY_MARKDOWN_FILENAME = "reference_asset_registry.md" +CERTIFICATE_LIFECYCLE_SUMMARY_MARKDOWN_FILENAME = "certificate_lifecycle_summary.md"
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -40,0 +43,2 @@ CERTIFICATE_READINESS_SUMMARY_MARKDOWN_FILENAME = "certificate_readiness_summary +PRE_RUN_READINESS_GATE_FILENAME = "pre_run_readiness_gate.json"
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -40,0 +43,2 @@ CERTIFICATE_READINESS_SUMMARY_MARKDOWN_FILENAME = "certificate_readiness_summary +PRE_RUN_READINESS_GATE_MARKDOWN_FILENAME = "pre_run_readiness_gate.md"
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -59,0 +64 @@ RECOGNITION_READINESS_SUMMARY_FILENAMES = ( +    CERTIFICATE_LIFECYCLE_SUMMARY_FILENAME,

### `709a314313fb4d5014edde11e6b27fbf52490e02`
- 时间: 2026-04-10 14:38:32 +0800
- 标题: chore: sync 2026-04-10 14:38:30
- 涉及文件: `src/gas_calibrator/v2/review_surface_formatter.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -960,262 +959,0 @@ def build_measurement_review_digest_lines(payload: dict[str, Any]) -> dict[str, -def build_readiness_review_digest_lines(payload: dict[str, Any]) -> dict[str, list[str]]:
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -960,262 +959,0 @@ def build_measurement_review_digest_lines(payload: dict[str, Any]) -> dict[str, -    raw = dict(payload.get("raw") or payload or {})
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -960,262 +959,0 @@ def build_measurement_review_digest_lines(payload: dict[str, Any]) -> dict[str, -    digest = dict(raw.get("digest") or payload.get("digest") or {})
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -960,262 +959,0 @@ def build_measurement_review_digest_lines(payload: dict[str, Any]) -> dict[str, -    title = str(dict(raw.get("review_surface") or payload.get("review_surface") or {}).get("title_text") or raw.get("artifact_type") or "--")
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -960,262 +959,0 @@ def build_measurement_review_digest_lines(payload: dict[str, Any]) -> dict[str, -    phase_rows = [

### `c83eef2d164b0a55fbaba04d72ca8e372623cf1c`
- 时间: 2026-04-10 14:28:32 +0800
- 标题: chore: sync 2026-04-10 14:28:31
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1267 +1267 @@ class ResultsGateway: -            lines.extend((localized_lines.get("detail_lines") or [])[:3])
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1267 +1267 @@ class ResultsGateway: +            lines.extend((localized_lines.get("detail_lines") or [])[:5])

### `71c2cdff0bf35f252d270c542cf28e45186fcbc0`
- 时间: 2026-04-10 14:23:33 +0800
- 标题: chore: sync 2026-04-10 14:23:32
- 涉及文件: `src/gas_calibrator/v2/core/recognition_scope_repository.py`, `src/gas_calibrator/v2/review_surface_formatter.py`, `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`, `tests/v2/test_build_offline_governance_artifacts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, mode, protocol, step, store
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/recognition_scope_repository.py @@ -213,0 +214,74 @@ class FileBackedRecognitionScopeRepository: +        default_digest = {
  - src/gas_calibrator/v2/core/recognition_scope_repository.py @@ -213,0 +214,74 @@ class FileBackedRecognitionScopeRepository: +            "summary": str(
  - src/gas_calibrator/v2/core/recognition_scope_repository.py @@ -213,0 +214,74 @@ class FileBackedRecognitionScopeRepository: +                dict(payload.get("digest") or {}).get("summary")
  - src/gas_calibrator/v2/core/recognition_scope_repository.py @@ -213,0 +214,74 @@ class FileBackedRecognitionScopeRepository: +                or dict(payload.get("scope_overview") or {}).get("summary")
  - src/gas_calibrator/v2/core/recognition_scope_repository.py @@ -213,0 +214,74 @@ class FileBackedRecognitionScopeRepository: +                or dict(payload.get("decision_rule_overview") or {}).get("summary")

### `7db9ac1bbcabf651d80754584ce8db34ff9e3d25`
- 时间: 2026-04-10 14:13:33 +0800
- 标题: chore: sync 2026-04-10 14:13:31
- 涉及文件: `src/gas_calibrator/v2/ui_v2/locales/en_US.json`, `tests/v2/test_build_offline_governance_artifacts.py`, `tests/v2/test_historical_artifacts_cli.py`, `tests/v2/test_recognition_scope_repository.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_device_workbench.py`, `tests/v2/test_ui_v2_review_center.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: MODE, cali, db, insert, mode, protocol, report, store
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/locales/en_US.json @@ -1585 +1585,7 @@ -      "artifact_compatibility_non_claim": "Compatibility non-claim: {value}"
  - src/gas_calibrator/v2/ui_v2/locales/en_US.json @@ -1585 +1585,7 @@ +      "artifact_compatibility_non_claim": "Compatibility non-claim: {value}",
  - src/gas_calibrator/v2/ui_v2/locales/en_US.json @@ -1585 +1585,7 @@ +      "scope_package": "Scope package: {value}",
  - src/gas_calibrator/v2/ui_v2/locales/en_US.json @@ -1585 +1585,7 @@ +      "decision_rule_profile": "Decision rule profile: {value}",
  - src/gas_calibrator/v2/ui_v2/locales/en_US.json @@ -1585 +1585,7 @@ +      "conformity_boundary": "Conformity boundary: {value}",

### `34898a382ff2ea6c4f661129ed33ad267c5edbd7`
- 时间: 2026-04-10 14:08:33 +0800
- 标题: chore: sync 2026-04-10 14:08:33
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`, `src/gas_calibrator/v2/core/recognition_scope_repository.py`, `src/gas_calibrator/v2/review_surface_formatter.py`, `src/gas_calibrator/v2/scripts/historical_artifacts.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`, `src/gas_calibrator/v2/ui_v2/locales/zh_CN.json`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: DB, MODE, Step, cali, db, mode, report, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -400,0 +401,2 @@ class ResultsGateway: +        scope_definition_pack = dict(payload.get("scope_definition_pack", {}) or {})
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -400,0 +401,2 @@ class ResultsGateway: +        decision_rule_profile = dict(payload.get("decision_rule_profile", {}) or {})
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -409,0 +412 @@ class ResultsGateway: +        recognition_scope_rollup = dict(payload.get("recognition_scope_rollup", {}) or {})
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -634,0 +638,8 @@ class ResultsGateway: +            row = self._decorate_scope_definition_pack_row(
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -634,0 +638,8 @@ class ResultsGateway: +                row,

### `2c0db55400ba54906c342122404047a5ebe9078c`
- 时间: 2026-04-10 13:58:31 +0800
- 标题: chore: sync 2026-04-10 13:58:31
- 涉及文件: `src/gas_calibrator/v2/adapters/recognition_scope_gateway.py`, `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/artifact_compatibility.py`, `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`, `src/gas_calibrator/v2/core/recognition_scope_repository.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: DB, MODE, Protocol, Step, cali, db, mode, protocol
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/recognition_scope_gateway.py @@ -0,0 +1,34 @@ +from __future__ import annotations
  - src/gas_calibrator/v2/adapters/recognition_scope_gateway.py @@ -0,0 +1,34 @@ +
  - src/gas_calibrator/v2/adapters/recognition_scope_gateway.py @@ -0,0 +1,34 @@ +from pathlib import Path
  - src/gas_calibrator/v2/adapters/recognition_scope_gateway.py @@ -0,0 +1,34 @@ +from typing import Any
  - src/gas_calibrator/v2/adapters/recognition_scope_gateway.py @@ -0,0 +1,34 @@ +from ..core.recognition_scope_repository import (

### `72d513493ec06a7bf3301a57821deeb1bab005f8`
- 时间: 2026-04-10 13:48:35 +0800
- 标题: chore: sync 2026-04-10 13:48:31
- 涉及文件: `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, H2O, Step, cali, co2, h2o, mode, point
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -282,0 +283,6 @@ def build_recognition_readiness_artifacts( +        "gas_or_humidity_range": {
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -282,0 +283,6 @@ def build_recognition_readiness_artifacts( +            "co2_ppm_range": _range_text(_collect_numeric_values(sample_rows, point_rows, "co2_ppm")),
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -282,0 +283,6 @@ def build_recognition_readiness_artifacts( +            "h2o_mmol_range": _range_text(_collect_numeric_values(sample_rows, point_rows, "h2o_mmol")),
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -282,0 +283,6 @@ def build_recognition_readiness_artifacts( +            "humidity_pct_range": _range_text(_collect_numeric_values(sample_rows, point_rows, "humidity_pct")),
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -282,0 +283,6 @@ def build_recognition_readiness_artifacts( +            "dew_point_c_range": _range_text(_collect_numeric_values(sample_rows, point_rows, "dew_point_c")),

### `56b3064355c95c615243b061ba8a27583b5d51b3`
- 时间: 2026-04-10 13:28:32 +0800
- 标题: chore: sync 2026-04-10 13:28:32
- 涉及文件: `tests/v2/test_historical_artifacts_cli.py`
- 判定原因: diff 内容命中关键词: report
- 关键 diff hunk 摘要:
  - tests/v2/test_historical_artifacts_cli.py @@ -109 +109 @@ def test_historical_scan_supports_single_run_dir_and_root_dir(tmp_path: Path, ca -    assert batch_report["compatibility_rollup"]["regenerate_recommended_count"] == 1
  - tests/v2/test_historical_artifacts_cli.py @@ -109 +109 @@ def test_historical_scan_supports_single_run_dir_and_root_dir(tmp_path: Path, ca +    assert batch_report["compatibility_rollup"]["regenerate_recommended_count"] == 2

### `684377f49f48e3aa070f80b4701e916621b51187`
- 时间: 2026-04-10 13:23:31 +0800
- 标题: chore: sync 2026-04-10 13:23:30
- 涉及文件: `src/gas_calibrator/v2/core/artifact_compatibility.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `tests/v2/test_historical_artifacts_cli.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_device_workbench.py`, `tests/v2/test_ui_v2_review_center.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, mode, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -1784,8 +1784 @@ def build_artifact_compatibility_rollup( -        if not bool(run.get("regenerate_recommended", False))
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -1784,8 +1784 @@ def build_artifact_compatibility_rollup( -        and str(run.get("current_reader_mode") or "").strip() == "canonical_direct"
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -1784,8 +1784 @@ def build_artifact_compatibility_rollup( -    )
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -1784,8 +1784 @@ def build_artifact_compatibility_rollup( -    legacy_run_count = sum(
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -1784,8 +1784 @@ def build_artifact_compatibility_rollup( -        1

### `d28048e695da64f91d277459a8807b2aaa28c26b`
- 时间: 2026-04-10 13:18:34 +0800
- 标题: chore: sync 2026-04-10 13:18:34
- 涉及文件: `src/gas_calibrator/config.py`, `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`, `src/gas_calibrator/v2/ui_v2/locales/en_US.json`, `src/gas_calibrator/v2/ui_v2/locales/zh_CN.json`, `src/gas_calibrator/workflow/runner.py`, `tests/test_config_runtime_defaults.py`, `tests/test_runner_h2o_sequence.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, cali, co2, h2o, point, span
- 关键 diff hunk 摘要:
  - src/gas_calibrator/config.py @@ -197,2 +197,2 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { -            "gas_route_dewpoint_gate_tail_span_max_c": 0.35,
  - src/gas_calibrator/config.py @@ -197,2 +197,2 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { -            "gas_route_dewpoint_gate_tail_slope_abs_max_c_per_s": 0.003,
  - src/gas_calibrator/config.py @@ -197,2 +197,2 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { +            "gas_route_dewpoint_gate_tail_span_max_c": 0.45,
  - src/gas_calibrator/config.py @@ -197,2 +197,2 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { +            "gas_route_dewpoint_gate_tail_slope_abs_max_c_per_s": 0.005,
  - src/gas_calibrator/config.py @@ -200 +200 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { -            "gas_route_dewpoint_gate_rebound_min_rise_c": 1.0,

### `c59949d1dff37da92024da305f8300c333b59c00`
- 时间: 2026-04-10 13:13:33 +0800
- 标题: chore: sync 2026-04-10 13:13:30
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, mode
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1698,0 +1699,97 @@ class ResultsGateway: +
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1698,0 +1699,97 @@ class ResultsGateway: +def _results_gateway_decorate_artifact_compatibility_row(
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1698,0 +1699,97 @@ class ResultsGateway: +    row: dict[str, Any],
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1698,0 +1699,97 @@ class ResultsGateway: +    *,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1698,0 +1699,97 @@ class ResultsGateway: +    compatibility_lookup: dict[str, dict[str, Any]],

### `f4b983a06bd3ea266f31c4b799a74c6007239cb0`
- 时间: 2026-04-10 13:08:40 +0800
- 标题: chore: sync 2026-04-10 13:08:38
- 涉及文件: `src/gas_calibrator/config.py`, `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/artifact_compatibility.py`, `src/gas_calibrator/v2/scripts/historical_artifacts.py`, `src/gas_calibrator/workflow/runner.py`, `tests/test_config_runtime_defaults.py`, `tests/test_runner_h2o_sequence.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, H2O, Point, cali, co2, h2o, mode, point
- 关键 diff hunk 摘要:
  - src/gas_calibrator/config.py @@ -193 +193 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { -            "gas_route_dewpoint_gate_policy": "reject",
  - src/gas_calibrator/config.py @@ -193 +193 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { +            "gas_route_dewpoint_gate_policy": "warn",
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -264,0 +265,5 @@ class ResultsGateway: +        compatibility_rollup = dict(
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -264,0 +265,5 @@ class ResultsGateway: +            compatibility_scan_summary.get("compatibility_rollup")
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -264,0 +265,5 @@ class ResultsGateway: +            or compatibility_overview.get("compatibility_rollup")

### `9d9b5602c3c3a87713b49947b23b8775016445a1`
- 时间: 2026-04-10 13:03:36 +0800
- 标题: chore: sync 2026-04-10 13:03:34
- 涉及文件: `src/gas_calibrator/v2/core/artifact_compatibility.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, mode, report, step, v1
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -33,0 +34,5 @@ ARTIFACT_COMPATIBILITY_SCHEMA_VERSION = "step2-artifact-compatibility-v1" +ARTIFACT_COMPATIBILITY_INDEX_SCHEMA_VERSION = "step2-artifact-compatibility-index-v1"
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -33,0 +34,5 @@ ARTIFACT_COMPATIBILITY_SCHEMA_VERSION = "step2-artifact-compatibility-v1" +ARTIFACT_COMPATIBILITY_BUNDLE_TOOL = (
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -33,0 +34,5 @@ ARTIFACT_COMPATIBILITY_SCHEMA_VERSION = "step2-artifact-compatibility-v1" +    "gas_calibrator.v2.core.artifact_compatibility.build_artifact_compatibility_bundle"
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -33,0 +34,5 @@ ARTIFACT_COMPATIBILITY_SCHEMA_VERSION = "step2-artifact-compatibility-v1" +)
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -33,0 +34,5 @@ ARTIFACT_COMPATIBILITY_SCHEMA_VERSION = "step2-artifact-compatibility-v1" +HISTORICAL_ARTIFACT_ROLLUP_TOOL = "gas_calibrator.v2.scripts.historical_artifacts"

### `32341a14d642091529a1739596762545eff8388a`
- 时间: 2026-04-10 12:53:32 +0800
- 标题: chore: sync 2026-04-10 12:53:31
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/artifact_compatibility.py`, `src/gas_calibrator/v2/scripts/_cli_safety.py`, `src/gas_calibrator/v2/scripts/historical_artifacts.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`, `tests/v2/test_historical_artifacts_cli.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_run_v2.py`, `tests/v2/test_test_v2_safe.py`, `tests/v2/test_ui_v2_device_workbench.py`, `tests/v2/test_ui_v2_review_center.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Step, cali, db, insert, mode, point, report, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -263,0 +264 @@ class ResultsGateway: +        compatibility_overview = dict(compatibility_scan_summary.get("compatibility_overview") or {})
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -351,0 +353 @@ class ResultsGateway: +            "compatibility_overview": compatibility_overview,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -379,0 +382 @@ class ResultsGateway: +        compatibility_overview = dict(payload.get("compatibility_overview", {}) or {})
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -647,0 +651 @@ class ResultsGateway: +            "compatibility_overview": compatibility_overview,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -768,0 +773 @@ class ResultsGateway: +        compatibility_overview = dict(compatibility_summary.get("compatibility_overview") or {})

### `3882814aa4a2b19dfe30e8fd6e0495cf04a9dd44`
- 时间: 2026-04-10 10:23:33 +0800
- 标题: chore: sync 2026-04-10 10:23:30
- 涉及文件: `src/gas_calibrator/v2/core/offline_artifacts.py`, `tests/v2/test_result_store.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_run_v2.py`, `tests/v2/test_test_v2_device.py`, `tests/v2/test_test_v2_safe.py`, `tests/v2/test_ui_v2_device_workbench.py`, `tests/v2/test_ui_v2_review_center.py`, `tests/v2/test_ui_v2_review_center_index.py`, `tests/v2/test_ui_v2_workbench_evidence.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Step, cali, coefficient, mode, point, report, step, store
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/offline_artifacts.py @@ -1563,0 +1564,21 @@ def export_run_offline_artifacts( +    compatibility_output_files = [
  - src/gas_calibrator/v2/core/offline_artifacts.py @@ -1563,0 +1564,21 @@ def export_run_offline_artifacts( +        str(acceptance_path),
  - src/gas_calibrator/v2/core/offline_artifacts.py @@ -1563,0 +1564,21 @@ def export_run_offline_artifacts( +        str(analytics_path),
  - src/gas_calibrator/v2/core/offline_artifacts.py @@ -1563,0 +1564,21 @@ def export_run_offline_artifacts( +        str(lineage_path),
  - src/gas_calibrator/v2/core/offline_artifacts.py @@ -1563,0 +1564,21 @@ def export_run_offline_artifacts( +        str(trend_path),

### `9f25721abd00d28485e1530838d77cc71151dcda`
- 时间: 2026-04-10 10:18:33 +0800
- 标题: chore: sync 2026-04-10 10:18:32
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/scripts/_cli_safety.py`, `src/gas_calibrator/v2/scripts/run_v2.py`, `src/gas_calibrator/v2/scripts/test_v2_device.py`, `src/gas_calibrator/v2/scripts/test_v2_safe.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: REPORT, Step, cali, calibration, db, mode, protocol, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -749,0 +750 @@ class ResultsGateway: +        compatibility_scan_summary: dict[str, Any] | None,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -766,0 +768 @@ class ResultsGateway: +        compatibility_summary = dict(compatibility_scan_summary or {})
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -903,0 +906,34 @@ class ResultsGateway: +        if compatibility_summary:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -903,0 +906,34 @@ class ResultsGateway: +            compatibility_reader_mode = str(
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -903,0 +906,34 @@ class ResultsGateway: +                compatibility_summary.get("current_reader_mode_display")

### `d9f73e2e73e9fdfb3fefa10ad5de53c5da37deb4`
- 时间: 2026-04-10 10:13:34 +0800
- 标题: chore: sync 2026-04-10 10:13:32
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/artifact_catalog.py`, `src/gas_calibrator/v2/core/artifact_compatibility.py`, `src/gas_calibrator/v2/core/offline_artifacts.py`, `src/gas_calibrator/v2/core/result_store.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: MODE, REPORT, Step, Store, cali, coefficient, mode, protocol
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -31,0 +32,11 @@ from ..core.measurement_phase_coverage import ( +from ..core.artifact_compatibility import (
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -31,0 +32,11 @@ from ..core.measurement_phase_coverage import ( +    ARTIFACT_CONTRACT_CATALOG_FILENAME,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -31,0 +32,11 @@ from ..core.measurement_phase_coverage import ( +    ARTIFACT_CONTRACT_CATALOG_MARKDOWN_FILENAME,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -31,0 +32,11 @@ from ..core.measurement_phase_coverage import ( +    COMPATIBILITY_SCAN_SUMMARY_FILENAME,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -31,0 +32,11 @@ from ..core.measurement_phase_coverage import ( +    COMPATIBILITY_SCAN_SUMMARY_MARKDOWN_FILENAME,

### `90e7a01062247eec1744c6736566cd4f333610e3`
- 时间: 2026-04-10 10:08:30 +0800
- 标题: chore: sync 2026-04-10 10:08:30
- 涉及文件: `src/gas_calibrator/v2/core/artifact_compatibility.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: MODE, REPORT, Step, cali, mode, report, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -1,1113 +0,0 @@ -from __future__ import annotations
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -1,1113 +0,0 @@ -
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -1,1113 +0,0 @@ -from datetime import datetime
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -1,1113 +0,0 @@ -import json
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -1,1113 +0,0 @@ -from pathlib import Path

### `bc36f5d94047692a57933e707c310260f4e0f139`
- 时间: 2026-04-10 09:58:32 +0800
- 标题: chore: sync 2026-04-10 09:58:30
- 涉及文件: `src/gas_calibrator/v2/core/artifact_compatibility.py`, `src/gas_calibrator/v2/src/gas_calibrator/v2/core/artifact_compatibility.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: MODE, REPORT, Step, cali, mode, report, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -631,0 +632,482 @@ def _build_reindex_manifest_payload( +
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -631,0 +632,482 @@ def _build_reindex_manifest_payload( +def _build_base_entry(
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -631,0 +632,482 @@ def _build_reindex_manifest_payload( +    path: Path,
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -631,0 +632,482 @@ def _build_reindex_manifest_payload( +    *,
  - src/gas_calibrator/v2/core/artifact_compatibility.py @@ -631,0 +632,482 @@ def _build_reindex_manifest_payload( +    run_dir: Path,

### `efd74b74f949fce5208458222935b5f7fd99f3ee`
- 时间: 2026-04-10 09:53:35 +0800
- 标题: chore: sync 2026-04-10 09:53:34
- 涉及文件: `src/gas_calibrator/v2/src/gas_calibrator/v2/core/artifact_compatibility.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: MODE, REPORT, cali, mode
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/src/gas_calibrator/v2/core/artifact_compatibility.py @@ -0,0 +1,631 @@ +from __future__ import annotations
  - src/gas_calibrator/v2/src/gas_calibrator/v2/core/artifact_compatibility.py @@ -0,0 +1,631 @@ +
  - src/gas_calibrator/v2/src/gas_calibrator/v2/core/artifact_compatibility.py @@ -0,0 +1,631 @@ +from datetime import datetime
  - src/gas_calibrator/v2/src/gas_calibrator/v2/core/artifact_compatibility.py @@ -0,0 +1,631 @@ +import json
  - src/gas_calibrator/v2/src/gas_calibrator/v2/core/artifact_compatibility.py @@ -0,0 +1,631 @@ +from pathlib import Path

### `745196ccf2b37b777f03dc41f4d4a788c8357c5b`
- 时间: 2026-04-10 08:38:32 +0800
- 标题: chore: sync 2026-04-10 08:38:31
- 涉及文件: `src/gas_calibrator/workflow/runner.py`, `tests/test_runner_corrected_delivery_hooks.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, cali, calibration, delivery, point, 校准
- 关键 diff hunk 摘要:
  - src/gas_calibrator/workflow/runner.py @@ -4586,11 +4586,6 @@ class CalibrationRunner: -            if self._skip_startup_pressure_work_for_current_selection():
  - src/gas_calibrator/workflow/runner.py @@ -4586,11 +4586,6 @@ class CalibrationRunner: -                self.set_status("初始化：跳过启动压力工作（当前大气压）")
  - src/gas_calibrator/workflow/runner.py @@ -4586,11 +4586,6 @@ class CalibrationRunner: -                self._emit_stage_event(current="初始化", wait_reason="跳过启动压力工作（当前大气压）")
  - src/gas_calibrator/workflow/runner.py @@ -4586,11 +4586,6 @@ class CalibrationRunner: -                self.log("Startup pressure work skipped: ambient-only pressure selection")
  - src/gas_calibrator/workflow/runner.py @@ -4586,11 +4586,6 @@ class CalibrationRunner: -            else:

### `3d0b105229525b4a9f033d1b794be8a4f19016f9`
- 时间: 2026-04-09 17:58:30 +0800
- 标题: chore: sync 2026-04-09 17:58:29
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -212,3 +212 @@ def _build_fragment_filter_options( -                item.get("label")
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -212,3 +212 @@ def _build_fragment_filter_options( -                or item.get("display_text")
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -212,3 +212 @@ def _build_fragment_filter_options( -                or display_fragment_value(
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -212,3 +212 @@ def _build_fragment_filter_options( +                display_fragment_value(
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -219,0 +218,2 @@ def _build_fragment_filter_options( +                or item.get("label")

### `1ece45d81050f270ec5bc45543b41484f3fdd41d`
- 时间: 2026-04-09 17:53:35 +0800
- 标题: chore: sync 2026-04-09 17:53:29
- 涉及文件: `src/gas_calibrator/v2/core/reviewer_fragments_contract.py`, `src/gas_calibrator/v2/ui_v2/locales/en_US.json`, `tests/v2/test_results_gateway.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/reviewer_fragments_contract.py @@ -27,4 +27,4 @@ _MEASUREMENT_LAYER_LABELS = { -    "reference": {"zh_CN": "\u53c2\u8003\u5c42", "en_US": "reference"},
  - src/gas_calibrator/v2/core/reviewer_fragments_contract.py @@ -27,4 +27,4 @@ _MEASUREMENT_LAYER_LABELS = { -    "analyzer_raw": {"zh_CN": "\u5206\u6790\u4eea\u539f\u59cb\u5c42", "en_US": "analyzer raw"},
  - src/gas_calibrator/v2/core/reviewer_fragments_contract.py @@ -27,4 +27,4 @@ _MEASUREMENT_LAYER_LABELS = { -    "output": {"zh_CN": "\u8f93\u51fa\u5c42", "en_US": "output"},
  - src/gas_calibrator/v2/core/reviewer_fragments_contract.py @@ -27,4 +27,4 @@ _MEASUREMENT_LAYER_LABELS = { -    "data_quality": {"zh_CN": "\u6570\u636e\u8d28\u91cf\u5c42", "en_US": "data quality"},
  - src/gas_calibrator/v2/core/reviewer_fragments_contract.py @@ -27,4 +27,4 @@ _MEASUREMENT_LAYER_LABELS = { +    "reference": {"zh_CN": "\u53c2\u8003\u5c42", "en_US": "reference layer"},

### `583a0de27cdeed04f48bce06b8315f784c077423`
- 时间: 2026-04-09 17:48:34 +0800
- 标题: chore: sync 2026-04-09 17:48:32
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/locales/zh_CN.json`, `src/gas_calibrator/v2/ui_v2/review_center_presenter.py`, `tests/v2/test_phase_taxonomy_contract.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_results_page.py`, `tests/v2/test_ui_v2_review_center.py`, `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Step, cali, report, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -358,3 +358,5 @@ def _build_measurement_core_filter_options(items: list[dict[str, Any]]) -> dict[ -        "boundary_options": [
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -358,3 +358,5 @@ def _build_measurement_core_filter_options(items: list[dict[str, Any]]) -> dict[ -            {"id": "all", "label": t("results.review_center.filter.all_boundaries", default="鍏ㄩ儴杈圭晫")}
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -358,3 +358,5 @@ def _build_measurement_core_filter_options(items: list[dict[str, Any]]) -> dict[ -        ] + [{"id": value, "label": humanize_review_surface_text(value)} for value in boundary_values],
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -358,3 +358,5 @@ def _build_measurement_core_filter_options(items: list[dict[str, Any]]) -> dict[ +        "boundary_options": _build_fragment_filter_options(
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -358,3 +358,5 @@ def _build_measurement_core_filter_options(items: list[dict[str, Any]]) -> dict[ +            boundary_rows,

### `00a46e7ee8773b2b6131233552e549d5cedf6f9c`
- 时间: 2026-04-09 17:43:33 +0800
- 标题: chore: sync 2026-04-09 17:43:30
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`, `src/gas_calibrator/v2/core/reviewer_fragments_contract.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: REPORT, cali, db
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -66,0 +67 @@ from ..review_surface_formatter import ( +    collect_boundary_digest_lines,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1332,5 +1333 @@ class ResultsGateway: -        boundary_summary = " | ".join(
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1332,5 +1333 @@ class ResultsGateway: -            str(item).strip()
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1332,5 +1333 @@ class ResultsGateway: -            for item in list(review_surface.get("boundary_filters") or evidence_payload.get("boundary_statements") or [])
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1332,5 +1333 @@ class ResultsGateway: -            if str(item).strip()

### `a3719c3485a7a81e5619e78101b9fe8b91901114`
- 时间: 2026-04-09 17:38:32 +0800
- 标题: chore: sync 2026-04-09 17:38:31
- 涉及文件: `src/gas_calibrator/v2/core/measurement_phase_coverage.py`, `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`, `src/gas_calibrator/v2/core/reviewer_fragments_contract.py`, `src/gas_calibrator/v2/ui_v2/i18n.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, db, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -8,0 +9 @@ from .phase_taxonomy_contract import ( +    METHOD_CONFIRMATION_FAMILY,
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -9,0 +11,2 @@ from .phase_taxonomy_contract import ( +    TRACEABILITY_NODE_FAMILY,
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -9,0 +11,2 @@ from .phase_taxonomy_contract import ( +    UNCERTAINTY_INPUT_FAMILY,
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -29,0 +33 @@ from .reviewer_fragments_contract import ( +    fragment_filter_rows_to_ids,
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -32,0 +37 @@ from .reviewer_fragments_contract import ( +    normalize_fragment_filter_rows,

### `22fb2922c8994314cfed5cb20be82f03b9d7a249`
- 时间: 2026-04-09 17:33:29 +0800
- 标题: chore: sync 2026-04-09 17:33:29
- 涉及文件: `src/gas_calibrator/v2/core/reviewer_fragments_contract.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/reviewer_fragments_contract.py @@ -25,0 +26,7 @@ _TOKEN_RE = re.compile(r"[^0-9a-z\u4e00-\u9fff]+") +_MEASUREMENT_LAYER_LABELS = {
  - src/gas_calibrator/v2/core/reviewer_fragments_contract.py @@ -25,0 +26,7 @@ _TOKEN_RE = re.compile(r"[^0-9a-z\u4e00-\u9fff]+") +    "reference": {"zh_CN": "\u53c2\u8003\u5c42", "en_US": "reference"},
  - src/gas_calibrator/v2/core/reviewer_fragments_contract.py @@ -25,0 +26,7 @@ _TOKEN_RE = re.compile(r"[^0-9a-z\u4e00-\u9fff]+") +    "analyzer_raw": {"zh_CN": "\u5206\u6790\u4eea\u539f\u59cb\u5c42", "en_US": "analyzer raw"},
  - src/gas_calibrator/v2/core/reviewer_fragments_contract.py @@ -25,0 +26,7 @@ _TOKEN_RE = re.compile(r"[^0-9a-z\u4e00-\u9fff]+") +    "output": {"zh_CN": "\u8f93\u51fa\u5c42", "en_US": "output"},
  - src/gas_calibrator/v2/core/reviewer_fragments_contract.py @@ -25,0 +26,7 @@ _TOKEN_RE = re.compile(r"[^0-9a-z\u4e00-\u9fff]+") +    "data_quality": {"zh_CN": "\u6570\u636e\u8d28\u91cf\u5c42", "en_US": "data quality"},

### `bb887b19a9a7020d7dd69f87980a1b15ac1307de`
- 时间: 2026-04-09 16:58:34 +0800
- 标题: chore: sync 2026-04-09 16:58:31
- 涉及文件: `tests/v2/test_phase_taxonomy_contract.py`, `tests/v2/test_results_gateway.py`
- 判定原因: diff 内容命中关键词: cali, db, report
- 关键 diff hunk 摘要:
  - tests/v2/test_phase_taxonomy_contract.py @@ -393,0 +394,11 @@ def test_taxonomy_contract_preserves_partial_complete_and_payload_backed_phase_d +    assert "preseal_partial_vs_pressure_stable_complete" in list(report["raw"].get("phase_contrast_fragment_keys") or [])
  - tests/v2/test_phase_taxonomy_contract.py @@ -393,0 +394,11 @@ def test_taxonomy_contract_preserves_partial_complete_and_payload_backed_phase_d +        report["raw"].get("phase_contrast_fragment_keys") or []
  - tests/v2/test_phase_taxonomy_contract.py @@ -393,0 +394,11 @@ def test_taxonomy_contract_preserves_partial_complete_and_payload_backed_phase_d +    localized_lines = build_measurement_review_digest_lines(report)
  - tests/v2/test_phase_taxonomy_contract.py @@ -393,0 +394,11 @@ def test_taxonomy_contract_preserves_partial_complete_and_payload_backed_phase_d +    localized_text = "\n".join(
  - tests/v2/test_phase_taxonomy_contract.py @@ -393,0 +394,11 @@ def test_taxonomy_contract_preserves_partial_complete_and_payload_backed_phase_d +        list(localized_lines.get("summary_lines") or []) + list(localized_lines.get("detail_lines") or [])

### `94f149ab8977e51ce74cca72828c4baf930d6b77`
- 时间: 2026-04-09 16:53:30 +0800
- 标题: chore: sync 2026-04-09 16:53:30
- 涉及文件: `tests/v2/test_phase_taxonomy_contract.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_workbench_evidence.py`
- 判定原因: diff 内容命中关键词: STEP, cali, db, report, step, store
- 关键 diff hunk 摘要:
  - tests/v2/test_phase_taxonomy_contract.py @@ -21,0 +22 @@ from gas_calibrator.v2.core.phase_taxonomy_contract import ( +    REVIEWER_NEXT_STEP_TEMPLATE_FAMILY,
  - tests/v2/test_phase_taxonomy_contract.py @@ -47 +52 @@ from gas_calibrator.v2.scripts.build_offline_governance_artifacts import rebuild -from gas_calibrator.v2.ui_v2.i18n import display_fragment_value, display_taxonomy_value
  - tests/v2/test_phase_taxonomy_contract.py @@ -47 +52 @@ from gas_calibrator.v2.scripts.build_offline_governance_artifacts import rebuild +from gas_calibrator.v2.ui_v2.i18n import display_fragment_value, display_taxonomy_value, set_locale, t
  - tests/v2/test_phase_taxonomy_contract.py @@ -206,0 +224,15 @@ def test_reviewer_fragments_contract_normalizes_aliases_and_labels() -> None: +        REVIEWER_NEXT_STEP_TEMPLATE_FAMILY,
  - tests/v2/test_phase_taxonomy_contract.py @@ -206,0 +224,15 @@ def test_reviewer_fragments_contract_normalizes_aliases_and_labels() -> None: +    ) == t("taxonomy.reviewer_next_step.ambient_diagnostic_trace_promotion", locale="zh_CN")

### `61dedcd83268b9c32bdc3093c0d08c4f5eea7fe5`
- 时间: 2026-04-09 16:48:32 +0800
- 标题: chore: sync 2026-04-09 16:48:30
- 涉及文件: `src/gas_calibrator/v2/review_surface_formatter.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`, `src/gas_calibrator/v2/ui_v2/locales/en_US.json`, `src/gas_calibrator/v2/ui_v2/locales/zh_CN.json`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Protocol, Step, cali, db, point, protocol, report, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -1162,0 +1163,508 @@ def build_readiness_review_digest_lines(payload: dict[str, Any]) -> dict[str, li +def build_measurement_review_digest_lines(payload: dict[str, Any]) -> dict[str, list[str]]:
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -1162,0 +1163,508 @@ def build_readiness_review_digest_lines(payload: dict[str, Any]) -> dict[str, li +    raw = dict(payload.get("raw") or payload or {})
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -1162,0 +1163,508 @@ def build_readiness_review_digest_lines(payload: dict[str, Any]) -> dict[str, li +    digest = dict(raw.get("digest") or payload.get("digest") or {})
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -1162,0 +1163,508 @@ def build_readiness_review_digest_lines(payload: dict[str, Any]) -> dict[str, li +    phase_rows = [
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -1162,0 +1163,508 @@ def build_readiness_review_digest_lines(payload: dict[str, Any]) -> dict[str, li +        _normalize_measurement_phase_row(dict(item))

### `6af323100e2304a5a1156d2244dc3e7b55c3dfbf`
- 时间: 2026-04-09 16:43:31 +0800
- 标题: chore: sync 2026-04-09 16:43:31
- 涉及文件: `src/gas_calibrator/v2/review_surface_formatter.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -537,2 +537,3 @@ def _display_boundary_summary(payload: dict[str, Any]) -> str: -        text_values=list(payload.get("boundary_statements") or []) or [payload.get("boundary_digest")],
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -537,2 +537,3 @@ def _display_boundary_summary(payload: dict[str, Any]) -> str: -        default_text=str(payload.get("boundary_digest") or ""),
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -537,2 +537,3 @@ def _display_boundary_summary(payload: dict[str, Any]) -> str: +        text_values=list(payload.get("boundary_statements") or [])
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -537,2 +537,3 @@ def _display_boundary_summary(payload: dict[str, Any]) -> str: +        or [payload.get("phase_boundary_digest") or payload.get("boundary_digest")],
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -537,2 +537,3 @@ def _display_boundary_summary(payload: dict[str, Any]) -> str: +        default_text=str(payload.get("phase_boundary_digest") or payload.get("boundary_digest") or ""),

### `41ec20d23eaaeea56171e0136f6bf135f0d2bdd1`
- 时间: 2026-04-09 16:38:33 +0800
- 标题: chore: sync 2026-04-09 16:38:30
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/measurement_phase_coverage.py`, `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`, `src/gas_calibrator/v2/review_surface_formatter.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Step, cali, db, report, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1392,0 +1393,4 @@ class ResultsGateway: +                "boundary_fragments": [dict(item) for item in list(evidence_payload.get("boundary_fragments") or []) if isinstance(item, dict)],
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1392,0 +1393,4 @@ class ResultsGateway: +                "boundary_fragment_keys": list(evidence_payload.get("boundary_fragment_keys") or []),
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1392,0 +1393,4 @@ class ResultsGateway: +                "non_claim_fragments": [dict(item) for item in list(evidence_payload.get("non_claim_fragments") or []) if isinstance(item, dict)],
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1392,0 +1393,4 @@ class ResultsGateway: +                "non_claim_fragment_keys": list(evidence_payload.get("non_claim_fragment_keys") or []),
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1417,0 +1422,7 @@ class ResultsGateway: +                "phase_contrast_fragments": [dict(item) for item in list(evidence_payload.get("phase_contrast_fragments") or []) if isinstance(item, dict)],

### `b78c37ab3addacd369e0c852c361e52f79b493cc`
- 时间: 2026-04-09 16:33:31 +0800
- 标题: chore: sync 2026-04-09 16:33:31
- 涉及文件: `src/gas_calibrator/v2/core/reviewer_fragments_contract.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: STEP, Step, cali, protocol, report, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/reviewer_fragments_contract.py @@ -19,0 +20,3 @@ REVIEWER_NEXT_STEP_FRAGMENT_FAMILY = "reviewer_next_step" +BOUNDARY_FRAGMENT_FAMILY = "boundary"
  - src/gas_calibrator/v2/core/reviewer_fragments_contract.py @@ -19,0 +20,3 @@ REVIEWER_NEXT_STEP_FRAGMENT_FAMILY = "reviewer_next_step" +NON_CLAIM_FRAGMENT_FAMILY = "non_claim"
  - src/gas_calibrator/v2/core/reviewer_fragments_contract.py @@ -19,0 +20,3 @@ REVIEWER_NEXT_STEP_FRAGMENT_FAMILY = "reviewer_next_step" +PHASE_CONTRAST_FRAGMENT_FAMILY = "phase_contrast"
  - src/gas_calibrator/v2/core/reviewer_fragments_contract.py @@ -49,0 +53,3 @@ _FRAGMENT_REGISTRY: dict[str, dict[str, dict[str, Any]]] = { +    BOUNDARY_FRAGMENT_FAMILY: {},
  - src/gas_calibrator/v2/core/reviewer_fragments_contract.py @@ -49,0 +53,3 @@ _FRAGMENT_REGISTRY: dict[str, dict[str, dict[str, Any]]] = { +    NON_CLAIM_FRAGMENT_FAMILY: {},

### `c878f89cc2d9f1e8d999c91a26ba1fc9dd8b9341`
- 时间: 2026-04-09 16:13:32 +0800
- 标题: chore: sync 2026-04-09 16:13:30
- 涉及文件: `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`, `tests/v2/test_measurement_phase_coverage_report.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, db, report, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -1728,0 +1729,3 @@ def _enrich_recognition_readiness_artifact( +    raw["linked_gap_reason_fragment_keys"] = linked_gap_reason_fragment_keys
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -1728,0 +1729,3 @@ def _enrich_recognition_readiness_artifact( +    raw["linked_blocker_fragment_keys"] = linked_blocker_fragment_keys
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -1728,0 +1729,3 @@ def _enrich_recognition_readiness_artifact( +    raw["linked_reviewer_next_step_fragment_keys"] = linked_reviewer_next_step_fragment_keys
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -1734 +1737 @@ def _enrich_recognition_readiness_artifact( -    raw["gap_reason_fragment_keys"] = fragment_rows_to_keys(gap_reason_fragments)
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -1734 +1737 @@ def _enrich_recognition_readiness_artifact( +    raw["gap_reason_fragment_keys"] = fragment_rows_to_keys(gap_reason_fragments) or linked_gap_reason_fragment_keys

### `3b85177da3ae306e43f74dd5cfe91a59dcc170f1`
- 时间: 2026-04-09 16:08:37 +0800
- 标题: chore: sync 2026-04-09 16:08:36
- 涉及文件: `src/gas_calibrator/v2/core/measurement_phase_coverage.py`, `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`, `src/gas_calibrator/v2/core/reviewer_fragments_contract.py`, `src/gas_calibrator/v2/review_surface_formatter.py`, `src/gas_calibrator/v2/ui_v2/i18n.py`, `tests/v2/test_phase_taxonomy_contract.py`, `tests/v2/test_results_gateway.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: STEP, cali, report, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -1101 +1100,0 @@ def _build_phase_row( -        phase_name=phase_name,
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -1570,3 +1570,5 @@ def _enrich_recognition_readiness_artifact( -        f"{str(item.get('route_phase') or '--')}: {fragment_summary(item.get('blocker_fragments') or [], default=' | '.join(list(item.get('blockers') or [])) or '--')}"
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -1570,3 +1570,5 @@ def _enrich_recognition_readiness_artifact( -        for item in linked_measurement_gaps
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -1570,3 +1570,5 @@ def _enrich_recognition_readiness_artifact( -        if list(item.get("blocker_fragments") or []) or list(item.get("blockers") or [])
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -1570,3 +1570,5 @@ def _enrich_recognition_readiness_artifact( +        [

### `b135e7f1bad27a039abbbf9504507a97db7a13c9`
- 时间: 2026-04-09 16:03:34 +0800
- 标题: chore: sync 2026-04-09 16:03:32
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/measurement_phase_coverage.py`, `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`, `src/gas_calibrator/v2/core/reviewer_fragments_contract.py`, `src/gas_calibrator/v2/review_surface_formatter.py`, `src/gas_calibrator/v2/ui_v2/i18n.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: STEP, cali, db, mode, report, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1361,0 +1362,3 @@ class ResultsGateway: +                "reviewer_fragments_contract_version": str(
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1361,0 +1362,3 @@ class ResultsGateway: +                    evidence_payload.get("reviewer_fragments_contract_version") or ""
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1361,0 +1362,3 @@ class ResultsGateway: +                ),
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1384,0 +1388,2 @@ class ResultsGateway: +                "blocker_fragments": [dict(item) for item in list(evidence_payload.get("blocker_fragments") or []) if isinstance(item, dict)],
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1384,0 +1388,2 @@ class ResultsGateway: +                "blocker_fragment_keys": list(evidence_payload.get("blocker_fragment_keys") or []),

### `951dfb823a74b79e7f8dea51e036a0b6824a6f29`
- 时间: 2026-04-09 15:58:33 +0800
- 标题: chore: sync 2026-04-09 15:58:33
- 涉及文件: `src/gas_calibrator/v2/core/reviewer_fragments_contract.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Protocol, STEP, Step, cali, db, mode, protocol, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/reviewer_fragments_contract.py @@ -0,0 +1,465 @@ +from __future__ import annotations
  - src/gas_calibrator/v2/core/reviewer_fragments_contract.py @@ -0,0 +1,465 @@ +
  - src/gas_calibrator/v2/core/reviewer_fragments_contract.py @@ -0,0 +1,465 @@ +from typing import Any, Iterable
  - src/gas_calibrator/v2/core/reviewer_fragments_contract.py @@ -0,0 +1,465 @@ +import re
  - src/gas_calibrator/v2/core/reviewer_fragments_contract.py @@ -0,0 +1,465 @@ +from .phase_taxonomy_contract import (

### `9056f395fbf9282dc2c44fc294dffe57fb8a8883`
- 时间: 2026-04-09 15:08:33 +0800
- 标题: chore: sync 2026-04-09 15:08:30
- 涉及文件: `src/gas_calibrator/v2/core/measurement_phase_coverage.py`, `tests/v2/test_phase_taxonomy_contract.py`, `tests/v2/test_ui_v2_device_workbench.py`, `tests/v2/test_ui_v2_workbench_evidence.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, Point, REPORT, cali, co2, db, h2o, insert
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -858,0 +859 @@ def build_measurement_phase_coverage_report( +        "taxonomy_contract_version": TAXONOMY_CONTRACT_VERSION,
  - tests/v2/test_phase_taxonomy_contract.py @@ -0,0 +1,361 @@ +from gas_calibrator.v2.adapters.results_gateway import ResultsGateway
  - tests/v2/test_phase_taxonomy_contract.py @@ -0,0 +1,361 @@ +from gas_calibrator.v2.core.measurement_phase_coverage import (
  - tests/v2/test_phase_taxonomy_contract.py @@ -0,0 +1,361 @@ +    MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME,
  - tests/v2/test_phase_taxonomy_contract.py @@ -0,0 +1,361 @@ +    MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME,

### `7eedcb3238ddd6119fb9c28c76c9d244f6db44a5`
- 时间: 2026-04-09 15:03:30 +0800
- 标题: chore: sync 2026-04-09 15:03:30
- 涉及文件: `src/gas_calibrator/v2/review_surface_formatter.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, db, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -590,0 +591 @@ def build_measurement_review_digest_lines(payload: dict[str, Any]) -> dict[str, +    gap_rows: list[dict[str, Any]] = []
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -691,0 +693,3 @@ def build_measurement_review_digest_lines(payload: dict[str, Any]) -> dict[str, +        next_artifacts_text = _display_text_list(list(row.get("next_required_artifacts") or []))
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -691,0 +693,3 @@ def build_measurement_review_digest_lines(payload: dict[str, Any]) -> dict[str, +        blockers_text = _display_text_list(list(row.get("blockers") or []))
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -691,0 +693,3 @@ def build_measurement_review_digest_lines(payload: dict[str, Any]) -> dict[str, +        reviewer_next_step_text = _display_reviewer_next_step(row)
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -718,6 +722,20 @@ def build_measurement_review_digest_lines(payload: dict[str, Any]) -> dict[str, -                method=_display_text_list(list(row.get("linked_method_confirmation_items") or [])),

### `48de09a84af372098537ad76f61429e7dea25d10`
- 时间: 2026-04-09 14:58:31 +0800
- 标题: chore: sync 2026-04-09 14:58:31
- 涉及文件: `src/gas_calibrator/v2/review_surface_formatter.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -383,0 +384,9 @@ def _dedupe_lines(lines: list[str]) -> list[str]: +def _dedupe(values: Any) -> list[str]:
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -383,0 +384,9 @@ def _dedupe_lines(lines: list[str]) -> list[str]: +    rows: list[str] = []
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -383,0 +384,9 @@ def _dedupe_lines(lines: list[str]) -> list[str]: +    for value in values:
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -383,0 +384,9 @@ def _dedupe_lines(lines: list[str]) -> list[str]: +        text = str(value or "").strip()
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -383,0 +384,9 @@ def _dedupe_lines(lines: list[str]) -> list[str]: +        if text and text not in rows:

### `a62825175c9591d4a21412fd195b34c47b0ee2c3`
- 时间: 2026-04-09 14:53:32 +0800
- 标题: chore: sync 2026-04-09 14:53:31
- 涉及文件: `src/gas_calibrator/v2/review_surface_formatter.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -577 +577,29 @@ def build_measurement_review_digest_lines(payload: dict[str, Any]) -> dict[str, -    phase_rows = [dict(item) for item in list(raw.get("phase_rows") or payload.get("phase_rows") or []) if isinstance(item, dict)]
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -577 +577,29 @@ def build_measurement_review_digest_lines(payload: dict[str, Any]) -> dict[str, +    phase_rows = [
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -577 +577,29 @@ def build_measurement_review_digest_lines(payload: dict[str, Any]) -> dict[str, +        _normalize_measurement_phase_row(dict(item))
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -577 +577,29 @@ def build_measurement_review_digest_lines(payload: dict[str, Any]) -> dict[str, +        for item in list(raw.get("phase_rows") or payload.get("phase_rows") or [])
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -577 +577,29 @@ def build_measurement_review_digest_lines(payload: dict[str, Any]) -> dict[str, +        if isinstance(item, dict)

### `3afcb3698480de1182650e968603afdd7ad5a23a`
- 时间: 2026-04-09 14:48:33 +0800
- 标题: chore: sync 2026-04-09 14:48:30
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/measurement_phase_coverage.py`, `src/gas_calibrator/v2/core/phase_taxonomy_contract.py`, `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`, `src/gas_calibrator/v2/review_surface_formatter.py`, `src/gas_calibrator/v2/ui_v2/i18n.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: STEP, Step, cali, mode, point, report, step, 气路
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1372,0 +1373,3 @@ class ResultsGateway: +                "linked_method_confirmation_item_keys": list(
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1372,0 +1373,3 @@ class ResultsGateway: +                    evidence_payload.get("linked_method_confirmation_item_keys") or []
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1372,0 +1373,3 @@ class ResultsGateway: +                ),
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1373,0 +1377 @@ class ResultsGateway: +                "linked_uncertainty_input_keys": list(evidence_payload.get("linked_uncertainty_input_keys") or []),
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1374,0 +1379 @@ class ResultsGateway: +                "linked_traceability_node_keys": list(evidence_payload.get("linked_traceability_node_keys") or []),

### `fe3aa3e688a906431838890054be2bddf461a605`
- 时间: 2026-04-09 14:43:31 +0800
- 标题: chore: sync 2026-04-09 14:43:30
- 涉及文件: `src/gas_calibrator/v2/core/phase_taxonomy_contract.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Mode, STEP, cali, mode, point, step, v1, 气路
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/phase_taxonomy_contract.py @@ -0,0 +1,899 @@ +from __future__ import annotations
  - src/gas_calibrator/v2/core/phase_taxonomy_contract.py @@ -0,0 +1,899 @@ +
  - src/gas_calibrator/v2/core/phase_taxonomy_contract.py @@ -0,0 +1,899 @@ +from typing import Any
  - src/gas_calibrator/v2/core/phase_taxonomy_contract.py @@ -0,0 +1,899 @@ +import re
  - src/gas_calibrator/v2/core/phase_taxonomy_contract.py @@ -0,0 +1,899 @@ +TAXONOMY_CONTRACT_VERSION = "step2-taxonomy-contract-v1"

### `810eeb15c635f55fd54876c0c4886f7362da1d6f`
- 时间: 2026-04-09 14:23:34 +0800
- 标题: chore: sync 2026-04-09 14:23:32
- 涉及文件: `src/gas_calibrator/v2/core/measurement_phase_coverage.py`, `tests/v2/test_build_offline_governance_artifacts.py`, `tests/v2/test_measurement_phase_coverage_report.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_review_center.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, report, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -664 +663,0 @@ def build_measurement_phase_coverage_report( -            if row.get("coverage_bucket") != _PAYLOAD_COMPLETE_BUCKET
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -692 +690,0 @@ def build_measurement_phase_coverage_report( -            and row.get("coverage_bucket") != _PAYLOAD_COMPLETE_BUCKET
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -703 +700,0 @@ def build_measurement_phase_coverage_report( -            and row.get("coverage_bucket") != _PAYLOAD_COMPLETE_BUCKET
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -714 +710,0 @@ def build_measurement_phase_coverage_report( -            and row.get("coverage_bucket") != _PAYLOAD_COMPLETE_BUCKET
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -725 +720,0 @@ def build_measurement_phase_coverage_report( -            and row.get("coverage_bucket") != _PAYLOAD_COMPLETE_BUCKET

### `bb59ba9f3b351a75ab29e72aea6d27df6001a0d5`
- 时间: 2026-04-09 14:18:33 +0800
- 标题: chore: sync 2026-04-09 14:18:30
- 涉及文件: `src/gas_calibrator/v2/core/measurement_phase_coverage.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Step, cali, mode, protocol, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -160 +160,10 @@ _PHASE_READINESS_ARTIFACT_TYPES: dict[str, tuple[str, ...]] = { -    "ambient_diagnostic": ("scope_definition_pack", "scope_readiness_summary", "decision_rule_profile"),
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -160 +160,10 @@ _PHASE_READINESS_ARTIFACT_TYPES: dict[str, tuple[str, ...]] = { +    "ambient_diagnostic": (
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -160 +160,10 @@ _PHASE_READINESS_ARTIFACT_TYPES: dict[str, tuple[str, ...]] = { +        "scope_definition_pack",
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -160 +160,10 @@ _PHASE_READINESS_ARTIFACT_TYPES: dict[str, tuple[str, ...]] = { +        "scope_readiness_summary",
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -160 +160,10 @@ _PHASE_READINESS_ARTIFACT_TYPES: dict[str, tuple[str, ...]] = { +        "decision_rule_profile",

### `781e97498e304601e31472df4b27cd8e57ff7e36`
- 时间: 2026-04-09 14:03:32 +0800
- 标题: chore: sync 2026-04-09 14:03:30
- 涉及文件: `src/gas_calibrator/v2/core/measurement_phase_coverage.py`, `tests/v2/test_ui_v2_device_workbench.py`, `tests/v2/test_ui_v2_workbench_evidence.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, db, mode
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -231 +231 @@ _PHASE_GAP_NAVIGATION_PROFILES: dict[tuple[str, str], dict[str, Any]] = { -            "while keeping preseal explicit as payload-partial."
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -231 +231 @@ _PHASE_GAP_NAVIGATION_PROFILES: dict[tuple[str, str], dict[str, Any]] = { +            "while keeping preseal partial explicit as payload-partial."
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -255 +255 @@ _PHASE_GAP_NAVIGATION_PROFILES: dict[tuple[str, str], dict[str, Any]] = { -            "while keeping preseal explicit as payload-partial."
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -255 +255 @@ _PHASE_GAP_NAVIGATION_PROFILES: dict[tuple[str, str], dict[str, Any]] = { +            "while keeping preseal partial explicit as payload-partial."
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -1419,2 +1418,0 @@ def _phase_linked_method_confirmation_items(*, route_family: str, phase_name: st -    if coverage_bucket in {"model_only", "test_only", "gap"}:

### `e55cae6b363709fb286a9e7908cc14afd0629661`
- 时间: 2026-04-09 13:58:42 +0800
- 标题: chore: sync 2026-04-09 13:58:42
- 涉及文件: `src/gas_calibrator/v2/core/measurement_phase_coverage.py`, `src/gas_calibrator/v2/review_surface_formatter.py`, `tests/v2/test_ui_v2_device_workbench.py`, `tests/v2/test_ui_v2_review_center.py`, `tests/v2/test_ui_v2_workbench_evidence.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, report, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -627,0 +628,37 @@ def build_measurement_phase_coverage_report( +        "linked_measurement_phases": [
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -627,0 +628,37 @@ def build_measurement_phase_coverage_report( +            f"{str(row.get('route_family') or '').strip()}/{str(row.get('phase_name') or '').strip()}".strip("/")
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -627,0 +628,37 @@ def build_measurement_phase_coverage_report( +            for row in phase_rows
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -627,0 +628,37 @@ def build_measurement_phase_coverage_report( +            if str(row.get("route_family") or "").strip() and str(row.get("phase_name") or "").strip()
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -627,0 +628,37 @@ def build_measurement_phase_coverage_report( +        ],

### `01d4835b05964cb51cf3856cc6fb1392edd767da`
- 时间: 2026-04-09 13:53:32 +0800
- 标题: chore: sync 2026-04-09 13:53:30
- 涉及文件: `src/gas_calibrator/v2/review_surface_formatter.py`, `src/gas_calibrator/v2/ui_v2/locales/en_US.json`, `src/gas_calibrator/v2/ui_v2/locales/zh_CN.json`, `tests/v2/test_build_offline_governance_artifacts.py`, `tests/v2/test_measurement_phase_coverage_report.py`, `tests/v2/test_results_gateway.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: REPORT, cali, co2, db, h2o, point, report, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -130,0 +131 @@ _REVIEW_SURFACE_PREFIX_LABELS = { +    "blockers": ("results.review_center.detail.measurement.blockers", "\u5f53\u524d\u963b\u585e"),
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -135,0 +137,16 @@ _REVIEW_SURFACE_PREFIX_LABELS = { +    "linked method confirmation items": (
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -135,0 +137,16 @@ _REVIEW_SURFACE_PREFIX_LABELS = { +        "results.review_center.detail.measurement.linked_method_items",
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -135,0 +137,16 @@ _REVIEW_SURFACE_PREFIX_LABELS = { +        "\u5173\u8054\u65b9\u6cd5\u786e\u8ba4\u6761\u76ee",
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -135,0 +137,16 @@ _REVIEW_SURFACE_PREFIX_LABELS = { +    ),

### `7010fd51f9c056208e0c6f37461b8945a7e446f2`
- 时间: 2026-04-09 13:48:33 +0800
- 标题: chore: sync 2026-04-09 13:48:30
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/measurement_phase_coverage.py`, `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, mode, point, report, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1368,0 +1369,7 @@ class ResultsGateway: +                "linked_measurement_phases": list(evidence_payload.get("linked_measurement_phases") or []),
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1368,0 +1369,7 @@ class ResultsGateway: +                "linked_measurement_gaps": [
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1368,0 +1369,7 @@ class ResultsGateway: +                    dict(item) for item in list(evidence_payload.get("linked_measurement_gaps") or []) if isinstance(item, dict)
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1368,0 +1369,7 @@ class ResultsGateway: +                ],
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -1368,0 +1369,7 @@ class ResultsGateway: +                "linked_method_confirmation_items": list(evidence_payload.get("linked_method_confirmation_items") or []),

### `91f16dc9fd7d56d07f5b4af877e71d06ce49d06b`
- 时间: 2026-04-09 13:28:42 +0800
- 标题: chore: sync 2026-04-09 13:28:42
- 涉及文件: `src/gas_calibrator/workflow/runner.py`, `tests/test_runner_corrected_delivery_hooks.py`, `tests/v2/test_ui_v2_device_workbench.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, cali, calibration, coefficient, db, delivery, point, 校准
- 关键 diff hunk 摘要:
  - src/gas_calibrator/workflow/runner.py @@ -4586,6 +4586,11 @@ class CalibrationRunner: -            self.set_status("初始化：启动压力预检查")
  - src/gas_calibrator/workflow/runner.py @@ -4586,6 +4586,11 @@ class CalibrationRunner: -            self._emit_stage_event(current="初始化", wait_reason="启动压力预检查")
  - src/gas_calibrator/workflow/runner.py @@ -4586,6 +4586,11 @@ class CalibrationRunner: -            self._startup_pressure_precheck(points)
  - src/gas_calibrator/workflow/runner.py @@ -4586,6 +4586,11 @@ class CalibrationRunner: -            self.set_status("初始化：压力传感器单点校准")
  - src/gas_calibrator/workflow/runner.py @@ -4586,6 +4586,11 @@ class CalibrationRunner: -            self._emit_stage_event(current="初始化", wait_reason="压力传感器单点校准")

### `8f4f94f1e1da733b1705520abe508dd76143b7db`
- 时间: 2026-04-09 13:23:38 +0800
- 标题: chore: sync 2026-04-09 13:23:32
- 涉及文件: `src/gas_calibrator/v2/core/measurement_phase_coverage.py`, `src/gas_calibrator/v2/ui_v2/locales/zh_CN.json`, `tests/v2/test_build_offline_governance_artifacts.py`, `tests/v2/test_measurement_phase_coverage_report.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_review_center.py`, `tests/v2/test_ui_v2_workbench_evidence.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, db, mode, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -359,3 +359 @@ def build_measurement_phase_coverage_report( -    phase_contrast_summary = " | ".join(
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -359,3 +359 @@ def build_measurement_phase_coverage_report( -        _dedupe(str(row.get("comparison_digest") or "").strip() for row in phase_rows)
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -359,3 +359 @@ def build_measurement_phase_coverage_report( -    ) or "no complete-vs-partial phase contrast recorded"
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -359,3 +359 @@ def build_measurement_phase_coverage_report( +    phase_contrast_summary = _phase_contrast_summary(phase_rows)
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -1234,0 +1233,36 @@ def _phase_comparison_digest(*, row: dict[str, Any] | None, comparison_row: dict +def _phase_contrast_summary(phase_rows: list[dict[str, Any]]) -> str:

### `83c9aefd43a58beebda078236d05b1184e2d5408`
- 时间: 2026-04-09 13:18:34 +0800
- 标题: chore: sync 2026-04-09 13:18:32
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/measurement_phase_coverage.py`, `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`, `src/gas_calibrator/v2/review_surface_formatter.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`, `src/gas_calibrator/v2/ui_v2/locales/en_US.json`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Step, cali, mode, protocol, report, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -66,0 +67,2 @@ from ..review_surface_formatter import ( +    build_measurement_review_digest_lines,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -66,0 +67,2 @@ from ..review_surface_formatter import ( +    build_readiness_review_digest_lines,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -69,0 +72 @@ from ..review_surface_formatter import ( +    humanize_review_surface_text,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -925,0 +929 @@ class ResultsGateway: +        measurement_review_lines = build_measurement_review_digest_lines(phase_coverage_summary)
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -928 +932,9 @@ class ResultsGateway: -            lines.append(f"measurement-core shadow: {measurement_core_stability_text}")

### `7b87d77daa36587d618769625daf6161803440ba`
- 时间: 2026-04-09 12:13:36 +0800
- 标题: chore: sync 2026-04-09 12:13:36
- 涉及文件: `src/gas_calibrator/v2/core/measurement_phase_coverage.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_results_page.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Step, cali, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -407,0 +408,16 @@ def build_measurement_phase_coverage_report( +    linked_artifact_refs = _dedupe_artifact_refs(
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -407,0 +408,16 @@ def build_measurement_phase_coverage_report( +        [
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -407,0 +408,16 @@ def build_measurement_phase_coverage_report( +            {
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -407,0 +408,16 @@ def build_measurement_phase_coverage_report( +                "artifact_type": artifact_name,
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -407,0 +408,16 @@ def build_measurement_phase_coverage_report( +                "path": str(path_value or ""),

### `af58b8f10524f7558e95da54d73119bb4c0d07bf`
- 时间: 2026-04-09 12:08:34 +0800
- 标题: chore: sync 2026-04-09 12:08:34
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/measurement_phase_coverage.py`, `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`, `tests/v2/test_build_offline_governance_artifacts.py`, `tests/v2/test_measurement_phase_coverage_report.py`, `tests/v2/test_ui_v2_review_center.py`, `tests/v2/test_ui_v2_workbench_evidence.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: REPORT, Step, cali, co2, h2o, mode, point, protocol
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -892,0 +893,10 @@ class ResultsGateway: +        measurement_core_payload_complete_text = (
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -892,0 +893,10 @@ class ResultsGateway: +            str(phase_coverage_digest.get("payload_complete_phase_summary") or "").strip()
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -892,0 +893,10 @@ class ResultsGateway: +            if phase_coverage_summary
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -892,0 +893,10 @@ class ResultsGateway: +            else ""
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -892,0 +893,10 @@ class ResultsGateway: +        )

### `8026e630a91dd02662e6d974d01dfcf7b556fa54`
- 时间: 2026-04-09 12:03:31 +0800
- 标题: chore: sync 2026-04-09 12:03:31
- 涉及文件: `src/gas_calibrator/v2/core/measurement_phase_coverage.py`, `src/gas_calibrator/v2/core/offline_artifacts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, Point, cali, co2, h2o, mode, point, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -86,0 +87,72 @@ _PHASE_ACTIONS: dict[str, tuple[str, ...]] = { +_PAYLOAD_COMPLETE_BUCKET = "actual_simulated_run_with_payload_complete"
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -86,0 +87,72 @@ _PHASE_ACTIONS: dict[str, tuple[str, ...]] = { +_PAYLOAD_PARTIAL_BUCKET = "actual_simulated_run_with_payload_partial"
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -86,0 +87,72 @@ _PHASE_ACTIONS: dict[str, tuple[str, ...]] = { +_PAYLOAD_BACKED_BUCKETS = {_PAYLOAD_COMPLETE_BUCKET, _PAYLOAD_PARTIAL_BUCKET}
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -86,0 +87,72 @@ _PHASE_ACTIONS: dict[str, tuple[str, ...]] = { +
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -86,0 +87,72 @@ _PHASE_ACTIONS: dict[str, tuple[str, ...]] = { +_READINESS_ARTIFACT_ANCHORS: dict[str, dict[str, str]] = {

### `609d48d50d3cfdbdcfb858c310406a38062dc978`
- 时间: 2026-04-09 11:28:33 +0800
- 标题: chore: sync 2026-04-09 11:28:33
- 涉及文件: `tests/v2/test_build_offline_governance_artifacts.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_review_center.py`
- 判定原因: diff 内容命中关键词: db, step
- 关键 diff hunk 摘要:
  - tests/v2/test_build_offline_governance_artifacts.py @@ -940 +940 @@ def test_rebuild_run_generates_recognition_readiness_artifacts(tmp_path: Path) - -    assert decision_rule["current_stage_applicability"] == "step2_reviewer_readiness_only"

### `f19aff9d5546342a80f7bfa78677a3a9a60b791e`
- 时间: 2026-04-09 11:23:34 +0800
- 标题: chore: sync 2026-04-09 11:23:33
- 涉及文件: `tests/v2/test_build_offline_governance_artifacts.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_device_workbench.py`, `tests/v2/test_ui_v2_results_page.py`, `tests/v2/test_ui_v2_review_center.py`, `tests/v2/test_ui_v2_workbench_evidence.py`
- 判定原因: diff 内容命中关键词: PROTOCOL, Step, cali, coefficient, report, step, store
- 关键 diff hunk 摘要:
  - tests/v2/test_build_offline_governance_artifacts.py @@ -34,0 +35 @@ from gas_calibrator.v2.core.stage3_standards_alignment_matrix import ( +from gas_calibrator.v2.core import recognition_readiness_artifacts as recognition_readiness
  - tests/v2/test_build_offline_governance_artifacts.py @@ -869,0 +871,125 @@ def test_main_reports_clear_error_for_non_run_directory(tmp_path: Path, capsys) +    run_dir = Path(facade.result_store.run_dir)
  - tests/v2/test_build_offline_governance_artifacts.py @@ -869,0 +871,125 @@ def test_main_reports_clear_error_for_non_run_directory(tmp_path: Path, capsys) +        recognition_readiness.METHOD_CONFIRMATION_PROTOCOL_FILENAME,
  - tests/v2/test_build_offline_governance_artifacts.py @@ -869,0 +871,125 @@ def test_main_reports_clear_error_for_non_run_directory(tmp_path: Path, capsys) +        recognition_readiness.METHOD_CONFIRMATION_PROTOCOL_MARKDOWN_FILENAME,
  - tests/v2/test_build_offline_governance_artifacts.py @@ -869,0 +871,125 @@ def test_main_reports_clear_error_for_non_run_directory(tmp_path: Path, capsys) +    assert decision_rule["current_stage_applicability"] == "step2_reviewer_readiness_only"

### `8a6abd728246acf9fea73dc9d0797d2d59a07b37`
- 时间: 2026-04-09 11:18:35 +0800
- 标题: chore: sync 2026-04-09 11:18:35
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -361,0 +362,67 @@ class DeviceWorkbenchController: +    def _load_recognition_readiness_evidence(self) -> dict[str, Any]:
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -361,0 +362,67 @@ class DeviceWorkbenchController: +        gateway = getattr(self.facade, "results_gateway", None)
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -361,0 +362,67 @@ class DeviceWorkbenchController: +        if gateway is None:
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -361,0 +362,67 @@ class DeviceWorkbenchController: +            return {}
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -361,0 +362,67 @@ class DeviceWorkbenchController: +        scope_summary = dict(gateway.load_json(recognition_readiness.SCOPE_READINESS_SUMMARY_FILENAME) or {})

### `36cf2eae92cd0da8ff5e0192fb56362441aa4c9b`
- 时间: 2026-04-09 11:13:33 +0800
- 标题: chore: sync 2026-04-09 11:13:33
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/artifact_catalog.py`, `src/gas_calibrator/v2/core/offline_artifacts.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, PROTOCOL, Point, REPORT, cali, mode, point, protocol
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -31,0 +32 @@ from ..core.measurement_phase_coverage import ( +from ..core import recognition_readiness_artifacts as recognition_readiness
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -190,0 +192,42 @@ class ResultsGateway: +        scope_readiness_summary = self.load_json(recognition_readiness.SCOPE_READINESS_SUMMARY_FILENAME)
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -190,0 +192,42 @@ class ResultsGateway: +        if not scope_readiness_summary:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -190,0 +192,42 @@ class ResultsGateway: +            scope_readiness_summary = self._read_summary_section(
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -190,0 +192,42 @@ class ResultsGateway: +                "scope_readiness_summary",

### `5ed5fbd17618cd70c9ed489e2d5c987daf2d1b0d`
- 时间: 2026-04-09 11:08:35 +0800
- 标题: chore: sync 2026-04-09 11:08:31
- 涉及文件: `src/gas_calibrator/v2/core/recognition_readiness_artifacts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, H2O, PROTOCOL, Protocol, Step, cali, calibration, co2
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -0,0 +1,1449 @@ +from __future__ import annotations
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -0,0 +1,1449 @@ +
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -0,0 +1,1449 @@ +from datetime import datetime, timezone
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -0,0 +1,1449 @@ +from typing import Any, Iterable
  - src/gas_calibrator/v2/core/recognition_readiness_artifacts.py @@ -0,0 +1,1449 @@ +from .models import SamplingResult

### `015e9d2c9c996b93f793ed0883956e365d52e281`
- 时间: 2026-04-09 10:33:32 +0800
- 标题: chore: sync 2026-04-09 10:33:30
- 涉及文件: `tests/v2/test_ui_v2_review_center.py`
- 判定原因: diff 内容命中关键词: REPORT, Step, cali, mode, report, store, v1
- 关键 diff hunk 摘要:
  - tests/v2/test_ui_v2_review_center.py @@ -21,0 +22,4 @@ from gas_calibrator.v2.core.stage3_real_validation_plan import ( +from gas_calibrator.v2.core.measurement_phase_coverage import (
  - tests/v2/test_ui_v2_review_center.py @@ -21,0 +22,4 @@ from gas_calibrator.v2.core.stage3_real_validation_plan import ( +    MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME,
  - tests/v2/test_ui_v2_review_center.py @@ -21,0 +22,4 @@ from gas_calibrator.v2.core.stage3_real_validation_plan import ( +    MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME,
  - tests/v2/test_ui_v2_review_center.py @@ -306,0 +316,116 @@ def test_review_center_aggregates_multi_evidence_and_acceptance_readiness(tmp_pa +    run_dir = Path(facade.result_store.run_dir)
  - tests/v2/test_ui_v2_review_center.py @@ -306,0 +316,116 @@ def test_review_center_aggregates_multi_evidence_and_acceptance_readiness(tmp_pa +    json_path = str((run_dir / MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME).resolve())

### `60b7619b5cd8f0819a961a173951e81f3d780198`
- 时间: 2026-04-09 10:28:33 +0800
- 标题: chore: sync 2026-04-09 10:28:32
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/measurement_phase_coverage.py`, `src/gas_calibrator/v2/core/multi_source_stability.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`, `src/gas_calibrator/v2/ui_v2/locales/en_US.json`, `src/gas_calibrator/v2/ui_v2/locales/zh_CN.json`, `tests/v2/test_measurement_phase_coverage_report.py`, `tests/v2/test_multi_source_stability.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_device_workbench.py`, `tests/v2/test_ui_v2_results_page.py`, `tests/v2/test_ui_v2_workbench_evidence.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: REPORT, cali, co2, mode, point, report, v1
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -804,0 +805,15 @@ class ResultsGateway: +        measurement_core_payload_phase_text = (
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -804,0 +805,15 @@ class ResultsGateway: +            str(phase_coverage_digest.get("payload_phase_summary") or "").strip()
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -804,0 +805,15 @@ class ResultsGateway: +            if phase_coverage_summary
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -804,0 +805,15 @@ class ResultsGateway: +            else ""
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -804,0 +805,15 @@ class ResultsGateway: +        )

### `bbedd9bc89f64ea2eaabe43fd2b18bc51f7ced85`
- 时间: 2026-04-09 10:23:32 +0800
- 标题: chore: sync 2026-04-09 10:23:31
- 涉及文件: `src/gas_calibrator/v2/core/measurement_phase_coverage.py`, `src/gas_calibrator/v2/core/offline_artifacts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, Point, cali, co2, h2o, mode, point, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -145,4 +145,8 @@ def build_measurement_phase_coverage_report( -    actual_count = sum(1 for row in phase_rows if row.get("evidence_source") == "actual_simulated_run")
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -145,4 +145,8 @@ def build_measurement_phase_coverage_report( -    model_only_count = sum(1 for row in phase_rows if row.get("evidence_source") == "model_only")
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -145,4 +145,8 @@ def build_measurement_phase_coverage_report( -    test_only_count = sum(1 for row in phase_rows if row.get("evidence_source") == "test_only")
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -145,4 +145,8 @@ def build_measurement_phase_coverage_report( -    gap_count = sum(1 for row in phase_rows if row.get("evidence_source") == "gap")
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -145,4 +145,8 @@ def build_measurement_phase_coverage_report( +    payload_backed_count = sum(

### `ac9bd2af8ee869ea9e3619c93a20764fb84fcb2a`
- 时间: 2026-04-09 09:38:34 +0800
- 标题: chore: sync 2026-04-09 09:38:32
- 涉及文件: `src/gas_calibrator/v2/configs/smoke_v2_measurement_trace.json`, `src/gas_calibrator/v2/core/offline_artifacts.py`, `tests/v2/test_measurement_phase_coverage_report.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: REPORT, cali, point, report, v1
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/configs/smoke_v2_measurement_trace.json @@ -110,0 +111 @@ +    "profile_name": "measurement_trace_rich_v1",
  - src/gas_calibrator/v2/core/offline_artifacts.py @@ -85,0 +86,64 @@ def _load_route_trace_events(run_dir: Path) -> list[dict[str, Any]]: +def _route_trace_text(event: dict[str, Any]) -> str:
  - src/gas_calibrator/v2/core/offline_artifacts.py @@ -85,0 +86,64 @@ def _load_route_trace_events(run_dir: Path) -> list[dict[str, Any]]: +    payload = dict(event or {})
  - src/gas_calibrator/v2/core/offline_artifacts.py @@ -85,0 +86,64 @@ def _load_route_trace_events(run_dir: Path) -> list[dict[str, Any]]: +    return " ".join(
  - src/gas_calibrator/v2/core/offline_artifacts.py @@ -85,0 +86,64 @@ def _load_route_trace_events(run_dir: Path) -> list[dict[str, Any]]: +        str(payload.get(key) or "")

### `9049dc3ed11ce220b719a05d986553c7087fd7f7`
- 时间: 2026-04-09 09:29:02 +0800
- 标题: chore: sync 2026-04-09 09:28:59
- 涉及文件: `tests/v2/test_controlled_state_machine_profile.py`, `tests/v2/test_multi_source_stability.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_device_workbench.py`, `tests/v2/test_ui_v2_results_page.py`, `tests/v2/test_ui_v2_workbench_evidence.py`
- 判定原因: diff 内容命中关键词: REPORT, Step, cali, co2, mode, point, report, step
- 关键 diff hunk 摘要:
  - tests/v2/test_controlled_state_machine_profile.py @@ -100,0 +101,16 @@ def test_state_transition_evidence_captures_recovery_trace_and_boundaries() -> N +                "point_index": 1,
  - tests/v2/test_controlled_state_machine_profile.py @@ -100,0 +101,16 @@ def test_state_transition_evidence_captures_recovery_trace_and_boundaries() -> N +                "route": "co2",
  - tests/v2/test_controlled_state_machine_profile.py @@ -100,0 +101,16 @@ def test_state_transition_evidence_captures_recovery_trace_and_boundaries() -> N +                "point_tag": "sealed_gas",
  - tests/v2/test_multi_source_stability.py @@ -11,0 +12 @@ from gas_calibrator.v2.core.multi_source_stability import ( +from gas_calibrator.v2.core.measurement_phase_coverage import build_measurement_phase_coverage_report
  - tests/v2/test_multi_source_stability.py @@ -194,0 +196,7 @@ def test_simulation_evidence_sidecar_bundle_stays_contract_only() -> None: +    phase_coverage = build_measurement_phase_coverage_report(

### `e22b29637e03d0dcd63cab63e453a7804698b6f2`
- 时间: 2026-04-09 09:23:32 +0800
- 标题: chore: sync 2026-04-09 09:23:32
- 涉及文件: `src/gas_calibrator/v2/configs/smoke_points_measurement_trace.json`, `src/gas_calibrator/v2/configs/smoke_v2_measurement_trace.json`, `src/gas_calibrator/v2/ui_v2/locales/en_US.json`, `src/gas_calibrator/v2/ui_v2/locales/zh_CN.json`, `tests/v2/test_measurement_phase_coverage_report.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, POINT, Point, REPORT, Step, cali, co2, coefficient
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/configs/smoke_points_measurement_trace.json @@ -0,0 +1,25 @@ +[
  - src/gas_calibrator/v2/configs/smoke_points_measurement_trace.json @@ -0,0 +1,25 @@ +  {
  - src/gas_calibrator/v2/configs/smoke_points_measurement_trace.json @@ -0,0 +1,25 @@ +    "index": 1,
  - src/gas_calibrator/v2/configs/smoke_points_measurement_trace.json @@ -0,0 +1,25 @@ +    "temperature": 25.0,
  - src/gas_calibrator/v2/configs/smoke_points_measurement_trace.json @@ -0,0 +1,25 @@ +    "co2_ppm": 400.0,

### `ec0595acf08a1ff5b4a2ce30caf500f4e5619054`
- 时间: 2026-04-09 09:18:33 +0800
- 标题: chore: sync 2026-04-09 09:18:32
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`, `src/gas_calibrator/v2/ui_v2/pages/results_page.py`, `src/gas_calibrator/v2/ui_v2/review_center_presenter.py`, `src/gas_calibrator/v2/ui_v2/widgets/review_center_panel.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: REPORT, cali, report, store
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -620,0 +621 @@ class ResultsGateway: +        measurement_phase_coverage_report: dict[str, Any] | None,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -632,0 +634 @@ class ResultsGateway: +        phase_coverage_summary = dict(measurement_phase_coverage_report or {})
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -771,0 +774 @@ class ResultsGateway: +        phase_coverage_digest = dict(phase_coverage_summary.get("digest") or {})
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -791,0 +795,10 @@ class ResultsGateway: +        measurement_core_phase_coverage_text = (
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -791,0 +795,10 @@ class ResultsGateway: +            str(

### `2b0e9c1eb9119f44842fbd550c152ab5d158e77a`
- 时间: 2026-04-09 08:58:33 +0800
- 标题: chore: sync 2026-04-09 08:58:30
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/artifact_catalog.py`, `src/gas_calibrator/v2/core/controlled_state_machine_profile.py`, `src/gas_calibrator/v2/core/multi_source_stability.py`, `src/gas_calibrator/v2/core/offline_artifacts.py`, `src/gas_calibrator/v2/core/result_store.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: REPORT, Store, cali, mode, point, report, store, v1
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -27,0 +28,4 @@ from ..core.multi_source_stability import ( +from ..core.measurement_phase_coverage import (
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -27,0 +28,4 @@ from ..core.multi_source_stability import ( +    MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -27,0 +28,4 @@ from ..core.multi_source_stability import ( +    MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -27,0 +28,4 @@ from ..core.multi_source_stability import ( +)
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -176,0 +181,10 @@ class ResultsGateway: +        measurement_phase_coverage_report = self.load_json(MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME)

### `cd978fd28169579a40047635c9df3050c84d4394`
- 时间: 2026-04-09 08:53:35 +0800
- 标题: chore: sync 2026-04-09 08:53:33
- 涉及文件: `src/gas_calibrator/v2/core/measurement_phase_coverage.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: REPORT, Report, Step, cali, co2, h2o, mode, point
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -0,0 +1,591 @@ +from __future__ import annotations
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -0,0 +1,591 @@ +
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -0,0 +1,591 @@ +from datetime import datetime, timezone
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -0,0 +1,591 @@ +from typing import Any, Iterable
  - src/gas_calibrator/v2/core/measurement_phase_coverage.py @@ -0,0 +1,591 @@ +from .models import SamplingResult

### `8b8bc39bf774295ac9ac2cc551afff572b388d66`
- 时间: 2026-04-08 21:53:32 +0800
- 标题: chore: sync 2026-04-08 21:53:32
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, store
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -749,0 +750,32 @@ class ResultsGateway: +        measurement_core_stability_text = (
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -749,0 +750,32 @@ class ResultsGateway: +            str(
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -749,0 +750,32 @@ class ResultsGateway: +                stability_digest.get("summary")
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -749,0 +750,32 @@ class ResultsGateway: +                or stability_summary.get("summary")
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -749,0 +750,32 @@ class ResultsGateway: +                or stability_summary.get("coverage_status")

### `e5d398533083c8115a28b5e421b4003bd893d536`
- 时间: 2026-04-08 21:43:32 +0800
- 标题: chore: sync 2026-04-08 21:43:32
- 涉及文件: `src/gas_calibrator/v2/core/controlled_state_machine_profile.py`, `src/gas_calibrator/v2/ui_v2/locales/zh_CN.json`, `tests/v2/test_multi_source_stability.py`, `tests/v2/test_plan_compiler.py`, `tests/v2/test_ui_v2_device_workbench.py`, `tests/v2/test_ui_v2_results_page.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, Point, cali, co2, coefficient, mode, point, v1
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/controlled_state_machine_profile.py @@ -5 +5 @@ from datetime import datetime, timezone -from typing import Any, Iterable
  - src/gas_calibrator/v2/core/controlled_state_machine_profile.py @@ -5 +5 @@ from datetime import datetime, timezone +from typing import TYPE_CHECKING, Any, Iterable
  - src/gas_calibrator/v2/core/controlled_state_machine_profile.py @@ -8 +8,3 @@ from .models import CalibrationPoint, SamplingResult -from .plan_compiler import CompiledPlan
  - src/gas_calibrator/v2/core/controlled_state_machine_profile.py @@ -8 +8,3 @@ from .models import CalibrationPoint, SamplingResult +
  - src/gas_calibrator/v2/core/controlled_state_machine_profile.py @@ -8 +8,3 @@ from .models import CalibrationPoint, SamplingResult +if TYPE_CHECKING:

### `a00622ca157ab33a4814c6d5c3dd6b8e36549e97`
- 时间: 2026-04-08 21:38:31 +0800
- 标题: chore: sync 2026-04-08 21:38:31
- 涉及文件: `tests/v2/test_controlled_state_machine_profile.py`, `tests/v2/test_multi_source_stability.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_device_workbench.py`, `tests/v2/test_ui_v2_workbench_evidence.py`
- 判定原因: diff 内容命中关键词: Calibration, Point, Step, cali, co2, h2o, mode, point
- 关键 diff hunk 摘要:
  - tests/v2/test_controlled_state_machine_profile.py @@ -0,0 +1,121 @@ +from gas_calibrator.v2.core.controlled_state_machine_profile import (
  - tests/v2/test_controlled_state_machine_profile.py @@ -0,0 +1,121 @@ +from gas_calibrator.v2.core.models import CalibrationPoint, SamplingResult
  - tests/v2/test_controlled_state_machine_profile.py @@ -0,0 +1,121 @@ +from gas_calibrator.v2.core.plan_compiler import PlanCompiler
  - tests/v2/test_controlled_state_machine_profile.py @@ -0,0 +1,121 @@ +from gas_calibrator.v2.domain.plan_models import (
  - tests/v2/test_controlled_state_machine_profile.py @@ -0,0 +1,121 @@ +    CalibrationPlanProfile,

### `4c3d3c1f8e00baa81b53e410693dee5b60b3152e`
- 时间: 2026-04-08 21:33:32 +0800
- 标题: chore: sync 2026-04-08 21:33:31
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`, `src/gas_calibrator/v2/ui_v2/locales/en_US.json`, `src/gas_calibrator/v2/ui_v2/locales/zh_CN.json`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Point, cali, point, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -4277,0 +4278,21 @@ class DeviceWorkbenchController: +            {
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -4277,0 +4278,21 @@ class DeviceWorkbenchController: +                "id": "measurement_core",
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -4277,0 +4278,21 @@ class DeviceWorkbenchController: +                "title": t(
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -4277,0 +4278,21 @@ class DeviceWorkbenchController: +                    "pages.devices.workbench.engineer_section.measurement_core.title",
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -4277,0 +4278,21 @@ class DeviceWorkbenchController: +                    default="measurement-core readiness",

### `8a6ce494570134be7764a587687ab9871a5fc736`
- 时间: 2026-04-08 21:28:30 +0800
- 标题: chore: sync 2026-04-08 21:28:30
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -16,0 +17,5 @@ from ...config import ( +from ...core.controlled_state_machine_profile import STATE_TRANSITION_EVIDENCE_FILENAME
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -16,0 +17,5 @@ from ...config import ( +from ...core.multi_source_stability import (
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -16,0 +17,5 @@ from ...config import ( +    MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME,
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -16,0 +17,5 @@ from ...config import ( +    SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME,
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -16,0 +17,5 @@ from ...config import ( +)

### `54a02e406da1608d082a3180516593a1f8c14e44`
- 时间: 2026-04-08 21:23:33 +0800
- 标题: chore: sync 2026-04-08 21:23:32
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/review_center_presenter.py`, `src/gas_calibrator/v2/ui_v2/widgets/review_center_panel.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, mode, protocol, store
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -595,0 +596,3 @@ class ResultsGateway: +        multi_source_stability_evidence: dict[str, Any] | None,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -595,0 +596,3 @@ class ResultsGateway: +        state_transition_evidence: dict[str, Any] | None,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -595,0 +596,3 @@ class ResultsGateway: +        simulation_evidence_sidecar_bundle: dict[str, Any] | None,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -604,0 +608,3 @@ class ResultsGateway: +        stability_summary = dict(multi_source_stability_evidence or {})
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -604,0 +608,3 @@ class ResultsGateway: +        transition_summary = dict(state_transition_evidence or {})

### `11a4cfa153dc6c70f7d7735b7629a567df0684ac`
- 时间: 2026-04-08 21:18:31 +0800
- 标题: chore: sync 2026-04-08 21:18:31
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/controlled_state_machine_profile.py`, `src/gas_calibrator/v2/core/offline_artifacts.py`, `src/gas_calibrator/v2/core/result_store.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, Point, Store, cali, h2o, mode, point, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -18,0 +19,9 @@ from ..core.engineering_isolation_admission_checklist_artifact_entry import ( +from ..core.controlled_state_machine_profile import (
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -18,0 +19,9 @@ from ..core.engineering_isolation_admission_checklist_artifact_entry import ( +    STATE_TRANSITION_EVIDENCE_FILENAME,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -18,0 +19,9 @@ from ..core.engineering_isolation_admission_checklist_artifact_entry import ( +    STATE_TRANSITION_EVIDENCE_MARKDOWN_FILENAME,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -18,0 +19,9 @@ from ..core.engineering_isolation_admission_checklist_artifact_entry import ( +)
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -18,0 +19,9 @@ from ..core.engineering_isolation_admission_checklist_artifact_entry import ( +from ..core.multi_source_stability import (

### `41f7544169cfc64e78954e3189fed83369d8c86e`
- 时间: 2026-04-08 21:08:34 +0800
- 标题: chore: sync 2026-04-08 21:08:32
- 涉及文件: `src/gas_calibrator/v2/core/artifact_catalog.py`, `src/gas_calibrator/v2/core/controlled_state_machine_profile.py`, `src/gas_calibrator/v2/core/multi_source_stability.py`, `src/gas_calibrator/v2/core/offline_artifacts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, POINT, Point, REPORT, Step, cali, co2, h2o
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -32,0 +33 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "simulation_evidence_sidecar_bundle",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -50,0 +52,4 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "multi_source_stability_evidence",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -50,0 +52,4 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "multi_source_stability_evidence_markdown",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -50,0 +52,4 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "state_transition_evidence",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -50,0 +52,4 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "state_transition_evidence_markdown",

### `5947b2df3f1c4316be63f2e80c0a83e7329a6fe1`
- 时间: 2026-04-08 18:58:31 +0800
- 标题: chore: sync 2026-04-08 18:58:30
- 涉及文件: `tests/v2/test_acceptance_governance.py`, `tests/v2/test_build_offline_governance_artifacts.py`, `tests/v2/test_ui_v2_reports_page.py`, `tests/v2/test_ui_v2_review_center.py`, `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Report, Step, cali, insert, report, store
- 关键 diff hunk 摘要:
  - tests/v2/test_build_offline_governance_artifacts.py @@ -35,0 +37,6 @@ from gas_calibrator.v2.scripts.build_offline_governance_artifacts import main, r +    sys.path.insert(0, str(SUPPORT_DIR))
  - tests/v2/test_build_offline_governance_artifacts.py @@ -737,2 +744,2 @@ def test_rebuild_run_generates_stage3_standards_alignment_matrix_artifacts(tmp_p +    run_dir = Path(facade.result_store.run_dir)
  - tests/v2/test_ui_v2_reports_page.py @@ -907,0 +908,75 @@ def test_reports_page_artifact_list_surfaces_stage3_real_validation_plan_from_sa +
  - tests/v2/test_ui_v2_reports_page.py @@ -907,0 +908,75 @@ def test_reports_page_artifact_list_surfaces_stage3_real_validation_plan_from_sa +def test_reports_page_artifact_list_surfaces_stage3_standards_alignment_matrix_from_same_rebuilt_run(
  - tests/v2/test_ui_v2_reports_page.py @@ -907,0 +908,75 @@ def test_reports_page_artifact_list_surfaces_stage3_real_validation_plan_from_sa +    tmp_path: Path,

### `487a8e8095d800e281f50226bc2fe5f1bfc6be11`
- 时间: 2026-04-08 18:53:32 +0800
- 标题: chore: sync 2026-04-08 18:53:32
- 涉及文件: `tests/v2/test_acceptance_governance.py`, `tests/v2/test_build_offline_governance_artifacts.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_reports_page.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CALIBRATION, STEP, Step, cali, calibration, mode, report, step
- 关键 diff hunk 摘要:
  - tests/v2/test_acceptance_governance.py @@ -50,0 +51,10 @@ from gas_calibrator.v2.core.stage3_real_validation_plan_artifact_entry import ( +from gas_calibrator.v2.core.stage3_standards_alignment_matrix import (
  - tests/v2/test_acceptance_governance.py @@ -50,0 +51,10 @@ from gas_calibrator.v2.core.stage3_real_validation_plan_artifact_entry import ( +from gas_calibrator.v2.core.stage3_standards_alignment_matrix_artifact_entry import (
  - tests/v2/test_acceptance_governance.py @@ -487,0 +498,181 @@ def test_phase_transition_bridge_reviewer_artifact_entry_reuses_manifest_and_pan +    readiness = build_step2_readiness_summary(
  - tests/v2/test_acceptance_governance.py @@ -487,0 +498,181 @@ def test_phase_transition_bridge_reviewer_artifact_entry_reuses_manifest_and_pan +        simulation_mode=True,
  - tests/v2/test_acceptance_governance.py @@ -487,0 +498,181 @@ def test_phase_transition_bridge_reviewer_artifact_entry_reuses_manifest_and_pan +            "step2_default_workflow_allowed": True,

### `89e4e6d2cb612e6f2ea1ebe063b5762aa7074e1a`
- 时间: 2026-04-08 18:48:31 +0800
- 标题: chore: sync 2026-04-08 18:48:30
- 涉及文件: `src/gas_calibrator/v2/core/codex_patch_smoke.txt`, `src/gas_calibrator/v2/ui_v2/locales/en_US.json`, `src/gas_calibrator/v2/ui_v2/locales/zh_CN.json`, `src/gas_calibrator/v2/ui_v2/widgets/review_center_panel.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, db, mode
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/codex_patch_smoke.txt @@ -1 +0,0 @@ -smoke
  - src/gas_calibrator/v2/ui_v2/locales/en_US.json @@ -1861,10 +1861,22 @@ -      "count": "Showing {visible}/{total}",
  - src/gas_calibrator/v2/ui_v2/locales/en_US.json @@ -1861,10 +1861,22 @@ -      "all_types": "All Types",
  - src/gas_calibrator/v2/ui_v2/locales/en_US.json @@ -1861,10 +1861,22 @@ -      "all_statuses": "All Statuses",
  - src/gas_calibrator/v2/ui_v2/locales/en_US.json @@ -1861,10 +1861,22 @@ -      "all_sources": "All Sources",

### `405fc7fbdcce935ae3496c69670e7271dcc00302`
- 时间: 2026-04-08 18:43:34 +0800
- 标题: chore: sync 2026-04-08 18:43:32
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/engineering_isolation_admission_checklist_artifact_entry.py`, `src/gas_calibrator/v2/core/stage3_real_validation_plan_artifact_entry.py`, `src/gas_calibrator/v2/core/stage_admission_review_pack_artifact_entry.py`, `src/gas_calibrator/v2/scripts/build_offline_governance_artifacts.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py`, `src/gas_calibrator/v2/ui_v2/review_center_presenter.py`, `src/gas_calibrator/v2/ui_v2/review_scope_export_index.py`, `src/gas_calibrator/v2/ui_v2/widgets/review_center_panel.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Step, cali, calibration, report, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -42,0 +43,9 @@ from ..core.stage3_real_validation_plan_artifact_entry import ( +from ..core.stage3_standards_alignment_matrix import (
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -42,0 +43,9 @@ from ..core.stage3_real_validation_plan_artifact_entry import ( +    STAGE3_STANDARDS_ALIGNMENT_MATRIX_FILENAME,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -42,0 +43,9 @@ from ..core.stage3_real_validation_plan_artifact_entry import ( +    STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_FILENAME,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -42,0 +43,9 @@ from ..core.stage3_real_validation_plan_artifact_entry import ( +)
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -42,0 +43,9 @@ from ..core.stage3_real_validation_plan_artifact_entry import ( +from ..core.stage3_standards_alignment_matrix_artifact_entry import (

### `31f2269959bc4d77336743ff4a377771e015e3f0`
- 时间: 2026-04-08 18:38:33 +0800
- 标题: chore: sync 2026-04-08 18:38:32
- 涉及文件: `src/gas_calibrator/v2/core/artifact_catalog.py`, `src/gas_calibrator/v2/core/codex_patch_smoke.txt`, `src/gas_calibrator/v2/core/stage3_standards_alignment_matrix.py`, `src/gas_calibrator/v2/core/stage3_standards_alignment_matrix_artifact_entry.py`, `src/gas_calibrator/v2/scripts/build_offline_governance_artifacts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CALIBRATION, REPORT, STEP, Step, cali, calibration, mode, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -39,0 +40 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "stage3_standards_alignment_matrix",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -77,0 +79 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "stage3_standards_alignment_matrix_reviewer_artifact",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -120,0 +123,2 @@ KNOWN_ARTIFACT_KEYS_BY_FILENAME: dict[str, str] = { +    "stage3_standards_alignment_matrix.json": "stage3_standards_alignment_matrix",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -120,0 +123,2 @@ KNOWN_ARTIFACT_KEYS_BY_FILENAME: dict[str, str] = { +    "stage3_standards_alignment_matrix.md": "stage3_standards_alignment_matrix_reviewer_artifact",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -171,0 +176,2 @@ KNOWN_REPORT_ARTIFACTS = [ +    "stage3_standards_alignment_matrix.json",

### `caa214604ca93ffd5e6ba784cd0dadee77ef2198`
- 时间: 2026-04-08 18:28:31 +0800
- 标题: chore: sync 2026-04-08 18:28:31
- 涉及文件: `src/gas_calibrator/v2/core/stage3_standards_alignment_matrix.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CALIBRATION, STEP, Step, cali, calibration, mode, step, 校准
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/stage3_standards_alignment_matrix.py @@ -0,0 +1,675 @@ +from __future__ import annotations
  - src/gas_calibrator/v2/core/stage3_standards_alignment_matrix.py @@ -0,0 +1,675 @@ +
  - src/gas_calibrator/v2/core/stage3_standards_alignment_matrix.py @@ -0,0 +1,675 @@ +from datetime import datetime, timezone
  - src/gas_calibrator/v2/core/stage3_standards_alignment_matrix.py @@ -0,0 +1,675 @@ +from typing import Any
  - src/gas_calibrator/v2/core/stage3_standards_alignment_matrix.py @@ -0,0 +1,675 @@ +from .engineering_isolation_admission_checklist import (

### `7f9d7d77fa3a72c2fbef1adfe680c403d58a9784`
- 时间: 2026-04-08 17:43:31 +0800
- 标题: chore: sync 2026-04-08 17:43:30
- 涉及文件: `src/gas_calibrator/workflow/runner.py`, `tests/test_runner_multi_analyzers.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, cali, mode
- 关键 diff hunk 摘要:
  - src/gas_calibrator/workflow/runner.py @@ -5730 +5730,2 @@ class CalibrationRunner: -                raise RuntimeError(f"Analyzer startup config failed: {label} err={exc}") from exc
  - src/gas_calibrator/workflow/runner.py @@ -5730 +5730,2 @@ class CalibrationRunner: +                self._disable_analyzers([label], reason="startup_mode2_verify_failed")
  - src/gas_calibrator/workflow/runner.py @@ -5730 +5730,2 @@ class CalibrationRunner: +                self._disabled_analyzer_last_reprobe_ts[label] = time.time()
  - tests/test_runner_multi_analyzers.py @@ -213 +213 @@ def test_configure_devices_forces_mode2_for_all_analyzers(tmp_path: Path) -> Non -def test_configure_devices_raises_when_analyzer_mode2_verify_fails(tmp_path: Path) -> None:
  - tests/test_runner_multi_analyzers.py @@ -213 +213 @@ def test_configure_devices_forces_mode2_for_all_analyzers(tmp_path: Path) -> Non +def test_configure_devices_disables_failing_analyzer_when_others_reach_mode2(tmp_path: Path) -> None:

### `9679006f9bd0cb030c78e270881aafb6b7010357`
- 时间: 2026-04-08 15:53:32 +0800
- 标题: chore: sync 2026-04-08 15:53:31
- 涉及文件: `src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py`, `src/gas_calibrator/v2/ui_v2/widgets/review_center_panel.py`, `tests/v2/test_acceptance_governance.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_reports_page.py`, `tests/v2/test_ui_v2_review_center.py`, `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Step, cali, report, store
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py @@ -364,0 +365,5 @@ def build_review_scope_manifest_payload( +    stage3_real_validation_plan_entry = _find_stage3_real_validation_plan_artifact_entry(
  - src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py @@ -364,0 +365,5 @@ def build_review_scope_manifest_payload( +        list(registry.get("rows", []) or [])
  - src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py @@ -364,0 +365,5 @@ def build_review_scope_manifest_payload( +    )
  - src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py @@ -364,0 +365,5 @@ def build_review_scope_manifest_payload( +    if stage3_real_validation_plan_entry:
  - src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py @@ -364,0 +365,5 @@ def build_review_scope_manifest_payload( +        payload["stage3_real_validation_plan_artifact_entry"] = stage3_real_validation_plan_entry

### `a3b4ab047c741390bf723d5423be177e3a4a1247`
- 时间: 2026-04-08 15:48:41 +0800
- 标题: chore: sync 2026-04-08 15:48:34
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/stage3_real_validation_plan_artifact_entry.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/review_center_presenter.py`, `src/gas_calibrator/v2/ui_v2/review_scope_export_index.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, mode, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -33,0 +34,9 @@ from ..core.stage_admission_review_pack_artifact_entry import ( +from ..core.stage3_real_validation_plan import (
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -33,0 +34,9 @@ from ..core.stage_admission_review_pack_artifact_entry import ( +    STAGE3_REAL_VALIDATION_PLAN_FILENAME,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -33,0 +34,9 @@ from ..core.stage_admission_review_pack_artifact_entry import ( +    STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -33,0 +34,9 @@ from ..core.stage_admission_review_pack_artifact_entry import ( +)
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -33,0 +34,9 @@ from ..core.stage_admission_review_pack_artifact_entry import ( +from ..core.stage3_real_validation_plan_artifact_entry import (

### `8b66680d15c6257913a1d2ea377c6beed17694de`
- 时间: 2026-04-08 15:13:33 +0800
- 标题: chore: sync 2026-04-08 15:13:31
- 涉及文件: `src/gas_calibrator/workflow/runner.py`, `tests/test_runner_h2o_sequence.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, Calibration, Point, cali, co2, h2o, mode, point
- 关键 diff hunk 摘要:
  - src/gas_calibrator/workflow/runner.py @@ -4689,0 +4690,2 @@ class CalibrationRunner: +        if self._selected_pressure_points_is_ambient_only():
  - src/gas_calibrator/workflow/runner.py @@ -4689,0 +4690,2 @@ class CalibrationRunner: +            out = [self._ambient_pressure_reference_point(point) for point in out]
  - src/gas_calibrator/workflow/runner.py @@ -5168 +5170,5 @@ class CalibrationRunner: -            gas_sources = self._co2_source_points(self._filter_execution_points_by_selected_pressure(points))
  - src/gas_calibrator/workflow/runner.py @@ -5168 +5170,5 @@ class CalibrationRunner: +            # Pressure selection chooses which pressure references to execute.
  - src/gas_calibrator/workflow/runner.py @@ -5168 +5170,5 @@ class CalibrationRunner: +            # CO2 source ppm steps still come from the whole temperature group;

### `5bc495c843491fc8154b488e4af71c1a30f796d0`
- 时间: 2026-04-08 14:58:31 +0800
- 标题: chore: sync 2026-04-08 14:58:30
- 涉及文件: `src/gas_calibrator/diagnostics.py`, `src/gas_calibrator/ui/app.py`, `src/gas_calibrator/workflow/runner.py`, `tests/test_diagnostics_subset.py`, `tests/test_runner_quality.py`, `tests/test_ui_app.py`, `tests/v2/test_acceptance_governance.py`, `tests/v2/test_build_offline_governance_artifacts.py`, `tests/v2/test_ui_v2_reports_page.py`, `tests/v2/test_ui_v2_review_center.py`, `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CALIBRATION, Calibration, Report, STEP, Step, cali, calibration, co2
- 关键 diff hunk 摘要:
  - src/gas_calibrator/diagnostics.py @@ -105 +105,8 @@ def _has_usable_ratio_value(parsed: Dict[str, Any], qcfg: Dict[str, Any]) -> boo -    for key in ("co2_ratio_f", "h2o_ratio_f"):
  - src/gas_calibrator/diagnostics.py @@ -105 +105,8 @@ def _has_usable_ratio_value(parsed: Dict[str, Any], qcfg: Dict[str, Any]) -> boo +    for key in (
  - src/gas_calibrator/diagnostics.py @@ -105 +105,8 @@ def _has_usable_ratio_value(parsed: Dict[str, Any], qcfg: Dict[str, Any]) -> boo +        "co2_ratio_f",
  - src/gas_calibrator/diagnostics.py @@ -105 +105,8 @@ def _has_usable_ratio_value(parsed: Dict[str, Any], qcfg: Dict[str, Any]) -> boo +        "co2_ratio_raw",
  - src/gas_calibrator/diagnostics.py @@ -105 +105,8 @@ def _has_usable_ratio_value(parsed: Dict[str, Any], qcfg: Dict[str, Any]) -> boo +        "h2o_ratio_f",

### `4e186896513c011dd6c849fd43921160d99bde91`
- 时间: 2026-04-08 14:53:41 +0800
- 标题: chore: sync 2026-04-08 14:53:39
- 涉及文件: `src/gas_calibrator/v2/core/artifact_catalog.py`, `src/gas_calibrator/v2/core/stage3_real_validation_plan.py`, `src/gas_calibrator/v2/scripts/build_offline_governance_artifacts.py`, `tests/v2/test_acceptance_governance.py`, `tests/v2/test_build_offline_governance_artifacts.py`, `tests/v2/test_ui_v2_reports_page.py`, `tests/v2/test_ui_v2_review_center.py`, `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CALIBRATION, REPORT, STEP, cali, calibration, coefficient, mode, point
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -38,0 +39 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "stage3_real_validation_plan",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -75,0 +77 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "stage3_real_validation_plan_reviewer_artifact",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -116,0 +119,2 @@ KNOWN_ARTIFACT_KEYS_BY_FILENAME: dict[str, str] = { +    "stage3_real_validation_plan.json": "stage3_real_validation_plan",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -116,0 +119,2 @@ KNOWN_ARTIFACT_KEYS_BY_FILENAME: dict[str, str] = { +    "stage3_real_validation_plan.md": "stage3_real_validation_plan_reviewer_artifact",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -165,0 +170,2 @@ KNOWN_REPORT_ARTIFACTS = [ +    "stage3_real_validation_plan.json",

### `62786e64522758d0b08f75b98628f6f17e7d465a`
- 时间: 2026-04-08 14:43:36 +0800
- 标题: chore: sync 2026-04-08 14:43:36
- 涉及文件: `src/gas_calibrator/diagnostics.py`, `src/gas_calibrator/ui/app.py`, `src/gas_calibrator/workflow/runner.py`, `tests/test_diagnostics_subset.py`, `tests/test_runner_multi_analyzers.py`, `tests/test_runner_quality.py`, `tests/test_ui_app.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, cali, co2, h2o, mode, point
- 关键 diff hunk 摘要:
  - src/gas_calibrator/diagnostics.py @@ -4,0 +5 @@ from __future__ import annotations +import math
  - src/gas_calibrator/diagnostics.py @@ -79,0 +81,34 @@ def _gas_analyzer_probe( +def _coerce_float(value: Any) -> float | None:
  - src/gas_calibrator/diagnostics.py @@ -79,0 +81,34 @@ def _gas_analyzer_probe( +    try:
  - src/gas_calibrator/diagnostics.py @@ -79,0 +81,34 @@ def _gas_analyzer_probe( +        numeric = float(value)
  - src/gas_calibrator/diagnostics.py @@ -79,0 +81,34 @@ def _gas_analyzer_probe( +    except Exception:

### `9f3ea4894bcca1fe18514a4eb16200420ed3ea26`
- 时间: 2026-04-08 13:08:35 +0800
- 标题: chore: sync 2026-04-08 13:08:33
- 涉及文件: `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_app_facade.py`, `tests/v2/test_ui_v2_reports_page.py`, `tests/v2/test_ui_v2_review_center.py`, `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: report
- 关键 diff hunk 摘要:
  - tests/v2/test_ui_v2_reports_page.py @@ -667,10 +666,0 @@ def test_reports_page_keeps_phase_bridge_section_aligned_with_reviewer_artifact_ -    review_center_entry = dict(
  - tests/v2/test_ui_v2_reports_page.py @@ -667,10 +666,0 @@ def test_reports_page_keeps_phase_bridge_section_aligned_with_reviewer_artifact_ -        results_snapshot["review_center"].get("engineering_isolation_admission_checklist_artifact_entry", {}) or {}
  - tests/v2/test_ui_v2_reports_page.py @@ -667,10 +666,0 @@ def test_reports_page_keeps_phase_bridge_section_aligned_with_reviewer_artifact_ -    )
  - tests/v2/test_ui_v2_reports_page.py @@ -667,10 +666,0 @@ def test_reports_page_keeps_phase_bridge_section_aligned_with_reviewer_artifact_ -    checklist_entry = dict(
  - tests/v2/test_ui_v2_reports_page.py @@ -667,10 +666,0 @@ def test_reports_page_keeps_phase_bridge_section_aligned_with_reviewer_artifact_ -        reports_snapshot.get("engineering_isolation_admission_checklist_artifact_entry", {}) or {}

### `87f969f7128769eb28f6b838a0ee687226e92a57`
- 时间: 2026-04-08 12:58:33 +0800
- 标题: chore: sync 2026-04-08 12:58:31
- 涉及文件: `tests/v2/test_ui_v2_reports_page.py`, `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: report
- 关键 diff hunk 摘要:
  - tests/v2/test_ui_v2_reports_page.py @@ -666,0 +667,10 @@ def test_reports_page_keeps_phase_bridge_section_aligned_with_reviewer_artifact_ +    review_center_entry = dict(
  - tests/v2/test_ui_v2_reports_page.py @@ -666,0 +667,10 @@ def test_reports_page_keeps_phase_bridge_section_aligned_with_reviewer_artifact_ +        results_snapshot["review_center"].get("engineering_isolation_admission_checklist_artifact_entry", {}) or {}
  - tests/v2/test_ui_v2_reports_page.py @@ -666,0 +667,10 @@ def test_reports_page_keeps_phase_bridge_section_aligned_with_reviewer_artifact_ +    )
  - tests/v2/test_ui_v2_reports_page.py @@ -666,0 +667,10 @@ def test_reports_page_keeps_phase_bridge_section_aligned_with_reviewer_artifact_ +    checklist_entry = dict(
  - tests/v2/test_ui_v2_reports_page.py @@ -666,0 +667,10 @@ def test_reports_page_keeps_phase_bridge_section_aligned_with_reviewer_artifact_ +        reports_snapshot.get("engineering_isolation_admission_checklist_artifact_entry", {}) or {}

### `1d1452f293a5c1f96453772c73b41f7ffe791482`
- 时间: 2026-04-08 12:53:30 +0800
- 标题: chore: sync 2026-04-08 12:53:30
- 涉及文件: `tests/v2/test_acceptance_governance.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_app_facade.py`, `tests/v2/test_ui_v2_review_center.py`, `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: diff 内容命中关键词: CALIBRATION, STEP, Step, cali, calibration, mode, report, step
- 关键 diff hunk 摘要:
  - tests/v2/test_acceptance_governance.py @@ -780,0 +781,102 @@ def test_engineering_isolation_admission_checklist_reuses_existing_pack_and_brid +    readiness = build_step2_readiness_summary(
  - tests/v2/test_acceptance_governance.py @@ -780,0 +781,102 @@ def test_engineering_isolation_admission_checklist_reuses_existing_pack_and_brid +        simulation_mode=True,
  - tests/v2/test_acceptance_governance.py @@ -780,0 +781,102 @@ def test_engineering_isolation_admission_checklist_reuses_existing_pack_and_brid +            "step2_default_workflow_allowed": True,
  - tests/v2/test_acceptance_governance.py @@ -780,0 +781,102 @@ def test_engineering_isolation_admission_checklist_reuses_existing_pack_and_brid +    metrology = build_metrology_calibration_contract(
  - tests/v2/test_acceptance_governance.py @@ -780,0 +781,102 @@ def test_engineering_isolation_admission_checklist_reuses_existing_pack_and_brid +        step2_readiness_summary=readiness,

### `a5385e1296a342a874f54b4c1f164470a79cc987`
- 时间: 2026-04-08 12:48:32 +0800
- 标题: chore: sync 2026-04-08 12:48:30
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/engineering_isolation_admission_checklist_artifact_entry.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py`, `src/gas_calibrator/v2/ui_v2/review_center_presenter.py`, `src/gas_calibrator/v2/ui_v2/review_scope_export_index.py`, `src/gas_calibrator/v2/ui_v2/widgets/review_center_panel.py`, `tests/v2/test_acceptance_governance.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: REPORT, cali, mode, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -9,0 +10,9 @@ from ..core.artifact_catalog import KNOWN_REPORT_ARTIFACTS +from ..core.engineering_isolation_admission_checklist import (
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -9,0 +10,9 @@ from ..core.artifact_catalog import KNOWN_REPORT_ARTIFACTS +    ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -9,0 +10,9 @@ from ..core.artifact_catalog import KNOWN_REPORT_ARTIFACTS +    ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -9,0 +10,9 @@ from ..core.artifact_catalog import KNOWN_REPORT_ARTIFACTS +)
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -9,0 +10,9 @@ from ..core.artifact_catalog import KNOWN_REPORT_ARTIFACTS +from ..core.engineering_isolation_admission_checklist_artifact_entry import (

### `446b65f9e3ae1da162b95a3d4c03afdacc62caeb`
- 时间: 2026-04-08 12:13:31 +0800
- 标题: chore: sync 2026-04-08 12:13:30
- 涉及文件: `src/gas_calibrator/v2/core/artifact_catalog.py`, `src/gas_calibrator/v2/core/engineering_isolation_admission_checklist.py`, `src/gas_calibrator/v2/scripts/build_offline_governance_artifacts.py`, `tests/v2/test_acceptance_governance.py`, `tests/v2/test_build_offline_governance_artifacts.py`, `tests/v2/test_ui_v2_reports_page.py`, `tests/v2/test_ui_v2_review_center.py`, `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CALIBRATION, REPORT, Report, STEP, Step, cali, calibration, mode
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -37,0 +38 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "engineering_isolation_admission_checklist",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -73,0 +75 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "engineering_isolation_admission_checklist_reviewer_artifact",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -112,0 +115,2 @@ KNOWN_ARTIFACT_KEYS_BY_FILENAME: dict[str, str] = { +    "engineering_isolation_admission_checklist.json": "engineering_isolation_admission_checklist",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -112,0 +115,2 @@ KNOWN_ARTIFACT_KEYS_BY_FILENAME: dict[str, str] = { +    "engineering_isolation_admission_checklist.md": "engineering_isolation_admission_checklist_reviewer_artifact",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -159,0 +164,2 @@ KNOWN_REPORT_ARTIFACTS = [ +    "engineering_isolation_admission_checklist.json",

### `dac9c396fcd65f6afac67ce0c5b62d66d8ae01c6`
- 时间: 2026-04-08 11:48:30 +0800
- 标题: chore: sync 2026-04-08 11:48:30
- 涉及文件: `tests/v2/test_ui_v2_reports_page.py`, `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Step, report, step
- 关键 diff hunk 摘要:
  - tests/v2/test_ui_v2_reports_page.py @@ -706,0 +707 @@ def test_reports_page_artifact_list_surfaces_stage_admission_review_pack_from_sa +    review_center_entry = dict(results_snapshot["review_center"].get("stage_admission_review_pack_artifact_entry", {}) or {})
  - tests/v2/test_ui_v2_reports_page.py @@ -731,0 +733,4 @@ def test_reports_page_artifact_list_surfaces_stage_admission_review_pack_from_sa +        assert pack_entry["summary_text"] == review_center_entry["summary_text"]
  - tests/v2/test_ui_v2_reports_page.py @@ -731,0 +733,4 @@ def test_reports_page_artifact_list_surfaces_stage_admission_review_pack_from_sa +        assert pack_entry["status_line"] == review_center_entry["status_line"]
  - tests/v2/test_ui_v2_reports_page.py @@ -731,0 +733,4 @@ def test_reports_page_artifact_list_surfaces_stage_admission_review_pack_from_sa +        assert pack_entry["engineering_isolation_text"] == review_center_entry["engineering_isolation_text"]
  - tests/v2/test_ui_v2_reports_page.py @@ -731,0 +733,4 @@ def test_reports_page_artifact_list_surfaces_stage_admission_review_pack_from_sa +        assert pack_entry["real_acceptance_text"] == review_center_entry["real_acceptance_text"]

### `27f1bce2567692a8f259f02e20a6ae8554a7bc60`
- 时间: 2026-04-08 11:43:30 +0800
- 标题: chore: sync 2026-04-08 11:43:30
- 涉及文件: `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: diff 内容命中关键词: report
- 关键 diff hunk 摘要:
  - tests/v2/test_ui_v2_review_center_index.py @@ -1438 +1438,3 @@ def test_review_scope_manifest_and_export_index_surface_stage_admission_review_p +    reports_snapshot = facade.get_reports_snapshot(results_snapshot=results_snapshot)
  - tests/v2/test_ui_v2_review_center_index.py @@ -1446,0 +1450,4 @@ def test_review_scope_manifest_and_export_index_surface_stage_admission_review_p +    reports_rows_by_path = {
  - tests/v2/test_ui_v2_review_center_index.py @@ -1446,0 +1450,4 @@ def test_review_scope_manifest_and_export_index_surface_stage_admission_review_p +        for row in list(reports_snapshot.get("files", []) or [])
  - tests/v2/test_ui_v2_review_center_index.py @@ -1449,0 +1457 @@ def test_review_scope_manifest_and_export_index_surface_stage_admission_review_p +    reports_entry = dict(reports_snapshot.get("stage_admission_review_pack_artifact_entry", {}) or {})
  - tests/v2/test_ui_v2_review_center_index.py @@ -1452,0 +1461,4 @@ def test_review_scope_manifest_and_export_index_surface_stage_admission_review_p +    pack_json_row = reports_rows_by_path[pack_json_path]

### `2723e994edccc1b7a76a134f335d3853f1db07ba`
- 时间: 2026-04-08 11:28:30 +0800
- 标题: chore: sync 2026-04-08 11:28:30
- 涉及文件: `configs/default_config_corrected_autodelivery.json`, `configs/default_config_corrected_autodelivery_real_smoke_no500_20260407.json`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: db, delivery, point, span
- 关键 diff hunk 摘要:
  - configs/default_config_corrected_autodelivery.json @@ -636,2 +636,2 @@ -      "gas_route_dewpoint_gate_tail_span_max_c": 0.45,
  - configs/default_config_corrected_autodelivery.json @@ -636,2 +636,2 @@ -      "gas_route_dewpoint_gate_tail_slope_abs_max_c_per_s": 0.0045,
  - configs/default_config_corrected_autodelivery.json @@ -636,2 +636,2 @@ +      "gas_route_dewpoint_gate_tail_span_max_c": 0.6,
  - configs/default_config_corrected_autodelivery.json @@ -636,2 +636,2 @@ +      "gas_route_dewpoint_gate_tail_slope_abs_max_c_per_s": 0.006,
  - configs/default_config_corrected_autodelivery.json @@ -639 +639 @@ -      "gas_route_dewpoint_gate_rebound_min_rise_c": 1.0,

### `0cb3933ec4d740c4e431736d765f730c563499d4`
- 时间: 2026-04-08 10:58:31 +0800
- 标题: chore: sync 2026-04-08 10:58:30
- 涉及文件: `tests/v2/test_ui_v2_app_facade.py`, `tests/v2/test_ui_v2_review_center.py`, `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: diff 内容命中关键词: Step, cali, report, store
- 关键 diff hunk 摘要:
  - tests/v2/test_ui_v2_app_facade.py @@ -17,0 +18,4 @@ from gas_calibrator.v2.scripts.build_offline_governance_artifacts import rebuild +from gas_calibrator.v2.core.stage_admission_review_pack import (
  - tests/v2/test_ui_v2_app_facade.py @@ -1500,0 +1505,32 @@ def test_app_facade_promotes_phase_transition_bridge_reviewer_artifact_into_revi +    run_dir = Path(facade.result_store.run_dir)
  - tests/v2/test_ui_v2_app_facade.py @@ -1500,0 +1505,32 @@ def test_app_facade_promotes_phase_transition_bridge_reviewer_artifact_into_revi +    reports_snapshot = facade.get_reports_snapshot(results_snapshot=results_snapshot)
  - tests/v2/test_ui_v2_app_facade.py @@ -1500,0 +1505,32 @@ def test_app_facade_promotes_phase_transition_bridge_reviewer_artifact_into_revi +    reports_entry = dict(reports_snapshot.get("stage_admission_review_pack_artifact_entry", {}) or {})
  - tests/v2/test_ui_v2_app_facade.py @@ -1500,0 +1505,32 @@ def test_app_facade_promotes_phase_transition_bridge_reviewer_artifact_into_revi +    assert review_center_entry["path"] == reports_entry["path"]

### `4de98f57215028c20576112518a8aa2931db76a5`
- 时间: 2026-04-08 10:53:30 +0800
- 标题: chore: sync 2026-04-08 10:53:30
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/review_center_presenter.py`, `src/gas_calibrator/v2/ui_v2/widgets/review_center_panel.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, db, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1627,0 +1628 @@ class AppFacade: +        stage_admission_review_pack_artifact_entry: dict[str, Any] = {}
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1628,0 +1630 @@ class AppFacade: +            reports_payload = dict(self.results_gateway.read_reports_payload() or {})
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1630 +1632,4 @@ class AppFacade: -                self.results_gateway.read_reports_payload().get("phase_transition_bridge_reviewer_artifact_entry") or {}
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1630 +1632,4 @@ class AppFacade: +                reports_payload.get("phase_transition_bridge_reviewer_artifact_entry") or {}
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1630 +1632,4 @@ class AppFacade: +            )

### `1d765f9ecb188687ed242630f3d9aeaaac562a23`
- 时间: 2026-04-08 10:23:30 +0800
- 标题: chore: sync 2026-04-08 10:23:29
- 涉及文件: `configs/default_config_corrected_autodelivery.json`, `configs/default_config_corrected_autodelivery_real_smoke_no500_20260407.json`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py`, `src/gas_calibrator/workflow/runner.py`, `tests/test_run_v1_corrected_autodelivery.py`, `tests/test_runner_corrected_delivery_hooks.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, Calibration, Readback, SENCO, cali, calibration, coefficient, db
- 关键 diff hunk 摘要:
  - configs/default_config_corrected_autodelivery.json @@ -420 +420,3 @@ -      "fallback_pressure_to_controller": false
  - configs/default_config_corrected_autodelivery.json @@ -420 +420,3 @@ +      "fallback_pressure_to_controller": false,
  - configs/default_config_corrected_autodelivery.json @@ -420 +420,3 @@ +      "pressure_row_source": "startup_calibration",
  - configs/default_config_corrected_autodelivery.json @@ -420 +420,3 @@ +      "write_pressure_coefficients": false
  - configs/default_config_corrected_autodelivery.json @@ -692 +694 @@ -      "pressure_source_preference": "reference_first"

### `ebaae83e7966d89a81b1b8d3cbe5e673dbfa3578`
- 时间: 2026-04-08 10:08:32 +0800
- 标题: chore: sync 2026-04-08 10:08:30
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py`, `src/gas_calibrator/v2/ui_v2/review_scope_export_index.py`, `tests/v2/test_acceptance_governance.py`, `tests/v2/test_build_offline_governance_artifacts.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_reports_page.py`, `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CALIBRATION, STEP, Step, cali, calibration, db, mode, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -15,0 +16,9 @@ from ..core.phase_transition_bridge_reviewer_artifact import PHASE_TRANSITION_BR +from ..core.stage_admission_review_pack import (
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -15,0 +16,9 @@ from ..core.phase_transition_bridge_reviewer_artifact import PHASE_TRANSITION_BR +    STAGE_ADMISSION_REVIEW_PACK_FILENAME,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -15,0 +16,9 @@ from ..core.phase_transition_bridge_reviewer_artifact import PHASE_TRANSITION_BR +    STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -15,0 +16,9 @@ from ..core.phase_transition_bridge_reviewer_artifact import PHASE_TRANSITION_BR +)
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -15,0 +16,9 @@ from ..core.phase_transition_bridge_reviewer_artifact import PHASE_TRANSITION_BR +from ..core.stage_admission_review_pack_artifact_entry import (

### `69ad84c031789f823f9de7cad457b850c4c38ede`
- 时间: 2026-04-08 10:03:37 +0800
- 标题: chore: sync 2026-04-08 10:03:35
- 涉及文件: `src/gas_calibrator/v2/core/artifact_catalog.py`, `src/gas_calibrator/v2/core/stage_admission_review_pack_artifact_entry.py`, `src/gas_calibrator/v2/scripts/build_offline_governance_artifacts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: REPORT, cali, mode, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -36,0 +37 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "stage_admission_review_pack",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -71,0 +73 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "stage_admission_review_pack_reviewer_artifact",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -108,0 +111,2 @@ KNOWN_ARTIFACT_KEYS_BY_FILENAME: dict[str, str] = { +    "stage_admission_review_pack.json": "stage_admission_review_pack",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -108,0 +111,2 @@ KNOWN_ARTIFACT_KEYS_BY_FILENAME: dict[str, str] = { +    "stage_admission_review_pack.md": "stage_admission_review_pack_reviewer_artifact",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -153,0 +158,2 @@ KNOWN_REPORT_ARTIFACTS = [ +    "stage_admission_review_pack.json",

### `d38f25392e4e65f49e8ef71ad090323f720b3315`
- 时间: 2026-04-08 09:28:39 +0800
- 标题: chore: sync 2026-04-08 09:28:38
- 涉及文件: `tests/v2/test_acceptance_governance.py`, `tests/v2/test_ui_v2_reports_page.py`, `tests/v2/test_ui_v2_review_center.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Report, STEP, Step, cali, report, step, store
- 关键 diff hunk 摘要:
  - tests/v2/test_acceptance_governance.py @@ -26,0 +27 @@ from gas_calibrator.v2.core.step2_readiness import build_step2_readiness_summary +from gas_calibrator.v2.core.step2_readiness import STEP2_READINESS_SUMMARY_FILENAME
  - tests/v2/test_ui_v2_reports_page.py @@ -13,0 +14,4 @@ from gas_calibrator.v2.scripts.build_offline_governance_artifacts import rebuild +from gas_calibrator.v2.core.stage_admission_review_pack import (
  - tests/v2/test_ui_v2_reports_page.py @@ -13,0 +14,4 @@ from gas_calibrator.v2.scripts.build_offline_governance_artifacts import rebuild +    STAGE_ADMISSION_REVIEW_PACK_FILENAME,
  - tests/v2/test_ui_v2_reports_page.py @@ -13,0 +14,4 @@ from gas_calibrator.v2.scripts.build_offline_governance_artifacts import rebuild +    STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME,
  - tests/v2/test_ui_v2_reports_page.py @@ -13,0 +14,4 @@ from gas_calibrator.v2.scripts.build_offline_governance_artifacts import rebuild +)

### `a6702705c673c66e96d4461020af700fe5cac8b4`
- 时间: 2026-04-08 09:23:33 +0800
- 标题: chore: sync 2026-04-08 09:23:32
- 涉及文件: `src/gas_calibrator/v2/core/stage_admission_review_pack.py`, `src/gas_calibrator/v2/scripts/build_offline_governance_artifacts.py`, `tests/v2/test_acceptance_governance.py`, `tests/v2/test_build_offline_governance_artifacts.py`, `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CALIBRATION, STEP, Step, cali, calibration, mode, report, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/stage_admission_review_pack.py @@ -0,0 +1,290 @@ +from __future__ import annotations
  - src/gas_calibrator/v2/core/stage_admission_review_pack.py @@ -0,0 +1,290 @@ +
  - src/gas_calibrator/v2/core/stage_admission_review_pack.py @@ -0,0 +1,290 @@ +from datetime import datetime, timezone
  - src/gas_calibrator/v2/core/stage_admission_review_pack.py @@ -0,0 +1,290 @@ +from pathlib import Path
  - src/gas_calibrator/v2/core/stage_admission_review_pack.py @@ -0,0 +1,290 @@ +from typing import Any

### `7136dc034ba897e9d6c84465affd1bdf2812fd4e`
- 时间: 2026-04-08 08:58:35 +0800
- 标题: chore: sync 2026-04-08 08:58:33
- 涉及文件: `src/gas_calibrator/workflow/runner.py`, `tests/test_runner_h2o_sequence.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, Point, cali, co2, h2o, mode, point
- 关键 diff hunk 摘要:
  - src/gas_calibrator/workflow/runner.py @@ -4714,0 +4715,2 @@ class CalibrationRunner: +        if self._selected_pressure_points_is_ambient_only():
  - src/gas_calibrator/workflow/runner.py @@ -4714,0 +4715,2 @@ class CalibrationRunner: +            return [self._ambient_pressure_reference_point(point) for point in expanded]
  - src/gas_calibrator/workflow/runner.py @@ -5155 +5157,3 @@ class CalibrationRunner: -            h2o_points = [point for point in points if point.is_h2o_point]
  - src/gas_calibrator/workflow/runner.py @@ -5155 +5157,3 @@ class CalibrationRunner: +            h2o_points = self._filter_execution_points_by_selected_pressure(
  - src/gas_calibrator/workflow/runner.py @@ -5155 +5157,3 @@ class CalibrationRunner: +                [point for point in points if point.is_h2o_point]

### `7c7d2a20a98096eb6c597f5e14afbfbd3f9e5e96`
- 时间: 2026-04-08 08:48:30 +0800
- 标题: chore: sync 2026-04-08 08:48:30
- 涉及文件: `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: diff 内容命中关键词: report
- 关键 diff hunk 摘要:
  - tests/v2/test_ui_v2_review_center_index.py @@ -1418,2 +1416,0 @@ def test_phase_transition_bridge_reviewer_artifact_stays_in_sync_across_governan -        assert reports_entry["engineering_isolation_text"] in text
  - tests/v2/test_ui_v2_review_center_index.py @@ -1418,2 +1416,0 @@ def test_phase_transition_bridge_reviewer_artifact_stays_in_sync_across_governan -        assert reports_entry["real_acceptance_text"] in text

### `b0828490cb162a61391f5cdc3f105e3ad153640f`
- 时间: 2026-04-08 08:43:32 +0800
- 标题: chore: sync 2026-04-08 08:43:30
- 涉及文件: `tests/v2/test_ui_v2_reports_page.py`, `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Report, Step, cali, report, step, store
- 关键 diff hunk 摘要:
  - tests/v2/test_ui_v2_reports_page.py @@ -12,0 +13 @@ from gas_calibrator.v2.core.phase_transition_bridge_reviewer_artifact_entry impo +from gas_calibrator.v2.scripts.build_offline_governance_artifacts import rebuild_run
  - tests/v2/test_ui_v2_reports_page.py @@ -22 +23 @@ if str(SUPPORT_DIR) not in sys.path: -from ui_v2_support import make_root
  - tests/v2/test_ui_v2_reports_page.py @@ -22 +23 @@ if str(SUPPORT_DIR) not in sys.path: +from ui_v2_support import build_fake_facade, make_root
  - tests/v2/test_ui_v2_reports_page.py @@ -647,0 +649,44 @@ def test_reports_page_artifact_list_surfaces_phase_transition_bridge_reviewer_ma +
  - tests/v2/test_ui_v2_reports_page.py @@ -647,0 +649,44 @@ def test_reports_page_artifact_list_surfaces_phase_transition_bridge_reviewer_ma +def test_reports_page_keeps_phase_bridge_section_aligned_with_reviewer_artifact_entry_from_same_run(

### `2d4753e529876ba94a32677dc78bc6cd309797f6`
- 时间: 2026-04-07 18:58:34 +0800
- 标题: chore: sync 2026-04-07 18:58:31
- 涉及文件: `src/gas_calibrator/v2/ui_v2/widgets/review_center_panel.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/widgets/review_center_panel.py @@ -120,2 +120,2 @@ class ReviewCenterPanel(ttk.LabelFrame): -                "results.review_center.section.phase_bridge_artifact",
  - src/gas_calibrator/v2/ui_v2/widgets/review_center_panel.py @@ -120,2 +120,2 @@ class ReviewCenterPanel(ttk.LabelFrame): -                default="阶段桥独立审阅工件",
  - src/gas_calibrator/v2/ui_v2/widgets/review_center_panel.py @@ -120,2 +120,2 @@ class ReviewCenterPanel(ttk.LabelFrame): +                "results.review_center.section.phase_bridge",
  - src/gas_calibrator/v2/ui_v2/widgets/review_center_panel.py @@ -120,2 +120,2 @@ class ReviewCenterPanel(ttk.LabelFrame): +                default="阶段准入桥",

### `3109e44d131c4b700be4df3b406b4dfa68b2749f`
- 时间: 2026-04-07 18:53:36 +0800
- 标题: chore: sync 2026-04-07 18:53:36
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/review_center_presenter.py`, `src/gas_calibrator/v2/ui_v2/widgets/review_center_panel.py`, `tests/v2/test_ui_v2_app_facade.py`, `tests/v2/test_ui_v2_review_center.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Step, cali, report, store
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1626,0 +1627,7 @@ class AppFacade: +        phase_bridge_reviewer_artifact_entry: dict[str, Any] = {}
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1626,0 +1627,7 @@ class AppFacade: +        try:
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1626,0 +1627,7 @@ class AppFacade: +            phase_bridge_reviewer_artifact_entry = dict(
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1626,0 +1627,7 @@ class AppFacade: +                self.results_gateway.read_reports_payload().get("phase_transition_bridge_reviewer_artifact_entry") or {}
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1626,0 +1627,7 @@ class AppFacade: +            )

### `4abb2b6462c4ddfd17f8af54e802bfa5e0cfd5bc`
- 时间: 2026-04-07 18:33:32 +0800
- 标题: chore: sync 2026-04-07 18:33:31
- 涉及文件: `configs/default_config_corrected_autodelivery.json`, `configs/default_config_corrected_autodelivery_real_smoke_no500_20260407.json`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: delivery, h2o, point, span
- 关键 diff hunk 摘要:
  - configs/default_config_corrected_autodelivery.json @@ -407 +407 @@ -      "flush_soak_s": 10.0,
  - configs/default_config_corrected_autodelivery.json @@ -407 +407 @@ +      "flush_soak_s": 120.0,
  - configs/default_config_corrected_autodelivery.json @@ -487,3 +487,3 @@ -      "h2o_postseal_dewpoint_timeout_s": 5.5,
  - configs/default_config_corrected_autodelivery.json @@ -487,3 +487,3 @@ -      "h2o_postseal_dewpoint_span_c": 0.18,
  - configs/default_config_corrected_autodelivery.json @@ -487,3 +487,3 @@ -      "h2o_postseal_dewpoint_slope_c_per_s": 0.06,

### `bbe680f0416debbd37ddb257506d15504baa3bc8`
- 时间: 2026-04-07 18:18:33 +0800
- 标题: chore: sync 2026-04-07 18:18:32
- 涉及文件: `src/gas_calibrator/senco_format.py`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py`, `src/gas_calibrator/workflow/runner.py`, `tests/test_senco_format.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, SENCO, cali, coefficient, db, delivery, mode, point
- 关键 diff hunk 摘要:
  - src/gas_calibrator/senco_format.py @@ -33,0 +34,22 @@ def format_senco_values(values: Sequence[Any]) -> Tuple[str, ...]: +def rounded_senco_values(values: Sequence[Any]) -> Tuple[float, ...]:
  - src/gas_calibrator/senco_format.py @@ -33,0 +34,22 @@ def format_senco_values(values: Sequence[Any]) -> Tuple[str, ...]: +    """Round values the same way SENCO payload formatting does, then parse back to float."""
  - src/gas_calibrator/senco_format.py @@ -33,0 +34,22 @@ def format_senco_values(values: Sequence[Any]) -> Tuple[str, ...]: +
  - src/gas_calibrator/senco_format.py @@ -33,0 +34,22 @@ def format_senco_values(values: Sequence[Any]) -> Tuple[str, ...]: +    return tuple(float(text) for text in format_senco_values(values))
  - src/gas_calibrator/senco_format.py @@ -33,0 +34,22 @@ def format_senco_values(values: Sequence[Any]) -> Tuple[str, ...]: +def senco_readback_matches(expected: Sequence[Any], actual: Sequence[Any], *, atol: float = 1e-9) -> bool:

### `dae9f1432d1555fd68d579637b0cbd67a19b6500`
- 时间: 2026-04-07 18:13:31 +0800
- 标题: chore: sync 2026-04-07 18:13:30
- 涉及文件: `configs/default_config_corrected_autodelivery.json`, `configs/default_config_corrected_autodelivery_real_smoke_no500_20260407.json`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: delivery
- 关键 diff hunk 摘要:
  - configs/default_config_corrected_autodelivery.json @@ -408 +408 @@ -      "sample_duration_s": 8.0,
  - configs/default_config_corrected_autodelivery.json @@ -408 +408 @@ +      "sample_duration_s": 15.0,
  - configs/default_config_corrected_autodelivery_real_smoke_no500_20260407.json @@ -408 +408 @@ -      "sample_duration_s": 8.0,
  - configs/default_config_corrected_autodelivery_real_smoke_no500_20260407.json @@ -408 +408 @@ +      "sample_duration_s": 15.0,
  - configs/default_config_corrected_autodelivery_real_smoke_no500_20260407.json @@ -861 +861 @@ -}

### `15a207b9b973c613b17cbfd8f6542d2efdf80583`
- 时间: 2026-04-07 18:08:32 +0800
- 标题: chore: sync 2026-04-07 18:08:31
- 涉及文件: `src/gas_calibrator/workflow/runner.py`, `tests/test_runner_h2o_sequence.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, cali, db, h2o
- 关键 diff hunk 摘要:
  - src/gas_calibrator/workflow/runner.py @@ -9952,0 +9953,12 @@ class CalibrationRunner: +        except Exception as fast_exc:
  - src/gas_calibrator/workflow/runner.py @@ -9952,0 +9953,12 @@ class CalibrationRunner: +            self.log(f"Pre-seal pressure-gauge fast read failed: {fast_exc}; retrying normal read")
  - src/gas_calibrator/workflow/runner.py @@ -9952,0 +9953,12 @@ class CalibrationRunner: +            try:
  - src/gas_calibrator/workflow/runner.py @@ -9952,0 +9953,12 @@ class CalibrationRunner: +                value = self._read_pressure_gauge_value(fast=False)
  - src/gas_calibrator/workflow/runner.py @@ -9952,0 +9953,12 @@ class CalibrationRunner: +            except Exception as exc:

### `2e7bed849e7d2be0232d85f354c20e40cf5f6697`
- 时间: 2026-04-07 17:58:33 +0800
- 标题: chore: sync 2026-04-07 17:58:32
- 涉及文件: `configs/default_config_corrected_autodelivery_real_smoke_no500_20260407.json`, `configs/points_real_smoke_corrected_autodelivery_no500_min_20260407.xlsx`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, H2O, Mode, calibration, co2, coefficient, delivery, h2o
- 关键 diff hunk 摘要:
  - configs/default_config_corrected_autodelivery_real_smoke_no500_20260407.json @@ -0,0 +1,861 @@ +{
  - configs/default_config_corrected_autodelivery_real_smoke_no500_20260407.json @@ -0,0 +1,861 @@ +  "devices": {
  - configs/default_config_corrected_autodelivery_real_smoke_no500_20260407.json @@ -0,0 +1,861 @@ +    "pressure_controller": {
  - configs/default_config_corrected_autodelivery_real_smoke_no500_20260407.json @@ -0,0 +1,861 @@ +      "enabled": true,
  - configs/default_config_corrected_autodelivery_real_smoke_no500_20260407.json @@ -0,0 +1,861 @@ +      "port": "COM31",

### `a92ea047e483f5183de7f85eb566690ae5a574f6`
- 时间: 2026-04-07 17:53:30 +0800
- 标题: chore: sync 2026-04-07 17:53:30
- 涉及文件: `configs/default_config_corrected_autodelivery.json`, `tests/test_pressure_point_selection.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: co2, delivery, mode, point, span
- 关键 diff hunk 摘要:
  - configs/default_config_corrected_autodelivery.json @@ -288 +288,9 @@ -    "selected_pressure_points": [],
  - configs/default_config_corrected_autodelivery.json @@ -288 +288,9 @@ +    "selected_pressure_points": [
  - configs/default_config_corrected_autodelivery.json @@ -288 +288,9 @@ +      "ambient",
  - configs/default_config_corrected_autodelivery.json @@ -288 +288,9 @@ +      1100,
  - configs/default_config_corrected_autodelivery.json @@ -288 +288,9 @@ +      1000,

### `54feeb15dbba6dfc23d209cfe08a34dd8c67a6b8`
- 时间: 2026-04-07 17:43:30 +0800
- 标题: chore: sync 2026-04-07 17:43:30
- 涉及文件: `configs/default_config_corrected_autodelivery.json`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, H2O, Mode, calibration, co2, coefficient, db, delivery
- 关键 diff hunk 摘要:
  - configs/default_config_corrected_autodelivery.json @@ -0,0 +1,851 @@ +{
  - configs/default_config_corrected_autodelivery.json @@ -0,0 +1,851 @@ +  "devices": {
  - configs/default_config_corrected_autodelivery.json @@ -0,0 +1,851 @@ +    "pressure_controller": {
  - configs/default_config_corrected_autodelivery.json @@ -0,0 +1,851 @@ +      "enabled": true,
  - configs/default_config_corrected_autodelivery.json @@ -0,0 +1,851 @@ +      "port": "COM31",

### `e95111aa4c04bb49ceb2acd2ca7893ff035875a3`
- 时间: 2026-04-07 17:38:31 +0800
- 标题: chore: sync 2026-04-07 17:38:30
- 涉及文件: `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, coefficient, delivery, v1
- 关键 diff hunk 摘要:
  - src/gas_calibrator/tools/run_v1_corrected_autodelivery.py @@ -548,0 +549,2 @@ def build_corrected_delivery( +    _write_csv(output_dir / "fit_summary_no_500.csv", summary.to_dict(orient="records"))
  - src/gas_calibrator/tools/run_v1_corrected_autodelivery.py @@ -548,0 +549,2 @@ def build_corrected_delivery( +    _write_csv(output_dir / "simplified_coefficients_no_500.csv", simplified.to_dict(orient="records"))

### `8fa3f3ecd44e1c3a853e095f182da6f9cf70e27e`
- 时间: 2026-04-07 17:33:30 +0800
- 标题: chore: sync 2026-04-07 17:33:29
- 涉及文件: `src/gas_calibrator/tools/validate_verification_doc.py`, `src/gas_calibrator/workflow/runner.py`, `tests/test_run_v1_corrected_autodelivery.py`, `tests/test_runner_corrected_delivery_hooks.py`, `tests/test_validate_verification_doc_targets.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, Calibration, H2O, Point, Readback, SENCO, cali, calibration
- 关键 diff hunk 摘要:
  - src/gas_calibrator/tools/validate_verification_doc.py @@ -184,0 +185 @@ def _load_targets(cfg: Mapping[str, Any], targets_json: Optional[str]) -> List[D +        explicit_targets = True
  - src/gas_calibrator/tools/validate_verification_doc.py @@ -186,0 +188 @@ def _load_targets(cfg: Mapping[str, Any], targets_json: Optional[str]) -> List[D +        explicit_targets = False
  - src/gas_calibrator/tools/validate_verification_doc.py @@ -192 +194 @@ def _load_targets(cfg: Mapping[str, Any], targets_json: Optional[str]) -> List[D -        if device_id not in DEFAULT_DEVICE_IDS:
  - src/gas_calibrator/tools/validate_verification_doc.py @@ -192 +194 @@ def _load_targets(cfg: Mapping[str, Any], targets_json: Optional[str]) -> List[D +        if (not explicit_targets) and device_id not in DEFAULT_DEVICE_IDS:
  - src/gas_calibrator/tools/validate_verification_doc.py @@ -434,7 +436,20 @@ def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace: -def main(argv: Optional[Iterable[str]] = None) -> int:

### `5570747dbe0765109d5eaba6663699657938ebe1`
- 时间: 2026-04-07 17:28:31 +0800
- 标题: chore: sync 2026-04-07 17:28:31
- 涉及文件: `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, H2O, MODE, Mode, Readback, SENCO, cali, calibration
- 关键 diff hunk 摘要:
  - src/gas_calibrator/tools/run_v1_corrected_autodelivery.py @@ -0,0 +1,646 @@ +from __future__ import annotations
  - src/gas_calibrator/tools/run_v1_corrected_autodelivery.py @@ -0,0 +1,646 @@ +
  - src/gas_calibrator/tools/run_v1_corrected_autodelivery.py @@ -0,0 +1,646 @@ +import csv
  - src/gas_calibrator/tools/run_v1_corrected_autodelivery.py @@ -0,0 +1,646 @@ +import json
  - src/gas_calibrator/tools/run_v1_corrected_autodelivery.py @@ -0,0 +1,646 @@ +import math

### `9feaad83062e3356bdc9d47e29af626eacf5f604`
- 时间: 2026-04-07 17:08:31 +0800
- 标题: chore: sync 2026-04-07 17:08:30
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py`, `tests/v2/test_acceptance_governance.py`, `tests/v2/test_results_gateway.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -14,0 +15 @@ from ..core.phase_transition_bridge_reviewer_artifact_entry import ( +from ..core.phase_transition_bridge_reviewer_artifact import PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -188,0 +190,5 @@ class ResultsGateway: +        reviewer_artifact_path = str(reviewer_artifact_section.get("path") or "").strip()
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -188,0 +190,5 @@ class ResultsGateway: +        if not reviewer_artifact_path:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -188,0 +190,5 @@ class ResultsGateway: +            fallback_path = self.run_dir / PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -188,0 +190,5 @@ class ResultsGateway: +            if fallback_path.exists():

### `76e1102551c1e5445b1c9312619df77a8850c5a8`
- 时间: 2026-04-07 17:03:33 +0800
- 标题: chore: sync 2026-04-07 17:03:30
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/artifact_catalog.py`, `src/gas_calibrator/v2/core/phase_transition_bridge_reviewer_artifact_entry.py`, `src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py`, `src/gas_calibrator/v2/ui_v2/review_scope_export_index.py`, `tests/v2/test_acceptance_governance.py`, `tests/v2/test_build_offline_governance_artifacts.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_reports_page.py`, `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: REPORT, Report, Step, cali, calibration, coefficient, mode, point
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -10,0 +11,4 @@ from ..core.offline_artifacts import build_point_taxonomy_handoff, summarize_off +from ..core.phase_transition_bridge_reviewer_artifact_entry import (
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -10,0 +11,4 @@ from ..core.offline_artifacts import build_point_taxonomy_handoff, summarize_off +    PHASE_TRANSITION_BRIDGE_REVIEWER_ARTIFACT_KEY,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -10,0 +11,4 @@ from ..core.offline_artifacts import build_point_taxonomy_handoff, summarize_off +    build_phase_transition_bridge_reviewer_artifact_entry,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -10,0 +11,4 @@ from ..core.offline_artifacts import build_point_taxonomy_handoff, summarize_off +)
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -178,0 +183,11 @@ class ResultsGateway: +        analytics_summary = dict(payload.get("analytics_summary", {}) or {})

### `6d97505021e3295a675ef9def0945c44b88e78d4`
- 时间: 2026-04-07 16:18:36 +0800
- 标题: chore: sync 2026-04-07 16:18:35
- 涉及文件: `src/gas_calibrator/v2/core/phase_transition_bridge_presenter.py`, `src/gas_calibrator/v2/core/phase_transition_bridge_reviewer_artifact.py`, `src/gas_calibrator/v2/scripts/build_offline_governance_artifacts.py`, `tests/v2/test_acceptance_governance.py`, `tests/v2/test_build_offline_governance_artifacts.py`, `tests/v2/test_ui_v2_reports_page.py`, `tests/v2/test_ui_v2_review_center.py`, `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, db, report, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/phase_transition_bridge_presenter.py @@ -49,0 +50,10 @@ def build_phase_transition_bridge_digest( +    engineering_isolation_text = str(reviewer_display.get("engineering_isolation_text") or "").strip() or (
  - src/gas_calibrator/v2/core/phase_transition_bridge_presenter.py @@ -49,0 +50,10 @@ def build_phase_transition_bridge_digest( +        _default_engineering_isolation_text(
  - src/gas_calibrator/v2/core/phase_transition_bridge_presenter.py @@ -49,0 +50,10 @@ def build_phase_transition_bridge_digest( +            ready_for_engineering_isolation=ready_for_engineering_isolation,
  - src/gas_calibrator/v2/core/phase_transition_bridge_presenter.py @@ -49,0 +50,10 @@ def build_phase_transition_bridge_digest( +        )
  - src/gas_calibrator/v2/core/phase_transition_bridge_presenter.py @@ -49,0 +50,10 @@ def build_phase_transition_bridge_digest( +    )

### `9736005b78557c856b1b0f0f2ce67db2f32ecbba`
- 时间: 2026-04-07 15:48:38 +0800
- 标题: chore: sync 2026-04-07 15:48:38
- 涉及文件: `src/gas_calibrator/v2/core/phase_transition_bridge_reviewer_artifact.py`, `src/gas_calibrator/v2/scripts/build_offline_governance_artifacts.py`, `tests/v2/test_acceptance_governance.py`, `tests/v2/test_build_offline_governance_artifacts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Step, cali, calibration, db, mode, report, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/phase_transition_bridge_reviewer_artifact.py @@ -0,0 +1,110 @@ +from __future__ import annotations
  - src/gas_calibrator/v2/core/phase_transition_bridge_reviewer_artifact.py @@ -0,0 +1,110 @@ +
  - src/gas_calibrator/v2/core/phase_transition_bridge_reviewer_artifact.py @@ -0,0 +1,110 @@ +from typing import Any
  - src/gas_calibrator/v2/core/phase_transition_bridge_reviewer_artifact.py @@ -0,0 +1,110 @@ +from .phase_transition_bridge_presenter import build_phase_transition_bridge_panel_payload
  - src/gas_calibrator/v2/core/phase_transition_bridge_reviewer_artifact.py @@ -0,0 +1,110 @@ +PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME = "phase_transition_bridge_reviewer.md"

### `801e7bf8b409b72220c5c8b21002a3651ebb2b8f`
- 时间: 2026-04-07 15:13:38 +0800
- 标题: chore: sync 2026-04-07 15:13:31
- 涉及文件: `src/gas_calibrator/tools/run_v1_no500_postprocess.py`, `tests/test_v1_no500_postprocess.py`, `tests/v2/test_ui_v2_reports_page.py`, `tests/v2/test_ui_v2_review_center.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, H2O, Mode, Point, Report, Step, V1, cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/tools/run_v1_no500_postprocess.py @@ -0,0 +1,223 @@ +"""Canonical offline postprocess entry that removes 500 hPa rows first.
  - src/gas_calibrator/tools/run_v1_no500_postprocess.py @@ -0,0 +1,223 @@ +
  - src/gas_calibrator/tools/run_v1_no500_postprocess.py @@ -0,0 +1,223 @@ +This keeps the "2026-04-03 no-500" workflow explicit and reproducible:
  - src/gas_calibrator/tools/run_v1_no500_postprocess.py @@ -0,0 +1,223 @@ +1. filter completed summary rows to exclude 500 hPa points
  - src/gas_calibrator/tools/run_v1_no500_postprocess.py @@ -0,0 +1,223 @@ +2. export the standard calibration workbook from filtered summaries

### `8198cecedcea939df6b3044f1d3f5e066544927a`
- 时间: 2026-04-07 14:48:30 +0800
- 标题: chore: sync 2026-04-07 14:48:30
- 涉及文件: `tests/v2/test_build_offline_governance_artifacts.py`, `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: diff 内容命中关键词: Step, cali, mode, report, step
- 关键 diff hunk 摘要:
  - tests/v2/test_build_offline_governance_artifacts.py @@ -5,0 +6,3 @@ from pathlib import Path +from gas_calibrator.v2.core.phase_transition_bridge_presenter import (
  - tests/v2/test_build_offline_governance_artifacts.py @@ -253,0 +270,12 @@ def test_rebuild_run_generates_governance_artifacts(tmp_path: Path) -> None: +    assert "Step 2 tail / Stage 3 bridge" in section_text
  - tests/v2/test_ui_v2_review_center_index.py @@ -21,0 +22,3 @@ from gas_calibrator.v2.ui_v2.review_scope_export_index import ( +from gas_calibrator.v2.core.phase_transition_bridge_presenter import (
  - tests/v2/test_ui_v2_review_center_index.py @@ -1080,0 +1084,125 @@ def test_review_scope_manifest_markdown_hydrates_top_level_reviewer_fields_when_ +        "phase": "step2_tail_stage3_bridge",
  - tests/v2/test_ui_v2_review_center_index.py @@ -1080,0 +1084,125 @@ def test_review_scope_manifest_markdown_hydrates_top_level_reviewer_fields_when_ +        "mode": "simulation_only",

### `a544606e416d5316b75c4eb9183d5aa3a0ad86f4`
- 时间: 2026-04-07 14:43:38 +0800
- 标题: chore: sync 2026-04-07 14:43:37
- 涉及文件: `src/gas_calibrator/v2/scripts/build_offline_governance_artifacts.py`, `src/gas_calibrator/v2/ui_v2/review_scope_export_index.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/scripts/build_offline_governance_artifacts.py @@ -23,0 +24 @@ from ..core.phase_transition_bridge import ( +from ..core.phase_transition_bridge_presenter import build_phase_transition_bridge_panel_payload
  - src/gas_calibrator/v2/scripts/build_offline_governance_artifacts.py @@ -88,0 +90 @@ def _augment_run_payload_with_step2_readiness( +    phase_transition_bridge_surface_bundle = build_phase_transition_bridge_panel_payload(phase_transition_bridge)
  - src/gas_calibrator/v2/scripts/build_offline_governance_artifacts.py @@ -89,0 +92 @@ def _augment_run_payload_with_step2_readiness( +    analytics_summary["phase_transition_bridge_reviewer_section"] = dict(phase_transition_bridge_surface_bundle)
  - src/gas_calibrator/v2/scripts/build_offline_governance_artifacts.py @@ -127,0 +131 @@ def _augment_run_payload_with_step2_readiness( +    summary_stats["phase_transition_bridge_reviewer_section"] = dict(phase_transition_bridge_surface_bundle)
  - src/gas_calibrator/v2/scripts/build_offline_governance_artifacts.py @@ -179,0 +184 @@ def _augment_run_payload_with_step2_readiness( +    manifest_sections["phase_transition_bridge_reviewer_section"] = dict(phase_transition_bridge_surface_bundle)

### `a9aea7ae9018b1aa231428b75e9702f7194caa9b`
- 时间: 2026-04-07 14:18:37 +0800
- 标题: chore: sync 2026-04-07 14:18:36
- 涉及文件: `src/gas_calibrator/v2/core/phase_transition_bridge_presenter.py`, `src/gas_calibrator/v2/ui_v2/pages/reports_page.py`, `src/gas_calibrator/v2/ui_v2/review_center_presenter.py`, `src/gas_calibrator/v2/ui_v2/widgets/review_center_panel.py`, `tests/v2/test_ui_v2_reports_page.py`, `tests/v2/test_ui_v2_review_center.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Report, Step, cali, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/phase_transition_bridge_presenter.py @@ -138,0 +139,24 @@ def build_phase_transition_bridge_panel_payload( +def _panel_warning_text(warning_text: str) -> str:
  - src/gas_calibrator/v2/core/phase_transition_bridge_presenter.py @@ -138,0 +139,24 @@ def build_phase_transition_bridge_panel_payload( +    required_prefix = "提示："
  - src/gas_calibrator/v2/core/phase_transition_bridge_presenter.py @@ -138,0 +139,24 @@ def build_phase_transition_bridge_panel_payload( +    required_parts = [
  - src/gas_calibrator/v2/core/phase_transition_bridge_presenter.py @@ -138,0 +139,24 @@ def build_phase_transition_bridge_panel_payload( +        "不是 real acceptance",
  - src/gas_calibrator/v2/core/phase_transition_bridge_presenter.py @@ -138,0 +139,24 @@ def build_phase_transition_bridge_panel_payload( +        "不能替代真实计量验证",

### `a95ef9196126f9d7a4c8b3b1f6d8a59cc42dd32d`
- 时间: 2026-04-07 14:13:31 +0800
- 标题: chore: sync 2026-04-07 14:13:30
- 涉及文件: `src/gas_calibrator/v2/core/phase_transition_bridge_presenter.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/phase_transition_bridge_presenter.py @@ -81,0 +82,57 @@ def build_phase_transition_bridge_digest( +def build_phase_transition_bridge_panel_payload(
  - src/gas_calibrator/v2/core/phase_transition_bridge_presenter.py @@ -81,0 +82,57 @@ def build_phase_transition_bridge_digest( +    bridge: dict[str, Any] | None,
  - src/gas_calibrator/v2/core/phase_transition_bridge_presenter.py @@ -81,0 +82,57 @@ def build_phase_transition_bridge_digest( +) -> dict[str, Any]:
  - src/gas_calibrator/v2/core/phase_transition_bridge_presenter.py @@ -81,0 +82,57 @@ def build_phase_transition_bridge_digest( +    digest = build_phase_transition_bridge_digest(bridge)
  - src/gas_calibrator/v2/core/phase_transition_bridge_presenter.py @@ -81,0 +82,57 @@ def build_phase_transition_bridge_digest( +    if not digest.get("available"):

### `ba8caa05f7b3db7a1a032fb73fa35e256cc932d1`
- 时间: 2026-04-07 14:08:30 +0800
- 标题: chore: sync 2026-04-07 14:08:30
- 涉及文件: `configs/tmp_pressure_sensor_offset_zero_1000_4ch.json`, `docs/pressure_calibration_collection_plan.md`, `src/gas_calibrator/v2/core/metrology_calibration_contract.py`, `src/gas_calibrator/v2/core/phase_transition_bridge.py`, `src/gas_calibrator/v2/core/phase_transition_bridge_presenter.py`, `src/gas_calibrator/v2/scripts/build_offline_governance_artifacts.py`, `src/gas_calibrator/v2/ui_v2/pages/reports_page.py`, `src/gas_calibrator/v2/ui_v2/review_center_presenter.py`, `tests/v2/test_acceptance_governance.py`, `tests/v2/test_build_offline_governance_artifacts.py`, `tests/v2/test_ui_v2_reports_page.py`, `tests/v2/test_ui_v2_review_center.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CALIBRATION, Report, STEP, Step, V1, cali, calibration, co2
- 关键 diff hunk 摘要:
  - configs/tmp_pressure_sensor_offset_zero_1000_4ch.json @@ -0,0 +1,418 @@ +﻿{
  - configs/tmp_pressure_sensor_offset_zero_1000_4ch.json @@ -0,0 +1,418 @@ +  "devices": {
  - configs/tmp_pressure_sensor_offset_zero_1000_4ch.json @@ -0,0 +1,418 @@ +    "pressure_controller": {
  - configs/tmp_pressure_sensor_offset_zero_1000_4ch.json @@ -0,0 +1,418 @@ +      "enabled": true,
  - configs/tmp_pressure_sensor_offset_zero_1000_4ch.json @@ -0,0 +1,418 @@ +      "port": "COM31",

### `596e24d220a27f7cb5e367d3ddce701e13686fc1`
- 时间: 2026-04-07 12:53:35 +0800
- 标题: chore: sync 2026-04-07 12:53:35
- 涉及文件: `src/gas_calibrator/v2/core/step2_readiness.py`, `src/gas_calibrator/v2/scripts/build_offline_governance_artifacts.py`, `tests/v2/test_acceptance_governance.py`, `tests/v2/test_build_offline_governance_artifacts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, report, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/step2_readiness.py @@ -2,0 +3 @@ from __future__ import annotations +from collections import Counter
  - src/gas_calibrator/v2/core/step2_readiness.py @@ -189,0 +191 @@ def build_step2_readiness_summary( +    gate_status_counts = dict(Counter(str(gate.get("status") or "unknown") for gate in gates))
  - src/gas_calibrator/v2/core/step2_readiness.py @@ -228,0 +231,2 @@ def build_step2_readiness_summary( +        "ready_for_engineering_isolation": overall_status == "ready_for_engineering_isolation",
  - src/gas_calibrator/v2/core/step2_readiness.py @@ -228,0 +231,2 @@ def build_step2_readiness_summary( +        "real_acceptance_ready": False,
  - src/gas_calibrator/v2/core/step2_readiness.py @@ -234,0 +239 @@ def build_step2_readiness_summary( +        "gate_status_counts": gate_status_counts,

### `be60a9e2357b9baf462bec8bb9c834d5e4b34069`
- 时间: 2026-04-07 12:48:30 +0800
- 标题: chore: sync 2026-04-07 12:48:29
- 涉及文件: `src/gas_calibrator/v2/core/step2_readiness.py`, `tests/v2/test_acceptance_governance.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Step, cali, mode, report, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/step2_readiness.py @@ -32,0 +33 @@ _GATE_LABELS = { +    "readiness_evidence_complete": "治理证据完整性",
  - src/gas_calibrator/v2/core/step2_readiness.py @@ -56,0 +58,2 @@ def build_step2_readiness_summary( +    evidence_completeness = _build_evidence_completeness(governance)
  - src/gas_calibrator/v2/core/step2_readiness.py @@ -56,0 +58,2 @@ def build_step2_readiness_summary( +    evidence_complete = bool(evidence_completeness.get("complete", False))
  - src/gas_calibrator/v2/core/step2_readiness.py @@ -151,0 +155,8 @@ def build_step2_readiness_summary( +        _gate(
  - src/gas_calibrator/v2/core/step2_readiness.py @@ -151,0 +155,8 @@ def build_step2_readiness_summary( +            "readiness_evidence_complete",

### `3cb1a78b8fdf874b4a8066608ad9f31be5a6cc17`
- 时间: 2026-04-07 09:43:29 +0800
- 标题: chore: sync 2026-04-07 09:43:29
- 涉及文件: `src/gas_calibrator/tools/run_room_temp_co2_pressure_diagnostic.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, co2
- 关键 diff hunk 摘要:
  - src/gas_calibrator/tools/run_room_temp_co2_pressure_diagnostic.py @@ -1781,0 +1782 @@ def _run_pressure_sweep_phase( +    phase_gate_live_path: Path,
  - src/gas_calibrator/tools/run_room_temp_co2_pressure_diagnostic.py @@ -4110,0 +4112 @@ def main(argv: Optional[Iterable[str]] = None) -> int: +                                phase_gate_live_path,

### `e53280643ea2a864ca985a5bfadb377b97c28ecc`
- 时间: 2026-04-07 09:23:30 +0800
- 标题: chore: sync 2026-04-07 09:23:29
- 涉及文件: `src/gas_calibrator/tools/run_room_temp_co2_pressure_diagnostic.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, co2, db
- 关键 diff hunk 摘要:
  - src/gas_calibrator/tools/run_room_temp_co2_pressure_diagnostic.py @@ -1780,0 +1781 @@ def _run_pressure_sweep_phase( +    pressure_summary_live_path: Path,
  - src/gas_calibrator/tools/run_room_temp_co2_pressure_diagnostic.py @@ -4108,0 +4110 @@ def main(argv: Optional[Iterable[str]] = None) -> int: +                                pressure_summary_live_path,

### `847b18e1236abab203ac0e161e12b43a05368340`
- 时间: 2026-04-07 08:58:30 +0800
- 标题: chore: sync 2026-04-07 08:58:30
- 涉及文件: `tests/test_room_temp_co2_pressure_diagnostic.py`, `tests/test_single_gas_pressure_curve.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, co2, mode, point, span
- 关键 diff hunk 摘要:
  - tests/test_room_temp_co2_pressure_diagnostic.py @@ -1624,0 +1625,72 @@ def test_append_live_csv_row_rewrites_with_expanded_header(tmp_path) -> None: +
  - tests/test_room_temp_co2_pressure_diagnostic.py @@ -1624,0 +1625,72 @@ def test_append_live_csv_row_rewrites_with_expanded_header(tmp_path) -> None: +def test_capture_phase_rows_records_extended_analyzer_fields(monkeypatch) -> None:
  - tests/test_room_temp_co2_pressure_diagnostic.py @@ -1624,0 +1625,72 @@ def test_append_live_csv_row_rewrites_with_expanded_header(tmp_path) -> None: +    class FakeAnalyzer:
  - tests/test_room_temp_co2_pressure_diagnostic.py @@ -1624,0 +1625,72 @@ def test_append_live_csv_row_rewrites_with_expanded_header(tmp_path) -> None: +        active_send = False
  - tests/test_room_temp_co2_pressure_diagnostic.py @@ -1624,0 +1625,72 @@ def test_append_live_csv_row_rewrites_with_expanded_header(tmp_path) -> None: +        def read_latest_data(self, **kwargs):

### `efb65fce213dca491ae6e859355e943f32d8ea63`
- 时间: 2026-04-07 08:53:30 +0800
- 标题: chore: sync 2026-04-07 08:53:30
- 涉及文件: `src/gas_calibrator/tools/export_single_gas_pressure_curve.py`, `src/gas_calibrator/tools/run_room_temp_co2_pressure_diagnostic.py`, `src/gas_calibrator/validation/single_gas_pressure_curve.py`, `tests/test_room_temp_co2_pressure_diagnostic.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, Point, cali, co2, mode, point, save, span
- 关键 diff hunk 摘要:
  - src/gas_calibrator/tools/export_single_gas_pressure_curve.py @@ -0,0 +1,69 @@ +"""Export point means and simple pressure-relationship plots from a room-temp diagnostic run."""
  - src/gas_calibrator/tools/export_single_gas_pressure_curve.py @@ -0,0 +1,69 @@ +
  - src/gas_calibrator/tools/export_single_gas_pressure_curve.py @@ -0,0 +1,69 @@ +from __future__ import annotations
  - src/gas_calibrator/tools/export_single_gas_pressure_curve.py @@ -0,0 +1,69 @@ +import argparse
  - src/gas_calibrator/tools/export_single_gas_pressure_curve.py @@ -0,0 +1,69 @@ +import csv

### `847b4122d520330b334e5bdaa96dab070f9e8567`
- 时间: 2026-04-07 08:43:33 +0800
- 标题: chore: sync 2026-04-07 08:43:32
- 涉及文件: `src/gas_calibrator/v2/core/step2_readiness.py`, `src/gas_calibrator/v2/scripts/build_offline_governance_artifacts.py`, `tests/v2/test_acceptance_governance.py`, `tests/v2/test_build_offline_governance_artifacts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CALI, STEP, Step, cali, mode, point, report, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/step2_readiness.py @@ -0,0 +1,338 @@ +from __future__ import annotations
  - src/gas_calibrator/v2/core/step2_readiness.py @@ -0,0 +1,338 @@ +
  - src/gas_calibrator/v2/core/step2_readiness.py @@ -0,0 +1,338 @@ +from datetime import datetime, timezone
  - src/gas_calibrator/v2/core/step2_readiness.py @@ -0,0 +1,338 @@ +from pathlib import Path
  - src/gas_calibrator/v2/core/step2_readiness.py @@ -0,0 +1,338 @@ +from typing import Any

### `9ac10e7a179a9cec4ef88a0803c0cd1f3231f86e`
- 时间: 2026-04-07 08:33:33 +0800
- 标题: chore: sync 2026-04-07 08:33:32
- 涉及文件: `src/gas_calibrator/workflow/runner.py`, `tests/test_runner_h2o_sequence.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, Calibration, cali, co2, h2o, point, span
- 关键 diff hunk 摘要:
  - src/gas_calibrator/workflow/runner.py @@ -10555,0 +10556,14 @@ class CalibrationRunner: +
  - src/gas_calibrator/workflow/runner.py @@ -10555,0 +10556,14 @@ class CalibrationRunner: +        def _snapshot_from_payload(payload: Any) -> Optional[Dict[str, Any]]:
  - src/gas_calibrator/workflow/runner.py @@ -10555,0 +10556,14 @@ class CalibrationRunner: +            if not isinstance(payload, dict):
  - src/gas_calibrator/workflow/runner.py @@ -10555,0 +10556,14 @@ class CalibrationRunner: +                return None
  - src/gas_calibrator/workflow/runner.py @@ -10555,0 +10556,14 @@ class CalibrationRunner: +            snapshot = {

### `669e2ab033f9a6e331b8cd1beb04090c4eccd182`
- 时间: 2026-04-06 23:38:30 +0800
- 标题: chore: sync 2026-04-06 23:38:29
- 涉及文件: `src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py`, `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py @@ -9 +8,0 @@ from ..review_surface_formatter import ( -    build_review_scope_payload_reviewer_display,
  - src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py @@ -10,0 +10 @@ from ..review_surface_formatter import ( +    hydrate_review_scope_reviewer_display,
  - src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py @@ -354 +354,5 @@ def render_review_scope_manifest_markdown(payload: dict[str, Any]) -> str: -    reviewer_display = dict(payload.get("reviewer_display", {}) or {}) or build_review_scope_payload_reviewer_display(
  - src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py @@ -354 +354,5 @@ def render_review_scope_manifest_markdown(payload: dict[str, Any]) -> str: +    reviewer_display = hydrate_review_scope_reviewer_display(
  - src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py @@ -354 +354,5 @@ def render_review_scope_manifest_markdown(payload: dict[str, Any]) -> str: +        payload,

### `daa235c3f58b2ebfe028d5bba0fb4c7a6be19171`
- 时间: 2026-04-06 23:33:29 +0800
- 标题: chore: sync 2026-04-06 23:33:29
- 涉及文件: `src/gas_calibrator/v2/review_surface_formatter.py`, `src/gas_calibrator/v2/ui_v2/review_scope_export_index.py`, `tests/v2/test_review_surface_formatter.py`, `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -89,0 +90,12 @@ _REVIEW_SURFACE_INLINE_REPLACEMENTS = ( +_REVIEW_SCOPE_REVIEWER_DISPLAY_FIELDS = (
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -89,0 +90,12 @@ _REVIEW_SURFACE_INLINE_REPLACEMENTS = ( +    "summary_text",
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -89,0 +90,12 @@ _REVIEW_SURFACE_INLINE_REPLACEMENTS = ( +    "selection_line",
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -89,0 +90,12 @@ _REVIEW_SURFACE_INLINE_REPLACEMENTS = ( +    "counts_line",
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -89,0 +90,12 @@ _REVIEW_SURFACE_INLINE_REPLACEMENTS = ( +    "run_dir_note_text",

### `c5fb14cd155ea603867c02bc19b3cb8a8aa994ec`
- 时间: 2026-04-06 23:28:35 +0800
- 标题: chore: sync 2026-04-06 23:28:34
- 涉及文件: `configs/analyzer_chain_isolation_4ch.json`, `configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404.json`, `configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404_even0.json`, `configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404_even0_500only.json`, `configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404_even0_500only_startup_precheck.json`, `configs/default_config.json`, `src/gas_calibrator/workflow/tuning.py`, `tests/test_config_runtime_defaults.py`, `tests/test_workflow_tuning.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, H2O, cali, co2, h2o, zero
- 关键 diff hunk 摘要:
  - configs/analyzer_chain_isolation_4ch.json @@ -557 +557 @@ -        "post_h2o_zero_ppm_soak_s": 240
  - configs/analyzer_chain_isolation_4ch.json @@ -557 +557 @@ +        "post_h2o_zero_ppm_soak_s": 900
  - configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404.json @@ -560 +560 @@ -        "post_h2o_zero_ppm_soak_s": 20
  - configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404.json @@ -560 +560 @@ +        "post_h2o_zero_ppm_soak_s": 900
  - configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404_even0.json @@ -570 +570 @@ -        "post_h2o_zero_ppm_soak_s": 60

### `92c92dea94b82ceccc4c8b2a9332732e7de98d7d`
- 时间: 2026-04-06 20:43:31 +0800
- 标题: chore: sync 2026-04-06 20:43:31
- 涉及文件: `src/gas_calibrator/v2/ui_v2/review_scope_export_index.py`, `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/review_scope_export_index.py @@ -77,0 +78,12 @@ def build_review_scope_export_entry( +    for field in (
  - src/gas_calibrator/v2/ui_v2/review_scope_export_index.py @@ -77,0 +78,12 @@ def build_review_scope_export_entry( +        "summary_text",
  - src/gas_calibrator/v2/ui_v2/review_scope_export_index.py @@ -77,0 +78,12 @@ def build_review_scope_export_entry( +        "run_dir_note_text",
  - src/gas_calibrator/v2/ui_v2/review_scope_export_index.py @@ -77,0 +78,12 @@ def build_review_scope_export_entry( +        "scope_note_text",
  - src/gas_calibrator/v2/ui_v2/review_scope_export_index.py @@ -77,0 +78,12 @@ def build_review_scope_export_entry( +        "present_note_text",

### `ce6391eb33edd8ff5312fd1b4ed9e37177bbd694`
- 时间: 2026-04-06 20:38:30 +0800
- 标题: chore: sync 2026-04-06 20:38:30
- 涉及文件: `src/gas_calibrator/v2/review_surface_formatter.py`, `tests/v2/test_review_surface_formatter.py`, `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -285,0 +286 @@ def build_review_scope_payload_reviewer_display( +        "summary_text": humanize_review_surface_text(str(summary_payload.get("summary_text") or "").strip()),

### `bc664fb0889b4fe68caee3c93543b31608804fc1`
- 时间: 2026-04-06 20:33:31 +0800
- 标题: chore: sync 2026-04-06 20:33:30
- 涉及文件: `src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py`, `tests/v2/test_ui_v2_reports_page.py`, `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Report, cali, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py @@ -9,0 +10 @@ from ..review_surface_formatter import ( +    build_review_scope_reviewer_display,
  - src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py @@ -324,0 +326,7 @@ def build_review_scope_manifest_payload( +    reviewer_display = {
  - src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py @@ -324,0 +326,7 @@ def build_review_scope_manifest_payload( +        **dict(registry.get("reviewer_display", {}) or {}),
  - src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py @@ -324,0 +326,7 @@ def build_review_scope_manifest_payload( +        **build_review_scope_reviewer_display(
  - src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py @@ -324,0 +326,7 @@ def build_review_scope_manifest_payload( +            selection=selection_snapshot,

### `3b42d9df96a1abafb1a2df599acec3a4c370001a`
- 时间: 2026-04-06 20:28:32 +0800
- 标题: chore: sync 2026-04-06 20:28:31
- 涉及文件: `src/gas_calibrator/v2/review_surface_formatter.py`, `src/gas_calibrator/v2/ui_v2/pages/reports_page.py`, `src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py`, `tests/v2/test_review_surface_formatter.py`, `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Report, cali, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -302,0 +303,33 @@ def build_review_scope_payload_reviewer_display( +def build_artifact_scope_view_reviewer_display(
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -302,0 +303,33 @@ def build_review_scope_payload_reviewer_display( +    *,
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -302,0 +303,33 @@ def build_review_scope_payload_reviewer_display( +    summary_text: Any,
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -302,0 +303,33 @@ def build_review_scope_payload_reviewer_display( +    scope_label: Any,
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -302,0 +303,33 @@ def build_review_scope_payload_reviewer_display( +    visible_count: Any,

### `f3107866f14285ede7c1a8c3b736696b476dbe54`
- 时间: 2026-04-06 19:58:38 +0800
- 标题: chore: sync 2026-04-06 19:58:35
- 涉及文件: `configs/analyzer_chain_isolation_4ch.json`, `configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404.json`, `configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404_even0.json`, `configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404_even0_500only.json`, `configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404_even0_500only_startup_precheck.json`, `configs/default_config.json`, `src/gas_calibrator/config.py`, `src/gas_calibrator/v2/review_surface_formatter.py`, `src/gas_calibrator/v2/ui_v2/pages/reports_page.py`, `src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py`, `src/gas_calibrator/v2/ui_v2/review_scope_export_index.py`, `src/gas_calibrator/workflow/runner.py`, `tests/test_config_runtime_defaults.py`, `tests/test_runner_co2_presample_long_guard.py`, `tests/test_runner_h2o_sequence.py`, `tests/v2/test_review_surface_formatter.py`, `tests/v2/test_ui_v2_reports_page.py`, `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, Calibration, Report, cali, co2, db, h2o, point
- 关键 diff hunk 摘要:
  - configs/analyzer_chain_isolation_4ch.json @@ -434,5 +434,5 @@ -      "co2_presample_long_guard_window_s": 8.0,
  - configs/analyzer_chain_isolation_4ch.json @@ -434,5 +434,5 @@ -      "co2_presample_long_guard_timeout_s": 20.0,
  - configs/analyzer_chain_isolation_4ch.json @@ -434,5 +434,5 @@ -      "co2_presample_long_guard_max_span_c": 0.15,
  - configs/analyzer_chain_isolation_4ch.json @@ -434,5 +434,5 @@ -      "co2_presample_long_guard_max_abs_slope_c_per_s": 0.02,
  - configs/analyzer_chain_isolation_4ch.json @@ -434,5 +434,5 @@ -      "co2_presample_long_guard_max_rise_c": 0.12,

### `486779e4a160ee99f6d2791532dc78bdf14469d7`
- 时间: 2026-04-06 15:33:30 +0800
- 标题: chore: sync 2026-04-06 15:33:30
- 涉及文件: `src/gas_calibrator/v2/review_surface_formatter.py`, `src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py`, `tests/v2/test_ui_v2_app_facade.py`, `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -83,0 +84,3 @@ _REVIEW_SURFACE_INLINE_REPLACEMENTS = ( +    ("scope=", "\u8303\u56f4="),
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -83,0 +84,3 @@ _REVIEW_SURFACE_INLINE_REPLACEMENTS = ( +    ("source=", "\u6765\u6e90="),
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -83,0 +84,3 @@ _REVIEW_SURFACE_INLINE_REPLACEMENTS = ( +    ("evidence=", "\u8bc1\u636e="),
  - src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py @@ -817,6 +817,8 @@ def _selection_summary_line(selection: dict[str, Any]) -> str: -    return t(
  - src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py @@ -817,6 +817,8 @@ def _selection_summary_line(selection: dict[str, Any]) -> str: -        "pages.reports.review_scope_manifest.selection_line",

### `f4676ec92ca76260ddd53b48613a9ca4fed53891`
- 时间: 2026-04-06 15:28:31 +0800
- 标题: chore: sync 2026-04-06 15:28:30
- 涉及文件: `src/gas_calibrator/v2/review_surface_formatter.py`, `tests/v2/test_ui_v2_reports_page.py`, `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -74,0 +75 @@ _REVIEW_SURFACE_INLINE_REPLACEMENTS = ( +    ("current-run \u57fa\u7ebf", "\u5f53\u524d\u8fd0\u884c\u57fa\u7ebf"),
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -77,0 +79,5 @@ _REVIEW_SURFACE_INLINE_REPLACEMENTS = ( +    ("\u5f53\u524d\u8fd0\u884c \u57fa\u7ebf", "\u5f53\u524d\u8fd0\u884c\u57fa\u7ebf"),
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -77,0 +79,5 @@ _REVIEW_SURFACE_INLINE_REPLACEMENTS = ( +    ("\u5f53\u524d scope \u603b\u91cf", "\u5f53\u524d\u53ef\u89c1"),
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -77,0 +79,5 @@ _REVIEW_SURFACE_INLINE_REPLACEMENTS = ( +    ("\u5f53\u524d scope ", "\u5f53\u524d\u8303\u56f4 "),
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -77,0 +79,5 @@ _REVIEW_SURFACE_INLINE_REPLACEMENTS = ( +    ("scope \u53ef\u89c1", "\u53ef\u89c1"),

### `e0d1995d3cdebe6d3056c6d075cad043a65248bc`
- 时间: 2026-04-06 15:23:34 +0800
- 标题: chore: sync 2026-04-06 15:23:33
- 涉及文件: `src/gas_calibrator/v2/review_surface_formatter.py`, `src/gas_calibrator/v2/ui_v2/pages/reports_page.py`, `src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py`, `src/gas_calibrator/v2/ui_v2/review_center_presenter.py`, `tests/v2/test_review_surface_formatter.py`, `tests/v2/test_ui_v2_app_facade.py`, `tests/v2/test_ui_v2_reports_page.py`, `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Report, cali, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -52,0 +53,28 @@ _REVIEW_CENTER_COVERAGE_LABELS = { +_REVIEW_SURFACE_FRAGMENT_LABELS = {
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -52,0 +53,28 @@ _REVIEW_CENTER_COVERAGE_LABELS = { +    "visible": "\u53ef\u89c1",
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -52,0 +53,28 @@ _REVIEW_CENTER_COVERAGE_LABELS = { +    "present": "\u5b58\u5728",
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -52,0 +53,28 @@ _REVIEW_CENTER_COVERAGE_LABELS = { +    "external": "\u5916\u90e8",
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -52,0 +53,28 @@ _REVIEW_CENTER_COVERAGE_LABELS = { +    "catalog": "\u5f53\u524d\u8fd0\u884c\u57fa\u7ebf",

### `036a777eee2af9cff42d4dfe9206573a8927cda8`
- 时间: 2026-04-06 14:48:29 +0800
- 标题: chore: sync 2026-04-06 14:48:29
- 涉及文件: `tests/test_config_runtime_defaults.py`
- 判定原因: diff 内容命中关键词: co2, h2o, zero
- 关键 diff hunk 摘要:
  - tests/test_config_runtime_defaults.py @@ -216 +216,4 @@ def test_analyzer_chain_isolation_4ch_enables_focused_quality_guards() -> None: -    assert cfg["workflow"]["stability"]["co2_route"]["post_h2o_zero_ppm_soak_s"] == 180
  - tests/test_config_runtime_defaults.py @@ -216 +216,4 @@ def test_analyzer_chain_isolation_4ch_enables_focused_quality_guards() -> None: +        cfg["workflow"]["stability"]["co2_route"]["post_h2o_zero_ppm_soak_s"]
  - tests/test_config_runtime_defaults.py @@ -216 +216,4 @@ def test_analyzer_chain_isolation_4ch_enables_focused_quality_guards() -> None: +        == cfg["workflow"]["stability"]["co2_route"]["preseal_soak_s"]
  - tests/test_config_runtime_defaults.py @@ -229,0 +233,4 @@ def test_default_config_shortens_h2o_preseal_soak_to_30s() -> None: +        cfg["workflow"]["stability"]["co2_route"]["post_h2o_zero_ppm_soak_s"]
  - tests/test_config_runtime_defaults.py @@ -229,0 +233,4 @@ def test_default_config_shortens_h2o_preseal_soak_to_30s() -> None: +        == cfg["workflow"]["stability"]["co2_route"]["preseal_soak_s"]

### `d74d60e443a3c9f6ff2ef198fbb3a1b9a1b8db12`
- 时间: 2026-04-06 14:43:30 +0800
- 标题: chore: sync 2026-04-06 14:43:29
- 涉及文件: `configs/analyzer_chain_isolation_4ch.json`, `configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404.json`, `configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404_even0.json`, `configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404_even0_500only.json`, `configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404_even0_500only_startup_precheck.json`, `configs/default_config.json`, `src/gas_calibrator/workflow/runner.py`, `tests/test_config_runtime_defaults.py`, `tests/test_runner_h2o_sequence.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, cali, co2, db, h2o, point, zero
- 关键 diff hunk 摘要:
  - configs/analyzer_chain_isolation_4ch.json @@ -557 +557 @@ -        "post_h2o_zero_ppm_soak_s": 900
  - configs/analyzer_chain_isolation_4ch.json @@ -557 +557 @@ +        "post_h2o_zero_ppm_soak_s": 240
  - configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404.json @@ -560 +560 @@ -        "post_h2o_zero_ppm_soak_s": 900
  - configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404.json @@ -560 +560 @@ +        "post_h2o_zero_ppm_soak_s": 20
  - configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404_even0.json @@ -570 +570 @@ -        "post_h2o_zero_ppm_soak_s": 900

### `26dc05601e288eafd4abee079442aea2bf886c6b`
- 时间: 2026-04-06 14:38:29 +0800
- 标题: chore: sync 2026-04-06 14:38:29
- 涉及文件: `src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py`, `tests/v2/test_ui_v2_review_center_index.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py @@ -6,0 +7 @@ from typing import Any +from ..review_surface_formatter import humanize_review_center_coverage_text
  - src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py @@ -35,0 +37,2 @@ def decorate_source_rows( +        coverage_display = humanize_review_center_coverage_text(str(payload.get("coverage_display") or t("common.none")))
  - src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py @@ -35,0 +37,2 @@ def decorate_source_rows( +        gaps_display = humanize_review_center_coverage_text(str(payload.get("gaps_display") or t("common.none")))
  - src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py @@ -38,0 +42,2 @@ def decorate_source_rows( +        payload["coverage_display"] = coverage_display
  - src/gas_calibrator/v2/ui_v2/review_center_artifact_scope.py @@ -38,0 +42,2 @@ def decorate_source_rows( +        payload["gaps_display"] = gaps_display

### `bf3db709b206edd25e331a962843b1eeebbcfe27`
- 时间: 2026-04-06 14:28:32 +0800
- 标题: chore: sync 2026-04-06 14:28:31
- 涉及文件: `src/gas_calibrator/v2/review_surface_formatter.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/locales/en_US.json`, `src/gas_calibrator/v2/ui_v2/locales/zh_CN.json`, `tests/v2/test_review_surface_formatter.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, db
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -13,0 +14,32 @@ _OFFLINE_DIAGNOSTIC_DISPLAY_LABELS = { +_OFFLINE_DIAGNOSTIC_DETAIL_LABELS = {
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -13,0 +14,32 @@ _OFFLINE_DIAGNOSTIC_DISPLAY_LABELS = { +    "classification": ("results.review_center.detail.offline_diagnostic_classification", "\u5206\u7c7b"),
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -13,0 +14,32 @@ _OFFLINE_DIAGNOSTIC_DISPLAY_LABELS = { +    "recommended_variant": ("results.review_center.detail.offline_diagnostic_recommended_variant", "\u5efa\u8bae\u53d8\u4f53"),
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -13,0 +14,32 @@ _OFFLINE_DIAGNOSTIC_DISPLAY_LABELS = { +    "dominant_error": ("results.review_center.detail.offline_diagnostic_dominant_error", "\u4e3b\u5bfc\u8bef\u5dee"),
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -13,0 +14,32 @@ _OFFLINE_DIAGNOSTIC_DISPLAY_LABELS = { +    "next_check": ("results.review_center.detail.offline_diagnostic_next_check", "\u4e0b\u4e00\u6b65\u68c0\u67e5"),

### `b02d9ea76c84d8eea0327053d2bcef6c6e8299d0`
- 时间: 2026-04-06 14:18:32 +0800
- 标题: chore: sync 2026-04-06 14:18:31
- 涉及文件: `src/gas_calibrator/v2/review_surface_formatter.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/review_center_presenter.py`, `tests/v2/test_review_surface_formatter.py`, `tests/v2/test_ui_v2_review_center.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -13,0 +14,7 @@ _OFFLINE_DIAGNOSTIC_DISPLAY_LABELS = { +_REVIEW_CENTER_COVERAGE_LABELS = {
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -13,0 +14,7 @@ _OFFLINE_DIAGNOSTIC_DISPLAY_LABELS = { +    "coverage": "\u8986\u76d6",
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -13,0 +14,7 @@ _OFFLINE_DIAGNOSTIC_DISPLAY_LABELS = { +    "complete": "\u5b8c\u6574",
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -13,0 +14,7 @@ _OFFLINE_DIAGNOSTIC_DISPLAY_LABELS = { +    "gapped": "\u7f3a\u53e3",
  - src/gas_calibrator/v2/review_surface_formatter.py @@ -13,0 +14,7 @@ _OFFLINE_DIAGNOSTIC_DISPLAY_LABELS = { +    "missing": "\u7f3a\u5c11",

### `f28a6b542987d208064769f8da2875980f998c48`
- 时间: 2026-04-06 14:03:29 +0800
- 标题: chore: sync 2026-04-06 14:03:29
- 涉及文件: `configs/default_config.json`, `src/gas_calibrator/config.py`, `tests/test_config_runtime_defaults.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, co2
- 关键 diff hunk 摘要:
  - configs/default_config.json @@ -442,0 +443,8 @@ +      "co2_postsample_late_rebound_guard_enabled": true,
  - configs/default_config.json @@ -442,0 +443,8 @@ +      "co2_postsample_late_rebound_max_rise_c": 0.12,
  - configs/default_config.json @@ -442,0 +443,8 @@ +      "co2_postsample_late_rebound_policy": "warn",
  - configs/default_config.json @@ -442,0 +443,8 @@ +      "co2_sampling_window_qc_enabled": true,
  - configs/default_config.json @@ -442,0 +443,8 @@ +      "co2_sampling_window_qc_max_range_c": 0.2,

### `b962944adeedaa25d154854ea00ef3ce2b68873b`
- 时间: 2026-04-06 13:53:30 +0800
- 标题: chore: sync 2026-04-06 13:53:30
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/review_surface_formatter.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/pages/reports_page.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_review_surface_formatter.py`, `tests/v2/test_ui_v2_app_facade.py`, `tests/v2/test_ui_v2_reports_page.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Report, cali, mode, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -14,0 +15 @@ from ..review_surface_formatter import ( +    humanize_offline_diagnostic_summary_value,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -359,0 +361 @@ class ResultsGateway: +            coverage_summary = humanize_offline_diagnostic_summary_value(str(offline_summary.get("coverage_summary") or ""))
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -363,2 +365,2 @@ class ResultsGateway: -                    value=str(offline_summary.get("coverage_summary") or ""),
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -363,2 +365,2 @@ class ResultsGateway: -                    default=f"离线诊断覆盖：{str(offline_summary.get('coverage_summary') or '')}",
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -363,2 +365,2 @@ class ResultsGateway: +                    value=coverage_summary,

### `a3ed08182584cb4cf63ef11074f73c6175e1bcbe`
- 时间: 2026-04-06 13:48:30 +0800
- 标题: chore: sync 2026-04-06 13:48:29
- 涉及文件: `configs/default_config.json`, `src/gas_calibrator/config.py`, `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/review_surface_formatter.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/pages/reports_page.py`, `tests/test_config_runtime_defaults.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Mode, Report, cali, co2, mode, point, report
- 关键 diff hunk 摘要:
  - configs/default_config.json @@ -432,2 +432,2 @@ -      "co2_postseal_dewpoint_window_s": 2.0,
  - configs/default_config.json @@ -432,2 +432,2 @@ -      "co2_postseal_dewpoint_timeout_s": 5.5,
  - configs/default_config.json @@ -432,2 +432,2 @@ +      "co2_postseal_dewpoint_window_s": 4.0,
  - configs/default_config.json @@ -432,2 +432,2 @@ +      "co2_postseal_dewpoint_timeout_s": 6.0,
  - configs/default_config.json @@ -436 +436 @@ -      "co2_postseal_dewpoint_min_samples": 4,

### `a99b5fc5deeea29389b8bb98663ce6c089f88d51`
- 时间: 2026-04-06 13:36:11 +0800
- 标题: chore: sync 2026-04-06 13:36:10
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_app_facade.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -477 +477 @@ class ResultsGateway: -            text = str(item).strip()
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -477 +477 @@ class ResultsGateway: +            text = ResultsGateway._normalize_offline_diagnostic_line(str(item).strip())
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -497,2 +497,22 @@ class ResultsGateway: -            return f"{line} | scope {scope}" if line else f"scope {scope}"
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -497,2 +497,22 @@ class ResultsGateway: -        return line
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -497,2 +497,22 @@ class ResultsGateway: +            scope_line = ResultsGateway._offline_diagnostic_scope_line(scope)

### `206998977c9b986fbd65160a0a5628ebfc5eb287`
- 时间: 2026-04-06 13:10:02 +0800
- 标题: chore: sync 2026-04-06 13:10:02
- 涉及文件: `tests/test_runner_collect_only.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, co2, h2o, point
- 关键 diff hunk 摘要:
  - tests/test_runner_collect_only.py @@ -267,0 +268,132 @@ def test_sample_and_log_aligns_first_sample_trace_and_keeps_first_row_fresh(tmp_ +def test_sample_and_log_relabels_prime_timeout_when_effective_sample_arrives_after_start(tmp_path: Path) -> None:
  - tests/test_runner_collect_only.py @@ -267,0 +268,132 @@ def test_sample_and_log_aligns_first_sample_trace_and_keeps_first_row_fresh(tmp_ +    cfg = {
  - tests/test_runner_collect_only.py @@ -267,0 +268,132 @@ def test_sample_and_log_aligns_first_sample_trace_and_keeps_first_row_fresh(tmp_ +        "workflow": {
  - tests/test_runner_collect_only.py @@ -267,0 +268,132 @@ def test_sample_and_log_aligns_first_sample_trace_and_keeps_first_row_fresh(tmp_ +            "sampling": {
  - tests/test_runner_collect_only.py @@ -267,0 +268,132 @@ def test_sample_and_log_aligns_first_sample_trace_and_keeps_first_row_fresh(tmp_ +                "stable_count": 3,

### `0fd1b461f02a8b3dae0b102d1047c89613c462a9`
- 时间: 2026-04-06 13:03:32 +0800
- 标题: chore: sync 2026-04-06 13:03:32
- 涉及文件: `src/gas_calibrator/workflow/runner.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, Point, cali, point, span
- 关键 diff hunk 摘要:
  - src/gas_calibrator/workflow/runner.py @@ -14376,0 +14377,92 @@ class CalibrationRunner: +    def _finalize_sampling_prime_metrics_after_collection(
  - src/gas_calibrator/workflow/runner.py @@ -14376,0 +14377,92 @@ class CalibrationRunner: +        self,
  - src/gas_calibrator/workflow/runner.py @@ -14376,0 +14377,92 @@ class CalibrationRunner: +        point: CalibrationPoint,
  - src/gas_calibrator/workflow/runner.py @@ -14376,0 +14377,92 @@ class CalibrationRunner: +        *,
  - src/gas_calibrator/workflow/runner.py @@ -14376,0 +14377,92 @@ class CalibrationRunner: +        phase: str,

### `5c591d1ae10cfb3c9e4147fdbca02f83296c7624`
- 时间: 2026-04-06 12:41:24 +0800
- 标题: chore: sync 2026-04-06 12:41:24
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/offline_artifacts.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_app_facade.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, db, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -475,5 +475,12 @@ class ResultsGateway: -        lines = [
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -475,5 +475,12 @@ class ResultsGateway: -            str(item).strip()
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -475,5 +475,12 @@ class ResultsGateway: -            for item in list(summary.get("review_highlight_lines") or summary.get("detail_lines") or [])
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -475,5 +475,12 @@ class ResultsGateway: -            if str(item).strip()
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -475,5 +475,12 @@ class ResultsGateway: -        ]

### `8a15f2e4ddf9d18792208002a8b3f4684847007f`
- 时间: 2026-04-06 12:26:22 +0800
- 标题: chore: sync 2026-04-06 12:26:21
- 涉及文件: `src/gas_calibrator/workflow/runner.py`, `tests/test_pressure_point_selection.py`, `tests/test_runner_group_workflow.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, Point, cali, co2, h2o, point
- 关键 diff hunk 摘要:
  - src/gas_calibrator/workflow/runner.py @@ -4689 +4689 @@ class CalibrationRunner: -            target_pressure_hpa=template.target_pressure_hpa,
  - src/gas_calibrator/workflow/runner.py @@ -4689 +4689 @@ class CalibrationRunner: +            target_pressure_hpa=None,
  - src/gas_calibrator/workflow/runner.py @@ -4737 +4737,4 @@ class CalibrationRunner: -            target_pressure_hpa=template.target_pressure_hpa,
  - src/gas_calibrator/workflow/runner.py @@ -4737 +4737,4 @@ class CalibrationRunner: +            # Synthetic source points represent route conditioning only.
  - src/gas_calibrator/workflow/runner.py @@ -4737 +4737,4 @@ class CalibrationRunner: +            # They must not inherit a sealed-pressure template and leak that

### `35a08224bc208d82f7d796077bf548571d5f1548`
- 时间: 2026-04-05 23:08:33 +0800
- 标题: chore: sync 2026-04-05 23:08:32
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/result_store.py`, `tests/v2/test_result_store.py`, `tests/v2/test_results_gateway.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Store, cali, report, step, store
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -143 +143,9 @@ class ResultsGateway: -            "config_governance_handoff": self._read_config_governance_handoff(config_safety, config_safety_review),
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -143 +143,9 @@ class ResultsGateway: +            "config_governance_handoff": self._read_config_governance_handoff(
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -143 +143,9 @@ class ResultsGateway: +                config_safety,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -143 +143,9 @@ class ResultsGateway: +                config_safety_review,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -143 +143,9 @@ class ResultsGateway: +                summary,

### `88908727fb925b88b6b630fcac5a99b98b7e4a8c`
- 时间: 2026-04-05 22:58:38 +0800
- 标题: chore: sync 2026-04-05 22:58:37
- 涉及文件: `configs/analyzer_chain_isolation_4ch.json`, `configs/default_config.json`, `src/gas_calibrator/config.py`, `tests/test_config_runtime_defaults.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, h2o, point, span
- 关键 diff hunk 摘要:
  - configs/analyzer_chain_isolation_4ch.json @@ -533 +533 @@ -        "analyzer_chamber_temp_span_c": 0.04,
  - configs/analyzer_chain_isolation_4ch.json @@ -533 +533 @@ +        "analyzer_chamber_temp_span_c": 0.08,
  - configs/default_config.json @@ -532 +532 @@ -        "analyzer_chamber_temp_span_c": 0.04,
  - configs/default_config.json @@ -532 +532 @@ +        "analyzer_chamber_temp_span_c": 0.08,
  - configs/default_config.json @@ -589 +589 @@ -      "gas_route_dewpoint_gate_max_total_wait_s": 300.0,

### `04df2a438e3f8dcc497ad87f6b5247385716f231`
- 时间: 2026-04-05 18:33:35 +0800
- 标题: chore: sync 2026-04-05 18:33:34
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_workbench_evidence.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, point, protocol, report, store
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -73,2 +73,7 @@ class ResultsGateway: -        artifact_role_summary = (
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -73,2 +73,7 @@ class ResultsGateway: -            dict(summary.get("stats", {}).get("artifact_role_summary", {}) or {}) if isinstance(summary, dict) else {}
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -73,2 +73,7 @@ class ResultsGateway: +        artifact_role_summary = self._read_summary_section(
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -73,2 +73,7 @@ class ResultsGateway: +            "artifact_role_summary",
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -73,2 +73,7 @@ class ResultsGateway: +            summary,

### `c05f9407033ebdc8851dbee7effe683263a2636d`
- 时间: 2026-04-05 17:55:05 +0800
- 标题: chore: sync 2026-04-05 17:55:04
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`, `tests/v2/test_ui_v2_workbench_evidence.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, db, point, protocol
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -481,3 +480,0 @@ class DeviceWorkbenchController: -        point_taxonomy_summary = dict(summary_payload.get("point_taxonomy_summary") or {})
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -481,3 +480,0 @@ class DeviceWorkbenchController: -        if point_taxonomy_summary:
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -481,3 +480,0 @@ class DeviceWorkbenchController: -            return point_taxonomy_summary
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -485,0 +483,3 @@ class DeviceWorkbenchController: +        if point_taxonomy_summary:
  - src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py @@ -485,0 +483,3 @@ class DeviceWorkbenchController: +            return point_taxonomy_summary

### `19b531fa0b6f3bb2a65bcfa4cd5baaf746c8951d`
- 时间: 2026-04-05 17:49:58 +0800
- 标题: chore: sync 2026-04-05 17:49:56
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/result_store.py`, `tests/v2/test_result_store.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Store, cali, mode, point, protocol, report, save, store
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -250,3 +249,0 @@ class ResultsGateway: -        direct = payload.get(key)
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -250,3 +249,0 @@ class ResultsGateway: -        if isinstance(direct, dict):
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -250,3 +249,0 @@ class ResultsGateway: -            return dict(direct)
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -254,4 +251,6 @@ class ResultsGateway: -        if not isinstance(stats, dict):
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -254,4 +251,6 @@ class ResultsGateway: -            return {}

### `358a6c3f83e66dd29be1b099d9860a0c99dcb6fb`
- 时间: 2026-04-05 17:08:33 +0800
- 标题: chore: sync 2026-04-05 17:08:32
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/locales/en_US.json`, `src/gas_calibrator/v2/ui_v2/locales/zh_CN.json`, `tests/v2/test_ui_v2_app_facade.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -2859,0 +2860,11 @@ class AppFacade: +    @staticmethod
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -2859,0 +2860,11 @@ class AppFacade: +    def _offline_diagnostic_scope_line(*, artifact_count: int, plot_count: int) -> str:
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -2859,0 +2860,11 @@ class AppFacade: +        parts = [f"artifacts {max(0, int(artifact_count or 0))}"]
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -2859,0 +2860,11 @@ class AppFacade: +        if int(plot_count or 0) > 0:
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -2859,0 +2860,11 @@ class AppFacade: +            parts.append(f"plots {int(plot_count or 0)}")

### `d63c12ca4f4e8926d6c3124c9bd2a196561008c0`
- 时间: 2026-04-05 17:03:38 +0800
- 标题: chore: sync 2026-04-05 17:03:38
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/ui_v2/pages/reports_page.py`, `tests/v2/test_build_offline_governance_artifacts.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_app_facade.py`, `tests/v2/test_ui_v2_reports_page.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Report, cali, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -343 +343 @@ class ResultsGateway: -                    default=f"绂荤嚎璇婃柇宸ヤ欢鑼冨洿锛歿str(offline_summary.get('review_scope_summary') or '')}",
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -343 +343 @@ class ResultsGateway: +                    default="离线诊断工件范围：" + str(offline_summary.get("review_scope_summary") or ""),
  - src/gas_calibrator/v2/ui_v2/pages/reports_page.py @@ -223 +223 @@ class ReportsPage(ttk.Frame): -                    default=f"绂荤嚎璇婃柇宸ヤ欢鑼冨洿锛歿offline_scope_summary}",
  - src/gas_calibrator/v2/ui_v2/pages/reports_page.py @@ -223 +223 @@ class ReportsPage(ttk.Frame): +                    default="离线诊断工件范围：" + offline_scope_summary,
  - tests/v2/test_ui_v2_reports_page.py @@ -218 +218,2 @@ def test_reports_page_builds_result_summary_from_top_level_handoff() -> None: -                    "coverage_summary": "room-temp 2 | analyzer-chain 1 | artifacts 9 | plots 2",

### `7e9964d1c6d90b3cc12bd2579b2155da714f9f08`
- 时间: 2026-04-05 16:58:37 +0800
- 标题: chore: sync 2026-04-05 16:58:37
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/offline_artifacts.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/locales/en_US.json`, `src/gas_calibrator/v2/ui_v2/locales/zh_CN.json`, `src/gas_calibrator/v2/ui_v2/pages/reports_page.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Report, cali, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -337,0 +338,8 @@ class ResultsGateway: +        if str(offline_summary.get("review_scope_summary") or "").strip():
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -337,0 +338,8 @@ class ResultsGateway: +            lines.append(
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -337,0 +338,8 @@ class ResultsGateway: +                t(
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -337,0 +338,8 @@ class ResultsGateway: +                    "facade.results.result_summary.offline_diagnostic_scope",
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -337,0 +338,8 @@ class ResultsGateway: +                    value=str(offline_summary.get("review_scope_summary") or ""),

### `328d049852e884fd7cdff7cd6c52ef7bade0668c`
- 时间: 2026-04-05 16:53:37 +0800
- 标题: chore: sync 2026-04-05 16:53:37
- 涉及文件: `tests/v2/test_build_offline_governance_artifacts.py`, `tests/v2/test_result_store.py`, `tests/v2/test_ui_v2_device_workbench.py`, `tests/v2/test_ui_v2_reports_page.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: db, mode, point, report, store
- 关键 diff hunk 摘要:
  - tests/v2/test_result_store.py @@ -165,0 +166,2 @@ def test_result_store_persists_point_taxonomy_summary(tmp_path: Path) -> None: +    assert taxonomy["pressure_mode_summary"] == "sealed_controlled 1"
  - tests/v2/test_result_store.py @@ -165,0 +166,2 @@ def test_result_store_persists_point_taxonomy_summary(tmp_path: Path) -> None: +    assert taxonomy["pressure_target_label_summary"] == "1000hPa 1"
  - tests/v2/test_result_store.py @@ -280,0 +283 @@ def test_result_store_exports_offline_acceptance_and_analytics_artifacts(tmp_pat +    assert payload["summary_stats"]["point_taxonomy_summary"]["pressure_mode_summary"] == "sealed_controlled 1"
  - tests/v2/test_result_store.py @@ -282,0 +286 @@ def test_result_store_exports_offline_acceptance_and_analytics_artifacts(tmp_pat +    assert analytics_summary["point_taxonomy_summary"]["pressure_target_label_summary"] == "1000hPa 1"
  - tests/v2/test_ui_v2_device_workbench.py @@ -93,0 +94,3 @@ def test_workbench_snapshot_is_exposed_from_devices_payload(tmp_path: Path) -> N +    assert workbench["workbench"]["live_snapshot_evidence"]["point_taxonomy_summary"]["pressure_mode_summary"] == (

### `16e19352b50cd2789830d7c76646dd5805485984`
- 时间: 2026-04-05 16:48:32 +0800
- 标题: chore: sync 2026-04-05 16:48:32
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/offline_artifacts.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`, `src/gas_calibrator/v2/ui_v2/locales/en_US.json`, `src/gas_calibrator/v2/ui_v2/locales/zh_CN.json`, `src/gas_calibrator/v2/ui_v2/pages/reports_page.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_app_facade.py`, `tests/v2/test_ui_v2_device_workbench.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Report, cali, db, mode, point, report, step, store
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -329,0 +330,16 @@ class ResultsGateway: +        if str(offline_summary.get("coverage_summary") or "").strip():
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -329,0 +330,16 @@ class ResultsGateway: +            lines.append(
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -329,0 +330,16 @@ class ResultsGateway: +                t(
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -329,0 +330,16 @@ class ResultsGateway: +                    "facade.results.result_summary.offline_diagnostic_coverage",
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -329,0 +330,16 @@ class ResultsGateway: +                    value=str(offline_summary.get("coverage_summary") or ""),

### `8d79dc6e3a5a5a76ef7796482bb0ea649e466f5b`
- 时间: 2026-04-05 15:58:34 +0800
- 标题: chore: sync 2026-04-05 15:58:34
- 涉及文件: `src/gas_calibrator/workflow/runner.py`, `tests/test_runner_multi_analyzers.py`, `tests/test_runner_precheck.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, cali, mode
- 关键 diff hunk 摘要:
  - src/gas_calibrator/workflow/runner.py @@ -5774,0 +5775 @@ class CalibrationRunner: +            set_active_freq = getattr(ga, "set_active_freq_with_ack", None)
  - src/gas_calibrator/workflow/runner.py @@ -5799,0 +5801,6 @@ class CalibrationRunner: +                if callable(set_active_freq):
  - src/gas_calibrator/workflow/runner.py @@ -5799,0 +5801,6 @@ class CalibrationRunner: +                    set_active_freq(ftd_hz, require_ack=False)
  - src/gas_calibrator/workflow/runner.py @@ -5799,0 +5801,6 @@ class CalibrationRunner: +                else:
  - src/gas_calibrator/workflow/runner.py @@ -5799,0 +5801,6 @@ class CalibrationRunner: +                    ga.set_active_freq(ftd_hz)

### `3ddbf8d3210de9bd65ca19f85c77ccc51085f281`
- 时间: 2026-04-05 15:37:03 +0800
- 标题: chore: sync 2026-04-05 15:37:03
- 涉及文件: `configs/analyzer_chain_isolation_4ch.json`, `configs/default_config.json`, `src/gas_calibrator/config.py`, `tests/test_config_runtime_defaults.py`, `tests/test_runner_h2o_sequence.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, co2, db, h2o, point, zero
- 关键 diff hunk 摘要:
  - configs/analyzer_chain_isolation_4ch.json @@ -556 +556 @@ -        "first_point_preseal_soak_s": 420,
  - configs/analyzer_chain_isolation_4ch.json @@ -556 +556 @@ +        "first_point_preseal_soak_s": 180,
  - configs/default_config.json @@ -554 +555 @@ -        "first_point_preseal_soak_s": 420,
  - configs/default_config.json @@ -554 +555 @@ +        "first_point_preseal_soak_s": 180,
  - src/gas_calibrator/config.py @@ -64 +64 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { -            "pressure_gauge_continuous_enabled": False,

### `0b86b637fb712da85715fe3b854b5f99010fe1b9`
- 时间: 2026-04-05 15:28:33 +0800
- 标题: chore: sync 2026-04-05 15:28:32
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/offline_artifacts.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/pages/reports_page.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_app_facade.py`, `tests/v2/test_ui_v2_reports_page.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Report, cali, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -407 +407 @@ class ResultsGateway: -        limit: int = 2,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -407 +407 @@ class ResultsGateway: +        limit: int = 3,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -408,0 +409 @@ class ResultsGateway: +        summary = dict(offline_diagnostic_adapter_summary or {})
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -411 +412 @@ class ResultsGateway: -            for item in list(dict(offline_diagnostic_adapter_summary or {}).get("detail_lines") or [])
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -411 +412 @@ class ResultsGateway: +            for item in list(summary.get("review_highlight_lines") or summary.get("detail_lines") or [])

### `bfe2aa84ac01dc1381fc74af9e1887651a1e80eb`
- 时间: 2026-04-05 15:23:38 +0800
- 标题: chore: sync 2026-04-05 15:23:37
- 涉及文件: `configs/default_config.json`, `src/gas_calibrator/config.py`, `src/gas_calibrator/workflow/runner.py`, `tests/test_config_runtime_defaults.py`, `tests/test_v1_fasttrace_guards.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, cali, co2, h2o, mode, point, v1
- 关键 diff hunk 摘要:
  - configs/default_config.json @@ -402,0 +403,4 @@ +        "transition_pressure_gauge_continuous_mode": "P4",
  - src/gas_calibrator/config.py @@ -105,0 +106,4 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { +            "transition_pressure_gauge_continuous_enabled": True,
  - src/gas_calibrator/config.py @@ -105,0 +106,4 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { +            "transition_pressure_gauge_continuous_mode": "P4",
  - src/gas_calibrator/config.py @@ -105,0 +106,4 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { +            "transition_pressure_gauge_continuous_drain_s": 0.12,
  - src/gas_calibrator/config.py @@ -105,0 +106,4 @@ _RUNTIME_DEFAULTS: Dict[str, Any] = { +            "transition_pressure_gauge_continuous_read_timeout_s": 0.02,

### `44032e7ced751542da5b9aa43ab57715d884c5b5`
- 时间: 2026-04-05 15:13:33 +0800
- 标题: chore: sync 2026-04-05 15:13:33
- 涉及文件: `src/gas_calibrator/workflow/runner.py`, `tests/test_runner_h2o_sequence.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, cali, co2, h2o
- 关键 diff hunk 摘要:
  - src/gas_calibrator/workflow/runner.py @@ -12766 +12766 @@ class CalibrationRunner: -                defer_live_check=not use_preseal_topoff,
  - src/gas_calibrator/workflow/runner.py @@ -12766 +12766 @@ class CalibrationRunner: +                defer_live_check=(route_name == "co2") or (not use_preseal_topoff),
  - tests/test_runner_h2o_sequence.py @@ -1048,0 +1049,2 @@ def test_pressurize_co2_uses_cached_fast_trace_values_for_trigger_and_route_seal +    assert "preseal_ready=deferred_live_check" in route_rows[0]["note"]
  - tests/test_runner_h2o_sequence.py @@ -1048,0 +1049,2 @@ def test_pressurize_co2_uses_cached_fast_trace_values_for_trigger_and_route_seal +    assert runner._preseal_pressure_control_ready_state["ready_verification_pending"] is True

### `30f7b4dea22f22f9ef9800701de049be90591d80`
- 时间: 2026-04-05 14:58:33 +0800
- 标题: chore: sync 2026-04-05 14:58:33
- 涉及文件: `tests/test_runner_h2o_sequence.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, Calibration, co2, h2o, point
- 关键 diff hunk 摘要:
  - tests/test_runner_h2o_sequence.py @@ -1050,0 +1051,91 @@ def test_pressurize_co2_uses_cached_fast_trace_values_for_trigger_and_route_seal +def test_pressurize_co2_no_topoff_uses_cached_fast_trace_values_and_defers_live_ready_check(
  - tests/test_runner_h2o_sequence.py @@ -1050,0 +1051,91 @@ def test_pressurize_co2_uses_cached_fast_trace_values_for_trigger_and_route_seal +    tmp_path: Path,
  - tests/test_runner_h2o_sequence.py @@ -1050,0 +1051,91 @@ def test_pressurize_co2_uses_cached_fast_trace_values_for_trigger_and_route_seal +) -> None:
  - tests/test_runner_h2o_sequence.py @@ -1050,0 +1051,91 @@ def test_pressurize_co2_uses_cached_fast_trace_values_for_trigger_and_route_seal +    logger = RunLogger(tmp_path)
  - tests/test_runner_h2o_sequence.py @@ -1050,0 +1051,91 @@ def test_pressurize_co2_uses_cached_fast_trace_values_for_trigger_and_route_seal +    messages: list[str] = []

### `47cde2e5358dd74e8c0f6da2b810342c6b0c4e61`
- 时间: 2026-04-05 14:53:32 +0800
- 标题: chore: sync 2026-04-05 14:53:32
- 涉及文件: `src/gas_calibrator/workflow/runner.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, cali, point
- 关键 diff hunk 摘要:
  - src/gas_calibrator/workflow/runner.py @@ -7446,0 +7447 @@ class CalibrationRunner: +                ready_verification_pending = bool(preseal_ready_state.get("ready_verification_pending"))
  - src/gas_calibrator/workflow/runner.py @@ -7464,0 +7466 @@ class CalibrationRunner: +                        + (" live_ready_check=deferred" if ready_verification_pending else "")
  - src/gas_calibrator/workflow/runner.py @@ -7469 +7471,11 @@ class CalibrationRunner: -                if not preseal_failures:
  - src/gas_calibrator/workflow/runner.py @@ -7469 +7471,11 @@ class CalibrationRunner: +                if not preseal_failures and ready_verification_pending:
  - src/gas_calibrator/workflow/runner.py @@ -7469 +7471,11 @@ class CalibrationRunner: +                    ready_for_control = self._ensure_pressure_controller_ready_for_control(

### `20890d522fd032de1ad7bd2bd139b55c014221eb`
- 时间: 2026-04-05 14:43:35 +0800
- 标题: chore: sync 2026-04-05 14:43:35
- 涉及文件: `tests/v2/test_results_gateway.py`, `tests/v2/test_run_v2.py`, `tests/v2/test_ui_v2_app_facade.py`, `tests/v2/test_ui_v2_device_workbench.py`, `tests/v2/test_ui_v2_workbench_evidence.py`
- 判定原因: diff 内容命中关键词: CALI, Calibration, Step, calibration, db, mode, point, step
- 关键 diff hunk 摘要:
  - tests/v2/test_results_gateway.py @@ -101,0 +102,7 @@ def _inject_point_taxonomy_summary(run_dir: Path) -> None: +    stats["point_taxonomy_summary"] = {
  - tests/v2/test_results_gateway.py @@ -101,0 +102,7 @@ def _inject_point_taxonomy_summary(run_dir: Path) -> None: +        "preseal_summary": "points 1 | max overshoot 4.2 hPa | max sealed wait 1200 ms",
  - tests/v2/test_results_gateway.py @@ -101,0 +102,7 @@ def _inject_point_taxonomy_summary(run_dir: Path) -> None: +        "stale_gauge_summary": "points 1 | worst 25%",
  - tests/v2/test_run_v2.py @@ -214,0 +215,55 @@ def test_run_v2_headless_blocks_unsafe_step2_config_without_dual_unlock(tmp_path +    points_path = config_dir / "points.json"
  - tests/v2/test_run_v2.py @@ -214,0 +215,55 @@ def test_run_v2_headless_blocks_unsafe_step2_config_without_dual_unlock(tmp_path +    points_path.write_text('{"points": []}', encoding="utf-8")

### `245c7a9e2084eeb95df7185cd80fa735456a7774`
- 时间: 2026-04-05 14:38:34 +0800
- 标题: chore: sync 2026-04-05 14:38:34
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`, `src/gas_calibrator/v2/ui_v2/pages/reports_page.py`, `tests/v2/test_result_store.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_app_facade.py`, `tests/v2/test_ui_v2_device_workbench.py`, `tests/v2/test_ui_v2_reports_page.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Report, Store, cali, co2, point, report, save, store
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -58,0 +59,14 @@ class ResultsGateway: +        point_taxonomy_summary = self._read_summary_section(
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -58,0 +59,14 @@ class ResultsGateway: +            "point_taxonomy_summary",
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -58,0 +59,14 @@ class ResultsGateway: +            summary,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -58,0 +59,14 @@ class ResultsGateway: +            evidence_registry,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -58,0 +59,14 @@ class ResultsGateway: +            analytics_summary,

### `3dc6684f804b3b31db0f841e359760daf824cc7c`
- 时间: 2026-04-05 14:33:32 +0800
- 标题: chore: sync 2026-04-05 14:33:32
- 涉及文件: `src/gas_calibrator/v2/core/offline_artifacts.py`, `src/gas_calibrator/v2/core/result_store.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, Point, Store, cali, db, mode, point, store
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/offline_artifacts.py @@ -74,0 +75,11 @@ def summarize_offline_diagnostic_adapters(run_dir: Path) -> dict[str, Any]: +    latest_room_temp = dict(room_temp_bundles[0] or {}) if room_temp_bundles else {}
  - src/gas_calibrator/v2/core/offline_artifacts.py @@ -74,0 +75,11 @@ def summarize_offline_diagnostic_adapters(run_dir: Path) -> dict[str, Any]: +    latest_analyzer_chain = dict(analyzer_chain_bundles[0] or {}) if analyzer_chain_bundles else {}
  - src/gas_calibrator/v2/core/offline_artifacts.py @@ -74,0 +75,11 @@ def summarize_offline_diagnostic_adapters(run_dir: Path) -> dict[str, Any]: +    detail_items = _build_offline_diagnostic_detail_items(
  - src/gas_calibrator/v2/core/offline_artifacts.py @@ -74,0 +75,11 @@ def summarize_offline_diagnostic_adapters(run_dir: Path) -> dict[str, Any]: +        latest_room_temp=latest_room_temp,
  - src/gas_calibrator/v2/core/offline_artifacts.py @@ -74,0 +75,11 @@ def summarize_offline_diagnostic_adapters(run_dir: Path) -> dict[str, Any]: +        latest_analyzer_chain=latest_analyzer_chain,

### `557a23da6f03ec644bb19677a6f7f641cd10ee84`
- 时间: 2026-04-05 14:20:46 +0800
- 标题: chore: sync 2026-04-05 14:20:46
- 涉及文件: `configs/default_config.json`, `src/gas_calibrator/config.py`, `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/offline_artifacts.py`, `src/gas_calibrator/v2/docs/step2_v1_sync_matrix.md`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`, `src/gas_calibrator/v2/ui_v2/locales/en_US.json`, `src/gas_calibrator/v2/ui_v2/locales/zh_CN.json`, `src/gas_calibrator/v2/ui_v2/pages/reports_page.py`, `src/gas_calibrator/workflow/runner.py`, `tests/test_config_runtime_defaults.py`, `tests/test_runner_h2o_sequence.py`, `tests/test_runner_route_handoff.py`, `tests/test_v1_fasttrace_guards.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_app_facade.py`, `tests/v2/test_ui_v2_device_workbench.py`, `tests/v2/test_ui_v2_reports_page.py`, `tests/v2/test_ui_v2_workbench_evidence.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, Calibration, Point, REPORT, Report, V1, cali, co2
- 关键 diff hunk 摘要:
  - configs/default_config.json @@ -404,2 +404,2 @@ -        "co2_post_stable_sample_delay_s": 10.0,
  - configs/default_config.json @@ -404,2 +404,2 @@ +        "co2_post_stable_sample_delay_s": 5.0,
  - configs/default_config.json @@ -580 +580 @@ -      "gas_route_dewpoint_gate_enabled": false,
  - configs/default_config.json @@ -580 +580 @@ +      "gas_route_dewpoint_gate_enabled": true,
  - configs/default_config.json @@ -581,0 +582 @@ +      "gas_route_dewpoint_gate_policy": "reject",

### `26f7d7238926568028aa867010078b3e2609a39c`
- 时间: 2026-04-05 12:53:33 +0800
- 标题: chore: sync 2026-04-05 12:53:33
- 涉及文件: `src/gas_calibrator/v2/ui_v2/locales/en_US.json`, `src/gas_calibrator/v2/ui_v2/locales/zh_CN.json`, `src/gas_calibrator/v2/ui_v2/pages/reports_page.py`, `tests/v2/test_ui_v2_reports_page.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Report, cali, db, protocol, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/locales/en_US.json @@ -1180,0 +1181,7 @@ +      "summary_fallback": {
  - src/gas_calibrator/v2/ui_v2/locales/en_US.json @@ -1180,0 +1181,7 @@ +        "evidence_source": "Evidence source: {display} ({source})",
  - src/gas_calibrator/v2/ui_v2/locales/en_US.json @@ -1180,0 +1181,7 @@ +        "evidence_source_same": "Evidence source: {source}",
  - src/gas_calibrator/v2/ui_v2/locales/en_US.json @@ -1180,0 +1181,7 @@ +        "config_safety": "Config safety: {summary}",
  - src/gas_calibrator/v2/ui_v2/locales/en_US.json @@ -1180,0 +1181,7 @@ +        "offline_diagnostic": "Offline diagnostic: {summary}",

### `0b8ae2d0cdbab148842c9905ff5c41dbb571b50b`
- 时间: 2026-04-05 12:48:33 +0800
- 标题: chore: sync 2026-04-05 12:48:33
- 涉及文件: `configs/default_config.json`, `src/gas_calibrator/v2/ui_v2/pages/reports_page.py`, `tests/test_config_runtime_defaults.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Report, cali, db, h2o, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/pages/reports_page.py @@ -7 +7 @@ from typing import Any -from ..i18n import t
  - src/gas_calibrator/v2/ui_v2/pages/reports_page.py @@ -7 +7 @@ from typing import Any +from ..i18n import display_evidence_source, t
  - src/gas_calibrator/v2/ui_v2/pages/reports_page.py @@ -141,0 +142,3 @@ class ReportsPage(ttk.Frame): +        if not str(snapshot.get("result_summary_text", "") or "").strip():
  - src/gas_calibrator/v2/ui_v2/pages/reports_page.py @@ -141,0 +142,3 @@ class ReportsPage(ttk.Frame): +            snapshot = dict(snapshot)
  - src/gas_calibrator/v2/ui_v2/pages/reports_page.py @@ -141,0 +142,3 @@ class ReportsPage(ttk.Frame): +            snapshot["result_summary_text"] = self._build_result_summary_fallback(snapshot)

### `9a97543076afb0693e1c42aae024bd7111f74db4`
- 时间: 2026-04-05 12:43:35 +0800
- 标题: chore: sync 2026-04-05 12:43:35
- 涉及文件: `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `tests/v2/test_ui_v2_app_facade.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, db, protocol, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1223,0 +1224 @@ class AppFacade: +        result_evidence_source = _normalize_simulated_evidence_source(workbench_evidence_summary.get("evidence_source"))
  - src/gas_calibrator/v2/ui_v2/controllers/app_facade.py @@ -1298,0 +1300 @@ class AppFacade: +                f"证据来源: {result_evidence_source}",
  - tests/v2/test_ui_v2_app_facade.py @@ -382,0 +383,4 @@ def test_app_facade_surfaces_offline_diagnostic_adapter_review_items(tmp_path: P +    assert reports_snapshot["evidence_source"] == "simulated_protocol"
  - tests/v2/test_ui_v2_app_facade.py @@ -382,0 +383,4 @@ def test_app_facade_surfaces_offline_diagnostic_adapter_review_items(tmp_path: P +    assert reports_snapshot["not_real_acceptance_evidence"] is True
  - tests/v2/test_ui_v2_app_facade.py @@ -382,0 +383,4 @@ def test_app_facade_surfaces_offline_diagnostic_adapter_review_items(tmp_path: P +    assert "simulated_protocol" in results_snapshot["result_summary_text"]

### `1f5b4266edc34bafa39d97a68c7b742b47bce75f`
- 时间: 2026-04-05 12:38:34 +0800
- 标题: chore: sync 2026-04-05 12:38:34
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_app_facade.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, db, insert, mode, protocol, report, step
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -7,0 +8 @@ from ..config import build_step2_config_governance_handoff +from ..core.acceptance_model import normalize_evidence_source
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -62,0 +64,22 @@ class ResultsGateway: +        evidence_source = self._resolve_current_run_evidence_source(workbench_evidence_summary, workbench_action_report)
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -62,0 +64,22 @@ class ResultsGateway: +        evidence_state = str(
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -62,0 +64,22 @@ class ResultsGateway: +            workbench_evidence_summary.get("evidence_state")
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -62,0 +64,22 @@ class ResultsGateway: +            or dict(workbench_action_report or {}).get("evidence_state")

### `e546d0714e109bc66e697c440adfa783dd915252`
- 时间: 2026-04-05 12:33:34 +0800
- 标题: chore: sync 2026-04-05 12:33:34
- 涉及文件: `src/gas_calibrator/workflow/runner.py`, `tests/test_runner_collect_only.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, cali, co2, point
- 关键 diff hunk 摘要:
  - src/gas_calibrator/workflow/runner.py @@ -1659,0 +1660,10 @@ class CalibrationRunner: +    def _sampling_has_reusable_pace_state(self) -> bool:
  - src/gas_calibrator/workflow/runner.py @@ -1659,0 +1660,10 @@ class CalibrationRunner: +        snapshot = self._pace_state_cache_snapshot()
  - src/gas_calibrator/workflow/runner.py @@ -1659,0 +1660,10 @@ class CalibrationRunner: +        if snapshot.get("sample_ts"):
  - src/gas_calibrator/workflow/runner.py @@ -1659,0 +1660,10 @@ class CalibrationRunner: +            return True
  - src/gas_calibrator/workflow/runner.py @@ -1659,0 +1660,10 @@ class CalibrationRunner: +        completion = dict(self._last_sample_completion or {})

### `d1bf46008f31637186df810030d8754060928eef`
- 时间: 2026-04-05 12:28:33 +0800
- 标题: chore: sync 2026-04-05 12:28:33
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/result_store.py`, `tests/v2/test_result_store.py`, `tests/v2/test_results_gateway.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Store, cali, db, mode, point, report, store
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -56,0 +57,14 @@ class ResultsGateway: +        artifact_role_summary = (
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -56,0 +57,14 @@ class ResultsGateway: +            dict(summary.get("stats", {}).get("artifact_role_summary", {}) or {}) if isinstance(summary, dict) else {}
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -56,0 +57,14 @@ class ResultsGateway: +        )
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -56,0 +57,14 @@ class ResultsGateway: +        workbench_evidence_summary = (
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -56,0 +57,14 @@ class ResultsGateway: +            dict(summary.get("stats", {}).get("workbench_evidence_summary", {}) or {}) if isinstance(summary, dict) else {}

### `9fcccaf4a62bf683745c65a24c330acf7d10f44a`
- 时间: 2026-04-05 11:48:42 +0800
- 标题: chore: sync 2026-04-05 11:48:41
- 涉及文件: `src/gas_calibrator/v2/docs/step2_v1_sync_matrix.md`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `tests/v2/test_ui_v2_app_facade.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Step, V1, cali, co2, coefficient, db, mode, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/docs/step2_v1_sync_matrix.md @@ -12 +12 @@ Guardrails: -| capability | V1/shared source | current V2 status | action | suggested V2 target modules | required tests | reason |
  - src/gas_calibrator/v2/docs/step2_v1_sync_matrix.md @@ -12 +12 @@ Guardrails: +| capability | V1/shared source | current V2 status | Step 2 classification | suggested V2 target modules | required tests | reason |
  - src/gas_calibrator/v2/docs/step2_v1_sync_matrix.md @@ -17 +17 @@ Guardrails: -| room-temp pressure diagnostic / plots / analyzer-chain isolation | `src/gas_calibrator/validation/room_temp_co2_pressure_diagnostic.py`, `src/gas_calibrator/validation/room_temp
  - src/gas_calibrator/v2/docs/step2_v1_sync_matrix.md @@ -17 +17 @@ Guardrails: +| room-temp pressure diagnostic / plots / analyzer-chain isolation | `src/gas_calibrator/validation/room_temp_co2_pressure_diagnostic.py`, `src/gas_calibrator/validation/room_temp
  - src/gas_calibrator/v2/docs/step2_v1_sync_matrix.md @@ -19,3 +19,3 @@ Guardrails: -| coefficient quiet I/O | `src/gas_calibrator/devices/gas_analyzer.py` | already synced | do nothing | `src/gas_calibrator/v2/adapters/analyzer_coefficient_downloader.py` | `tests

### `78e68b0de3eae6f6fd0509e2484f624ca0710bfc`
- 时间: 2026-04-05 11:39:25 +0800
- 标题: chore: sync 2026-04-05 11:39:24
- 涉及文件: `src/gas_calibrator/v2/ui_v2/locales/en_US.json`, `src/gas_calibrator/v2/ui_v2/locales/zh_CN.json`, `src/gas_calibrator/v2/ui_v2/pages/reports_page.py`, `tests/v2/test_ui_v2_reports_page.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Report, cali, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/ui_v2/locales/en_US.json @@ -1178,0 +1179,2 @@ +      "result_summary": "Run & Governance Summary",
  - src/gas_calibrator/v2/ui_v2/locales/en_US.json @@ -1178,0 +1179,2 @@ +      "no_result_summary": "No run or governance summary yet.",
  - src/gas_calibrator/v2/ui_v2/locales/zh_CN.json @@ -1177,0 +1178,2 @@ +      "result_summary": "运行与治理摘要",
  - src/gas_calibrator/v2/ui_v2/locales/zh_CN.json @@ -1177,0 +1178,2 @@ +      "no_result_summary": "暂无运行与治理摘要。",
  - src/gas_calibrator/v2/ui_v2/pages/reports_page.py @@ -99,0 +100 @@ class ReportsPage(ttk.Frame): +        right.rowconfigure(5, weight=1)

### `0c5cbf71b58075b02210be32fe59e302cd021bab`
- 时间: 2026-04-04 23:43:42 +0800
- 标题: chore: sync 2026-04-04 23:43:42
- 涉及文件: `src/gas_calibrator/logging_utils.py`, `tests/test_logging_utils.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, co2, mode, point
- 关键 diff hunk 摘要:
  - src/gas_calibrator/logging_utils.py @@ -691,0 +692,39 @@ def _dedupe_keys_by_label(keys: List[str]) -> List[str]: +def _merge_sheet_header_keys(existing_labels: List[Any], header_keys: List[str]) -> List[str]:
  - src/gas_calibrator/logging_utils.py @@ -691,0 +692,39 @@ def _dedupe_keys_by_label(keys: List[str]) -> List[str]: +    """Preserve existing sheet-column order while appending newly discovered fields."""
  - src/gas_calibrator/logging_utils.py @@ -691,0 +692,39 @@ def _dedupe_keys_by_label(keys: List[str]) -> List[str]: +    keyed_labels = [
  - src/gas_calibrator/logging_utils.py @@ -691,0 +692,39 @@ def _dedupe_keys_by_label(keys: List[str]) -> List[str]: +        (idx, str(key), _field_label(str(key)))
  - src/gas_calibrator/logging_utils.py @@ -691,0 +692,39 @@ def _dedupe_keys_by_label(keys: List[str]) -> List[str]: +        for idx, key in enumerate(header_keys)

### `a23401c9ed93fdb0c31096c05b91734da9b92ac0`
- 时间: 2026-04-04 21:08:33 +0800
- 标题: chore: sync 2026-04-04 21:08:33
- 涉及文件: `configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404.json`, `configs/points_tiny_short_run_20c_even500.xlsx`, `tests/test_pressure_point_selection.py`, `tests/test_runner_h2o_sequence.py`, `tests/test_verify_short_run_tool.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, Point, co2, db, h2o, mode, point, span
- 关键 diff hunk 摘要:
  - configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404.json @@ -287 +287,4 @@ -    "selected_pressure_points": [],
  - configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404.json @@ -287 +287,4 @@ +    "selected_pressure_points": [
  - configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404.json @@ -288,0 +292 @@ +    "preserve_explicit_point_matrix": true,
  - configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404.json @@ -554,2 +558,2 @@ -        "first_point_preseal_soak_s": 420,
  - configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404.json @@ -554,2 +558,2 @@ +        "first_point_preseal_soak_s": 45,

### `b7da9c407fce41ad561ff9447f4ca8a8106c2bdd`
- 时间: 2026-04-04 21:03:32 +0800
- 标题: chore: sync 2026-04-04 21:03:32
- 涉及文件: `src/gas_calibrator/tools/verify_short_run.py`, `src/gas_calibrator/workflow/runner.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, Calibration, Point, cali, co2, point, span
- 关键 diff hunk 摘要:
  - src/gas_calibrator/tools/verify_short_run.py @@ -41,0 +42,2 @@ def build_short_verification_config( +    if points_excel_override:
  - src/gas_calibrator/tools/verify_short_run.py @@ -41,0 +42,2 @@ def build_short_verification_config( +        workflow_cfg["preserve_explicit_point_matrix"] = True
  - src/gas_calibrator/tools/verify_short_run.py @@ -113 +115 @@ def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace: -        default="100,200,300,500,600,700,800,900",
  - src/gas_calibrator/tools/verify_short_run.py @@ -113 +115 @@ def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace: +        default="",
  - src/gas_calibrator/workflow/runner.py @@ -4574,0 +4575,2 @@ class CalibrationRunner: +        if self._preserve_explicit_point_matrix():

### `cf861aff68d61852766d5c1f4cc7bf1d8aa9a93f`
- 时间: 2026-04-04 20:50:08 +0800
- 标题: chore: sync 2026-04-04 20:50:07
- 涉及文件: `configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404_even0_500only_startup_precheck.json`, `src/gas_calibrator/v2/adapters/results_gateway.py`, `tests/v2/test_build_offline_governance_artifacts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, H2O, Mode, cali, co2, coefficient, db, h2o
- 关键 diff hunk 摘要:
  - configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404_even0_500only_startup_precheck.json @@ -0,0 +1,830 @@ +    "dewpoint_meter": {
  - configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404_even0_500only_startup_precheck.json @@ -0,0 +1,830 @@ +      "mode": 2,
  - configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404_even0_500only_startup_precheck.json @@ -0,0 +1,830 @@ +      "average_co2": 1,
  - configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404_even0_500only_startup_precheck.json @@ -0,0 +1,830 @@ +      "average_h2o": 1
  - configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404_even0_500only_startup_precheck.json @@ -0,0 +1,830 @@ +        "mode": 2,

### `424b9420587ac9737ca1c2b78eebd4417d04a77d`
- 时间: 2026-04-04 20:33:33 +0800
- 标题: chore: sync 2026-04-04 20:33:32
- 涉及文件: `src/gas_calibrator/v2/docs/step2_v1_sync_matrix.md`, `tests/v2/test_build_offline_governance_artifacts.py`, `tests/v2/test_result_store.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_app_facade.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, COEFFICIENT, Step, V1, cali, co2, coefficient, mode
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/docs/step2_v1_sync_matrix.md @@ -1 +1 @@ -# Step 2 V1 -> V2 Sync Matrix
  - src/gas_calibrator/v2/docs/step2_v1_sync_matrix.md @@ -1 +1 @@ +# Step 2 V2-Only Sync Audit Matrix
  - src/gas_calibrator/v2/docs/step2_v1_sync_matrix.md @@ -3 +3 @@ -本矩阵基于最新代码、最新测试与本轮命令结果重审，不依赖旧 markdown 收口。
  - src/gas_calibrator/v2/docs/step2_v1_sync_matrix.md @@ -3 +3 @@ +This matrix is based on current `AGENTS.md`, `.ai-context`, latest code, latest tests, and actual local command results.
  - src/gas_calibrator/v2/docs/step2_v1_sync_matrix.md @@ -5,6 +5,6 @@ -边界声明：

### `019ef97676fe5ee9cbc6936f782fcc2ff22c08fe`
- 时间: 2026-04-04 20:28:34 +0800
- 标题: chore: sync 2026-04-04 20:28:33
- 涉及文件: `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/core/offline_artifacts.py`, `src/gas_calibrator/v2/core/result_store.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/locales/en_US.json`, `src/gas_calibrator/v2/ui_v2/locales/zh_CN.json`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: REPORT, Store, cali, db, mode, point, protocol, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -8,0 +9 @@ from ..core.artifact_catalog import KNOWN_REPORT_ARTIFACTS +from ..core.offline_artifacts import summarize_offline_diagnostic_adapters
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -45,0 +47,10 @@ class ResultsGateway: +        offline_diagnostic_adapter_summary = self._read_summary_section(
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -45,0 +47,10 @@ class ResultsGateway: +            "offline_diagnostic_adapter_summary",
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -45,0 +47,10 @@ class ResultsGateway: +            summary,
  - src/gas_calibrator/v2/adapters/results_gateway.py @@ -45,0 +47,10 @@ class ResultsGateway: +            evidence_registry,

### `cfb73c3f52fd7ac7bb741bbc67b5bdb8f993973c`
- 时间: 2026-04-04 20:23:35 +0800
- 标题: chore: sync 2026-04-04 20:23:35
- 涉及文件: `configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404_even0_500only.json`, `src/gas_calibrator/devices/pace5000.py`, `src/gas_calibrator/workflow/runner.py`, `tests/test_pace5000_driver.py`, `tests/test_runner_pressure_control_order.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, Calibration, H2O, Mode, Point, Serial, V1, cali
- 关键 diff hunk 摘要:
  - configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404_even0_500only.json @@ -0,0 +1,829 @@ +    "dewpoint_meter": {
  - configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404_even0_500only.json @@ -0,0 +1,829 @@ +      "mode": 2,
  - configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404_even0_500only.json @@ -0,0 +1,829 @@ +      "average_co2": 1,
  - configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404_even0_500only.json @@ -0,0 +1,829 @@ +      "average_h2o": 1
  - configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404_even0_500only.json @@ -0,0 +1,829 @@ +        "mode": 2,

### `ef32e6f84b273c1393039c34ff29897715447b4c`
- 时间: 2026-04-04 20:18:32 +0800
- 标题: chore: sync 2026-04-04 20:18:32
- 涉及文件: `src/gas_calibrator/v2/core/artifact_catalog.py`, `src/gas_calibrator/v2/core/offline_artifacts.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: cali, report
- 关键 diff hunk 摘要:
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -54,0 +55,12 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "room_temp_diagnostic_summary",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -54,0 +55,12 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "room_temp_diagnostic_report",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -54,0 +55,12 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "room_temp_diagnostic_workbook",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -54,0 +55,12 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "room_temp_diagnostic_plot",
  - src/gas_calibrator/v2/core/artifact_catalog.py @@ -54,0 +55,12 @@ DEFAULT_ROLE_CATALOG: dict[str, list[str]] = { +        "analyzer_chain_isolation_comparison",

### `ebbc9b00d02c7f49ee2218c0821c45ad65df4a29`
- 时间: 2026-04-04 19:50:25 +0800
- 标题: chore: sync 2026-04-04 19:50:25
- 涉及文件: `configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404_even0.json`, `configs/points_real_smoke_co2_tiny_20c_even0_20260404.xlsx`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: CO2, H2O, Mode, co2, coefficient, h2o, mode, point
- 关键 diff hunk 摘要:
  - configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404_even0.json @@ -0,0 +1,830 @@ +    "dewpoint_meter": {
  - configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404_even0.json @@ -0,0 +1,830 @@ +      "mode": 2,
  - configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404_even0.json @@ -0,0 +1,830 @@ +      "average_co2": 1,
  - configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404_even0.json @@ -0,0 +1,830 @@ +      "average_h2o": 1
  - configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404_even0.json @@ -0,0 +1,830 @@ +        "mode": 2,

### `8f4345826d708882640b46ad6e9e0483446786ff`
- 时间: 2026-04-04 19:25:01 +0800
- 标题: chore: sync 2026-04-04 19:25:01
- 涉及文件: `src/gas_calibrator/workflow/runner.py`, `tests/test_runner_h2o_sequence.py`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Calibration, cali, co2, h2o, point, span
- 关键 diff hunk 摘要:
  - src/gas_calibrator/workflow/runner.py @@ -10380,0 +10381,7 @@ class CalibrationRunner: +        # Give the gate at least one full tail window after the fixed preseal soak.
  - src/gas_calibrator/workflow/runner.py @@ -10380,0 +10381,7 @@ class CalibrationRunner: +        # Without this floor, a config that enables a longer first-point soak can
  - src/gas_calibrator/workflow/runner.py @@ -10380,0 +10381,7 @@ class CalibrationRunner: +        # timeout on the first gate read before span/slope are even observable.
  - src/gas_calibrator/workflow/runner.py @@ -10380,0 +10381,7 @@ class CalibrationRunner: +        effective_max_total_wait_s = max(
  - src/gas_calibrator/workflow/runner.py @@ -10380,0 +10381,7 @@ class CalibrationRunner: +            float(cfg["max_total_wait_s"]),

### `6c04f74bdab812dda7295008e5ab3e959141adc0`
- 时间: 2026-04-04 18:17:47 +0800
- 标题: Add scheduled auto sync wrapper
- 涉及文件: `README.md`, `scripts/run_auto_sync.ps1`
- 判定原因: diff 内容命中关键词: Mode, mode
- 关键 diff hunk 摘要:
  - scripts/run_auto_sync.ps1 @@ -0,0 +1,23 @@ +Set-StrictMode -Version Latest

### `4120baa751879a2b82402508408643cb591e857d`
- 时间: 2026-04-04 18:11:01 +0800
- 标题: chore: sync 2026-04-04 18:11:01
- 涉及文件: `configs/analyzer_chain_isolation_4ch_tiny_short_run_20260404.json`, `scripts/sync.ps1`
- 判定原因: diff 内容命中关键词: db
- 关键 diff hunk 摘要:
  - 未提取到关键词 hunk，建议结合 raw git log 复核。

### `666c897b1915417f7af9ac5232cc407995746cbb`
- 时间: 2026-04-04 18:08:42 +0800
- 标题: Add GitHub sync helper script
- 涉及文件: `README.md`, `scripts/sync.ps1`
- 判定原因: diff 内容命中关键词: Mode, Step, V1, dB, db, mode
- 关键 diff hunk 摘要:
  - scripts/sync.ps1 @@ -0,0 +1,167 @@ +Set-StrictMode -Version Latest
  - scripts/sync.ps1 @@ -0,0 +1,167 @@ +        $aheadBehind = Get-GitText -Arguments @("rev-list", "--left-right", "--count", "HEAD...origin/$branch")
  - scripts/sync.ps1 @@ -0,0 +1,167 @@ +        $parts = $aheadBehind -split "\s+"

### `17db2ce8dfb242a5f4dd8ad31d8342cbcb1ebd55`
- 时间: 2026-04-04 18:04:27 +0800
- 标题: Add GitHub collaboration templates
- 涉及文件: `.github/ISSUE_TEMPLATE/bug_report.yml`, `.github/ISSUE_TEMPLATE/config.yml`, `.github/ISSUE_TEMPLATE/work_item.yml`, `.github/PULL_REQUEST_TEMPLATE.md`
- 判定原因: 改动文件命中校准相关路径/关键词；diff 内容命中关键词: Report, Step, V1, cali, db, mode, report, step
- 关键 diff hunk 摘要:
  - .github/ISSUE_TEMPLATE/bug_report.yml @@ -0,0 +1,89 @@ +name: Bug Report
  - .github/ISSUE_TEMPLATE/bug_report.yml @@ -0,0 +1,89 @@ +description: Report a defect or regression within the current project phase.
  - .github/ISSUE_TEMPLATE/bug_report.yml @@ -0,0 +1,89 @@ +title: "[Bug] "
  - .github/ISSUE_TEMPLATE/bug_report.yml @@ -0,0 +1,89 @@ +labels:
  - .github/ISSUE_TEMPLATE/bug_report.yml @@ -0,0 +1,89 @@ +  - bug

### `c969b546b5d8f65ad3c23e29f69414c0373e6f01`
- 时间: 2026-04-04 17:53:31 +0800
- 标题: Initial commit
- 涉及文件: (none)
- 判定原因: diff 内容命中关键词: cali, mode
- 关键 diff hunk 摘要:
  - README.md @@ -0,0 +1 @@ +# gas_calibrator

### `d4487bb0e416a7ca4917ba3eefb8d2fd3268c735`
- 时间: 2026-04-04 17:49:02 +0800
- 标题: Initial import
- 涉及文件: (none)
- 判定原因: diff 内容命中关键词: CALI, CALIBRATION, CO2, COEFFICIENT, Cali, Calibration, Co2, Coefficient
- 关键 diff hunk 摘要:
  - .gitignore @@ -0,0 +1,31 @@ +# Runtime outputs and reports
  - .gitignore @@ -0,0 +1,31 @@ +src/gas_calibrator/output/
  - .gitignore @@ -0,0 +1,31 @@ +points.xlsx
  - AGENTS.md @@ -0,0 +1,156 @@ +# 气体分析仪自动校准 V2 项目长期总控规则
  - AGENTS.md @@ -0,0 +1,156 @@ +本项目目标是：在**绝不破坏 V1 已经可用的生产校准流程**前提下，稳步推进 V2，最终建设为**全球行业领先的气体分析仪自动校准与数据分析系统**。

## 未提交改动中与 V1 校准相关的文件

- `udit/v1_calibration_audit/01_git_changes_since_2026-04-03.md`
- `udit/v1_calibration_audit/02_v1_flow_map.md`
- `udit/v1_calibration_audit/03_point_storage_map.md`
- `udit/v1_calibration_audit/04_risk_checklist.md`
- `udit/v1_calibration_audit/05_evidence.json`
- `udit/v1_calibration_audit/06_trace_check.md`
- `udit/v1_calibration_audit/README.md`
- `udit/v1_calibration_audit/raw/git_log_since_2026-04-03.txt`
- `udit/v1_calibration_audit/raw/git_status.txt`
- `udit/v1_calibration_audit/raw/rg_hits.txt`
- `rc/gas_calibrator/config.py`
- `rc/gas_calibrator/devices/gas_analyzer.py`
- `rc/gas_calibrator/logging_utils.py`
- `rc/gas_calibrator/tools/audit_run.py`
- `rc/gas_calibrator/tools/run_headless.py`
- `rc/gas_calibrator/tools/run_v1_corrected_autodelivery.py`
- `rc/gas_calibrator/tools/run_v1_merged_calibration_sidecar.py`
- `rc/gas_calibrator/v2/adapters/results_gateway.py`
- `rc/gas_calibrator/v2/core/phase_transition_bridge.py`
- `rc/gas_calibrator/v2/ui_v2/controllers/app_facade.py`
- `rc/gas_calibrator/v2/ui_v2/locales/en_US.json`
- `rc/gas_calibrator/v2/ui_v2/locales/zh_CN.json`
- `rc/gas_calibrator/v2/ui_v2/review_center_scan_contracts.py`
- `rc/gas_calibrator/workflow/runner.py`
- `ests/test_v1_merged_calibration_sidecar.py`
- `ools/audit_v1_calibration.py`
- `audit/v1_calibration_acceptance/`
- `audit/v1_calibration_acceptance_online/`
- `src/gas_calibrator/tools/run_v1_online_acceptance.py`
- `src/gas_calibrator/v2/core/phase_evidence_display_contracts.py`
- `tests/test_runner_v1_writeback_safety.py`
- `tests/test_v1_online_acceptance_tool.py`
- `tests/test_v1_writeback_fault_injection.py`
- `tools/run_v1_online_acceptance.py`
- `udit/v1_calibration_audit/01_git_changes_since_2026-04-03.md`
- `udit/v1_calibration_audit/02_v1_flow_map.md`
- `udit/v1_calibration_audit/03_point_storage_map.md`
- `udit/v1_calibration_audit/04_risk_checklist.md`
- `udit/v1_calibration_audit/05_evidence.json`
- `udit/v1_calibration_audit/06_trace_check.md`
- `udit/v1_calibration_audit/README.md`
- `udit/v1_calibration_audit/raw/git_log_since_2026-04-03.txt`
- `udit/v1_calibration_audit/raw/git_status.txt`
- `udit/v1_calibration_audit/raw/rg_hits.txt`
- `rc/gas_calibrator/config.py`
- `rc/gas_calibrator/devices/gas_analyzer.py`
- `rc/gas_calibrator/logging_utils.py`
- `rc/gas_calibrator/tools/audit_run.py`
- `rc/gas_calibrator/tools/run_headless.py`
- `rc/gas_calibrator/tools/run_v1_corrected_autodelivery.py`
- `rc/gas_calibrator/tools/run_v1_merged_calibration_sidecar.py`
- `rc/gas_calibrator/v2/adapters/results_gateway.py`
- `rc/gas_calibrator/v2/core/phase_transition_bridge.py`
- `rc/gas_calibrator/v2/ui_v2/controllers/app_facade.py`
- `rc/gas_calibrator/v2/ui_v2/locales/en_US.json`
- `rc/gas_calibrator/v2/ui_v2/locales/zh_CN.json`
- `rc/gas_calibrator/v2/ui_v2/review_center_scan_contracts.py`
- `rc/gas_calibrator/workflow/runner.py`
- `ests/test_v1_merged_calibration_sidecar.py`
- `ools/audit_v1_calibration.py`
- `audit/v1_calibration_acceptance/01_capability_matrix.md`
- `audit/v1_calibration_acceptance/02_sample_points.csv`
- `audit/v1_calibration_acceptance/03_sample_samples.csv`
- `audit/v1_calibration_acceptance/04_sample_coefficient_writeback.csv`
- `audit/v1_calibration_acceptance/05_fault_injection_matrix.md`
- `audit/v1_calibration_acceptance/06_acceptance_summary.md`
- `audit/v1_calibration_acceptance_online/01_online_acceptance_checklist.md`
- `audit/v1_calibration_acceptance_online/02_online_run_template.json`
- `audit/v1_calibration_acceptance_online/03_online_protocol_log_schema.md`
- `audit/v1_calibration_acceptance_online/04_online_evidence_summary.md`
- `src/gas_calibrator/tools/run_v1_online_acceptance.py`
- `src/gas_calibrator/v2/core/phase_evidence_display_contracts.py`
- `tests/test_runner_v1_writeback_safety.py`
- `tests/test_v1_online_acceptance_tool.py`
- `tests/test_v1_writeback_fault_injection.py`
- `tools/run_v1_online_acceptance.py`
