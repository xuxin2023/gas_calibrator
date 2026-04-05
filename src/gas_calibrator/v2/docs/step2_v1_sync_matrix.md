# Step 2 V2-Only Sync Audit Matrix

This matrix is based on current `AGENTS.md`, `.ai-context`, latest code, latest tests, and actual local command results.

Guardrails:
- Only V2 changes are in scope.
- Do not change `run_app.py`.
- Do not reconnect anything into V1 UI.
- Do not change V1/shared default workflow files.
- All evidence below is simulation/offline/headless only, not real acceptance evidence.

| capability | V1/shared source | current V2 status | Step 2 classification | suggested V2 target modules | required tests | reason |
| --- | --- | --- | --- | --- | --- | --- |
| `dewpoint_flush_gate` | `src/gas_calibrator/validation/dewpoint_flush_gate.py` | partially synced | safe to extend | `src/gas_calibrator/v2/core/orchestrator.py`, `src/gas_calibrator/v2/core/services/qc_service.py`, `src/gas_calibrator/v2/core/offline_artifacts.py` | `tests/test_dewpoint_flush_gate.py`, `tests/v2/test_co2_route_runner.py`, `tests/v2/test_qc_service.py` | V2 already has route-level gate and offline QC hooks; keep any further sync in simulation/offline review only. |
| CO2 post-seal quality guards | `src/gas_calibrator/workflow/runner.py` | already synced | already synced | `src/gas_calibrator/v2/core/services/qc_service.py`, `src/gas_calibrator/v2/core/result_store.py`, `src/gas_calibrator/v2/core/offline_artifacts.py` | `tests/test_runner_co2_quality_guards.py`, `tests/v2/test_qc_service.py`, `tests/v2/test_measurement_analytics_service.py` | Rebound veto, pressure-scaled dewpoint QC, and stale gauge metrics are already present in V2 QC/artifacts/review surfaces. |
| pressure point selection ambient semantics | `src/gas_calibrator/workflow/runner.py`, `tests/test_pressure_point_selection.py` | already synced | already synced | `src/gas_calibrator/v2/domain/pressure_selection.py`, `src/gas_calibrator/v2/config/models.py`, `src/gas_calibrator/v2/core/plan_compiler.py`, `src/gas_calibrator/v2/ui_v2/controllers/plan_gateway.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py` | `tests/test_pressure_point_selection.py`, `tests/v2/test_config_models.py`, `tests/v2/test_plan_compiler.py`, `tests/v2/test_ui_v2_plan_editor_page.py` | `selected_pressure_points`, `ambient`, label, and `ambient_open` semantics already flow through V2 config/compiler/handoff; avoid redoing a large migration. |
| room-temp pressure diagnostic / plots / analyzer-chain isolation | `src/gas_calibrator/validation/room_temp_co2_pressure_diagnostic.py`, `src/gas_calibrator/validation/room_temp_co2_pressure_plots.py` | partially synced | offline-only | `src/gas_calibrator/v2/core/offline_artifacts.py`, `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/pages/reports_page.py` | `tests/test_room_temp_co2_pressure_diagnostic.py`, `tests/v2/test_build_offline_governance_artifacts.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_app_facade.py`, `tests/v2/test_ui_v2_reports_page.py`, `tests/v2/test_ui_v2_review_center.py` | High-value engineering analysis, but Step 2 safe scope is offline analytics/report/review adapter only; do not enter default execution path. |
| `validate_verification_doc` helper | `src/gas_calibrator/tools/validate_verification_doc.py`, `tests/test_validate_verification_doc.py` | partially synced | offline-only | standalone helper only; if V2-adapted, keep it under offline export/validation helpers | `tests/test_validate_verification_doc.py` | Current test is already portable; this should remain a standalone/offline helper and must not enter the default V2 run path. |
| coefficient quiet I/O | `src/gas_calibrator/devices/gas_analyzer.py` | already synced | already synced | `src/gas_calibrator/v2/adapters/analyzer_coefficient_downloader.py` | `tests/v2/test_analyzer_coefficient_downloader.py` | V2 adapter already inherits shared analyzer quiet-I/O behavior. |
| legacy `YGAS` mode1 parse | `src/gas_calibrator/devices/gas_analyzer.py` | already synced | already synced | shared analyzer parser used by V2 adapters | `tests/v2/test_analyzer_coefficient_downloader.py`, existing shared analyzer tests | V2 already inherits the shared parser; no V2-specific copy is needed. |
| logging/export taxonomy parity | `src/gas_calibrator/logging_utils.py` | partially synced | safe to extend | `src/gas_calibrator/v2/core/result_store.py`, `src/gas_calibrator/v2/core/offline_artifacts.py`, `src/gas_calibrator/v2/adapters/results_gateway.py`, `src/gas_calibrator/v2/ui_v2/controllers/app_facade.py`, `src/gas_calibrator/v2/ui_v2/controllers/device_workbench.py`, `src/gas_calibrator/v2/ui_v2/pages/reports_page.py` | `tests/v2/test_result_store.py`, `tests/v2/test_results_gateway.py`, `tests/v2/test_ui_v2_app_facade.py`, `tests/v2/test_ui_v2_results_page.py`, `tests/v2/test_ui_v2_device_workbench.py`, `tests/v2/test_ui_v2_workbench_evidence.py` | Sync the field taxonomy only: pressure mode/label, post-seal metrics, stale gauge metrics, flush-gate metrics. Do not copy the V1 logger implementation. |

## Current Step 2 classification

- `already synced`
  - ambient pressure semantics
  - CO2 post-seal quality guards
  - coefficient quiet I/O inheritance
  - legacy `YGAS` mode1 parse inheritance
- `safe to extend`
  - offline room-temp/analyzer-chain review adapter
  - V2 logging/export field taxonomy parity
- `offline-only`
  - room-temp diagnostic bundle adapter
  - verification-doc helper
- `do not sync into default path`
  - V1 Tk UI behavior
  - real-COM / engineering-only config paths
  - any Step 3 cutover behavior
