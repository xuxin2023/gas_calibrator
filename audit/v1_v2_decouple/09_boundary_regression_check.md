# Boundary Regression Check

- BASE_HEAD: `367a1089ebaca1388dbb9d11648f74513316e502`

## SAFE_CHANGE

- `postrun_corrected_delivery` default safety restored
  - affected_file: [src/gas_calibrator/config.py](D:/gas_calibrator/src/gas_calibrator/config.py:229)
  - old_behavior: defaults had drifted back to `enabled=True` and `write_devices=True`
  - new_behavior: defaults are back to `enabled=False` and `write_devices=False`
  - impact_on_v1_co2_main_flow: protects the V1 default runtime from accidental real-device writeback
  - risk_level: High
  - evidence: [config.py](D:/gas_calibrator/src/gas_calibrator/config.py:230), [config.py](D:/gas_calibrator/src/gas_calibrator/config.py:232), [tests/test_runner_v1_writeback_safety.py](D:/gas_calibrator/tests/test_runner_v1_writeback_safety.py:100), [tests/test_config_runtime_defaults.py](D:/gas_calibrator/tests/test_config_runtime_defaults.py:137)
  - recommendation: keep this default under regression coverage

- V1 runtime no longer reaches V2 through the no-500 sidecar import chain
  - affected_file: [src/gas_calibrator/tools/run_v1_corrected_autodelivery.py](D:/gas_calibrator/src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:33)
  - old_behavior: imported `run_v1_no500_postprocess`, which imported V2 modules
  - new_behavior: imports the pure helper [src/gas_calibrator/tools/_no500_filter.py](D:/gas_calibrator/src/gas_calibrator/tools/_no500_filter.py:55)
  - impact_on_v1_co2_main_flow: removes one real runtime boundary leak without changing calculation behavior
  - risk_level: Medium
  - evidence: [run_v1_corrected_autodelivery.py](D:/gas_calibrator/src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:33), [run_v1_no500_postprocess.py](D:/gas_calibrator/src/gas_calibrator/tools/run_v1_no500_postprocess.py:22), [tests/test_v1_v2_bridge_allowlist.py](D:/gas_calibrator/tests/test_v1_v2_bridge_allowlist.py:115)
  - recommendation: keep pure reusable helpers in shared-via-small-module form, not inside bridge files

## INTENTIONAL_BOUNDARY

- H2O zero/span remains explicitly blocked on the V1 side
  - affected_file: [src/gas_calibrator/config.py](D:/gas_calibrator/src/gas_calibrator/config.py:273)
  - old_behavior: not part of this round; boundary already fixed
  - new_behavior: unchanged `NOT_SUPPORTED` boundary
  - impact_on_v1_co2_main_flow: keeps V1 CO2-only semantics intact
  - risk_level: Low
  - evidence: [config.py](D:/gas_calibrator/src/gas_calibrator/config.py:325), [runner.py](D:/gas_calibrator/src/gas_calibrator/workflow/runner.py:4662), [tests/test_runner_v1_writeback_safety.py](D:/gas_calibrator/tests/test_runner_v1_writeback_safety.py:128)
  - recommendation: do not add H2O zero/span support to V1 without an explicit new requirement

- Online acceptance remains guarded and does not become a V2 entry
  - affected_file: [src/gas_calibrator/tools/run_v1_online_acceptance.py](D:/gas_calibrator/src/gas_calibrator/tools/run_v1_online_acceptance.py:1)
  - old_behavior: already guarded
  - new_behavior: unchanged; boundary note added
  - impact_on_v1_co2_main_flow: none, but keeps the ownership line explicit
  - risk_level: Low
  - evidence: [run_v1_online_acceptance.py](D:/gas_calibrator/src/gas_calibrator/tools/run_v1_online_acceptance.py:33), [run_v1_online_acceptance.py](D:/gas_calibrator/src/gas_calibrator/tools/run_v1_online_acceptance.py:611), [tests/test_v1_online_acceptance_tool.py](D:/gas_calibrator/tests/test_v1_online_acceptance_tool.py:78)
  - recommendation: keep dual-gate tests in the focused set

## POSSIBLE_REGRESSION

- None remaining after this round.
- The one newly found runtime boundary leak was the indirect `run_v1_corrected_autodelivery -> run_v1_no500_postprocess -> v2` chain, and it has been removed before close-out.
