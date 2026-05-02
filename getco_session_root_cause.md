# GETCO Session Root Cause (079 / COM39)

## Historical 6/6 Success Path

- The confirmed `079` success artifact is [`live_write_079_clean_20260419_204500/summary.csv`](/D:/gas_calibrator/logs/run_20260411_204123/recomputed_ambient_only_7feat_079_single_run_20260419_203948/live_write_079_clean_20260419_204500/summary.csv), which shows `COM39`, `TargetDeviceId=079`, `MatchedGroups=6`, `Status=ok`.
- The matching detail file [`live_write_079_clean_20260419_204500/detail.csv`](/D:/gas_calibrator/logs/run_20260411_204123/recomputed_ambient_only_7feat_079_single_run_20260419_203948/live_write_079_clean_20260419_204500/detail.csv) shows `GETCO1/2/3/4/7/8` all succeeded inside `write_senco_groups_with_full_verification(...)`.
- In code, that path is not a standalone raw probe. The visible session sequence is:
  1. `scan_live_targets(...)` opens the analyzer and identifies the live device from a stream frame after a best-effort `SETCOMWAY ... 0`.
  2. `write_coefficients_to_live_devices(...)` opens a fresh `GasAnalyzer`, sends another best-effort `SETCOMWAY ... 0`, and then enters `write_senco_groups_with_full_verification(...)`.
  3. `write_senco_groups_with_full_verification(...)` performs pre-write `GETCO` snapshots, enters calibration mode, performs readback, then exits mode.
  4. `_restore_stream_settings(...)` finally restores mode / FTD / averaging / comm-way.

## Current Standalone Failure Pattern

- The standalone probe artifact [`getco_probe_20260420_110531/getco_probe_summary.json`](/D:/gas_calibrator/logs/run_20260411_204123/runtime_micro_perturb_079_20260420_103526/getco_probe_20260420_110531/getco_probe_summary.json) shows every `GETCO1/3/7/8` attempt ending as `raw_received_but_unparsed` with `failure_reason=active_stream_only`.
- The direct diagnostic artifact [`readback_diagnostic_group1.json`](/D:/gas_calibrator/logs/run_20260411_204123/runtime_micro_perturb_079_20260420_103526/readback_diagnostic_group1.json) shows repeated `GETCO1` attempts failing with `NO_VALID_COEFFICIENT_LINE` even when timeout/retry are widened.
- Raw command probes on `COM39` showed that `MODE`, `SETCOMWAY`, `FTD`, `GETCO,YGAS,079,*`, and `GETCO,YGAS,FFF,*` all returned only `YGAS` stream frames during the current active-stream session.

## Key Differences

- The historical success was achieved inside a driver-managed writeback session, not a raw probe loop.
- The historical code path almost certainly used the old broadcast-style `GETCO,YGAS,FFF,*` query shape at the time of that 6/6 success. The current standalone tooling primarily exercised explicit `079` targeting first.
- The standalone probe did not re-create the full writeback-session sequencing around mode / stream / quiet-window / restore.

## What The 2026-04-20 Replay Established

- Replaying the visible session steps on `COM39 / 079` with read-only commands (`MODE -> FTD -> AVERAGE -> SETCOMWAY 1 -> SETCOMWAY 0 -> GETCO`) still produced only stream frames.
- Re-trying both target styles (`079` and `FFF`) inside that replayed session still produced `NO_VALID_COEFFICIENT_LINE`.

## Conclusion

- Parser strictness was only a secondary issue.
- The dominant current failure is session-state: the analyzer is not entering a readback-responsive window from the presently observable read-only command sequence.
- The historical 6/6 success depended on at least one additional precondition that is not reproduced by the current standalone probe, nor by the currently visible subset of the writeback-session steps.
