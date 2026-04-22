# Writeback Readback Truth Audit (079 / COM39)

## Scope

- Device under review: `079`
- Port under review: `COM39`
- Historical artifact under review:
  - [detail.csv](/D:/gas_calibrator/logs/run_20260411_204123/recomputed_ambient_only_7feat_079_single_run_20260419_203948/live_write_079_clean_20260419_204500/detail.csv)
  - [summary.csv](/D:/gas_calibrator/logs/run_20260411_204123/recomputed_ambient_only_7feat_079_single_run_20260419_203948/live_write_079_clean_20260419_204500/summary.csv)
  - [selected_write_summary.json](/D:/gas_calibrator/logs/run_20260411_204123/recomputed_ambient_only_7feat_079_single_run_20260419_203948/live_write_079_clean_20260419_204500/selected_write_summary.json)

## What Historical `6/6` Success Actually Meant

- The historical `6/6` success was produced by `write_senco_groups_with_full_verification(...)`.
- That path judged success as:
  1. `ga.read_coefficient_group(...)` returned parseable `C0..Cn` values.
  2. `_read_group_with_match_retry(...)` found numeric equality against the expected coefficients.
  3. `detail.csv` stored `ReadbackOk=True` and the numeric vectors.
- The historical artifact did **not** store:
  - raw GETCO transcript
  - the exact line that satisfied the parser
  - whether the accepted source line was an explicit standalone `C0:` line
  - whether the accepted line was mixed with legacy stream text

## What The Old Verifier Did Not Enforce

- Before this audit tightening, `parse_coefficient_group_line(...)` accepted any line containing `C0..Cn` tokens.
- That means the old verifier did **not** require an explicit standalone `C0:` line.
- A line like `YGAS,... <C0:...>` could be parsed and accepted.
- Pure legacy stream frames without `C0:` were not valid readback.
- Plain `SENCO...` write echo text without `C0:` was not valid readback.
- The ambiguous zone was therefore:
  - mixed line containing normal stream text plus embedded `C0:` tokens
  - any other non-standalone line carrying `C0:` tokens

## Historical Reliability Assessment

- The historical `detail.csv` proves that the old code believed all 6 groups matched numerically.
- It does **not** prove that the session saw explicit standalone `C0:` readback lines.
- The current real-device probes on `079 / COM39` have not reproduced explicit `C0:` responses in standalone readback sessions.
- Because the historical artifact lacks source-line evidence, the old `6/6` result cannot be promoted to the stronger “explicit-C0-backed truth” standard retroactively.

## Code Changes Introduced By This Audit

- Readback capture now classifies each GETCO result as exactly one of:
  - `parsed_from_explicit_c0_line`
  - `parsed_from_ambiguous_line`
  - `no_valid_coefficient_line`
- Live coefficient backup and writeback verification now require `parsed_from_explicit_c0_line` to count as success.
- Future writeback detail rows now record:
  - source classification
  - accepted source line
  - explicit-C0 flag
  - command text
  - target id
  - raw transcript lines
  - per-attempt transcript batches

## Real Replay Decision

- A new real replay writeback audit was **not** executed in this turn.
- Reason: current `079 / COM39` still lacks a reliable explicit-C0 live backup/readback window, so repeating a write session would add real-device risk without a trustworthy rollback evidence chain.

## Audit Conclusion

- Under the old numeric-match-only criterion, the historical `6/6` result was internally consistent.
- Under the new explicit-C0 truth criterion, the historical `6/6` result is **not sufficiently trustworthy**, because its artifact set does not prove that the accepted readback source was an explicit standalone `C0:` line.
