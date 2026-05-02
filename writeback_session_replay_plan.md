# writeback session replay plan

## scope

- Repo scope: V1 only
- Device scope: `079`
- Port scope: `COM39`
- This pass is offline replay and tooling preparation only. It does not authorize any live coefficient write.

## historical session reconstruction

Historical success under the old `6/6` criterion came from [`live_write_079_clean_20260419_204500/summary.csv`](/D:/gas_calibrator/logs/run_20260411_204123/recomputed_ambient_only_7feat_079_single_run_20260419_203948/live_write_079_clean_20260419_204500/summary.csv), [`detail.csv`](/D:/gas_calibrator/logs/run_20260411_204123/recomputed_ambient_only_7feat_079_single_run_20260419_203948/live_write_079_clean_20260419_204500/detail.csv), and [`selected_write_summary.json`](/D:/gas_calibrator/logs/run_20260411_204123/recomputed_ambient_only_7feat_079_single_run_20260419_203948/live_write_079_clean_20260419_204500/selected_write_summary.json). The code path was `write_coefficients_to_live_devices(...) -> write_senco_groups_with_full_verification(...) -> _restore_stream_settings(...)`.

### 1. 历史 writeback session 里，在 GETCO 之前到底执行了哪些命令？

Visible outer sequence:

1. `scan_live_targets(...)`
2. `open`
3. best-effort `SETCOMWAY,YGAS,FFF,0`
4. stream read used only to identify live device id
5. `close`
6. fresh writeback `open`
7. best-effort `SETCOMWAY,YGAS,FFF,0`
8. pre-write `GETCO` snapshots for groups `1/2/3/4/7/8`
9. `MODE,YGAS,FFF,2`
10. `SENCO*`
11. post-write `GETCO`
12. `MODE,YGAS,FFF,1`
13. restore helper writes `MODE,YGAS,FFF,2`, `FTD`, `AVERAGE`, `SETCOMWAY`

Important hidden detail from [gas_analyzer.py](/D:/gas_calibrator/src/gas_calibrator/devices/gas_analyzer.py:284):

- Every `GETCO` goes through `_prepare_coefficient_io()`, which itself does another best-effort `SETCOMWAY,YGAS,FFF,0`, waits the coefficient quiet window, then flushes input before the actual `GETCO`.
- Every `SENCO*` also goes through `_prepare_coefficient_io()` first.

So the real pre-`GETCO` command envelope was not just one outer `SETCOMWAY 0`; it was a repeated quieting pattern around each coefficient read and write attempt.

### 2. 哪些命令属于可能打开系数读写窗口的 session prime？

Most likely prime candidates, ordered by confidence:

1. Repeated `SETCOMWAY,YGAS,FFF,0` plus quiet delay plus input flush from `_prepare_coefficient_io()`
2. Entering `MODE,YGAS,FFF,2`
3. The fact that `GETCO` happened inside a driver-managed writeback session rather than a standalone probe loop
4. A preceding successful `SENCO*` write, after which the immediate verification `GETCO` may have been reading from a transient writeback-responsive window

Less supported by current evidence:

- `FTD` / `AVERAGE` toggles as a prime before the historical successful `GETCO`
- `SETCOMWAY 1 -> SETCOMWAY 0` alone as a sufficient standalone recipe

The current read-only replays in [getco_session_root_cause.md](/D:/gas_calibrator/getco_session_root_cause.md) did not reproduce explicit `C0` using visible read-only steps alone.

### 3. 哪些命令可能是持久写入？

Definitely persistent or state-changing:

- `SENCO1/2/3/4/7/8`
- `MODE`
- `SETCOMWAY`
- `FTD`
- `AVERAGE1` / `AVERAGE2` or `set_average_filter_with_ack(...)`

Risk ranking:

- Highest risk: `SENCO*` because they touch calibration coefficients directly
- Medium risk: `MODE`, `SETCOMWAY`, `FTD`, `AVERAGE*` because they change live session/runtime state and must be restored correctly
- Lowest risk: `GETCO` and stream reads, because they are read-only commands

### 4. 哪些命令可以安全复现？

Safe now:

- Offline code and log replay
- Historical artifact comparison
- Dry-run planning that does not open COM or send `SENCO*`

Controlled-only in a future live truth probe:

- `open` / `close`
- Stream reads for identity confirmation
- `GETCO`
- `SETCOMWAY 0`
- `MODE`, `FTD`, `AVERAGE*`

Not safe to replay without explicit approval:

- Any `SENCO*` payload, even if intended as a same-value no-op

### 5. 历史 “6/6 success” 是否可能只是写入 echo/混合行假阳性？

Yes, under the new explicit-`C0` truth rule it is plausible.

More precise conclusion:

- Pure numeric `6/6` was internally consistent under the old verifier.
- The old verifier did not persist the accepted source line.
- The old parser accepted any line containing `C0...Cn` tokens, not only standalone explicit `C0:` lines.
- A pure `SENCO` echo without `C0:` would not have been enough.
- But a mixed line such as `YGAS,... <C0:...>` or another write-adjacent hybrid line could have satisfied the old verifier and produced a false positive under today’s stricter truth standard.

That is exactly why [writeback_readback_truth_audit.md](/D:/gas_calibrator/writeback_readback_truth_audit.md) no longer treats the old `6/6` as sufficient truth evidence.

### 6. 如果要做 no-op same-value 写回，需要哪些前置保护？

Required protections:

1. Explicit operator approval before any live write
2. Limit the scope to `079 / COM39` only
3. Freeze the candidate inputs from the current candidate directory and exclude pressure group `9`
4. Record full raw transcript for prepare / `MODE` / `SENCO` / `GETCO` / restore
5. Mark each readback source as exactly one of `explicit_c0`, `ambiguous`, `none`
6. Count `verified=true` only when the readback source is `explicit_c0`
7. Abort if the live scanned device id is not exactly `079`
8. Refuse execution when same-value status is still unconfirmed, unless the operator explicitly accepts that risk

## replay recommendation

- First use the new dry-run tool to generate a frozen plan and review the candidate groups.
- If a live truth probe is later approved, run only a controlled no-op same-value writeback session with full transcript capture.
- Do not treat any dry-run, replay, or simulated evidence as real acceptance evidence.
