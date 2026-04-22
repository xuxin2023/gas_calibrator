# V1 Runtime Offset Trim

## Purpose
- `V1` can produce a candidate with good offline fit quality and still show a live runtime offset after the coefficients are written to a real analyzer.
- The new `run_v1_runtime_offset_trim.py` sidecar tool gives `V1` a controlled post-write correction path for that case.
- The tool is intentionally outside the main calibration workflow. It does not change `run_app.py`, device drivers, or the normal V1 workflow timing.

## Why Offline RMSE Alone Is Not Enough
- Offline RMSE is computed against stored calibration data and fitted features.
- A real analyzer can still show a runtime offset after download because the runtime execution path, transport state, or on-device baseline can differ from the offline fitting context.
- In practice this means a candidate may look acceptable offline, pass strict coefficient readback, and still be uniformly shifted at a real standard gas point.

## What a0 Trim Means
- `a0` trim is a runtime offset correction on the `CO2` main chain.
- It is not a full re-fit and it does not replace the underlying fitted curve.
- The tool only changes `SENCO1.C0` and keeps `C1..C5` unchanged.
- `H2O`, `SENCO3`, `SENCO7/8`, and `SENCO9` remain untouched.

## When It Is Allowed
- A trusted standard gas target must be available.
- The tool must obtain strict explicit-`C0` readback for `GETCO,YGAS,<device_id>,1` and `GETCO,YGAS,<device_id>,3`.
- The live device must already match the candidate under strict explicit-`C0` readback before trim is applied.
- A response slope may be supplied when there is prior evidence that runtime `CO2` responds to `a0` with a non-unit slope.
- If no better evidence exists, `response_slope=1.0` may be used conservatively.

## When It Is Not Allowed
- Do not use room air or ambient air as the target.
- Do not use the tool when strict explicit-`C0` readback is incomplete.
- Do not treat the tool as a substitute for multi-point model fitting.
- Do not auto-run it as part of the primary V1 workflow.

## Why High-Point Checks Still Matter
- A single trim point can fix a uniform runtime offset while still leaving high-end gain or curve error.
- The recommended sequence is:
- `400 ppm` trim point to remove obvious offset.
- At least one higher read-only confirmation point such as `600 ppm` or `1000 ppm`.
- If low/mid points pass but the high end shows `review`, keep the trim result but mark `high_end_review_needed`.

## Tool Outputs
- `runtime_offset_trim_plan.json`
- `runtime_offset_trim_pre_capture.csv`
- `runtime_offset_trim_pre_summary.json`
- `runtime_offset_trim_writeback.json`
- `runtime_offset_trim_post_capture.csv`
- `runtime_offset_trim_post_summary.json`
- `runtime_offset_trim_report.md`
- `raw_transcript.log`

## Post-Offset Candidate
- When `execute=true` and the post-trim point passes, the tool generates a new post-offset candidate directory.
- Only the target device `CO2 a0` is updated in `download_plan_no_500.csv`.
- Other coefficients remain unchanged.
- The post-offset manifest records:
- `correction_basis = runtime_standard_gas_single_point_offset`
- `full_range_verified = false`
- `final_write_ready = false` unless higher-point confirmation and broader runtime readiness are also closed.

## High-Point Read-Only Mode
- The same tool also provides a read-only high-point check mode.
- This mode never writes the device.
- It captures live runtime `YGAS`, computes residual against a known standard gas target, and classifies the result as `pass`, `review`, or `fail`.
