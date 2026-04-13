# Online Evidence Summary

- generated_at: 2026-04-13T12:47:29+08:00
- head: `f41b7b20c35a5051943fecd35bdaf62c05ae8d34`
- offline_fault_injection = PASS
- real_device_abnormal_recovery = ONLINE_EVIDENCE_REQUIRED
- latest_status: ONLINE_EVIDENCE_REQUIRED
- latest_mode: dry_run

## Boundary

- Current HEAD V1 only supports the CO2 main chain; H2O zero/span is NOT_SUPPORTED.
- Dry-run artifacts do not count as real acceptance evidence.

## Code / Offline Proven

- CO2 zero/span main chain is covered in code and offline acceptance artifacts.
- H2O zero/span is explicitly NOT_SUPPORTED on this HEAD; no H2O online acceptance should be attempted.
- Shared writeback helper already proves offline: snapshot -> write -> GETCO readback -> mismatch rollback -> finally restore mode.
- Offline fault injection already proves attempted-vs-confirmed mode exit semantics and unsafe marking.

## Real-Device Proven

- No real-device run has yet produced confirmed abnormal-recovery evidence that closes this item.

## Missing / Pending Real-Device Evidence

- A real-device `online_run_*.json` summary with complete required fields.
- A matching `online_protocol_*.jsonl` log preserving `raw_command` and `raw_response` for the actual bench session.
- A run where final mode exit is both attempted and confirmed on the real device.
- If rollback is triggered on bench, a run where rollback attempt and rollback confirmation are both evidenced.

## Latest Run

- run_id: ``
- session_id: ``
- device_id: ``
- unsafe: `False`
- mode_exit_confirmed: `False`
- rollback_confirmed: `False`
- failure_reason: (none)
- assessment: This is still dry-run/template-only evidence, so the overall item remains `ONLINE_EVIDENCE_REQUIRED`.
