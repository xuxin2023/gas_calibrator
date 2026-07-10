# V1.5 Route Physical Recovery Evidence Binder Test Evidence

## Scope

This package adds an offline binder that converts reviewed trace files into a
`v1_5_route_physical_recovery_evidence_packet_v1` JSON packet for the existing
packet validator. It does not collect live data.

## Focused Validation

```powershell
python -m pytest tests\test_v1_5_route_physical_recovery_evidence_binder.py -q
```

Result:

```text
4 passed in 1.93s
```

## Compatibility Validation

```powershell
python -m pytest tests\test_v1_5_route_physical_recovery_evidence_binder.py tests\test_v1_5_route_physical_recovery_evidence_packet_template.py tests\test_v1_5_route_physical_recovery_evidence_packet.py tests\test_v1_5_route_physical_recovery_readiness.py -q
```

Result:

```text
21 passed in 5.04s
```

## Entry Point Guard Validation

```powershell
python -m pytest tests\test_v1_5_route_physical_recovery_evidence_binder.py tests\test_v1_5_route_physical_recovery_evidence_packet_template.py tests\test_v1_5_route_physical_recovery_evidence_packet.py tests\test_v1_5_route_physical_recovery_readiness.py tests\test_v1_5_entrypoint_inventory.py tests\test_v1_5_production_entrypoint_gate.py -q
```

Result:

```text
54 passed, 1 warning in 37.59s
```

The warning is the existing unregistered `v1_5_formal_gate` marker.

## Sample Packet Validator Check

```powershell
python -m gas_calibrator.tools.export_v1_5_route_physical_recovery_evidence_packet `
  --recovery-evidence-packet-path docs\v1_5_flow_contract\route_physical_recovery_evidence_binder\v1_5_route_physical_recovery_evidence_packet_from_traces.json `
  --output-dir %TEMP%\v1_5_binder_packet_review `
  --fail-on-blocker
```

Result:

```text
status=pass
blocker_count=0
readiness_input_ready=true
segmented_evidence_review_ready=true
```

This check uses the offline sample fixture only. It proves format compatibility
with the packet validator, not real physical recovery.

## Boundary

- opens_com_ports=false
- controls_pressure=false
- controls_water_or_gas_routes=false
- connects_postgresql=false
- writes_coefficients=false
- writes_sn_or_device_code=false
- formal_release_allowed=false
- database_import_allowed=false
- not_real_acceptance_evidence=true

The sample inputs under `sample_inputs/` are offline fixtures only. Real route
recovery evidence must replace them before any physical recovery packet is used
for readiness review.
