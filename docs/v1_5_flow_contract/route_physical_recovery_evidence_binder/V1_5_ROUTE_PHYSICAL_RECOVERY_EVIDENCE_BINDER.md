# V1.5 Route Physical Recovery Evidence Binder

- schema: `v1_5_route_physical_recovery_evidence_binder_v1`
- status: `packet_ready_for_validator`
- ready_for_validator: `True`
- blocker_count: `0`
- boundary: offline trace binding only; no COM, no pressure/route control, no writes, no PostgreSQL.

## Trace Inputs

- dewpoint_trace_path: `docs\v1_5_flow_contract\route_physical_recovery_evidence_binder\sample_inputs\dry_gas_dewpoint_trace.csv`
- pace_vent_trace_path: `docs\v1_5_flow_contract\route_physical_recovery_evidence_binder\sample_inputs\pace_vent_roundtrip.csv`
- pressure_gauge_trace_path: `docs\v1_5_flow_contract\route_physical_recovery_evidence_binder\sample_inputs\com22_inl_readback.csv`

## Findings

| severity | requirement | status | reason | required action |
|---|---|---|---|---|
| `info` | `dry_gas_dewpoint_recovery` | `pass` | Dry-gas dewpoint tail worst value is -29.9 C with stable span/slope. | Feed this packet to the route physical recovery packet validator. |
| `info` | `pace_vent_recovery` | `pass` | PACE vent trace proves ON/OFF roundtrip with no NO_RESPONSE. | Feed this packet to the route physical recovery packet validator. |
| `info` | `pressure_gauge_recovery` | `pass` | Pressure gauge trace proves INL absolute-pressure readback with valid numeric samples and no NO_RESPONSE. | Feed this packet to the route physical recovery packet validator. |

## Boundary

- This binder does not collect live data. It only reads reviewed trace files.
- The emitted packet must still pass `export_v1_5_route_physical_recovery_evidence_packet.py`.
- The binder output is not formal release, database import, route execution, or real acceptance evidence.
