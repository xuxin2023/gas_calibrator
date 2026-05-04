# A2.4 real probe fail-closed summary before A2.5

- branch: codex/run001-a1-no-write-dry-run
- head: 8ffb4bbfe314d35863cc1e9696aa45b2bae86ddf
- a2_4_dirty_diff_sha256: cd7ad1fa8a71ee0f901db8ad3242c08af7a6df27fdcbc954ae564452c8066a32
- final_decision: FAIL_CLOSED
- rejection_reasons: ["a2_route_conditioning_vent_gap_exceeded"]
- route_conditioning_phase: route_conditioning_flush_phase
- route_conditioning_vent_maintenance_active: true
- vent_pulse_count: 1
- vent_pulse_interval_ms: []
- last_vent_command_age_s: 11.967
- heartbeat_gap_observed_ms: 11966.95
- route_conditioning_vent_gap_exceeded: true
- route_conditioning_peak_pressure_hpa: 1017.399
- route_conditioning_pressure_overlimit: false
- pressure_points_completed: 0
- points_completed: 0
- sample_count_total: 0
- no_write: true
- attempted_write_count: 0
- any_write_command_sent: false
- chamber_set_temperature_command_sent: false
- chamber_start_command_sent: false
- chamber_stop_command_sent: false
- real_primary_latest_refresh: false

Interpretation: the deterministic maintenance loop direction is correct, but the real K0472/PACE A2 conditioning vent tick blocked for about 11.964 s before route open. A2.5 must move high-frequency maintenance to a bounded fast vent reassert path and keep slow diagnostics out of the scheduler path.
