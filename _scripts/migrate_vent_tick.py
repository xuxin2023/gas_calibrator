"""Extract _record_a2_co2_conditioning_vent_tick from orchestrator and migrate to conditioning_service."""
import re

orch_path = r'src/gas_calibrator/v2/core/orchestrator.py'
cond_path = r'src/gas_calibrator/v2/core/services/conditioning_service.py'

with open(orch_path, encoding='utf-8') as f:
    lines = f.readlines()

start = next(i for i, l in enumerate(lines) if 'def _record_a2_co2_conditioning_vent_tick' in l)
indent = len(lines[start]) - len(lines[start].lstrip())
end = next(i for i in range(start + 1, len(lines)) if lines[i].strip() and not lines[i].startswith(' ' * (indent + 1)))

body_lines = lines[start:end]

orchestrator_only = [
    'self.a2_hooks', 'self.pressure_control_service',
    'self._as_float(', 'self._cfg_get(',
    'self._fail_a2_co2_route_conditioning_closed(',
    'self._record_a2_conditioning_workflow_timing(',
    'self._record_pressure_source_latency_events(',
    'self._a2_conditioning_vent_schedule(',
    'self._a2_conditioning_defer_reschedule_state(',
    'self._a2_conditioning_unsafe_vent_reason(',
    'self._a2_conditioning_mark_vent_blocked(',
    'self._a2_conditioning_heartbeat_gap_state(',
    'self._a2_conditioning_vent_gap_source(',
    'self._a2_conditioning_terminal_gap_details(',
    'self._a2_conditioning_update_pressure_metrics(',
    'self._a2_route_open_transient_evidence(',
    'self._a2_conditioning_scheduler_evidence(',
    'self._a2_conditioning_digital_gauge_evidence(',
    'self._a2_conditioning_context_with_counts(',
    'self._a2_route_open_transient_mark_continuing_after_defer_warning(',
]

transformed = list(body_lines)
for i, l in enumerate(transformed):
    for old in orchestrator_only:
        new = 'self.host.' + old[5:]
        l = l.replace(old, new)
    l = l.replace('self.a2_hooks.', 'self.host.a2_hooks.')
    l = l.replace('self.pressure_control_service.', 'self.host.pressure_control_service.')
    l = l.replace('self._a2_conditioning_hard_abort_pressure_hpa', 'self._a2_route_conditioning_hard_abort_pressure_hpa')
    transformed[i] = l

# Build the new method with correct signature
method_sig = '    def _record_a2_co2_conditioning_vent_tick(self, point: Any, *, phase: str = "") -> dict[str, Any]:\n'
method_body = ''.join(transformed[1:])  # skip old signature line
new_method = method_sig + method_body

with open(cond_path, encoding='utf-8') as f:
    cond = f.read()

# Insert before the last method or at end
# Find _begin_a2_co2_route_conditioning_at_atmosphere method's closing
marker = 'return self.host.a2_hooks.co2_route_conditioning_at_atmosphere_context\n'
idx = cond.rindex(marker)
idx = cond.index('\n', idx) + 1

new_cond = cond[:idx] + '\n' + new_method + '\n' + cond[idx:]

with open(cond_path, 'w', encoding='utf-8', newline='\n') as f:
    f.write(new_cond)

print(f'conditioning_service.py: {len(new_cond)} chars, {new_cond.count(chr(10))} lines')
print(f'Added method: {len(body_lines)} lines')
print('Done')
