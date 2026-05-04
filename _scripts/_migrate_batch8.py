"""Batch 8: migrate remaining conditioning methods"""
import re, ast

ORCH = r'D:\gas_calibrator\src\gas_calibrator\v2\core\orchestrator.py'
CS = r'D:\gas_calibrator\src\gas_calibrator\v2\core\services\conditioning_service.py'

METHOD_NAMES = [
    '_a2_conditioning_pressure_sample',
    '_a2_conditioning_update_pressure_metrics',
    '_a2_route_open_transient_update',
    '_a2_conditioning_emergency_abort_relief_decision',
    '_a2_conditioning_pressure_sample_from_snapshot',
    '_a2_conditioning_defer_diagnostic_for_vent_priority',
    '_a2_conditioning_terminal_gap_details',
    '_a2_latest_route_conditioning_prearm_baseline',
    '_wait_a2_co2_route_open_settle_before_conditioning',
    '_a2_preseal_capture_arm_context',
    '_a2_conditioning_context_with_counts',
    '_fail_a2_co2_route_conditioning_closed',
    '_a2_conditioning_failure_context',
    '_a2_conditioning_reschedule_after_defer',
    '_a2_conditioning_heartbeat_gap_state',
    '_a2_conditioning_terminal_gap_source',
    '_a2_conditioning_vent_gap_source',
    '_a2_conditioning_fail_if_defer_not_rescheduled',
]

with open(ORCH, encoding='utf-8') as f:
    orch_lines = f.readlines()
with open(CS, encoding='utf-8') as f:
    cs_text = f.read()

orch_before = len(orch_lines)
cs_before = len(cs_text.split('\n'))
print(f"Before: orchestrator={orch_before} conditioning={cs_before}")

# Find line ranges
method_ranges = {}
for name in METHOD_NAMES:
    for i, line in enumerate(orch_lines):
        stripped = line.strip()
        if f'def {name}(' in stripped or f'def {name} ' in stripped:
            indent = len(line) - len(line.lstrip())
            end = i + 1
            for j in range(i+1, len(orch_lines)):
                s = orch_lines[j].strip()
                if s.startswith('def ') and len(orch_lines[j]) - len(orch_lines[j].lstrip()) == indent:
                    end = j
                    break
            method_ranges[name] = (i, end)
            break

print(f"Found {len(method_ranges)}/{len(METHOD_NAMES)} methods")

# Transform and collect
transformed_data = []
total_transforms = 0
total_lines_moved = 0
for name in METHOD_NAMES:
    if name not in method_ranges:
        continue
    start_0, end_0 = method_ranges[name]
    method_text = ''.join(orch_lines[start_0:end_0])
    nt = len(re.findall(r'\bself\.(?!host\.)', method_text))
    transformed = re.sub(r'\bself\.(?!host\.)', 'self.host.', method_text)
    transformed_data.append((name, start_0, end_0, transformed, nt))
    total_transforms += nt
    total_lines_moved += end_0 - start_0
    print(f"  {name}: {end_0 - start_0} lines, {nt} transforms")

# Append to CS
cs_new = cs_text.rstrip('\n')
for name, start_0, end_0, transformed, nt in transformed_data:
    cs_new += '\n' + transformed

# Replace in orchestrator (descending by start line)
new_orch = list(orch_lines)
for name, start_0, end_0, transformed, nt in sorted(transformed_data, key=lambda x: -x[1]):
    delegation = f'    def {name}(self, *args, **kwargs):\n        return self.conditioning_service.{name}(*args, **kwargs)\n\n'
    new_orch[start_0:end_0] = [delegation]

with open(ORCH, 'w', encoding='utf-8') as f:
    f.writelines(new_orch)
with open(CS, 'w', encoding='utf-8') as f:
    f.write(cs_new)

orch_after = len(new_orch)
cs_after = len(cs_new.split('\n'))
print(f"\nAfter: orchestrator={orch_after} conditioning={cs_after}")
print(f"Orchestrator: {orch_before} -> {orch_after} ({orch_before - orch_after} removed)")
print(f"Conditioning: {cs_before} -> {cs_after} (+{cs_after - cs_before})")
print(f"Total transforms: {total_transforms}, lines moved: {total_lines_moved}")

try:
    ast.parse(''.join(new_orch))
    print("orchestrator syntax: OK")
except SyntaxError as e:
    print(f"orchestrator syntax ERROR: {e}")
try:
    ast.parse(cs_new)
    print("conditioning_service syntax: OK")
except SyntaxError as e:
    print(f"conditioning_service syntax ERROR: {e}")
