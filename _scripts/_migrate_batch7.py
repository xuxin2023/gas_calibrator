"""Batch 7: migrate 6 methods from orchestrator to conditioning_service"""
import re, ast

ORCH = r'D:\gas_calibrator\src\gas_calibrator\v2\core\orchestrator.py'
CS = r'D:\gas_calibrator\src\gas_calibrator\v2\core\services\conditioning_service.py'

METHODS = [
    ('_a2_conditioning_pressure_details', 4326, 4711),
    ('_wait_co2_route_soak_before_seal', 1485, 1829),
    ('_prearm_a2_high_pressure_first_point_mode', 5737, 6077),
    ('_end_a2_co2_route_conditioning_at_atmosphere', 5276, 5491),
    ('_a2_mark_preseal_capture_pressure', 6245, 6458),
    ('_maybe_reassert_a2_conditioning_vent', 5064, 5259),
]

# Read files
with open(ORCH, encoding='utf-8') as f:
    orch_lines = f.readlines()
with open(CS, encoding='utf-8') as f:
    cs_text = f.read()

orch_before = len(orch_lines)
cs_before = len(cs_text.split('\n'))
print(f"Before: orchestrator={orch_before} conditioning={cs_before}")

# Step 1: Extract and transform each method
transformed_methods = {}
for name, start_0, end_0 in METHODS:
    method_lines = orch_lines[start_0:end_0+1]
    method_text = ''.join(method_lines)
    count = len(re.findall(r'\bself\.(?!host\.)', method_text))
    transformed = re.sub(r'\bself\.(?!host\.)', 'self.host.', method_text)
    transformed_methods[(name, start_0, end_0)] = (transformed, count)

# Step 2: Append transformed methods to conditioning_service (in reverse order so they appear in order)
cs_new = cs_text.rstrip('\n')
for name, start_0, end_0 in sorted(METHODS, key=lambda x: x[1]):  # order by line number
    transformed, count = transformed_methods[(name, start_0, end_0)]
    cs_new += '\n' + transformed
cs_new += '\n'

# Step 3: Replace methods in orchestrator with delegations
# Sort by start line descending so deletions don't shift indices
sorted_methods = sorted(METHODS, key=lambda x: -x[1])
new_orch_lines = list(orch_lines)
for name, start_0, end_0 in sorted_methods:
    delegation = f'    def {name}(self, *args, **kwargs):\n        return self.conditioning_service.{name}(*args, **kwargs)\n\n'
    new_orch_lines[start_0:end_0+1] = [delegation]

# Step 4: Write back
with open(ORCH, 'w', encoding='utf-8') as f:
    f.writelines(new_orch_lines)
with open(CS, 'w', encoding='utf-8') as f:
    f.write(cs_new)

orch_after = len(new_orch_lines)
cs_after = len(cs_new.split('\n'))
print(f"\nAfter: orchestrator={orch_after} conditioning={cs_after}")
print(f"Orchestrator: {orch_before} -> {orch_after} (removed {orch_before - orch_after})")
print(f"Conditioning: {cs_before} -> {cs_after} (added {cs_after - cs_before})")

# Syntax check
try:
    ast.parse(''.join(new_orch_lines))
    print("orchestrator syntax: OK")
except SyntaxError as e:
    print(f"orchestrator syntax ERROR: {e}")

try:
    ast.parse(cs_new)
    print("conditioning_service syntax: OK")
except SyntaxError as e:
    print(f"conditioning_service syntax ERROR: {e}")
