"""A2.25: Replace _preclose_a2_high_pressure_first_point_vent body with fixed 0.5s+1300 hard abort+seal"""
import os

filepath = r"D:\gas_calibrator\src\gas_calibrator\v2\core\orchestrator.py"
with open(filepath, "r", encoding="utf-8") as f:
    content = f.read()

start_marker = "def _preclose_a2_high_pressure_first_point_vent"
start_idx = content.find(start_marker)
if start_idx == -1:
    raise SystemExit("ERROR: method not found")

lines = content[start_idx:].splitlines(True)
method_header = lines[0]
body_start_line = 1

# Skip docstring if present
if body_start_line < len(lines):
    stripped = lines[body_start_line].strip()
    if stripped.startswith('"""') or stripped.startswith("'''"):
        if stripped.count('"""') == 1 or stripped.count("'''") == 1:
            for i in range(body_start_line + 1, len(lines)):
                if '"""' in lines[i] or "'''" in lines[i]:
                    body_start_line = i + 1
                    break
        else:
            body_start_line += 1

# Find end of method: next definition at same or lower indent level
indent_level = len(method_header) - len(method_header.lstrip())
end_line = len(lines)
for i in range(body_start_line, len(lines)):
    stripped = lines[i].rstrip()
    if stripped and not lines[i][0] in (' ', '\t'):
        end_line = i
        break
    if stripped.startswith('def ') or stripped.startswith('class '):
        line_indent = len(lines[i]) - len(lines[i].lstrip())
        if line_indent <= indent_level:
            end_line = i
            break

# Build base indent
base_indent = ""
if body_start_line < end_line:
    body_line = lines[body_start_line]
    if body_line.strip():
        base_indent = body_line[:len(body_line) - len(body_line.lstrip())]
if not base_indent:
    base_indent = " " * (indent_level + 4)

# New method body
new_body_lines = [
    f"{base_indent}# A2.25: Fixed 0.5s delay seal after vent close, with hard abort safety gate",
    f"{base_indent}time.sleep(0.5)",
    f"{base_indent}reader = getattr(self.pressure_control_service, '_current_high_pressure_first_point_sample', None)",
    f"{base_indent}sample = dict(reader(stage='seal_preparation_vent_off_settle', point_index=point.index)) if callable(reader) else {{}}",
    f"{base_indent}latest_pressure_hpa = float(sample.get('pressure_hpa')) if sample.get('pressure_hpa') is not None else None",
    f"{base_indent}if latest_pressure_hpa is not None and latest_pressure_hpa >= 1300.0:",
    f"{base_indent}    raise WorkflowValidationError(",
    f"{base_indent}        f\"Hard abort: pressure {{latest_pressure_hpa:.2f}} hPa exceeds safety limit before seal\"",
    f"{base_indent}    )",
]

# Preserve everything before method
before_method = content[:start_idx]
# Method header + original body start lines (empty beyond header)
header_block = "".join(lines[:body_start_line])
# Everything after method body
after_method = "".join(lines[end_line:])

new_method = header_block + "\n".join(new_body_lines) + "\n"

new_content = before_method + new_method + after_method

with open(filepath, "w", encoding="utf-8") as f:
    f.write(new_content)

print("A2.25 patch applied successfully.")