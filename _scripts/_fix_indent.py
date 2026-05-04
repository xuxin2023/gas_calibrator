"""Fix indent: re-indent migrated method into ConditioningService class"""
import re

dst = r'd:\gas_calibrator\src\gas_calibrator\v2\core\services\conditioning_service.py'
lines = open(dst, encoding='utf-8').readlines()

# Find the boundary: after the last class method (ends at index 1336), the migrated method starts
# The method starts at line 1338 (0-indexed 1337)

print(f"Line 1339: {repr(lines[1338].rstrip())}")

# Re-indent everything from the migrated method onward
new_lines = lines[:1337]  # keep everything before the migrated method
for i, line in enumerate(lines[1337:], start=1337):
    # Detect class boundary - if we hit a line that starts with non-whitespace
    # and is not a decorator, blank, comment, or the new method signature
    stripped = line.rstrip()
    if not stripped:
        new_lines.append(line)
        continue

    # The new method and everything after it needs 4-space indent
    leading = line[:len(line) - len(line.lstrip())]
    content = line.lstrip()
    new_lines.append('    ' + content)

with open(dst, 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

# Verify
check = open(dst, encoding='utf-8').readlines()
print(f"Line 1339 (after fix): {repr(check[1338].rstrip())}")
print(f"Total lines: {len(check)}")
