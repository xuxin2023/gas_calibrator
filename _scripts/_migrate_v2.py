"""Properly migrate method with correct indentation preservation"""
import re

src = r'd:\gas_calibrator\src\gas_calibrator\v2\core\orchestrator.py'
dst = r'd:\gas_calibrator\src\gas_calibrator\v2\core\services\conditioning_service.py'

lines = open(src, encoding='utf-8').readlines()
# Verify line count
print(f"Orchestrator total lines: {len(lines)}")

# Find the method boundaries
start_line = None
end_line = None
for i, line in enumerate(lines):
    if 'def _record_a2_co2_conditioning_pressure_monitor(' in line and start_line is None:
        start_line = i
    elif start_line is not None and not end_line:
        # Find next def that's NOT inside the method body (same indentation level)
        stripped = line.lstrip()
        if stripped.startswith('def ') and line.startswith('    ') and i > start_line + 1:
            end_line = i
            break

if start_line is None or end_line is None:
    print(f"Could not find method boundaries! start={start_line}, end={end_line}")
else:
    print(f"Method: lines {start_line+1} to {end_line} (0-indexed: {start_line} to {end_line-1})")
    method = ''.join(lines[start_line:end_line])
    
    # Transform self.X -> self.host.X where X != 'host'
    count = len(re.findall(r'\bself\.(?!host\.)', method))
    transformed = re.sub(r'\bself\.(?!host\.)', 'self.host.', method)
    
    service_lines = open(dst, encoding='utf-8').readlines()
    total_before = len(service_lines)
    
    insertion = '\n' + transformed + '\n'
    new_content = ''.join(service_lines).rstrip('\n') + insertion
    
    with open(dst, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    total_after = len(new_content.split('\n'))
    print(f"conditioning_service.py: {total_before} -> {total_after} lines")
    print(f"Method length: {method.count(chr(10)) + 1} lines")
    print(f"Self->Host transforms: {count}")
    
    # Verify
    check = open(dst, encoding='utf-8').readlines()
    for i in [total_before, total_before+1, total_before+2, total_after-5, total_after-4, total_after-3, total_after-2, total_after-1]:
        if i < len(check):
            print(f"  L{i+1}: {repr(check[i].rstrip()[:80])}")
    
    # Syntax check
    import ast
    try:
        ast.parse(open(dst, encoding='utf-8').read())
        print("Syntax OK")
    except SyntaxError as e:
        print(f"Syntax ERROR: {e}")
