"""Migration helper: move _record_a2_co2_conditioning_pressure_monitor to conditioning_service"""
import re

src = r'd:\gas_calibrator\src\gas_calibrator\v2\core\orchestrator.py'
dst = r'd:\gas_calibrator\src\gas_calibrator\v2\core\services\conditioning_service.py'

lines = open(src, encoding='utf-8').readlines()
method = ''.join(lines[4779:5331])

count = len(re.findall(r'self\.(?!host\.)', method))
transformed = re.sub(r'self\.(?!host\.)', 'self.host.', method)

out_lines = []
for line in transformed.split('\n'):
    if line.startswith('    '):
        out_lines.append(line[4:])
    else:
        out_lines.append(line)
transformed_body = '\n'.join(out_lines)

service_text = open(dst, encoding='utf-8').read()
total_before = len(service_text.split('\n'))

insertion = '\n' + transformed_body + '\n'
new_content = service_text.rstrip('\n') + insertion

with open(dst, 'w', encoding='utf-8') as f:
    f.write(new_content)

total_after = len(new_content.split('\n'))
print(f"conditioning_service.py: {total_before} -> {total_after} lines")
print(f"Method length: {method.count(chr(10)) + 1} lines")
print(f"Self->Host transforms: {count}")
