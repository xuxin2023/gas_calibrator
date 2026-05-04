"""Replace _record_a2_co2_conditioning_vent_tick with delegation in orchestrator.py"""
import re

path = r'src/gas_calibrator/v2/core/orchestrator.py'
with open(path, encoding='utf-8') as f:
    lines = f.readlines()

start = next(i for i, l in enumerate(lines) if 'def _record_a2_co2_conditioning_vent_tick' in l)
indent = len(lines[start]) - len(lines[start].lstrip())
end = next(i for i in range(start + 1, len(lines)) if lines[i].strip() and not lines[i].startswith(' ' * (indent + 1)))

body = ''.join(lines[start:end])

replacement = (
    '    def _record_a2_co2_conditioning_vent_tick(self, point: CalibrationPoint, *, phase: str) -> dict[str, Any]:\n'
    '        return self.conditioning_service._record_a2_co2_conditioning_vent_tick(point, phase=phase)\n'
)

new_content = ''.join(lines[:start]) + replacement + ''.join(lines[end:])

with open(path, 'w', encoding='utf-8', newline='\n') as f:
    f.write(new_content)

print(f'Removed {end - start} lines, added {replacement.count(chr(10))} lines')
print(f'New orchestrator size: {new_content.count(chr(10))} lines')
print('Done')
