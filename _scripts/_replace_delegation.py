"""Replace orchestrator method with delegation to conditioning_service"""
import os

src = r'd:\gas_calibrator\src\gas_calibrator\v2\core\orchestrator.py'
with open(src, encoding='utf-8') as f:
    lines = f.readlines()

delegation = '''    def _record_a2_co2_conditioning_pressure_monitor(
        self,
        point: CalibrationPoint,
        *,
        phase: str,
    ) -> dict[str, Any]:
        return self.conditioning_service._record_a2_co2_conditioning_pressure_monitor(point, phase=phase)

'''

# Keep lines 0-4780, delegation, lines 5333-end
new_lines = lines[:4780] + [delegation] + lines[5332:]

with open(src, 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

before = len(lines)
after = len(new_lines)
print(f"orchestrator.py: {before} -> {after} lines (removed {before - after})")
