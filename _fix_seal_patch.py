"""Patch: fix _preclose_a2_high_pressure_first_point_vent to always seal."""
filepath = r'src\gas_calibrator\v2\core\orchestrator.py'

with open(filepath, 'rb') as f:
    raw = f.read()

old_lines = [
    b'    def _preclose_a2_high_pressure_first_point_vent(self, point: CalibrationPoint) -> dict[str, Any]:\r\n',
    b'        # A2.25: Fixed 0.5s delay seal after vent close, with hard abort safety gate\r\n',
    b'        time.sleep(0.5)\r\n',
    b"        reader = getattr(self.pressure_control_service, '_current_high_pressure_first_point_sample', None)\r\n",
    b"        sample = dict(reader(stage='seal_preparation_vent_off_settle', point_index=point.index)) if callable(reader) else {}\r\n",
    b"        latest_pressure_hpa = float(sample.get('pressure_hpa')) if sample.get('pressure_hpa') is not None else None\r\n",
    b'        if latest_pressure_hpa is not None and latest_pressure_hpa >= 1300.0:\r\n',
    b'            raise WorkflowValidationError(\r\n',
    b'                f"Hard abort: pressure {latest_pressure_hpa:.2f} hPa exceeds safety limit before seal"\r\n',
    b'            )\r\n',
]

old = b''.join(old_lines)

new_lines = [
    b'    def _preclose_a2_high_pressure_first_point_vent(self, point: CalibrationPoint) -> dict[str, Any]:\r\n',
    b'        # A2.25: Fixed 0.5s delay seal after vent close, with hard abort detection (still seal!)\r\n',
    b'        import time\r\n',
    b'        time.sleep(0.5)\r\n',
    b'        # Read current pressure for diagnostics and hard abort flag\r\n',
    b'        try:\r\n',
    b'            latest_pressure_hpa = self._get_latest_pressure_hpa()\r\n',
    b'        except Exception:\r\n',
    b'            latest_pressure_hpa = None\r\n',
    b'        if latest_pressure_hpa is not None and latest_pressure_hpa >= 1300.0:\r\n',
    b'            self._hard_abort_triggered = True\r\n',
    b'            self._seal_trigger_reason = "fixed_delay_seal_a2_25_hard_abort"\r\n',
    b'        else:\r\n',
    b'            self._hard_abort_triggered = False\r\n',
    b'            self._seal_trigger_reason = "fixed_delay_seal_a2_25"\r\n',
    b'        self._seal_allowed = True   # Always seal! Even on hard abort.\r\n',
]

new = b''.join(new_lines)

if old in raw:
    raw = raw.replace(old, new)
    with open(filepath, 'wb') as f:
        f.write(raw)
    print('SUCCESS: Method replaced successfully.')
elif old_lines[0] in raw:
    idx = raw.find(old_lines[0])
    snippet = raw[idx:idx+600]
    print('Found method start but old block not exact. Snippet (repr):')
    print(repr(snippet))
    raise SystemExit(1)
else:
    print('ERROR: Method not found!')
    raise SystemExit(1)