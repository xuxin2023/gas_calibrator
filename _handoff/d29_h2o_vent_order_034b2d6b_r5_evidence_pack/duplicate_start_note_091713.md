# Duplicate-Start Fail-Closed Note - run_20260512_091713

## Run metadata
- run_id: run_20260512_091713
- start: 2026-05-12 09:17:13 CST
- end: 2026-05-12 09:17:22 CST
- duration: ~9 seconds

## Why this run failed
This run was started while **run_20260512_085322** was still running (started at 08:53).

Both runs use the same COM ports:
- COM16 (humidity_generator)
- COM17 (dewpoint_meter)
- COM20 (relay/modbus)
- COM22 (pressure_gauge)
- COM23 (pressure_controller)
- COM35/37/41/42 (gas_analyzers)

Since 085322 already had all COM ports open, 091713 could not open any device.

## Evidence
```
Calibration failed: Critical device initialization failed
  serial not open
  Modbus Error: [Connection] Failed to connect[ModbusSerialClient COM20:0]
  Pressure controller output disable failed: serial not open
  Pressure controller vent command failed: serial not open
  Startup pressure precheck read failed (pressure_meter): serial not open
  Startup pressure precheck read failed (pressure_controller): serial not open
  
failed_devices=[
  dewpoint_meter, humidity_generator, pressure_controller,
  pressure_meter, relay_a, relay_b, temperature_chamber,
  gas_analyzer_0, gas_analyzer_1, gas_analyzer_2, gas_analyzer_3
]
```

## System response
- `real_probe_executed: false`
- `real_com_opened: false`
- `any_device_command_sent: false`
- `any_write_command_sent: false`
- `final_decision: FAIL_CLOSED`

System correctly failed-closed with no device commands and no writes.

## Classification
- This is a **duplicate-start COM port contention failure**
- NOT a physical-process failure
- NOT a vent/valve regression
- NOT a humidity generator issue
- NOT a reason to modify runtime
- Primary D29-R5 evidence is run_20260512_085322 (PASS)
