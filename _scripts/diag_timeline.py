"""Diagnostic: extract in-limits → sample_start → sample_end timeline from run.log"""
import json, sys
from datetime import datetime

run_log = r"D:\output\run001_a2\co2_only_7_pressure_no_write\run_20260503_152955\run.log"
with open(run_log, encoding="utf-8") as f:
    lines = f.readlines()

events = []
for i, l in enumerate(lines):
    r = json.loads(l)
    msg = r.get("message", "")
    ts = r.get("timestamp", "")
    if "Pressure in-limits at target" in msg:
        events.append((i + 1, ts, "in-limits", msg))
    elif "CO2 sampling start" in msg or "sample_start" in msg.lower():
        events.append((i + 1, ts, "sample_start", msg))
    elif "CO2 sampling complete" in msg or "sample_end" in msg.lower():
        events.append((i + 1, ts, "sample_end", msg))

print("=== TIMELINE ===")
for ev in events:
    ln, ts, typ, msg = ev
    print(f"  L{ln:5d}  {ts}  [{typ}]  {msg[:150]}")

print()
print("=== in-limits → sample_start intervals ===")
j = 0
while j < len(events):
    if events[j][2] == "in-limits":
        t_stable = events[j][1]
        for k in range(j + 1, len(events)):
            if events[k][2] == "sample_start":
                dt1 = datetime.fromisoformat(t_stable)
                dt2 = datetime.fromisoformat(events[k][1])
                delta = (dt2 - dt1).total_seconds()
                print(f"  {t_stable[11:19]} → {events[k][1][11:19]}  =  {delta:.1f}s")
                j = k
                break
        else:
            j += 1
    else:
        j += 1

# Also find setpoint-related events
print()
print("=== SLEW/setpoint related ===")
for i, l in enumerate(lines):
    r = json.loads(l)
    msg = r.get("message", "")
    if "output=ON" in msg or "setpoint update" in msg:
        print(f"  [{i+1}] {msg[:200]}")
