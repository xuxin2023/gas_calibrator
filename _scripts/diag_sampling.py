"""Diagnostic v2: find sample and point events in run.log"""
import json, sys
from datetime import datetime

run_log = r"D:\output\run001_a2\co2_only_7_pressure_no_write\run_20260503_152955\run.log"
with open(run_log, encoding="utf-8") as f:
    lines = f.readlines()

# Search for sampling-related keywords
print("=== Searching for: sampled / sample / point / stability ===")
for i, l in enumerate(lines):
    r = json.loads(l)
    msg = r.get("message", "")
    ts = r.get("timestamp", "")
    if any(kw in msg.lower() for kw in ["sampl", "point 1", "point 2", "point 3", "stability_time", "stability", "co2_mean"]):
        print(f"  [{i+1}] {ts[11:19]}  {msg[:200]}")

print()
print("=== Full timeline: in-limits + next 5 lines ===")
for i, l in enumerate(lines):
    r = json.loads(l)
    msg = r.get("message", "")
    if "Pressure in-limits at target" in msg:
        ts = r.get("timestamp", "")
        print(f"  [{i+1}] {ts[11:19]}  >>> {msg}")
        # print next 5 non-empty lines
        count = 0
        for k in range(i + 1, min(i + 10, len(lines))):
            r2 = json.loads(lines[k])
            m2 = r2.get("message", "")
            if m2:
                print(f"  [{k+1}] {r2.get('timestamp','')[:19].split('T')[-1] if 'T' in r2.get('timestamp','') else ''}  {m2[:200]}")
                count += 1
        print("  ----")
