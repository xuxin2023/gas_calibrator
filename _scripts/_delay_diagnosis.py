"""Diagnose delays between pressure stable and sampling"""
import os, glob, json
from datetime import datetime, timezone

base = r'D:\output\run001_a2\co2_only_7_pressure_no_write'
dirs = sorted(glob.glob(os.path.join(base, 'run_2026*')), key=os.path.getmtime, reverse=True)
latest = dirs[0]
print(f"Analyzing: {os.path.basename(latest)}")

# 1. run.log: pressure_in_limits and sample_start
log_path = os.path.join(latest, 'run.log')
events = []
for line in open(log_path, encoding='utf-8'):
    obj = json.loads(line)
    msg = obj.get('message', '')
    ts = obj.get('timestamp', '')
    if 'Pressure in-limits at target 1100' in msg:
        events.append(('pressure_in_limits', datetime.fromisoformat(ts), msg[:80]))
    if 'Point 1 sampled' in msg:
        events.append(('point1_sampled', datetime.fromisoformat(ts), msg[:120]))

print("\n--- run.log events ---")
for et, dt, msg in events:
    print(f"  {et}: {dt.strftime('%H:%M:%S.%f')[:-3]}  {msg}")

# 2. route_trace: full point-1 flow
rt_path = os.path.join(latest, 'route_trace.jsonl')
rt_events = []
for line in open(rt_path, encoding='utf-8'):
    obj = json.loads(line)
    if obj.get('point_index') != 1:
        continue
    action = obj.get('action', '')
    ts = datetime.fromisoformat(obj['ts'])
    msg = obj.get('message', '')
    result = obj.get('result', '')
    if action in ('set_pressure', 'wait_post_pressure', 'sample_start', 'sample_end',
                  'pressure_control_ready_gate', 'seal_route'):
        rt_events.append((ts, action, result, msg[:80]))

print("\n--- route_trace point-1 flow ---")
for ts, action, result, msg in rt_events:
    print(f"  {ts.strftime('%H:%M:%S.%f')[:-3]}  {action:30s} {result:5s}  {msg}")

# Calculate deltas
print("\n--- Delta analysis ---")
if len(events) >= 2:
    pil = events[0][1]  # pressure_in_limits
    p1s = events[1][1]   # point1_sampled
    delta = (p1s - pil).total_seconds()
    print(f"  pressure_in_limits -> Point1 sampled: {delta:.1f}s")

if rt_events:
    # Find set_pressure OK and sample_start
    sp = None; ss = None; se = None
    for ts, action, result, _ in rt_events:
        if action == 'set_pressure' and result == 'ok' and sp is None:
            sp = ts
        if action == 'sample_start':
            ss = ts
        if action == 'sample_end':
            se = ts
    if sp and ss:
        print(f"  pressure stable -> sample_start: {(ss-sp).total_seconds():.3f}s")
    if ss and se:
        print(f"  sample_start -> sample_end (sampling loop): {(se-ss).total_seconds():.1f}s")
    if sp and se:
        print(f"  pressure stable -> sample_end (point total): {(se-sp).total_seconds():.1f}s")
