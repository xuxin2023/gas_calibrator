import json, sys
path = r"D:\output\run001_a2\co2_only_7_pressure_no_write\run_20260504_103938\route_trace.jsonl"
events = []
with open(path) as f:
    for line in f:
        q = json.loads(line)
        if q.get("point_index") == 1:
            ts = q["ts"][-15:]
            act = q["action"]
            msg = q["message"][:90]
            events.append((ts, act, msg))
            print(f"{ts} | {act:<35s} | {msg}")
print(f"\nTotal point-1 events: {len(events)}")
