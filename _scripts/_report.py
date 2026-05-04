"""Summarize parallel sampling results"""
import csv, glob, os, json
from collections import defaultdict
from datetime import datetime

base = r'D:\output\run001_a2\co2_only_7_pressure_no_write'
dirs = sorted(glob.glob(os.path.join(base, 'run_2026*')), key=os.path.getmtime, reverse=True)

print("=" * 60)
print("TASK 3: Parallel Sampling Smoke Test Report")
print("=" * 60)

# Get latest run
latest = dirs[0]
prev = dirs[1] if len(dirs) > 1 else None
print(f"\nLatest run: {os.path.basename(latest)}")
if prev:
    print(f"Previous run: {os.path.basename(prev)}")

# Read run.log
log_path = os.path.join(latest, 'run.log')
log_lines = open(log_path, encoding='utf-8').readlines()

# Extract seal -> pressure timings
print("\n--- Point 1 Timeline ---")
seal_time = None
output_on_time = None
pressure_in_limits_time = None
sample_end_time = None

for line in log_lines:
    try:
        obj = json.loads(line)
    except:
        continue
    msg = obj.get('message', '')
    ts = obj.get('timestamp', '')
    if not seal_time and 'sealed for pressure control' in msg:
        seal_time = ts
        print(f"seal:               {ts[11:19]}  pressure={msg.split('sealed pressure=')[1].split(')')[0] if 'sealed pressure=' in msg else '?'}")
    if 'Pressure in-limits at target 1100' in msg:
        pressure_in_limits_time = ts
        print(f"pressure in-limits: {ts[11:19]}")
    if 'Pressure controller output=ON' in msg and not output_on_time:
        output_on_time = ts
        print(f"output=ON:          {ts[11:19]}")
    if 'Point 1 sampled' in msg:
        sample_end_time = ts
        for part in msg.split():
            if part.startswith('stability_time'):
                print(f"stability_time_s:   {part.split('=')[1]}")
            if part.startswith('total_time'):
                print(f"total_time_s:       {part.split('=')[1]}")
        print(f"sample_end:         {ts[11:19]}")

if seal_time and pressure_in_limits_time:
    d_seal = datetime.fromisoformat(seal_time)
    d_pil = datetime.fromisoformat(pressure_in_limits_time)
    seal_to_pil = (d_pil - d_seal).total_seconds()
    print(f"\nseal -> pressure in-limits: {seal_to_pil:.1f}s")

if seal_time and sample_end_time:
    d_seal = datetime.fromisoformat(seal_time)
    d_end = datetime.fromisoformat(sample_end_time)
    print(f"seal -> sample_end:         {(d_end - d_seal).total_seconds():.1f}s")

# Read samples_runtime.csv
for csv_name in ('samples.csv', 'samples_runtime.csv'):
    csv_path = os.path.join(latest, csv_name)
    if not os.path.exists(csv_path):
        continue
    with open(csv_path, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        all_rows = list(reader)
        point1_rows = [r for r in all_rows if r.get('point_index') == '1']
    print(f"\n--- {csv_name} ---")
    print(f"Point 1 entries: {len(point1_rows)}")
    # Check if each entry has all 4 analyzers or one per entry
    ts_key = 'sample_ts' if 'sample_ts' in (point1_rows[0] if point1_rows else {}) else 'timestamp'
    analyzer_key = 'analyzer_id' if 'analyzer_id' in (point1_rows[0] if point1_rows else {}) else None
    
    if analyzer_key:
        print(f"Format: one row per analyzer (key: {analyzer_key})")
        # Reconstruct per-round timings
        d = defaultdict(list)
        for r in point1_rows:
            d[int(r['sample_index'])].append((r[ts_key], r[analyzer_key]))
        for si in sorted(d.keys())[:3]:
            items = sorted(d[si], key=lambda x: x[0])
            spans = []
            for ts, a_name in items:
                t = datetime.fromisoformat(ts)
                ms = t.strftime('%H:%M:%S.%f')[:-3]
                spans.append((t, a_name, ms))
            t0 = spans[0][0]
            tn = spans[-1][0]
            span = (tn - t0).total_seconds()
            for _, a_name, ms in spans:
                print(f'  r{si:2d} {a_name} @ {ms}')
            print(f'  => span={span:.2f}s {"PARALLEL!" if span < 1.0 else "(serial)"}')
        # Average span
        all_spans = []
        for si in sorted(d.keys()):
            items = sorted(d[si], key=lambda x: x[0])
            span = (datetime.fromisoformat(items[-1][0]) - datetime.fromisoformat(items[0][0])).total_seconds()
            all_spans.append(span)
        print(f'  avg_span={sum(all_spans)/len(all_spans):.2f}s')
    else:
        print(f"Format: all 4 analyzers in single row ({len(point1_rows)} round rows)")
        for r in point1_rows[:3]:
            t = datetime.fromisoformat(r[ts_key])
            print(f'  sample_{r["sample_index"]}: {t.strftime("%H:%M:%S.%f")[:-3]} '
                  f'co2_ppm={r.get("co2_ppm","?")}')

# Comparison with previous run
if prev:
    prev_log = os.path.join(prev, 'run.log')
    if os.path.exists(prev_log):
        for line in open(prev_log, encoding='utf-8'):
            try:
                obj = json.loads(line)
            except:
                continue
            msg = obj.get('message', '')
            if 'Point 1 sampled' in msg:
                for part in msg.split():
                    if part.startswith('total_time'):
                        print(f'\n--- Comparison ---')
                        print(f'Previous total_time_s: {part.split("=")[1]}')
                        break
