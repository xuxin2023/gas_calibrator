import csv, os, glob, json
from datetime import datetime
from collections import defaultdict

base = r'D:\output\run001_a2\co2_only_7_pressure_no_write'
dirs = sorted(glob.glob(os.path.join(base, 'run_2026*')), key=os.path.getmtime, reverse=True)
latest = dirs[0]
prev = dirs[1] if len(dirs) > 1 else None
print(f"Latest: {os.path.basename(latest)}")
if prev:
    print(f"Prev:    {os.path.basename(prev)}")

# 1. Round gaps from samples.csv
csv_path = os.path.join(latest, 'samples.csv')
with open(csv_path, encoding='utf-8') as f:
    reader = csv.DictReader(f)
    all_rows = list(reader)
rows_pt1 = [r for r in all_rows if r.get('point_index') == '1']
d = defaultdict(list)
for r in rows_pt1:
    d[int(r['sample_index'])].append(datetime.fromisoformat(r['timestamp']))

ks = sorted(d.keys())
print(f"\n--- Point 1 Sampling ---")
print(f"{len(ks)} rounds, {len(rows_pt1)} rows")
gaps = []
for i in range(len(ks)-1):
    gaps.append((min(d[ks[i+1]]) - max(d[ks[i]])).total_seconds())

print(f"Round gaps: avg={sum(gaps)/len(gaps):.2f}s min={min(gaps):.2f}s max={max(gaps):.2f}s")

for k in ks[:3]:
    times = sorted(d[k])
    t0 = times[0]
    tn = times[-1]
    span_ms = (tn - t0).total_seconds() * 1000
    ms0 = t0.strftime('%H:%M:%S.%f')[:-3]
    msn = tn.strftime('%H:%M:%S.%f')[:-3]
    print(f"  r{k}: {ms0} .. {msn}  span={span_ms:.1f}ms")

# 2. Point data from run.log
log_path = os.path.join(latest, 'run.log')
print(f"\n--- Point summary ---")
print(f"  {'#':>2} {'stab_s':>7} {'total_s':>8} {'p_mean':>8} {'co2_mean':>9}")
for line in open(log_path, encoding='utf-8'):
    obj = json.loads(line)
    msg = obj.get('message', '')
    if 'Point' in msg and 'sampled' in msg and 'co2_mean' in msg:
        parts = msg.split()
        kv = {}
        for p in parts:
            if '=' in p:
                k, v = p.split('=', 1)
                try:
                    kv[k] = float(v)
                except:
                    pass
        for p in parts:
            if 'Point' in p and 'sampled' in p:
                idx = p.split()[1]
                break
        else:
            idx = '?'
        print(f"  {idx:>2} {kv.get('stability_time_s', 0):7.2f} {kv.get('total_time_s', 0):8.2f} {kv.get('pressure_mean', 0):8.2f} {kv.get('co2_mean', 0):9.2f}")

# 3. Comparison
if prev:
    prev_log = os.path.join(prev, 'run.log')
    if os.path.exists(prev_log):
        for line in open(prev_log, encoding='utf-8'):
            obj = json.loads(line)
            msg = obj.get('message', '')
            if 'Point 1 sampled' in msg:
                for p in msg.split():
                    if p.startswith('total_time'):
                        prev_tot = p.split('=')[1]
                        print(f"\n--- Comparison ---")
                        print(f"Previous total_time_s: {prev_tot}")
                        if rows_pt1:
                            this_tot = kv.get('total_time_s', 0)
                            print(f"Latest total_time_s:   {this_tot}")
                            try:
                                change_pct = (float(this_tot) - float(prev_tot)) / float(prev_tot) * 100
                                print(f"Change: {change_pct:+.1f}%")
                            except:
                                pass
                break

# 4. Decision
summary_path = os.path.join(latest, 'summary.json')
with open(summary_path, encoding='utf-8') as f:
    summary = json.load(f)
print(f"\n--- Acceptance ---")
print(f"final_decision: {summary.get('final_decision')}")
print(f"attempted_write: {summary.get('attempted_write_count')}")
print(f"artifact_completeness_pass: {summary.get('artifact_completeness_pass', True)}")
