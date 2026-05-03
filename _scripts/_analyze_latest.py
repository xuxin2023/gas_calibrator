"""Analyze the latest smoke test run for parallel sampling effect"""
import os, glob, csv, json
from collections import defaultdict
from datetime import datetime

base = r'D:\output\run001_a2\co2_only_7_pressure_no_write'
dirs = sorted(glob.glob(os.path.join(base, 'run_2026*')), key=os.path.getmtime, reverse=True)
latest = dirs[0]
prev = dirs[1] if len(dirs) > 1 else None
print(f"Latest run: {os.path.basename(latest)} ({datetime.fromtimestamp(os.path.getmtime(latest))})")
if prev:
    print(f"Previous:    {os.path.basename(prev)}")

# Analyze run.log
log_path = os.path.join(latest, 'run.log')
totals = {}
for line in open(log_path, encoding='utf-8'):
    try:
        obj = json.loads(line)
        msg = obj.get('message', '')
        if 'Point' in msg and 'sampled' in msg:
            ts = obj['timestamp'][11:19]
            parts = msg.split()
            for p in parts:
                if '=' in p:
                    k, v = p.split('=', 1)
                    if k in ('stability_time_s', 'total_time_s', 'co2_mean'):
                        totals[k] = float(v)
            print(f"\nPoint1: ts={ts} stability={totals.get('stability_time_s','?')}s total={totals.get('total_time_s','?')}s")
    except:
        pass

# Analyze sampling CSV
for csv_name in ('samples.csv', 'samples_runtime.csv'):
    csv_path = os.path.join(latest, csv_name)
    if not os.path.exists(csv_path):
        continue
    with open(csv_path, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    r0 = rows[0] if rows else {}
    has_aid = 'analyzer_id' in r0
    ts_key = 'sample_ts' if 'sample_ts' in r0 else ('timestamp' if 'timestamp' in r0 else None)
    print(f"\n{csv_name}: {len(rows)} rows, has_analyzer_id={has_aid}")

    if has_aid and ts_key:
        d = defaultdict(list)
        for r in rows:
            if str(r.get('point_index', '')) == '1':
                d[int(r['sample_index'])].append((r[ts_key], r['analyzer_id']))
        spans = []
        for si in sorted(d.keys()):
            items = sorted(d[si], key=lambda x: x[0])
            t0 = datetime.fromisoformat(items[0][0])
            tn = datetime.fromisoformat(items[-1][0])
            span = (tn - t0).total_seconds()
            spans.append(span)
            if si <= 3:
                for ts_str, aname in items:
                    t = datetime.fromisoformat(ts_str)
                    ms = t.strftime('%H:%M:%S.%f')[:-3]
                    print(f'  r{si:2d} {aname} @ {ms}')
                marker = '*** PARALLEL! ***' if span < 1.0 else '(serial)'
                print(f'  => span={span:.2f}s {marker}')
        avg = sum(spans) / len(spans) if spans else 0
        print(f'  avg_span={avg:.2f}s over {len(spans)} rounds')

# Comparison with previous run
if prev:
    prev_csv = os.path.join(prev, 'samples_runtime.csv')
    if not os.path.exists(prev_csv):
        prev_csv = os.path.join(prev, 'samples.csv')
    if os.path.exists(prev_csv):
        with open(prev_csv, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            prows = list(reader)
        prows = [r for r in prows if str(r.get('point_index','')) == '1']
        if prows:
            pr0 = prows[0] if prows else {}
            phas_aid = 'analyzer_id' in pr0
            pts_key = 'sample_ts' if 'sample_ts' in pr0 else ('timestamp' if 'timestamp' in pr0 else None)
            if phas_aid and pts_key:
                pd = defaultdict(list)
                for r in prows:
                    pd[int(r['sample_index'])].append((r[pts_key], r['analyzer_id']))
                pspans = []
                for si in sorted(pd.keys()):
                    items = sorted(pd[si], key=lambda x: x[0])
                    pspans.append((datetime.fromisoformat(items[-1][0]) - datetime.fromisoformat(items[0][0])).total_seconds())
                pavg = sum(pspans) / len(pspans) if pspans else 0
                print(f"\n--- Comparison ---")
                print(f"Previous avg span: {pavg:.2f}s")
                print(f"Latest avg span:   {avg:.2f}s")
                print(f"Change: {(avg-pavg)/pavg*100:+.1f}%")

# Check run.log for previous total_time_s
if prev:
    prev_log = os.path.join(prev, 'run.log')
    if os.path.exists(prev_log):
        for line in open(prev_log, encoding='utf-8'):
            try:
                obj = json.loads(line)
                msg = obj.get('message','')
                if 'Point 1 sampled' in msg:
                    for p in msg.split():
                        if p.startswith('total_time'):
                            print(f"Previous total_time_s: {p.split('=')[1]}")
                    break
            except:
                pass
