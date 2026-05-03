"""Check parallel sampling timing"""
import csv, glob, os
from collections import defaultdict
from datetime import datetime

base = r'D:\output\run001_a2\co2_only_7_pressure_no_write'
dirs = sorted(glob.glob(os.path.join(base, 'run_2026*')), key=os.path.getmtime, reverse=True)
for latest in dirs[:1]:
    print(f"Run: {os.path.basename(latest)}")
    for csv_name in ('samples.csv', 'samples_runtime.csv'):
        csv_path = os.path.join(latest, csv_name)
        if not os.path.exists(csv_path):
            continue
        print(f"File: {csv_name}")
        with open(csv_path, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = [r for r in reader if r.get('point_index') == '1']
        d = defaultdict(list)
        for r in rows:
            d[int(r['sample_index'])].append((r['timestamp'], r['analyzer_id']))
        for si in sorted(d.keys())[:3]:
            items = sorted(d[si], key=lambda x: x[0])
            t0 = datetime.fromisoformat(items[0][0])
            tn = datetime.fromisoformat(items[-1][0])
            span = (tn - t0).total_seconds()
            for ts, a_name in items:
                t = datetime.fromisoformat(ts)
                ms = t.strftime('%H:%M:%S.%f')[:-3]
                print(f'  r{si:2d} {a_name} @ {ms}')
            marker = ' *** PARALLEL! ***' if span < 1.0 else ' (serial)'
            print(f'  span={span:.2f}s{marker}')
        ks = sorted(d.keys())
        print()
        for i in range(min(3, len(ks)-1)):
            te = max(datetime.fromisoformat(t) for t,a in d[ks[i]])
            ts = min(datetime.fromisoformat(t) for t,a in d[ks[i+1]])
            print(f'  r{ks[i]:2d}->r{ks[i+1]:2d}: {(ts-te).total_seconds():.2f}s')
