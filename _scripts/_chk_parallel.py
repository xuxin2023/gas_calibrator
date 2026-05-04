"""Check if parallel sampling is working"""
import os, glob
from collections import defaultdict
from datetime import datetime

base = r'D:\output\run001_a2\co2_only_7_pressure_no_write'
dirs = sorted(glob.glob(os.path.join(base, 'run_2026*')), key=os.path.getmtime, reverse=True)
latest = dirs[0]
print(f"Latest: {os.path.basename(latest)}")

csv_path = os.path.join(latest, 'samples.csv')
lines = open(csv_path, encoding='utf-8').readlines()
hdr = lines[0].strip().split(',')
si_i = hdr.index('sample_index')
ai_i = hdr.index('analyzer_id')

pts = [l.strip().split(',') for l in lines[1:] if l.split(',')[1]=='1']
d = defaultdict(list)
for r in pts:
    d[int(r[si_i])].append((r[0], r[ai_i]))

print('First 3 rounds timing:')
for si in sorted(d.keys())[:3]:
    items = sorted(d[si], key=lambda x: x[0])
    t0 = datetime.fromisoformat(items[0][0])
    tn = datetime.fromisoformat(items[-1][0])
    span = (tn - t0).total_seconds()
    for ts, a in items:
        t = datetime.fromisoformat(ts)
        ms = t.strftime('%H:%M:%S.%f')[:-3]
        print(f'  r{si:2d} {a} @ {ms}')
    print(f'  => span={span:.2f}s')
    if span < 1.0:
        print(f'  *** PARALLEL: all 4 analyzers read within {span:.2f}s! ***')

print()
print('Round gaps:')
ks = sorted(d.keys())
for i in range(min(3, len(ks)-1)):
    te = max(datetime.fromisoformat(t) for t,a in d[ks[i]])
    ts = min(datetime.fromisoformat(t) for t,a in d[ks[i+1]])
    print(f'  r{ks[i]:2d}->r{ks[i+1]:2d}: {(ts-te).total_seconds():.2f}s')

# Compare with previous run
prev_dirs = sorted(glob.glob(os.path.join(base, 'run_2026*')), key=os.path.getmtime, reverse=True)
if len(prev_dirs) > 1:
    prev = prev_dirs[1]
    prev_csv = os.path.join(prev, 'samples.csv')
    if os.path.exists(prev_csv):
        plines = open(prev_csv, encoding='utf-8').readlines()
        ppts = [l.strip().split(',') for l in plines[1:] if l.split(',')[1]=='1']
        pd = defaultdict(list)
        for r in ppts:
            pd[int(r[si_i])].append((r[0], r[ai_i]))
        prev_spans = []
        new_spans = []
        for si in pd:
            items = sorted(pd[si], key=lambda x: x[0])
            span = (datetime.fromisoformat(items[-1][0]) - datetime.fromisoformat(items[0][0])).total_seconds()
            prev_spans.append(span)
        for si in d:
            items = sorted(d[si], key=lambda x: x[0])
            span = (datetime.fromisoformat(items[-1][0]) - datetime.fromisoformat(items[0][0])).total_seconds()
            new_spans.append(span)
        print(f'\n--- Comparison with {os.path.basename(prev)} ---')
        print(f'Previous avg span: {sum(prev_spans)/len(prev_spans):.2f}s')
        print(f'New avg span:      {sum(new_spans)/len(new_spans):.2f}s')
        change = (sum(new_spans)/len(new_spans) - sum(prev_spans)/len(prev_spans)) / (sum(prev_spans)/len(prev_spans)) * 100
        print(f'Change: {change:+.1f}%')
