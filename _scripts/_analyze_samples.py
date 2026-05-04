"""Analyze sampling timestamps for parallelism diagnosis"""
import os, glob
from datetime import datetime

base = r'D:\output\run001_a2\co2_only_7_pressure_no_write'
dirs = sorted(glob.glob(os.path.join(base, 'run_2026*')), key=os.path.getmtime, reverse=True)
latest = dirs[0]

csv_path = os.path.join(latest, 'samples.csv')
lines = open(csv_path, encoding='utf-8').readlines()
hdr = lines[0].strip().split(',')
si_idx = hdr.index('sample_index')
ai_idx = hdr.index('analyzer_id')

pts = [l.strip().split(',') for l in lines[1:] if l.split(',')[1]=='1']
from collections import defaultdict
d = defaultdict(list)
for r in pts:
    d[int(r[si_idx])].append((r[0], r[ai_idx]))

def parse_ts(ts_str):
    return datetime.fromisoformat(ts_str)

print('Per-round timing:')
for si in sorted(d.keys()):
    items = sorted(d[si], key=lambda x: x[0])
    t0 = parse_ts(items[0][0])
    tn = parse_ts(items[-1][0])
    span = (tn - t0).total_seconds()
    a = [x[1] for x in items]
    print(f'  r{si:2d}: {len(items)}台 span={span:.2f}s analyzers={a}')
    for ts_str, aname in items:
        t = parse_ts(ts_str)
        print(f'        {aname} @ {t.strftime("%H:%M:%S.%f")[:-3]}')

print()
print('Inter-round gaps:')
keys = sorted(d.keys())
for i in range(len(keys)-1):
    t_end = parse_ts(max(t for t,a in d[keys[i]]))
    t_start = parse_ts(min(t for t,a in d[keys[i+1]]))
    gap = (t_start - t_end).total_seconds()
    print(f'  r{keys[i]} end -> r{keys[i+1]} start: gap={gap:.2f}s')
