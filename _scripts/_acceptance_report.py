"""Acceptance test report for 7-point real probe"""
import os, glob, json, csv
from collections import defaultdict
from datetime import datetime

base = r'D:\output\run001_a2\co2_only_7_pressure_no_write'
dirs = sorted(glob.glob(os.path.join(base, 'run_2026*')), key=os.path.getmtime, reverse=True)
latest = dirs[0]
print("=" * 70)
print(f"验收报告 - {os.path.basename(latest)}")
print(f"HEAD: 73a8e0a6  branch: codex/run001-a1-no-write-dry-run")
print("=" * 70)

# === 1. summary.json ===
summary_path = os.path.join(latest, 'summary.json')
if os.path.exists(summary_path):
    with open(summary_path, encoding='utf-8') as f:
        summary = json.load(f)

    print("\n--- 1. summary.json 关键字段 ---")
    keys = [
        'final_decision', 'readiness_final_decision', 'a1_final_decision', 'a2_final_decision',
        'sample_count', 'sample_count_total', 'attempted_write_count',
        'planned_pressure_points_completed', 'pressure_points_completed',
        'a2_fail_reason', 'readiness_fail_reason', 'fail_closed_reason',
        'seal_command_sent', 'pressure_setpoint_command_sent',
    ]
    for k in keys:
        val = summary.get(k, 'N/A')
        print(f"  {k}: {val}")

    # Check seal/pressure commands
    for k in ('seal_command_sent', 'pressure_setpoint_command_sent', 'vent_off_command_sent'):
        val = summary.get(k, 'N/A')
        if val != 'N/A':
            print(f"  {k}: {val}")

    # rejection_reasons
    rr = summary.get('rejection_reasons', summary.get('a2_fail_reason', ''))
    if rr:
        print(f"  rejection_reasons: {rr}")

# === 2. run.log - extract per-point stats ===
log_path = os.path.join(latest, 'run.log')
points_data = []
seal_ts = None
vent_off_ts = None
dewpoint_msgs = []
errors = []

for line in open(log_path, encoding='utf-8'):
    try:
        obj = json.loads(line)
    except:
        continue
    msg = obj.get('message', '')
    ts = obj.get('timestamp', '')

    if 'sealed for pressure control' in msg:
        seal_ts = ts
        # extract pressure
        if 'sealed pressure=' in msg:
            print(f"\n  封路: {ts[11:19]} sealed_pressure={msg.split('sealed pressure=')[1].split(')')[0] if 'sealed pressure=' in msg else '?'}")

    if 'vent=OFF' in msg and 'positive CO2 preseal' in msg and not vent_off_ts:
        vent_off_ts = ts

    if 'Point' in msg and 'sampled' in msg and 'co2_mean' in msg:
        parts = msg.split()
        pdict = {}
        for p in parts:
            if '=' in p:
                k, v = p.split('=', 1)
                try:
                    pdict[k] = float(v)
                except:
                    pdict[k] = v
        # extract point index
        pidx = None
        for p in parts:
            if 'Point' in p and 'sampled' in p:
                import re
                m = re.search(r'Point\s+(\d+)', p)
                if m:
                    pidx = int(m.group(1))
                    break
        points_data.append({
            'point': pidx,
            'ts': ts[11:19],
            'stability_s': pdict.get('stability_time_s', 0),
            'total_s': pdict.get('total_time_s', 0),
            'co2_mean': pdict.get('co2_mean', 0),
            'pressure_mean': pdict.get('pressure_mean', 0),
        })

    # Dewpoint messages
    if 'dewpoint' in msg.lower():
        dewpoint_msgs.append(msg[:100])

    if any(w in msg.lower() for w in ('error', 'exception', 'fail', 'warn')) and 'safe stop' not in msg.lower():
        errors.append(f"{ts[11:19]}: {msg[:120]}")

if vent_off_ts:
    print(f"  关大气(vent=OFF): {vent_off_ts[11:19]}")

print("\n--- 2. 各压力点统计 ---")
print(f"  {'#':>3} {'时间':>8} {'stability_s':>11} {'total_s':>9} {'co2_mean':>10} {'p_mean':>8}")
for p in points_data:
    print(f"  {p['point']:>3} {p['ts']:>8} {p['stability_s']:11.2f} {p['total_s']:9.2f} {p['co2_mean']:10.2f} {p['pressure_mean']:8.2f}")

if points_data:
    avg_stab = sum(p['stability_s'] for p in points_data) / len(points_data)
    avg_total = sum(p['total_s'] for p in points_data) / len(points_data)
    print(f"\n  avg stability: {avg_stab:.2f}s  avg total: {avg_total:.2f}s")

# dewpoint check
print(f"\n  dewpoint 相关日志: {len(dewpoint_msgs)} 条")
for m in dewpoint_msgs[:3]:
    print(f"    {m}")

if errors:
    print(f"\n  错误/警告: {len(errors)} 条")
    for e in errors[:5]:
        print(f"    {e}")

# === 3. samples_*.csv - verify parallel sampling ===
for csv_name in ('samples.csv', 'samples_runtime.csv'):
    csv_path = os.path.join(latest, csv_name)
    if not os.path.exists(csv_path):
        continue
    with open(csv_path, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    has_aid = 'analyzer_id' in (rows[0] if rows else {})
    ts_key = 'sample_ts' if 'sample_ts' in (rows[0] if rows else {}) else ('timestamp' if 'timestamp' in (rows[0] if rows else {}) else None)

    print(f"\n--- 3. {csv_name} ({len(rows)} rows) ---")

    if has_aid and ts_key:
        d = defaultdict(list)
        for r in rows:
            if str(r.get('point_index', '')) == '1':
                d[int(r['sample_index'])].append((r[ts_key], r['analyzer_id']))

        # First round only
        if 1 in d:
            items = sorted(d[1], key=lambda x: x[0])
            t0 = datetime.fromisoformat(items[0][0])
            tn = datetime.fromisoformat(items[-1][0])
            span = (tn - t0).total_seconds()
            print(f"  Point 1, Round 1: {len(items)}台分析仪")
            for ts_str, aname in items:
                t = datetime.fromisoformat(ts_str)
                print(f"    {aname} @ {t.strftime('%H:%M:%S.%f')[:-3]}")
            print(f"  span={span:.4f}s  {'*** 并行生效 ***' if span < 0.1 else '(疑似串行)'}")

        # Average span across all rounds
        all_spans = []
        for si in d:
            items = sorted(d[si], key=lambda x: x[0])
            sp = (datetime.fromisoformat(items[-1][0]) - datetime.fromisoformat(items[0][0])).total_seconds()
            all_spans.append(sp)
        avg = sum(all_spans)/len(all_spans)
        print(f"  全部{len(all_spans)}轮平均跨度: {avg:.4f}s, min={min(all_spans):.4f}s, max={max(all_spans):.4f}s")

# === 4. Acceptance verdict ===
print("\n" + "=" * 70)
print("验收判定")
print("=" * 70)

verdicts = []

# final_decision
fd = summary.get('final_decision', 'N/A')
verdicts.append(('final_decision', fd, 'PASS' if fd == 'PASS' else fd))

# pressure_points_completed
pp = summary.get('planned_pressure_points_completed', summary.get('pressure_points_completed', []))
pp_count = len(pp) if isinstance(pp, list) else int(pp) if isinstance(pp, (int,float)) else 0
verdicts.append(('points_completed', pp_count, '7' if pp_count >= 7 else str(pp_count)))

# sample_count
sc = summary.get('sample_count', summary.get('sample_count_total', 0))
verdicts.append(('sample_count', sc, f'{sc} (>=28)' if int(sc) >= 28 else str(sc)))

# attempted_write
awc = summary.get('attempted_write_count', 0)
verdicts.append(('attempted_write', awc, '0' if int(awc) == 0 else str(awc)))

# parallel span
par_span = 'N/A'
if 'all_spans' in dir():
    par_span = f'{avg:.4f}s'
verdicts.append(('parallel_avg_span', avg if 'avg' in dir() else 'N/A', '<0.1s' if ('avg' in dir() and avg < 0.1) else str(par_span)))

print(f"  {'项目':<20} {'实际值':<15} {'期望值':<15} {'结果':>6}")
print(f"  {'-'*20} {'-'*15} {'-'*15} {'-'*6}")
all_pass = True
for name, actual, expected in verdicts:
    # Determine pass/fail
    if name == 'final_decision':
        ok = str(actual) == 'PASS'
    elif name == 'points_completed':
        ok = int(actual) >= 7
    elif name == 'sample_count':
        ok = int(actual) >= 28
    elif name == 'attempted_write':
        ok = int(actual) == 0
    elif name == 'parallel_avg_span':
        ok = isinstance(actual, float) and actual < 0.1
    else:
        ok = str(actual) == str(expected)
    
    status = '✅' if ok else '❌'
    if not ok:
        all_pass = False
    print(f"  {name:<20} {str(actual):<15} {str(expected):<15} {status:>6}")

print()
if all_pass:
    print("*** 验收通过 ***")
    print("\n建议下一步:")
    print("  1. 将分支 codex/run001-a1-no-write-dry-run 合并到 main/master")
    print("  2. 在执行正式标定前，考虑轮间传感器读取优化 (当前 ~15.7s/轮)")
    print("  3. 归档本次验收报告作为 release evidence")
else:
    print("*** 验收失败 ***")
    if fd != 'PASS':
        print(f"  原因: final_decision={fd}")
        fr = summary.get('a2_fail_reason', summary.get('readiness_fail_reason', ''))
        if fr:
            print(f"  失败详情: {fr}")
