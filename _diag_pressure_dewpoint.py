"""Post-hoc 压力/露点微观测序诊断 (从route_trace.jsonl提取)"""
import json, sys, os
from datetime import datetime

def _parse_ts(ts: str):
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

def main(run_dir: str):
    trace_path = os.path.join(run_dir, "route_trace.jsonl")
    log_path = os.path.join(run_dir, "run.log")
    if not os.path.exists(trace_path):
        print(f"ERROR: {trace_path} not found")
        return

    with open(trace_path, encoding="utf-8") as f:
        lines = f.readlines()

    events = []
    for line in lines:
        try:
            obj = json.loads(line)
        except Exception:
            continue
        action = obj.get("action", "")
        ts = obj.get("ts", "")
        msg = obj.get("message", "")
        result = obj.get("result", "")
        target = obj.get("target", {})
        actual = obj.get("actual", {})

        # Collect key events
        if action in ("set_vent", "seal_route", "set_pressure", "pressure_control_ready_gate",
                       "sample_start", "sample_end", "wait_post_pressure"):
            vent_on = target.get("vent_on")
            pressure_hpa = actual.get("pressure_hpa")
            preseal_hpa = actual.get("preseal_pressure_peak_hpa") or actual.get("pre_seal_pressure_hpa")
            sealed_hpa = actual.get("sealed_pressure_hpa") or actual.get("sealed_pressure")
            point_tag = obj.get("point_tag", "")
            events.append({
                "ts": ts, "action": action, "vent_on": vent_on,
                "pressure_hpa": pressure_hpa, "preseal_peak": preseal_hpa,
                "sealed_hpa": sealed_hpa, "msg": msg, "result": result,
                "point_tag": point_tag,
            })

    # Find vent=OFF -> seal -> pressure timeline for first cycle
    vent_off_ts = None
    seal_ts = None
    pressure_ok_ts = None

    for e in events:
        if e["action"] == "set_vent" and e["vent_on"] is False and vent_off_ts is None:
            vent_off_ts = _parse_ts(e["ts"])
        if e["action"] == "seal_route" and seal_ts is None:
            seal_ts = _parse_ts(e["ts"])
        if e["action"] == "set_pressure" and e["result"] == "ok" and pressure_ok_ts is None:
            pressure_ok_ts = _parse_ts(e["ts"])

    print("=" * 60)
    print("封路前后微观测序分析")
    print("=" * 60)

    if vent_off_ts and seal_ts:
        delta = (seal_ts - vent_off_ts).total_seconds()
        print(f"\nvent=OFF 时间: {vent_off_ts}")
        print(f"seal_route 时间: {seal_ts}")
        print(f"关大气到封路间隔: {delta:.1f}s")

    if seal_ts and pressure_ok_ts:
        delta = (pressure_ok_ts - seal_ts).total_seconds()
        print(f"\nseal_route 时间: {seal_ts}")
        print(f"控压达标 时间: {pressure_ok_ts}")
        print(f"封路到控压达标间隔: {delta:.1f}s")

    if vent_off_ts and pressure_ok_ts:
        delta = (pressure_ok_ts - vent_off_ts).total_seconds()
        print(f"\n关大气到控压达标总耗: {delta:.1f}s")

    # Show all events in the vent-off to pressure-ok window
    print(f"\n--- 封路窗口内所有事件 ---")
    for e in events:
        ts = _parse_ts(e["ts"])
        if vent_off_ts and pressure_ok_ts:
            if vent_off_ts <= ts <= pressure_ok_ts:
                vent_str = f"vent_on={e['vent_on']}" if e['vent_on'] is not None else ""
                p_str = f"p={e['pressure_hpa']:.1f}" if e['pressure_hpa'] is not None else ""
                print(f"  [{e['ts'][11:19]}] {e['action']:30s} {vent_str:16s} {p_str} {e['msg'][:60]}")

    # Check for dewpoint readings in the run.log
    if os.path.exists(log_path):
        with open(log_path, encoding="utf-8") as f:
            log_lines = f.readlines()
        dewpoint_msgs = [l for l in log_lines if "dewpoint" in l.lower() and "skipped" not in l.lower()]
        print(f"\n--- run.log 中 dewpoint 相关行 ---")
        print(f"共 {len(dewpoint_msgs)} 条")
        for l in dewpoint_msgs[:5]:
            print(f"  {l.rstrip()[:150]}")

    # Check summary for dewpoint values
    summary_path = os.path.join(run_dir, "summary.json")
    if os.path.exists(summary_path):
        with open(summary_path, encoding="utf-8") as f:
            summary = json.load(f)
        decision = summary.get("final_decision", "N/A")
        sample_count = summary.get("sample_count", 0)
        write_count = summary.get("attempted_write_count", "N/A")
        print(f"\n--- summary 关键字段 ---")
        print(f"  final_decision: {decision}")
        print(f"  sample_count: {sample_count}")
        print(f"  attempted_write_count: {write_count}")

    # Check points.csv for dewpoint values
    points_csv = os.path.join(run_dir, "points_readable.csv")
    if os.path.exists(points_csv):
        with open(points_csv, encoding="utf-8") as f:
            header = f.readline().strip()
            cols = header.split(",")
            dew_cols = [c for c in cols if "dew" in c.lower()]
            print(f"\n--- points_readable.csv 露点相关列 ---")
            print(f"  列: {dew_cols}")
            for line in f:
                vals = line.strip().split(",")
                dew_vals = {c: vals[cols.index(c)] if c in cols else "N/A" for c in dew_cols}
                print(f"  {dew_vals}")

if __name__ == "__main__":
    import glob
    base = r"D:\output\run001_a2\co2_only_7_pressure_no_write"
    dirs = sorted(glob.glob(base + r"\run_2026*"), key=os.path.getmtime, reverse=True)
    if len(sys.argv) > 1:
        main(sys.argv[1])
    elif dirs:
        main(dirs[0])
    else:
        print("No run directories found")
