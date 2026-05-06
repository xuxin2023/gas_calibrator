import sqlite3, os

db = os.path.join(os.path.dirname(__file__), "gas_calibrator_index.db")
conn = sqlite3.connect(db)

indexes = [
    ("ix_stability_windows_run_sn_window",
     "CREATE INDEX IF NOT EXISTS ix_stability_windows_run_sn_window ON stability_windows (run_id, analyzer_sn, window_start_time)"),
    ("ix_state_transition_logs_run_time",
     "CREATE INDEX IF NOT EXISTS ix_state_transition_logs_run_time ON state_transition_logs (run_id, timestamp)"),
    ("ix_fit_results_run_analyzer",
     "CREATE INDEX IF NOT EXISTS ix_fit_results_run_analyzer ON fit_results (run_id, analyzer_id)"),
]

all_idx = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()}

for name, sql in indexes:
    if name in all_idx:
        print(f"SKIP (exists): {name}")
    else:
        try:
            conn.execute(sql)
            print(f"CREATED: {name}")
        except sqlite3.OperationalError as e:
            print(f"ERROR: {name} -> {e}")

conn.commit()

all_idx2 = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()}
for name, _ in indexes:
    status = "OK" if name in all_idx2 else "MISSING"
    print(f"  verify {name:50s} {status}")

conn.close()
