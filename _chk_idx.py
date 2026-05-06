import sqlite3
conn = sqlite3.connect("gas_calibrator_index.db")
targets = [
    "ix_stability_windows_run_sn_window",
    "ix_state_transition_logs_run_time",
    "ix_fit_results_run_analyzer",
]
all_idx = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()}
for t in targets:
    print(f"  {t:50s} {'OK' if t in all_idx else 'MISSING'}")
conn.close()
