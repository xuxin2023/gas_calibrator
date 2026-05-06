import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "gas_calibrator_index.db")
conn = sqlite3.connect(DB_PATH)

existing = set(row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall())
print("existing tables:", sorted(existing))

# Create stability_windows if not exists
if "stability_windows" not in existing:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stability_windows (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            analyzer_sn TEXT NOT NULL,
            state_name TEXT DEFAULT '',
            window_start_time TEXT DEFAULT '',
            window_end_time TEXT DEFAULT '',
            signal_list TEXT DEFAULT '',
            span_value REAL,
            slope_value REAL,
            std_value REAL,
            valid_ratio REAL,
            jump_count INTEGER DEFAULT 0,
            hard_threshold_passed INTEGER DEFAULT 0,
            composite_score REAL,
            passed INTEGER DEFAULT 0,
            fail_reason TEXT DEFAULT '',
            created_at TEXT NOT NULL
        )
    """)
    print("created stability_windows table")

# Create state_transition_logs if not exists
if "state_transition_logs" not in existing:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS state_transition_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            analyzer_sn TEXT NOT NULL,
            from_state TEXT DEFAULT '',
            to_state TEXT DEFAULT '',
            trigger TEXT DEFAULT '',
            decision_context TEXT DEFAULT '',
            created_at TEXT NOT NULL
        )
    """)
    print("created state_transition_logs table")

# Apply the migration SQL (indexes only, tables already handled above)
sql_path = os.path.join(os.path.dirname(__file__), "src", "gas_calibrator", "v2", "storage", "migrations", "003_high_frequency_query_indexes.sql")
with open(sql_path, encoding="utf-8") as f:
    sql = f.read()

# Skip the CREATE TABLE parts since we handled them, only run CREATE INDEX
for stmt in sql.split(";"):
    stmt = stmt.strip()
    if stmt and stmt.upper().startswith("CREATE INDEX"):
        try:
            conn.execute(stmt)
            print(f"executed: {stmt[:80]}...")
        except sqlite3.OperationalError as e:
            print(f"skipped (already exists?): {e}")

conn.commit()

# Verify
indexes = conn.execute("SELECT name FROM sqlite_master WHERE type='index' ORDER BY name").fetchall()
print("\nall indexes:")
for idx in indexes:
    print(f"  {idx[0]}")

conn.close()
print("\nDONE")
