import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PACKAGE_DIR = ROOT / "docs" / "v1_5_flow_contract" / "current_round_feedback_20260709"
DOC = PACKAGE_DIR / "V1_5_CURRENT_ROUND_FEEDBACK_PACKAGE_20260709.md"
CHECKLIST = PACKAGE_DIR / "v1_5_current_round_feedback_checklist_20260709.csv"


def test_current_round_feedback_package_locks_mature_baseline_language():
    text = DOC.read_text(encoding="utf-8")

    assert "0613-style V1.5 fitting method" in text
    assert "0620/0621 mature physical execution path" in text
    assert "0624 is diagnostic/migration evidence" in text
    assert "Root-directory migrated scripts" in text
    assert "not a production point" in text
    assert "Do not implement live route changes" in text


def test_current_round_feedback_checklist_covers_required_regression_themes():
    rows = list(csv.DictReader(CHECKLIST.open(encoding="utf-8", newline="")))

    assert len(rows) >= 20
    required = {row["check_id"]: row for row in rows if row["status"] == "required"}

    for check_id in [
        "CFG-001",
        "ROUTE-001",
        "ROUTE-003",
        "PRESS-001",
        "PRESS-002",
        "FIT-CO2-003",
        "FIT-H2O-004",
        "EVID-001",
    ]:
        assert check_id in required

    checklist_text = CHECKLIST.read_text(encoding="utf-8")
    assert "0624 is not mature reference" in checklist_text
    assert "Only public physical gates fail the point" in checklist_text
    assert "Use PACE INL absolute pressure" in checklist_text
    assert "Use CLEARSENCO5,YGAS,FFF" in checklist_text
    assert "Keep CO2 zero gas and H2O dry-gas/low-water anchor physically separate" in checklist_text
    assert "no_write_candidate is not real_pass" in checklist_text
