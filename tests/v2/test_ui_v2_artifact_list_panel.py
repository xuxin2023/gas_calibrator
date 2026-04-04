from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.widgets.artifact_list_panel import ArtifactListPanel

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def test_artifact_list_panel_renders_rows() -> None:
    root = make_root()
    try:
        panel = ArtifactListPanel(root)
        panel.render(
            [
                {
                    "name": "summary.json",
                    "present_on_disk": True,
                    "listed_in_current_run": True,
                    "artifact_origin": "current_run",
                    "artifact_role_display": "执行摘要",
                    "export_status_display": "ok",
                    "exportability_display": "当前运行可导出",
                    "path": "D:/tmp/summary.json",
                }
            ]
        )
        assert len(panel.tree.get_children()) == 1
        values = panel.tree.item(panel.tree.get_children()[0], "values")
        assert values[2] == "当前运行"
        assert "执行摘要" in values[3]
        assert "ok" in values[3]
    finally:
        root.destroy()
