from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.widgets.export_bar import ExportBar

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import make_root


def test_export_bar_invokes_callbacks() -> None:
    root = make_root()
    calls: list[str] = []
    try:
        widget = ExportBar(
            root,
            on_export_json=lambda: calls.append("json") or (True, "json exported"),
            on_export_csv=lambda: calls.append("csv") or (True, "csv exported"),
            on_export_all=lambda: calls.append("all") or (True, "all exported"),
            on_export_review_manifest=lambda: calls.append("manifest")
            or {
                "ok": True,
                "message": "review_scope_20260328_142210_all.json",
            },
        )
        widget.render({"available_formats": ["json", "csv", "all"], "last_export_message": "Ready"})
        widget.export_json()
        widget.export_csv()
        widget.export_all()
        widget.export_review_manifest()
        assert calls == ["json", "csv", "all", "manifest"]
        assert "review_scope_20260328_142210_all.json" in widget.status_var.get()
    finally:
        root.destroy()
