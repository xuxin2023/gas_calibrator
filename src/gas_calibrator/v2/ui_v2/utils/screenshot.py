from __future__ import annotations

from pathlib import Path
from typing import Any


def export_widget_screenshot(widget: Any, path: str | Path) -> Path:
    """Export a best-effort screenshot artifact for a Tk widget.

    Uses PIL.ImageGrab when available; otherwise writes a lightweight fallback
    text artifact describing the widget geometry.
    """

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    widget.update_idletasks()

    try:
        from PIL import ImageGrab  # type: ignore

        x0 = int(widget.winfo_rootx())
        y0 = int(widget.winfo_rooty())
        x1 = x0 + int(widget.winfo_width())
        y1 = y0 + int(widget.winfo_height())
        image = ImageGrab.grab(bbox=(x0, y0, x1, y1))
        image.save(target)
        return target
    except Exception:
        fallback = target if target.suffix.lower() == ".txt" else target.with_suffix(".txt")
        fallback.write_text(
            "\n".join(
                [
                    "screenshot_unavailable",
                    f"widget={widget.winfo_class()}",
                    f"width={widget.winfo_width()}",
                    f"height={widget.winfo_height()}",
                ]
            ),
            encoding="utf-8",
        )
        return fallback
