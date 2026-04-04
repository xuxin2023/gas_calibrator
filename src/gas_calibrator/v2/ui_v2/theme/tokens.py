from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class UITheme:
    bg: str = "#eef3f8"
    panel: str = "#ffffff"
    panel_soft: str = "#f7fafc"
    line: str = "#d5dee8"
    text: str = "#0f2338"
    muted: str = "#627587"
    accent: str = "#0e7c86"
    accent_soft: str = "#dff5f4"
    warn: str = "#f59e0b"
    danger: str = "#dc2626"
    success: str = "#15803d"
    overlay: str = "#153047"
    overlay_text: str = "#ffffff"
    notification_bg: str = "#f1f6fb"
    error_bg: str = "#fee2e2"
    font_family: str = "Segoe UI"
    font_size_sm: int = 9
    font_size_md: int = 10
    font_size_lg: int = 11
    font_size_xl: int = 14
    spacing_xs: int = 4
    spacing_sm: int = 8
    spacing_md: int = 12
    spacing_lg: int = 16


THEME = UITheme()
