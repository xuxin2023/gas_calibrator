from __future__ import annotations

import tkinter as tk
from tkinter import ttk


class ScrollablePageFrame(ttk.Frame):
    """Reusable page scaffold that adds mouse-wheel aware vertical scrolling."""

    _INNER_SCROLL_CLASSES = {"Text", "Treeview", "Listbox", "Canvas"}

    def __init__(
        self,
        parent: tk.Misc,
        *,
        padding: int | tuple[int, ...] = 12,
        style: str = "Card.TFrame",
        canvas_background: str = "#eef3f8",
    ) -> None:
        super().__init__(parent, style=style)
        self._scrollbar_visible = False
        self._canvas_background = canvas_background

        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        self.canvas = tk.Canvas(
            self,
            highlightthickness=0,
            borderwidth=0,
            background=self._canvas_background,
        )
        self.canvas.grid(row=0, column=0, sticky="nsew")
        self.v_scrollbar = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=self._on_canvas_yview)

        self.content = ttk.Frame(self.canvas, style=style, padding=padding)
        self.content.columnconfigure(0, weight=1)
        self._window_id = self.canvas.create_window((0, 0), window=self.content, anchor="nw")

        self.canvas.bind("<Configure>", self._on_canvas_configure, add="+")
        self.content.bind("<Configure>", self._on_content_configure, add="+")
        self.bind_all("<MouseWheel>", self._on_mousewheel, add="+")
        self.bind_all("<Button-4>", self._on_mousewheel_linux, add="+")
        self.bind_all("<Button-5>", self._on_mousewheel_linux, add="+")

    def has_overflow(self) -> bool:
        return int(self.content.winfo_reqheight() or 0) > int(self.canvas.winfo_height() or 0) + 1

    def is_scrollbar_visible(self) -> bool:
        return self._scrollbar_visible and bool(self.v_scrollbar.winfo_ismapped())

    def scroll_to_top(self) -> None:
        self.canvas.yview_moveto(0.0)

    def _on_canvas_yview(self, first: str, last: str) -> None:
        self.v_scrollbar.set(first, last)
        self._update_scrollbar_visibility()

    def _on_canvas_configure(self, event: tk.Event[tk.Canvas]) -> None:
        self.canvas.itemconfigure(self._window_id, width=max(1, int(event.width)))
        self._update_scroll_region()

    def _on_content_configure(self, _event: tk.Event[ttk.Frame]) -> None:
        self._update_scroll_region()

    def _update_scroll_region(self) -> None:
        self.update_idletasks()
        bbox = self.canvas.bbox(self._window_id)
        if bbox is not None:
            self.canvas.configure(scrollregion=bbox)
        self._update_scrollbar_visibility()

    def _update_scrollbar_visibility(self) -> None:
        should_show = self.has_overflow()
        if should_show == self._scrollbar_visible:
            return
        self._scrollbar_visible = should_show
        if should_show:
            self.v_scrollbar.grid(row=0, column=1, sticky="ns")
        else:
            self.v_scrollbar.grid_remove()

    def _on_mousewheel(self, event: tk.Event[tk.Misc]) -> None:
        if not self._can_scroll_event(event):
            return
        delta = int(getattr(event, "delta", 0) or 0)
        if delta == 0:
            return
        step = -1 if delta > 0 else 1
        self.canvas.yview_scroll(step, "units")

    def _on_mousewheel_linux(self, event: tk.Event[tk.Misc]) -> None:
        if not self._can_scroll_event(event):
            return
        num = int(getattr(event, "num", 0) or 0)
        if num == 4:
            self.canvas.yview_scroll(-1, "units")
        elif num == 5:
            self.canvas.yview_scroll(1, "units")

    def _can_scroll_event(self, event: tk.Event[tk.Misc]) -> bool:
        if not self.has_overflow():
            return False
        widget = getattr(event, "widget", None)
        if widget is None:
            return False
        if not self._is_descendant(widget):
            return False
        if self._belongs_to_inner_scroller(widget):
            return False
        return True

    def _is_descendant(self, widget: tk.Misc) -> bool:
        current: tk.Misc | None = widget
        while current is not None:
            if current in {self, self.canvas, self.content}:
                return True
            current = self._parent_widget(current)
        return False

    def _belongs_to_inner_scroller(self, widget: tk.Misc) -> bool:
        current: tk.Misc | None = widget
        while current is not None and current not in {self, self.canvas, self.content}:
            if str(current.winfo_class() or "") in self._INNER_SCROLL_CLASSES:
                return True
            current = self._parent_widget(current)
        return False

    @staticmethod
    def _parent_widget(widget: tk.Misc) -> tk.Misc | None:
        parent_name = str(widget.winfo_parent() or "")
        if not parent_name:
            return None
        try:
            return widget.nametowidget(parent_name)
        except Exception:
            return None
