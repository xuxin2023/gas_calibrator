from __future__ import annotations

from typing import Optional

from .app_facade import AppFacade


class RunController:
    """Thin command surface for the V2 cockpit."""

    def __init__(self, facade: AppFacade) -> None:
        self.facade = facade

    def start(
        self,
        points_path: Optional[str] = None,
        *,
        points_source: str = "use_points_file",
        run_mode: Optional[str] = None,
    ) -> tuple[bool, str]:
        return self.facade.start(points_path=points_path, points_source=points_source, run_mode=run_mode)

    def preview_points(
        self,
        points_path: Optional[str] = None,
        *,
        points_source: str = "use_points_file",
        run_mode: Optional[str] = None,
    ) -> dict:
        return self.facade.preview_points(points_path=points_path, points_source=points_source, run_mode=run_mode)

    def edit_points_file(
        self,
        points_path: Optional[str] = None,
        *,
        points_source: str = "use_points_file",
    ) -> tuple[bool, str]:
        return self.facade.edit_points_file(points_path=points_path, points_source=points_source)

    def stop(self) -> tuple[bool, str]:
        return self.facade.stop()

    def pause(self) -> tuple[bool, str]:
        return self.facade.pause()

    def resume(self) -> tuple[bool, str]:
        return self.facade.resume()
