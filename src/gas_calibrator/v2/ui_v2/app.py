from __future__ import annotations

import argparse
from pathlib import Path
import sys
import tkinter as tk
from typing import Optional

from ..config import AppConfig
from .controllers.app_facade import AppFacade
from .controllers.live_state_feed import LiveStateFeed
from .controllers.run_controller import RunController
from .runtime.build_info_loader import load_build_info
from .runtime.crash_recovery import CrashRecovery
from .runtime.recovery_store import RecoveryStore
from .runtime.release_notes_loader import load_release_notes
from .shell import AppShell
from .i18n import t
from .widgets.startup_splash import StartupSplash
from .utils.preferences_store import PreferencesStore
from .utils.recent_runs_store import RecentRunsStore
from .utils.route_memory import RouteMemory
from .utils.runtime_paths import RuntimePaths


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=t("app.description"))
    parser.add_argument("--config", type=str, default=None, help=t("app.arg.config"))
    parser.add_argument("--simulation", action="store_true", help=t("app.arg.simulation"))
    parser.add_argument(
        "--allow-unsafe-step2-config",
        action="store_true",
        help="Allow loading a non-default Step 2 config only when the matching environment unlock is also set.",
    )
    return parser


def load_config(config_path: Optional[str], *, simulation: bool) -> AppConfig:
    config = AppConfig.from_json_file(config_path) if config_path else AppConfig.from_dict({})
    config.features.use_v2 = True
    if simulation:
        config.features.simulation_mode = True
    return config


def build_application(
    *,
    config_path: Optional[str] = None,
    simulation: bool = False,
    allow_unsafe_step2_config: bool = False,
    root: Optional[tk.Tk] = None,
    facade: Optional[AppFacade] = None,
    start_feed: bool = False,
) -> tuple[tk.Tk, AppShell, AppFacade]:
    tk_root = root or tk.Tk()
    runtime_paths = (getattr(facade, "runtime_paths", None) or RuntimePaths.default()).ensure_dirs()
    preferences_store = PreferencesStore(runtime_paths.preferences_path)
    recent_runs_store = RecentRunsStore(runtime_paths.recent_runs_path)
    route_memory = RouteMemory(runtime_paths.route_memory_path)
    recovery_store = RecoveryStore.from_runtime_paths(runtime_paths)
    crash_recovery = CrashRecovery(recovery_store)
    ui_root = Path(__file__).resolve().parent
    build_info = load_build_info(ui_root / "build_info.json")
    release_notes_text = load_release_notes(ui_root / "release_notes.md")
    licenses_path = ui_root / "licenses.txt"
    licenses_text = licenses_path.read_text(encoding="utf-8") if licenses_path.exists() else ""
    preferences = preferences_store.load()
    resolved_config_path = config_path or str(preferences.get("last_config_path", "") or "") or None
    resolved_simulation = bool(simulation) if simulation else bool(preferences.get("simulation_default", False))
    app_facade = facade or (
        AppFacade.from_config_path(
            resolved_config_path,
            simulation=resolved_simulation,
            allow_unsafe_step2_config=allow_unsafe_step2_config,
        )
        if resolved_config_path
        else AppFacade(
            config=load_config(resolved_config_path, simulation=resolved_simulation),
            simulation=resolved_simulation,
            runtime_paths=runtime_paths,
            preferences_store=preferences_store,
            recent_runs_store=recent_runs_store,
        )
    )
    if facade is None:
        app_facade.runtime_paths = runtime_paths
        app_facade.preferences_store = preferences_store
        app_facade.recent_runs_store = recent_runs_store
    run_controller = RunController(app_facade)
    live_state_feed = LiveStateFeed(app_facade)
    shell = AppShell(
        tk_root,
        facade=app_facade,
        run_controller=run_controller,
        live_state_feed=live_state_feed,
        route_memory=route_memory,
        crash_recovery=crash_recovery,
        app_info=build_info,
        release_notes_text=release_notes_text,
        licenses_text=licenses_text,
    )
    if start_feed:
        shell.start()
    return tk_root, shell, app_facade


def main(argv: Optional[list[str]] = None) -> int:
    args = create_argument_parser().parse_args(argv)
    root = tk.Tk()
    root.withdraw()
    splash = StartupSplash(root)
    splash.set_progress(15, t("app.startup.preparing_runtime"))
    root.update_idletasks()
    root.update()
    splash.set_progress(45, t("app.startup.loading_config"))
    root.update_idletasks()
    root.update()
    root, shell, _ = build_application(
        config_path=args.config,
        simulation=bool(args.simulation),
        allow_unsafe_step2_config=bool(args.allow_unsafe_step2_config),
        root=root,
        start_feed=True,
    )
    splash.set_progress(90, t("app.startup.connecting_feed"))
    root.update_idletasks()
    root.update()
    splash.set_progress(100, t("app.startup.ready"))
    root.update_idletasks()
    root.update()
    splash.destroy()
    root.deiconify()

    def _on_close() -> None:
        shell.shutdown()
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", _on_close)
    root.mainloop()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
