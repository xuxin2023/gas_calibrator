from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.app import build_application, create_argument_parser

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import build_fake_facade, make_root


def test_ui_v2_app_builds_shell(tmp_path: Path) -> None:
    root = make_root()
    shell = None
    try:
        facade = build_fake_facade(tmp_path)
        _, shell, returned_facade = build_application(
            root=root,
            facade=facade,
            start_feed=False,
        )
        assert shell.root is root
        assert returned_facade is facade
        assert set(shell._pages) == {"run", "qc", "results", "devices", "algorithms", "reports", "plan"}
        assert shell.run_id_var.get() == facade.session.run_id
    finally:
        if shell is not None:
            shell.shutdown()
        root.destroy()


def test_ui_v2_app_parser_supports_config_and_simulation() -> None:
    args = create_argument_parser().parse_args(["--config", "demo.json", "--simulation"])
    assert args.config == "demo.json"
    assert args.simulation is True
