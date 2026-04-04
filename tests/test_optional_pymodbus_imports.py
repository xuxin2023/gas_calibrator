import os
import subprocess
import sys
from pathlib import Path


def test_optional_pymodbus_import_chain_survives_without_dependency() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    src_root = repo_root / "src"
    script = """
import builtins
import importlib

real_import = builtins.__import__

def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
    if name == "pymodbus" or name.startswith("pymodbus."):
        raise ModuleNotFoundError("No module named 'pymodbus'")
    return real_import(name, globals, locals, fromlist, level)

builtins.__import__ = fake_import

for module_name in (
    "gas_calibrator.devices",
    "gas_calibrator.devices.relay",
    "gas_calibrator.devices.temperature_chamber",
    "gas_calibrator.tools.run_headless",
    "gas_calibrator.diagnostics",
    "gas_calibrator.ui.app",
    "gas_calibrator.ui.valve_page",
):
    importlib.import_module(module_name)

relay_module = importlib.import_module("gas_calibrator.devices.relay")
chamber_module = importlib.import_module("gas_calibrator.devices.temperature_chamber")

class FakeRelayClient:
    def connect(self):
        return True

    def close(self):
        return None

class FakeChamberClient(FakeRelayClient):
    pass

relay_module.RelayController("COM1", client=FakeRelayClient())
chamber_module.TemperatureChamber("COM2", client=FakeChamberClient())

try:
    relay_module.RelayController("COM1")
except ModuleNotFoundError as exc:
    assert "pymodbus is required to open real relay devices" in str(exc)
else:
    raise AssertionError("expected relay real-device constructor to require pymodbus")

try:
    chamber_module.TemperatureChamber("COM2")
except ModuleNotFoundError as exc:
    assert "pymodbus is required to open real temperature chamber devices" in str(exc)
else:
    raise AssertionError("expected chamber real-device constructor to require pymodbus")
"""
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        str(src_root)
        if not existing_pythonpath
        else os.pathsep.join([str(src_root), existing_pythonpath])
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr or result.stdout
