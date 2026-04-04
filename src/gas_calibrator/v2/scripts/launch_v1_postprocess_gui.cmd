@echo off
setlocal
set SCRIPT_DIR=%~dp0
set SRC_ROOT=%SCRIPT_DIR%..\..\..
set PYTHONPATH=%SRC_ROOT%
pythonw -m gas_calibrator.v2.scripts.v1_postprocess_gui
if errorlevel 1 (
  python -m gas_calibrator.v2.scripts.v1_postprocess_gui
)
