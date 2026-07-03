from argparse import Namespace

import pytest

from gas_calibrator.tools.v1_5_serial_safety import require_fragile_serial_timing


def test_fragile_serial_timing_rejects_subsecond_analyzer_commands():
    args = Namespace(
        readback_retry_delay_s=0.5,
        restore_command_gap_s=1.0,
        coefficient_read_delay_s=1.0,
        post_write_settle_s=1.0,
    )

    with pytest.raises(ValueError, match="below 1s"):
        require_fragile_serial_timing(args, tool_name="unit-test-tool")


def test_fragile_serial_timing_accepts_one_second_or_slower():
    args = Namespace(
        readback_retry_delay_s=1.0,
        restore_command_gap_s=1.2,
        coefficient_read_delay_s=1.0,
        post_write_settle_s=2.0,
    )

    require_fragile_serial_timing(args, tool_name="unit-test-tool")
