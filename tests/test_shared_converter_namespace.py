from __future__ import annotations

from gas_calibrator import utils as shared_utils
from gas_calibrator.utils import converters as shared_converters
from gas_calibrator.v2 import utils as v2_utils
from gas_calibrator.v2.utils import converters as v2_converters


EXPORTED_NAMES = (
    "as_bool",
    "as_float",
    "as_int",
    "clamp",
    "format_number",
    "parse_first_float",
    "parse_first_int",
    "safe_get",
)


def test_v2_converter_package_preserves_shared_function_identity() -> None:
    for name in EXPORTED_NAMES:
        shared_function = getattr(shared_converters, name)
        assert getattr(shared_utils, name) is shared_function
        assert getattr(v2_converters, name) is shared_function
        assert getattr(v2_utils, name) is shared_function
