from __future__ import annotations

import importlib
import sys


def _clear_modules(*names: str) -> None:
    for name in names:
        sys.modules.pop(name, None)


def test_analytics_package_init_is_lazy_for_sqlalchemy_imports() -> None:
    _clear_modules(
        "sqlalchemy",
        "gas_calibrator.v2.analytics",
        "gas_calibrator.v2.analytics.feature_builder",
        "gas_calibrator.v2.analytics.service",
    )

    module = importlib.import_module("gas_calibrator.v2.analytics")

    assert module.__all__
    assert "gas_calibrator.v2.analytics.feature_builder" not in sys.modules
    assert "gas_calibrator.v2.analytics.service" not in sys.modules
    assert "sqlalchemy" not in sys.modules


def test_analytics_marts_import_stays_lightweight() -> None:
    _clear_modules(
        "sqlalchemy",
        "gas_calibrator.v2.analytics.marts",
        "gas_calibrator.v2.analytics.feature_builder",
        "gas_calibrator.v2.analytics.service",
    )

    module = importlib.import_module("gas_calibrator.v2.analytics.marts")

    assert module.__all__
    assert "gas_calibrator.v2.analytics.feature_builder" not in sys.modules
    assert "gas_calibrator.v2.analytics.service" not in sys.modules
    assert "sqlalchemy" not in sys.modules


def test_measurement_analytics_package_init_is_lazy_for_sqlalchemy_imports() -> None:
    _clear_modules(
        "sqlalchemy",
        "gas_calibrator.v2.analytics.measurement",
        "gas_calibrator.v2.analytics.measurement.feature_builder",
        "gas_calibrator.v2.analytics.measurement.service",
    )

    module = importlib.import_module("gas_calibrator.v2.analytics.measurement")

    assert module.__all__
    assert "gas_calibrator.v2.analytics.measurement.feature_builder" not in sys.modules
    assert "gas_calibrator.v2.analytics.measurement.service" not in sys.modules
    assert "sqlalchemy" not in sys.modules


def test_measurement_marts_import_stays_lightweight() -> None:
    _clear_modules(
        "sqlalchemy",
        "gas_calibrator.v2.analytics.measurement.marts",
        "gas_calibrator.v2.analytics.measurement.feature_builder",
        "gas_calibrator.v2.analytics.measurement.service",
    )

    module = importlib.import_module("gas_calibrator.v2.analytics.measurement.marts")

    assert module.__all__
    assert "gas_calibrator.v2.analytics.measurement.feature_builder" not in sys.modules
    assert "gas_calibrator.v2.analytics.measurement.service" not in sys.modules
    assert "sqlalchemy" not in sys.modules
