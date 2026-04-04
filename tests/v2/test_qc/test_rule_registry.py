from gas_calibrator.v2.qc.rule_registry import QCRuleRegistry
from gas_calibrator.v2.qc.rule_templates import ModeType, RouteType


def test_rule_registry_registers_defaults() -> None:
    registry = QCRuleRegistry()

    names = set(registry.list_templates())

    assert {"co2_strict", "h2o_strict", "fast_mode", "verify_mode", "subzero", "default"}.issubset(names)


def test_rule_registry_matches_route_and_mode() -> None:
    registry = QCRuleRegistry()

    co2_rule = registry.get_for_route_mode(RouteType.CO2, ModeType.NORMAL)
    fast_rule = registry.get_for_route_mode(RouteType.H2O, ModeType.FAST)

    assert co2_rule.name == "co2_strict"
    assert fast_rule.name == "fast_mode"
