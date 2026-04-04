from gas_calibrator.v2.qc.rule_templates import (
    ModeType,
    QCRuleTemplate,
    RouteType,
    SampleCountRule,
)


def test_rule_template_round_trip() -> None:
    template = QCRuleTemplate(
        name="co2_custom",
        route=RouteType.CO2,
        mode=ModeType.VERIFY,
        sample_count=SampleCountRule(min_count=7, max_missing=1),
        description="custom rule",
        tags=["co2", "verify"],
    )

    payload = template.to_dict()
    restored = QCRuleTemplate.from_dict(payload)

    assert payload["route"] == "co2"
    assert payload["mode"] == "verify"
    assert restored.name == "co2_custom"
    assert restored.sample_count.min_count == 7
    assert restored.tags == ["co2", "verify"]
