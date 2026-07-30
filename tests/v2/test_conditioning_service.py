from __future__ import annotations

from inspect import isfunction
from typing import get_type_hints

from gas_calibrator.v2.core.services.conditioning_service import ConditioningService


def test_conditioning_service_annotations_are_resolvable() -> None:
    annotated_methods = [
        member
        for member in vars(ConditioningService).values()
        if isfunction(member) and member.__annotations__
    ]

    assert annotated_methods
    for method in annotated_methods:
        get_type_hints(method)
