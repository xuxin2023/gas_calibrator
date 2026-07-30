from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace

from gas_calibrator.validation.simulation.sampling_contracts import (
    filter_samples_for_point,
)
from gas_calibrator.v2.core.services.sampling_service import (
    SamplingService,
)


def _sample(
    name: str,
    *,
    index: int = 7,
    route: str = "co2",
    tag: str = "",
    phase: str = "",
) -> SimpleNamespace:
    return SimpleNamespace(
        name=name,
        point=SimpleNamespace(index=index, route=route),
        point_tag=tag,
        point_phase=phase,
    )


def _samples() -> list[SimpleNamespace]:
    return [
        _sample("a", tag="", phase=""),
        _sample("b", tag=" target ", phase="CO2"),
        _sample("c", tag="target", phase="h2o"),
        _sample("d", index=8, tag="target", phase="co2"),
        _sample("e", route="CO2", tag="target", phase="co2"),
        _sample("f", tag="target", phase=" co2 "),
    ]


def test_sampling_selection_has_shared_owner_and_no_v2_import() -> None:
    assert filter_samples_for_point.__module__ == (
        "gas_calibrator.validation.simulation.sampling_contracts"
    )

    path = (
        Path(__file__).resolve().parents[2]
        / "src/gas_calibrator/validation/simulation/sampling_contracts.py"
    )
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imported_modules = {
        str(node.module or "")
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    assert all(
        not module.startswith("gas_calibrator.v2")
        for module in imported_modules
    )


def test_sampling_selection_preserves_exact_route_tag_and_phase_rules() -> None:
    samples = _samples()
    point = SimpleNamespace(index=7, route="co2")

    assert [
        item.name
        for item in filter_samples_for_point(samples, point)
    ] == ["a", "b", "c", "f"]
    assert [
        item.name
        for item in filter_samples_for_point(
            samples,
            point,
            point_tag=" target ",
        )
    ] == ["b", "c", "f"]
    assert [
        item.name
        for item in filter_samples_for_point(
            samples,
            point,
            phase=" CO2 ",
        )
    ] == ["a", "b", "f"]
    assert [
        item.name
        for item in filter_samples_for_point(
            samples,
            point,
            point_tag="target",
            phase="co2",
        )
    ] == ["b", "f"]
    assert [
        item.name
        for item in filter_samples_for_point(
            samples,
            point,
            phase="h2o",
        )
    ] == ["a", "c"]


def test_sampling_selection_preserves_identity_order_and_v2_delegate() -> None:
    samples = _samples()
    point = SimpleNamespace(index=7, route="co2")
    selected = filter_samples_for_point(
        tuple(samples),
        point,
        phase="co2",
    )

    assert selected == [samples[0], samples[1], samples[5]]
    assert selected[0] is samples[0]
    assert selected[1] is samples[1]
    assert selected[2] is samples[5]

    result_store = SimpleNamespace(get_samples=lambda: samples)
    service = object.__new__(SamplingService)
    service.context = SimpleNamespace(result_store=result_store)
    assert service.samples_for_point(
        point,
        phase="co2",
    ) == selected
