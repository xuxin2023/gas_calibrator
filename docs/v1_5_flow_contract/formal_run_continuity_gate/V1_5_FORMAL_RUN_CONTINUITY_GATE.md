# V1.5 Formal Run Continuity Gate

This offline gate protects the V1.5 mature route workflow from a failure mode
seen in the July 2026 six-analyzer run: a route can be physically interrupted,
restarted with adjusted parameters, recovered by targeted points, and then
summarized into an accepted manifest. That evidence may be useful for fitting
after review, but it is not the same as one continuous formal run.

## Purpose

- Keep the 0613 fitting method and 0620/0621 mature physical route baseline explicit.
- Distinguish a true continuous formal run from a segmented engineering recovery run.
- Require a segment ledger before segmented CO2/H2O evidence can be reviewed for fitting.
- Prevent failed, running, diagnostic, smoke, targeted, or reverify rows from silently becoming production fit evidence.
- Keep `_handoff`, 0624 migration queues, root migration surfaces, and per-point workers out of formal run continuity evidence.

## Status Semantics

- `pass`: one canonical queue segment closes the expected point count with no failed or running points.
- `review_required`: multiple canonical queue segments have a complete accepted manifest and explicit segment reasons, so a reviewer may decide fit eligibility.
- `blocked`: the ledger is missing, incomplete, uses forbidden sources, contains running/failed fit-eligible segments, lacks accepted manifest coverage, or changes parameters without review.

## Legacy Point Contracts

- Legacy CO2: 45 points.
- Legacy H2O: 13 wet points.

New-algorithm point counts remain profile-gated and must not alter the legacy route contract.

## Non-Execution Boundary

This gate is offline only:

- `opens_com_ports=false`
- `controls_pressure=false`
- `controls_water_or_gas_routes=false`
- `connects_postgresql=false`
- `writes_coefficients=false`
- `writes_sn_or_device_code=false`
- `formal_release_allowed=false`
- `database_import_allowed=false`
- `not_real_acceptance_evidence=true`

## Reviewer Rule

Fitting tools should only consume segmented route evidence after this gate
produces either:

- `pass`, meaning continuous formal evidence; or
- `review_required` with zero blockers, meaning segmented evidence has a complete accepted manifest and explicit supersedence/reasoning.

If the gate is `blocked`, the evidence package must not be used as formal fit input.
