# V1.5 Historical Route Attestation Binder Test Evidence

## Validation

```text
87 passed, 1 warning in 18.57s
```

The compatibility run covered:

- historical mature-root attestation binder;
- historical fit evidence normalizer;
- attestation hash revalidation at normalizer consumption time;
- historical fit profile parity;
- historical replay evidence;
- mature route contract;
- V1.5 entrypoint inventory.

Static validation also passed:

- `python -m ruff check` on changed Python files;
- `python -m py_compile` on the binder, exporter, and tests;
- `git diff --check`;
- zero diff on protected mature CO2/H2O queues, shared sampling, workflow runner, analyzer protocol, default configuration, and `run_app.py`.

The single warning is the existing unregistered `v1_5_formal_gate` pytest mark. No test failed. No COM port, device, pressure, gas/water route, coefficient write, PostgreSQL connection, release, or import action was used.
