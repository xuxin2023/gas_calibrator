# V1.5 production promotion preflight test evidence

Date: 2026-07-13

## Focused tests

```text
12 passed in 4.18s
```

Coverage includes one-device and six-device packages, committed and idempotent staging results, immutable source hashes, independent authorization semantics, identity/table readback, canonical production DSN environment naming, forbidden production execution flags, and entrypoint classification.

## Database and entrypoint compatibility

```text
145 passed, 1 skipped, 1 warning in 28.55s
```

The skipped test is the isolated staging PostgreSQL integration because `V1_5_POSTGRES_STAGING_DSN_TEST` was not configured in this shell. PR #142 retains its independent PostgreSQL 18 integration evidence (`13 passed`). The warning is the pre-existing unregistered `v1_5_formal_gate` marker.

## Evidence, artifact, and parity regression

```text
69 passed, 1 warning in 36.01s
```

## Mature-route and final-suite guard

```text
20 passed in 2.47s
```

No test opens COM, writes analyzer state, controls pressure/routes, or connects to the production database.
