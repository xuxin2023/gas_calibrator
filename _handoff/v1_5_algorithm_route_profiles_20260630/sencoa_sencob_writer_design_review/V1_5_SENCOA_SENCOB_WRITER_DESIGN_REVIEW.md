# V1.5 SENCOA/SENCOB writer design review

This is an offline no-write design review. It does not implement a real writer.

- SENCOA is reserved for `R0_CO2(T)` and must be read back through GETCOA.
- SENCOB is reserved for `R0_H2O(T)` and must be read back through GETCOB.
- Payloads are four finite float coefficients in the reviewed order.
- Future real writes must use MODE2 and a serial command gap of at least 1 second.
- Future real writes require old GETCOA/GETCOB snapshots, readback verification, rollback, and independent CO2/H2O reverification.
- Current status remains blocked because the controlled SENCOA/SENCOB writer does not exist yet.
