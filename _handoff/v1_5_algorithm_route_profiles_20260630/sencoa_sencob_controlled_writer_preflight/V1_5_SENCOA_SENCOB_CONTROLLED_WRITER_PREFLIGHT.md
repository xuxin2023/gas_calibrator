# V1.5 SENCOA/SENCOB controlled-writer preflight

This is a no-write preflight and future real-write boundary document.

- It does not open COM ports, import GasAnalyzer, or write coefficients.
- Real writing remains blocked until a controlled writer is implemented and reviewed.
- Future payloads must contain SENCOA and SENCOB rows with four finite coefficients each.
- Future write attempts must snapshot GETCOA/GETCOB first, verify readback after each write, and rollback on mismatch.
- Analyzer command pacing must stay at or above 1 second.
- Production acceptance still requires independent CO2/H2O no-write reverification after any future write.
