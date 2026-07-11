# V1.5 Authoritative Resume State Consumer Contract

This offline contract validates that a post-write-verified authoritative state belongs to the exact full-flow plan, uses the same run ID, contains one contiguous completed prefix, points to the immediately following step, has no failed steps, and keeps all real-COM, pressure, route, and write authorizations false.

Even when ready, `execution_supported=false`, `resume_execution_allowed=false`, and `would_execute=false`. The package does not execute the next step or alter full-flow ordering. The 0613 fitting baseline and 0620/0621 mature CO2/H2O routes remain unchanged.
