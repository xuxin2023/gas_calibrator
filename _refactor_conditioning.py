"""
Batch refactor: move a2 conditioning methods from orchestrator.py to conditioning_service.py
"""
import re
import os

BASE = r"d:\gas_calibrator\src\gas_calibrator\v2\core"
ORCH_PATH = os.path.join(BASE, "orchestrator.py")
SVC_PATH = os.path.join(BASE, "services", "conditioning_service.py")

with open(ORCH_PATH, encoding="utf-8") as f:
    orch_lines = f.readlines()

# Method prefixes that belong to conditioning service
PREFIXES = [
    '_a2_conditioning_', '_begin_a2_co2_route_conditioning', '_confirm_a2_co2_conditioning',
    '_record_a2_co2_conditioning', '_guard_a2_conditioning', '_a2_route_open_transient',
    '_a2_co2_route_conditioning', '_record_a2_conditioning', '_a2_conditioning',
    '_a2_prearm_route_conditioning', '_a2_prearm_baseline', '_a2_latest_route_conditioning',
    '_a2_cfg_bool', '_a2_route_conditioning_hard', '_a2_route_open_settle',
    '_a2_conditioning_stream_snapshot', '_a2_conditioning_pressure_source',
    '_begin_a2_co2_route_open_transition',
]

# Find all methods to extract
methods = []  # (name, start_idx_0based, end_idx_0based_inclusive)
in_method = False
current_name = ""
method_start = 0

for i, line in enumerate(orch_lines):
    stripped = line.strip()
    if stripped.startswith("def "):
        name = stripped.split("(")[0].split()[-1]
        if in_method:
            methods.append((current_name, method_start, i - 1))
        matches = any(name.startswith(p) for p in PREFIXES)
        if matches:
            in_method = True
            current_name = name
            method_start = i
        else:
            in_method = False
    elif in_method and i == len(orch_lines) - 1:
        methods.append((current_name, method_start, i))

# Collect lines to remove (in reverse order) and generate service body
remove_ranges = []  # (start, end_inclusive)
svc_body_lines = []

for name, start, end in methods:
    remove_ranges.append((start, end))
    # Get method lines including leading blank line separator
    block_start = start
    while block_start > 0 and orch_lines[block_start - 1].strip() == "":
        block_start -= 1
    if block_start > 0 and orch_lines[block_start - 1].strip().startswith("#"):
        block_start -= 1
    # Get trailing blank line
    block_end = end
    while block_end + 1 < len(orch_lines) and orch_lines[block_end + 1].strip() == "":
        block_end += 1
    remove_ranges[-1] = (block_start, block_end)
    for j in range(start, end + 1):
        svc_body_lines.append(orch_lines[j])
    svc_body_lines.append("\n")

# Build service file content
svc_header = '''from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Optional

from ...utils import safe_get
from ..orchestration_context import OrchestrationContext
from ..run_state import RunState


class ConditioningService:

    def __init__(self, context: OrchestrationContext, run_state: RunState, *, host: Any) -> None:
        self.context = context
        self.run_state = run_state
        self.host = host

'''

svc_content = svc_header + "".join(svc_body_lines)

# Write service file
with open(SVC_PATH, "w", encoding="utf-8", newline="\n") as f:
    f.write(svc_content)

# Build new orchestrator: replace removed blocks with delegation stubs
# Sort remove_ranges descending
remove_ranges.sort(key=lambda x: -x[0])

result_lines = list(orch_lines)

delegations = []
for _, start, end in sorted(methods, key=lambda x: x[1]):
    name = _
    # Build a simple delegation
    stub = f'    def {name}(self, *args, **kwargs):\n'
    stub += f'        return self.conditioning_service.{name}(*args, **kwargs)\n\n'
    delegations.append((start, stub))

# Apply removals
for block_start, block_end in remove_ranges:
    # Remove lines from block_start to block_end inclusive
    del result_lines[block_start:block_end + 1]

# Insert delegations at the first removed position
if methods:
    insert_at = min(s for _, s, _ in methods)  # insert at the earliest method position
    # Actually, delegation stubs should replace the original methods.
    # Since we already deleted them, we insert in reverse order at correct positions
    pass  # We'll handle this differently

# Write modified orchestrator
with open(ORCH_PATH + ".tmp", "w", encoding="utf-8", newline="\n") as f:
    f.writelines(result_lines)

print(f"Extracted {len(methods)} methods to conditioning_service.py")
print(f"Service file: {SVC_PATH} ({len(svc_body_lines)} lines of body)")
print(f"Orchestrator reduced from {len(orch_lines)} to {len(result_lines)} lines")
print("WARNING: orchestrator.py.tmp has gaps where methods were removed.")
print("Next step: manually add delegation stubs and wire up conditioning_service in __init__")
