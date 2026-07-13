"""Inventory and classify V1.5 entrypoints.

The classifier is intentionally conservative. It does not decide whether a tool
is correct; it records what kind of entrypoint it appears to be so formal V1.5
work can avoid mixing production runners with diagnostics and one-off analysis.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable


V1_5_TOOL_PREFIXES = (
    "run_v1_5_",
    "export_v1_5_",
    "prepare_v1_5_",
    "import_v1_5_",
    "query_v1_5_",
    "probe_v1_5_",
    "verify_v1_5_",
    "summarize_v1_5_",
    "collect_v1_5_",
    "migrate_v1_5_",
    "archive_v1_5_",
    "build_v1_5_",
)

EXTRA_V1_5_REVIEW_TOOL_NAMES = (
    "export_single_gas_pressure_curve",
    "validate_pressure_only",
)

EXTRA_V1_5_DIAGNOSTIC_TOOL_NAMES = (
    "run_room_temp_co2_pressure_diagnostic",
)

LEGACY_V1_REFERENCE_TOOL_NAMES = (
    "run_v1_corrected_autodelivery",
    "run_v1_merged_calibration_sidecar",
    "run_v1_online_acceptance",
    "run_v1_no500_postprocess",
)

CANONICAL_FORMAL_WORKER_TOOL_NAMES = (
    "run_v1_5_formal_open_flow_sampling",
    "run_v1_5_formal_h2o_open_flow_sampling",
)

FORMAL_INITIALIZATION_SUPPORT_TOOL_NAMES = (
    "run_v1_5_analyzer_runtime_setup",
    "run_v1_5_initialization_db_preflight",
    "run_v1_5_sn_identity_initialization",
)

FORMAL_PREFLIGHT_SUPPORT_TOOL_NAMES = (
    "run_v1_5_formal_route_readiness_probe",
)


CANONICAL_FORMAL_PATH: tuple[dict[str, str], ...] = (
    {
        "stage": "00_full_flow_guard",
        "entrypoint": "src/gas_calibrator/tools/run_v1_5_full_calibration_chain.py",
        "category": "full_flow_orchestration",
        "status": "offline_plan_and_gate_only",
        "physical_meaning": "Orders pressure, temperature, open-flow CO2, open-flow H2O, QC, review, write gate, reverify, and archive without opening COM by default.",
        "safety_boundary": "Do not use it as a hidden real-device runner; it is the formal sequence contract and supervised planner.",
    },
    {
        "stage": "01_formal_initialization",
        "entrypoint": "src/gas_calibrator/tools/run_v1_5_formal_initialization_runner.py",
        "category": "full_flow_orchestration",
        "status": "offline_initialization_planner_and_gate",
        "physical_meaning": "Owns the formal initialization contract: device-ID binding, GETCO1-9 epoch-0 snapshot, S5/S6/S7/S8/S9 gates, startup acquisition settings, readiness, pre-gas gap list, and database evidence indexing.",
        "safety_boundary": "This is the single formal initialization entrypoint. It plans and gates; subordinate probe/writer/readiness tools do the authorized read-only or controlled-write work and historical logs remain traceability evidence only.",
    },
    {
        "stage": "02_pressure_channel",
        "entrypoint": "src/gas_calibrator/tools/export_v1_5_pressure_channel_validation.py",
        "category": "formal_review_evidence",
        "status": "implemented_offline_report_from_evidence",
        "physical_meaning": "Verifies analyzer pressure P against COM22/PACE before CO2/H2O fitting so pressure error is not absorbed into gas coefficients.",
        "safety_boundary": "Pressure fitting and SENCO9 handling are separate from CO2/H2O concentration fitting.",
    },
    {
        "stage": "03_temperature_channel",
        "entrypoint": "src/gas_calibrator/tools/export_v1_5_temperature_channel_review.py",
        "category": "formal_review_evidence",
        "status": "implemented_review_from_evidence",
        "physical_meaning": "Reviews chamber/case temperature behavior against temperature evidence before interpreting multi-temperature gas response.",
        "safety_boundary": "Temperature review is not an automatic temperature coefficient write.",
    },
    {
        "stage": "04_co2_open_flow_sampling",
        "entrypoint": "src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py",
        "category": "formal_runner",
        "status": "real_route_runner_when_authorized",
        "physical_meaning": "Collects clean open-flow CO2 data while standard gas continuously refreshes the analyzer cavity and downstream line.",
        "safety_boundary": "Do not include sealed-pressure, VENT-hold, or dynamic-pressure diagnostic rows in the formal CO2 fit.",
    },
    {
        "stage": "05_h2o_open_flow_sampling",
        "entrypoint": "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_queue.py",
        "category": "formal_runner",
        "status": "real_route_runner_when_authorized",
        "physical_meaning": "Collects open-flow H2O data with dewpoint/reference evidence so dry-gas and wet-gas anchors remain physically interpretable.",
        "safety_boundary": "Dry-gas low-water evidence is not automatically the same thing as CO2 zero-gas evidence.",
    },
    {
        "stage": "06_candidate_coefficients",
        "entrypoint": "src/gas_calibrator/tools/export_v1_5_candidate_coefficients.py",
        "category": "formal_review_evidence",
        "status": "implemented_offline_review",
        "physical_meaning": "Builds candidate coefficients from eligible A-grade open-flow samples and preserves rejected points with reasons.",
        "safety_boundary": "Candidate generation is no-write; diagnostic rows and rejected rows stay outside formal fit eligibility by default.",
    },
    {
        "stage": "07_write_review_gate",
        "entrypoint": "src/gas_calibrator/tools/export_v1_5_candidate_write_review.py",
        "category": "formal_review_evidence",
        "status": "implemented_offline_review",
        "physical_meaning": "Checks old coefficients, candidate coefficients, residuals, blockers, and reviewer evidence before any SENCO write.",
        "safety_boundary": "A review artifact is not authorization to write; controlled-write tools remain separate.",
    },
    {
        "stage": "08_controlled_write",
        "entrypoint": "src/gas_calibrator/tools/run_v1_5_co2_senco13_controlled_write.py",
        "category": "controlled_write",
        "status": "manual_authorized_only",
        "physical_meaning": "Writes CO2 main-chain coefficients only after identity, old-coefficient snapshot, candidate review, and readback plan are available.",
        "safety_boundary": "Never run as a background evidence or report step; H2O, pressure, and linear-trim writes use their own controlled tools.",
    },
    {
        "stage": "09_post_write_reverify",
        "entrypoint": "src/gas_calibrator/tools/export_v1_5_post_write_reverification.py",
        "category": "formal_review_evidence",
        "status": "implemented_offline_report_from_evidence",
        "physical_meaning": "Confirms written coefficients against independent reverify samples before release.",
        "safety_boundary": "A coefficient write without post-write reverify remains incomplete for formal release.",
    },
    {
        "stage": "10_archive_report_database",
        "entrypoint": "src/gas_calibrator/tools/run_v1_5_formal_archive_closure.py",
        "category": "formal_review_evidence",
        "status": "implemented_offline_archive",
        "physical_meaning": "Closes the run evidence chain with artifact hashes, reports, database index inputs, and traceability status.",
        "safety_boundary": "Archive closure does not change device state and must not hide rejected frames or points.",
    },
    {
        "stage": "11_formal_run_status_dashboard",
        "entrypoint": "src/gas_calibrator/tools/export_v1_5_formal_run_status.py",
        "category": "formal_review_evidence",
        "status": "implemented_offline_status_rollup",
        "physical_meaning": "Summarizes current stage, next action, physical-flow continuation, and formal archive/database release readiness from existing sidecars.",
        "safety_boundary": "Formal run status is read-only evidence; it must not open COM, connect PostgreSQL, control routes, or write analyzer state.",
    },
)

CANONICAL_FORMAL_STAGE_ORDER: tuple[str, ...] = (
    "00_full_flow_guard",
    "01_formal_initialization",
    "02_pressure_channel",
    "03_temperature_channel",
    "04_co2_open_flow_sampling",
    "05_h2o_open_flow_sampling",
    "06_candidate_coefficients",
    "07_write_review_gate",
    "08_controlled_write",
    "09_post_write_reverify",
    "10_archive_report_database",
    "11_formal_run_status_dashboard",
)

CANONICAL_FORMAL_SUPPORT_TOOL_NAMES = (
    *CANONICAL_FORMAL_WORKER_TOOL_NAMES,
    *FORMAL_INITIALIZATION_SUPPORT_TOOL_NAMES,
    *FORMAL_PREFLIGHT_SUPPORT_TOOL_NAMES,
)


NON_START_HERE_GUARDRAILS: dict[str, dict[str, str]] = {
    "controlled_write": {
        "guardrail": "authorized_write_only",
        "allowed_use": "Use only after candidate review, old-coefficient snapshot, explicit write authorization, and readback/reverify plan.",
    },
    "diagnostic_only": {
        "guardrail": "diagnostic_not_acceptance",
        "allowed_use": "Use for engineering investigation or replay evidence; exclude from formal CO2/H2O fitting by default.",
    },
    "housekeeping_archive": {
        "guardrail": "archive_housekeeping_only",
        "allowed_use": "Use for housekeeping/archive organization; do not treat as calibration evidence generation.",
    },
    "noncanonical_formal_runner": {
        "guardrail": "review_before_formal_use",
        "allowed_use": "Route/COM-capable runner exists outside the canonical path; inspect purpose and safeguards before using it.",
    },
    "formal_sampling_worker": {
        "guardrail": "use_via_canonical_queue_only",
        "allowed_use": "Per-point open-flow worker called by the canonical CO2/H2O queue runners; do not delete, but do not treat as a top-level formal start point.",
    },
    "unclassified_v1_5_tool": {
        "guardrail": "classification_required",
        "allowed_use": "Do not use until the inventory assigns an explicit category, status, and risk boundary.",
    },
    "formal_pressure_no_write_runner": {
        "guardrail": "pressure_no_write_only",
        "allowed_use": "Use only for pressure-channel validation/calibration evidence; do not feed CO2/H2O fitting directly.",
    },
    "legacy_v1_reference": {
        "guardrail": "legacy_v1_reference_only",
        "allowed_use": "Keep only as historical algorithm/audit reference; do not use to start V1.5 formal calibration.",
    },
}


@dataclass(frozen=True)
class V15Entrypoint:
    path: str
    name: str
    artifact_type: str
    category: str
    stage: str
    formal_status: str
    risk_level: str
    opens_com_ports: bool
    controls_routes: bool
    writes_coefficients: bool
    notes: tuple[str, ...] = ()

    def to_json(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class V15WorkspaceSurfaceRow:
    surface: str
    path: str
    status: str
    action: str
    file_count: int
    examples: tuple[str, ...] = ()
    reason: str = ""

    def to_json(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class V15ActiveSurfacePolicyIssue:
    severity: str
    rule: str
    path: str
    message: str

    def to_json(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class V15IsolationReferenceIssue:
    severity: str
    isolated_path: str
    isolated_category: str
    reference_path: str
    reference_category: str
    matched_token: str
    message: str

    def to_json(self) -> dict:
        return asdict(self)


def _rel(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def _stage_from_name(name: str) -> str:
    lower = name.lower()
    if "serial_port" in lower:
        return "identity_and_serial_binding"
    if "pressure" in lower or "senco9" in lower or "pace" in lower:
        return "pressure_channel"
    if "temperature" in lower or "temp" in lower:
        return "temperature_channel"
    if "h2o" in lower or "dewpoint" in lower or "humidity" in lower:
        return "h2o_component"
    if "co2" in lower or "senco1" in lower or "senco3" in lower or "senco5" in lower:
        return "co2_component"
    if "evidence" in lower or "database" in lower or "db" in lower or "registry" in lower:
        return "evidence_database"
    if "report" in lower or "calibration_package" in lower:
        return "reporting"
    if "qc" in lower or "quality" in lower:
        return "qc_review"
    if "operation_console" in lower or "workbench" in lower or "review_surface" in lower:
        return "ui_review"
    if "full_flow" in lower or "formal_run_package" in lower:
        return "full_flow_orchestration"
    if "candidate" in lower or "coefficient" in lower or "fit" in lower:
        return "coefficient_review"
    return "general"


def _notes_for_name(name: str) -> list[str]:
    lower = name.lower()
    notes: list[str] = []
    if lower in LEGACY_V1_REFERENCE_TOOL_NAMES:
        notes.append("legacy V1 reference only; do not use as a V1.5 formal entrypoint")
    if lower == "validate_pressure_only":
        notes.append("pressure-channel no-write validation/calibration runner; separate from CO2/H2O fitting")
    if lower == "probe_v1_5_getco_component_snapshot":
        notes.append("subordinate initialization evidence tool; read-only GETCO1-9 and device-ID snapshot")
    elif lower in FORMAL_INITIALIZATION_SUPPORT_TOOL_NAMES:
        notes.append("formal initialization support; use only through the initialization owner or explicit preflight")
    elif lower in FORMAL_PREFLIGHT_SUPPORT_TOOL_NAMES:
        notes.append("formal route-readiness preflight support; records readiness evidence before mature route runners")
    elif lower == "run_v1_5_formal_initialization_runner":
        notes.append("canonical initialization owner; offline planner, evidence indexer, and readiness gate")
    elif lower == "export_v1_5_formal_initialization_executor_dry_run":
        notes.append("offline initialization executor dry-run review; classifies plan steps without executing COM or write commands")
    elif lower == "run_v1_5_formal_initialization_blocked_executor":
        notes.append("offline initialization blocked executor stub; refuses live COM, SN/device-code writes, SENCO writes, PostgreSQL, pressure, and route actions")
    elif lower == "export_v1_5_formal_initialization_controlled_executor_design":
        notes.append("offline initialization controlled executor design; defines future authorization, real-COM, write/readback, CHECK, and hold contract without opening COM")
    elif lower == "export_v1_5_formal_initialization_readonly_com_preflight_design":
        notes.append("offline initialization read-only real-COM preflight design; defines future port, pacing, identity, GETCO, CHECK, and hold contract without opening COM")
    elif lower == "run_v1_5_formal_initialization_readonly_com_preflight_blocked_executor":
        notes.append("offline initialization read-only real-COM preflight blocked executor stub; refuses analyzer COM, SN/device-code writes, SENCO writes, PostgreSQL, pressure, and route actions")
    elif lower == "export_v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design":
        notes.append("offline initialization read-only real-COM preflight controlled executor design; defines future authorization, port inventory, read sequence, evidence, and hold contract without opening COM")
    elif lower == "run_v1_5_formal_initialization_readonly_com_preflight_controlled_blocked_executor":
        notes.append("offline initialization read-only real-COM preflight controlled blocked executor stub; refuses analyzer COM, authorization unlocks, SN/device-code writes, SENCO writes, PostgreSQL, pressure, and route actions")
    elif lower == "export_v1_5_formal_readonly_com_execution_contract":
        notes.append("offline read-only COM execution packet contract; defines future authorization, port inventory, active analyzer, pacing, CHECK-skip, and denied-action rules without opening COM")
    elif lower == "run_v1_5_formal_readonly_com_execution_blocked_executor":
        notes.append("offline read-only COM execution blocked executor stub; refuses analyzer COM, authorization unlocks, SN/device-code writes, SENCO writes, PostgreSQL, pressure, and route actions")
    elif lower == "export_v1_5_formal_readonly_com_execution_packet_validator":
        notes.append("offline read-only COM execution packet validator; validates future authorization, port inventory, active analyzer list, pacing, and CHECK-skip rules without opening COM")
    elif lower == "export_v1_5_formal_readonly_com_execution_plan_preview":
        notes.append("offline read-only COM execution plan preview; renders future identity, SN, GETCO, runtime, and CHECK read order without opening COM")
    elif lower == "export_v1_5_formal_readonly_com_minimal_executor_review":
        notes.append("offline read-only COM minimal executor review; defines future output evidence and failure hold matrix without opening COM")
    elif lower == "run_v1_5_formal_readonly_com_minimal_executor_stub":
        notes.append("offline read-only COM minimal executor stub; records would-execute evidence without opening COM or using authorization context as unlock")
    elif lower == "export_v1_5_resume_prefix_application_review":
        notes.append("offline resume-prefix application review; validates the hash-bound completed prefix without writing state or executing the next stage")
    elif lower == "export_v1_5_authoritative_resume_state_writer_design":
        notes.append("offline authoritative resume-state writer design; defines atomic replace, compare-and-swap, snapshot, readback, and rollback requirements without writing state")
    elif lower == "run_v1_5_authoritative_resume_state_writer_blocked_executor":
        notes.append("offline authoritative resume-state writer blocked executor; refuses state target, expected-state hash, authorization, execute, and replace inputs without creating or replacing state")
    elif lower == "export_v1_5_authoritative_resume_state_controlled_write_preflight":
        notes.append("offline authoritative resume-state controlled-write preflight; generates a deterministic candidate state preview and validates target SHA256 plus distinct authorization without writing state")
    elif lower == "run_v1_5_authoritative_resume_state_atomic_writer":
        notes.append("manual-authorized atomic authoritative resume-state writer; consumes only the exact ready preflight and performs lock, current-SHA check, snapshot, fsync, atomic replace, readback, and rollback without opening COM")
    elif lower == "export_v1_5_authoritative_resume_state_post_write_verification":
        notes.append("offline authoritative resume-state post-write verification; hash-checks writer evidence, authorization, preflight, candidate, target, snapshot, and released lock without writing state or opening COM")
    elif lower == "export_v1_5_authoritative_resume_state_consumer_contract":
        notes.append("offline default-locked resume-state consumer contract; validates plan, run identity, contiguous prefix, next step, state hash, and authorization locks without executing the resumed step")
    elif lower == "export_v1_5_authoritative_resume_executor_plan_preview":
        notes.append("offline plan-only resume executor preview; independently recomputes the consumer contract and displays the next command and authorization requirements without executing it")
    elif lower == "run_v1_5_authoritative_resume_executor_blocked":
        notes.append("offline blocked resume executor; independently recomputes the plan preview and rejects execute, resume, COM, pressure, route, write, and database unlocks")
    elif lower == "export_v1_5_authoritative_resume_executor_controlled_design":
        notes.append("offline controlled resume executor design; binds future authorization to exact plan, state, next-step, command hash, expiry, and least-privilege capabilities without executing")
    elif lower == "export_v1_5_authoritative_resume_executor_authorization_validator":
        notes.append("offline resume executor authorization validator; recomputes the design and validates identity, expiry, evidence hashes, canonical next-step, command hash, and least-privilege capabilities without executing")
    elif lower == "export_v1_5_authoritative_resume_execution_preflight":
        notes.append("offline last-moment resume execution preflight; revalidates authorization, state, plan, next-step, command hash, expiry, and least-privilege envelope while keeping execution locked")
    elif lower == "export_v1_5_authoritative_resume_offline_candidate_gate":
        notes.append("offline resume candidate classifier; admits only fresh canonical steps with offline mode and no COM, pressure, route, device, coefficient, or database side effects without executing")
    elif lower == "run_v1_5_authoritative_resume_offline_executor":
        notes.append("manual-authorized offline-only resume executor; revalidates a fresh candidate, runs one exact Python module with shell disabled, verifies fresh outputs, and never advances authoritative state")
    elif lower == "export_v1_5_authoritative_resume_offline_post_execution_verifier":
        notes.append("offline resume post-execution verifier; binds the executor, gate, plan, output SHA256 values, authorization packet, and unchanged authoritative state without executing or advancing state")
    elif lower == "export_v1_5_authoritative_resume_offline_state_advance_preflight":
        notes.append("offline resume state-advance preflight; recomputes post-execution evidence and generates one deterministic compare-and-swap candidate state without writing or replacing authoritative state")
    elif lower == "export_v1_5_authoritative_resume_offline_state_advance_authorization":
        notes.append("offline resume state-advance authorization validator; binds a short-lived distinct-reviewer packet to the exact preflight, current-state SHA, candidate SHA, run, attempt, and verified step without writing state")
    elif lower == "run_v1_5_authoritative_resume_offline_state_advance_blocked_executor":
        notes.append("offline resume state-advance blocked executor; freshly revalidates authorization but exposes no execute or state-write path")
    elif lower == "run_v1_5_authoritative_resume_offline_state_advance_atomic_writer":
        notes.append("manual-authorized one-step offline resume state-advance writer; freshly revalidates authorization under the shared lock, checks current and candidate SHA256 values, snapshots, atomically replaces, reads back, and rolls back without opening COM")
    elif lower == "export_v1_5_authoritative_resume_offline_state_advance_post_write_verification":
        notes.append("offline one-step resume state-advance post-write verifier; binds writer, authorization, preflight, candidate, final state, rollback snapshot, invocation, and released lock without writing state")
    elif lower == "export_v1_5_authoritative_resume_offline_state_advance_consumer_readiness":
        notes.append("offline advanced resume-state consumer readiness gate; independently recomputes post-write verification and checks the locked contiguous state prefix without executing the next step")
    elif lower == "export_v1_5_authoritative_resume_offline_state_advance_next_step_plan":
        notes.append("offline advanced resume-state next-step preview; recomputes consumer readiness and binds the exact canonical next command plus authorization envelope without executing it")
    elif lower == "export_v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight":
        notes.append("offline next-step review authorization preflight; binds a short-lived three-party packet to the exact plan, consumer, run, attempt, next step, and mature module while keeping execution locked")
    elif lower == "run_v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor":
        notes.append("offline next-step blocked executor; freshly revalidates the review authorization but exposes no execute, COM, route, write, or database path")
    elif lower == "export_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design":
        notes.append("offline next-step controlled executor design; freezes exact-command authorization, least privilege, failure holds, and output evidence while keeping execution unavailable")
    elif lower == "export_v1_5_authoritative_resume_offline_state_advance_next_step_execution_authorization":
        notes.append("offline next-step execution authorization validator; binds a short-lived three-party packet to the exact plan, command hash, evidence chain, and least-privilege capabilities")
    elif lower == "export_v1_5_authoritative_resume_offline_state_advance_next_step_execution_preflight":
        notes.append("last-moment next-step execution preflight; freshly rehashes authorization, state, plan, exact mature command, and output boundaries without starting a process")
    elif lower == "run_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor":
        notes.append("manual-authorized single-step V1.5 executor; permits one exact shell-free process only after fresh authorization, never retries, substitutes, imports PostgreSQL, or advances authoritative state")
    elif lower == "run_v1_5_authoritative_resume_offline_state_advance_next_step_operator_bundle":
        notes.append("manual-authorized operator bundle launcher; derives least-privilege authorization from the exact plan, revalidates immediately, and defaults to locked evidence unless one explicit next-step execution is requested")
    elif lower == "run_v1_5_new_run_bootstrap":
        notes.append("offline atomic new-run bootstrap; snapshots one reviewed config and creates a zero-authority plan/state at the first canonical stage without opening COM or marking any step complete")
    elif lower == "run_v1_5_formal_readonly_com_minimal_executor":
        notes.append("manual-authorized minimal read-only COM executor; reads SN/GETCO/runtime/CHECK evidence only, never writes analyzer state, database, pressure, or routes")
    elif "formal_archive_closure" in lower:
        notes.append("offline archive closure; does not open COM ports or control routes")
    elif lower == "export_v1_5_pre_gas_readiness":
        notes.append("offline pre-gas readiness sidecar; summarizes identity, DB, GETCO, S7/S8, S9, route, and CHECK gates before live identity")
    elif lower == "export_v1_5_getco_identity_readiness":
        notes.append("offline identity/GETCO readiness sidecar; consumes read-only GETCO artifacts and does not open COM")
    elif lower == "export_v1_5_formal_run_status":
        notes.append("offline formal run status rollup; reads readiness/archive sidecars and does not open COM")
    elif lower == "export_v1_5_formal_database_dry_run":
        notes.append("offline PostgreSQL 18 database dry-run contract; previews schema and insert roles without connecting or importing data")
    elif lower == "export_v1_5_formal_database_import_preflight":
        notes.append("offline PostgreSQL 18 database import preflight; reviews DSN presence and import locks without connecting or importing data")
    elif lower == "export_v1_5_formal_database_import_authorization":
        notes.append("offline PostgreSQL 18 database import authorization guard; reviews archive release and manual authorization without connecting or importing data")
    elif lower == "export_v1_5_formal_database_import_command_contract":
        notes.append("offline PostgreSQL 18 database import command contract; reviews required real-import inputs without connecting or importing data")
    elif lower == "import_v1_5_evidence_package":
        notes.append("offline PostgreSQL 18 blocked import executor stub; legacy bundle dry-run only, no connection, no migration, no row import")
    elif lower == "export_v1_5_formal_database_import_controlled_executor_design":
        notes.append("offline PostgreSQL 18 controlled import executor design; defines future authorization, transaction, readback, and rollback contract without connecting")
    elif lower == "export_v1_5_historical_replay_contract":
        notes.append("offline historical replay contract; validates replay interpretation without opening COM or releasing archive/database evidence")
    elif lower == "export_v1_5_historical_replay_evidence":
        notes.append("offline historical replay evidence binder; reads historical CSV/JSON point evidence without opening COM or changing release state")
    elif lower == "export_v1_5_historical_replay_missing_point_audit":
        notes.append("offline historical replay missing-point audit; searches segmented/retry evidence without opening COM or changing fit eligibility")
    elif lower == "export_v1_5_historical_replay_qc_gap_audit":
        notes.append("offline historical replay QC gap audit; searches substitute QC/retry evidence without opening COM or changing fit eligibility")
    elif lower == "export_v1_5_algorithm_formal_point_plan_guard":
        notes.append("offline algorithm formal point-plan guard; validates legacy 45/13 and new algorithm 47/14 without opening COM or changing runners")
    elif lower == "export_v1_5_algorithm_formal_runlist_preview":
        notes.append("offline algorithm formal runlist preview; emits queue-compatible 47/14 CSV artifacts without opening COM or changing runners")
    elif lower == "export_v1_5_algorithm_runlist_readiness":
        notes.append("offline algorithm runlist readiness gate; blocks incomplete 47/14 previews before any runner integration")
    elif lower == "export_v1_5_algorithm_runner_integration_dry_run":
        notes.append("offline algorithm runner integration dry-run; plans queue invocations without executing formal runners")
    elif lower == "export_v1_5_algorithm_profile_runner_dry_run":
        notes.append("offline algorithm profile runner dry-run; bundles runlist, readiness, and runner dry-run evidence without executing formal runners")
    elif lower == "export_v1_5_algorithm_queue_handoff_preflight":
        notes.append("offline algorithm queue handoff preflight; requires dry-run/no-prompt evidence before any future live queue wiring")
    elif lower == "export_v1_5_algorithm_mature_queue_inputs":
        notes.append("offline profile queue materializer; emits immutable 45/13 or 47/14 inputs for the mature V1.5 CO2/H2O queues without executing them")
    elif lower == "export_v1_5_algorithm_profile_lineage_gate":
        notes.append("offline algorithm-profile lineage gate; binds bootstrap and queue hashes to legacy R or absorption A/R0(T) fit semantics without executing hardware")
    elif lower == "export_v1_5_new_algorithm_mature_queue_live_handoff":
        notes.append("offline new-algorithm 47/14 live-handoff contract; binds profile, queue, mature-runner hashes, fit semantics, and future authorization while live execution stays blocked")
    elif lower == "run_v1_5_new_algorithm_mature_queue_live_handoff_blocked_executor":
        notes.append("offline new-algorithm mature-queue blocked executor; refuses live queue, COM, route, authorization, device, write, and database inputs")
    elif lower == "export_v1_5_historical_fit_profile_parity":
        notes.append("offline historical fitting parity replay; enforces 0613 fitting plus 0620/0621 route baselines and legacy R versus absorption A/R0(T) without executing hardware")
    elif lower == "export_v1_5_historical_fit_evidence_normalizer":
        notes.append("offline historical fit evidence normalizer; extracts ratio, chamber T1, pressure, dewpoint, and component-matched QC without opening COM or fitting coefficients")
    elif lower == "export_v1_5_historical_route_attestation_binder":
        notes.append("offline historical mature-root attestation binder; binds exact queue, sidecar, sample, QC, and 0613/0620/0621 contract hashes without opening COM")
    elif lower == "export_v1_5_historical_mature_root_discovery":
        notes.append("offline historical mature-root discovery; ranks exact queue candidates without promoting them to fitting or acceptance evidence")
    elif lower == "export_v1_5_legacy_historical_evidence_catalog":
        notes.append("offline legacy historical evidence catalog; hashes segmented, retry, recovery, and accepted-composite point evidence without promoting it")
    elif lower == "export_v1_5_legacy_evidence_gap_task_plan":
        notes.append("offline legacy evidence-gap task plan; revalidates cataloged artifact hashes and schedules manual QC/traceability review without repairing or promoting evidence")
    elif lower == "export_v1_5_p1_evidence_lineage_audit":
        notes.append("offline P1 evidence lineage audit; searches only bounded same-run siblings for retry evidence without copying files, deriving QC, or binding cross-run data")
    elif lower == "export_v1_5_p2_qc_derivation_design":
        notes.append("offline P2 component-QC derivation design; validates same-point input structure while keeping QC generation and fit promotion blocked until a reviewed mature generator exists")
    elif lower == "export_v1_5_component_qc_authority_audit":
        notes.append("offline component-QC authority audit; separates mature pre-sample stability gates from the untracked 0624/migration writer and keeps QC backfill blocked")
    elif lower == "export_v1_5_component_qc_generator_contract":
        notes.append("offline component-QC generator contract review; fixes per-analyzer physical grading semantics while keeping implementation and backfill disabled")
    elif lower == "export_v1_5_component_qc_reference_evaluator":
        notes.append("offline synthetic-only component-QC reference evaluator; exercises the reviewed per-analyzer contract without historical writes, fitting, COM, or production promotion")
    elif lower == "export_v1_5_production_component_qc_fit_matrix":
        notes.append("offline production-semantics component-QC evaluator and canonical 0613 no-write strategy matrix; reads immutable point evidence without backfilling history or executing fits")
    elif lower == "export_v1_5_unified_controlled_write_reverify":
        notes.append("offline unified S1-S9/SENCOA-B write, GETCO readback, rollback, and independent short-reverify contract; never executes a writer or route")
    elif lower == "export_v1_5_historical_component_qc_generator_preflight":
        notes.append("offline historical component-QC generator preflight; revalidates P2 source hashes and overwrite boundaries while keeping generation, backfill, fitting, COM, and production promotion locked")
    elif lower == "export_v1_5_historical_component_qc_blocked_generator_plan":
        notes.append("offline historical component-QC blocked generator plan; revalidates the exact preflight and emits only a no-evaluation, no-write, no-overwrite preview")
    elif lower == "export_v1_5_historical_component_qc_controlled_writer_design":
        notes.append("offline historical component-QC controlled-writer design; records future authorization, exclusive-create, readback, and compensating-rollback contracts while keeping evaluator and writer absent")
    elif lower == "export_v1_5_automation_control_contract":
        notes.append("offline V1.5 automation control contract; keeps automation as a shell around the 0613/0620/0621 mature core")
    elif lower == "export_v1_5_full_flow_automation_closure":
        notes.append("offline V1.5 full-flow automation closure map; records mature baseline, remaining automation gaps, and forbidden formal surfaces without executing hardware")
    elif lower == "export_v1_5_full_flow_next_action_plan":
        notes.append("offline V1.5 full-flow next-action plan; ranks remaining automation handoffs without executing hardware, writes, routes, or database imports")
    elif lower == "export_v1_5_final_production_gap_freeze":
        notes.append("offline V1.5 final production-gap freeze; supersedes stale gap snapshots and fixes the remaining production scope without executing hardware, writes, routes, or database imports")
    elif lower == "export_v1_5_legacy_full_flow_offline_replay":
        notes.append("offline V1.5 legacy full-flow offline replay; walks initialization through archive from historical evidence without promoting segmented data or executing hardware")
    elif lower == "export_v1_5_mature_route_continuity_gate":
        notes.append("offline V1.5 mature route continuity gate; blocks segmented, retry, direct-recovery, 0624/migration, diagnostic, worker, and empty manifest route evidence from formal fitting")
    elif lower == "export_v1_5_batch_initialization_closeout_index":
        notes.append("offline V1.5 batch initialization closeout index; binds SN/device_code, GETCO, S5-S8, S9, and route evidence without opening COM")
    elif lower == "export_v1_5_post_closeout_resume_gate":
        notes.append("offline V1.5 post-closeout resume gate; binds a reviewed completed-step prefix to exact plan and closeout hashes without applying or executing it")
    elif lower == "export_v1_5_pressure_s9_readiness_index":
        notes.append("offline V1.5 pressure/SENCO9 readiness index; separates offset-only S9 and linear controlled exceptions without opening COM or writing coefficients")
    elif lower == "export_v1_5_production_entrypoint_map":
        notes.append("offline V1.5 production entrypoint map; separates formal launchers, workers, diagnostics, controlled writes, and forbidden surfaces")
    elif lower == "export_v1_5_production_entrypoint_gate":
        notes.append("offline V1.5 production entrypoint gate; blocks _handoff, root migration, 0624, diagnostic, worker, V1, and V2 references in formal plans")
    elif lower == "export_v1_5_route_physical_recovery_evidence_packet":
        notes.append("offline V1.5 route physical recovery evidence packet validator; checks dry-gas, PACE vent, pressure INL, fresh queue, and no-write boundaries before recovery readiness")
    elif lower == "export_v1_5_route_physical_recovery_evidence_packet_template":
        notes.append("offline V1.5 route physical recovery evidence packet template; prepares dry-gas, PACE vent, pressure INL, and fresh queue evidence fields without collecting live data")
    elif lower == "export_v1_5_route_physical_recovery_evidence_binder":
        notes.append("offline V1.5 route physical recovery evidence binder; converts reviewed trace files into a recovery packet without collecting live data")
    elif "formal_evidence_sidecar" in lower or "formal_offline_review_chain" in lower:
        notes.append("offline review/evidence sidecar; no COM or route control")
    elif "diagnostic" in lower or "probe" in lower or "tune" in lower:
        notes.append("diagnostic evidence only; not formal acceptance by default")
    if "sealed" in lower or "dynamic_pressure" in lower or "no_outp" in lower:
        notes.append("pressure/route engineering probe; keep outside formal CO2/H2O fit")
    if (
        "controlled_write" in lower or "rollback" in lower
    ) and lower != "export_v1_5_authoritative_resume_state_controlled_write_preflight":
        notes.append("requires explicit coefficient-write authorization and readback evidence")
    if lower in CANONICAL_FORMAL_WORKER_TOOL_NAMES:
        notes.append("canonical per-point sampling worker; invoked by the formal CO2/H2O queue runners")
    elif "formal" in lower and "run_" in lower and any(
        token in lower for token in ("open_flow_sampling", "open_flow_queue", "h2o_open_flow", "co2_open_flow")
    ):
        notes.append("formal V1.5 entrypoint candidate; still requires operator authorization for real COM")
    if "serial_port" in lower:
        notes.append("COM port is transport only; analyzer identity remains MODE2 device ID")
    return notes


def classify_v1_5_entrypoint(path: Path, *, root: Path | None = None) -> V15Entrypoint:
    root = root or Path.cwd()
    rel_path = _rel(path, root)
    name = path.stem
    parts = set(path.parts)
    lower = name.lower()
    rel_lower = rel_path.lower()

    if "tests" in parts:
        artifact_type = "test"
        category = "test_gate"
        formal_status = "verification_only"
        risk_level = "none"
        opens_com_ports = False
        controls_routes = False
        writes_coefficients = False
    elif "\\storage\\" in str(path).lower() or "/storage/" in rel_lower:
        artifact_type = "storage"
        category = "evidence_database"
        formal_status = "formal_support"
        risk_level = "offline"
        opens_com_ports = False
        controls_routes = False
        writes_coefficients = False
    elif "\\v1_5\\" in str(path).lower() or "/v1_5/" in rel_lower:
        artifact_type = "library"
        if "qc_advanced" in rel_lower:
            category = "advanced_qc"
            formal_status = "formal_support"
        elif "orchestration" in rel_lower:
            category = "full_flow_orchestration"
            formal_status = "formal_support"
        elif "ui" in rel_lower or "review_surface" in lower:
            category = "ui_review"
            formal_status = "prototype_or_review_surface"
        elif "parameters" in rel_lower:
            category = "parameter_governance"
            formal_status = "formal_support"
        else:
            category = "v1_5_library"
            formal_status = "formal_support"
        risk_level = "offline"
        opens_com_ports = False
        controls_routes = False
        writes_coefficients = False
    else:
        artifact_type = "tool"
        controls_routes = any(
            token in lower for token in ("open_flow_sampling", "open_flow_queue", "h2o_open_flow", "co2_open_flow")
        )
        writes_coefficients = any(token in lower for token in ("controlled_write", "rollback", "neutral_controlled"))
        opens_com_ports = lower.startswith("run_v1_5_") or lower.startswith("probe_v1_5_")

        if lower in LEGACY_V1_REFERENCE_TOOL_NAMES:
            category = "legacy_v1_reference"
            formal_status = "legacy_v1_reference_only"
            risk_level = "legacy_write_or_acceptance_risk"
            opens_com_ports = lower.startswith("run_v1_")
            controls_routes = False
            writes_coefficients = lower in {
                "run_v1_corrected_autodelivery",
                "run_v1_merged_calibration_sidecar",
                "run_v1_online_acceptance",
            }
        elif lower == "validate_pressure_only":
            category = "formal_pressure_no_write_runner"
            formal_status = "formal_pressure_no_write_when_authorized"
            risk_level = "real_com_or_route_risk"
            opens_com_ports = True
            controls_routes = True
            writes_coefficients = False
        elif lower == "export_single_gas_pressure_curve":
            category = "diagnostic_only"
            formal_status = "diagnostic_only"
            risk_level = "offline"
            opens_com_ports = False
            controls_routes = False
            writes_coefficients = False
        elif lower == "run_room_temp_co2_pressure_diagnostic":
            category = "diagnostic_only"
            formal_status = "diagnostic_only"
            risk_level = "real_com_or_route_risk"
            opens_com_ports = True
            controls_routes = True
            writes_coefficients = False
        elif lower == "run_v1_5_full_calibration_chain":
            category = "full_flow_orchestration"
            formal_status = "formal_support"
            risk_level = "offline"
            opens_com_ports = False
            controls_routes = False
        elif lower == "run_v1_5_formal_initialization_runner":
            category = "full_flow_orchestration"
            formal_status = "canonical_initialization_planner"
            risk_level = "offline"
            opens_com_ports = False
            controls_routes = False
            writes_coefficients = False
        elif lower == "run_v1_5_authoritative_resume_state_atomic_writer":
            category = "controlled_state_writer"
            formal_status = "manual_authorized_only"
            risk_level = "state_file_write_risk"
            opens_com_ports = False
            controls_routes = False
            writes_coefficients = False
        elif lower == "run_v1_5_authoritative_resume_offline_state_advance_atomic_writer":
            category = "controlled_state_writer"
            formal_status = "manual_authorized_only"
            risk_level = "state_file_write_risk"
            opens_com_ports = False
            controls_routes = False
            writes_coefficients = False
        elif lower == "run_v1_5_authoritative_resume_offline_executor":
            category = "full_flow_orchestration"
            formal_status = "manual_authorized_offline_resume_only"
            risk_level = "offline_subprocess_risk"
            opens_com_ports = False
            controls_routes = False
            writes_coefficients = False
        elif lower == "run_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor":
            category = "full_flow_orchestration"
            formal_status = "manual_authorized_single_step_resume_only"
            risk_level = "real_com_or_route_or_write_risk"
            opens_com_ports = True
            controls_routes = True
            writes_coefficients = True
        elif lower == "run_v1_5_authoritative_resume_offline_state_advance_next_step_operator_bundle":
            category = "full_flow_orchestration"
            formal_status = "manual_authorized_single_step_resume_only"
            risk_level = "real_com_or_route_or_write_risk"
            opens_com_ports = True
            controls_routes = True
            writes_coefficients = True
        elif lower == "run_v1_5_new_run_bootstrap":
            category = "full_flow_orchestration"
            formal_status = "manual_new_batch_bootstrap_no_com"
            risk_level = "state_file_write_risk"
            opens_com_ports = False
            controls_routes = False
            writes_coefficients = False
        elif lower in {
            "run_v1_5_formal_initialization_blocked_executor",
            "run_v1_5_formal_initialization_readonly_com_preflight_blocked_executor",
            "run_v1_5_formal_initialization_readonly_com_preflight_controlled_blocked_executor",
            "export_v1_5_formal_readonly_com_execution_contract",
            "run_v1_5_formal_readonly_com_execution_blocked_executor",
            "run_v1_5_authoritative_resume_state_writer_blocked_executor",
            "export_v1_5_authoritative_resume_state_controlled_write_preflight",
            "export_v1_5_authoritative_resume_state_post_write_verification",
            "export_v1_5_authoritative_resume_state_consumer_contract",
            "export_v1_5_authoritative_resume_executor_plan_preview",
            "run_v1_5_authoritative_resume_executor_blocked",
            "export_v1_5_authoritative_resume_executor_controlled_design",
            "export_v1_5_authoritative_resume_executor_authorization_validator",
            "export_v1_5_authoritative_resume_execution_preflight",
            "export_v1_5_authoritative_resume_offline_candidate_gate",
            "export_v1_5_authoritative_resume_offline_post_execution_verifier",
            "export_v1_5_authoritative_resume_offline_state_advance_preflight",
            "export_v1_5_authoritative_resume_offline_state_advance_authorization",
            "run_v1_5_authoritative_resume_offline_state_advance_blocked_executor",
            "export_v1_5_authoritative_resume_offline_state_advance_post_write_verification",
            "export_v1_5_authoritative_resume_offline_state_advance_consumer_readiness",
            "export_v1_5_authoritative_resume_offline_state_advance_next_step_plan",
            "export_v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight",
            "run_v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor",
            "export_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design",
            "export_v1_5_authoritative_resume_offline_state_advance_next_step_execution_authorization",
            "export_v1_5_authoritative_resume_offline_state_advance_next_step_execution_preflight",
            "export_v1_5_formal_readonly_com_execution_packet_validator",
            "export_v1_5_formal_readonly_com_execution_plan_preview",
            "export_v1_5_formal_readonly_com_minimal_executor_review",
            "run_v1_5_formal_readonly_com_minimal_executor_stub",
            "export_v1_5_automation_control_contract",
            "export_v1_5_algorithm_mature_queue_inputs",
            "export_v1_5_algorithm_profile_lineage_gate",
            "export_v1_5_new_algorithm_mature_queue_live_handoff",
            "run_v1_5_new_algorithm_mature_queue_live_handoff_blocked_executor",
            "export_v1_5_full_flow_automation_closure",
            "export_v1_5_production_entrypoint_gate",
            "export_v1_5_route_physical_recovery_evidence_packet",
            "export_v1_5_route_physical_recovery_evidence_packet_template",
            "export_v1_5_route_physical_recovery_evidence_binder",
            "export_v1_5_mature_route_continuity_gate",
            "export_v1_5_historical_route_attestation_binder",
            "export_v1_5_historical_mature_root_discovery",
            "export_v1_5_legacy_historical_evidence_catalog",
            "export_v1_5_legacy_evidence_gap_task_plan",
            "export_v1_5_p1_evidence_lineage_audit",
            "export_v1_5_p2_qc_derivation_design",
            "export_v1_5_component_qc_authority_audit",
            "export_v1_5_component_qc_generator_contract",
            "export_v1_5_component_qc_reference_evaluator",
            "export_v1_5_production_component_qc_fit_matrix",
            "export_v1_5_unified_controlled_write_reverify",
            "export_v1_5_historical_component_qc_generator_preflight",
            "export_v1_5_historical_component_qc_blocked_generator_plan",
            "export_v1_5_historical_component_qc_controlled_writer_design",
        }:
            category = "formal_review_evidence"
            formal_status = "formal_support"
            risk_level = "offline"
            opens_com_ports = False
            controls_routes = False
            writes_coefficients = False
        elif lower == "run_v1_5_formal_readonly_com_minimal_executor":
            category = "formal_review_evidence"
            formal_status = "manual_authorized_read_only_com_support"
            risk_level = "real_com_read_only_no_write_risk"
            opens_com_ports = True
            controls_routes = False
            writes_coefficients = False
        elif lower in FORMAL_INITIALIZATION_SUPPORT_TOOL_NAMES:
            category = "identity_and_serial_binding"
            formal_status = "formal_initialization_support"
            risk_level = "real_com_or_database_risk" if lower != "run_v1_5_initialization_db_preflight" else "offline"
            opens_com_ports = lower in {
                "run_v1_5_analyzer_runtime_setup",
                "run_v1_5_sn_identity_initialization",
            }
            controls_routes = False
            writes_coefficients = False
        elif lower in FORMAL_PREFLIGHT_SUPPORT_TOOL_NAMES:
            category = "formal_review_evidence"
            formal_status = "formal_preflight_support"
            risk_level = "real_com_or_route_risk"
            opens_com_ports = True
            controls_routes = True
            writes_coefficients = False
        elif lower == "run_v1_5_temperature_current_point_review":
            category = "controlled_write"
            formal_status = "formal_but_manual_authorized"
            risk_level = "writes_device_coefficients"
            opens_com_ports = True
            controls_routes = False
            writes_coefficients = True
        elif "formal_evidence_sidecar" in lower or "formal_offline_review_chain" in lower:
            category = "formal_review_evidence"
            formal_status = "formal_support"
            risk_level = "offline"
            opens_com_ports = False
            controls_routes = False
        elif "formal_archive_closure" in lower:
            category = "formal_review_evidence"
            formal_status = "formal_support"
            risk_level = "offline"
            opens_com_ports = False
            controls_routes = False
        elif lower.startswith("archive_v1_5_"):
            category = "housekeeping_archive"
            formal_status = "formal_support"
            risk_level = "offline"
            opens_com_ports = False
        elif writes_coefficients:
            category = "controlled_write"
            formal_status = "formal_but_manual_authorized"
            risk_level = "writes_device_coefficients"
        elif lower in CANONICAL_FORMAL_WORKER_TOOL_NAMES:
            category = "formal_sampling_worker"
            formal_status = "canonical_queue_worker"
            risk_level = "real_com_or_route_risk"
            opens_com_ports = True
            controls_routes = True
            writes_coefficients = False
        elif lower == "probe_v1_5_getco_component_snapshot":
            category = "formal_review_evidence"
            formal_status = "formal_support"
            risk_level = "real_com_or_route_risk"
        elif any(
            token in lower
            for token in ("diagnostic", "probe", "sealed", "dynamic_pressure", "no_outp", "tune", "extended_hold")
        ):
            category = "diagnostic_only"
            formal_status = "diagnostic_only"
            risk_level = "real_com_or_route_risk" if opens_com_ports else "offline"
        elif lower.startswith("run_v1_5_formal_") or lower == "run_v1_5_full_calibration_chain":
            category = "formal_runner"
            formal_status = "formal_entry_candidate"
            risk_level = "real_com_or_route_risk" if opens_com_ports or controls_routes else "offline"
        elif lower.startswith(
            (
                "prepare_v1_5_",
                "export_v1_5_",
                "import_v1_5_",
                "query_v1_5_",
                "migrate_v1_5_",
                "summarize_v1_5_",
                "verify_v1_5_",
                "build_v1_5_",
            )
        ):
            category = "formal_review_evidence"
            formal_status = "formal_support"
            risk_level = "offline"
            opens_com_ports = False
        elif lower.startswith("collect_v1_5_"):
            category = "identity_and_serial_binding"
            formal_status = "formal_support"
            risk_level = "offline"
            opens_com_ports = False
        else:
            category = "unclassified_v1_5_tool"
            formal_status = "needs_review"
            risk_level = "unknown"

    return V15Entrypoint(
        path=rel_path,
        name=name,
        artifact_type=artifact_type,
        category=category,
        stage=_stage_from_name(name),
        formal_status=formal_status,
        risk_level=risk_level,
        opens_com_ports=opens_com_ports,
        controls_routes=controls_routes,
        writes_coefficients=writes_coefficients,
        notes=tuple(_notes_for_name(name)),
    )


def discover_v1_5_entrypoints(root: Path) -> list[V15Entrypoint]:
    paths: list[Path] = []
    for base in ("src/gas_calibrator/tools", "src/gas_calibrator/v1_5", "src/gas_calibrator/storage", "tests"):
        folder = root / base
        if not folder.exists():
            continue
        for path in folder.rglob("*.py"):
            rel = _rel(path, root)
            name = path.stem
            if base == "tests" and not name.startswith("test_v1_5"):
                continue
            if base.endswith("tools") and (
                not name.startswith(V1_5_TOOL_PREFIXES)
                and name not in EXTRA_V1_5_REVIEW_TOOL_NAMES
                and name not in EXTRA_V1_5_DIAGNOSTIC_TOOL_NAMES
                and name not in LEGACY_V1_REFERENCE_TOOL_NAMES
            ):
                continue
            if base.endswith("storage") and "v1_5_evidence" not in rel:
                continue
            paths.append(path)
    return [classify_v1_5_entrypoint(path, root=root) for path in sorted(paths)]


def summarize_entrypoints(entries: Iterable[V15Entrypoint]) -> dict[str, int]:
    summary: dict[str, int] = {}
    for entry in entries:
        summary[entry.category] = summary.get(entry.category, 0) + 1
    return dict(sorted(summary.items()))


def guardrailed_entrypoint_rows(entries: Iterable[V15Entrypoint]) -> list[dict[str, str]]:
    canonical_paths = {item["entrypoint"] for item in CANONICAL_FORMAL_PATH}
    rows: list[dict[str, str]] = []
    for entry in entries:
        rule_key = entry.category
        if entry.category == "formal_runner" and entry.path not in canonical_paths:
            rule_key = "noncanonical_formal_runner"
        rule = NON_START_HERE_GUARDRAILS.get(rule_key)
        if not rule:
            continue
        rows.append(
            {
                "path": entry.path,
                "name": entry.name,
                "category": entry.category,
                "stage": entry.stage,
                "risk_level": entry.risk_level,
                "guardrail": rule["guardrail"],
                "allowed_use": rule["allowed_use"],
            }
        )
    return sorted(rows, key=lambda item: (item["guardrail"], item["stage"], item["path"]))


def validate_v1_5_canonical_formal_path_contract(
    root: Path,
    *,
    entries: Iterable[V15Entrypoint] | None = None,
) -> list[V15ActiveSurfacePolicyIssue]:
    """Validate the fixed V1.5 formal-stage contract.

    This catches a different class of mistake from the active-surface policy:
    the canonical path itself must stay ordered, unique, and limited to formal
    stage owners. Support tools such as SN identity, runtime setup, readiness
    probes, and per-point workers remain callable by their owning stages, but
    must not drift into the top-level stage list.
    """

    entry_list = list(entries) if entries is not None else discover_v1_5_entrypoints(root)
    by_path = {entry.path: entry for entry in entry_list}
    issues: list[V15ActiveSurfacePolicyIssue] = []
    stages = [str(item.get("stage") or "") for item in CANONICAL_FORMAL_PATH]
    paths = [str(item.get("entrypoint") or "").replace("\\", "/") for item in CANONICAL_FORMAL_PATH]

    if tuple(stages) != CANONICAL_FORMAL_STAGE_ORDER:
        issues.append(
            V15ActiveSurfacePolicyIssue(
                severity="blocker",
                rule="canonical_stage_order_changed",
                path="CANONICAL_FORMAL_PATH",
                message="Canonical V1.5 stages must keep the documented 00-10 order unless the formal flow contract is deliberately revised.",
            )
        )
    if len(set(stages)) != len(stages):
        issues.append(
            V15ActiveSurfacePolicyIssue(
                severity="blocker",
                rule="canonical_stage_duplicate",
                path="CANONICAL_FORMAL_PATH",
                message="Canonical V1.5 stages must be unique.",
            )
        )
    if len(set(paths)) != len(paths):
        issues.append(
            V15ActiveSurfacePolicyIssue(
                severity="blocker",
                rule="canonical_entrypoint_duplicate",
                path="CANONICAL_FORMAL_PATH",
                message="Canonical V1.5 entrypoints must be unique so one tool cannot silently own two physical stages.",
            )
        )

    support_names = set(CANONICAL_FORMAL_SUPPORT_TOOL_NAMES)
    route_stages = {"04_co2_open_flow_sampling", "05_h2o_open_flow_sampling"}
    for item in CANONICAL_FORMAL_PATH:
        stage = str(item.get("stage") or "")
        canonical_path = str(item.get("entrypoint") or "").replace("\\", "/")
        name = Path(canonical_path).stem
        required = {"stage", "entrypoint", "category", "status", "physical_meaning", "safety_boundary"}
        missing_keys = sorted(required.difference(item))
        if missing_keys:
            issues.append(
                V15ActiveSurfacePolicyIssue(
                    severity="blocker",
                    rule="canonical_stage_missing_metadata",
                    path=canonical_path or "CANONICAL_FORMAL_PATH",
                    message=f"Canonical stage {stage!r} is missing required metadata: {', '.join(missing_keys)}.",
                )
            )
        if name in support_names:
            issues.append(
                V15ActiveSurfacePolicyIssue(
                    severity="blocker",
                    rule="canonical_uses_support_tool_as_stage_owner",
                    path=canonical_path,
                    message="SN/runtime/readiness probes and sampling workers must stay subordinate to their formal owner, not become top-level canonical stages.",
                )
            )

        entry = by_path.get(canonical_path)
        if entry is None:
            issues.append(
                V15ActiveSurfacePolicyIssue(
                    severity="blocker",
                    rule="canonical_entrypoint_missing",
                    path=canonical_path,
                    message="Canonical V1.5 entrypoint is missing from the discovered inventory.",
                )
            )
            continue
        if entry.controls_routes and stage not in route_stages:
            issues.append(
                V15ActiveSurfacePolicyIssue(
                    severity="blocker",
                    rule="canonical_route_control_outside_route_stage",
                    path=canonical_path,
                    message="Only the formal CO2/H2O queue stages may control gas/water routes in the canonical path.",
                )
            )
        if entry.writes_coefficients and stage != "08_controlled_write":
            issues.append(
                V15ActiveSurfacePolicyIssue(
                    severity="blocker",
                    rule="canonical_write_outside_controlled_write_stage",
                    path=canonical_path,
                    message="Coefficient-writing entrypoints must appear only at the controlled-write stage.",
                )
            )
        if stage == "08_controlled_write" and str(item.get("status") or "") != "manual_authorized_only":
            issues.append(
                V15ActiveSurfacePolicyIssue(
                    severity="blocker",
                    rule="controlled_write_stage_not_manual_authorized",
                    path=canonical_path,
                    message="The controlled-write canonical stage must remain manual_authorized_only.",
                )
            )

    return issues


def validate_v1_5_active_surface_policy(
    root: Path,
    *,
    entries: Iterable[V15Entrypoint] | None = None,
) -> list[V15ActiveSurfacePolicyIssue]:
    """Check that the formal V1.5 backbone does not point at isolated surfaces.

    This is an offline navigation guard. It does not say whether legacy or
    diagnostic files may remain in the repository; it only prevents those files
    from becoming the default V1.5 formal path by accident.
    """

    entry_list = list(entries) if entries is not None else discover_v1_5_entrypoints(root)
    by_path = {entry.path: entry for entry in entry_list}
    issues: list[V15ActiveSurfacePolicyIssue] = []
    blocked_prefixes = (
        "src/gas_calibrator/v2/",
        "tests/v2/",
        "configs/",
    )
    blocked_categories = {
        "diagnostic_only",
        "legacy_v1_reference",
        "unclassified_v1_5_tool",
        "housekeeping_archive",
    }

    for item in CANONICAL_FORMAL_PATH:
        canonical_path = item["entrypoint"].replace("\\", "/")
        if canonical_path == "run_app.py" or canonical_path.endswith("/run_app.py"):
            issues.append(
                V15ActiveSurfacePolicyIssue(
                    severity="blocker",
                    rule="run_app_not_v1_5_canonical",
                    path=canonical_path,
                    message="run_app.py must remain outside the V1.5 canonical formal path.",
                )
            )
        if canonical_path.startswith(blocked_prefixes):
            issues.append(
                V15ActiveSurfacePolicyIssue(
                    severity="blocker",
                    rule="canonical_path_in_isolated_surface",
                    path=canonical_path,
                    message="Canonical V1.5 stages must not point into V2, tests/v2, or runtime config files.",
                )
            )

        entry = by_path.get(canonical_path)
        if entry is None:
            issues.append(
                V15ActiveSurfacePolicyIssue(
                    severity="blocker",
                    rule="canonical_entrypoint_missing",
                    path=canonical_path,
                    message="Canonical V1.5 entrypoint is missing from the discovered inventory.",
                )
            )
            continue
        if entry.category != item["category"]:
            issues.append(
                V15ActiveSurfacePolicyIssue(
                    severity="blocker",
                    rule="canonical_category_mismatch",
                    path=canonical_path,
                    message=f"Canonical stage expects {item['category']} but inventory classified {entry.category}.",
                )
            )
        if entry.category in blocked_categories:
            issues.append(
                V15ActiveSurfacePolicyIssue(
                    severity="blocker",
                    rule="canonical_entrypoint_blocked_category",
                    path=canonical_path,
                    message="Canonical V1.5 stages must not use diagnostic, legacy V1, unclassified, or housekeeping entries.",
                )
            )

    canonical_paths = {item["entrypoint"] for item in CANONICAL_FORMAL_PATH}
    for entry in entry_list:
        if entry.category == "formal_runner" and entry.path not in canonical_paths:
            issues.append(
                V15ActiveSurfacePolicyIssue(
                    severity="review",
                    rule="noncanonical_formal_runner",
                    path=entry.path,
                    message="Route/COM-capable V1.5 runner exists outside the canonical path and requires human review before formal use.",
                )
            )

    return issues


def _reference_audit_files(root: Path) -> list[Path]:
    files: list[Path] = []
    source_root = root / "src/gas_calibrator"
    if not source_root.exists():
        return files
    excluded = {
        "src/gas_calibrator/validation/v1_5_entrypoint_inventory.py",
        "src/gas_calibrator/tools/export_v1_5_entrypoint_inventory.py",
    }
    for path in source_root.rglob("*.py"):
        rel = _rel(path, root)
        if "__pycache__" in path.parts or rel in excluded:
            continue
        files.append(path)
    return sorted(files)


def _reference_category_for_path(path: str, by_path: dict[str, V15Entrypoint]) -> str:
    entry = by_path.get(path)
    if entry is not None:
        return entry.category
    if path.startswith("src/gas_calibrator/v1_5/"):
        return "active_v1_5_library"
    if path.startswith("src/gas_calibrator/workflow/"):
        return "legacy_workflow_surface"
    if path.startswith("src/gas_calibrator/validation/"):
        return "validation_or_audit_support"
    if path.startswith("src/gas_calibrator/tools/"):
        return "non_inventory_tool"
    return "source_support"


def _reference_severity(
    *,
    isolated_category: str,
    reference_category: str,
    reference_path: str,
) -> tuple[str, str]:
    formal_runtime_categories = {
        "formal_runner",
        "formal_sampling_worker",
        "full_flow_orchestration",
        "active_v1_5_library",
    }
    if reference_category in formal_runtime_categories:
        return (
            "blocker",
            "A V1.5 formal runtime surface references an isolated entrypoint; reclassify the dependency or remove the runtime reference before using the formal flow.",
        )
    if reference_category == "controlled_write":
        return (
            "review",
            "A controlled-write tool references an isolated entrypoint; confirm this is only for rollback/audit support and not hidden runtime execution.",
        )
    if reference_category in {"validation_or_audit_support", "formal_review_evidence"}:
        return (
            "review",
            "Offline validation/review code references an isolated entrypoint as evidence or historical context; keep it out of runtime flow.",
        )
    if reference_category == "legacy_workflow_surface" or isolated_category == "legacy_v1_reference":
        return (
            "review",
            "Legacy workflow or V1 reference remains linked for historical/V1 fallback code; do not delete without a separate migration plan.",
        )
    if reference_path.startswith("src/gas_calibrator/tools/"):
        return (
            "review",
            "A tool references an isolated entrypoint; confirm the tool is not part of the canonical V1.5 run chain.",
        )
    return (
        "review",
        "Source code references an isolated entrypoint; inspect before archiving or deleting.",
    )


def audit_v1_5_isolated_reference_integrity(
    root: Path,
    *,
    entries: Iterable[V15Entrypoint] | None = None,
) -> list[V15IsolationReferenceIssue]:
    """Find isolated V1.5/V1 tools that are still referenced by source code.

    This is a guard against false cleanup. A diagnostic or legacy file may be
    correctly isolated as a top-level start point, but if formal runtime code
    imports or names it, the classification must be reviewed before archiving.
    """

    entry_list = list(entries) if entries is not None else discover_v1_5_entrypoints(root)
    by_path = {entry.path: entry for entry in entry_list}
    isolated_categories = {
        "diagnostic_only",
        "legacy_v1_reference",
        "housekeeping_archive",
        "unclassified_v1_5_tool",
    }
    isolated_entries = [entry for entry in entry_list if entry.category in isolated_categories]
    issues: list[V15IsolationReferenceIssue] = []
    seen: set[tuple[str, str, str]] = set()

    for source_path in _reference_audit_files(root):
        rel_source = _rel(source_path, root)
        try:
            text = source_path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            text = source_path.read_text(encoding="utf-8", errors="ignore")
        reference_category = _reference_category_for_path(rel_source, by_path)
        for isolated in isolated_entries:
            if rel_source == isolated.path:
                continue
            module_name = f"gas_calibrator.tools.{isolated.name}"
            tokens = (module_name, isolated.path, isolated.name)
            matched = next((token for token in tokens if token and token in text), None)
            if matched is None:
                continue
            key = (isolated.path, rel_source, matched)
            if key in seen:
                continue
            seen.add(key)
            severity, message = _reference_severity(
                isolated_category=isolated.category,
                reference_category=reference_category,
                reference_path=rel_source,
            )
            issues.append(
                V15IsolationReferenceIssue(
                    severity=severity,
                    isolated_path=isolated.path,
                    isolated_category=isolated.category,
                    reference_path=rel_source,
                    reference_category=reference_category,
                    matched_token=matched,
                    message=message,
                )
            )

    return sorted(
        issues,
        key=lambda issue: (
            0 if issue.severity == "blocker" else 1,
            issue.isolated_category,
            issue.isolated_path,
            issue.reference_path,
        ),
    )


def _is_inventory_file(path: Path) -> bool:
    return (
        path.is_file()
        and "__pycache__" not in path.parts
        and path.suffix.lower() not in {".pyc", ".pyo"}
    )


def _existing_files_under(path: Path) -> list[Path]:
    if not path.exists():
        return []
    if path.is_file():
        return [path] if _is_inventory_file(path) else []
    return sorted(item for item in path.rglob("*") if _is_inventory_file(item))


def _glob_existing_files(root: Path, patterns: Iterable[str]) -> list[Path]:
    files: list[Path] = []
    seen: set[Path] = set()
    for pattern in patterns:
        for path in root.glob(pattern):
            if _is_inventory_file(path) and path not in seen:
                seen.add(path)
                files.append(path)
    return sorted(files)


def _surface_row(
    *,
    root: Path,
    surface: str,
    path: str,
    status: str,
    action: str,
    files: Iterable[Path],
    reason: str,
) -> V15WorkspaceSurfaceRow:
    file_list = sorted(files)
    return V15WorkspaceSurfaceRow(
        surface=surface,
        path=path,
        status=status,
        action=action,
        file_count=len(file_list),
        examples=tuple(_rel(item, root) for item in file_list[:8]),
        reason=reason,
    )


def build_v1_5_workspace_surface_rows(root: Path) -> list[V15WorkspaceSurfaceRow]:
    """Summarize what is inside or outside the active V1.5 work surface.

    This is intentionally a navigation and cleanup report, not a delete plan.
    V2 and legacy V1 trees may still contain useful historical algorithms, but
    they must not be treated as active V1.5 formal entrypoints.
    """

    rows: list[V15WorkspaceSurfaceRow] = []
    entries = discover_v1_5_entrypoints(root)

    v2_source = _existing_files_under(root / "src/gas_calibrator/v2")
    if v2_source:
        rows.append(
            _surface_row(
                root=root,
                surface="legacy_v2_source_tree",
                path="src/gas_calibrator/v2",
                status="legacy_reference_only",
                action="exclude_from_v1_5_active_surface",
                files=v2_source,
                reason="V1.5 is the final production direction; V2 source stays as archived reference unless explicitly revived.",
            )
        )

    v2_tests = _existing_files_under(root / "tests/v2")
    if v2_tests:
        rows.append(
            _surface_row(
                root=root,
                surface="legacy_v2_tests",
                path="tests/v2",
                status="legacy_reference_only",
                action="exclude_from_v1_5_active_surface",
                files=v2_tests,
                reason="V2 tests protect old simulation/replay work only; they are not V1.5 formal-flow gates.",
            )
        )

    v2_docs = _glob_existing_files(root, ("docs/architecture/*v2*", "docs/architecture/*V2*"))
    if v2_docs:
        rows.append(
            _surface_row(
                root=root,
                surface="legacy_v2_docs",
                path="docs/architecture/*v2*",
                status="legacy_reference_only",
                action="exclude_from_v1_5_active_surface",
                files=v2_docs,
                reason="V2 cutover/replay documents are historical context and must not steer V1.5 formal operations.",
            )
        )

    legacy_entries = [entry for entry in entries if entry.category == "legacy_v1_reference"]
    if legacy_entries:
        files = [root / entry.path for entry in legacy_entries]
        rows.append(
            _surface_row(
                root=root,
                surface="legacy_v1_reference_tools",
                path="src/gas_calibrator/tools/run_v1_*",
                status="legacy_reference_only",
                action="do_not_start_v1_5_here",
                files=files,
                reason="Old V1 write/acceptance tools may preserve algorithm history but must not launch V1.5.",
            )
        )

    diagnostic_entries = [entry for entry in entries if entry.category == "diagnostic_only"]
    if diagnostic_entries:
        files = [root / entry.path for entry in diagnostic_entries]
        rows.append(
            _surface_row(
                root=root,
                surface="v1_5_diagnostic_tools",
                path="src/gas_calibrator/tools/*diagnostic*",
                status="diagnostic_only",
                action="guarded_engineering_use_only",
                files=files,
                reason="Diagnostics are useful for root cause work but cannot enter formal acceptance or CO2/H2O fitting by default.",
            )
        )

    config_files = _glob_existing_files(
        root,
        (
            "configs/site_v1_5_*current*.json",
            "configs/site_v1_5_*observed*.json",
            "configs/site_v1_5_*generated*.json",
            "configs/default_config_corrected_autodelivery*.json",
            "configs/points_v1_5_*limited*.xlsx",
        ),
    )
    if config_files:
        rows.append(
            _surface_row(
                root=root,
                surface="temporary_or_observed_v1_5_configs",
                path="configs/site_v1_5_*current* / *observed* / *generated*",
                status="review_before_use",
                action="do_not_use_as_default_config",
                files=config_files,
                reason="Observed/generated/current configs may reflect a bench snapshot; formal runs should use an explicit reviewed site config.",
            )
        )

    root_artifacts = _glob_existing_files(
        root,
        (
            "*.stdout.log",
            "*.stderr.log",
            "post_route_close*.csv",
            "post_route_close*.md",
            "v1_5_vs_v2*.csv",
        ),
    )
    if root_artifacts:
        rows.append(
            _surface_row(
                root=root,
                surface="root_temporary_run_artifacts",
                path="repo-root logs/csv/md",
                status="archive_candidate",
                action="move_to_logs_or_archive_after_review",
                files=root_artifacts,
                reason="Root-level run artifacts slow navigation and can look like active inputs; keep evidence, but outside the code entrypoint surface.",
            )
        )

    return rows
