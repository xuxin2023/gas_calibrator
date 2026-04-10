from pathlib import Path
import sys

from gas_calibrator.v2.adapters.recognition_scope_gateway import RecognitionScopeGateway
from gas_calibrator.v2.core.recognition_scope_repository import (
    DatabaseReadyRecognitionScopeRepositoryStub,
    FileBackedRecognitionScopeRepository,
    RECOGNITION_SCOPE_GATEWAY_MODE,
    RECOGNITION_SCOPE_REPOSITORY_MODE,
    RECOGNITION_SCOPE_REPOSITORY_SCHEMA_VERSION,
)
from gas_calibrator.v2.scripts.build_offline_governance_artifacts import rebuild_run

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import build_fake_facade


def test_recognition_scope_repository_keeps_file_backed_default_path(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)

    rebuild_run(run_dir)

    snapshot = FileBackedRecognitionScopeRepository(run_dir).load_snapshot()
    gateway_payload = RecognitionScopeGateway(run_dir).read_payload()
    stub_payload = DatabaseReadyRecognitionScopeRepositoryStub(run_dir).load_snapshot()

    scope_payload = dict(snapshot.get("scope_definition_pack") or {})
    decision_payload = dict(snapshot.get("decision_rule_profile") or {})
    rollup = dict(snapshot.get("recognition_scope_rollup") or {})
    gateway_rollup = dict(gateway_payload.get("recognition_scope_rollup") or {})
    stub_rollup = dict(stub_payload.get("recognition_scope_rollup") or {})

    assert scope_payload["artifact_type"] == "scope_definition_pack"
    assert scope_payload["scope_export_pack"]["ready_for_readiness_mapping"] is True
    assert scope_payload["not_real_acceptance_evidence"] is True
    assert decision_payload["artifact_type"] == "decision_rule_profile"
    assert decision_payload["decision_rule_id"]
    assert decision_payload["not_real_acceptance_evidence"] is True

    assert rollup["index_schema_version"] == RECOGNITION_SCOPE_REPOSITORY_SCHEMA_VERSION
    assert rollup["repository_mode"] == RECOGNITION_SCOPE_REPOSITORY_MODE
    assert rollup["gateway_mode"] == RECOGNITION_SCOPE_GATEWAY_MODE
    assert rollup["rollup_scope"] == "run-dir"
    assert rollup["parent_run_count"] == 1
    assert rollup["artifact_count"] == 2
    assert rollup["primary_evidence_rewritten"] is False
    assert rollup["not_real_acceptance_evidence"] is True
    assert rollup["db_ready_stub"]["enabled"] is False
    assert rollup["db_ready_stub"]["not_in_default_chain"] is True
    assert gateway_rollup["repository_mode"] == RECOGNITION_SCOPE_REPOSITORY_MODE
    assert gateway_rollup["gateway_mode"] == RECOGNITION_SCOPE_GATEWAY_MODE
    assert stub_rollup["db_ready_stub"]["requires_explicit_injection"] is True
    assert stub_rollup["db_ready_stub"]["not_in_default_chain"] is True
