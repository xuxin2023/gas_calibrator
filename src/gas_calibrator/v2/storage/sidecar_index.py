from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any


SIDECAR_COLLECTIONS = (
    "runs",
    "artifacts",
    "manifests",
    "reviews",
    "coefficients",
    "anomaly_cases",
    "anomaly_labels",
    "feature_snapshots",
    "model_registry",
    "model_evaluations",
    "review_digests",
    "run_risk_scores",
)

_COLLECTION_ID_FIELDS: dict[str, tuple[str, ...]] = {
    "runs": ("record_id", "run_id", "id"),
    "artifacts": ("record_id", "artifact_id", "artifact_key", "path"),
    "manifests": ("record_id", "manifest_id", "manifest_key", "path"),
    "reviews": ("record_id", "review_id", "id", "summary"),
    "coefficients": ("record_id", "coefficient_id", "id", "version"),
    "anomaly_cases": ("record_id", "anomaly_id", "case_id", "id", "tag"),
    "anomaly_labels": ("record_id", "label_id", "id", "tag"),
    "feature_snapshots": ("record_id", "snapshot_id", "id", "feature_version"),
    "model_registry": ("record_id", "model_id", "id", "model_version"),
    "model_evaluations": ("record_id", "evaluation_id", "id", "model_version"),
    "review_digests": ("record_id", "digest_id", "id", "run_id"),
    "run_risk_scores": ("record_id", "score_id", "id", "run_id"),
}


def _utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _stringify(value: Any) -> str:
    return str(value or "").strip()


def _normalize_string_list(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, (list, tuple, set)):
        return [_stringify(item) for item in value if _stringify(item)]
    text = _stringify(value)
    if not text:
        return []
    for delimiter in ("|", ";", ","):
        if delimiter in text:
            return [_stringify(part) for part in text.split(delimiter) if _stringify(part)]
    return [text]


def _normalize_dict_list(value: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                rows.append(dict(item))
            else:
                text = _stringify(item)
                if text:
                    rows.append({"label": text})
        return rows
    if isinstance(value, dict):
        return [dict(value)]
    text = _stringify(value)
    return [{"label": text}] if text else []


def _compute_record_id(collection: str, payload: dict[str, Any]) -> str:
    for field in _COLLECTION_ID_FIELDS.get(collection, ("record_id", "id")):
        text = _stringify(payload.get(field))
        if text:
            return text
    digest = hashlib.sha1(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    return f"{collection}:{digest[:16]}"


def _base_record(collection: str, payload: dict[str, Any]) -> dict[str, Any]:
    record = dict(payload or {})
    record_id = _compute_record_id(collection, record)
    run_id = _stringify(record.get("run_id") or record.get("linked_run_id"))
    timestamp = _utc_now_text()
    return {
        **record,
        "collection": collection,
        "record_id": record_id,
        "run_id": run_id,
        "updated_at": _stringify(record.get("updated_at")) or timestamp,
        "created_at": _stringify(record.get("created_at")) or timestamp,
        "backend_role": _stringify(record.get("backend_role")) or "sidecar_index",
        "reviewer_only": bool(record.get("reviewer_only", True)),
        "advisory_only": bool(record.get("advisory_only", True)),
        "sidecar_only": bool(record.get("sidecar_only", True)),
        "file_artifact_first_preserved": bool(record.get("file_artifact_first_preserved", True)),
        "main_chain_dependency": bool(record.get("main_chain_dependency", False)),
        "not_real_acceptance_evidence": bool(record.get("not_real_acceptance_evidence", True)),
        "not_ready_for_formal_claim": bool(record.get("not_ready_for_formal_claim", True)),
        "not_device_control": bool(record.get("not_device_control", True)),
        "not_sampling_release": bool(record.get("not_sampling_release", True)),
        "not_coefficient_writeback": bool(record.get("not_coefficient_writeback", True)),
        "not_formal_metrology_conclusion": bool(
            record.get("not_formal_metrology_conclusion", True)
        ),
    }


def normalize_sidecar_record(collection: str, payload: dict[str, Any]) -> dict[str, Any]:
    if collection not in SIDECAR_COLLECTIONS:
        raise ValueError(f"unsupported sidecar collection: {collection}")
    record = _base_record(collection, payload)
    if collection == "anomaly_cases":
        record.update(
            {
                "tag": _stringify(record.get("tag") or record.get("anomaly_tag") or "unclassified"),
                "severity": _stringify(record.get("severity") or "medium"),
                "state": _stringify(record.get("state") or "open"),
                "device": _stringify(record.get("device") or record.get("device_name") or "unknown"),
                "window_refs": _normalize_string_list(
                    record.get("window_refs") or record.get("window_ref")
                ),
                "root_cause_candidates": _normalize_string_list(
                    record.get("root_cause_candidates") or record.get("root_causes")
                ),
                "reviewer_conclusion": _stringify(
                    record.get("reviewer_conclusion") or record.get("conclusion")
                ),
            }
        )
    elif collection == "anomaly_labels":
        record.update(
            {
                "tag": _stringify(record.get("tag") or "unclassified"),
                "severity": _stringify(record.get("severity") or "medium"),
                "state": _stringify(record.get("state") or "open"),
                "device": _stringify(record.get("device") or record.get("device_name")),
            }
        )
    elif collection == "feature_snapshots":
        record.update(
            {
                "feature_version": _stringify(record.get("feature_version") or "feature_v1"),
                "window_refs": _normalize_string_list(
                    record.get("window_refs")
                    or record.get("window_ref")
                    or record.get("time_range")
                ),
                "signal_family": _stringify(record.get("signal_family") or "mixed"),
                "values": dict(record.get("values") or {}),
                "linked_decision_diff": _stringify(
                    record.get("linked_decision_diff") or record.get("decision_diff")
                ),
            }
        )
    elif collection == "model_registry":
        record.update(
            {
                "model_version": _stringify(record.get("model_version") or "model_v1"),
                "feature_version": _stringify(record.get("feature_version") or "feature_v1"),
                "label_version": _stringify(record.get("label_version") or "label_v1"),
                "evaluation_metrics": dict(record.get("evaluation_metrics") or {}),
                "release_status": _stringify(record.get("release_status") or "draft"),
                "rollback_target": _stringify(record.get("rollback_target") or "none"),
                "human_review_required": bool(record.get("human_review_required", True)),
            }
        )
    elif collection == "model_evaluations":
        record.update(
            {
                "model_version": _stringify(record.get("model_version") or "model_v1"),
                "feature_version": _stringify(record.get("feature_version") or "feature_v1"),
                "label_version": _stringify(record.get("label_version") or "label_v1"),
                "evaluation_metrics": dict(record.get("evaluation_metrics") or {}),
                "release_status": _stringify(record.get("release_status") or "candidate"),
                "rollback_target": _stringify(record.get("rollback_target") or "none"),
                "human_review_required": bool(record.get("human_review_required", True)),
            }
        )
    elif collection == "review_digests":
        record.update(
            {
                "risk_summary": _stringify(record.get("risk_summary") or record.get("summary")),
                "evidence_gaps": _normalize_string_list(record.get("evidence_gaps")),
                "revalidation_suggestions": _normalize_string_list(
                    record.get("revalidation_suggestions") or record.get("follow_up_actions")
                ),
                "standards_gap_navigation": _normalize_dict_list(
                    record.get("standards_gap_navigation") or record.get("standards_gaps")
                ),
            }
        )
    elif collection == "run_risk_scores":
        record.update(
            {
                "risk_score": float(record.get("risk_score", record.get("score", 0.0)) or 0.0),
                "risk_level": _stringify(record.get("risk_level") or "low"),
                "risk_summary": _stringify(
                    record.get("risk_summary")
                    or record.get("summary")
                    or record.get("risk_level")
                    or "low"
                ),
            }
        )
    return record


@dataclass(slots=True)
class SidecarIndexStore:
    backend: str
    path: Path

    @classmethod
    def file_backed(cls, path: str | Path) -> "SidecarIndexStore":
        return cls(backend="file", path=Path(path))

    @classmethod
    def sqlite_sidecar(cls, path: str | Path) -> "SidecarIndexStore":
        return cls(backend="sqlite", path=Path(path))

    @property
    def enabled(self) -> bool:
        return bool(self.path)

    def describe(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "backend": self.backend,
            "path": str(self.path),
            "exists": self.path.exists(),
        }

    def upsert(self, collection: str, payload: dict[str, Any]) -> dict[str, Any]:
        record = normalize_sidecar_record(collection, payload)
        if self.backend == "sqlite":
            self._sqlite_upsert(record)
        else:
            self._file_upsert(record)
        return dict(record)

    def extend(self, collection: str, payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [self.upsert(collection, payload) for payload in list(payloads or [])]

    def query(
        self,
        collection: str,
        *,
        run_id: str | None = None,
        filters: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        rows = self._sqlite_query(collection) if self.backend == "sqlite" else self._file_query(collection)
        run_ref = _stringify(run_id)
        normalized_filters = {str(key): value for key, value in dict(filters or {}).items()}
        matched: list[dict[str, Any]] = []
        for row in rows:
            if run_ref and _stringify(row.get("run_id")) != run_ref:
                continue
            if not self._matches_filters(row, normalized_filters):
                continue
            matched.append(dict(row))
        matched.sort(key=lambda item: _stringify(item.get("updated_at")), reverse=True)
        if limit is not None:
            return matched[: max(0, int(limit))]
        return matched

    def collection_counts(self, *, run_id: str | None = None) -> dict[str, int]:
        return {
            collection: len(self.query(collection, run_id=run_id))
            for collection in SIDECAR_COLLECTIONS
        }

    def all_records(self, *, run_id: str | None = None) -> dict[str, list[dict[str, Any]]]:
        return {
            collection: self.query(collection, run_id=run_id)
            for collection in SIDECAR_COLLECTIONS
        }

    @staticmethod
    def _matches_filters(row: dict[str, Any], filters: dict[str, Any]) -> bool:
        for key, expected in filters.items():
            actual = row.get(key)
            if isinstance(expected, (list, tuple, set)):
                if actual not in expected:
                    return False
            elif actual != expected:
                return False
        return True

    def _file_payload(self) -> dict[str, Any]:
        if not self.path.exists():
            return {
                "schema_version": "sidecar_index_v1",
                "collections": {collection: [] for collection in SIDECAR_COLLECTIONS},
            }
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
        collections = dict(payload.get("collections") or {})
        for collection in SIDECAR_COLLECTIONS:
            collections.setdefault(collection, [])
        return {
            "schema_version": _stringify(payload.get("schema_version")) or "sidecar_index_v1",
            "collections": collections,
        }

    def _file_write(self, payload: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def _file_upsert(self, record: dict[str, Any]) -> None:
        payload = self._file_payload()
        collection = record["collection"]
        rows = [
            dict(item)
            for item in list(dict(payload.get("collections") or {}).get(collection) or [])
            if isinstance(item, dict)
            and _stringify(item.get("record_id")) != _stringify(record.get("record_id"))
        ]
        rows.append(dict(record))
        payload["collections"][collection] = rows
        self._file_write(payload)

    def _file_query(self, collection: str) -> list[dict[str, Any]]:
        payload = self._file_payload()
        return [
            dict(item)
            for item in list(dict(payload.get("collections") or {}).get(collection) or [])
            if isinstance(item, dict)
        ]

    def _sqlite_connection(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(str(self.path))
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS sidecar_records (
                collection TEXT NOT NULL,
                record_id TEXT NOT NULL,
                run_id TEXT,
                updated_at TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (collection, record_id)
            )
            """
        )
        connection.commit()
        return connection

    def _sqlite_upsert(self, record: dict[str, Any]) -> None:
        with self._sqlite_connection() as connection:
            connection.execute(
                """
                INSERT INTO sidecar_records (collection, record_id, run_id, updated_at, payload_json)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(collection, record_id)
                DO UPDATE SET
                    run_id = excluded.run_id,
                    updated_at = excluded.updated_at,
                    payload_json = excluded.payload_json
                """,
                (
                    record["collection"],
                    record["record_id"],
                    _stringify(record.get("run_id")),
                    _stringify(record.get("updated_at")) or _utc_now_text(),
                    json.dumps(record, ensure_ascii=False, sort_keys=True),
                ),
            )
            connection.commit()

    def _sqlite_query(self, collection: str) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        with self._sqlite_connection() as connection:
            rows = connection.execute(
                """
                SELECT payload_json
                FROM sidecar_records
                WHERE collection = ?
                """,
                (collection,),
            ).fetchall()
        records: list[dict[str, Any]] = []
        for (payload_json,) in rows:
            try:
                payload = json.loads(str(payload_json))
            except Exception:
                payload = {}
            if isinstance(payload, dict):
                records.append(payload)
        return records
