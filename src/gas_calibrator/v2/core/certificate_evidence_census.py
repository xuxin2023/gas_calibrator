"""Read-only local certificate-evidence census for GA-D6A.

The census identifies files that deserve documentary review.  It never confirms
certificate validity, infers physical-asset cardinality, or authorizes execution.
"""

from __future__ import annotations

from collections import Counter
import csv
from datetime import datetime
from hashlib import sha256
import logging
import json
import os
from pathlib import Path
import re
from typing import Any, Iterable, Mapping
import zipfile


DEFAULT_CONTRACT_PATH = (
    Path(__file__).resolve().parents[1]
    / "configs"
    / "metrology"
    / "certificate_evidence_census_contract_v1.json"
)

_BOUNDARY = {
    "device_io_allowed": False,
    "database_write_allowed": False,
    "coefficient_fit_allowed": False,
    "coefficient_write_allowed": False,
    "real_primary_latest_refresh_allowed": False,
    "source_file_mutation_allowed": False,
    "report_output_only": True,
    "not_real_acceptance_evidence": True,
    "promotion_state": "blocked",
}
_REQUIRED_ROLES = {
    "co2_zero_gas",
    "co2_standard_gas_series",
    "h2o_dewpoint_reference",
    "digital_pressure_reference",
    "temperature_reference",
    "flow_reference",
    "timebase_reference",
}
_XML_TAG_RE = re.compile(r"<[^>]+>")
_SPACE_RE = re.compile(r"\s+")


def load_certificate_evidence_census_contract(
    path: str | Path = DEFAULT_CONTRACT_PATH,
) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if payload.get("schema_version") != "certificate_evidence_census_contract_v1":
        raise ValueError("unexpected certificate census contract schema")
    if dict(payload.get("evidence_boundary") or {}) != _BOUNDARY:
        raise ValueError("GA-D6A must remain read-only, offline, and blocked")
    roles = [str(item) for item in payload.get("required_roles") or []]
    if set(roles) != _REQUIRED_ROLES or len(roles) != len(_REQUIRED_ROLES):
        raise ValueError("GA-D6A must retain every reference-asset role")
    if set(dict(payload.get("role_terms") or {})) != _REQUIRED_ROLES:
        raise ValueError("GA-D6A role terms must cover every required role")
    interpretation = dict(payload.get("interpretation") or {})
    required_false = {
        "candidate_is_confirmed_certificate",
        "document_count_proves_asset_cardinality",
        "device_output_certificate_is_reference_asset_certificate",
        "measurement_evidence_is_reference_asset_certificate",
        "h2o_dry_gas_is_dewpoint_reference_certificate",
        "census_is_execution_authorization",
        "census_is_real_acceptance",
    }
    if any(interpretation.get(key) is not False for key in required_false):
        raise ValueError("GA-D6A must not promote documentary candidates")
    if interpretation.get("co2_zero_and_h2o_dry_are_distinct") is not True:
        raise ValueError("GA-D6A must keep CO2 zero and H2O dry gas distinct")
    return payload


def scan_certificate_evidence(
    roots: Iterable[str | Path],
    *,
    contract: Mapping[str, Any],
    seeded_candidates: Iterable[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Inventory supported files and conservatively classify documentary candidates."""

    active = dict(contract)
    _validate_runtime_contract(active)
    root_paths = [Path(item).resolve() for item in roots]
    if not root_paths:
        raise ValueError("at least one scan root is required")

    root_summaries: list[dict[str, Any]] = []
    records: list[dict[str, Any]] = []
    for root in root_paths:
        summary, root_records = _scan_root(root, active)
        root_summaries.append(summary)
        records.extend(root_records)

    seed_summary = _merge_seeded_candidates(
        records,
        seeded_candidates=seeded_candidates or [],
        contract=active,
    )
    linked_seed_reviews = [
        {
            "record_id": item.get("record_id"),
            "source_path": item.get("source_path"),
            "sha256": item.get("sha256"),
            "candidate_roles": item.get("candidate_roles"),
            "legacy_registry_seed_roles": item.get("legacy_registry_seed_roles"),
            "classification": item.get("classification"),
            "documentary_review": item.get("documentary_review"),
        }
        for item in records
        if item.get("lineage_state") == "legacy_registry_linked_candidate"
    ]

    role_summary = _build_role_summary(records, active)
    candidates = [
        item
        for item in records
        if item.get("classification") == "reference_asset_certificate_candidate"
    ]
    candidate_hashes = {
        str(item.get("sha256")) for item in candidates if item.get("sha256")
    }
    duplicate_groups = _duplicate_candidate_groups(candidates)
    candidate_count = len(candidates)
    root_complete = all(
        item.get("inventory_status") in {"complete", "complete_with_bounded_exclusions"}
        for item in root_summaries
    )
    bounded_exclusion_count = sum(
        int(item.get("oversize_files_skipped") or 0) for item in root_summaries
    )
    return {
        "artifact_type": "certificate_evidence_census_result",
        "artifact_role": "diagnostic_analysis",
        "schema_version": "certificate_evidence_census_result_v1",
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "contract_id": active.get("contract_id"),
        "evidence_source": "local_filesystem_census",
        "evidence_state": "documentary_candidates_unreviewed",
        "not_real_acceptance_evidence": True,
        "promotion_state": "blocked",
        "ready_for_real_execution": False,
        "device_io_status": "not_attempted",
        "database_write_status": "not_attempted",
        "coefficient_fit_status": "not_attempted",
        "coefficient_writeback_status": "not_attempted",
        "real_primary_latest_refresh_status": "not_attempted",
        "source_mutation_status": "not_attempted",
        "scan_root_count": len(root_summaries),
        "scan_roots_complete": root_complete,
        "bounded_exclusion_count": bounded_exclusion_count,
        "coverage_state": (
            "enumeration_complete_with_bounded_exclusions"
            if root_complete and bounded_exclusion_count
            else "enumeration_complete"
            if root_complete
            else "enumeration_incomplete"
        ),
        "candidate_count": candidate_count,
        "candidate_unique_sha256_count": len(candidate_hashes),
        "supplementary_candidate_count": sum(
            item.get("classification") == "supplementary_reference_evidence_candidate"
            for item in records
        ),
        "duplicate_candidate_groups": duplicate_groups,
        "record_count": len(records),
        "root_summaries": root_summaries,
        "role_summary": role_summary,
        "seed_summary": seed_summary,
        "linked_seed_reviews": linked_seed_reviews,
        "records": records,
        "documentary_ready": False,
        "status": (
            "CENSUS_COMPLETE_WITH_BOUNDED_EXCLUSIONS_AND_CANDIDATES_REQUIRE_REVIEW"
            if root_complete and bounded_exclusion_count and candidate_count
            else "CENSUS_COMPLETE_WITH_BOUNDED_EXCLUSIONS_AND_GAPS"
            if root_complete and bounded_exclusion_count
            else "CENSUS_COMPLETE_CANDIDATES_REQUIRE_REVIEW"
            if root_complete and candidate_count
            else "CENSUS_COMPLETE_WITH_GAPS"
            if root_complete
            else "CENSUS_INCOMPLETE"
        ),
        "interpretation": dict(active.get("interpretation") or {}),
        "limitations": [
            "候选文件未经人工核对，不能视为已确认或有效的计量证书。",
            "文档数量不能证明物理标准器或十瓶标准气的数量与身份。",
            "图片需 OCR/人工复核，旧版 DOC/XLS 需兼容解析器，RAR/7Z 仅登记容器。",
            "非参考证书分类在 execution_rows 中按类抽样，但分类总数保留在 root_summaries。",
            "扫描结果不构成真实执行授权、系数拟合依据或 real acceptance 证据。",
        ],
    }


def write_certificate_evidence_census_artifacts(
    result: Mapping[str, Any],
    *,
    output_dir: str | Path,
) -> dict[str, str]:
    """Write only derived reports; source files remain untouched."""

    target = Path(output_dir).resolve()
    target.mkdir(parents=True, exist_ok=True)
    records = [dict(item) for item in result.get("records") or []]

    rows_path = target / "certificate_evidence_execution_rows.csv"
    fieldnames = [
        "record_id",
        "source_path",
        "relative_path",
        "extension",
        "size_bytes",
        "modified_at",
        "sha256",
        "classification",
        "confidence",
        "candidate_roles",
        "review_state",
        "lineage_state",
        "legacy_registry_seed_roles",
        "documentary_review",
        "extraction_status",
        "archive_candidate_members",
        "reasons",
    ]
    with rows_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for item in records:
            writer.writerow(
                {
                    key: json.dumps(item.get(key), ensure_ascii=False)
                    if isinstance(item.get(key), (list, dict))
                    else item.get(key, "")
                    for key in fieldnames
                }
            )

    summary = {
        "artifact_type": "certificate_evidence_census_summary",
        "artifact_role": "execution_summary",
        "schema_version": "certificate_evidence_census_summary_v1",
        "generated_at": result.get("generated_at"),
        "status": result.get("status"),
        "evidence_source": result.get("evidence_source"),
        "not_real_acceptance_evidence": True,
        "promotion_state": "blocked",
        "scan_roots_complete": result.get("scan_roots_complete"),
        "bounded_exclusion_count": result.get("bounded_exclusion_count"),
        "coverage_state": result.get("coverage_state"),
        "candidate_count": result.get("candidate_count"),
        "candidate_unique_sha256_count": result.get("candidate_unique_sha256_count"),
        "supplementary_candidate_count": result.get("supplementary_candidate_count"),
        "record_count": result.get("record_count"),
        "root_summaries": result.get("root_summaries"),
        "role_summary": result.get("role_summary"),
        "seed_summary": result.get("seed_summary"),
        "documentary_ready": False,
        "ready_for_real_execution": False,
    }
    summary_path = _write_json(target / "certificate_evidence_execution_summary.json", summary)

    report = dict(result)
    report.pop("records", None)
    report["artifact_role"] = "diagnostic_analysis"
    report["artifacts"] = {
        "execution_rows": str(rows_path),
        "execution_summary": str(summary_path),
    }
    report_path = _write_json(target / "certificate_evidence_diagnostic_analysis.json", report)
    markdown_path = target / "certificate_evidence_diagnostic_analysis.md"
    markdown_path.write_text(_format_markdown(report), encoding="utf-8")

    digest_path = _write_json(
        target / "certificate_evidence_sha256_manifest.json",
        {
            "artifact_type": "certificate_evidence_sha256_manifest",
            "artifact_role": "execution_rows",
            "schema_version": "certificate_evidence_sha256_manifest_v1",
            "not_real_acceptance_evidence": True,
            "entries": [
                {
                    "record_id": item.get("record_id"),
                    "source_path": item.get("source_path"),
                    "sha256": item.get("sha256"),
                }
                for item in records
            ],
        },
    )
    report["artifacts"].update(
        {
            "diagnostic_analysis": str(report_path),
            "diagnostic_markdown": str(markdown_path),
            "sha256_manifest": str(digest_path),
        }
    )
    _write_json(report_path, report)
    return {str(key): str(value) for key, value in report["artifacts"].items()}


def _scan_root(root: Path, contract: Mapping[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    include = {str(item).casefold() for item in contract.get("include_extensions") or []}
    probe_extensions = {
        str(item).casefold() for item in contract.get("content_probe_extensions") or []
    }
    excluded = {
        str(item).casefold() for item in contract.get("excluded_directory_names") or []
    }
    limits = dict(contract.get("limits") or {})
    max_probe = int(limits.get("maximum_content_probe_files_per_root") or 0)
    max_emit = int(limits.get("maximum_emitted_records_per_root") or 0)
    max_non_candidate_per_class = int(
        limits.get("maximum_non_candidate_records_per_class_per_root") or 0
    )
    max_source_bytes = int(limits.get("maximum_source_file_bytes") or 0)

    summary: dict[str, Any] = {
        "root": str(root),
        "inventory_status": "complete",
        "directories_visited": 0,
        "supported_files_seen": 0,
        "supported_bytes_seen": 0,
        "content_probe_eligible": 0,
        "content_probe_attempted": 0,
        "content_probe_limit_reached": False,
        "emitted_record_limit_reached": False,
        "non_candidate_records_sampled": False,
        "excluded_directories": 0,
        "reparse_or_symlink_directories_skipped": 0,
        "oversize_files_skipped": 0,
        "oversize_files": [],
        "access_errors": [],
        "candidate_hash_errors": [],
        "non_candidate_hash_error_count": 0,
    }
    if not root.exists() or not root.is_dir():
        summary["inventory_status"] = "missing_root"
        summary["access_errors"] = ["root_missing_or_not_directory"]
        return summary, []

    metadata: list[dict[str, Any]] = []
    stack = [root]
    while stack:
        directory = stack.pop()
        summary["directories_visited"] += 1
        try:
            entries = list(os.scandir(directory))
        except OSError as exc:
            summary["inventory_status"] = "partial"
            summary["access_errors"].append(f"{directory}: {type(exc).__name__}")
            continue
        for entry in entries:
            try:
                if entry.is_dir(follow_symlinks=False):
                    if entry.name.casefold() in excluded:
                        summary["excluded_directories"] += 1
                    elif entry.is_symlink() or bool(
                        getattr(os.path, "isjunction", lambda _path: False)(entry.path)
                    ):
                        summary["reparse_or_symlink_directories_skipped"] += 1
                    else:
                        stack.append(Path(entry.path))
                    continue
                if not entry.is_file(follow_symlinks=False):
                    continue
                path = Path(entry.path)
                extension = path.suffix.casefold()
                if extension not in include:
                    continue
                stat = entry.stat(follow_symlinks=False)
            except OSError as exc:
                summary["inventory_status"] = "partial"
                summary["access_errors"].append(f"{entry.path}: {type(exc).__name__}")
                continue
            summary["supported_files_seen"] += 1
            summary["supported_bytes_seen"] += int(stat.st_size)
            if int(stat.st_size) > max_source_bytes:
                summary["oversize_files_skipped"] += 1
                if len(summary["oversize_files"]) < 100:
                    summary["oversize_files"].append(
                        {"path": str(path), "size_bytes": int(stat.st_size)}
                    )
                continue
            path_text = str(path)
            path_score = _path_context_score(path_text, contract)
            certificate_in_path = _has_any(path_text, contract.get("certificate_terms"))
            role_in_path = bool(_match_roles(path_text, contract))
            obvious_non_reference = any(
                _has_any(path_text, contract.get(key))
                for key in (
                    "device_output_terms",
                    "generated_artifact_terms",
                    "manual_terms",
                )
            )
            direct_signal = certificate_in_path or role_in_path
            archive = extension in {
                str(item).casefold() for item in contract.get("archive_extensions") or []
            }
            should_probe = extension in probe_extensions and (
                not obvious_non_reference
                and (certificate_in_path or path_score >= 2 or role_in_path)
            )
            if should_probe:
                summary["content_probe_eligible"] += 1
            if path_score >= 2 or direct_signal or archive:
                metadata.append(
                    {
                        "path": path,
                        "extension": extension,
                        "size_bytes": int(stat.st_size),
                        "modified_at": datetime.fromtimestamp(stat.st_mtime).astimezone().isoformat(timespec="seconds"),
                        "path_score": path_score,
                        "should_probe": should_probe,
                    }
                )

    metadata.sort(key=lambda item: (-int(item["path_score"]), str(item["path"]).casefold()))
    records: list[dict[str, Any]] = []
    probe_count = 0
    classification_counts: Counter[str] = Counter()
    emitted_classification_counts: Counter[str] = Counter()
    omitted_classification_counts: Counter[str] = Counter()
    for item in metadata:
        path = Path(item["path"])
        extension = str(item["extension"])
        extraction_status = "not_attempted"
        extracted_text = ""
        archive_members: list[str] = []
        if extension == ".zip":
            archive_members, extraction_status = _inventory_zip(path, contract)
        elif extension in {".rar", ".7z"}:
            extraction_status = "archive_inventory_only_parser_unavailable"
        elif bool(item["should_probe"]):
            if probe_count >= max_probe:
                summary["content_probe_limit_reached"] = True
                summary["inventory_status"] = "partial"
                extraction_status = "content_probe_limit_reached"
            else:
                probe_count += 1
                extracted_text, extraction_status = _extract_text(path, extension, contract)
        elif extension in {
            str(value).casefold() for value in contract.get("ocr_required_extensions") or []
        }:
            extraction_status = "ocr_or_visual_review_required"
        elif extension in {
            str(value).casefold()
            for value in contract.get("legacy_parser_required_extensions") or []
        }:
            extraction_status = "legacy_parser_required"

        classification = _classify(
            path_text=str(path),
            extracted_text=extracted_text,
            archive_members=archive_members,
            contract=contract,
        )
        if classification is None:
            continue
        classification_name = str(classification["classification"])
        classification_counts[classification_name] += 1
        is_reference_candidate = (
            classification_name == "reference_asset_certificate_candidate"
        )
        if (
            not is_reference_candidate
            and emitted_classification_counts[classification_name]
            >= max_non_candidate_per_class
        ):
            omitted_classification_counts[classification_name] += 1
            summary["non_candidate_records_sampled"] = True
            continue
        if len(records) >= max_emit:
            omitted_classification_counts[classification_name] += 1
            summary["emitted_record_limit_reached"] = True
            summary["inventory_status"] = "partial"
            continue
        try:
            digest = _sha256_file(path)
        except OSError as exc:
            digest = ""
            extraction_status = f"hash_error:{type(exc).__name__}"
            if is_reference_candidate:
                summary["inventory_status"] = "partial"
                summary["candidate_hash_errors"].append(str(path))
            else:
                summary["non_candidate_hash_error_count"] += 1
        relative = _safe_relative(path, root)
        record_id = sha256(str(path).casefold().encode("utf-8")).hexdigest()[:20]
        records.append(
            {
                "record_id": record_id,
                "source_path": str(path),
                "relative_path": relative,
                "extension": extension,
                "size_bytes": item["size_bytes"],
                "modified_at": item["modified_at"],
                "sha256": digest,
                "classification": classification["classification"],
                "confidence": classification["confidence"],
                "candidate_roles": classification["candidate_roles"],
                "review_state": classification["review_state"],
                "extraction_status": extraction_status,
                "archive_candidate_members": archive_members[:100],
                "h2o_dry_gas_point_evidence": classification["h2o_dry_gas_point_evidence"],
                "lineage_state": "filesystem_discovery_unlinked",
                "legacy_registry_seed_roles": [],
                "documentary_review": {},
                "reasons": classification["reasons"],
            }
        )
        emitted_classification_counts[classification_name] += 1
    summary["content_probe_attempted"] = probe_count
    summary["emitted_records"] = len(records)
    summary["classified_files"] = sum(classification_counts.values())
    summary["classification_counts"] = dict(classification_counts)
    summary["emitted_classification_counts"] = dict(emitted_classification_counts)
    summary["omitted_classification_counts"] = dict(omitted_classification_counts)
    if len(summary["access_errors"]) > 100:
        summary["access_error_count"] = len(summary["access_errors"])
        summary["access_errors"] = summary["access_errors"][:100]
    else:
        summary["access_error_count"] = len(summary["access_errors"])
    if summary["inventory_status"] == "complete" and summary["oversize_files_skipped"]:
        summary["inventory_status"] = "complete_with_bounded_exclusions"
    return summary, records


def _classify(
    *,
    path_text: str,
    extracted_text: str,
    archive_members: list[str],
    contract: Mapping[str, Any],
) -> dict[str, Any] | None:
    path_haystack = path_text.casefold()
    name_haystack = Path(path_text).name.casefold()
    content_haystack = extracted_text.casefold()
    member_haystack = "\n".join(archive_members).casefold()
    combined = "\n".join((path_haystack, content_haystack, member_haystack))
    name_roles = _match_roles(name_haystack, contract)
    path_roles = _match_roles(path_haystack, contract)
    content_roles = _match_roles("\n".join((content_haystack, member_haystack)), contract)
    member_roles = _match_roles(member_haystack, contract)
    roles = sorted(name_roles or member_roles or path_roles or content_roles)
    certificate_in_name = _has_any(name_haystack, contract.get("certificate_terms"))
    certificate_in_path = _has_any(path_haystack, contract.get("certificate_terms"))
    certificate_in_content = _has_any(
        "\n".join((content_haystack, member_haystack)), contract.get("certificate_terms")
    )
    certificate_signal = certificate_in_path or certificate_in_content
    device_output = _has_any(combined, contract.get("device_output_terms"))
    generated_artifact = _has_any(path_haystack, contract.get("generated_artifact_terms"))
    design_document = _has_any(name_haystack, contract.get("design_document_terms"))
    manual = _has_any(
        "\n".join((name_haystack, content_haystack)), contract.get("manual_terms")
    )
    measurement = _has_any(combined, contract.get("measurement_terms"))
    dry_gas = _has_any(combined, contract.get("dry_gas_terms"))

    reasons: list[str] = []
    if roles:
        reasons.append("role_terms:" + ",".join(roles))
    if certificate_in_path:
        reasons.append("certificate_term_in_path")
    if certificate_in_content:
        reasons.append("certificate_term_in_content_or_archive_member")
    if dry_gas:
        reasons.append("h2o_dry_gas_kept_distinct_from_dewpoint_reference")

    if device_output:
        classification = "device_output_certificate"
        confidence = "high"
        review_state = "not_reference_asset_certificate"
        reasons.append("device_output_term")
    elif generated_artifact:
        classification = "software_or_generated_artifact"
        confidence = "high"
        review_state = "not_reference_asset_certificate"
        reasons.append("generated_artifact_term")
    elif manual:
        classification = "manual_or_protocol"
        confidence = "high"
        review_state = "not_reference_asset_certificate"
        reasons.append("manual_or_protocol_term")
    elif design_document:
        classification = "manual_or_protocol"
        confidence = "high"
        review_state = "not_reference_asset_certificate"
        reasons.append("design_or_test_plan_term")
    elif certificate_signal and roles:
        classification = "reference_asset_certificate_candidate"
        confidence = "high" if certificate_in_path and path_roles else "medium"
        review_state = "candidate_requires_human_review"
    elif certificate_signal:
        classification = "unknown_certificate_candidate"
        confidence = "low"
        review_state = "candidate_requires_human_role_assignment"
    elif measurement and roles and not certificate_in_name:
        classification = "measurement_evidence"
        confidence = "medium"
        review_state = "not_reference_asset_certificate"
        reasons.append("measurement_term_without_certificate_identity")
    elif roles or dry_gas:
        classification = "unknown_role_candidate"
        confidence = "low"
        review_state = "candidate_requires_document_type_review"
    else:
        return None

    if dry_gas and "h2o_dewpoint_reference" not in roles:
        roles = [role for role in roles if role != "co2_zero_gas"]
    return {
        "classification": classification,
        "confidence": confidence,
        "candidate_roles": roles or ["unassigned"],
        "review_state": review_state,
        "h2o_dry_gas_point_evidence": dry_gas,
        "reasons": reasons,
    }


def _match_roles(text: str, contract: Mapping[str, Any]) -> set[str]:
    matches = {
        str(role)
        for role, terms in dict(contract.get("role_terms") or {}).items()
        if _has_any(text, terms)
    }
    if "co2_zero_gas" in matches:
        matches.discard("co2_standard_gas_series")
    if "温度探头" in str(text).casefold() or "temperature probe" in str(text).casefold():
        matches.discard("h2o_dewpoint_reference")
        matches.add("temperature_reference")
    return matches


def _has_any(text: str, terms: Any) -> bool:
    haystack = str(text).casefold()
    return any(str(term).casefold() in haystack for term in terms or [])


def _path_context_score(path_text: str, contract: Mapping[str, Any]) -> int:
    lowered = path_text.casefold()
    return sum(
        1
        for term in contract.get("path_context_terms") or []
        if str(term).casefold() in lowered
    )


def _extract_text(
    path: Path,
    extension: str,
    contract: Mapping[str, Any],
) -> tuple[str, str]:
    limit = int(
        dict(contract.get("limits") or {}).get("maximum_extracted_characters_per_file")
        or 0
    )
    try:
        if extension == ".pdf":
            try:
                from pypdf import PdfReader  # type: ignore
            except ImportError:
                return "", "pdf_parser_unavailable"
            logging.getLogger("pypdf").setLevel(logging.ERROR)
            page_limit = int(
                dict(contract.get("limits") or {}).get("maximum_pdf_pages_per_file")
                or 0
            )
            reader = PdfReader(str(path), strict=False)
            chunks = []
            for page in reader.pages[:page_limit]:
                chunks.append(page.extract_text() or "")
                if sum(len(item) for item in chunks) >= limit:
                    break
            text = "\n".join(chunks)[:limit]
            return _normalize_text(text), "text_extracted" if text.strip() else "pdf_no_extractable_text_ocr_required"
        if extension == ".docx":
            with zipfile.ZipFile(path) as archive:
                chunks = []
                for name in archive.namelist():
                    if not name.startswith("word/") or not name.endswith(".xml"):
                        continue
                    chunks.append(_xml_text(archive.read(name)))
                    if sum(len(item) for item in chunks) >= limit:
                        break
            text = "\n".join(chunks)[:limit]
            return _normalize_text(text), "text_extracted" if text.strip() else "docx_no_extractable_text"
        if extension == ".xlsx":
            with zipfile.ZipFile(path) as archive:
                chunks = []
                for name in archive.namelist():
                    if name == "xl/sharedStrings.xml" or (
                        name.startswith("xl/worksheets/") and name.endswith(".xml")
                    ):
                        chunks.append(_xml_text(archive.read(name)))
                        if sum(len(item) for item in chunks) >= limit:
                            break
            text = "\n".join(chunks)[:limit]
            return _normalize_text(text), "text_extracted" if text.strip() else "xlsx_no_extractable_text"
        if extension == ".csv":
            raw = path.read_bytes()[: min(limit * 4, 1_000_000)]
            for encoding in ("utf-8-sig", "gb18030", "utf-16"):
                try:
                    return _normalize_text(raw.decode(encoding)[:limit]), "text_extracted"
                except UnicodeError:
                    continue
            return raw.decode("utf-8", errors="replace")[:limit], "text_extracted_with_replacement"
    except Exception as exc:  # Untrusted office/PDF inputs may raise parser-specific errors.
        return "", f"extract_error:{type(exc).__name__}"
    return "", "unsupported_content_probe"


def _inventory_zip(path: Path, contract: Mapping[str, Any]) -> tuple[list[str], str]:
    maximum = int(dict(contract.get("limits") or {}).get("maximum_zip_members") or 0)
    try:
        with zipfile.ZipFile(path) as archive:
            names = archive.namelist()
    except (OSError, ValueError, zipfile.BadZipFile):
        return [], "zip_inventory_error"
    candidate_names = [
        name
        for name in names[:maximum]
        if _path_context_score(name, contract) > 0
        or _has_any(name, contract.get("certificate_terms"))
        or bool(_match_roles(name, contract))
    ]
    status = "zip_members_truncated" if len(names) > maximum else "zip_members_inventoried"
    return candidate_names, status


def _build_role_summary(
    records: list[dict[str, Any]],
    contract: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    summary: dict[str, dict[str, Any]] = {}
    cardinality = dict(contract.get("expected_asset_cardinality") or {})
    for role in contract.get("required_roles") or []:
        candidates = [
            item
            for item in records
            if item.get("classification") == "reference_asset_certificate_candidate"
            and str(role) in (item.get("candidate_roles") or [])
        ]
        conflicts = [item for item in candidates if len(item.get("candidate_roles") or []) > 1]
        summary[str(role)] = {
            "expected_physical_asset_count": cardinality.get(str(role)),
            "candidate_document_count": len(candidates),
            "unique_candidate_sha256_count": len(
                {str(item.get("sha256")) for item in candidates if item.get("sha256")}
            ),
            "high_confidence_candidate_count": sum(
                item.get("confidence") == "high" for item in candidates
            ),
            "legacy_registry_linked_candidate_count": sum(
                item.get("lineage_state") == "legacy_registry_linked_candidate"
                for item in candidates
            ),
            "multi_role_conflict_count": len(conflicts),
            "census_state": (
                "candidate_conflict_requires_review"
                if conflicts
                else "candidate_found_requires_review"
                if candidates
                else "missing"
            ),
            "confirmed_certificate_count": 0,
            "cardinality_verified": False,
            "documentary_ready": False,
        }
    return summary


def _merge_seeded_candidates(
    records: list[dict[str, Any]],
    *,
    seeded_candidates: Iterable[Mapping[str, Any]],
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    seeds = [dict(item) for item in seeded_candidates]
    by_path = {
        os.path.normcase(str(item.get("source_path") or "")): item for item in records
    }
    summary: dict[str, Any] = {
        "requested": len(seeds),
        "found": 0,
        "missing": [],
        "added_records": 0,
        "matched_existing_records": 0,
        "linked_required_role_candidates": 0,
        "linked_supplementary_candidates": 0,
    }
    for seed in seeds:
        path = Path(str(seed.get("path") or "")).resolve()
        role = str(seed.get("candidate_role") or "").strip()
        if not role:
            raise ValueError("seeded certificate evidence requires candidate_role")
        if not path.exists() or not path.is_file():
            summary["missing"].append(str(path))
            continue
        summary["found"] += 1
        key = os.path.normcase(str(path))
        existing = by_path.get(key)
        documentary_review = dict(seed.get("documentary_review") or {})
        if existing is None:
            stat = path.stat()
            extension = path.suffix.casefold()
            extracted_text, extraction_status = _extract_text(path, extension, contract)
            if extension in {
                str(item).casefold()
                for item in contract.get("ocr_required_extensions") or []
            }:
                extraction_status = "ocr_or_visual_review_required"
            digest = _sha256_file(path)
            required = role in _REQUIRED_ROLES
            existing = {
                "record_id": sha256(str(path).casefold().encode("utf-8")).hexdigest()[:20],
                "source_path": str(path),
                "relative_path": str(path),
                "extension": extension,
                "size_bytes": int(stat.st_size),
                "modified_at": datetime.fromtimestamp(stat.st_mtime).astimezone().isoformat(timespec="seconds"),
                "sha256": digest,
                "classification": (
                    "reference_asset_certificate_candidate"
                    if required
                    else "supplementary_reference_evidence_candidate"
                ),
                "confidence": "high",
                "candidate_roles": [role],
                "review_state": "candidate_requires_human_review_and_asset_linkage",
                "extraction_status": extraction_status,
                "archive_candidate_members": [],
                "h2o_dry_gas_point_evidence": False,
                "lineage_state": "legacy_registry_linked_candidate",
                "legacy_registry_seed_roles": [role],
                "documentary_review": documentary_review,
                "reasons": [
                    "explicit_legacy_registry_seed",
                    f"legacy_registry_role:{role}",
                ],
            }
            records.append(existing)
            by_path[key] = existing
            summary["added_records"] += 1
        else:
            seed_roles = list(existing.get("legacy_registry_seed_roles") or [])
            if role not in seed_roles:
                seed_roles.append(role)
            existing["legacy_registry_seed_roles"] = sorted(seed_roles)
            if documentary_review:
                existing["documentary_review"] = documentary_review
            existing["lineage_state"] = "legacy_registry_linked_candidate"
            reasons = list(existing.get("reasons") or [])
            for reason in ("explicit_legacy_registry_seed", f"legacy_registry_role:{role}"):
                if reason not in reasons:
                    reasons.append(reason)
            existing["reasons"] = reasons
            roles = list(existing.get("candidate_roles") or [])
            if role in _REQUIRED_ROLES and role not in roles:
                roles.append(role)
                existing["candidate_roles"] = sorted(roles)
            if role in _REQUIRED_ROLES:
                existing["classification"] = "reference_asset_certificate_candidate"
                existing["review_state"] = "candidate_requires_human_review_and_asset_linkage"
            summary["matched_existing_records"] += 1
        if role in _REQUIRED_ROLES:
            summary["linked_required_role_candidates"] += 1
        else:
            summary["linked_supplementary_candidates"] += 1
    summary["missing_count"] = len(summary["missing"])
    summary["complete"] = not summary["missing"]
    return summary


def _duplicate_candidate_groups(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[str]] = {}
    for item in candidates:
        digest = str(item.get("sha256") or "")
        if digest:
            grouped.setdefault(digest, []).append(str(item.get("source_path") or ""))
    return [
        {"sha256": digest, "path_count": len(paths), "source_paths": sorted(paths)}
        for digest, paths in sorted(grouped.items())
        if len(paths) > 1
    ]


def _format_markdown(report: Mapping[str, Any]) -> str:
    roots = list(report.get("root_summaries") or [])
    roles = dict(report.get("role_summary") or {})
    lines = [
        "# GA-D6A 本地计量证书证据普查",
        "",
        "> 只读文件系统普查；不连接 COM、不读写设备、不写数据库或系数，也不刷新 real_primary_latest。",
        "",
        "## 结论",
        "",
        f"- 状态：`{report.get('status')}`",
        f"- 扫描根目录完整：`{report.get('scan_roots_complete')}`",
        f"- 覆盖状态：`{report.get('coverage_state')}`",
        f"- 超大文件受控排除：`{report.get('bounded_exclusion_count')}`",
        f"- 待人工复核候选：`{report.get('candidate_count')}`",
        f"- 候选唯一 SHA-256：`{report.get('candidate_unique_sha256_count')}`",
        f"- 补充参考证据候选：`{report.get('supplementary_candidate_count')}`",
        "- 资料包就绪：`False`",
        "- 真实执行就绪：`False`",
        "- real acceptance：`blocked`",
        "",
        "候选文件只是检索命中，不代表证书真实、有效、在有效期内、覆盖所需量程，亦不证明十瓶标准气的物理身份。",
        "",
        "## 扫描覆盖",
        "",
        "| 根目录 | 状态 | 支持文件 | 内容探测 | 候选记录 | 访问错误 |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for item in roots:
        lines.append(
            "| {root} | {status} | {files} | {probes} | {records} | {errors} |".format(
                root=str(item.get("root") or "").replace("|", "\\|"),
                status=item.get("inventory_status"),
                files=item.get("supported_files_seen"),
                probes=item.get("content_probe_attempted"),
                records=item.get("emitted_records"),
                errors=item.get("access_error_count"),
            )
        )
    lines.extend(
        [
            "",
            "## 角色候选",
            "",
            "| 参考资产角色 | 预期物理数量 | 候选路径 | 唯一 SHA-256 | 历史登记关联 | 高置信候选 | 冲突 | 普查状态 |",
            "|---|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    for role, item in roles.items():
        lines.append(
            f"| `{role}` | {item.get('expected_physical_asset_count')} | "
            f"{item.get('candidate_document_count')} | {item.get('unique_candidate_sha256_count')} | "
            f"{item.get('legacy_registry_linked_candidate_count')} | "
            f"{item.get('high_confidence_candidate_count')} | "
            f"{item.get('multi_role_conflict_count')} | `{item.get('census_state')}` |"
        )
    linked_reviews = list(report.get("linked_seed_reviews") or [])
    lines.extend(["", "## 历史登记关联证据", ""])
    if not linked_reviews:
        lines.append("- 无。")
    for item in linked_reviews:
        review = dict(item.get("documentary_review") or {})
        conclusion = str(review.get("review_conclusion") or "待人工核对")
        lines.append(
            f"- `{','.join(item.get('legacy_registry_seed_roles') or [])}`："
            f"`{Path(str(item.get('source_path') or '')).name}`；{conclusion}"
        )
    lines.extend(
        [
            "",
            "## 固定解释边界",
            "",
            "- CO2 零气与 H2O 干气点/露点参考保持独立，不互相替代。",
            "- 设备输出校准证书和历史测量表不属于参考资产证书。",
            "- 图片、扫描 PDF、旧版 DOC/XLS 及 RAR/7Z 的未解析内容属于剩余人工复核范围。",
            "- 本报告只服务于后续资料导入治理，不构成系数拟合依据或真实执行授权。",
            "",
        ]
    )
    return "\n".join(lines)


def _validate_runtime_contract(contract: Mapping[str, Any]) -> None:
    if dict(contract.get("evidence_boundary") or {}) != _BOUNDARY:
        raise ValueError("unsafe GA-D6A runtime boundary")


def _xml_text(raw: bytes) -> str:
    text = raw.decode("utf-8", errors="replace")
    text = text.replace("</w:p>", "\n").replace("</row>", "\n")
    return _XML_TAG_RE.sub(" ", text)


def _normalize_text(text: str) -> str:
    return _SPACE_RE.sub(" ", text).strip()


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_relative(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def _write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


__all__ = [
    "DEFAULT_CONTRACT_PATH",
    "load_certificate_evidence_census_contract",
    "scan_certificate_evidence",
    "write_certificate_evidence_census_artifacts",
]
