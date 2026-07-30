from __future__ import annotations

from copy import deepcopy
import csv
import json
from pathlib import Path
import zipfile

import pytest

from gas_calibrator.validation.certificate_evidence_census import (
    load_certificate_evidence_census_contract,
    scan_certificate_evidence,
    write_certificate_evidence_census_artifacts,
)


def _write_docx(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            "word/document.xml",
            f'<w:document xmlns:w="urn:test"><w:body><w:p><w:t>{text}</w:t></w:p></w:body></w:document>',
        )


def _build_fixture(root: Path) -> None:
    _write_docx(
        root / "参考标准" / "CO2标准气体校准证书.docx",
        "Certificate of analysis CO2 standard gas cylinder 400 ppm",
    )
    _write_docx(
        root / "参考标准" / "CO2零气校准证书.docx",
        "二氧化碳零气 校准证书 0 ppm",
    )
    _write_docx(
        root / "设备输出" / "气体分析仪校准证书.docx",
        "气体分析仪校准证书，使用 CO2 标准气体完成设备校准",
    )
    _write_docx(
        root / "手册" / "露点仪校准说明书.docx",
        "露点仪用户手册 calibration manual",
    )
    dry_gas = root / "0621测量数据" / "H2O干气点校准数据.csv"
    dry_gas.parent.mkdir(parents=True, exist_ok=True)
    dry_gas.write_text("time,H2O dry gas point\n1,0\n", encoding="utf-8")
    image = root / "参考标准" / "露点仪校准证书.jpg"
    image.write_bytes(b"synthetic-image-placeholder")
    archive_path = root / "归档" / "资料.zip"
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("标准气体/CO2标准气体校准证书.pdf", b"fixture")

    generated = root / "gas_calibrator" / "_runtime" / "fit_inputs" / "h2o_certificate_absolute.csv"
    generated.parent.mkdir(parents=True, exist_ok=True)
    generated.write_text("certificate,h2o dewpoint\nsynthetic,0\n", encoding="utf-8")


def test_census_separates_reference_candidates_from_outputs_and_measurements(
    tmp_path: Path,
) -> None:
    root = tmp_path / "source"
    _build_fixture(root)
    contract = load_certificate_evidence_census_contract()

    result = scan_certificate_evidence([root], contract=contract)
    records = result["records"]
    by_name = {Path(item["source_path"]).name: item for item in records}

    assert result["scan_roots_complete"] is True
    assert by_name["CO2标准气体校准证书.docx"]["classification"] == (
        "reference_asset_certificate_candidate"
    )
    assert by_name["CO2标准气体校准证书.docx"]["candidate_roles"] == [
        "co2_standard_gas_series"
    ]
    assert by_name["CO2零气校准证书.docx"]["candidate_roles"] == ["co2_zero_gas"]
    assert by_name["气体分析仪校准证书.docx"]["classification"] == (
        "device_output_certificate"
    )
    assert by_name["H2O干气点校准数据.csv"]["classification"] != (
        "reference_asset_certificate_candidate"
    )
    assert by_name["H2O干气点校准数据.csv"]["h2o_dry_gas_point_evidence"] is True
    assert "h2o_dewpoint_reference" not in by_name["H2O干气点校准数据.csv"][
        "candidate_roles"
    ]
    assert by_name["露点仪校准说明书.docx"]["classification"] == "manual_or_protocol"
    assert by_name["h2o_certificate_absolute.csv"]["classification"] == (
        "software_or_generated_artifact"
    )


def test_images_and_archives_are_inventory_candidates_without_mutating_sources(
    tmp_path: Path,
) -> None:
    root = tmp_path / "source"
    _build_fixture(root)
    before = sorted((path.relative_to(root), path.read_bytes()) for path in root.rglob("*") if path.is_file())

    result = scan_certificate_evidence(
        [root], contract=load_certificate_evidence_census_contract()
    )
    after = sorted((path.relative_to(root), path.read_bytes()) for path in root.rglob("*") if path.is_file())
    by_name = {Path(item["source_path"]).name: item for item in result["records"]}

    assert before == after
    assert by_name["露点仪校准证书.jpg"]["extraction_status"] == (
        "ocr_or_visual_review_required"
    )
    assert by_name["资料.zip"]["classification"] == (
        "reference_asset_certificate_candidate"
    )
    assert by_name["资料.zip"]["archive_candidate_members"] == [
        "标准气体/CO2标准气体校准证书.pdf"
    ]


def test_report_roles_and_boundaries_are_explicit(tmp_path: Path) -> None:
    root = tmp_path / "source"
    _build_fixture(root)
    result = scan_certificate_evidence(
        [root], contract=load_certificate_evidence_census_contract()
    )
    artifacts = write_certificate_evidence_census_artifacts(
        result, output_dir=tmp_path / "reports"
    )

    summary = json.loads(Path(artifacts["execution_summary"]).read_text(encoding="utf-8"))
    report = json.loads(Path(artifacts["diagnostic_analysis"]).read_text(encoding="utf-8"))
    with Path(artifacts["execution_rows"]).open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert rows
    assert summary["artifact_role"] == "execution_summary"
    assert report["artifact_role"] == "diagnostic_analysis"
    assert report["not_real_acceptance_evidence"] is True
    assert report["promotion_state"] == "blocked"
    assert report["documentary_ready"] is False
    assert report["ready_for_real_execution"] is False
    assert report["device_io_status"] == "not_attempted"
    assert report["database_write_status"] == "not_attempted"
    assert report["coefficient_writeback_status"] == "not_attempted"
    markdown = Path(artifacts["diagnostic_markdown"]).read_text(encoding="utf-8")
    assert "CO2 零气与 H2O 干气点/露点参考保持独立" in markdown


def test_role_document_counts_never_claim_physical_cardinality(tmp_path: Path) -> None:
    root = tmp_path / "source"
    _write_docx(root / "CO2标准气体校准证书.docx", "CO2 standard gas certificate")
    result = scan_certificate_evidence(
        [root], contract=load_certificate_evidence_census_contract()
    )
    standard_gas = result["role_summary"]["co2_standard_gas_series"]

    assert standard_gas["expected_physical_asset_count"] == 10
    assert standard_gas["candidate_document_count"] == 1
    assert standard_gas["unique_candidate_sha256_count"] == 1
    assert standard_gas["confirmed_certificate_count"] == 0
    assert standard_gas["cardinality_verified"] is False
    assert standard_gas["documentary_ready"] is False


def test_duplicate_candidate_paths_are_grouped_by_sha256(tmp_path: Path) -> None:
    root = tmp_path / "source"
    first = root / "a" / "数字压力计校准证书.pdf"
    second = root / "b" / "数字压力计校准证书.pdf"
    first.parent.mkdir(parents=True)
    second.parent.mkdir(parents=True)
    first.write_bytes(b"same scanned certificate")
    second.write_bytes(first.read_bytes())

    result = scan_certificate_evidence(
        [root], contract=load_certificate_evidence_census_contract()
    )
    pressure = result["role_summary"]["digital_pressure_reference"]

    assert result["candidate_count"] == 2
    assert result["candidate_unique_sha256_count"] == 1
    assert pressure["candidate_document_count"] == 2
    assert pressure["unique_candidate_sha256_count"] == 1
    assert result["duplicate_candidate_groups"][0]["path_count"] == 2


def test_contract_rejects_any_promotion_or_write_boundary(tmp_path: Path) -> None:
    contract = load_certificate_evidence_census_contract()
    unsafe = deepcopy(contract)
    unsafe["evidence_boundary"]["device_io_allowed"] = True
    path = tmp_path / "unsafe.json"
    path.write_text(json.dumps(unsafe, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(ValueError, match="read-only"):
        load_certificate_evidence_census_contract(path)

    unsafe = deepcopy(contract)
    unsafe["interpretation"]["candidate_is_confirmed_certificate"] = True
    path.write_text(json.dumps(unsafe, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(ValueError, match="must not promote"):
        load_certificate_evidence_census_contract(path)


def test_missing_root_is_reported_as_incomplete(tmp_path: Path) -> None:
    result = scan_certificate_evidence(
        [tmp_path / "missing"], contract=load_certificate_evidence_census_contract()
    )
    assert result["status"] == "CENSUS_INCOMPLETE"
    assert result["scan_roots_complete"] is False
    assert result["root_summaries"][0]["inventory_status"] == "missing_root"


def test_non_reference_sampling_does_not_make_inventory_incomplete(tmp_path: Path) -> None:
    root = tmp_path / "source"
    for index in range(3):
        _write_docx(root / f"压力校准说明书_{index}.docx", "pressure calibration manual")
    contract = load_certificate_evidence_census_contract()
    contract["limits"]["maximum_non_candidate_records_per_class_per_root"] = 1

    result = scan_certificate_evidence([root], contract=contract)
    summary = result["root_summaries"][0]

    assert result["scan_roots_complete"] is True
    assert summary["classification_counts"]["manual_or_protocol"] == 3
    assert summary["emitted_classification_counts"]["manual_or_protocol"] == 1
    assert summary["omitted_classification_counts"]["manual_or_protocol"] == 2
    assert summary["non_candidate_records_sampled"] is True


def test_parent_manual_directory_does_not_hide_a_certificate(tmp_path: Path) -> None:
    root = tmp_path / "source"
    _write_docx(
        root / "手册" / "数字压力计校准证书.docx",
        "数字压力计 calibration certificate",
    )

    result = scan_certificate_evidence(
        [root], contract=load_certificate_evidence_census_contract()
    )

    assert result["candidate_count"] == 1
    assert result["records"][0]["classification"] == (
        "reference_asset_certificate_candidate"
    )


def test_temperature_probe_is_not_counted_as_dewpoint_reference(tmp_path: Path) -> None:
    root = tmp_path / "source"
    _write_docx(
        root / "精密露点仪(温度探头)校准证书.docx",
        "temperature probe calibration certificate",
    )

    result = scan_certificate_evidence(
        [root], contract=load_certificate_evidence_census_contract()
    )
    record = result["records"][0]

    assert record["candidate_roles"] == ["temperature_reference"]
    assert result["role_summary"]["h2o_dewpoint_reference"]["candidate_document_count"] == 0


def test_legacy_seed_adds_generic_image_without_claiming_required_role(
    tmp_path: Path,
) -> None:
    root = tmp_path / "source"
    image = root / "generic_001.jpg"
    image.parent.mkdir(parents=True)
    image.write_bytes(b"certificate photo")

    result = scan_certificate_evidence(
        [root],
        contract=load_certificate_evidence_census_contract(),
        seeded_candidates=[
            {
                "path": str(image),
                "candidate_role": "co2_standard_gas_single_point_897ppm",
                "documentary_review": {
                    "review_conclusion": "single point label only"
                },
            }
        ],
    )

    assert result["candidate_count"] == 0
    assert result["supplementary_candidate_count"] == 1
    assert result["seed_summary"]["complete"] is True
    assert result["seed_summary"]["added_records"] == 1
    record = result["records"][0]
    assert record["classification"] == "supplementary_reference_evidence_candidate"
    assert record["extraction_status"] == "ocr_or_visual_review_required"
    assert record["lineage_state"] == "legacy_registry_linked_candidate"
    assert record["documentary_review"]["review_conclusion"] == (
        "single point label only"
    )
    assert result["linked_seed_reviews"][0]["documentary_review"] == (
        record["documentary_review"]
    )


def test_oversize_supported_file_is_an_explicit_bounded_exclusion(
    tmp_path: Path,
) -> None:
    root = tmp_path / "source"
    root.mkdir()
    oversized = root / "标准气体校准证书.jpg"
    oversized.write_bytes(b"x" * 20)
    contract = load_certificate_evidence_census_contract()
    contract["limits"]["maximum_source_file_bytes"] = 10

    result = scan_certificate_evidence([root], contract=contract)
    summary = result["root_summaries"][0]

    assert result["scan_roots_complete"] is True
    assert result["bounded_exclusion_count"] == 1
    assert result["coverage_state"] == "enumeration_complete_with_bounded_exclusions"
    assert result["status"] == "CENSUS_COMPLETE_WITH_BOUNDED_EXCLUSIONS_AND_GAPS"
    assert summary["inventory_status"] == "complete_with_bounded_exclusions"
    assert summary["oversize_files"] == [
        {"path": str(oversized), "size_bytes": 20}
    ]
