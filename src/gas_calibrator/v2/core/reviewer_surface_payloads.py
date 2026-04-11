"""Shared payload extraction helpers for WP6 + step2_closeout_digest reviewer surfaces.

All modules that extract WP6+closeout payloads from a results payload dict must
use these helpers instead of hand-writing payload.get("pt_ilc_registry") etc.

Step 2 boundary:
  - evidence_source = "simulated"
  - not_real_acceptance_evidence = True
  - not_ready_for_formal_claim = True
"""

from __future__ import annotations

from typing import Any

from .reviewer_surface_contracts import (
    WP6_CLOSEOUT_ARTIFACT_KEYS,
    WP6_CLOSEOUT_DISPLAY_LABELS,
    WP6_CLOSEOUT_DISPLAY_LABELS_EN,
    WP6_CLOSEOUT_I18N_KEYS,
    WP6_CLOSEOUT_ARTIFACT_ROLES,
    WP6_CLOSEOUT_FILENAME_MAP,
)


# ---------------------------------------------------------------------------
# Core extraction: extract WP6+closeout payloads from a source dict
# ---------------------------------------------------------------------------


def extract_wp6_closeout_payloads(
    source: dict[str, Any],
    *,
    default_empty: bool = True,
) -> dict[str, dict[str, Any]]:
    """Extract WP6+closeout payloads from a source dict in canonical key order.

    Args:
        source: A dict containing WP6+closeout payload entries
                (e.g. a results payload, or a wp6_gateway read_payload result).
        default_empty: If True, missing keys return empty dict instead of None.

    Returns:
        Ordered dict mapping each artifact key to its payload dict.
        Keys are in WP6_CLOSEOUT_ARTIFACT_KEYS order.
    """
    default = {} if default_empty else None
    result: dict[str, dict[str, Any]] = {}
    for key in WP6_CLOSEOUT_ARTIFACT_KEYS:
        raw = source.get(key)
        if raw is not None:
            result[key] = dict(raw)
        else:
            result[key] = dict(default) if default is not None else None  # type: ignore[arg-type]
    return result


# ---------------------------------------------------------------------------
# Enriched extraction: include filename, label, role, i18n_key
# ---------------------------------------------------------------------------


def extract_wp6_closeout_enriched(
    source: dict[str, Any],
    *,
    default_empty: bool = True,
) -> list[dict[str, Any]]:
    """Extract WP6+closeout payloads with enriched metadata.

    Returns a list of dicts in canonical key order, each containing:
        - key: artifact key
        - payload: the extracted payload dict
        - filename: json filename
        - markdown_filename: markdown filename
        - display_label: Chinese display label
        - display_label_en: English display label
        - i18n_key: i18n lookup key
        - role: artifact role
    """
    payloads = extract_wp6_closeout_payloads(source, default_empty=default_empty)
    result: list[dict[str, Any]] = []
    for key in WP6_CLOSEOUT_ARTIFACT_KEYS:
        filenames = WP6_CLOSEOUT_FILENAME_MAP.get(key, (f"{key}.json", f"{key}.md"))
        result.append({
            "key": key,
            "payload": payloads.get(key),
            "filename": filenames[0],
            "markdown_filename": filenames[1],
            "display_label": WP6_CLOSEOUT_DISPLAY_LABELS.get(key, key),
            "display_label_en": WP6_CLOSEOUT_DISPLAY_LABELS_EN.get(key, key),
            "i18n_key": WP6_CLOSEOUT_I18N_KEYS.get(key, ""),
            "role": WP6_CLOSEOUT_ARTIFACT_ROLES.get(key, "diagnostic_analysis"),
        })
    return result


# ---------------------------------------------------------------------------
# Review center readiness_summary_payloads builder
# ---------------------------------------------------------------------------


def build_wp6_closeout_readiness_pairs(
    source: dict[str, Any],
    *,
    filename_module: Any = None,
) -> list[tuple[str, dict[str, Any]]]:
    """Build (filename, payload) pairs for review center readiness_summary_payloads.

    Args:
        source: A dict containing WP6+closeout payload entries.
        filename_module: Optional module with filename constants
                        (e.g. recognition_readiness_artifacts).
                        If None, uses WP6_CLOSEOUT_FILENAME_MAP directly.

    Returns:
        List of (filename, payload_dict) tuples in canonical key order.
    """
    payloads = extract_wp6_closeout_payloads(source, default_empty=True)
    pairs: list[tuple[str, dict[str, Any]]] = []
    for key in WP6_CLOSEOUT_ARTIFACT_KEYS:
        if filename_module is not None:
            # Look up the filename constant from the module
            const_name = key.upper() + "_FILENAME"
            filename = getattr(filename_module, const_name, None)
            if filename is None:
                filenames = WP6_CLOSEOUT_FILENAME_MAP.get(key, (f"{key}.json", f"{key}.md"))
                filename = filenames[0]
        else:
            filenames = WP6_CLOSEOUT_FILENAME_MAP.get(key, (f"{key}.json", f"{key}.md"))
            filename = filenames[0]
        pairs.append((filename, dict(payloads.get(key) or {})))
    return pairs
