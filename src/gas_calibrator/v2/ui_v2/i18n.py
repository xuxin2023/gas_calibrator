from __future__ import annotations

from functools import lru_cache
import json
from pathlib import Path
import re
from typing import Any

from ..core.phase_taxonomy_contract import taxonomy_display_label, taxonomy_i18n_key
from ..core.reviewer_fragments_contract import fragment_display_label, fragment_i18n_key

DEFAULT_LOCALE = "zh_CN"
FALLBACK_LOCALE = "en_US"
LOCALES_DIR = Path(__file__).resolve().parent / "locales"
_TOKEN_RE = re.compile(r"[^a-z0-9]+")
_current_locale = DEFAULT_LOCALE
_MEASUREMENT_LAYER_DEFAULTS = {
    "reference": {"zh_CN": "参考层", "en_US": "reference layer"},
    "analyzer_raw": {"zh_CN": "分析仪原始层", "en_US": "analyzer raw layer"},
    "output": {"zh_CN": "输出层", "en_US": "output layer"},
    "data_quality": {"zh_CN": "数据质量层", "en_US": "data quality layer"},
}


@lru_cache(maxsize=8)
def _load_locale(locale: str) -> dict[str, Any]:
    path = LOCALES_DIR / f"{locale}.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def set_locale(locale: str | None) -> str:
    global _current_locale
    _current_locale = str(locale or DEFAULT_LOCALE).strip() or DEFAULT_LOCALE
    return _current_locale


def get_locale() -> str:
    return _current_locale


def _resolve_key(payload: dict[str, Any], key: str) -> Any:
    current: Any = payload
    for part in key.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def lookup(key: str, *, locale: str | None = None) -> str | None:
    preferred = str(locale or _current_locale or DEFAULT_LOCALE)
    for candidate in (preferred, FALLBACK_LOCALE):
        value = _resolve_key(_load_locale(candidate), key)
        if value is not None:
            return str(value)
    return None


def t(key: str, *, locale: str | None = None, default: str | None = None, **kwargs: Any) -> str:
    text = lookup(key, locale=locale)
    if text is None:
        text = default if default is not None else key
    if kwargs:
        text = text.format_map({name: value for name, value in kwargs.items()})
    return text


def _normalize_token(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("%", "pct").replace("°", "deg").replace("掳", "deg")
    text = _TOKEN_RE.sub("_", text).strip("_")
    return text or "empty"


def display_enum(namespace: str, value: Any, *, locale: str | None = None, default: str | None = None) -> str:
    if value in (None, ""):
        return default if default is not None else "--"
    text = lookup(f"enum.{namespace}.{_normalize_token(value)}", locale=locale)
    if text is not None:
        return text
    return str(value if default is None else default)


def display_phase(value: Any, *, locale: str | None = None, default: str | None = None) -> str:
    return display_enum("phase", value, locale=locale, default=default)


def display_page(value: Any, *, locale: str | None = None, default: str | None = None) -> str:
    return display_enum("page", value, locale=locale, default=default)


def display_route(value: Any, *, locale: str | None = None, default: str | None = None) -> str:
    return display_enum("route", value, locale=locale, default=default)


def display_run_mode(value: Any, *, locale: str | None = None, default: str | None = None) -> str:
    return display_enum("run_mode", value, locale=locale, default=default)


def display_notification_level(value: Any, *, locale: str | None = None, default: str | None = None) -> str:
    return display_enum("notification_level", value, locale=locale, default=default)


def display_risk_level(value: Any, *, locale: str | None = None, default: str | None = None) -> str:
    return display_enum("risk_level", value, locale=locale, default=default)


def display_compare_status(value: Any, *, locale: str | None = None, default: str | None = None) -> str:
    return display_enum("compare_status", value, locale=locale, default=default)


def display_evidence_source(value: Any, *, locale: str | None = None, default: str | None = None) -> str:
    return display_enum("evidence_source", value, locale=locale, default=default)


def display_evidence_state(value: Any, *, locale: str | None = None, default: str | None = None) -> str:
    return display_enum("evidence_state", value, locale=locale, default=default)


def display_acceptance_value(value: Any, *, locale: str | None = None, default: str | None = None) -> str:
    return display_enum("acceptance", value, locale=locale, default=default)


def display_artifact_role(value: Any, *, locale: str | None = None, default: str | None = None) -> str:
    return display_enum("artifact_role", value, locale=locale, default=default)


def display_device_status(value: Any, *, locale: str | None = None, default: str | None = None) -> str:
    return display_enum("device_status", value, locale=locale, default=default)


def display_reference_quality(value: Any, *, locale: str | None = None, default: str | None = None) -> str:
    return display_enum("reference_quality", value, locale=locale, default=default)


def display_analyzer_software_version(value: Any, *, locale: str | None = None, default: str | None = None) -> str:
    return display_enum("analyzer_software_version", value, locale=locale, default=default)


def display_device_id_assignment(value: Any, *, locale: str | None = None, default: str | None = None) -> str:
    return display_enum("device_id_assignment", value, locale=locale, default=default)


def display_winner_status(value: Any, *, locale: str | None = None, default: str | None = None) -> str:
    return display_enum("winner_status", value, locale=locale, default=default)


def display_suite_failure_type(value: Any, *, locale: str | None = None, default: str | None = None) -> str:
    return display_enum("suite_failure_type", value, locale=locale, default=default)


def display_taxonomy_value(
    family: str,
    value: Any,
    *,
    locale: str | None = None,
    default: str | None = None,
) -> str:
    preferred = str(locale or _current_locale or DEFAULT_LOCALE)
    i18n_key = taxonomy_i18n_key(family, value)
    fallback = taxonomy_display_label(family, value, locale=preferred, default=default)
    if i18n_key:
        return t(i18n_key, locale=preferred, default=fallback)
    return fallback


def display_taxonomy_values(
    family: str,
    values: list[Any] | tuple[Any, ...] | None,
    *,
    locale: str | None = None,
    default: str | None = None,
) -> list[str]:
    rows: list[str] = []
    for value in list(values or []):
        label = display_taxonomy_value(family, value, locale=locale, default=default)
        if label and label not in rows:
            rows.append(label)
    return rows


def _display_measurement_layer(value: Any, *, locale: str | None = None, default: str | None = None) -> str:
    preferred = str(locale or _current_locale or DEFAULT_LOCALE)
    token = _normalize_token(value)
    defaults = _MEASUREMENT_LAYER_DEFAULTS.get(token, {})
    fallback = default if default is not None else str(
        defaults.get(preferred)
        or defaults.get(FALLBACK_LOCALE)
        or str(value or "").replace("_", " ").strip()
        or "--"
    )
    return t(
        f"results.review_center.detail.measurement.layer.{token}",
        locale=preferred,
        default=fallback,
    )


def _localize_fragment_param_value(value: Any, *, locale: str | None = None) -> Any:
    preferred = str(locale or _current_locale or DEFAULT_LOCALE)
    if isinstance(value, dict):
        kind = str(value.get("kind") or "").strip().lower()
        if kind == "measurement_layer_list":
            labels = [
                _display_measurement_layer(item, locale=preferred)
                for item in list(value.get("values") or [])
                if str(item or "").strip()
            ]
            return " / ".join(labels) if labels else "--"
        if kind == "taxonomy_list":
            family = str(value.get("family") or "").strip()
            labels = display_taxonomy_values(family, list(value.get("values") or []), locale=preferred)
            return " / ".join(labels) if labels else "--"
        if kind == "route_phase_list":
            labels = []
            for item in list(value.get("values") or []):
                payload = dict(item or {}) if isinstance(item, dict) else {}
                route_label = display_route(payload.get("route"), locale=preferred, default=str(payload.get("route") or "--"))
                phase_label = display_phase(payload.get("phase"), locale=preferred, default=str(payload.get("phase") or "--"))
                label = f"{route_label}/{phase_label}".strip("/")
                if label and label not in labels:
                    labels.append(label)
            return " | ".join(labels) if labels else "--"
        if kind == "route":
            return display_route(value.get("value"), locale=preferred, default=str(value.get("value") or "--"))
        if kind == "phase":
            return display_phase(value.get("value"), locale=preferred, default=str(value.get("value") or "--"))
        if kind == "text_list":
            labels = []
            for item in list(value.get("values") or []):
                localized = _localize_fragment_param_value(item, locale=preferred)
                text = str(localized or "").strip()
                if text and text not in labels:
                    labels.append(text)
            return " / ".join(labels) if labels else "--"
        return {
            key: _localize_fragment_param_value(item, locale=preferred)
            for key, item in value.items()
            if str(key).strip()
        }
    if isinstance(value, (list, tuple, set)):
        labels = []
        for item in value:
            localized = _localize_fragment_param_value(item, locale=preferred)
            text = str(localized or "").strip()
            if text and text not in labels:
                labels.append(text)
        return " / ".join(labels) if labels else "--"
    return value


def _localize_fragment_params(params: dict[str, Any] | None, *, locale: str | None = None) -> dict[str, Any]:
    preferred = str(locale or _current_locale or DEFAULT_LOCALE)
    return {
        str(key): _localize_fragment_param_value(value, locale=preferred)
        for key, value in dict(params or {}).items()
        if str(key).strip()
    }


def display_fragment_value(
    family: str,
    value: Any,
    *,
    params: dict[str, Any] | None = None,
    locale: str | None = None,
    default: str | None = None,
) -> str:
    preferred = str(locale or _current_locale or DEFAULT_LOCALE)
    localized_params = _localize_fragment_params(params, locale=preferred)
    i18n_key = fragment_i18n_key(family, value)
    fallback = fragment_display_label(family, value, locale=preferred, params=localized_params, default=default)
    if i18n_key:
        return t(i18n_key, locale=preferred, default=fallback, **localized_params)
    return fallback


def display_fragment_values(
    family: str,
    values: list[Any] | tuple[Any, ...] | None,
    *,
    locale: str | None = None,
    default: str | None = None,
) -> list[str]:
    rows: list[str] = []
    source_values: list[Any]
    if isinstance(values, (str, bytes, dict)):
        source_values = [values]
    else:
        source_values = list(values or [])
    for value in source_values:
        params = dict(value.get("params") or {}) if isinstance(value, dict) else {}
        label = display_fragment_value(
            family,
            value.get("fragment_key") if isinstance(value, dict) else value,
            params=params,
            locale=locale,
            default=(value.get("text") if isinstance(value, dict) else default),
        )
        if label and label not in rows:
            rows.append(label)
    return rows


def display_bool(value: bool, *, locale: str | None = None) -> str:
    return t("common.yes", locale=locale) if bool(value) else t("common.no", locale=locale)


def display_presence(value: bool, *, locale: str | None = None) -> str:
    return t("common.present", locale=locale) if bool(value) else t("common.missing", locale=locale)


def format_percent(value: float, *, digits: int = 1) -> str:
    return f"{float(value):.{int(digits)}f}%"


def format_temperature_c(value: Any) -> str:
    if value in (None, ""):
        return "--"
    return f"{float(value):g}\N{DEGREE SIGN}C"


def format_pressure_hpa(value: Any) -> str:
    if value in (None, ""):
        return "--"
    return f"{float(value):g} hPa"


def format_ppm(value: Any) -> str:
    if value in (None, ""):
        return "--"
    return f"{float(value):g} ppm"
