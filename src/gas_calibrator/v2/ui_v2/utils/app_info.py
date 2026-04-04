from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any

from ..i18n import t


@dataclass(frozen=True)
class AppInfo:
    product_name: str = t("app_info.product_name")
    product_id: str = "gas-calibrator-v2"
    vendor: str = "OpenAI / Industrial Calibration"
    version: str = "0.6.0-demo"
    build: str = "local-dev"

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


APP_INFO = AppInfo()
