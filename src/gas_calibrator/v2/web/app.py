# src/gas_calibrator/v2/web/app.py
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

WEB_DIR = Path(__file__).resolve().parent
STATIC_DIR = WEB_DIR / "static"
FRONTEND_DIST = STATIC_DIR

_logger = logging.getLogger(__name__)


def _init_database(state: Any) -> None:
    try:
        from ..storage.database import get_engine_manager

        db = get_engine_manager()
        db.initialize()
        state.database_manager = db
        _logger.info("数据库管理器已就绪")
    except Exception as exc:
        _logger.warning("数据库初始化失败: %s", exc)
        state.database_manager = None


def create_app() -> FastAPI:
    app = FastAPI(
        title="气体分析仪自动校准 V2 — Web 控制台",
        version="2.0.0",
        docs_url="/api/docs",
        redoc_url="/api/redoc",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.state.calibration_service: Optional[Any] = None
    app.state.telemetry_state: dict[str, Any] = {
        "pressure_hpa": None,
        "temperature_c": None,
        "humidity_pct": None,
        "dewpoint_c": None,
        "co2_ppm": None,
        "phase": "idle",
        "point_index": 0,
        "total_points": 0,
        "progress_pct": 0.0,
    }

    _init_database(app.state)

    static_root = FRONTEND_DIST if FRONTEND_DIST.exists() else STATIC_DIR
    if static_root.exists():
        assets_dir = static_root / "assets"
        if assets_dir.exists():
            app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")

    from .routes_api import router as api_router
    from .ws_manager import router as ws_router

    app.include_router(api_router)
    app.include_router(ws_router)

    from .routes import router as spa_router

    app.include_router(spa_router)

    return app


app = create_app()
