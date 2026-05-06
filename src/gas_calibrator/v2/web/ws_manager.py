# src/gas_calibrator/v2/web/ws_manager.py
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter()


class ConnectionManager:
    def __init__(self):
        self._connections: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self._connections.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self._connections:
            self._connections.remove(ws)

    async def broadcast(self, message: dict):
        text = json.dumps(message, ensure_ascii=False)
        stale: list[WebSocket] = []
        for conn in self._connections:
            try:
                await conn.send_text(text)
            except Exception:
                stale.append(conn)
        for s in stale:
            self._connections.remove(s)

    @property
    def active_count(self) -> int:
        return len(self._connections)


manager = ConnectionManager()


def _read_telemetry_state() -> dict:
    try:
        from .app import app

        tele = getattr(app.state, "telemetry_state", None)
        if isinstance(tele, dict):
            return {
                "type": "telemetry",
                "ts": datetime.now(timezone.utc).isoformat(),
                "pressure_hpa": tele.get("pressure_hpa"),
                "temperature_c": tele.get("temperature_c"),
                "humidity_pct": tele.get("humidity_pct"),
                "dewpoint_c": tele.get("dewpoint_c"),
                "co2_ppm": tele.get("co2_ppm"),
                "phase": tele.get("phase", "idle"),
                "point_index": tele.get("point_index", 0),
                "total_points": tele.get("total_points", 0),
                "progress_pct": tele.get("progress_pct", 0.0),
            }
    except Exception:
        pass
    return {
        "type": "telemetry",
        "ts": datetime.now(timezone.utc).isoformat(),
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


async def _broadcast_loop():
    while True:
        await asyncio.sleep(2.0)
        if manager.active_count > 0:
            await manager.broadcast(_read_telemetry_state())


@router.on_event("startup")
async def start_broadcast():
    asyncio.create_task(_broadcast_loop())


@router.websocket("/ws/monitor")
async def ws_monitor(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if msg.get("action") == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception:
        manager.disconnect(websocket)
