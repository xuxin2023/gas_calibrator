# src/gas_calibrator/v2/web/ws_manager.py
from __future__ import annotations

import asyncio
import json
import random
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


def _simulate_data() -> dict:
    return {
        "type": "telemetry",
        "ts": datetime.now(timezone.utc).isoformat(),
        "pressure_hpa": round(random.uniform(990, 1010), 1),
        "temperature_c": round(random.uniform(22.5, 23.5), 2),
        "humidity_pct": round(random.uniform(45, 55), 1),
        "dewpoint_c": round(random.uniform(10, 14), 2),
        "co2_ppm": round(random.uniform(400, 420), 1),
        "phase": "idle",
        "point_index": 0,
        "total_points": 0,
        "progress_pct": 0.0,
    }


async def _broadcast_loop():
    while True:
        await asyncio.sleep(2.0)
        if manager.active_count > 0:
            await manager.broadcast(_simulate_data())


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
