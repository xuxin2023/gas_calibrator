# src/gas_calibrator/v2/web/routes.py
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

router = APIRouter(include_in_schema=False)

FRONTEND_DIST = Path(__file__).resolve().parent / "static"
INDEX_HTML = FRONTEND_DIST / "index.html"

SPA_ROUTES = ["/", "/status", "/history", "/config", "/params", "/monitor"]


if INDEX_HTML.exists():

    @router.get("/{full_path:path}", response_class=HTMLResponse)
    async def spa_fallback(request: Request, full_path: str = ""):
        if full_path.startswith("api/") or full_path.startswith("ws/"):
            from fastapi import HTTPException

            raise HTTPException(status_code=404)
        return HTMLResponse(INDEX_HTML.read_text(encoding="utf-8"))

else:

    for route_path in SPA_ROUTES:

        @router.get(route_path, response_class=HTMLResponse)
        async def _placeholder(request: Request):
            return HTMLResponse(
                """<!DOCTYPE html>
<html lang="zh-CN">
<head><meta charset="UTF-8"><title>气体校准 V2</title></head>
<body style="font-family:sans-serif;padding:40px;">
<h2>前端尚未构建</h2>
<p>请先运行 <code>cd frontend && npm install && npm run build</code></p>
</body></html>"""
            )
