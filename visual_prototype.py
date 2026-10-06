from __future__ import annotations

import os
from datetime import date
from typing import Any, Optional

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from portfolio_backend import web_data_service as web_data
from portfolio_backend.app_settings import load_monthly_target_band
from portfolio_backend.visual_prototype import build_visual_prototype_data
from portfolio_backend.visual_prototype_templates import VISUAL_PROTOTYPE_HTML


app = FastAPI(title="Options ROI Visual Prototype", version="0.1.0")


def _truthy_env(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _prototype_payload(
    *,
    as_of: Optional[date] = None,
    include_unrealized: bool = True,
) -> dict[str, Any]:
    band = load_monthly_target_band()
    dashboard = web_data._get_cached_dashboard_data(
        as_of=as_of,
        include_unrealized=include_unrealized,
        target_return=float(band["target_return"]),
        target_floor=float(band["target_floor"]),
    )
    decision_lab = web_data._get_cached_decision_lab_payload(dashboard)
    assignment_quality: dict[str, Any] = {}
    if _truthy_env("VISUAL_PROTOTYPE_ASSIGNMENT_QUALITY", True):
        assignment_quality = web_data._get_cached_assignment_quality_data(
            source_payload=dashboard,
            as_of=as_of,
        )
    return build_visual_prototype_data(dashboard, decision_lab, assignment_quality)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "options-roi-visual-prototype"}


@app.get("/", response_class=HTMLResponse)
def prototype_page() -> HTMLResponse:
    return HTMLResponse(VISUAL_PROTOTYPE_HTML)


@app.get("/api/prototype")
def prototype_data(
    as_of: Optional[date] = None,
    include_unrealized: bool = True,
) -> dict[str, Any]:
    return _prototype_payload(as_of=as_of, include_unrealized=include_unrealized)
