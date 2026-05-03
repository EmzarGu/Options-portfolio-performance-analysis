from __future__ import annotations

from typing import Any, Dict

from portfolio_backend.models import PipelineState
from portfolio_backend.serializers import json_safe, serialize_portfolio_state
from portfolio_backend.view_models import build_dashboard_view_model


PORTFOLIO_PAYLOAD_VERSION = "portfolio_payload_v1"


def build_portfolio_payload(
    state: PipelineState,
    include_unrealized_current_year: bool,
) -> Dict[str, Any]:
    """Build the stable Python shape that a future HTTP API can return as JSON."""
    payload = serialize_portfolio_state(state, include_unrealized_current_year)
    view_model = build_dashboard_view_model(state, include_unrealized_current_year)

    payload["snapshot"].update(
        {
            "covered_period_note": json_safe(view_model.covered_period_note),
            "dividend_warning_note": json_safe(view_model.dividend_warning_note),
        }
    )
    payload["metadata"] = {
        "payload_version": PORTFOLIO_PAYLOAD_VERSION,
        **payload["metadata"],
    }
    return payload
