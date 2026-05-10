#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


READ_ENDPOINTS = {
    "/v1/mobile/config": {
        "available_sheets",
        "default_selected_sheets",
        "missing_default_sheets",
        "include_unrealized_default",
        "as_of_default",
        "source",
        "capabilities",
    },
    "/v1/mobile/dashboard": {
        "request",
        "data_freshness",
        "snapshot",
        "monthly_target",
        "open_option_short_preview",
        "issue_summary",
    },
    "/v1/mobile/positions": {"request", "data_freshness", "inventory", "open_option_shorts"},
    "/v1/mobile/open-option-shorts": {"request", "data_freshness", "moneyness_legend", "items"},
    "/v1/mobile/tickers": {"request", "data_freshness", "items"},
    "/v1/mobile/performance/monthly": {
        "request",
        "data_freshness",
        "target_return",
        "target_basis",
        "return_metric",
        "current_month",
        "months",
        "future_months",
    },
    "/v1/mobile/performance/yearly": {"request", "data_freshness", "years"},
    "/v1/mobile/issues": {
        "request",
        "data_freshness",
        "summary",
        "issues",
        "audit_summary",
        "audit_notes",
        "coverage",
    },
}

REFRESH_KEYS = {"request", "data_freshness", "refresh"}
HEALTH_KEYS = {"status", "service", "version"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke test the Options ROI mobile FastAPI endpoints.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8700", help="Mobile API base URL.")
    parser.add_argument("--as-of", default=None, help="Optional YYYY-MM-DD as_of query value.")
    parser.add_argument(
        "--include-unrealized",
        choices=["0", "1", "false", "true"],
        default="1",
        help="Whether to include unrealized-adjusted values.",
    )
    parser.add_argument(
        "--selected-sheet",
        action="append",
        default=None,
        help="Selected option sheet. Repeat for multiple sheets.",
    )
    parser.add_argument("--timeout", type=float, default=60.0, help="Request timeout in seconds.")
    parser.add_argument("--api-key", default=None, help="Optional mobile API key to send as X-API-Key.")
    return parser.parse_args()


def common_query(args: argparse.Namespace) -> str:
    params: list[tuple[str, str]] = [("include_unrealized", args.include_unrealized)]
    if args.as_of:
        params.append(("as_of", args.as_of))
    for sheet in args.selected_sheet or ["Options 2024", "Options 2025", "Options 2026"]:
        params.append(("selected_sheets", sheet))
    return urlencode(params)


def request_json(
    url: str,
    *,
    method: str = "GET",
    timeout: float = 60.0,
    api_key: str | None = None,
) -> tuple[int, dict[str, Any]]:
    request = Request(url, method=method)
    if api_key:
        request.add_header("X-API-Key", api_key)
    with urlopen(request, timeout=timeout) as response:
        return response.status, json.load(response)


def assert_keys(endpoint: str, payload: dict[str, Any], expected: set[str]) -> None:
    actual = set(payload)
    if actual != expected:
        raise AssertionError(f"{endpoint} keys mismatch: expected {sorted(expected)}, got {sorted(actual)}")


def summarize(endpoint: str, payload: dict[str, Any]) -> str:
    if endpoint.endswith("/config"):
        return f"available_sheets={len(payload.get('available_sheets', []))}"
    if endpoint.endswith("/dashboard"):
        return f"snapshot_keys={len(payload.get('snapshot', {}))}, preview={len(payload.get('open_option_short_preview', []))}"
    if endpoint.endswith("/positions"):
        return f"inventory={len(payload.get('inventory', []))}, shorts={len(payload.get('open_option_shorts', []))}"
    if endpoint.endswith("/open-option-shorts"):
        items = payload.get("items", [])
        first = items[0].get("ticker") if items else None
        return f"items={len(items)}, first={first}"
    if endpoint.endswith("/tickers"):
        return f"items={len(payload.get('items', []))}"
    if endpoint.endswith("/monthly"):
        return f"months={len(payload.get('months', []))}, future_months={len(payload.get('future_months', []))}"
    if endpoint.endswith("/yearly"):
        return f"years={len(payload.get('years', []))}"
    if endpoint.endswith("/issues"):
        summary = payload.get("summary", {})
        return f"severity={summary.get('severity')}, total={summary.get('total_count')}"
    return "ok"


def assert_error(
    base_url: str,
    path: str,
    expected_status: int,
    expected_code: str,
    timeout: float,
    api_key: str | None,
) -> None:
    try:
        request_json(f"{base_url}{path}", timeout=timeout, api_key=api_key)
    except HTTPError as exc:
        payload = json.loads(exc.read().decode("utf-8"))
        code = payload.get("error", {}).get("code")
        if exc.code != expected_status or code != expected_code:
            raise AssertionError(f"{path}: expected {expected_status}/{expected_code}, got {exc.code}/{code}") from exc
        print(f"{path}: {exc.code} {code}")
        return
    raise AssertionError(f"{path}: expected {expected_status}/{expected_code}, got success")


def assert_protected(base_url: str, path: str, timeout: float) -> None:
    assert_error(base_url, path, 401, "unauthorized", timeout, api_key=None)
    assert_error(base_url, path, 401, "unauthorized", timeout, api_key="invalid-mobile-api-key")


def main() -> int:
    args = parse_args()
    base_url = args.base_url.rstrip("/")
    query = common_query(args)

    status, health = request_json(f"{base_url}/v1/mobile/health", timeout=args.timeout)
    if status != 200:
        raise AssertionError(f"/v1/mobile/health: expected 200, got {status}")
    assert_keys("/v1/mobile/health", health, HEALTH_KEYS)
    if health.get("status") != "ok":
        raise AssertionError(f"/v1/mobile/health: expected status=ok, got {health}")
    print(f"/v1/mobile/health: {status} service={health.get('service')}, version={health.get('version')}")

    if args.api_key:
        assert_protected(base_url, f"/v1/mobile/config?{query}", args.timeout)

    for endpoint, expected_keys in READ_ENDPOINTS.items():
        status, payload = request_json(f"{base_url}{endpoint}?{query}", timeout=args.timeout, api_key=args.api_key)
        if status != 200:
            raise AssertionError(f"{endpoint}: expected 200, got {status}")
        assert_keys(endpoint, payload, expected_keys)
        print(f"{endpoint}: {status} {summarize(endpoint, payload)}")

    status, refresh = request_json(
        f"{base_url}/v1/mobile/refresh?{query}",
        method="POST",
        timeout=args.timeout,
        api_key=args.api_key,
    )
    if status != 200:
        raise AssertionError(f"/v1/mobile/refresh: expected 200, got {status}")
    assert_keys("/v1/mobile/refresh", refresh, REFRESH_KEYS)
    refresh_status = refresh["refresh"].get("status")
    reloads = refresh["refresh"].get("reload_endpoints", [])
    if refresh_status not in {"refreshed", "partial"} or not reloads:
        raise AssertionError(f"/v1/mobile/refresh: unexpected refresh block {refresh['refresh']}")
    print(
        "/v1/mobile/refresh:"
        f" {status} status={refresh_status}, cache_bust={refresh['refresh'].get('cache_bust')}, reloads={len(reloads)}"
    )

    assert_error(
        base_url,
        "/v1/mobile/open-option-shorts?sort=unsupported",
        400,
        "invalid_open_option_sort",
        args.timeout,
        args.api_key,
    )
    assert_error(base_url, "/v1/mobile/open-option-shorts?limit=-1", 400, "invalid_limit", args.timeout, args.api_key)
    assert_error(
        base_url,
        "/v1/mobile/performance/monthly?range=unsupported",
        400,
        "invalid_monthly_range",
        args.timeout,
        args.api_key,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"mobile API smoke failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
