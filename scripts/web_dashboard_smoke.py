from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Optional

import requests


def _password_from_args(args: argparse.Namespace) -> Optional[str]:
    if args.password:
        return args.password
    if args.password_file:
        return Path(args.password_file).read_text(encoding="utf-8").strip()
    return os.getenv("WEB_DASHBOARD_PASSWORD")


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke test the Options ROI web dashboard.")
    parser.add_argument("--base-url", required=True, help="Dashboard base URL, e.g. https://...run.app")
    parser.add_argument("--password", help="Dashboard password. Prefer env or --password-file.")
    parser.add_argument("--password-file", help="File containing dashboard password.")
    parser.add_argument("--timeout", type=float, default=120.0)
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    session = requests.Session()

    health = session.get(f"{base_url}/health", timeout=args.timeout)
    health.raise_for_status()
    if health.json().get("status") != "ok":
        raise AssertionError(f"unexpected health response: {health.text}")

    home = session.get(f"{base_url}/", timeout=args.timeout, allow_redirects=False)
    if home.status_code in {301, 302, 303, 307, 308} and home.headers.get("location") == "/login":
        password = _password_from_args(args)
        if not password:
            raise AssertionError("dashboard requires login but no password was provided")
        login = session.post(
            f"{base_url}/login",
            data={"password": password},
            timeout=args.timeout,
            allow_redirects=False,
        )
        if login.status_code not in {301, 302, 303, 307, 308}:
            raise AssertionError(f"login failed: status={login.status_code}")
        home = session.get(f"{base_url}/", timeout=args.timeout)
    else:
        home.raise_for_status()

    if home.status_code != 200:
        raise AssertionError(f"dashboard status={home.status_code}")
    for expected in ("Portfolio Dashboard", "IBKR Flex", "dashboard-data"):
        if expected not in home.text:
            raise AssertionError(f"dashboard HTML missing {expected!r}")

    api = session.get(f"{base_url}/api/dashboard", timeout=args.timeout)
    api.raise_for_status()
    payload = api.json()
    if payload.get("source", {}).get("kind") != "ibkr_flex":
        raise AssertionError(f"unexpected source: {payload.get('source')}")
    issue_count = payload.get("dashboard", {}).get("issue_summary", {}).get("total_count")
    rows = payload.get("source", {}).get("row_count")
    print(
        "web_dashboard_smoke ok "
        f"source=ibkr_flex rows={rows} actionable_issues={issue_count}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
