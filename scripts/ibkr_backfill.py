#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from portfolio_backend.ibkr.flex_client import FlexClient, parse_iso_date, plan_backfill_ranges  # noqa: E402
from portfolio_backend.ibkr.flex_parser import parse_flex_xml  # noqa: E402


def _object_path(output_dir: Path, query_id: str, chunk) -> Path:
    return output_dir / f"query-{query_id}" / f"{chunk.fd}-{chunk.td}.xml"


def _count_summary(xml_bytes: bytes) -> dict:
    report = parse_flex_xml(xml_bytes)
    return {
        "metadata": report.metadata,
        "counts": {
            section: report.section_counts.get(section, 0)
            for section in ("Trade", "OptionEAE", "CashTransaction", "OpenPosition", "SecurityInfo")
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill IBKR Flex Activity reports by fetching <=365-day date chunks."
    )
    parser.add_argument("--from", dest="from_date", required=True, help="Start date, YYYY-MM-DD")
    parser.add_argument("--to", dest="to_date", default=date.today().isoformat(), help="End date, YYYY-MM-DD")
    parser.add_argument("--env", default=str(REPO_ROOT / ".env"), help="Path to local .env with IBKR Flex secrets")
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "tmp" / "ibkr_backfill"))
    parser.add_argument(
        "--max-days",
        type=int,
        default=365,
        help="Maximum inclusive days per request. IBKR allows up to 365; smaller chunks can avoid unavailable statement ranges.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Only print planned chunks")
    parser.add_argument("--force", action="store_true", help="Refetch chunks even if XML already exists")
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Record failed chunks and continue with later ranges.",
    )
    parser.add_argument("--polls", type=int, default=30)
    parser.add_argument("--poll-interval", type=float, default=5.0)
    parser.add_argument(
        "--chunk-interval",
        type=float,
        default=30.0,
        help="Seconds to wait between chunks to avoid IBKR Flex token rate limiting.",
    )
    parser.add_argument(
        "--request-retries",
        type=int,
        default=3,
        help="Retry count for transient per-chunk request failures such as rate limiting.",
    )
    parser.add_argument(
        "--retry-interval",
        type=float,
        default=60.0,
        help="Seconds to wait before retrying a transient failed chunk.",
    )
    parser.add_argument(
        "--no-split-unavailable",
        dest="split_unavailable",
        action="store_false",
        help="Do not split ranges when IBKR says a statement is not available.",
    )
    parser.add_argument(
        "--min-split-days",
        type=int,
        default=30,
        help="Smallest inclusive date window to try when splitting unavailable statement ranges.",
    )
    args = parser.parse_args()

    start = parse_iso_date(args.from_date)
    end = parse_iso_date(args.to_date)
    chunks = plan_backfill_ranges(start, end, max_days=args.max_days)
    output_dir = Path(args.output_dir).expanduser()

    if args.dry_run:
        print(
            json.dumps(
                {
                    "from": start.isoformat(),
                    "to": end.isoformat(),
                    "chunks": [
                        {
                            "fd": chunk.fd,
                            "td": chunk.td,
                            "start": chunk.start.isoformat(),
                            "end": chunk.end.isoformat(),
                            "days": chunk.days_inclusive,
                        }
                        for chunk in chunks
                    ],
                },
                indent=2,
            )
        )
        return 0

    client = FlexClient.from_env(env_path=args.env)
    results = []
    state = {"fetches": 0}
    for chunk in chunks:
        _process_chunk(
            client,
            chunk,
            output_dir=output_dir,
            results=results,
            state=state,
            force=args.force,
            continue_on_error=args.continue_on_error,
            polls=args.polls,
            poll_interval=args.poll_interval,
            chunk_interval=args.chunk_interval,
            request_retries=args.request_retries,
            retry_interval=args.retry_interval,
            split_unavailable=args.split_unavailable,
            min_split_days=args.min_split_days,
        )

    print(
        json.dumps(
            {
                "query_id": client.query_id,
                "output_dir": str(output_dir),
                "chunks": len(chunks),
                "results": results,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _process_chunk(
    client,
    chunk,
    *,
    output_dir: Path,
    results: list[dict],
    state: dict,
    force: bool,
    continue_on_error: bool,
    polls: int,
    poll_interval: float,
    chunk_interval: float,
    request_retries: int,
    retry_interval: float,
    split_unavailable: bool,
    min_split_days: int,
) -> None:
    target = _object_path(output_dir, client.query_id, chunk)
    if target.exists() and not force:
        summary = _count_summary(target.read_bytes())
        results.append(
            {
                "fd": chunk.fd,
                "td": chunk.td,
                "status": "exists",
                "path": str(target),
                "bytes": target.stat().st_size,
                **summary,
            }
        )
        return

    target.parent.mkdir(parents=True, exist_ok=True)
    if state["fetches"] and chunk_interval > 0:
        time.sleep(chunk_interval)
    state["fetches"] += 1
    try:
        fetched = _fetch_with_retries(
            client,
            chunk,
            polls=polls,
            poll_interval=poll_interval,
            request_retries=request_retries,
            retry_interval=retry_interval,
        )
        target.write_bytes(fetched.report)
        summary = _count_summary(fetched.report)
        results.append(
            {
                "fd": chunk.fd,
                "td": chunk.td,
                "status": "fetched",
                "path": str(target),
                "bytes": len(fetched.report),
                "polls": fetched.polls,
                **summary,
            }
        )
        return
    except Exception as exc:
        error = str(exc)
        if split_unavailable and _is_statement_unavailable(error) and chunk.days_inclusive > min_split_days:
            left, right = _split_range(chunk)
            results.append(
                {
                    "fd": chunk.fd,
                    "td": chunk.td,
                    "status": "split",
                    "error": error,
                    "children": [left.label, right.label],
                }
            )
            for child in (left, right):
                _process_chunk(
                    client,
                    child,
                    output_dir=output_dir,
                    results=results,
                    state=state,
                    force=force,
                    continue_on_error=continue_on_error,
                    polls=polls,
                    poll_interval=poll_interval,
                    chunk_interval=chunk_interval,
                    request_retries=request_retries,
                    retry_interval=retry_interval,
                    split_unavailable=split_unavailable,
                    min_split_days=min_split_days,
                )
            return

        results.append(
            {
                "fd": chunk.fd,
                "td": chunk.td,
                "status": "failed",
                "error": error,
            }
        )
        if not continue_on_error:
            raise


def _fetch_with_retries(client, chunk, *, polls: int, poll_interval: float, request_retries: int, retry_interval: float):
    last_exc = None
    for attempt in range(1, max(1, request_retries) + 1):
        try:
            return client.fetch_statement(chunk, polls=polls, poll_interval=poll_interval)
        except Exception as exc:
            last_exc = exc
            text = str(exc).lower()
            transient = "1018" in text or "too many requests" in text or "temporarily" in text
            if not transient or attempt >= request_retries:
                raise
            time.sleep(retry_interval)
    raise last_exc  # type: ignore[misc]


def _is_statement_unavailable(error: str) -> bool:
    lowered = error.lower()
    return "1003" in lowered and "statement is not available" in lowered


def _split_range(chunk):
    first_days = max(1, chunk.days_inclusive // 2)
    left_end = chunk.start + timedelta(days=first_days - 1)
    right_start = left_end + timedelta(days=1)
    return type(chunk)(chunk.start, left_end), type(chunk)(right_start, chunk.end)


if __name__ == "__main__":
    raise SystemExit(main())
