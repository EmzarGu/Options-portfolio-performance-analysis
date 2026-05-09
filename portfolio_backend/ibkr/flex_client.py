from __future__ import annotations

import os
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional

from portfolio_backend.ibkr.flex_parser import strip_namespace


BASE_URL = "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService"
MAX_FLEX_RANGE_DAYS = 365


@dataclass(frozen=True)
class DateRange:
    start: date
    end: date

    @property
    def fd(self) -> str:
        return self.start.strftime("%Y%m%d")

    @property
    def td(self) -> str:
        return self.end.strftime("%Y%m%d")

    @property
    def label(self) -> str:
        return f"{self.fd}-{self.td}"

    @property
    def days_inclusive(self) -> int:
        return (self.end - self.start).days + 1


@dataclass(frozen=True)
class FlexFetchResult:
    reference_code: str
    report: bytes
    polls: int


def load_env(path: str | Path | None) -> None:
    if not path:
        return
    env_path = Path(path).expanduser()
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def plan_backfill_ranges(start: date, end: date, max_days: int = MAX_FLEX_RANGE_DAYS) -> list[DateRange]:
    if max_days < 1:
        raise ValueError("max_days must be positive")
    if end < start:
        raise ValueError("end must be on or after start")
    ranges: list[DateRange] = []
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=max_days - 1), end)
        ranges.append(DateRange(current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return ranges


def parse_iso_date(value: str) -> date:
    return date.fromisoformat(value)


class FlexClient:
    def __init__(
        self,
        token: str,
        query_id: str,
        *,
        base_url: str = BASE_URL,
        user_agent: str = "options-portfolio-ibkr-flex/1.0",
        timeout: int = 30,
    ) -> None:
        if not token:
            raise ValueError("IBKR Flex token is required")
        if not query_id:
            raise ValueError("IBKR Flex query ID is required")
        self.token = token
        self.query_id = query_id
        self.base_url = base_url.rstrip("/")
        self.user_agent = user_agent
        self.timeout = timeout

    @classmethod
    def from_env(
        cls,
        *,
        env_path: str | Path | None = None,
        token_env: str = "IBKR_FLEX_TOKEN",
        query_id_env: str = "IBKR_FLEX_QUERY_ID",
    ) -> "FlexClient":
        load_env(env_path)
        return cls(
            token=os.environ.get(token_env, "").strip(),
            query_id=os.environ.get(query_id_env, "").strip(),
        )

    def send_request(self, date_range: Optional[DateRange] = None) -> str:
        params = {"t": self.token, "q": self.query_id, "v": "3"}
        if date_range is not None:
            if date_range.days_inclusive > MAX_FLEX_RANGE_DAYS:
                raise ValueError("IBKR Flex date override ranges must be 365 days or less")
            params.update({"fd": date_range.fd, "td": date_range.td})
        root = self._request_xml("/SendRequest", params)
        error = _error_from_xml(root)
        if error:
            raise RuntimeError(f"IBKR SendRequest error: {error}")
        reference = _xml_text(root, "ReferenceCode", "referenceCode")
        if not reference:
            raise RuntimeError("IBKR SendRequest did not return a reference code")
        return reference

    def get_statement(self, reference_code: str) -> bytes:
        return self._request_bytes("/GetStatement", {"t": self.token, "q": reference_code, "v": "3"})

    def fetch_statement(
        self,
        date_range: Optional[DateRange] = None,
        *,
        polls: int = 30,
        poll_interval: float = 5.0,
    ) -> FlexFetchResult:
        reference = self.send_request(date_range)
        last_processing = ""
        for attempt in range(1, polls + 1):
            report = self.get_statement(reference)
            processing = processing_message(report)
            if not processing:
                return FlexFetchResult(reference_code=reference, report=report, polls=attempt)
            last_processing = processing
            if attempt < polls:
                time.sleep(poll_interval)
        raise RuntimeError(f"Report still not available after {polls} polls: {last_processing}")

    def _request_bytes(self, path: str, params: Dict[str, str]) -> bytes:
        url = f"{self.base_url}{path}?{urllib.parse.urlencode(params)}"
        request = urllib.request.Request(url, headers={"User-Agent": self.user_agent})
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return response.read()
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"IBKR HTTP {exc.code}: {body[:500]}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"IBKR request failed: {exc.reason}") from exc

    def _request_xml(self, path: str, params: Dict[str, str]) -> ET.Element:
        data = self._request_bytes(path, params)
        try:
            return ET.fromstring(data)
        except ET.ParseError as exc:
            preview = data.decode("utf-8", errors="replace")[:500]
            raise RuntimeError(f"Expected XML response, got: {preview}") from exc


def processing_message(data: bytes) -> Optional[str]:
    try:
        root = ET.fromstring(data)
    except ET.ParseError:
        return None
    error = _error_from_xml(root)
    if not error:
        return None
    lowered = error.lower()
    if "processing" in lowered or "in progress" in lowered or "not ready" in lowered or "temporarily" in lowered:
        return error
    raise RuntimeError(f"IBKR GetStatement error: {error}")


def _xml_text(root: ET.Element, *names: str) -> Optional[str]:
    wanted = {name.lower() for name in names}
    for elem in root.iter():
        if strip_namespace(elem.tag).lower() in wanted and elem.text:
            return elem.text.strip()
    return None


def _error_from_xml(root: ET.Element) -> Optional[str]:
    code = _xml_text(root, "ErrorCode", "errorCode", "code")
    message = _xml_text(root, "ErrorMessage", "errorMessage", "message")
    if code or message:
        return f"{code or 'ERROR'}: {message or 'No message'}"
    return None


def iter_existing_reports(output_dir: Path) -> Iterator[Path]:
    if not output_dir.exists():
        return iter(())
    return iter(sorted(output_dir.glob("**/*.xml")))
