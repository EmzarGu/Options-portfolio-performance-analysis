from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional


DEFAULT_REGION = "europe-west6"
DEFAULT_IBKR_IMPORT_JOB = "ibkr-flex-import"


@dataclass(frozen=True)
class CloudRunJobStart:
    status: str
    job_name: str
    region: str
    project_id: str
    operation_name: Optional[str]
    started_at: str
    message: str

    def as_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "job_name": self.job_name,
            "region": self.region,
            "project_id": self.project_id,
            "operation_name": self.operation_name,
            "started_at": self.started_at,
            "message": self.message,
        }


def trigger_ibkr_import_job(
    *,
    project_id: Optional[str] = None,
    region: Optional[str] = None,
    job_name: Optional[str] = None,
    session: Any = None,
    timeout: int = 30,
) -> CloudRunJobStart:
    """Start the production IBKR import Cloud Run Job asynchronously."""
    resolved_project = project_id or _project_id()
    resolved_region = region or _region()
    resolved_job = job_name or os.getenv("IBKR_IMPORT_JOB_NAME", DEFAULT_IBKR_IMPORT_JOB)
    started_at = datetime.now().astimezone().isoformat(timespec="seconds")
    resolved_session = session or _authorized_session()
    url = (
        f"https://run.googleapis.com/v2/projects/{resolved_project}/"
        f"locations/{resolved_region}/jobs/{resolved_job}:run"
    )
    response = resolved_session.post(url, json={}, timeout=timeout)
    response.raise_for_status()
    payload = response.json() if getattr(response, "content", b"") else {}
    operation_name = payload.get("name") if isinstance(payload, dict) else None
    return CloudRunJobStart(
        status="started",
        job_name=resolved_job,
        region=resolved_region,
        project_id=resolved_project,
        operation_name=operation_name,
        started_at=started_at,
        message="IBKR import job started. Refresh data after the job finishes to load newly imported rows.",
    )


def _project_id() -> str:
    project = (
        os.getenv("GOOGLE_CLOUD_PROJECT")
        or os.getenv("GCP_PROJECT")
        or os.getenv("FIRESTORE_PROJECT_ID")
        or os.getenv("CLOUD_RUN_PROJECT")
    )
    if not project:
        raise RuntimeError("GOOGLE_CLOUD_PROJECT or FIRESTORE_PROJECT_ID is required to start the IBKR import job.")
    return project


def _region() -> str:
    return (
        os.getenv("CLOUD_RUN_REGION")
        or os.getenv("GOOGLE_CLOUD_REGION")
        or os.getenv("REGION")
        or DEFAULT_REGION
    )


def _authorized_session():
    try:
        import google.auth
        from google.auth.transport.requests import AuthorizedSession

        from portfolio_backend.gcp import service_account_credentials_from_config
    except ImportError as exc:  # pragma: no cover - dependency/runtime guard
        raise RuntimeError("google-auth is required to start Cloud Run jobs.") from exc

    credentials, _ = service_account_credentials_from_config()
    if credentials is None:
        credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return AuthorizedSession(credentials)
