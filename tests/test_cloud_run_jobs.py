from __future__ import annotations

from portfolio_backend.cloud_run_jobs import trigger_ibkr_import_job


class FakeResponse:
    content = b"{}"

    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self):
        self.calls = []

    def post(self, url, *, json, timeout):
        self.calls.append({"url": url, "json": json, "timeout": timeout})
        return FakeResponse({"name": "operations/import-started"})


def test_trigger_ibkr_import_job_uses_cloud_run_v2_run_endpoint():
    session = FakeSession()

    started = trigger_ibkr_import_job(
        project_id="project-1",
        region="europe-west6",
        job_name="ibkr-flex-import",
        session=session,
    )

    assert session.calls == [
        {
            "url": "https://run.googleapis.com/v2/projects/project-1/locations/europe-west6/jobs/ibkr-flex-import:run",
            "json": {},
            "timeout": 30,
        }
    ]
    assert started.status == "started"
    assert started.operation_name == "operations/import-started"
    assert started.as_dict()["job_name"] == "ibkr-flex-import"
