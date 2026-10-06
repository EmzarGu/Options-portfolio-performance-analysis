"""Stage tested Cloud Run revisions, verify them, then promote with rollback.

Called only after container tests pass. Configuration updates preserve existing
provider, schedule, scaling and accounting settings. No import is executed here.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

SERVICES = {"options-roi-mobile-api": ("mobile_api", "/v1/mobile/health", "/v1/mobile/config"),
            "options-roi-web": ("web_dashboard", "/health", "/api/dashboard")}
JOBS = {"ibkr-flex-import": ["-m", "portfolio_backend.ibkr.import_job"],
        "option-market-history-import": None}


def run(*args, parse=False):
    """Capture command results without printing service environment values."""
    result = subprocess.run(["gcloud", *args, "--quiet"], capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(f"gcloud {args[:4]} failed: {result.stderr[-3000:]}")
    return json.loads(result.stdout) if parse else result.stdout.strip()


def check_url(url, expected):
    """Allow cold-start time while requiring the expected HTTP status."""
    for attempt in range(6):
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                status = response.status
        except urllib.error.HTTPError as exc:
            status = exc.code
        except (urllib.error.URLError, TimeoutError):
            status = 0
        if status == expected:
            return
        if attempt < 5:
            time.sleep(5)
    raise RuntimeError(f"Verification failed: {url} expected={expected} actual={status}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("project", "region", "image", "commit"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.commit):
        raise ValueError("A full source commit SHA is required")
    scope = ["--project=" + args.project, "--region=" + args.region]
    image_info = run("artifacts", "docker", "images", "describe", args.image,
                     "--project=" + args.project, "--format=json", parse=True)
    digest = image_info["image_summary"]["digest"]
    image = args.image.rsplit(":", 1)[0] + "@" + digest
    before_services = {name: run("run", "services", "describe", name, *scope, "--format=json", parse=True) for name in SERVICES}
    before_jobs = {name: run("run", "jobs", "describe", name, *scope, "--format=json", parse=True) for name in JOBS}
    Path("release-rollback.json").write_text(json.dumps({"services": before_services, "jobs": before_jobs}, indent=2))
    promoted, updated_jobs, staged = [], [], {}
    try:
        for name, (module, health, protected) in SERVICES.items():
            extra = []
            if module == "web_dashboard":
                extra = ["--update-secrets=WEB_DASHBOARD_COOKIE_SECRET=options-roi-web-cookie-secret:1"]
            run("run", "services", "update", name, *scope, "--image=" + image,
                "--command=sh", "--args=-c,exec uvicorn " + module + ":app --host 0.0.0.0 --port ${PORT:-8080}",
                "--update-labels=commit-sha=" + args.commit,
                "--startup-probe=httpGet.path=" + health + ",httpGet.port=8080,periodSeconds=10,timeoutSeconds=5,failureThreshold=24",
                "--liveness-probe=httpGet.path=" + health + ",httpGet.port=8080,periodSeconds=60,timeoutSeconds=5,failureThreshold=3",
                "--no-traffic", "--tag=release-candidate", *extra)
            current = run("run", "services", "describe", name, *scope, "--format=json", parse=True)
            candidate = next(t for t in current["status"]["traffic"] if t.get("tag") == "release-candidate")
            revision = candidate["revisionName"]
            rev = run("run", "revisions", "describe", revision, *scope, "--format=json", parse=True)
            if digest not in rev["status"]["imageDigest"]:
                raise RuntimeError(f"Unexpected staged image for {name}")
            staged[name] = revision
            check_url(candidate["url"] + health, 200)
            check_url(candidate["url"] + protected, 401)
            if module == "web_dashboard":
                check_url(candidate["url"] + "/login", 200)
            print(f"Verified staged revision: {name} {revision}", flush=True)
        for name, revision in staged.items():
            # Record before the mutation so partial successes are rolled back too.
            promoted.append(name)
            run("run", "services", "update-traffic", name, *scope, "--to-revisions=" + revision + "=100", "--remove-tags=release-candidate")
            check_url(before_services[name]["status"]["url"] + SERVICES[name][1], 200)
        for name, command in JOBS.items():
            updated_jobs.append(name)
            extra = ["--command=python", "--args=" + ",".join(command)] if command else []
            run("run", "jobs", "update", name, *scope, "--image=" + image, "--update-labels=commit-sha=" + args.commit, *extra)
            job = run("run", "jobs", "describe", name, *scope, "--format=json", parse=True)
            if job["spec"]["template"]["spec"]["template"]["spec"]["containers"][0]["image"] != image:
                raise RuntimeError("Job image mismatch: " + name)
        print(json.dumps({"commit": args.commit, "image": image, "services": staged, "jobs": list(JOBS), "status": "verified"}), flush=True)
    except Exception:
        failures = []
        for name in reversed(promoted):
            traffic = ",".join(f"{t['revisionName']}={t['percent']}" for t in before_services[name]["status"]["traffic"] if t.get("percent"))
            try:
                run("run", "services", "update-traffic", name, *scope, "--to-revisions=" + traffic, "--remove-tags=release-candidate")
            except Exception:
                failures.append(name)
        for name in updated_jobs:
            old = before_jobs[name]["spec"]["template"]["spec"]["template"]["spec"]["containers"][0]["image"]
            try:
                run("run", "jobs", "update", name, *scope, "--image=" + old)
            except Exception:
                failures.append(name)
        for name in SERVICES:
            if name not in promoted:
                try:
                    run("run", "services", "update-traffic", name, *scope, "--remove-tags=release-candidate")
                except Exception:
                    failures.append(name)
        print(json.dumps({"status": "deployment_failed", "rollback_failures": failures}), flush=True)
        raise


if __name__ == "__main__":
    main()
