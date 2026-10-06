"""A failed candidate must not move traffic; partial promotions roll back."""
import sys

import pytest
from scripts import deploy_verified as deploy


@pytest.mark.parametrize("fail_at", ["candidate", "second_promotion", None])
def test_release_order_and_rollback(monkeypatch, tmp_path, fail_at):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["deploy", "--project", "p", "--region", "r", "--image", "registry/image:tag", "--commit", "a" * 40])
    calls, staged, promoted = [], set(), set()
    def run(*args, parse=False):
        calls.append(args)
        if args[:4] == ("artifacts", "docker", "images", "describe"):
            return {"image_summary": {"digest": "sha256:test"}}
        kind, action, name = args[1:4]
        if kind == "services" and action == "describe":
            traffic = [{"revisionName": name + "-old", "percent": 100}]
            if name in staged:
                traffic.append({"tag": "release-candidate", "revisionName": name + "-new", "url": "https://candidate"})
            return {"status": {"traffic": traffic, "url": "https://live"}}
        if kind == "jobs" and action == "describe":
            image = "registry/image@sha256:test" if name in promoted else "old/image@sha256:old"
            return {"spec": {"template": {"spec": {"template": {"spec": {"containers": [{"image": image}]}}}}}}
        if kind == "services" and action == "update":
            staged.add(name)
        if kind == "revisions":
            return {"status": {"imageDigest": "registry/image@sha256:test"}}
        if kind == "services" and action == "update-traffic":
            if fail_at == "second_promotion" and name == "options-roi-web" and any("-new=100" in x for x in args):
                raise RuntimeError("promotion failed")
        if kind == "jobs" and action == "update":
            promoted.add(name)
    monkeypatch.setattr(deploy, "run", run)
    def check(url, expected):
        if fail_at == "candidate" and url.startswith("https://candidate"):
            raise RuntimeError("candidate failed")
    monkeypatch.setattr(deploy, "check_url", check)
    if fail_at:
        with pytest.raises(RuntimeError):
            deploy.main()
    else:
        deploy.main()
    moves = [x for x in calls if x[:3] == ("run", "services", "update-traffic")]
    if fail_at == "candidate":
        assert not any(any("--to-revisions=" in a for a in x) for x in moves)
        assert not promoted
    elif fail_at == "second_promotion":
        assert any("--to-revisions=options-roi-mobile-api-old=100" in x for x in moves)
        assert not promoted
    else:
        assert set(deploy.JOBS) == promoted
        first_promotion = next(i for i, x in enumerate(calls) if any("-new=100" in a for a in x))
        assert sum(x[:3] == ("run", "revisions", "describe") for x in calls[:first_promotion]) == 2
