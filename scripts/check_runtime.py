"""Smoke-test the actual runtime image without test or backup-UI dependencies."""
import importlib.util
import os
import sys

assert os.getuid() != 0, "Production must run as a non-root user"
for package in ("pytest", "streamlit", "altair"):
    assert importlib.util.find_spec(package) is None, f"Unexpected runtime dependency: {package}"
import mobile_api
import web_dashboard
import portfolio_backend.ibkr.import_job
assert "streamlit_app" not in sys.modules
assert os.getenv("APP_BUILD_VERSION") not in (None, "development")
print("Runtime imports, non-root execution and build identity verified.")
