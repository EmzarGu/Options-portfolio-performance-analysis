from __future__ import annotations

import ast
import json
import os
import re
from typing import Any, Optional


def firestore_client(*, project: Optional[str] = None, database: str = "(default)"):
    from google.cloud import firestore

    credentials, credential_project = service_account_credentials_from_config()
    resolved_project = project or os.getenv("FIRESTORE_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT") or credential_project
    if credentials is not None:
        return firestore.Client(project=resolved_project, database=database, credentials=credentials)
    return firestore.Client(project=resolved_project, database=database)


def service_account_credentials_from_config():
    raw = _service_account_secret()
    if raw is None:
        return None, None

    from google.oauth2 import service_account

    info = _parse_service_account_info(raw)
    credentials = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return credentials, info.get("project_id")


def _service_account_secret() -> Any:
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if raw:
        return raw

    try:
        import streamlit as st

        raw = st.secrets.get("GOOGLE_SERVICE_ACCOUNT_JSON")
        if raw is not None:
            return raw
        for key in ("gcp_service_account", "service_account"):
            if key in st.secrets:
                return st.secrets[key]
    except Exception:
        return None
    return None


def _parse_service_account_info(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str):
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON must be a JSON string or table.")

    text = raw.strip()
    for triple in ('"""', "'''"):
        if text.startswith(triple) and text.endswith(triple):
            text = text[len(triple) : -len(triple)].strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    try:
        fixed = re.sub(
            r'"private_key"\s*:\s*"(.*?)"',
            lambda match: '"private_key": "' + match.group(1).replace("\r\n", "\n").replace("\n", "\\n") + '"',
            text,
            flags=re.DOTALL,
        )
        return json.loads(fixed)
    except Exception:
        pass

    try:
        return json.loads(text.replace("'", '"'))
    except Exception:
        pass

    parsed = ast.literal_eval(text)
    if isinstance(parsed, dict):
        return parsed
    raise RuntimeError("Could not parse GOOGLE_SERVICE_ACCOUNT_JSON.")
