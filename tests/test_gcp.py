from portfolio_backend import gcp


def test_cloud_run_firestore_uses_adc_even_when_google_sheet_secret_exists(monkeypatch):
    monkeypatch.setenv("K_SERVICE", "options-roi-mobile-api")
    monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_JSON", "not-firestore-credentials")
    monkeypatch.delenv("FIRESTORE_SERVICE_ACCOUNT_JSON", raising=False)

    credentials, project = gcp.service_account_credentials_from_config()

    assert credentials is None
    assert project is None


def test_firestore_service_account_secret_takes_precedence(monkeypatch):
    monkeypatch.setenv("GOOGLE_SERVICE_ACCOUNT_JSON", "google-sheet-secret")
    monkeypatch.setenv("FIRESTORE_SERVICE_ACCOUNT_JSON", "firestore-secret")

    assert gcp._service_account_secret() == "firestore-secret"
