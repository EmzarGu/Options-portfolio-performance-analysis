# Cloud Run deployment

Use the version-controlled, test-gated pipeline described in [README](../README.md)
and [the hardening record](production-hardening-2026-10-06.md). It stages web/mobile
revisions and updates both import jobs using one tested image digest.

Use the attached runtime service identity for Google Cloud APIs. Store application
secrets in Secret Manager and bind only the references to the services. Do not
paste service-account private keys into deployment commands or commit them.

For local configuration see the web/mobile runbooks. For rollback retain the
previous service revisions and job image digests recorded before deployment.
