# Monitoring

Production alerting currently includes a logs-based metric and alert policy for
the IBKR Flex import job.

## IBKR Flex Import Failures

Metric:

```text
ibkr_flex_import_failures
```

Log filter:

```text
resource.type="cloud_run_job"
resource.labels.job_name="ibkr-flex-import"
(severity>=ERROR OR textPayload=~"(?i)(image not found|failed|traceback|error)" OR jsonPayload.status=~"failed")
```

Alert policy definition:

```text
infra/monitoring/ibkr_import_failure_policy.json
```

Apply/update:

```bash
gcloud logging metrics describe ibkr_flex_import_failures >/dev/null || \
gcloud logging metrics create ibkr_flex_import_failures \
  --description="Counts error logs from the ibkr-flex-import Cloud Run job." \
  --log-filter='resource.type="cloud_run_job" AND resource.labels.job_name="ibkr-flex-import" AND (severity>=ERROR OR textPayload=~"(?i)(image not found|failed|traceback|error)" OR jsonPayload.status=~"failed")'

gcloud monitoring policies create \
  --policy-from-file=infra/monitoring/ibkr_import_failure_policy.json
```

The app payloads separately flag stale successful imports when the latest
successful IBKR statement end date is older than `IBKR_IMPORT_STALE_DAYS`.
