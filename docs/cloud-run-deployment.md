# Cloud Run deployment

Use the version-controlled, test-gated pipeline described in [README](../README.md)
and [the hardening record](production-hardening-2026-10-06.md). It stages web/mobile
revisions and updates the IBKR import job using one tested image digest. The
obsolete CuteMarkets historical import job and its schedule were retired on
11 October 2026 and must not be recreated by releases.

The benchmark previously requested as PUTW now uses WTPI. The fund's ticker,
name and investment policy changed on 4 April 2025, as documented in the
[WisdomTree prospectus supplement](https://www.sec.gov/Archives/edgar/data/1350487/000121465925005428/z43251497.htm).
The Performance page discloses that returns before this date reflect the former
policy. Snapshot schema 10 rebuilds previously cached PUTW comparisons. A live
Yahoo price-history check on 11 October 2026 returned 946 daily observations
from 3 January 2023 through 9 October 2026, with no missing completed months.

Use the attached runtime service identity for Google Cloud APIs. Store application
secrets in Secret Manager and bind only the references to the services. Do not
paste service-account private keys into deployment commands or commit them.

For local configuration see the web/mobile runbooks. For rollback retain the
previous service revisions and job image digests recorded before deployment.
