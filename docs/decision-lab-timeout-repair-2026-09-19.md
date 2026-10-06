# Decision Lab timeout repair — 2026-09-19

The user approved this repair after observing a five-minute production timeout.
Eighteen serial CuteMarkets connection timeouts had blocked the whole page. The
frontend then parsed the gateway's text error as JSON, hiding the underlying cause.

## Resulting behavior

- Normal page loads reuse stored contracts and never fetch option chains. With no
  stored data, the action queue and history load with an explicit availability message.
- Fetch option data remains manual. A shared 30-second provider budget covers
  chains, pagination, throttling and retries; connection attempts use at most five
  seconds. Connection failures and service-wide HTTP failures stop further chains.
  This budget applies to provider work; database and portfolio loading add overhead.
- Failed/partial chains cannot overwrite the last good stored contracts. A failed
  refresh can reuse existing chains even when no successful universe run exists.
- Both integrated and standalone pages handle plain-text HTTP failures and display
  provider status messages. The derived Lab cache version is increased to discard
  payloads assembled before the new behavior.
- Provider selection, subscription, candidate scoring and portfolio accounting are
  unchanged. No background collector or new dependency is introduced.

## Verification

- Local Python suite: 464 passed. The repository's make wrapper remains blocked
  by the previously observed unaccepted Xcode license; the equivalent project-venv
  pytest invocation was used without changing system settings.
- Seven new regression tests cover uncached reads, 18-chain outage short-circuiting,
  failed partial-chain preservation, shared deadline, excessive Retry-After,
  pagination deadline, and the authenticated application route with a missing cache.
- Both pages' embedded JavaScript passed syntax checks. Four executable response
  tests cover successful JSON, plain-text 504, JSON 401 and malformed success data.
- Production container suite: 464 passed, one warning, in 17.10 seconds.
- Authenticated production browser: uncached Decision Lab GET returned 200 in
  1.44 seconds. Manual refresh returned 200 in 5.94 seconds; its fetch run recorded
  one failed connection and 17 skipped chains. The page displayed the readable
  unavailable-service message and all nine action rows.
- Standalone Decision Lab also rendered its portfolio and history with the same
  provider failure message. No new provider was selected or subscribed.

## Release

The release overlays five changed application modules on the exact previously
verified production image, with no dependency rebuild. Only the web service is
updated; the mobile API and import jobs do not use this page-loading path.
Previous web revision: `options-roi-web-00119-wnr`.
Build: `8a9b7b97-789a-4754-91ab-ae4d0e541ece`.

Deployed web revision: `options-roi-web-00120-94n`, ready with 100% traffic.
Build completed successfully at 2026-09-19 20:22:07 UTC.
Image digest: `sha256:f264c181859ab3ff21880c0b89f5763fc15ac9a329d1aac15779f37db05a57f8`.
