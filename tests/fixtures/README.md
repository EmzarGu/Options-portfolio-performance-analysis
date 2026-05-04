# Test Fixtures

`mobile_portfolio_payload_v1.json` is a legacy generic backend payload fixture
for `portfolio_backend.api_payloads.build_portfolio_payload`. Despite the file
name, it is not the mobile API contract described in
`docs/mobile-api-contract.md`.

Mobile contract fixtures should be added separately once
`portfolio_backend.mobile_payloads` covers a full endpoint shape.

`mobile_dashboard_v1.json` is the first mobile contract fixture. It covers the
launch dashboard payload emitted by `build_mobile_dashboard`.

`mobile_positions_v1.json` covers the mobile positions payload emitted by
`build_mobile_positions`, including assigned stock inventory and open option
short rows.

`mobile_open_option_shorts_v1.json` covers the standalone mobile open-option
shorts payload emitted by `build_mobile_open_option_shorts`, including the
moneyness legend and risk-sorted option rows.

`mobile_tickers_v1.json` covers the mobile ticker list/detail source emitted by
`build_mobile_tickers`, including optional yearly history rows.

`mobile_monthly_performance_v1.json` covers the mobile monthly performance
payload emitted by `build_mobile_monthly_performance`, including target tracking
for the current month and filtered monthly rows.

`mobile_yearly_performance_v1.json` covers the mobile yearly performance payload
emitted by `build_mobile_yearly_performance`, including unrealized current-year
adjustment fields and capital-history metric availability.

`mobile_issues_v1.json` covers the mobile data health payload emitted by
`build_mobile_issues`, including typed issue rows and coverage summaries.

`mobile_refresh_v1.json` covers the explicit mobile refresh payload emitted by
`build_mobile_refresh`, including freshness metadata, partial-refresh status,
and the read endpoints the client should reload after refresh succeeds.

`mobile_config_v1.json` covers the mobile configuration payload emitted by
`build_mobile_config`, including available sheets, default sheet selection, and
source metadata.
