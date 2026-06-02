# Mobile Contract Alignment Checklist

Use this as acceptance criteria when implementing either the backend DTO builders or the SwiftUI API client.

- Read endpoints are read-only. Price/data refresh only happens through `POST /v1/mobile/refresh`.
- Every SwiftUI list row has a deterministic `id`: `optlot:*`, `inventory:*`, `ticker:*`, `month:*`, or `year:*`.
- Numeric missing/suppressed values serialize as JSON `null`, never `NaN`, `"n/a"`, formatted strings, or omitted fields.
- Moneyness is backend-computed: puts use `(strike - current_price) / strike`, calls use `(current_price - strike) / strike`; positive means in the money for both.
- Every response includes `data_freshness` with populated `source_sheets`, price coverage, and timestamps when known.
- Issues use typed shape: `category`, `severity`, `message`, `tickers`, and `action`.
- Monthly target fields are explicit: `target_return`, `target_floor`, `target_basis`, `return_metric`, `target_pnl`, and `remaining_pnl`.
- Mobile and web use the same shared monthly target band setting; request query parameters are temporary overrides, not persistent client state.
- Partial refresh returns `refresh.status: "partial"` with freshness details; invalid requests use the standard error envelope.
