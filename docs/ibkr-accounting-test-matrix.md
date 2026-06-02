# IBKR Accounting Acceptance Matrix

This matrix is the gate for IBKR wheel-accounting changes. Production logic
must not change unless the relevant rule here and in
`docs/ibkr-accounting-rules.md` is updated first, then covered by a regression
test. If a case is not covered, it is not production-ready.

## Non-Negotiable Rules

1. Wheel strategy starts with a short put.
2. Short calls count only when backed by prior short-put assignment inventory.
3. Stock P&L counts only for assignment-derived shares.
4. Dividends count only during assignment-derived holding periods.
5. Non-rolled options realize on close, expiration, or assignment, never on
   open.
6. Roll netting applies only when IBKR execution IDs prove the close and
   replacement belong to the same roll order.
7. A non-wheel call roll chain stays non-wheel until it is closed.
8. Identical open contracts display as one position, while execution lots stay
   separate for audit and realized P&L matching.

## Required Case Coverage

| ID | Case | Expected behavior | Regression coverage | Status |
|---|---|---|---|---|
| PUT-EXP-001 | Short put expires worthless | Realize premium net of fees on expiration date/year. No stock/dividend impact. | `test_ibkr_short_put_expiration_books_premium_without_stock`; `test_ibkr_pipeline_reports_non_rolled_option_on_close_or_expiration_year` | Covered |
| PUT-CLOSE-001 | Short put bought to close | Realize open credit minus close debit/fees on close date. Remaining partial quantity stays open. | `test_ibkr_partial_buy_to_close_preserves_remaining_short_lot` | Covered |
| PUT-ASSIGN-001 | Short put assigned | Realize option premium at assignment. Create stock buy only when option-side put assignment and stock-side buy evidence match. | `test_ibkr_short_put_assignment_creates_stock_buy`; `test_ibkr_assignment_book_trade_does_not_mask_assignment`; `test_ibkr_stock_side_buy_without_put_assignment_does_not_seed_wheel_call_inventory` | Covered |
| CALL-NOWHEEL-001 | Call without prior assigned-put inventory | Exclude call open/close/assignment from wheel option P&L. Do not include dividends. | `test_ibkr_yearly_performance_excludes_call_without_prior_put_assignment_inventory`; `test_ibkr_wheel_option_executions_include_calls_only_against_assignment_inventory`; `test_ibkr_stock_side_buy_without_put_assignment_does_not_seed_wheel_call_inventory` | Covered |
| CALL-EXP-001 | Wheel covered call expires worthless | Include only if covered by assignment-derived shares. Realize premium on expiration. Keep stock inventory open. | `test_ibkr_wheel_covered_call_expiration_realizes_premium_and_keeps_stock` | Covered |
| CALL-CLOSE-001 | Wheel covered call bought to close | Include only if backed by assignment-derived shares. Realize open credit minus close debit/fees on close date. | `test_ibkr_wheel_call_filter_does_not_reuse_same_shares_for_overlapping_calls` | Covered |
| CALL-ASSIGN-001 | Wheel covered call assigned | Realize call premium. Sell only matched assignment-derived shares FIFO. | `test_ibkr_short_call_assignment_creates_stock_sell`; `test_ibkr_wheel_stock_transactions_use_option_eae_stock_rows_and_ignore_uncovered_calls` | Covered |
| CALL-PARTIAL-ASSIGN-001 | Partial call assignment after put assignment | Sell only assigned quantity, realize stock P&L for sold shares, leave remaining assignment-derived inventory open. | `test_ibkr_partial_call_assignment_sells_only_assigned_quantity_and_keeps_remaining_inventory` | Covered |
| CALL-POOL-001 | Several calls share one original assigned put inventory pool | Multiple calls can match one prior put assignment up to available shares. Do not over-allocate beyond available assignment-derived inventory. | `test_ibkr_multiple_calls_can_share_one_prior_assigned_put_inventory_pool`; `test_ibkr_wheel_call_filter_does_not_reuse_same_shares_for_overlapping_calls` | Covered |
| CALL-PRORATE-001 | Call quantity exceeds assignment-derived shares | Include only covered quantity, prorate premium/fees, exclude uncovered quantity with warning. | `test_ibkr_wheel_options_dataframe_preserves_prorated_call_execution` | Covered |
| CALL-DISPLAY-001 | IBKR has multiple fills for same open contract | Display one open position by ticker/type/strike/expiration with summed quantity and weighted-average open price. Keep execution lots separate. | `test_build_open_options_frame_groups_same_contract_lots` | Covered |
| ROLL-CALL-001 | Same-order call roll | If close and replacement share IBKR execution group, net replacement credit into old close event and keep replacement open with zero unrecognized premium. Preserve roll-adjusted open premium separately for display/reconciliation. | `test_ibkr_pipeline_nets_same_day_roll_credit_on_close_date_without_double_counting_replacement`; `test_ibkr_pipeline_keeps_same_day_roll_replacement_open_with_zero_unrealized_premium` | Covered |
| ROLL-CALL-002 | Same-day unrelated call close/open | Do not net unless IBKR execution group proves a roll. | `test_ibkr_pipeline_does_not_net_unrelated_same_day_close_and_open` | Covered |
| ROLL-CALL-003 | Non-wheel call roll chain | Exclude close and replacement when the original call was non-wheel. Replacement must not enter wheel P&L later. | `test_ibkr_wheel_call_filter_keeps_excluded_roll_chain_out_of_wheel_pnl` | Covered |
| ROLL-PUT-001 | Same-order put roll | Same execution-group roll netting as calls, with no stock effect unless later assigned. | `test_ibkr_pipeline_nets_same_order_put_roll_without_double_counting_replacement` | Covered |
| MONTH-PROJ-001 | Monthly/current-cycle projection with rolled open replacement | Projected premium/P&L uses only real unrecognized open premium. `roll_adjusted_*` display values remain audit context and must not inflate active cycle, future cycle, dashboard, or mobile monthly projections. | `test_cycle_projection_keeps_risk_signals_and_roll_adjusted_display_values_out_of_projected_pnl`; `test_current_month_aliases_match_canonical_cycle_projection`; `test_future_monthly_performance_rows_emit_open_expiry_months`; `test_mobile_monthly_performance_matches_contract_fixture` | Covered |
| LONG-OPT-001 | Long option open/close | Preserve raw transactions. Exclude from short-option wheel metrics until long-option strategy support exists. | `test_ibkr_long_option_legs_are_excluded_from_short_strategy_adapter` | Covered |
| SPREAD-001 | Vertical put/call spread | Exclude paired spread legs from wheel P&L. Do not count the short leg alone as a wheel option. | `test_ibkr_vertical_put_spread_is_excluded_from_wheel_put_pnl` | Covered |
| STOCK-ASSIGN-001 | Stock bought through assigned put | Include only when matched to option-side put assignment. Cost basis from stock-side row/strike. | `test_ibkr_wheel_stock_transactions_use_option_eae_stock_rows_and_ignore_uncovered_calls`; `test_ibkr_stock_side_buy_without_put_assignment_does_not_seed_wheel_call_inventory` | Covered |
| STOCK-CALL-SELL-001 | Stock sold through assigned call | Consume assignment-derived inventory FIFO; ignore unmatched assigned-call shares. | `test_ibkr_wheel_stock_transactions_use_option_eae_stock_rows_and_ignore_uncovered_calls`; `test_ibkr_partial_call_assignment_sells_only_assigned_quantity_and_keeps_remaining_inventory` | Covered |
| STOCK-CALL-SELL-DEDUP-001 | Assigned-call stock sale duplicated by `OptionEAE` and `Trade`/`BookTrade` | Consume assignment-derived inventory once. Prefer the stock-side assignment movement and skip the matching book-trade row as a manual sell. | `test_ibkr_assignment_book_trade_stock_sell_is_not_double_counted` | Covered |
| STOCK-MANUAL-SELL-001 | Assignment-derived stock later sold manually | Manual stock sell consumes assignment-derived inventory FIFO and realizes wheel stock P&L for the matched shares only; unrelated manual stock sells remain excluded. | `test_ibkr_manual_stock_sell_consumes_assignment_inventory_only` | Covered |
| STOCK-MANUAL-BUY-001 | Manual stock buy followed by covered call | Exclude stock, call, dividends, and stock P&L from wheel metrics. | `test_ibkr_manual_stock_trade_is_preserved_as_normalized_transaction_but_not_option_row`; call exclusion tests | Covered |
| DIV-001 | Dividend during assignment-derived holding period | Include net dividend/withholding only for eligible assignment-derived shares and holding dates. | `test_ibkr_cashflows_preserve_net_dividend_components`; `test_ibkr_yearly_performance_combines_option_cash_stock_realized_and_net_dividends` | Covered |
| DIV-PRORATE-001 | Dividend on mixed wheel/non-wheel shares | Include only assignment-derived share portion. Use ex-date for eligibility when available. | `test_ibkr_dividends_are_prorated_to_assignment_derived_shares` | Covered |
| CASH-001 | Interest, fees, deposits, withdrawals | Store/classify separately; exclude from wheel option/stock/dividend P&L unless directly attached to a trade. | No explicit end-to-end exclusion tests for interest/fees/transfers. | Gap |
| PRIOR-001 | Positions before import window | Use automated backfill from `2022-11-01` as baseline for the agreed wheel inception date. If Firestore is empty or has gaps, auto mode plans the missing 365-day Flex ranges plus recent overlap. The newest target day is isolated so IBKR `1003` unavailability can be deferred without failing the available import. Pre-inception position seeding remains out of scope unless real evidence appears. | `test_plan_missing_import_ranges_backfills_full_inception_after_reset`; `test_plan_missing_import_ranges_backfills_gaps_and_recent_overlap`; `test_plan_missing_import_ranges_only_recent_overlap_when_coverage_complete`; `test_split_trailing_target_day_isolates_latest_calendar_day`; `test_auto_import_defers_only_unavailable_trailing_statement_day`; `test_auto_summary_reports_succeeded_with_deferred_trailing_day` | Covered for agreed inception scope |
| CORP-001 | Corporate action adjusts shares/contracts/basis | Preserve and flag first. Implement specific action only when encountered and documented. | No accounting regression yet. | Deferred |

## Remaining Non-Blocking Gaps

These are not blockers for the agreed wheel scope, but they must become
documented blockers if real IBKR activity matching the case appears:

1. `CASH-001`: interest, account fees, deposits, and withdrawals need explicit
   end-to-end exclusion/classification tests.
2. `CORP-001`: corporate actions should remain preserved and flagged until a
   real encountered action is documented and implemented.
3. Pre-`2022-11-01` position seeding is out of scope because the agreed options
   history starts in November 2022. If evidence of earlier wheel positions
   appears, `PRIOR-001` must be reopened before production use.

## Change Procedure

For every future IBKR accounting change:

1. Update `docs/ibkr-accounting-rules.md` if the intended behavior changes.
2. Update this matrix with the affected case ID.
3. Add or update the regression test before changing production logic.
4. Run targeted case tests and the full test suite.
5. Compare IBKR-mode yearly and ticker totals against the sheet baseline and
   explain every material difference before deployment.
