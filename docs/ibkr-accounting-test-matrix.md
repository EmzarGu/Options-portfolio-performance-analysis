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
| CALL-EXP-001 | Wheel covered call expires worthless | Include only if covered by assignment-derived shares. Realize premium on expiration. Keep stock inventory open. | Covered indirectly through option lifecycle and call-eligibility tests. Needs explicit end-to-end expiration test. | Gap |
| CALL-CLOSE-001 | Wheel covered call bought to close | Include only if backed by assignment-derived shares. Realize open credit minus close debit/fees on close date. | `test_ibkr_wheel_call_filter_does_not_reuse_same_shares_for_overlapping_calls` | Covered |
| CALL-ASSIGN-001 | Wheel covered call assigned | Realize call premium. Sell only matched assignment-derived shares FIFO. | `test_ibkr_short_call_assignment_creates_stock_sell`; `test_ibkr_wheel_stock_transactions_use_option_eae_stock_rows_and_ignore_uncovered_calls` | Covered |
| CALL-PARTIAL-ASSIGN-001 | Partial call assignment after put assignment | Sell only assigned quantity, realize stock P&L for sold shares, leave remaining assignment-derived inventory open. | `test_ibkr_partial_call_assignment_sells_only_assigned_quantity_and_keeps_remaining_inventory` | Covered |
| CALL-POOL-001 | Several calls share one original assigned put inventory pool | Multiple calls can match one prior put assignment up to available shares. Do not over-allocate beyond available assignment-derived inventory. | `test_ibkr_multiple_calls_can_share_one_prior_assigned_put_inventory_pool`; `test_ibkr_wheel_call_filter_does_not_reuse_same_shares_for_overlapping_calls` | Covered |
| CALL-PRORATE-001 | Call quantity exceeds assignment-derived shares | Include only covered quantity, prorate premium/fees, exclude uncovered quantity with warning. | `test_ibkr_wheel_options_dataframe_preserves_prorated_call_execution` | Covered |
| CALL-DISPLAY-001 | IBKR has multiple fills for same open contract | Display one open position by ticker/type/strike/expiration with summed quantity and weighted-average open price. Keep execution lots separate. | `test_build_open_options_frame_groups_same_contract_lots` | Covered |
| ROLL-CALL-001 | Same-order call roll | If close and replacement share IBKR execution group, net replacement credit into old close event and keep replacement open with zero unrecognized premium. | `test_ibkr_pipeline_nets_same_day_roll_credit_on_close_date_without_double_counting_replacement`; `test_ibkr_pipeline_keeps_same_day_roll_replacement_open_with_zero_unrealized_premium` | Covered |
| ROLL-CALL-002 | Same-day unrelated call close/open | Do not net unless IBKR execution group proves a roll. | `test_ibkr_pipeline_does_not_net_unrelated_same_day_close_and_open` | Covered |
| ROLL-CALL-003 | Non-wheel call roll chain | Exclude close and replacement when the original call was non-wheel. Replacement must not enter wheel P&L later. | `test_ibkr_wheel_call_filter_keeps_excluded_roll_chain_out_of_wheel_pnl` | Covered |
| ROLL-PUT-001 | Same-order put roll | Same execution-group roll netting as calls, with no stock effect unless later assigned. | No explicit put-roll end-to-end test. | Gap |
| LONG-OPT-001 | Long option open/close | Preserve raw transactions. Exclude from short-option wheel metrics until long-option strategy support exists. | `test_ibkr_long_option_legs_are_excluded_from_short_strategy_adapter` | Covered |
| STOCK-ASSIGN-001 | Stock bought through assigned put | Include only when matched to option-side put assignment. Cost basis from stock-side row/strike. | `test_ibkr_wheel_stock_transactions_use_option_eae_stock_rows_and_ignore_uncovered_calls`; `test_ibkr_stock_side_buy_without_put_assignment_does_not_seed_wheel_call_inventory` | Covered |
| STOCK-CALL-SELL-001 | Stock sold through assigned call | Consume assignment-derived inventory FIFO; ignore unmatched assigned-call shares. | `test_ibkr_wheel_stock_transactions_use_option_eae_stock_rows_and_ignore_uncovered_calls`; `test_ibkr_partial_call_assignment_sells_only_assigned_quantity_and_keeps_remaining_inventory` | Covered |
| STOCK-MANUAL-SELL-001 | Assignment-derived stock later sold manually | Manual stock sell should consume assignment-derived inventory FIFO and realize wheel stock P&L; unrelated manual stock sells remain excluded. | Current tests preserve stock trades but do not assert wheel consumption of manual sells after assignment. | Gap |
| STOCK-MANUAL-BUY-001 | Manual stock buy followed by covered call | Exclude stock, call, dividends, and stock P&L from wheel metrics. | `test_ibkr_manual_stock_trade_is_preserved_as_normalized_transaction_but_not_option_row`; call exclusion tests | Covered |
| DIV-001 | Dividend during assignment-derived holding period | Include net dividend/withholding only for eligible assignment-derived shares and holding dates. | `test_ibkr_cashflows_preserve_net_dividend_components`; `test_ibkr_yearly_performance_combines_option_cash_stock_realized_and_net_dividends` | Covered |
| DIV-PRORATE-001 | Dividend on mixed wheel/non-wheel shares | Include only assignment-derived share portion. Use ex-date for eligibility when available. | Covered in implementation path, but no explicit mixed-share regression. | Gap |
| CASH-001 | Interest, fees, deposits, withdrawals | Store/classify separately; exclude from wheel option/stock/dividend P&L unless directly attached to a trade. | No explicit end-to-end exclusion tests for interest/fees/transfers. | Gap |
| PRIOR-001 | Positions before import window | Use backfill from `2022-11-01` as baseline. If a required basis predates available data, flag missing basis instead of inventing P&L. | Import range planning tests exist; accounting seed behavior is not fully tested. | Gap |
| CORP-001 | Corporate action adjusts shares/contracts/basis | Preserve and flag first. Implement specific action only when encountered and documented. | No accounting regression yet. | Deferred |

## Immediate Blockers Before IBKR Becomes Production Source

These gaps are material because they can change reported P&L, inventory, or
dividends:

1. `STOCK-MANUAL-SELL-001`: assignment-derived stock sold manually after a put
   assignment must be included in wheel stock P&L. This is the Apple-style case.
2. `ROLL-PUT-001`: put rolls need the same explicit end-to-end protection as
   call rolls.
3. `DIV-PRORATE-001`: dividend allocation must be explicitly tested for mixed
   wheel/non-wheel holdings.
4. `CALL-EXP-001`: wheel covered-call expiration needs a direct pipeline test.
5. `PRIOR-001`: missing baseline/basis must fail loudly or warn clearly.

## Change Procedure

For every future IBKR accounting change:

1. Update `docs/ibkr-accounting-rules.md` if the intended behavior changes.
2. Update this matrix with the affected case ID.
3. Add or update the regression test before changing production logic.
4. Run targeted case tests and the full test suite.
5. Compare IBKR-mode yearly and ticker totals against the sheet baseline and
   explain every material difference before deployment.
