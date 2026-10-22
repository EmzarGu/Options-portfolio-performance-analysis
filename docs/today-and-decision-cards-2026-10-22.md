# Today and Decision cards tabs (2026-10-22)

Two tabs were added to the web dashboard as a preview of a decision-first
layout. Every existing tab, route and payload is unchanged, and Dashboard stays
the landing page. The new tabs are marked `NEW` in the navigation.

## Today

One screen for the weekly wheel review:

- **Freshness line.** IBKR statement end date (`source_metadata.ibkr_import_to_date`),
  price time, and the Decision Lab quote date. The dot turns amber when the
  statement is more than three days old and red when prices are missing or
  data issues exist.
- **Cycle gauge.** The active cycle's projected RoAC against the monthly
  target band from Settings, with its four parts: realized this cycle, premium
  still open, stock P&L if ITM calls assign, and loss if ITM puts assign. All
  values come from `monthly.active_cycle` (`cycle_projection.py`).
- **Year to date.** `roac_year` for the current year against a straight-line
  pace for the annual target in Decision rules (default 20%).
- **Capital at work.** Put notional, in-the-money notional, cash needed if ITM
  puts assign, assigned shares at cost, and available cash when IBKR cash is
  imported.
- **Action queue.** The Decision Lab's ticker situations grouped into
  *Act now*, *Plan this week* and *Monitor*. A row opens that ticker's
  decision card in place.
- **Risk map and put notional by expiry,** drawn from the open option shorts.

Labels use plain wording. Dotted labels show the definition on hover; each
definition maps to the existing field and the accounting rules in
[ibkr-accounting-rules.md](ibkr-accounting-rules.md).

## Decision cards

One card per ticker that the Decision Lab prices, side by side:

| Column | Source |
| --- | --- |
| Keep | The current-position baseline candidate (`is_current_position`) |
| Close now | The baseline's close cost. Chain result = carried chain net credit (`premium`) minus close cost, which is how a final close is recognized under the completed-chain rule |
| Alternatives | The Decision Lab's top two scored candidates, in its own order |

Each alternative is checked against the owner's entry rules: DTE range, put
delta range (puts only) and the existing 35% bid/ask spread limit. The top
alternative is marked only when it scores better than keeping; otherwise the
card says no alternative beats the current position.

## Decision rules setting

Settings has a new **Decision rules** panel stored in Firestore
`app_settings/decision_rules`:

| Field | Default |
| --- | --- |
| `annual_target_return` | 0.20 |
| `put_delta_min` / `put_delta_max` | 0.15 / 0.25 |
| `dte_min` / `dte_max` | 20 / 45 |

The rules only drive the checks and the yearly pace. They do not filter
candidates or change any calculation.

## Code

- `portfolio_backend/decision_cards.py` groups situations (`action_group`) and
  builds cards (`with_decision_cards`). It reshapes Decision Lab output only.
- `/api/decision-lab` and `/api/decision-lab/options/refresh` add
  `action_queue`, `decision_cards` and `decision_rules` to their responses. The
  persisted Decision Lab cache is unchanged; cards are built per response.
- `GET`/`POST /api/settings/decision-rules` read and save the rules (signed-in
  only, validated ranges).
- Rendering lives in `portfolio_backend/web_dashboard_templates.py`
  (`renderToday`, `renderDecisionCards`, `decisionRulesPanel`).
- Tests: `tests/test_decision_cards.py`.

The grouping matches the local visual prototype's `_action_group`; the
prototype itself is unchanged.
