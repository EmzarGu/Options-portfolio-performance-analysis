from __future__ import annotations

from typing import Any, Iterable, Mapping, Optional

import pandas as pd

from portfolio_backend.constants import CONTRACT_MULTIPLIER


def number(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed


def canonical_open_price(open_price: Any, adjusted_open_price: Any = None) -> Optional[float]:
    """Canonical open option premium per share.

    New IBKR builds store the actual replacement premium in ``open_price``. The
    adjusted field remains only as a migration fallback for cached rows created
    before that accounting cleanup.
    """
    base = number(open_price)
    if base is not None and abs(base) > 1e-12:
        return base
    adjusted = number(adjusted_open_price)
    if adjusted is not None:
        return adjusted
    return base


def open_option_premium(
    *,
    quantity: Any,
    open_price: Any,
    adjusted_open_price: Any = None,
    multiplier: float = CONTRACT_MULTIPLIER,
) -> Optional[float]:
    price = canonical_open_price(open_price, adjusted_open_price)
    qty = number(quantity)
    if price is None or qty is None:
        return None
    return abs(qty) * price * multiplier


def first_number(values: Iterable[Any]) -> Optional[float]:
    for value in values:
        parsed = number(value)
        if parsed is not None:
            return parsed
    return None


def row_quantity(row: Mapping[str, Any]) -> Optional[float]:
    return first_number([row.get("quantity"), row.get("qty")])


def open_option_premium_from_row(row: Mapping[str, Any]) -> Optional[float]:
    computed = open_option_premium(
        quantity=row_quantity(row),
        open_price=row.get("open_price") if row.get("open_price") is not None else row.get("trade_price"),
        adjusted_open_price=row.get("roll_adjusted_open_price"),
    )
    if computed is not None:
        return computed
    explicit = first_number(
        [
            row.get("display_premium_collected"),
            row.get("roll_adjusted_premium_collected"),
            row.get("premium_collected"),
        ]
    )
    if explicit is not None:
        return explicit
    return None
