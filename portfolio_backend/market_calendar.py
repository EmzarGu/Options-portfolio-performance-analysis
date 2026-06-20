from __future__ import annotations

from datetime import date, timedelta


def is_us_market_trading_day(value: date) -> bool:
    """Return whether US equity/options markets are normally open."""
    return value.weekday() < 5 and value not in us_market_holidays(value.year)


def previous_us_market_trading_day(value: date) -> date:
    current = value
    while not is_us_market_trading_day(current):
        current -= timedelta(days=1)
    return current


def next_us_market_trading_day(value: date) -> date:
    current = value
    while not is_us_market_trading_day(current):
        current += timedelta(days=1)
    return current


def us_market_holidays(year: int) -> set[date]:
    holidays = {
        _observed_fixed_holiday(year, 1, 1),
        _nth_weekday(year, 1, 0, 3),
        _nth_weekday(year, 2, 0, 3),
        _good_friday(year),
        _last_weekday(year, 5, 0),
        _observed_fixed_holiday(year, 6, 19),
        _observed_fixed_holiday(year, 7, 4),
        _nth_weekday(year, 9, 0, 1),
        _nth_weekday(year, 11, 3, 4),
        _observed_fixed_holiday(year, 12, 25),
    }
    next_new_year = _observed_fixed_holiday(year + 1, 1, 1)
    if next_new_year.year == year:
        holidays.add(next_new_year)
    return holidays


def _observed_fixed_holiday(year: int, month: int, day: int) -> date:
    value = date(year, month, day)
    if value.weekday() == 5:
        return value - timedelta(days=1)
    if value.weekday() == 6:
        return value + timedelta(days=1)
    return value


def _nth_weekday(year: int, month: int, weekday: int, occurrence: int) -> date:
    value = date(year, month, 1)
    return value + timedelta(days=(weekday - value.weekday()) % 7 + 7 * (occurrence - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    value = date(year + int(month == 12), 1 if month == 12 else month + 1, 1) - timedelta(days=1)
    while value.weekday() != weekday:
        value -= timedelta(days=1)
    return value


def _good_friday(year: int) -> date:
    return _easter_sunday(year) - timedelta(days=2)


def _easter_sunday(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)
