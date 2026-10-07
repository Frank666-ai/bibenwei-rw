"""Vectorized market features for RWUSD external-capital allocation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Optional

import numpy as np


FIVE_MINUTES_MS = 5 * 60 * 1000
THIRTY_MINUTES_MS = 30 * 60 * 1000
FIVE_MINUTE_DAY_CANDLE_COUNT = 24 * 60 // 5
ALLOCATION_HISTORY_CANDLE_COUNT = FIVE_MINUTE_DAY_CANDLE_COUNT + 1
MAX_LATEST_FIVE_MINUTE_CANDLE_AGE_MS = 2 * FIVE_MINUTES_MS
ATR4_PERIOD = 4
ATR14_PERIOD = 14
RSI12_PERIOD = 12


@dataclass(frozen=True)
class FiveMinuteMarketFeatures:
    range_5m: float = 0.0
    atr4: float = 0.0
    atr14: float = 0.0
    rsi12: float = 0.0
    previous_rsi12: float = 0.0
    latest_close_ms: int = 0
    blocked: bool = True
    reject_reason: str = "five_minute_history_incomplete"

    @property
    def rsi_cross_down_60(self) -> bool:
        return self.previous_rsi12 >= 60.0 and self.rsi12 < 60.0

    @property
    def rsi_cross_up_40(self) -> bool:
        return self.previous_rsi12 <= 40.0 and self.rsi12 > 40.0

    def to_dict(self) -> dict:
        return {
            "five_minute_range": self.range_5m,
            "five_minute_atr4": self.atr4,
            "five_minute_atr14": self.atr14,
            "rsi12": self.rsi12,
            "previous_rsi12": self.previous_rsi12,
            "rsi12_cross_down_60": self.rsi_cross_down_60,
            "rsi12_cross_up_40": self.rsi_cross_up_40,
            "five_minute_latest_close_ms": self.latest_close_ms,
            "five_minute_data_blocked": self.blocked,
            "five_minute_reject_reason": self.reject_reason or None,
        }


@dataclass(frozen=True)
class ThirtyMinuteMarketFeatures:
    range_30m: float = 0.0
    atr4: float = 0.0
    atr14: float = 0.0
    rsi12: float = 0.0
    previous_rsi12: float = 0.0
    latest_close_ms: int = 0
    blocked: bool = True
    reject_reason: str = "thirty_minute_history_incomplete"

    @property
    def rsi_cross_down_60(self) -> bool:
        return self.previous_rsi12 >= 60.0 and self.rsi12 < 60.0

    @property
    def rsi_cross_up_40(self) -> bool:
        return self.previous_rsi12 <= 40.0 and self.rsi12 > 40.0

    def to_dict(self) -> dict:
        return {
            "thirty_minute_range": self.range_30m,
            "thirty_minute_atr4": self.atr4,
            "thirty_minute_atr14": self.atr14,
            "thirty_minute_rsi12": self.rsi12,
            "thirty_minute_previous_rsi12": self.previous_rsi12,
            "thirty_minute_rsi12_cross_down_60": (
                self.rsi_cross_down_60
            ),
            "thirty_minute_rsi12_cross_up_40": self.rsi_cross_up_40,
            "thirty_minute_latest_close_ms": self.latest_close_ms,
            "thirty_minute_data_blocked": self.blocked,
            "thirty_minute_reject_reason": self.reject_reason or None,
        }


@dataclass(frozen=True)
class AllocationMarketFeatures:
    daily_return: float = 0.0
    staged_entry_gross_capacity: float = 0.0
    return_series: tuple[float, ...] = ()
    history_start_ms: int = 0
    history_end_ms: int = 0
    history_interval_ms: int = 0
    blocked: bool = True
    reject_reason: str = "market_data_unavailable"

    def to_dict(self) -> dict:
        return {
            "daily_return": self.daily_return,
            "staged_entry_gross_capacity": (
                self.staged_entry_gross_capacity
            ),
            "return_series": list(self.return_series),
            "history_start_ms": self.history_start_ms,
            "history_end_ms": self.history_end_ms,
            "history_interval_ms": self.history_interval_ms,
            "blocked": self.blocked,
            "reject_reason": self.reject_reason or None,
        }


def build_allocation_market_features(
    *,
    klines: Iterable[Mapping[str, object]],
    order_book: object,
    now_ms: int,
    depth_bps: float,
    opening_tranche_fraction: float,
    depth_utilization: float,
) -> AllocationMarketFeatures:
    completed = _completed_klines(
        klines,
        now_ms,
        interval_ms=FIVE_MINUTES_MS,
    )
    if len(completed) < ALLOCATION_HISTORY_CANDLE_COUNT:
        return AllocationMarketFeatures(
            reject_reason="five_minute_history_incomplete"
        )
    window = completed[-ALLOCATION_HISTORY_CANDLE_COUNT:]
    timestamps = _finite_array(window, "timestamp")
    highs = _finite_array(window, "high")
    lows = _finite_array(window, "low")
    closes = _finite_array(window, "close")
    if (
        timestamps is None
        or highs is None
        or lows is None
        or closes is None
        or np.any(np.diff(timestamps) != FIVE_MINUTES_MS)
        or np.any(highs <= 0)
        or np.any(lows <= 0)
        or np.any(closes <= 0)
        or np.any(highs < lows)
        or np.any(highs < closes)
        or np.any(lows > closes)
    ):
        return AllocationMarketFeatures(
            reject_reason="five_minute_history_invalid"
        )
    latest_close_ms = _kline_close_ms(
        window[-1],
        interval_ms=FIVE_MINUTES_MS,
    )
    if (
        latest_close_ms <= 0
        or int(now_ms) - latest_close_ms
        > MAX_LATEST_FIVE_MINUTE_CANDLE_AGE_MS
    ):
        return AllocationMarketFeatures(
            reject_reason="five_minute_history_stale"
        )
    close_returns = np.diff(np.log(closes))
    daily_return = float(closes[-1] / closes[0] - 1.0)
    staged_entry_capacity = _staged_entry_gross_capacity(
        order_book=order_book,
        depth_bps=depth_bps,
        opening_tranche_fraction=opening_tranche_fraction,
        depth_utilization=depth_utilization,
    )
    return AllocationMarketFeatures(
        daily_return=daily_return,
        staged_entry_gross_capacity=staged_entry_capacity,
        return_series=tuple(float(value) for value in close_returns),
        history_start_ms=int(timestamps[0]),
        history_end_ms=int(timestamps[-1]),
        history_interval_ms=FIVE_MINUTES_MS,
        blocked=staged_entry_capacity <= 0,
        reject_reason=(
            "entry_order_book_depth_unavailable"
            if staged_entry_capacity <= 0
            else ""
        ),
    )


def _completed_klines(
    klines: Iterable[Mapping[str, object]],
    now_ms: int,
    *,
    interval_ms: int,
) -> list[Mapping[str, object]]:
    rows = [
        row
        for row in klines
        if isinstance(row, Mapping)
        and _positive_float(row.get("timestamp")) is not None
    ]
    rows.sort(key=lambda row: float(row.get("timestamp", 0) or 0))
    completed = []
    for row in rows:
        opened = _positive_float(row.get("timestamp"))
        if opened is None:
            continue
        closes_at = _kline_close_ms(row, interval_ms=interval_ms)
        if closes_at <= int(now_ms):
            completed.append(row)
    return completed


def completed_24h_return(
    *,
    klines: Iterable[Mapping[str, object]],
    now_ms: int,
    lookback_candles: int = FIVE_MINUTE_DAY_CANDLE_COUNT,
) -> Optional[float]:
    completed = _completed_klines(
        klines,
        now_ms,
        interval_ms=FIVE_MINUTES_MS,
    )
    required = max(1, int(lookback_candles))
    if len(completed) < required + 1:
        return None
    window = completed[-(required + 1):]
    timestamps = _finite_array(window, "timestamp")
    closes = _finite_array(window, "close")
    latest_close_ms = _kline_close_ms(
        window[-1],
        interval_ms=FIVE_MINUTES_MS,
    )
    if (
        timestamps is None
        or closes is None
        or np.any(np.diff(timestamps) != FIVE_MINUTES_MS)
        or np.any(closes <= 0)
        or latest_close_ms <= 0
        or int(now_ms) - latest_close_ms
        > MAX_LATEST_FIVE_MINUTE_CANDLE_AGE_MS
    ):
        return None
    value = closes[-1] / closes[0] - 1.0
    return float(value) if np.isfinite(value) else None


def _kline_close_ms(
    row: Mapping[str, object],
    *,
    interval_ms: int,
) -> float:
    explicit_close = _positive_float(row.get("close_time"))
    if explicit_close is not None:
        return explicit_close
    opened = _positive_float(row.get("timestamp"))
    return (
        opened + interval_ms - 1
        if opened is not None
        else 0.0
    )


def build_five_minute_market_features(
    *,
    klines: Iterable[Mapping[str, object]],
    now_ms: int,
    max_latest_age_ms: int = MAX_LATEST_FIVE_MINUTE_CANDLE_AGE_MS,
) -> FiveMinuteMarketFeatures:
    completed = _completed_klines(
        klines,
        now_ms,
        interval_ms=FIVE_MINUTES_MS,
    )
    minimum_count = max(ATR14_PERIOD + 1, RSI12_PERIOD + 2)
    if len(completed) < minimum_count:
        return FiveMinuteMarketFeatures()
    timestamps = _finite_array(completed, "timestamp")
    highs = _finite_array(completed, "high")
    lows = _finite_array(completed, "low")
    closes = _finite_array(completed, "close")
    if (
        timestamps is None
        or highs is None
        or lows is None
        or closes is None
        or np.any(np.diff(timestamps) != FIVE_MINUTES_MS)
        or np.any(highs <= 0)
        or np.any(lows <= 0)
        or np.any(closes <= 0)
        or np.any(highs < lows)
        or np.any(highs < closes)
        or np.any(lows > closes)
    ):
        return FiveMinuteMarketFeatures(
            reject_reason="five_minute_history_invalid"
        )
    latest_close_ms = _kline_close_ms(
        completed[-1],
        interval_ms=FIVE_MINUTES_MS,
    )
    if (
        latest_close_ms <= 0
        or int(now_ms) < latest_close_ms
        or int(now_ms) - latest_close_ms > max(1, int(max_latest_age_ms))
    ):
        return FiveMinuteMarketFeatures(
            reject_reason="five_minute_history_stale"
        )
    previous_closes = np.roll(closes, 1)
    true_ranges = np.maximum(
        highs - lows,
        np.maximum(
            np.abs(highs - previous_closes),
            np.abs(lows - previous_closes),
        ),
    )
    true_ranges[0] = highs[0] - lows[0]
    atr4_series = _wilder_average(true_ranges, ATR4_PERIOD)
    atr14_series = _wilder_average(true_ranges, ATR14_PERIOD)
    rsi_series = _wilder_rsi(closes, RSI12_PERIOD)
    if (
        atr4_series is None
        or atr14_series is None
        or rsi_series is None
        or not np.isfinite(atr4_series[-1])
        or not np.isfinite(atr14_series[-1])
        or not np.isfinite(rsi_series[-1])
        or not np.isfinite(rsi_series[-2])
    ):
        return FiveMinuteMarketFeatures(
            reject_reason="five_minute_indicators_unavailable"
        )
    latest_close = closes[-1]
    range_5m = (highs[-1] - lows[-1]) / latest_close
    atr4 = atr4_series[-1] / latest_close
    atr14 = atr14_series[-1] / latest_close
    if (
        not np.isfinite(range_5m)
        or not np.isfinite(atr4)
        or not np.isfinite(atr14)
        or range_5m < 0
        or atr4 <= 0
        or atr14 <= 0
    ):
        return FiveMinuteMarketFeatures(
            reject_reason="five_minute_indicators_invalid"
        )
    return FiveMinuteMarketFeatures(
        range_5m=float(range_5m),
        atr4=float(atr4),
        atr14=float(atr14),
        rsi12=float(rsi_series[-1]),
        previous_rsi12=float(rsi_series[-2]),
        latest_close_ms=int(latest_close_ms),
        blocked=False,
        reject_reason="",
    )


def five_minute_volatility_allowed(
    *,
    range_5m: object,
    atr_value: object,
    maximum_range: float,
    atr_multiplier: float,
) -> bool:
    try:
        observed_range = float(range_5m)
        observed_atr = float(atr_value)
        maximum = float(maximum_range)
        multiplier = float(atr_multiplier)
    except (TypeError, ValueError):
        return False
    if not all(
        np.isfinite(value)
        for value in (observed_range, observed_atr, maximum, multiplier)
    ):
        return False
    if observed_range < 0 or observed_atr <= 0 or maximum <= 0:
        return False
    return observed_range <= min(maximum, multiplier * observed_atr) + 1e-12


def build_thirty_minute_market_features(
    *,
    klines: Iterable[Mapping[str, object]],
    now_ms: int,
    max_latest_age_ms: int = 2 * THIRTY_MINUTES_MS,
) -> ThirtyMinuteMarketFeatures:
    completed = _completed_klines(
        klines,
        now_ms,
        interval_ms=THIRTY_MINUTES_MS,
    )
    minimum_count = max(ATR14_PERIOD + 1, RSI12_PERIOD + 2)
    if len(completed) < minimum_count:
        return ThirtyMinuteMarketFeatures()
    timestamps = _finite_array(completed, "timestamp")
    highs = _finite_array(completed, "high")
    lows = _finite_array(completed, "low")
    closes = _finite_array(completed, "close")
    if (
        timestamps is None
        or highs is None
        or lows is None
        or closes is None
        or np.any(np.diff(timestamps) != THIRTY_MINUTES_MS)
        or np.any(highs <= 0)
        or np.any(lows <= 0)
        or np.any(closes <= 0)
        or np.any(highs < lows)
        or np.any(highs < closes)
        or np.any(lows > closes)
    ):
        return ThirtyMinuteMarketFeatures(
            reject_reason="thirty_minute_history_invalid"
        )
    latest_close_ms = _kline_close_ms(
        completed[-1],
        interval_ms=THIRTY_MINUTES_MS,
    )
    if (
        latest_close_ms <= 0
        or int(now_ms) < latest_close_ms
        or int(now_ms) - latest_close_ms > max(1, int(max_latest_age_ms))
    ):
        return ThirtyMinuteMarketFeatures(
            reject_reason="thirty_minute_history_stale"
        )
    previous_closes = np.roll(closes, 1)
    true_ranges = np.maximum(
        highs - lows,
        np.maximum(
            np.abs(highs - previous_closes),
            np.abs(lows - previous_closes),
        ),
    )
    true_ranges[0] = highs[0] - lows[0]
    atr4_series = _wilder_average(true_ranges, ATR4_PERIOD)
    atr14_series = _wilder_average(true_ranges, ATR14_PERIOD)
    rsi_series = _wilder_rsi(closes, RSI12_PERIOD)
    if (
        atr4_series is None
        or atr14_series is None
        or rsi_series is None
        or not np.isfinite(atr4_series[-1])
        or not np.isfinite(atr14_series[-1])
        or not np.isfinite(rsi_series[-1])
        or not np.isfinite(rsi_series[-2])
    ):
        return ThirtyMinuteMarketFeatures(
            reject_reason="thirty_minute_indicators_unavailable"
        )
    latest_close = closes[-1]
    range_30m = (highs[-1] - lows[-1]) / latest_close
    atr4 = atr4_series[-1] / latest_close
    atr14 = atr14_series[-1] / latest_close
    if (
        not np.isfinite(range_30m)
        or not np.isfinite(atr4)
        or not np.isfinite(atr14)
        or range_30m < 0
        or atr4 <= 0
        or atr14 <= 0
    ):
        return ThirtyMinuteMarketFeatures(
            reject_reason="thirty_minute_indicators_invalid"
        )
    return ThirtyMinuteMarketFeatures(
        range_30m=float(range_30m),
        atr4=float(atr4),
        atr14=float(atr14),
        rsi12=float(rsi_series[-1]),
        previous_rsi12=float(rsi_series[-2]),
        latest_close_ms=int(latest_close_ms),
        blocked=False,
        reject_reason="",
    )


def thirty_minute_row_volatility_allowed(
    row: Mapping[str, object],
    *,
    maximum_range: float,
    atr_multiplier: float,
    atr_key: str = "thirty_minute_atr14",
) -> bool:
    return (
        not bool(row.get("thirty_minute_data_blocked", False))
        and five_minute_volatility_allowed(
            range_5m=row.get("thirty_minute_range"),
            atr_value=row.get(atr_key),
            maximum_range=maximum_range,
            atr_multiplier=atr_multiplier,
        )
    )


def five_minute_row_volatility_allowed(
    row: Mapping[str, object],
    *,
    maximum_range: float,
    atr_multiplier: float,
    atr_key: str = "five_minute_atr14",
) -> bool:
    return (
        not bool(row.get("five_minute_data_blocked"))
        and five_minute_volatility_allowed(
            range_5m=row.get("five_minute_range"),
            atr_value=row.get(atr_key),
            maximum_range=maximum_range,
            atr_multiplier=atr_multiplier,
        )
    )


def _wilder_average(values: np.ndarray, period: int) -> Optional[np.ndarray]:
    if len(values) < period or period <= 0:
        return None
    output = np.full(len(values), np.nan, dtype=np.float64)
    output[period - 1] = float(np.mean(values[:period]))
    smoothing = float(period)
    for index in range(period, len(values)):
        output[index] = (
            output[index - 1] * (smoothing - 1.0) + values[index]
        ) / smoothing
    return output


def _wilder_rsi(
    closes: np.ndarray,
    period: int,
) -> Optional[np.ndarray]:
    if len(closes) < period + 1 or period <= 0:
        return None
    deltas = np.diff(closes)
    gains = np.maximum(deltas, 0.0)
    losses = np.maximum(-deltas, 0.0)
    average_gain = float(np.mean(gains[:period]))
    average_loss = float(np.mean(losses[:period]))
    output = np.full(len(closes), np.nan, dtype=np.float64)

    def _value(gain: float, loss: float) -> float:
        if loss <= 1e-15:
            return 100.0 if gain > 1e-15 else 50.0
        return 100.0 - 100.0 / (1.0 + gain / loss)

    output[period] = _value(average_gain, average_loss)
    smoothing = float(period)
    for index in range(period + 1, len(closes)):
        average_gain = (
            average_gain * (smoothing - 1.0) + gains[index - 1]
        ) / smoothing
        average_loss = (
            average_loss * (smoothing - 1.0) + losses[index - 1]
        ) / smoothing
        output[index] = _value(average_gain, average_loss)
    return output


def _finite_array(
    rows: list[Mapping[str, object]],
    field: str,
) -> Optional[np.ndarray]:
    try:
        values = np.asarray(
            [float(row.get(field, 0) or 0) for row in rows],
            dtype=np.float64,
        )
    except (TypeError, ValueError):
        return None
    if values.size != len(rows) or not np.all(np.isfinite(values)):
        return None
    return values


def _staged_entry_gross_capacity(
    *,
    order_book: object,
    depth_bps: float,
    opening_tranche_fraction: float,
    depth_utilization: float,
) -> float:
    data = order_book if isinstance(order_book, Mapping) else {}
    bids = _depth_levels(data.get("bids"))
    asks = _depth_levels(data.get("asks"))
    if not bids or not asks:
        return 0.0
    best_bid = max(price for price, _ in bids)
    best_ask = min(price for price, _ in asks)
    mid = (best_bid + best_ask) / 2.0
    if mid <= 0 or best_ask < best_bid:
        return 0.0
    band = max(0.1, float(depth_bps)) / 10000.0
    tolerance = mid * 1e-12
    bid_floor = mid * (1.0 - band) - tolerance
    ask_ceiling = mid * (1.0 + band) + tolerance
    bid_notional = sum(
        price * quantity
        for price, quantity in bids
        if price >= bid_floor
    )
    ask_notional = sum(
        price * quantity
        for price, quantity in asks
        if price <= ask_ceiling
    )
    per_side_depth = min(bid_notional, ask_notional)
    tranche = min(1.0, max(0.01, float(opening_tranche_fraction)))
    utilization = min(1.0, max(0.01, float(depth_utilization)))
    # This scales only the opening tranche on both sides. It is not an exit
    # liquidity estimate because later book depth is not yet observable.
    return max(0.0, 2.0 * per_side_depth * utilization / tranche)


def _depth_levels(value: object) -> list[tuple[float, float]]:
    output: list[tuple[float, float]] = []
    for row in value if isinstance(value, list) else []:
        try:
            price = float(row[0])
            quantity = float(row[1])
        except (IndexError, TypeError, ValueError):
            continue
        if (
            np.isfinite(price)
            and np.isfinite(quantity)
            and price > 0
            and quantity > 0
        ):
            output.append((price, quantity))
    return output


def _positive_float(value: object) -> Optional[float]:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if np.isfinite(parsed) and parsed >= 0 else None
