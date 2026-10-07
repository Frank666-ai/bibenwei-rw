"""Fast market snapshots for active RWUSD symbol lanes."""

from __future__ import annotations

import asyncio
import math
import time
from typing import Dict, Iterable, List, Tuple

from backend.strategy.coin_margined_rwusd.market_features import (
    build_thirty_minute_market_features,
    completed_24h_return,
    thirty_minute_row_volatility_allowed,
)
from backend.strategy.coin_margined_rwusd.models import MarketRow


class RWUSDMarketRuntime:
    def __init__(self, *, client: object, config: object) -> None:
        self.client = client
        self.config = config
        self.selector_input_cache: Dict[str, dict] = {}

    async def build_rows(
        self,
        symbols: Iterable[str],
    ) -> List[MarketRow]:
        requested = tuple(
            dict.fromkeys(
                str(symbol or "").strip().upper()
                for symbol in symbols
                if str(symbol or "").strip()
            )
        )
        if not requested:
            return []
        raw = await asyncio.gather(
            *(self._fetch_symbol_row(symbol) for symbol in requested),
            return_exceptions=True,
        )
        rows: List[MarketRow] = [
            item
            for item in raw
            if isinstance(item, dict) and float(item.get("close", 0) or 0) > 0
        ]
        for field in ("liquidity", "funding", "margin"):
            maximum = max(
                (float(row.get(field, 0) or 0) for row in rows),
                default=0.0,
            )
            for row in rows:
                value = max(0.0, float(row.get(field, 0) or 0))
                row[field] = (
                    math.sqrt(value / maximum) if maximum > 0 else 0.0
                )
        return rows

    async def _fetch_symbol_row(self, symbol: str) -> MarketRow:
        selector_inputs, ticker, book = await asyncio.gather(
            self._fetch_selector_inputs(symbol),
            self.client.get_ticker(symbol),
            self.client.get_book_ticker(symbol),
        )
        indicator_klines, allocation_klines, premium = selector_inputs
        now_ms = int(time.time() * 1000)
        daily_return = completed_24h_return(
            klines=allocation_klines or [],
            now_ms=now_ms,
        )
        thirty_minute_features = build_thirty_minute_market_features(
            klines=indicator_klines or [],
            now_ms=now_ms,
        )
        opening_volatility_allowed = (
            not thirty_minute_features.blocked
            and thirty_minute_row_volatility_allowed(
                thirty_minute_features.to_dict(),
                maximum_range=self.config.OPENING_VOLATILITY_MAX_RANGE,
                atr_multiplier=(
                    self.config.OPENING_VOLATILITY_ATR_MULTIPLIER
                ),
                atr_key="thirty_minute_atr4",
            )
        )
        closes = [
            float(item.get("close", 0) or 0)
            for item in (allocation_klines or [])
            if float(item.get("close", 0) or 0) > 0
        ]
        volumes = [
            max(0.0, float(item.get("volume", 0) or 0))
            for item in (allocation_klines or [])
        ]
        mark = float((ticker or {}).get("mark_price", 0) or 0)
        close = mark or float((ticker or {}).get("last_price", 0) or 0)
        if close <= 0 and closes:
            close = closes[-1]
        bid = float((book or {}).get("bid_price", 0) or 0)
        ask = float((book or {}).get("ask_price", 0) or 0)
        mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else close
        execution_cost_bps = (
            ((ask - bid) / mid) * 10000.0
            if mid > 0 and ask >= bid > 0
            else 999.0
        )
        volatility = (
            (max(closes) - min(closes)) / close
            if closes and close > 0
            else 0.0
        )
        liquidity = max(volumes) if volumes else 0.0
        quote_notional = sum(
            volume * price for volume, price in zip(volumes, closes)
        )
        basic_blocked = (
            close <= 0
            or liquidity <= 0
            or quote_notional <= 0
            or bid <= 0
            or ask <= 0
        )
        appreciation_exempt = symbol in {
            str(item or "").strip().upper()
            for item in getattr(
                self.config,
                "ALLOCATION_APPRECIATION_EXEMPT_SYMBOLS",
                (),
            )
        }
        appreciation_allowed = (
            appreciation_exempt
            or (
                daily_return is not None
                and daily_return
                > float(
                    getattr(
                        self.config,
                        "ALLOCATION_MIN_DAILY_RETURN",
                        0.10,
                    )
                )
            )
        )
        appreciation_block_reason = (
            ""
            if appreciation_allowed
            else (
                "daily_return_data_unavailable"
                if daily_return is None
                else "daily_return_below_threshold"
            )
        )
        return {
            "symbol": symbol,
            "close": close,
            "bid_price": bid,
            "bid_quantity": float((book or {}).get("bid_qty", 0) or 0),
            "ask_price": ask,
            "ask_quantity": float((book or {}).get("ask_qty", 0) or 0),
            "liquidity": liquidity,
            "volatility": volatility,
            "funding": abs(
                float((premium or {}).get("last_funding_rate", 0) or 0)
            ),
            "margin": quote_notional,
            "execution_cost_bps": execution_cost_bps,
            "daily_return": daily_return,
            "appreciation_exempt": appreciation_exempt,
            **thirty_minute_features.to_dict(),
            "opening_volatility_blocked": not opening_volatility_allowed,
            "opening_volatility_block_reason": (
                thirty_minute_features.reject_reason
                if thirty_minute_features.blocked
                else (
                    "opening_volatility_too_high"
                    if not opening_volatility_allowed
                    else ""
                )
            ),
            "blocked": (
                basic_blocked
                or thirty_minute_features.blocked
                or not opening_volatility_allowed
                or not appreciation_allowed
            ),
            "reject_reason": (
                "market_data_unavailable"
                if basic_blocked
                else (
                    thirty_minute_features.reject_reason
                    if thirty_minute_features.blocked
                    else (
                        "opening_volatility_too_high"
                        if not opening_volatility_allowed
                        else appreciation_block_reason
                    )
                )
            ),
        }

    async def _fetch_selector_inputs(
        self,
        symbol: str,
    ) -> Tuple[List[dict], List[dict], dict]:
        now = time.monotonic()
        cached = self.selector_input_cache.get(symbol)
        age = (
            max(0.0, now - float(cached.get("fetched_at", 0) or 0))
            if cached
            else float("inf")
        )
        refresh_sec = max(
            float(self.config.POLL_INTERVAL_SEC),
            float(self.config.SELECTOR_INPUT_REFRESH_SEC),
        )
        if cached and age < refresh_sec:
            return (
                cached["indicator_klines"],
                cached["allocation_klines"],
                cached["premium"],
            )
        try:
            indicator_klines, allocation_klines, premium = await asyncio.gather(
                self.client.get_klines(
                    symbol,
                    self.config.KLINE_INTERVAL,
                    limit=self.config.KLINE_LIMIT,
                ),
                self.client.get_klines(
                    symbol,
                    getattr(
                        self.config,
                        "ALLOCATION_KLINE_INTERVAL",
                        "5m",
                    ),
                    limit=getattr(
                        self.config,
                        "ALLOCATION_KLINE_LIMIT",
                        self.config.KLINE_LIMIT,
                    ),
                ),
                self.client.get_premium_index(symbol),
            )
        except Exception:
            max_stale_sec = max(
                refresh_sec,
                float(self.config.SELECTOR_INPUT_MAX_STALE_SEC),
            )
            if cached and age <= max_stale_sec:
                return (
                    cached["indicator_klines"],
                    cached["allocation_klines"],
                    cached["premium"],
                )
            raise
        payload = {
            "fetched_at": now,
            "indicator_klines": list(indicator_klines or []),
            "allocation_klines": list(allocation_klines or []),
            "premium": dict(premium or {}),
        }
        self.selector_input_cache[symbol] = payload
        return (
            payload["indicator_klines"],
            payload["allocation_klines"],
            payload["premium"],
        )

    def cache_ages(self) -> Dict[str, float]:
        now = time.monotonic()
        return {
            symbol: max(
                0.0,
                now - float(item.get("fetched_at", 0) or 0),
            )
            for symbol, item in self.selector_input_cache.items()
        }

    def clear(self) -> None:
        self.selector_input_cache.clear()
