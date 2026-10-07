"""Market and official-capacity inputs for RWUSD capital allocation."""

from __future__ import annotations

import asyncio
import time
from typing import Dict, List, Tuple

from backend.strategy.coin_margined_rwusd.market_features import (
    build_allocation_market_features,
    build_thirty_minute_market_features,
    thirty_minute_row_volatility_allowed,
)
from backend.strategy.coin_margined_rwusd.models import MarketRow


class RWUSDAllocationMarket:
    """Build allocation rows only when capital needs placement."""

    def __init__(
        self,
        *,
        client: object,
        config: object,
        execution_enabled: bool,
    ) -> None:
        self.client = client
        self.config = config
        self.execution_enabled = bool(execution_enabled)

    async def build(self) -> List[MarketRow]:
        positions = await self.client.get_portfolio_positions()
        existing_by_symbol: Dict[str, float] = {}
        for position in positions or []:
            symbol = str(position.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            quantity = abs(float(position.get("size", 0) or 0))
            notional = abs(float(position.get("notional", 0) or 0))
            if notional <= 0:
                notional = quantity * max(
                    0.0,
                    float(position.get("entry_price", 0) or 0),
                )
            existing_by_symbol[symbol] = (
                existing_by_symbol.get(symbol, 0.0) + notional
            )
        symbols = tuple(getattr(self.config, "ALLOCATION_SYMBOLS", ()))
        now_ms = int(time.time() * 1000)
        results = await asyncio.gather(
            *[
                self._fetch_row(
                    symbol,
                    existing_gross_notional=existing_by_symbol.get(
                        symbol,
                        0.0,
                    ),
                    now_ms=now_ms,
                )
                for symbol in symbols
            ],
            return_exceptions=True,
        )
        rows: List[MarketRow] = []
        for symbol, result in zip(symbols, results):
            if isinstance(result, dict):
                rows.append(result)
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "blocked": True,
                    "reject_reason": (
                        f"allocation_market_data_unavailable:{result}"
                    ),
                    "existing_gross_notional": existing_by_symbol.get(
                        symbol,
                        0.0,
                    ),
                }
            )
        return rows

    async def _fetch_row(
        self,
        symbol: str,
        *,
        existing_gross_notional: float,
        now_ms: int,
    ) -> MarketRow:
        requests = [
            self.client.get_klines(
                symbol,
                getattr(self.config, "ALLOCATION_KLINE_INTERVAL", "5m"),
                limit=getattr(
                    self.config,
                    "ALLOCATION_KLINE_LIMIT",
                    self.config.KLINE_LIMIT,
                ),
            ),
            self.client.get_klines(
                symbol,
                self.config.KLINE_INTERVAL,
                limit=self.config.KLINE_LIMIT,
            ),
            self.client.get_order_book(symbol, limit=100),
        ]
        if self.execution_enabled:
            requests.extend(
                [
                    self.client.get_um_symbol_config(symbol),
                    self.client.get_um_leverage_brackets(symbol),
                ]
            )
        results = await asyncio.gather(*requests)
        klines = results[0]
        indicator_klines = results[1]
        order_book = results[2]
        target_leverage = int(
            self.config.SYMBOL_TARGET_LEVERAGE[symbol]
        )
        sizing_leverage = target_leverage
        maximum_notional = 1_000_000_000_000.0
        if self.execution_enabled:
            sizing_leverage, maximum_notional = leverage_capacity(
                symbol=symbol,
                target_leverage=target_leverage,
                symbol_config=results[3],
                leverage_brackets=results[4],
            )
        features = build_allocation_market_features(
            klines=klines or [],
            order_book=order_book,
            now_ms=now_ms,
            depth_bps=self.config.ALLOCATION_ENTRY_DEPTH_BPS,
            opening_tranche_fraction=self.config.OPENING_TRANCHE_FRACTION,
            depth_utilization=(
                self.config.ALLOCATION_ENTRY_DEPTH_UTILIZATION
            ),
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
        bids = list((order_book or {}).get("bids") or [])
        asks = list((order_book or {}).get("asks") or [])
        best_bid = float(bids[0][0]) if bids else 0.0
        best_ask = float(asks[0][0]) if asks else 0.0
        mid = (
            (best_bid + best_ask) / 2.0
            if best_bid > 0 and best_ask > best_bid
            else 0.0
        )
        spread_bps = (
            (best_ask - best_bid) / mid * 10000.0
            if mid > 0
            else 999.0
        )
        return {
            "symbol": symbol,
            "sizing_leverage": sizing_leverage,
            "max_notional_value": maximum_notional,
            "existing_gross_notional": existing_gross_notional,
            "execution_cost_bps": spread_bps,
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
            **features.to_dict(),
            **thirty_minute_features.to_dict(),
        }


def leverage_capacity(
    *,
    symbol: str,
    target_leverage: int,
    symbol_config: object,
    leverage_brackets: object,
) -> Tuple[int, float]:
    target = str(symbol or "").strip().upper()
    config_rows = (
        list(symbol_config)
        if isinstance(symbol_config, list)
        else [symbol_config]
    )
    config_row = next(
        (
            row
            for row in config_rows
            if isinstance(row, dict)
            and str(row.get("symbol") or "").strip().upper() == target
        ),
        None,
    )
    if not isinstance(config_row, dict):
        raise RuntimeError(
            f"official leverage configuration unavailable for {target}"
        )
    current_leverage = max(
        1,
        int(config_row.get("leverage", 0) or 0),
    )
    current_maximum = max(
        0.0,
        float(config_row.get("maxNotionalValue", 0) or 0),
    )
    bracket_rows = (
        list(leverage_brackets)
        if isinstance(leverage_brackets, list)
        else [leverage_brackets]
    )
    bracket_payload = next(
        (
            row
            for row in bracket_rows
            if isinstance(row, dict)
            and str(row.get("symbol") or "").strip().upper() == target
        ),
        None,
    )
    if not isinstance(bracket_payload, dict):
        raise RuntimeError(
            f"official leverage brackets unavailable for {target}"
        )
    coefficient = max(
        0.0,
        float(bracket_payload.get("notionalCoef", 1) or 1),
    ) or 1.0
    target_maximum = max(
        (
            max(0.0, float(row.get("notionalCap", 0) or 0))
            * coefficient
            for row in list(bracket_payload.get("brackets") or [])
            if isinstance(row, dict)
            and int(row.get("initialLeverage", 0) or 0)
            >= target_leverage
        ),
        default=0.0,
    )
    if current_maximum <= 0 or target_maximum <= 0:
        raise RuntimeError(
            f"official notional capacity unavailable for {target}"
        )
    return (
        min(current_leverage, target_leverage),
        min(current_maximum, target_maximum),
    )
