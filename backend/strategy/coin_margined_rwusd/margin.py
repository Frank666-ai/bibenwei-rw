"""Binance UM notional capacity and maintenance-margin projection."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, replace
from typing import Dict, List, Tuple

from backend.strategy.coin_margined_rwusd.models import PortfolioSnapshot


@dataclass(frozen=True)
class MaintenanceBracket:
    notional_floor: float
    notional_cap: float
    initial_leverage: int
    maintenance_margin_rate: float
    cumulative_maintenance: float


@dataclass(frozen=True)
class SymbolRiskProfile:
    symbol: str
    max_notional_value: float
    maintenance_brackets: Tuple[MaintenanceBracket, ...]
    loaded_at_monotonic: float


class BinanceUMMarginModel:
    def __init__(
        self,
        *,
        client: object,
        leverage: int,
        refresh_sec: float,
        minimum_capacity_ratio: float,
        soft_unimmr: float,
        projected_unimmr_buffer: float,
    ) -> None:
        self.client = client
        self.leverage = max(1, int(leverage))
        self.refresh_sec = max(1.0, float(refresh_sec))
        self.minimum_capacity_ratio = max(
            0.0,
            float(minimum_capacity_ratio),
        )
        self.projected_unimmr_floor = (
            float(soft_unimmr)
            + max(0.0, float(projected_unimmr_buffer))
        )
        self._profiles: Dict[str, SymbolRiskProfile] = {}

    def set_leverage(self, leverage: int) -> None:
        value = max(1, int(leverage))
        if value == self.leverage:
            return
        self.leverage = value
        self._profiles.clear()

    def invalidate(self) -> None:
        self._profiles.clear()

    async def get_profile(self, symbol: str) -> SymbolRiskProfile:
        target = str(symbol or "").strip().upper()
        if not target:
            raise RuntimeError("RWUSD risk profile requires a symbol")
        cached = self._profiles.get(target)
        if (
            cached is not None
            and time.monotonic() - cached.loaded_at_monotonic
            < self.refresh_sec
        ):
            return cached
        try:
            payload = await self.client.get_um_leverage_brackets(target)
            profile = self.parse_profile(target, payload)
        except Exception:
            if cached is not None:
                return cached
            raise
        self._profiles[target] = profile
        return profile

    def update_max_notional(
        self,
        symbol: str,
        maximum: float,
    ) -> None:
        target = str(symbol or "").strip().upper()
        profile = self._profiles.get(target)
        value = max(0.0, float(maximum or 0))
        if profile is None or value <= 0:
            return
        self._profiles[target] = replace(
            profile,
            max_notional_value=min(
                profile.max_notional_value,
                value,
            ),
        )

    async def enrich_market_rows(
        self,
        *,
        rows: List[dict],
        portfolio: PortfolioSnapshot,
        gross_budget: float,
        current_managed_notional: float,
    ) -> List[dict]:
        other_abs_notional = max(
            0.0,
            float(portfolio.total_abs_notional or 0)
            - max(0.0, float(current_managed_notional or 0)),
        )
        required_gross = max(
            0.0,
            float(gross_budget or 0) - other_abs_notional,
        )

        async def enrich(row: dict) -> dict:
            item = dict(row)
            symbol = str(item.get("symbol") or "").strip().upper()
            if bool(item.get("blocked")):
                item["reject_reason"] = (
                    str(item.get("reject_reason") or "").strip()
                    or "market_data_unavailable"
                )
                return item
            try:
                profile = await self.get_profile(symbol)
            except Exception:
                item.update(
                    {
                        "blocked": True,
                        "reject_reason": "risk_profile_unavailable",
                        "capacity_ratio": 0.0,
                        "max_notional_value": 0.0,
                        "required_gross_notional": required_gross,
                    }
                )
                return item
            capacity_ratio = (
                profile.max_notional_value / required_gross
                if required_gross > 1e-12
                else 1.0
            )
            item.update(
                {
                    "capacity_ratio": capacity_ratio,
                    "max_notional_value": profile.max_notional_value,
                    "required_gross_notional": required_gross,
                }
            )
            if (
                capacity_ratio + 1e-12
                < self.minimum_capacity_ratio
            ):
                item["blocked"] = True
                item["reject_reason"] = "insufficient_max_notional"
            return item

        return list(await asyncio.gather(*(enrich(row) for row in rows)))

    def parse_profile(
        self,
        symbol: str,
        payload: object,
    ) -> SymbolRiskProfile:
        target = str(symbol or "").strip().upper()
        candidates = list(payload) if isinstance(payload, list) else [payload]
        data = next(
            (
                item
                for item in candidates
                if isinstance(item, dict)
                and str(item.get("symbol") or "").strip().upper() == target
            ),
            None,
        )
        if not isinstance(data, dict):
            raise RuntimeError(
                f"RWUSD leverage brackets are unavailable for {target}"
            )
        coefficient = max(
            0.0,
            float(data.get("notionalCoef", 1) or 1),
        ) or 1.0
        brackets: List[MaintenanceBracket] = []
        for raw in list(data.get("brackets") or []):
            if not isinstance(raw, dict):
                continue
            cap = max(0.0, float(raw.get("notionalCap", 0) or 0))
            rate = max(
                0.0,
                float(raw.get("maintMarginRatio", 0) or 0),
            )
            initial_leverage = max(
                0,
                int(raw.get("initialLeverage", 0) or 0),
            )
            if cap <= 0 or rate <= 0 or initial_leverage <= 0:
                continue
            brackets.append(
                MaintenanceBracket(
                    notional_floor=max(
                        0.0,
                        float(raw.get("notionalFloor", 0) or 0)
                        * coefficient,
                    ),
                    notional_cap=cap * coefficient,
                    initial_leverage=initial_leverage,
                    maintenance_margin_rate=rate,
                    cumulative_maintenance=max(
                        0.0,
                        float(raw.get("cum", 0) or 0) * coefficient,
                    ),
                )
            )
        brackets.sort(key=lambda item: item.notional_floor)
        if not brackets:
            raise RuntimeError(
                f"RWUSD leverage brackets are incomplete for {target}"
            )
        maximum = max(
            (
                bracket.notional_cap
                for bracket in brackets
                if bracket.initial_leverage >= self.leverage
            ),
            default=0.0,
        )
        if maximum <= 0:
            raise RuntimeError(
                f"RWUSD {target} does not support {self.leverage}x leverage"
            )
        return SymbolRiskProfile(
            symbol=target,
            max_notional_value=maximum,
            maintenance_brackets=tuple(brackets),
            loaded_at_monotonic=time.monotonic(),
        )

    @staticmethod
    def maintenance_margin(
        notional: float,
        profile: SymbolRiskProfile,
    ) -> float:
        value = max(0.0, float(notional or 0))
        if value <= 0:
            return 0.0
        bracket = next(
            (
                item
                for item in profile.maintenance_brackets
                if value <= item.notional_cap + 1e-12
            ),
            profile.maintenance_brackets[-1],
        )
        return max(
            0.0,
            value * bracket.maintenance_margin_rate
            - bracket.cumulative_maintenance,
        )

    def projected_pair_unimmr(
        self,
        *,
        profile: SymbolRiskProfile,
        portfolio: PortfolioSnapshot,
        current_long_notional: float,
        current_short_notional: float,
        added_notional_per_side: float,
        execution_cost_bps: float,
    ) -> float:
        current_selected_maintenance = (
            self.maintenance_margin(current_long_notional, profile)
            + self.maintenance_margin(current_short_notional, profile)
        )
        account_maintenance = max(
            0.0,
            float(portfolio.account_maint_margin or 0),
        )
        if (
            account_maintenance <= 0
            and portfolio.account_equity > 0
            and 0 < portfolio.uni_mmr < 999999.0
        ):
            account_maintenance = (
                portfolio.account_equity / portfolio.uni_mmr
            )
        other_maintenance = max(
            0.0,
            account_maintenance - current_selected_maintenance,
        )
        added = max(0.0, float(added_notional_per_side or 0))
        projected_maintenance = (
            other_maintenance
            + self.maintenance_margin(
                current_long_notional + added,
                profile,
            )
            + self.maintenance_margin(
                current_short_notional + added,
                profile,
            )
        )
        projected_equity = max(
            0.0,
            float(portfolio.account_equity or 0)
            - added
            * 2.0
            * max(0.0, float(execution_cost_bps or 0))
            / 10000.0,
        )
        return (
            projected_equity / projected_maintenance
            if projected_maintenance > 0
            else 999999.0
        )

    def safe_pair_tranche_notional(
        self,
        *,
        profile: SymbolRiskProfile,
        portfolio: PortfolioSnapshot,
        current_long_notional: float,
        current_short_notional: float,
        requested_notional_per_side: float,
        execution_cost_bps: float,
    ) -> float:
        requested = max(0.0, float(requested_notional_per_side or 0))
        if requested <= 0:
            return 0.0

        def projected(added: float) -> float:
            return self.projected_pair_unimmr(
                profile=profile,
                portfolio=portfolio,
                current_long_notional=current_long_notional,
                current_short_notional=current_short_notional,
                added_notional_per_side=added,
                execution_cost_bps=execution_cost_bps,
            )

        if projected(requested) + 1e-12 >= self.projected_unimmr_floor:
            return requested
        lower = 0.0
        upper = requested
        for _ in range(48):
            midpoint = (lower + upper) / 2.0
            if projected(midpoint) + 1e-12 >= self.projected_unimmr_floor:
                lower = midpoint
            else:
                upper = midpoint
        return lower
