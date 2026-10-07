"""Symbol ranking and switch gating for RWUSD."""

from __future__ import annotations

from typing import List, Optional

from backend.strategy.coin_margined_rwusd.models import MarketRow, SelectorSnapshot, SymbolScore


EXECUTION_COST_WEIGHT = 0.05
MAX_EXECUTION_COST_BPS = 5.0


class SymbolSelector:
    def __init__(
        self,
        switch_threshold: float,
        eval_interval_minutes: int = 15,
        switch_cooldown_minutes: int = 60,
    ) -> None:
        self._switch_threshold = max(0.0, float(switch_threshold))
        self._eval_interval_minutes = max(1, int(eval_interval_minutes))
        self._switch_cooldown_minutes = max(0, int(switch_cooldown_minutes))

    def select(
        self,
        current_symbol: Optional[str],
        rows: List[MarketRow],
        minute_of_day: Optional[int] = None,
        last_switch_minute: Optional[int] = None,
    ) -> SelectorSnapshot:
        ranked = sorted(
            (self._build_score(row) for row in rows),
            key=lambda item: item.score,
            reverse=True,
        )
        if self._should_hold_current(
            current_symbol,
            ranked,
            minute_of_day,
            last_switch_minute,
        ):
            return SelectorSnapshot(
                scores=ranked,
                selected_symbol=current_symbol,
                cooldown_symbol=current_symbol,
            )

        best = next((item for item in ranked if item.reject_reason is None), None)
        current = next((item for item in ranked if item.symbol == current_symbol), None)
        if current is not None and current.reject_reason is None and best is not None:
            if best.symbol != current.symbol and (best.score - current.score) < self._switch_threshold:
                return SelectorSnapshot(scores=ranked, selected_symbol=current.symbol)
        return SelectorSnapshot(
            scores=ranked,
            selected_symbol=best.symbol if best is not None else None,
        )

    def _build_score(self, row: MarketRow) -> SymbolScore:
        blocked = bool(row.get("blocked", False))
        reject_reason = str(row.get("reject_reason") or "").strip() or None
        execution_cost_bps = max(0.0, float(row.get("execution_cost_bps", MAX_EXECUTION_COST_BPS) or 0))
        execution_cost_score = max(
            0.0,
            (MAX_EXECUTION_COST_BPS - execution_cost_bps) / MAX_EXECUTION_COST_BPS,
        )
        liquidity = float(row.get("liquidity", 0) or 0)
        volatility = float(row.get("volatility", 0) or 0)
        funding = float(row.get("funding", 0) or 0)
        margin = float(row.get("margin", 0) or 0)
        score = 0.0
        if not blocked:
            score = (
                liquidity * 0.35
                + volatility * 0.20
                + funding * 0.20
                + margin * 0.25
                + execution_cost_score * EXECUTION_COST_WEIGHT
            )
        return SymbolScore(
            symbol=str(row.get("symbol") or ""),
            score=score,
            liquidity_score=liquidity,
            volatility_score=volatility,
            funding_score=funding,
            margin_efficiency_score=margin,
            execution_cost_score=execution_cost_score,
            execution_cost_bps=execution_cost_bps,
            capacity_ratio=max(
                0.0,
                float(row.get("capacity_ratio", 1.0) or 0),
            ),
            max_notional_value=max(
                0.0,
                float(row.get("max_notional_value", 0) or 0),
            ),
            required_gross_notional=max(
                0.0,
                float(row.get("required_gross_notional", 0) or 0),
            ),
            reject_reason=reject_reason or ("blocked" if blocked else None),
        )

    def _should_hold_current(
        self,
        current_symbol: Optional[str],
        ranked: List[SymbolScore],
        minute_of_day: Optional[int],
        last_switch_minute: Optional[int],
    ) -> bool:
        if current_symbol is None or minute_of_day is None:
            return False
        current = next((item for item in ranked if item.symbol == current_symbol), None)
        if current is None or current.reject_reason is not None:
            return False
        if minute_of_day % self._eval_interval_minutes != 0:
            return True
        if self._switch_cooldown_minutes <= 0 or last_switch_minute is None:
            return False
        elapsed = (minute_of_day - last_switch_minute) % (24 * 60)
        return elapsed < self._switch_cooldown_minutes
