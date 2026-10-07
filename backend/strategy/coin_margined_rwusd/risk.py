"""Risk rules shared by RWUSD dry-run, Testnet, and Mainnet execution."""

from __future__ import annotations

from typing import Optional

from backend.strategy.coin_margined_rwusd.models import PortfolioSnapshot, RiskDecision


class RiskRuleSet:
    def __init__(
        self,
        soft_unimmr: float,
        hard_unimmr: float,
        max_drawdown: float,
        redeem_unimmr: float = 0.0,
        max_total_net_leverage: Optional[float] = None,
        max_single_symbol_net_leverage: Optional[float] = None,
        hard_available_balance: float = 0.0,
    ) -> None:
        self.soft_unimmr = float(soft_unimmr)
        self.hard_unimmr = float(hard_unimmr)
        self.max_drawdown = float(max_drawdown)
        self.redeem_unimmr = float(redeem_unimmr)
        self.max_total_net_leverage = max_total_net_leverage
        self.max_single_symbol_net_leverage = max_single_symbol_net_leverage
        self.hard_available_balance = float(hard_available_balance)

    @staticmethod
    def _exceeds(notional: float, limit: Optional[float], equity: float) -> bool:
        if limit is None:
            return False
        if equity <= 0:
            return abs(notional) > 0
        return abs(notional) / equity > float(limit)

    def evaluate(
        self,
        snapshot: PortfolioSnapshot,
        current_drawdown: float,
    ) -> RiskDecision:
        redeem = snapshot.uni_mmr < self.redeem_unimmr
        total_net_limit = self._exceeds(
            snapshot.total_net_notional,
            self.max_total_net_leverage,
            snapshot.account_equity,
        )
        symbol_net_limit = self._exceeds(
            snapshot.single_symbol_net_notional,
            self.max_single_symbol_net_leverage,
            snapshot.account_equity,
        )
        drawdown_limit = (
            self.max_drawdown > 0
            and current_drawdown > self.max_drawdown
        )
        soft_margin_limit = snapshot.uni_mmr <= self.soft_unimmr
        available_margin_risk = (
            snapshot.available_balance_known
            and snapshot.available_balance <= self.hard_available_balance
        )
        hard_risk_source: Optional[str] = None
        reason: Optional[str] = None
        if snapshot.uni_mmr <= self.hard_unimmr:
            hard_risk_source = "uni_mmr"
            reason = "uni_mmr_hard_limit"
        elif snapshot.available_balance_known and available_margin_risk:
            hard_risk_source = "available_balance"
            reason = "available_balance_exhausted"
        elif snapshot.estimated_liquidation_risk:
            hard_risk_source = "estimated_liquidation_risk"
            reason = (
                snapshot.liquidation_risk_reason
                or "estimated_liquidation_risk"
            )
        elif total_net_limit:
            reason = "total_net_leverage_limit"
        elif symbol_net_limit:
            reason = "single_symbol_net_leverage_limit"
        elif drawdown_limit:
            reason = "max_drawdown_soft_limit"
        elif soft_margin_limit:
            reason = "uni_mmr_soft_limit"
        elif redeem:
            reason = "uni_mmr_redeem_limit"

        hard_risk = hard_risk_source is not None
        net_exposure_limit = total_net_limit or symbol_net_limit
        return RiskDecision(
            reason=reason,
            should_redeem_topup=redeem,
            block_expansion=bool(
                hard_risk
                or net_exposure_limit
                or drawdown_limit
                or soft_margin_limit
            ),
            block_harvest=bool(
                hard_risk or net_exposure_limit or drawdown_limit
            ),
            block_subscription=bool(
                hard_risk
                or net_exposure_limit
                or drawdown_limit
                or soft_margin_limit
            ),
            hard_risk=hard_risk,
            net_exposure_limit=net_exposure_limit,
            drawdown_limit=drawdown_limit,
            available_margin_risk=available_margin_risk,
            liquidation_risk=snapshot.estimated_liquidation_risk,
            hard_risk_source=hard_risk_source,
            soft_margin_limit=soft_margin_limit,
        )
