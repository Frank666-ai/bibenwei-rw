"""Canonical data models for the independent RWUSD hedge strategy."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


def parse_bool(value: object, default: bool = False) -> bool:
    """Parse API/persisted booleans without treating "false" as true."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on", "risk", "high"}:
            return True
        if normalized in {"0", "false", "no", "off", "none", ""}:
            return False
    return default


class PositionSide(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class StrategyPhase(str, Enum):
    IDLE = "IDLE"
    SELECTING_SYMBOL = "SELECTING_SYMBOL"
    OPENING_HEDGE = "OPENING_HEDGE"
    HEDGED = "HEDGED"
    REPAIRING_HEDGE = "REPAIRING_HEDGE"
    TAKING_PROFIT = "TAKING_PROFIT"
    REBALANCING = "REBALANCING"
    PROFIT_SWEEPING = "PROFIT_SWEEPING"
    FLATTENING = "FLATTENING"
    PAUSED = "PAUSED"


@dataclass
class SymbolScore:
    symbol: str
    score: float = 0.0
    liquidity_score: float = 0.0
    volatility_score: float = 0.0
    funding_score: float = 0.0
    margin_efficiency_score: float = 0.0
    execution_cost_score: float = 0.0
    execution_cost_bps: float = 0.0
    capacity_ratio: float = 1.0
    max_notional_value: float = 0.0
    required_gross_notional: float = 0.0
    reject_reason: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "score": self.score,
            "liquidity_score": self.liquidity_score,
            "volatility_score": self.volatility_score,
            "funding_score": self.funding_score,
            "margin_efficiency_score": self.margin_efficiency_score,
            "execution_cost_score": self.execution_cost_score,
            "execution_cost_bps": self.execution_cost_bps,
            "capacity_ratio": self.capacity_ratio,
            "max_notional_value": self.max_notional_value,
            "required_gross_notional": self.required_gross_notional,
            "reject_reason": self.reject_reason,
        }


@dataclass
class SelectorSnapshot:
    scores: List[SymbolScore] = field(default_factory=list)
    selected_symbol: Optional[str] = None
    cooldown_symbol: Optional[str] = None


@dataclass
class PortfolioSnapshot:
    account_equity: float
    available_balance: float = 0.0
    uni_mmr: float = 999999.0
    withdrawable_balance: float = 0.0
    withdrawable_official: bool = False
    total_abs_notional: float = 0.0
    total_net_notional: float = 0.0
    single_symbol_net_notional: float = 0.0
    source: str = "fallback"
    account_type: str = "unknown"
    account_status: str = "UNKNOWN"
    margin_health_source: str = "simulated"
    official_uni_mmr: bool = False
    account_maint_margin: float = 0.0
    available_balance_known: bool = False
    estimated_liquidation_risk: bool = False
    liquidation_risk_reason: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "account_equity": self.account_equity,
            "available_balance": self.available_balance,
            "available_balance_known": self.available_balance_known,
            "uni_mmr": self.uni_mmr,
            "withdrawable_balance": self.withdrawable_balance,
            "withdrawable_official": self.withdrawable_official,
            "total_abs_notional": self.total_abs_notional,
            "total_net_notional": self.total_net_notional,
            "single_symbol_net_notional": self.single_symbol_net_notional,
            "source": self.source,
            "account_type": self.account_type,
            "account_status": self.account_status,
            "margin_health": self.uni_mmr,
            "margin_health_source": self.margin_health_source,
            "official_uni_mmr": self.official_uni_mmr,
            "account_maint_margin": self.account_maint_margin,
            "estimated_liquidation_risk": self.estimated_liquidation_risk,
            "liquidation_risk_reason": self.liquidation_risk_reason,
        }


@dataclass
class RiskDecision:
    reason: Optional[str] = None
    should_redeem_topup: bool = False
    block_expansion: bool = False
    block_harvest: bool = False
    block_subscription: bool = False
    hard_risk: bool = False
    net_exposure_limit: bool = False
    drawdown_limit: bool = False
    available_margin_risk: bool = False
    liquidation_risk: bool = False
    hard_risk_source: Optional[str] = None
    soft_margin_limit: bool = False

    @property
    def allows_hedge_repair_pair(self) -> bool:
        if (
            self.hard_risk
            or self.drawdown_limit
            or self.soft_margin_limit
        ):
            return False
        return not self.block_expansion or self.net_exposure_limit

    def to_dict(self) -> dict:
        return {
            "should_redeem_topup": self.should_redeem_topup,
            "block_expansion": self.block_expansion,
            "block_harvest": self.block_harvest,
            "block_subscription": self.block_subscription,
            "hard_risk": self.hard_risk,
            "net_exposure_limit": self.net_exposure_limit,
            "drawdown_limit": self.drawdown_limit,
            "available_margin_risk": self.available_margin_risk,
            "liquidation_risk": self.liquidation_risk,
            "hard_risk_source": self.hard_risk_source,
            "soft_margin_limit": self.soft_margin_limit,
            "reason": self.reason,
        }


@dataclass
class ProfitBucket:
    realized_pnl_total: float = 0.0
    realized_pnl_available_for_deposit: float = 0.0
    harvest_buffer: float = 0.0
    capital_contribution_total: float = 0.0
    rwusd_principal: float = 0.0
    rwusd_interest_accrued: float = 0.0
    rwusd_redeemable: float = 0.0
    harvest_count: int = 0
    deposit_count: int = 0
    redeem_count: int = 0
    closed_loop_ready: bool = False
    last_rebalance_action: Optional[str] = None
    sweep_block_reason: Optional[str] = None
    rwusd_units: float = 0.0
    exchange_rwusd_amount: float = 0.0
    exchange_rwusd_total_profit: float = 0.0
    exchange_rwusd_annual_percentage_rate: float = 0.0
    exchange_rwusd_rate_time_ms: int = 0
    exchange_rwusd_collateral_amount: float = 0.0
    exchange_rwusd_collateral_equity: float = 0.0
    exchange_rwusd_transferable_amount: float = 0.0
    initial_collateralization_completed: bool = False
    initial_collateralization_mode: Optional[str] = None
    initial_collateralization_source_amount: float = 0.0
    initial_collateralization_source_amounts: Dict[str, float] = field(
        default_factory=dict
    )
    initial_collateralization_existing_rwusd_amount: float = 0.0
    initial_collateralization_rwusd: float = 0.0
    pending_subscription_amount: float = 0.0
    pending_subscription_asset: Optional[str] = None
    pending_subscription_kind: Optional[str] = None
    pending_source_asset: Optional[str] = None
    pending_source_amount: float = 0.0
    pending_source_transfer_amount: float = 0.0
    pending_conversion_quote_id: Optional[str] = None
    pending_conversion_order_id: Optional[str] = None
    pending_conversion_quote_expires_at_ms: int = 0
    pending_harvest_amount: float = 0.0
    pending_subscription_stage: Optional[str] = None
    pending_subscription_started_at_ms: int = 0
    pending_transfer_id: Optional[str] = None
    pending_rwusd_amount: float = 0.0
    pending_capital_contribution_amount: Optional[float] = None
    pending_capital_reference: Optional[str] = None
    confirmed_capital_reference: Optional[str] = None
    confirmed_capital_contribution_before: float = 0.0
    confirmed_capital_contribution_amount: float = 0.0
    pending_collateral_balance_before: float = 0.0
    pending_collateral_transfer_id: Optional[str] = None
    transfer_retry_after_ms: int = 0
    transfer_retry_reason: Optional[str] = None
    transfer_retry_count: int = 0
    exchange_realized_pnl_total: float = 0.0
    exchange_commission_total: float = 0.0
    exchange_trade_count: int = 0
    exchange_trade_sync_started_at_ms: int = 0
    exchange_trade_cursors: Dict[str, int] = field(default_factory=dict)
    unsettled_harvest_pnl: float = 0.0
    pending_harvest_net_credited: float = 0.0

    def to_dict(self) -> dict:
        return {
            "realized_pnl_total": self.realized_pnl_total,
            "realized_pnl_available_for_deposit": self.realized_pnl_available_for_deposit,
            "harvest_buffer": self.harvest_buffer,
            "capital_contribution_total": self.capital_contribution_total,
            "rwusd_principal": self.rwusd_principal,
            "rwusd_interest_accrued": self.rwusd_interest_accrued,
            "rwusd_redeemable": self.rwusd_redeemable,
            "harvest_count": self.harvest_count,
            "deposit_count": self.deposit_count,
            "redeem_count": self.redeem_count,
            "closed_loop_ready": self.closed_loop_ready,
            "last_rebalance_action": self.last_rebalance_action,
            "sweep_block_reason": self.sweep_block_reason,
            "rwusd_units": self.rwusd_units,
            "exchange_rwusd_amount": self.exchange_rwusd_amount,
            "exchange_rwusd_total_profit": self.exchange_rwusd_total_profit,
            "exchange_rwusd_annual_percentage_rate": (
                self.exchange_rwusd_annual_percentage_rate
            ),
            "exchange_rwusd_rate_time_ms": (
                self.exchange_rwusd_rate_time_ms
            ),
            "exchange_rwusd_collateral_amount": (
                self.exchange_rwusd_collateral_amount
            ),
            "exchange_rwusd_collateral_equity": (
                self.exchange_rwusd_collateral_equity
            ),
            "exchange_rwusd_transferable_amount": (
                self.exchange_rwusd_transferable_amount
            ),
            "initial_collateralization_completed": (
                self.initial_collateralization_completed
            ),
            "initial_collateralization_mode": (
                self.initial_collateralization_mode
            ),
            "initial_collateralization_source_amount": (
                self.initial_collateralization_source_amount
            ),
            "initial_collateralization_source_amounts": dict(
                self.initial_collateralization_source_amounts
            ),
            "initial_collateralization_existing_rwusd_amount": (
                self.initial_collateralization_existing_rwusd_amount
            ),
            "initial_collateralization_rwusd": (
                self.initial_collateralization_rwusd
            ),
            "pending_subscription_amount": self.pending_subscription_amount,
            "pending_subscription_asset": self.pending_subscription_asset,
            "pending_subscription_kind": self.pending_subscription_kind,
            "pending_source_asset": self.pending_source_asset,
            "pending_source_amount": self.pending_source_amount,
            "pending_source_transfer_amount": (
                self.pending_source_transfer_amount
            ),
            "pending_conversion_quote_id": (
                self.pending_conversion_quote_id
            ),
            "pending_conversion_order_id": (
                self.pending_conversion_order_id
            ),
            "pending_conversion_quote_expires_at_ms": (
                self.pending_conversion_quote_expires_at_ms
            ),
            "pending_harvest_amount": self.pending_harvest_amount,
            "pending_subscription_stage": self.pending_subscription_stage,
            "pending_subscription_started_at_ms": (
                self.pending_subscription_started_at_ms
            ),
            "pending_transfer_id": self.pending_transfer_id,
            "pending_rwusd_amount": self.pending_rwusd_amount,
            "pending_capital_contribution_amount": (
                self.pending_capital_contribution_amount
            ),
            "pending_capital_reference": self.pending_capital_reference,
            "confirmed_capital_reference": (
                self.confirmed_capital_reference
            ),
            "confirmed_capital_contribution_before": (
                self.confirmed_capital_contribution_before
            ),
            "confirmed_capital_contribution_amount": (
                self.confirmed_capital_contribution_amount
            ),
            "pending_collateral_balance_before": (
                self.pending_collateral_balance_before
            ),
            "pending_collateral_transfer_id": (
                self.pending_collateral_transfer_id
            ),
            "transfer_retry_after_ms": self.transfer_retry_after_ms,
            "transfer_retry_reason": self.transfer_retry_reason,
            "transfer_retry_count": self.transfer_retry_count,
            "exchange_realized_pnl_total": (
                self.exchange_realized_pnl_total
            ),
            "exchange_commission_total": self.exchange_commission_total,
            "exchange_trade_count": self.exchange_trade_count,
            "exchange_trade_sync_started_at_ms": (
                self.exchange_trade_sync_started_at_ms
            ),
            "exchange_trade_cursors": dict(self.exchange_trade_cursors),
            "unsettled_harvest_pnl": self.unsettled_harvest_pnl,
            "pending_harvest_net_credited": (
                self.pending_harvest_net_credited
            ),
        }

    @classmethod
    def from_dict(cls, payload: object) -> "ProfitBucket":
        data = payload if isinstance(payload, dict) else {}
        legacy_usdc = max(
            0.0,
            float(data.get("initial_collateralization_usdc", 0) or 0),
        )
        raw_source_amounts = data.get(
            "initial_collateralization_source_amounts"
        )
        source_amounts = {
            str(asset or "").strip().upper(): max(
                0.0,
                float(amount or 0),
            )
            for asset, amount in (
                raw_source_amounts.items()
                if isinstance(raw_source_amounts, dict)
                else ()
            )
            if str(asset or "").strip()
        }
        if not source_amounts and legacy_usdc > 0:
            source_amounts["USDC"] = legacy_usdc
        source_amount = max(
            0.0,
            float(
                data.get("initial_collateralization_source_amount")
                or sum(source_amounts.values())
                or legacy_usdc
                or 0
            ),
        )
        realized_pnl_total = float(
            data.get("realized_pnl_total", 0) or 0
        )
        harvest_buffer = float(data.get("harvest_buffer", 0) or 0)
        rwusd_principal = float(data.get("rwusd_principal", 0) or 0)
        raw_capital_contribution = data.get("capital_contribution_total")
        capital_contribution_total = (
            max(0.0, float(raw_capital_contribution or 0))
            if raw_capital_contribution is not None
            else max(
                source_amount,
                rwusd_principal
                - max(0.0, realized_pnl_total - harvest_buffer),
            )
        )
        return cls(
            realized_pnl_total=realized_pnl_total,
            realized_pnl_available_for_deposit=float(
                data.get("realized_pnl_available_for_deposit", 0) or 0
            ),
            harvest_buffer=harvest_buffer,
            capital_contribution_total=capital_contribution_total,
            rwusd_principal=rwusd_principal,
            rwusd_interest_accrued=float(data.get("rwusd_interest_accrued", 0) or 0),
            rwusd_redeemable=float(data.get("rwusd_redeemable", 0) or 0),
            harvest_count=int(data.get("harvest_count", 0) or 0),
            deposit_count=int(data.get("deposit_count", 0) or 0),
            redeem_count=int(data.get("redeem_count", 0) or 0),
            closed_loop_ready=bool(data.get("closed_loop_ready", False)),
            last_rebalance_action=data.get("last_rebalance_action"),
            sweep_block_reason=data.get("sweep_block_reason"),
            rwusd_units=float(data.get("rwusd_units", 0) or 0),
            exchange_rwusd_amount=float(
                data.get("exchange_rwusd_amount", 0) or 0
            ),
            exchange_rwusd_total_profit=float(
                data.get("exchange_rwusd_total_profit", 0) or 0
            ),
            exchange_rwusd_annual_percentage_rate=max(
                0.0,
                float(
                    data.get(
                        "exchange_rwusd_annual_percentage_rate",
                        0,
                    )
                    or 0
                ),
            ),
            exchange_rwusd_rate_time_ms=max(
                0,
                int(data.get("exchange_rwusd_rate_time_ms", 0) or 0),
            ),
            exchange_rwusd_collateral_amount=max(
                0.0,
                float(
                    data.get("exchange_rwusd_collateral_amount", 0) or 0
                ),
            ),
            exchange_rwusd_collateral_equity=max(
                0.0,
                float(
                    data.get("exchange_rwusd_collateral_equity", 0) or 0
                ),
            ),
            exchange_rwusd_transferable_amount=max(
                0.0,
                float(
                    data.get("exchange_rwusd_transferable_amount", 0)
                    or 0
                ),
            ),
            initial_collateralization_completed=bool(
                data.get("initial_collateralization_completed", False)
            ),
            initial_collateralization_mode=(
                str(data.get("initial_collateralization_mode") or "") or None
            ),
            initial_collateralization_source_amount=source_amount,
            initial_collateralization_source_amounts=source_amounts,
            initial_collateralization_existing_rwusd_amount=max(
                0.0,
                float(
                    data.get(
                        "initial_collateralization_existing_rwusd_amount",
                        0,
                    )
                    or 0
                ),
            ),
            initial_collateralization_rwusd=max(
                0.0,
                float(data.get("initial_collateralization_rwusd", 0) or 0),
            ),
            pending_subscription_amount=float(
                data.get("pending_subscription_amount", 0) or 0
            ),
            pending_subscription_asset=(
                str(data.get("pending_subscription_asset") or "") or None
            ),
            pending_subscription_kind=(
                str(data.get("pending_subscription_kind") or "") or None
            ),
            pending_source_asset=(
                str(
                    data.get("pending_source_asset")
                    or data.get("pending_subscription_asset")
                    or ""
                )
                or None
            ),
            pending_source_amount=max(
                0.0,
                float(
                    data.get("pending_source_amount")
                    or data.get("pending_subscription_amount")
                    or 0
                ),
            ),
            pending_source_transfer_amount=max(
                0.0,
                float(data.get("pending_source_transfer_amount", 0) or 0),
            ),
            pending_conversion_quote_id=(
                str(data.get("pending_conversion_quote_id") or "") or None
            ),
            pending_conversion_order_id=(
                str(data.get("pending_conversion_order_id") or "") or None
            ),
            pending_conversion_quote_expires_at_ms=max(
                0,
                int(
                    data.get("pending_conversion_quote_expires_at_ms", 0)
                    or 0
                ),
            ),
            pending_harvest_amount=max(
                0.0,
                float(data.get("pending_harvest_amount", 0) or 0),
            ),
            pending_subscription_stage=(
                str(data.get("pending_subscription_stage") or "") or None
            ),
            pending_subscription_started_at_ms=int(
                data.get("pending_subscription_started_at_ms", 0) or 0
            ),
            pending_transfer_id=(
                str(data.get("pending_transfer_id") or "") or None
            ),
            pending_rwusd_amount=max(
                0.0,
                float(data.get("pending_rwusd_amount", 0) or 0),
            ),
            pending_capital_contribution_amount=(
                max(
                    0.0,
                    float(
                        data.get(
                            "pending_capital_contribution_amount",
                            0,
                        )
                        or 0
                    ),
                )
                if data.get("pending_capital_contribution_amount")
                is not None
                else None
            ),
            pending_capital_reference=(
                str(data.get("pending_capital_reference") or "") or None
            ),
            confirmed_capital_reference=(
                str(data.get("confirmed_capital_reference") or "") or None
            ),
            confirmed_capital_contribution_before=max(
                0.0,
                float(
                    data.get("confirmed_capital_contribution_before", 0)
                    or 0
                ),
            ),
            confirmed_capital_contribution_amount=max(
                0.0,
                float(
                    data.get("confirmed_capital_contribution_amount", 0)
                    or 0
                ),
            ),
            pending_collateral_balance_before=max(
                0.0,
                float(
                    data.get("pending_collateral_balance_before", 0) or 0
                ),
            ),
            pending_collateral_transfer_id=(
                str(data.get("pending_collateral_transfer_id") or "") or None
            ),
            transfer_retry_after_ms=max(
                0,
                int(data.get("transfer_retry_after_ms", 0) or 0),
            ),
            transfer_retry_reason=(
                str(data.get("transfer_retry_reason") or "") or None
            ),
            transfer_retry_count=max(
                0,
                int(data.get("transfer_retry_count", 0) or 0),
            ),
            exchange_realized_pnl_total=float(
                data.get("exchange_realized_pnl_total", 0) or 0
            ),
            exchange_commission_total=max(
                0.0,
                float(data.get("exchange_commission_total", 0) or 0),
            ),
            exchange_trade_count=max(
                0,
                int(data.get("exchange_trade_count", 0) or 0),
            ),
            exchange_trade_sync_started_at_ms=max(
                0,
                int(data.get("exchange_trade_sync_started_at_ms", 0) or 0),
            ),
            exchange_trade_cursors={
                str(symbol or "").strip().upper(): max(0, int(trade_id or 0))
                for symbol, trade_id in dict(
                    data.get("exchange_trade_cursors") or {}
                ).items()
                if str(symbol or "").strip()
            },
            unsettled_harvest_pnl=float(
                data.get("unsettled_harvest_pnl", 0) or 0
            ),
            pending_harvest_net_credited=max(
                0.0,
                float(data.get("pending_harvest_net_credited", 0) or 0),
            ),
        )


@dataclass
class HedgeState:
    phase: StrategyPhase = StrategyPhase.IDLE
    selected_symbol: Optional[str] = None
    target_quantity: float = 0.0
    long_quantity: float = 0.0
    short_quantity: float = 0.0
    long_entry_price: float = 0.0
    short_entry_price: float = 0.0
    long_unrealized_pnl: float = 0.0
    short_unrealized_pnl: float = 0.0
    last_mark_price: float = 0.0
    pending_rebalance_side: Optional[PositionSide] = None
    pending_harvest_quantity: float = 0.0
    pending_restore_quantity: float = 0.0
    deferred_restore_carry_side: Optional[PositionSide] = None
    deferred_restore_carry_quantity: float = 0.0
    profit_harvest_rsi_confirmed: bool = False
    pending_harvest_trade_quantity: float = 0.0
    profit_sweep_started_at_ms: int = 0
    pending_repair_side: Optional[PositionSide] = None
    pending_repair_quantity: float = 0.0
    repair_retry_after_cycle: int = 0
    take_profit_count: int = 0
    restore_count: int = 0
    cycle_id: int = 0
    last_symbol_switch_minute: Optional[int] = None
    peak_equity: float = 0.0
    current_drawdown: float = 0.0
    opening_pair_failure_count: int = 0
    opening_retry_after_cycle: int = 0
    opening_imbalance_tolerance: float = 0.0
    repair_deferred_side: Optional[PositionSide] = None
    repair_deferred_quantity: float = 0.0
    repair_deferred_minimum_quantity: float = 0.0
    repair_deferred_minimum_notional: float = 0.0
    repair_deferred_reference_price: float = 0.0
    repair_deferred_cycle: int = 0
    repair_deferred_minimum_known: bool = False
    risk_degraded: bool = False
    risk_gross_cap_quantity: float = 0.0
    risk_recovery_healthy_since_ms: int = 0
    hard_risk_reduction_count: int = 0
    hard_risk_episode_reduction_count: int = 0
    last_action: str = "idle"
    last_error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "phase": self.phase.value,
            "selected_symbol": self.selected_symbol,
            "target_quantity": self.target_quantity,
            "long_quantity": self.long_quantity,
            "short_quantity": self.short_quantity,
            "long_entry_price": self.long_entry_price,
            "short_entry_price": self.short_entry_price,
            "long_unrealized_pnl": self.long_unrealized_pnl,
            "short_unrealized_pnl": self.short_unrealized_pnl,
            "last_mark_price": self.last_mark_price,
            "pending_rebalance_side": (
                self.pending_rebalance_side.value
                if self.pending_rebalance_side is not None
                else None
            ),
            "pending_harvest_quantity": self.pending_harvest_quantity,
            "pending_restore_quantity": self.pending_restore_quantity,
            "deferred_restore_carry_side": (
                self.deferred_restore_carry_side.value
                if self.deferred_restore_carry_side is not None
                else None
            ),
            "deferred_restore_carry_quantity": (
                self.deferred_restore_carry_quantity
            ),
            "profit_harvest_rsi_confirmed": (
                self.profit_harvest_rsi_confirmed
            ),
            "pending_harvest_trade_quantity": (
                self.pending_harvest_trade_quantity
            ),
            "profit_sweep_started_at_ms": self.profit_sweep_started_at_ms,
            "pending_repair_side": (
                self.pending_repair_side.value
                if self.pending_repair_side is not None
                else None
            ),
            "pending_repair_quantity": self.pending_repair_quantity,
            "repair_retry_after_cycle": self.repair_retry_after_cycle,
            "take_profit_count": self.take_profit_count,
            "restore_count": self.restore_count,
            "cycle_id": self.cycle_id,
            "last_symbol_switch_minute": self.last_symbol_switch_minute,
            "peak_equity": self.peak_equity,
            "current_drawdown": self.current_drawdown,
            "opening_pair_failure_count": self.opening_pair_failure_count,
            "opening_retry_after_cycle": self.opening_retry_after_cycle,
            "opening_imbalance_tolerance": (
                self.opening_imbalance_tolerance
            ),
            "repair_deferred_side": (
                self.repair_deferred_side.value
                if self.repair_deferred_side is not None
                else None
            ),
            "repair_deferred_quantity": self.repair_deferred_quantity,
            "repair_deferred_minimum_quantity": (
                self.repair_deferred_minimum_quantity
            ),
            "repair_deferred_minimum_notional": (
                self.repair_deferred_minimum_notional
            ),
            "repair_deferred_reference_price": (
                self.repair_deferred_reference_price
            ),
            "repair_deferred_cycle": self.repair_deferred_cycle,
            "repair_deferred_minimum_known": (
                self.repair_deferred_minimum_known
            ),
            "risk_degraded": self.risk_degraded,
            "risk_gross_cap_quantity": self.risk_gross_cap_quantity,
            "risk_recovery_healthy_since_ms": (
                self.risk_recovery_healthy_since_ms
            ),
            "hard_risk_reduction_count": (
                self.hard_risk_reduction_count
            ),
            "hard_risk_episode_reduction_count": (
                self.hard_risk_episode_reduction_count
            ),
            "last_action": self.last_action,
            "last_error": self.last_error,
        }

    @classmethod
    def from_dict(cls, payload: object) -> "HedgeState":
        data = payload if isinstance(payload, dict) else {}
        phase_text = str(data.get("phase") or StrategyPhase.IDLE.value)
        long_quantity = max(
            0.0,
            float(data.get("long_quantity", 0) or 0),
        )
        short_quantity = max(
            0.0,
            float(data.get("short_quantity", 0) or 0),
        )
        side_text = str(data.get("pending_rebalance_side") or "")
        deferred_restore_carry_side_text = str(
            data.get("deferred_restore_carry_side") or ""
        )
        repair_side_text = str(data.get("pending_repair_side") or "")
        repair_deferred_side_text = str(
            data.get("repair_deferred_side") or ""
        )
        if phase_text == "RISK_REDUCTION":
            if long_quantity > 1e-10 or short_quantity > 1e-10:
                phase = (
                    StrategyPhase.HEDGED
                    if abs(long_quantity - short_quantity) <= 1e-10
                    else StrategyPhase.REPAIRING_HEDGE
                )
            else:
                phase = StrategyPhase.SELECTING_SYMBOL
        else:
            try:
                phase = StrategyPhase(phase_text)
            except ValueError:
                phase = StrategyPhase.IDLE
        try:
            side = PositionSide(side_text) if side_text else None
        except ValueError:
            side = None
        try:
            deferred_restore_carry_side = (
                PositionSide(deferred_restore_carry_side_text)
                if deferred_restore_carry_side_text
                else None
            )
        except ValueError:
            deferred_restore_carry_side = None
        try:
            repair_side = (
                PositionSide(repair_side_text) if repair_side_text else None
            )
        except ValueError:
            repair_side = None
        try:
            repair_deferred_side = (
                PositionSide(repair_deferred_side_text)
                if repair_deferred_side_text
                else None
            )
        except ValueError:
            repair_deferred_side = None
        return cls(
            phase=phase,
            selected_symbol=data.get("selected_symbol"),
            target_quantity=float(data.get("target_quantity", 0) or 0),
            long_quantity=long_quantity,
            short_quantity=short_quantity,
            long_entry_price=float(data.get("long_entry_price", 0) or 0),
            short_entry_price=float(data.get("short_entry_price", 0) or 0),
            long_unrealized_pnl=float(data.get("long_unrealized_pnl", 0) or 0),
            short_unrealized_pnl=float(data.get("short_unrealized_pnl", 0) or 0),
            last_mark_price=float(data.get("last_mark_price", 0) or 0),
            pending_rebalance_side=side,
            pending_harvest_quantity=max(
                0.0,
                float(data.get("pending_harvest_quantity", 0) or 0),
            ),
            pending_restore_quantity=max(
                0.0,
                float(data.get("pending_restore_quantity", 0) or 0),
            ),
            deferred_restore_carry_side=deferred_restore_carry_side,
            deferred_restore_carry_quantity=max(
                0.0,
                float(
                    data.get("deferred_restore_carry_quantity", 0) or 0
                ),
            ),
            profit_harvest_rsi_confirmed=parse_bool(
                data.get("profit_harvest_rsi_confirmed", False)
            ),
            pending_harvest_trade_quantity=max(
                0.0,
                float(data.get("pending_harvest_trade_quantity", 0) or 0),
            ),
            profit_sweep_started_at_ms=max(
                0,
                int(data.get("profit_sweep_started_at_ms", 0) or 0),
            ),
            pending_repair_side=repair_side,
            pending_repair_quantity=max(
                0.0,
                float(data.get("pending_repair_quantity", 0) or 0),
            ),
            repair_retry_after_cycle=max(
                0,
                int(data.get("repair_retry_after_cycle", 0) or 0),
            ),
            take_profit_count=int(data.get("take_profit_count", 0) or 0),
            restore_count=int(data.get("restore_count", 0) or 0),
            cycle_id=int(data.get("cycle_id", 0) or 0),
            last_symbol_switch_minute=(
                None
                if data.get("last_symbol_switch_minute") is None
                else int(data.get("last_symbol_switch_minute"))
            ),
            peak_equity=float(data.get("peak_equity", 0) or 0),
            current_drawdown=float(data.get("current_drawdown", 0) or 0),
            opening_pair_failure_count=max(
                0,
                int(data.get("opening_pair_failure_count", 0) or 0),
            ),
            opening_retry_after_cycle=max(
                0,
                int(data.get("opening_retry_after_cycle", 0) or 0),
            ),
            opening_imbalance_tolerance=max(
                0.0,
                float(data.get("opening_imbalance_tolerance", 0) or 0),
            ),
            repair_deferred_side=repair_deferred_side,
            repair_deferred_quantity=max(
                0.0,
                float(data.get("repair_deferred_quantity", 0) or 0),
            ),
            repair_deferred_minimum_quantity=max(
                0.0,
                float(
                    data.get("repair_deferred_minimum_quantity", 0)
                    or 0
                ),
            ),
            repair_deferred_minimum_notional=max(
                0.0,
                float(
                    data.get("repair_deferred_minimum_notional", 0)
                    or 0
                ),
            ),
            repair_deferred_reference_price=max(
                0.0,
                float(
                    data.get("repair_deferred_reference_price", 0)
                    or 0
                ),
            ),
            repair_deferred_cycle=max(
                0,
                int(data.get("repair_deferred_cycle", 0) or 0),
            ),
            repair_deferred_minimum_known=parse_bool(
                data.get("repair_deferred_minimum_known", False)
            ),
            risk_degraded=parse_bool(data.get("risk_degraded", False)),
            risk_gross_cap_quantity=max(
                0.0,
                float(data.get("risk_gross_cap_quantity", 0) or 0),
            ),
            risk_recovery_healthy_since_ms=max(
                0,
                int(data.get("risk_recovery_healthy_since_ms", 0) or 0),
            ),
            hard_risk_reduction_count=max(
                0,
                int(data.get("hard_risk_reduction_count", 0) or 0),
            ),
            hard_risk_episode_reduction_count=max(
                0,
                int(
                    data.get(
                        "hard_risk_episode_reduction_count",
                        0,
                    )
                    or 0
                ),
            ),
            last_action=str(data.get("last_action") or "idle"),
            last_error=data.get("last_error"),
        )


@dataclass
class EngineStepResult:
    action: str
    events: List[dict] = field(default_factory=list)
    status_changed: bool = False


MarketRow = Dict[str, object]
