"""Canonical RWUSD selector, hedge, lifecycle, risk, and profit engine."""

from __future__ import annotations

import math
import time
from datetime import datetime, timezone
from typing import List, Optional

from backend.strategy.coin_margined_rwusd.exit import (
    ReductionPlan,
    RWUSDExitStage,
    RWUSDExitState,
)
from backend.strategy.coin_margined_rwusd.lifecycle import DryRunOrderLifecycle, FillEvent
from backend.strategy.coin_margined_rwusd.market_features import (
    thirty_minute_row_volatility_allowed,
)
from backend.strategy.coin_margined_rwusd.models import (
    EngineStepResult,
    HedgeState,
    MarketRow,
    PortfolioSnapshot,
    PositionSide,
    ProfitBucket,
    RiskDecision,
    SelectorSnapshot,
    StrategyPhase,
)
from backend.strategy.coin_margined_rwusd.persistence import RWUSDStateStore
from backend.strategy.coin_margined_rwusd.risk import RiskRuleSet
from backend.strategy.coin_margined_rwusd.selector import SymbolSelector


HARVEST_ORDER_PURPOSES = {"take_profit", "restore"}
HEDGE_REPAIR_PAIR_PURPOSE = "hedge_repair_pair"
PAIR_BUILD_PURPOSES = {
    "open_hedge",
    "top_up_hedge",
    HEDGE_REPAIR_PAIR_PURPOSE,
}
VECTOR_REPAIR_PURPOSES = {"hedge_repair"}
REDUCTION_ORDER_PURPOSES = {
    "customer_exit",
    "customer_exit_residual",
    "hard_risk_reduction",
}
# Hard risk has priority over every non-exit order, including restore. The
# reduction plan absorbs any asymmetry left after cancellation.
HARD_RISK_ALLOWED_PURPOSES = REDUCTION_ORDER_PURPOSES | {"manual_flatten"}
ENGINE_STATE_VERSION = 15
LEGACY_ENGINE_STATE_VERSION = 14
RWUSD_MINIMUM_BLOCK_REASON = "rwusd_amount_below_exchange_minimum"
RWUSD_WITHDRAW_BLOCK_REASON = "rwusd_source_not_withdrawable"
RWUSD_MARGIN_LOCKED_BLOCK_REASON = "rwusd_source_margin_locked"
RWUSD_SOURCE_BALANCE_BLOCK_REASON = "rwusd_source_balance_below_minimum"
PROFIT_SWEEP_IMMEDIATE_RELEASE_REASONS = {
    "below_min_sweep",
    RWUSD_MINIMUM_BLOCK_REASON,
    RWUSD_WITHDRAW_BLOCK_REASON,
    "rwusd_subscription_disabled",
}
PROFIT_CASH_SPOT_STAGES = frozenset(
    {
        "TRANSFERRED",
        "CONVERT_ACCEPT_PENDING",
        "CONVERT_ACCEPT_UNCERTAIN",
        "CONVERT_PROCESSING",
        "CONVERTED",
        "SUBSCRIBE_PENDING",
        "SUBSCRIBE_UNCERTAIN",
        "SUBSCRIBED",
        "COLLATERAL_TRANSFER_PENDING",
        "COLLATERAL_TRANSFER_UNCERTAIN",
        "COLLATERAL_TRANSFERRED",
    }
)


def _is_profit_sweep_immediate_release_reason(reason: str) -> bool:
    return (
        reason in PROFIT_SWEEP_IMMEDIATE_RELEASE_REASONS
        or reason == RWUSD_MARGIN_LOCKED_BLOCK_REASON
        or reason.startswith(f"{RWUSD_MARGIN_LOCKED_BLOCK_REASON}:")
        or reason.startswith(f"{RWUSD_SOURCE_BALANCE_BLOCK_REASON}:")
        or reason.startswith("transfer_retry_after_rejection:")
        or reason.startswith("transfer_retry_waiting_for_")
        or reason.startswith("transfer_capacity_unavailable:")
    )
EXTERNAL_ACTIVE_ORDER_STATUSES = {"NEW", "PARTIALLY_FILLED"}
EXTERNAL_TERMINAL_ORDER_STATUSES = {
    "FILLED",
    "CANCELED",
    "EXPIRED",
    "EXPIRED_IN_MATCH",
    "REJECTED",
}


class RWUSDEngine:
    """Single source of truth for backend behavior and frontend status."""

    def __init__(
        self,
        account_name: str,
        initial_capital: float,
        config: object,
        external_execution: bool = False,
        execution_environment: str = "dry_run",
        live_rwusd_execution: bool = False,
        leverage: Optional[float] = None,
        state_store: Optional[RWUSDStateStore] = None,
    ) -> None:
        self.account_name = account_name
        self.initial_capital = max(0.0, float(initial_capital or 0))
        self.config = config
        self.external_execution = bool(external_execution)
        self.execution_environment = str(
            execution_environment or "dry_run"
        ).strip()
        self.live_rwusd_execution = bool(live_rwusd_execution)
        self.leverage = max(
            1.0,
            float(
                getattr(config, "SIM_LEVERAGE", 50.0)
                if leverage is None
                else leverage
            ),
        )
        self.state_store = state_store or RWUSDStateStore(account_name)
        self.state = HedgeState()
        self.profit_bucket = ProfitBucket()
        self.lifecycle = DryRunOrderLifecycle()
        self.selector = SymbolSelector(
            switch_threshold=float(getattr(config, "SELECTOR_SWITCH_EDGE", 0.20)),
            eval_interval_minutes=int(getattr(config, "SELECTOR_EVAL_INTERVAL_MINUTES", 15)),
            switch_cooldown_minutes=int(
                getattr(config, "SELECTOR_SWITCH_COOLDOWN_MINUTES", 60)
            ),
        )
        self.risk_rules = RiskRuleSet(
            soft_unimmr=float(getattr(config, "SOFT_UNIMMR", 4.5)),
            hard_unimmr=float(getattr(config, "HARD_UNIMMR", 4.0)),
            max_drawdown=float(getattr(config, "MAX_DRAWDOWN", 0.15)),
            redeem_unimmr=float(getattr(config, "REDEEM_UNIMMR", 0.0)),
            max_total_net_leverage=getattr(config, "MAX_TOTAL_NET_LEVERAGE", None),
            max_single_symbol_net_leverage=getattr(
                config,
                "MAX_SINGLE_SYMBOL_NET_LEVERAGE",
                None,
            ),
            hard_available_balance=float(
                getattr(config, "HARD_AVAILABLE_BALANCE", 0.0)
            ),
        )
        self.selector_snapshot = SelectorSnapshot()
        self.last_risk_decision = RiskDecision()
        self.last_portfolio_snapshot = PortfolioSnapshot(self.initial_capital)
        self._pending_take_profit_gross = 0.0
        self._pending_take_profit_notional = 0.0
        self._last_market_row: Optional[MarketRow] = None
        self._last_step_at: Optional[str] = None
        self._external_target_quantity = 0.0
        self._external_target_notional = 0.0
        self._external_opening_tranche_quantity = 0.0
        self._external_opening_tranche_notional = 0.0
        self._external_opening_tranche_block_reason: Optional[str] = None
        self._external_minimum_order_quantity = 0.0
        self._external_restore_minimum_order_quantity = 0.0
        self._external_order_step_size = 0.0
        self._execution_cost_bps = float(
            getattr(config, "MAKER_FEE_FALLBACK_BPS", 2.0)
        ) + float(getattr(config, "MAKER_EXECUTION_BUFFER_BPS", 0.0))
        self._exchange_ownership_established = False
        self.exit_state = RWUSDExitState()
        self._last_persisted_payload: Optional[dict] = None

    def restore(self) -> bool:
        payload = self.state_store.load_strict()
        if not payload:
            return False
        version = int(payload.get("version", 0) or 0)
        if version not in {
            LEGACY_ENGINE_STATE_VERSION,
            ENGINE_STATE_VERSION,
        }:
            raise RuntimeError(
                f"unsupported RWUSD engine state version: {version}"
            )
        required = {
            "version",
            "mode",
            "execution_enabled",
            "rwusd_shadow",
            "rwusd_testnet_supported",
            "state",
            "profit_bucket",
            "lifecycle",
            "pending_take_profit_gross",
            "pending_take_profit_notional",
            "selector_scores",
            "last_step_at",
        }
        if version >= ENGINE_STATE_VERSION:
            required.update(
                {
                    "exchange_ownership_established",
                    "exit_state",
                }
            )
        missing = sorted(required.difference(payload))
        if missing:
            raise RuntimeError(
                "RWUSD engine state is missing fields: " + ",".join(missing)
            )
        persisted_mode = str(payload.get("mode") or "").strip()
        if persisted_mode != self.execution_environment:
            raise RuntimeError("RWUSD engine state belongs to another mode")
        for field in (
            "execution_enabled",
            "rwusd_shadow",
            "rwusd_testnet_supported",
        ):
            if not isinstance(payload.get(field), bool):
                raise RuntimeError(
                    f"RWUSD engine {field} state is invalid"
                )
        if bool(payload.get("execution_enabled")) != self.external_execution:
            raise RuntimeError("RWUSD engine execution mode changed")
        if bool(payload.get("rwusd_shadow")) == self.live_rwusd_execution:
            raise RuntimeError("RWUSD engine shadow mode changed")
        if bool(payload.get("rwusd_testnet_supported")):
            raise RuntimeError(
                "RWUSD engine testnet product support state is invalid"
            )
        if (
            version >= ENGINE_STATE_VERSION
            and not isinstance(
                payload.get("exchange_ownership_established"),
                bool,
            )
        ):
            raise RuntimeError(
                "RWUSD engine exchange ownership state is invalid"
            )
        for field in ("state", "profit_bucket", "lifecycle"):
            if not isinstance(payload.get(field), dict):
                raise RuntimeError(
                    f"RWUSD engine {field} state is not an object"
                )
        if not isinstance(payload.get("selector_scores"), list):
            raise RuntimeError("RWUSD selector state is not a list")
        restored_state = HedgeState.from_dict(payload.get("state"))
        restored_profit_bucket = ProfitBucket.from_dict(
            payload.get("profit_bucket")
        )
        restored_lifecycle = DryRunOrderLifecycle.from_dict(
            payload.get("lifecycle")
        )
        if (
            restored_state.phase
            in {
                StrategyPhase.TAKING_PROFIT,
                StrategyPhase.PROFIT_SWEEPING,
                StrategyPhase.REBALANCING,
            }
            and restored_state.pending_rebalance_side is not None
            and (
                restored_state.pending_harvest_quantity > 1e-10
                or restored_state.pending_restore_quantity > 1e-10
                or any(
                    order.purpose in HARVEST_ORDER_PURPOSES
                    for order in restored_lifecycle.active_orders
                )
            )
        ):
            restored_state.profit_harvest_rsi_confirmed = True
        raw_orders = list((payload.get("lifecycle") or {}).get("orders") or [])
        if len(restored_lifecycle.orders) != len(raw_orders):
            raise RuntimeError("RWUSD order lifecycle contains invalid orders")
        self._validate_restored_numbers(
            restored_state,
            restored_profit_bucket,
            restored_lifecycle,
        )
        if any(
            order.purpose
            in {
                "manual_flatten",
                "customer_exit",
                "customer_exit_residual",
            }
            for order in restored_lifecycle.active_orders
        ):
            restored_state.phase = StrategyPhase.FLATTENING
        restored_pending_take_profit_gross = self._finite_state_number(
            payload.get("pending_take_profit_gross"),
            "pending_take_profit_gross",
        )
        restored_pending_take_profit_notional = max(
            0.0,
            self._finite_state_number(
                payload.get("pending_take_profit_notional"),
                "pending_take_profit_notional",
            ),
        )
        explicit_ownership = payload.get("exchange_ownership_established")
        restored_exchange_ownership = (
            bool(explicit_ownership)
            if explicit_ownership is not None
            else self._exchange_ownership_present(
                restored_state,
                restored_lifecycle,
            )
        )
        scores = payload.get("selector_scores", [])
        rows = []
        for item in scores:
            if not isinstance(item, dict):
                raise RuntimeError("RWUSD selector score is not an object")
            rows.append(
                {
                    "symbol": item.get("symbol"),
                    "liquidity": item.get("liquidity_score", 0),
                    "volatility": item.get("volatility_score", 0),
                    "funding": item.get("funding_score", 0),
                    "margin": item.get("margin_efficiency_score", 0),
                    "execution_cost_bps": item.get("execution_cost_bps", 0),
                    "capacity_ratio": item.get("capacity_ratio", 1),
                    "max_notional_value": item.get(
                        "max_notional_value",
                        0,
                    ),
                    "required_gross_notional": item.get(
                        "required_gross_notional",
                        0,
                    ),
                    "reject_reason": item.get("reject_reason"),
                    "blocked": bool(item.get("reject_reason")),
                }
            )
        restored_selector_snapshot = SelectorSnapshot()
        if rows:
            restored_selector_snapshot = self.selector.select(
                restored_state.selected_symbol,
                rows,
            )
        restored_exit_state = RWUSDExitState.from_dict(
            payload.get("exit_state")
            if version >= ENGINE_STATE_VERSION
            else None
        )
        last_step_at = payload.get("last_step_at")
        if last_step_at is not None and not isinstance(last_step_at, str):
            raise RuntimeError("RWUSD engine last_step_at state is invalid")

        self.state = restored_state
        # The old field represented a historical bypass, not a valid hedge
        # health condition. Never allow it to survive into active logic.
        self.state.opening_imbalance_tolerance = 0.0
        self.profit_bucket = restored_profit_bucket
        self.lifecycle = restored_lifecycle
        self._pending_take_profit_gross = restored_pending_take_profit_gross
        self._pending_take_profit_notional = (
            restored_pending_take_profit_notional
        )
        self._exchange_ownership_established = restored_exchange_ownership
        self.selector_snapshot = restored_selector_snapshot
        self.exit_state = restored_exit_state
        self._last_step_at = last_step_at
        self._last_persisted_payload = payload
        self._hydrate_deferred_restore_carry()
        return True

    @staticmethod
    def _finite_state_number(value: object, field: str) -> float:
        if isinstance(value, bool):
            raise RuntimeError(f"RWUSD engine {field} state is invalid")
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"RWUSD engine {field} state is invalid"
            ) from exc
        if not math.isfinite(parsed):
            raise RuntimeError(f"RWUSD engine {field} state is invalid")
        return parsed

    def persist(self) -> None:
        payload = {
            "version": ENGINE_STATE_VERSION,
            "mode": self.execution_environment,
            "execution_enabled": self.external_execution,
            "rwusd_shadow": not self.live_rwusd_execution,
            "rwusd_testnet_supported": False,
            "exchange_ownership_established": (
                self._exchange_ownership_established
            ),
            "state": self.state.to_dict(),
            "profit_bucket": self.profit_bucket.to_dict(),
            "lifecycle": self.lifecycle.to_dict(),
            "pending_take_profit_gross": self._pending_take_profit_gross,
            "pending_take_profit_notional": (
                self._pending_take_profit_notional
            ),
            "selector_scores": [
                item.to_dict() for item in self.selector_snapshot.scores
            ],
            "exit_state": self.exit_state.to_dict(),
            "last_step_at": self._last_step_at,
        }
        if payload == self._last_persisted_payload:
            return
        self.state_store.save(payload)
        self._last_persisted_payload = payload

    def _validate_restored_numbers(
        self,
        state: HedgeState,
        profit_bucket: ProfitBucket,
        lifecycle: DryRunOrderLifecycle,
    ) -> None:
        def _walk(value: object, path: str) -> None:
            if isinstance(value, bool) or value is None or isinstance(value, str):
                return
            if isinstance(value, (int, float)):
                if not math.isfinite(float(value)):
                    raise RuntimeError(
                        f"RWUSD state contains non-finite value at {path}"
                    )
                return
            if isinstance(value, dict):
                for key, item in value.items():
                    _walk(item, f"{path}.{key}")
                return
            if isinstance(value, list):
                for index, item in enumerate(value):
                    _walk(item, f"{path}[{index}]")
                return
            raise RuntimeError(f"RWUSD state contains invalid value at {path}")

        _walk(state.to_dict(), "state")
        _walk(profit_bucket.to_dict(), "profit_bucket")
        _walk(lifecycle.to_dict(), "lifecycle")
        selected = str(state.selected_symbol or "").strip().upper()
        candidates = {
            str(symbol or "").strip().upper()
            for symbol in getattr(self.config, "CANDIDATE_SYMBOLS", ())
        }
        if selected and selected not in candidates:
            raise RuntimeError(
                f"RWUSD state contains unsupported symbol: {selected}"
            )

    def request_customer_exit(
        self,
        now_ms: Optional[int] = None,
        *,
        redemption_required: Optional[bool] = None,
    ) -> dict:
        effective_now = max(
            1,
            int(now_ms or time.time() * 1000),
        )
        if self.exit_state.stage == RWUSDExitStage.COMPLETE:
            raise RuntimeError(
                "RWUSD exit is complete; explicit strategy start is required "
                "before a new exit"
            )
        self.exit_state.request_liquidation(
            effective_now,
            redemption_required=(
                self.live_rwusd_execution
                if redemption_required is None
                else bool(redemption_required)
            ),
        )
        self.state.phase = StrategyPhase.FLATTENING
        self.state.last_action = "customer_exit_requested"
        self.profit_bucket.sweep_block_reason = "customer_exit_locked"
        self.persist()
        return {
            "type": "rwusd_exit",
            "action": "customer_exit_requested",
            "stage": self.exit_state.stage.value,
            "reopen_locked": True,
            "simulated": not self.external_execution,
        }

    def request_customer_redemption(
        self,
        now_ms: Optional[int] = None,
    ) -> dict:
        effective_now = max(
            1,
            int(now_ms or time.time() * 1000),
        )
        self.exit_state.request_redemption(effective_now)
        self.state.phase = StrategyPhase.FLATTENING
        self.state.last_action = "customer_redemption_requested"
        self.persist()
        return {
            "type": "rwusd_exit",
            "action": "customer_redemption_requested",
            "stage": self.exit_state.stage.value,
            "reopen_locked": True,
            "simulated": not self.live_rwusd_execution,
        }

    def clear_completed_exit_for_restart(self) -> bool:
        if self.exit_state.stage != RWUSDExitStage.COMPLETE:
            return False
        self.exit_state.clear_for_explicit_restart()
        self.state = HedgeState(
            phase=StrategyPhase.SELECTING_SYMBOL,
            last_action="customer_exit_explicitly_rearmed",
        )
        self.profit_bucket = ProfitBucket()
        self.lifecycle = DryRunOrderLifecycle()
        self.selector_snapshot = SelectorSnapshot()
        self.last_risk_decision = RiskDecision()
        self.last_portfolio_snapshot = PortfolioSnapshot(self.initial_capital)
        self._pending_take_profit_gross = 0.0
        self._pending_take_profit_notional = 0.0
        self._last_step_at = None
        self._external_target_quantity = 0.0
        self._external_target_notional = 0.0
        self._external_opening_tranche_quantity = 0.0
        self._external_opening_tranche_notional = 0.0
        self._external_opening_tranche_block_reason = None
        self._external_minimum_order_quantity = 0.0
        self._external_restore_minimum_order_quantity = 0.0
        self._external_order_step_size = 0.0
        self._exchange_ownership_established = False
        self.persist()
        return True

    def official_restart_rearmed(self) -> bool:
        if (
            self.exit_state.stage != RWUSDExitStage.INACTIVE
            or self.exit_state.reopen_locked
            or self.state.phase != StrategyPhase.SELECTING_SYMBOL
        ):
            return False
        try:
            self.validate_unfunded_primary_promotion()
        except RuntimeError:
            return False
        return True

    def exit_worker_required(self) -> bool:
        return self.exit_state.stage in {
            RWUSDExitStage.LIQUIDATING,
            RWUSDExitStage.REDEEMING,
            RWUSDExitStage.REDEEM_PENDING,
        }

    def exit_status(self) -> dict:
        return self.exit_state.status_payload()

    def plan_external_reduction(
        self,
        *,
        plan: ReductionPlan,
        purpose: str,
        mark_price: float,
    ) -> List[dict]:
        normalized_purpose = str(purpose or "").strip()
        if normalized_purpose not in REDUCTION_ORDER_PURPOSES:
            raise ValueError("unsupported RWUSD reduction purpose")
        if self.lifecycle.active_orders:
            return []
        mark = max(0.0, float(mark_price or self.state.last_mark_price or 0))
        if mark <= 0:
            raise RuntimeError("RWUSD reduction requires a valid mark price")
        created: List[str] = []
        if plan.long_quantity > 1e-12:
            order = self.lifecycle.create_order(
                position_side=PositionSide.LONG,
                side="SELL",
                quantity=min(plan.long_quantity, self.state.long_quantity),
                price=mark,
                reduce_only=True,
                purpose=normalized_purpose,
                order_type=(
                    "LIMIT"
                    if normalized_purpose == "customer_exit"
                    else "MARKET"
                ),
                time_in_force=(
                    "GTX"
                    if normalized_purpose == "customer_exit"
                    else ""
                ),
                maker_only=normalized_purpose == "customer_exit",
            )
            if order is not None:
                created.append(order.order_id)
        if plan.short_quantity > 1e-12:
            order = self.lifecycle.create_order(
                position_side=PositionSide.SHORT,
                side="BUY",
                quantity=min(plan.short_quantity, self.state.short_quantity),
                price=mark,
                reduce_only=True,
                purpose=normalized_purpose,
                order_type=(
                    "LIMIT"
                    if normalized_purpose == "customer_exit"
                    else "MARKET"
                ),
                time_in_force=(
                    "GTX"
                    if normalized_purpose == "customer_exit"
                    else ""
                ),
                maker_only=normalized_purpose == "customer_exit",
            )
            if order is not None:
                created.append(order.order_id)
        if not created:
            return []
        self.state.phase = StrategyPhase.FLATTENING
        self.state.last_action = normalized_purpose
        if normalized_purpose == "hard_risk_reduction":
            self.state.risk_degraded = True
            self.state.risk_recovery_healthy_since_ms = 0
            self.state.risk_gross_cap_quantity = min(
                max(0.0, self.state.long_quantity - plan.long_quantity),
                max(0.0, self.state.short_quantity - plan.short_quantity),
            )
        self.persist()
        return [
            {
                "type": "exchange_order_plan",
                "action": normalized_purpose,
                "order_ids": created,
                "long_quantity": plan.long_quantity,
                "short_quantity": plan.short_quantity,
                "desired_fraction": plan.desired_fraction,
                "bid_depth_quantity": plan.bid_depth_quantity,
                "ask_depth_quantity": plan.ask_depth_quantity,
                "reason": plan.reason,
                "simulated": False,
            }
        ]

    def finalize_external_reduction(self, purpose: str) -> None:
        normalized_purpose = str(purpose or "").strip()
        if normalized_purpose == "customer_exit":
            if (
                self.state.long_quantity <= 1e-10
                and self.state.short_quantity <= 1e-10
                and not self.lifecycle.active_orders
            ):
                peak_equity = self.state.peak_equity
                selected_symbol = self.state.selected_symbol
                last_mark_price = self.state.last_mark_price
                self.state = HedgeState(
                    phase=StrategyPhase.FLATTENING,
                    selected_symbol=selected_symbol,
                    last_mark_price=last_mark_price,
                    peak_equity=peak_equity,
                    last_action="customer_exit_flat_confirmed",
                )
                self.lifecycle = DryRunOrderLifecycle()
                self._pending_take_profit_gross = 0.0
                self._pending_take_profit_notional = 0.0
                self.exit_state.mark_flat(int(time.time() * 1000))
                if self.exit_state.stage == RWUSDExitStage.COMPLETE:
                    self.state.last_action = "customer_exit_complete"
        elif normalized_purpose == "hard_risk_reduction":
            self._clear_harvest_task_state()
            self.state.hard_risk_reduction_count += 1
            self.state.hard_risk_episode_reduction_count += 1
            self.state.risk_degraded = True
            self.state.risk_gross_cap_quantity = self.effective_hedge_quantity()
            if (
                self.state.long_quantity <= 1e-10
                and self.state.short_quantity <= 1e-10
            ):
                self.state.phase = StrategyPhase.SELECTING_SYMBOL
            elif self._positions_are_balanced():
                self.state.phase = StrategyPhase.HEDGED
            else:
                self.state.phase = StrategyPhase.REPAIRING_HEDGE
            self.state.last_action = "hard_risk_reduction_confirmed"
        self.persist()

    def update_risk_degrade_state(
        self,
        *,
        portfolio: PortfolioSnapshot,
        now_ms: int,
    ) -> None:
        if self.last_risk_decision.hard_risk:
            if not self.state.risk_degraded:
                self.state.hard_risk_episode_reduction_count = 0
            self.state.risk_degraded = True
            self.state.risk_recovery_healthy_since_ms = 0
            current = self.effective_hedge_quantity()
            if (
                self.state.risk_gross_cap_quantity <= 0
                or current < self.state.risk_gross_cap_quantity
            ):
                self.state.risk_gross_cap_quantity = current
            return
        if not self.state.risk_degraded:
            return
        release_unimmr = float(
            getattr(self.config, "HARD_RISK_RELEASE_UNIMMR", 5.5)
        )
        healthy = (
            portfolio.uni_mmr >= release_unimmr
            and not self.last_risk_decision.net_exposure_limit
        )
        if not healthy:
            self.state.risk_recovery_healthy_since_ms = 0
            return
        if self.state.risk_recovery_healthy_since_ms <= 0:
            self.state.risk_recovery_healthy_since_ms = max(1, int(now_ms))
            return
        stable_ms = int(
            max(
                0.0,
                float(
                    getattr(
                        self.config,
                        "HARD_RISK_RELEASE_STABLE_SEC",
                        1800.0,
                    )
                ),
            )
            * 1000
        )
        if now_ms - self.state.risk_recovery_healthy_since_ms < stable_ms:
            return
        self.state.risk_degraded = False
        self.state.risk_gross_cap_quantity = 0.0
        self.state.risk_recovery_healthy_since_ms = 0
        self.state.hard_risk_episode_reduction_count = 0
        self.state.last_action = "hard_risk_degrade_released"

    def risk_target_quantity_cap(self) -> Optional[float]:
        if not self.state.risk_degraded:
            return None
        return max(0.0, self.state.risk_gross_cap_quantity)

    def observe(
        self,
        *,
        rows: List[MarketRow],
        portfolio: PortfolioSnapshot,
        now: Optional[datetime] = None,
    ) -> None:
        """Refresh read-only status without advancing the simulated workflow."""
        effective_now = now or datetime.now(timezone.utc)
        minute_of_day = effective_now.hour * 60 + effective_now.minute
        self._last_step_at = effective_now.astimezone(timezone.utc).isoformat()
        self.last_portfolio_snapshot = portfolio
        self._update_drawdown(portfolio.account_equity)
        self.selector_snapshot = self.selector.select(
            current_symbol=self.state.selected_symbol,
            rows=rows,
            minute_of_day=minute_of_day,
            last_switch_minute=self.state.last_symbol_switch_minute,
        )
        self._hold_current_selector_during_active_cycle()
        selected_row = self._row_for_selected(rows)
        self._last_market_row = (
            dict(selected_row) if selected_row is not None else None
        )
        self.last_risk_decision = self.risk_rules.evaluate(
            portfolio,
            self.state.current_drawdown,
        )
        self.update_risk_degrade_state(
            portfolio=portfolio,
            now_ms=int(effective_now.timestamp() * 1000),
        )
        self._clear_recovered_subscription_block()
        self._resume_legacy_paused_state()
        if self.exit_state.reopen_locked:
            self.state.phase = StrategyPhase.FLATTENING
            self.state.last_action = "customer_exit_restored"
        elif self.state.phase in {
            StrategyPhase.IDLE,
            StrategyPhase.SELECTING_SYMBOL,
        }:
            selected = self.selector_snapshot.selected_symbol
            if selected:
                self.state.selected_symbol = selected
                self.state.last_symbol_switch_minute = minute_of_day
            self.state.phase = StrategyPhase.SELECTING_SYMBOL
            self.state.last_action = "initialized_read_only"
        self._update_mark_and_pnl(rows)
        self.persist()

    def _resume_legacy_paused_state(self) -> None:
        if self.state.phase != StrategyPhase.PAUSED:
            return
        purposes = {
            str(order.purpose or "").strip().lower()
            for order in self.lifecycle.active_orders
        }
        has_position = (
            self.state.long_quantity > 1e-10
            or self.state.short_quantity > 1e-10
        )
        if "manual_flatten" in purposes or purposes.intersection(
            REDUCTION_ORDER_PURPOSES
        ):
            phase = StrategyPhase.FLATTENING
        elif "take_profit" in purposes:
            phase = StrategyPhase.TAKING_PROFIT
        elif "restore" in purposes:
            phase = StrategyPhase.REBALANCING
        elif (
            self.state.pending_rebalance_side is not None
            and self.profit_bucket.pending_subscription_kind == "harvest"
        ):
            phase = StrategyPhase.PROFIT_SWEEPING
        elif purposes.intersection(
            VECTOR_REPAIR_PURPOSES | {HEDGE_REPAIR_PAIR_PURPOSE}
        ):
            phase = StrategyPhase.REPAIRING_HEDGE
        elif "open_hedge" in purposes:
            phase = StrategyPhase.OPENING_HEDGE
        elif has_position:
            phase = (
                StrategyPhase.HEDGED
                if self._positions_are_balanced()
                else StrategyPhase.REPAIRING_HEDGE
            )
        else:
            phase = StrategyPhase.SELECTING_SYMBOL
        self.state.phase = phase
        if phase == StrategyPhase.SELECTING_SYMBOL:
            self.state.pending_rebalance_side = None
            self.state.pending_harvest_quantity = 0.0
            self.state.pending_restore_quantity = 0.0
            self._clear_deferred_restore_carry()
            self.state.profit_harvest_rsi_confirmed = False
            self.state.pending_harvest_trade_quantity = 0.0
            self.state.profit_sweep_started_at_ms = 0
            self.state.opening_pair_failure_count = 0
            self.state.opening_retry_after_cycle = 0
            self.state.opening_imbalance_tolerance = 0.0
            self._clear_repair_state()
            self._pending_take_profit_gross = 0.0
            self._pending_take_profit_notional = 0.0
            self.lifecycle.external_consecutive_rejection_count = 0
            self.lifecycle.external_consecutive_maker_requote_count = 0
            self.lifecycle.clear_deferred_minimum()
        elif phase == StrategyPhase.HEDGED:
            self._clear_repair_state()
        self.state.last_action = "legacy_pause_migrated"
        self.state.last_error = None
        self.persist()

    def step(
        self,
        *,
        rows: List[MarketRow],
        portfolio: PortfolioSnapshot,
        now: Optional[datetime] = None,
        elapsed_hours: float = 0.0,
    ) -> EngineStepResult:
        effective_now = now or datetime.now(timezone.utc)
        minute_of_day = effective_now.hour * 60 + effective_now.minute
        self._last_step_at = effective_now.astimezone(timezone.utc).isoformat()
        self.state.cycle_id += 1
        self.lifecycle.advance_cycle()
        self.last_portfolio_snapshot = portfolio
        events: List[dict] = []

        if (
            not self.live_rwusd_execution
            and self.profit_bucket.rwusd_principal > 0
            and elapsed_hours > 0
        ):
            hourly_rate = float(getattr(self.config, "RWUSD_APR", 0.12)) / 365.0 / 24.0
            self.profit_bucket.rwusd_interest_accrued += (
                self.profit_bucket.rwusd_principal * hourly_rate * elapsed_hours
            )

        self._update_drawdown(portfolio.account_equity)
        self._update_mark_and_pnl(rows)
        self.selector_snapshot = self.selector.select(
            current_symbol=self.state.selected_symbol,
            rows=rows,
            minute_of_day=minute_of_day,
            last_switch_minute=self.state.last_symbol_switch_minute,
        )
        self._hold_current_selector_during_active_cycle()
        selected_row = self._row_for_selected(rows)
        self._last_market_row = (
            dict(selected_row) if selected_row is not None else None
        )
        previous_hard_risk = self.last_risk_decision.hard_risk
        self.last_risk_decision = self.risk_rules.evaluate(
            portfolio,
            self.state.current_drawdown,
        )
        now_ms = int(effective_now.timestamp() * 1000)
        self.update_risk_degrade_state(
            portfolio=portfolio,
            now_ms=now_ms,
        )
        self._clear_recovered_subscription_block()
        self._resume_legacy_paused_state()
        if self.exit_state.reopen_locked:
            if not self.external_execution:
                self.lifecycle.cancel_active_except(
                    REDUCTION_ORDER_PURPOSES | {"manual_flatten"}
                )
            self.state.phase = StrategyPhase.FLATTENING
            self.state.last_action = (
                "customer_exit_waiting_redemption"
                if self.exit_state.stage
                == RWUSDExitStage.READY_TO_REDEEM
                else "customer_exit_locked"
            )
            self._update_mark_and_pnl(rows)
            self.persist()
            return EngineStepResult(
                action=self.state.last_action,
                events=[],
                status_changed=False,
            )
        if self.last_risk_decision.hard_risk:
            self._enter_hard_risk_freeze(
                events,
                emit_transition=not previous_hard_risk,
            )
        elif previous_hard_risk:
            events.append(
                {
                    "type": "risk",
                    "action": "risk_freeze_cleared",
                    "simulated": True,
                }
            )
        if not self.live_rwusd_execution:
            self._simulate_redeem_if_needed(
                portfolio,
                self.last_risk_decision,
                events,
            )

        self._expire_and_requote(events)

        mark_price = self._selected_mark(rows)
        if not self.external_execution and mark_price > 0:
            self._match_dry_run_orders(mark_price, events)

        if self.state.phase in {
            StrategyPhase.TAKING_PROFIT,
            StrategyPhase.PROFIT_SWEEPING,
            StrategyPhase.REBALANCING,
        }:
            self._plan_pending_harvest_orders(mark_price, events)
        elif not self.lifecycle.active_orders:
            has_gap = not self._positions_are_balanced()
            if self.last_risk_decision.hard_risk:
                self.state.last_action = "hard_risk_reduction_pending"
            elif (
                has_gap
                and self.state.phase != StrategyPhase.PROFIT_SWEEPING
            ):
                self._quote_vector_repair(mark_price, events)
            elif self.state.phase == StrategyPhase.PROFIT_SWEEPING:
                self.state.last_action = "profit_sweep_pending"
            elif self.state.phase in {
                StrategyPhase.IDLE,
                StrategyPhase.SELECTING_SYMBOL,
            }:
                self._start_selected_hedge(rows, minute_of_day, events)
                selected_row = self._row_for_selected(rows)
                self._last_market_row = (
                    dict(selected_row) if selected_row is not None else None
                )
            elif self.state.phase == StrategyPhase.OPENING_HEDGE:
                self._quote_opening_hedge(
                    mark_price,
                    events,
                    market_row=selected_row,
                )
            elif self.state.phase == StrategyPhase.HEDGED:
                self._maybe_take_profit(
                    mark_price,
                    portfolio,
                    events,
                    market_row=selected_row,
                )
                if (
                    self.state.phase == StrategyPhase.HEDGED
                    and not self.lifecycle.active_orders
                ):
                    self._quote_symmetric_top_up(
                        mark_price,
                        portfolio,
                        events,
                        market_row=selected_row,
                    )
            elif self.state.phase == StrategyPhase.REPAIRING_HEDGE:
                self._quote_vector_repair(mark_price, events)

        # Symbol selection can happen above, so refresh the selected mark before
        # matching newly-created dry-run orders in the same cycle.
        mark_price = self._selected_mark(rows)
        if not self.external_execution and mark_price > 0:
            self._match_dry_run_orders(mark_price, events)
            self._plan_pending_harvest_orders(mark_price, events)
            self._match_dry_run_orders(
                mark_price,
                events,
                purposes={"restore"},
            )
            if (
                self.state.phase == StrategyPhase.REPAIRING_HEDGE
                and not self.lifecycle.active_orders
            ):
                self._quote_vector_repair(mark_price, events)
                self._match_dry_run_orders(mark_price, events)

        if (
            self.state.phase
            in {
                StrategyPhase.TAKING_PROFIT,
                StrategyPhase.PROFIT_SWEEPING,
                StrategyPhase.HEDGED,
            }
            and not self.live_rwusd_execution
        ):
            self._simulate_profit_sweep(portfolio, events)
        if not self.external_execution:
            events.extend(self.finalize_profit_sweep())
            if self.state.phase == StrategyPhase.REBALANCING:
                self._plan_pending_harvest_orders(mark_price, events)
                self._match_dry_run_orders(
                    mark_price,
                    events,
                    purposes={"restore"},
                )

        self._update_mark_and_pnl(rows)
        self.persist()
        action = self.state.last_action
        return EngineStepResult(
            action=action,
            events=events,
            status_changed=bool(events),
        )

    def set_external_target_sizing(
        self,
        *,
        quantity: float,
        target_notional: float,
        opening_tranche_quantity: float,
        opening_tranche_notional: Optional[float] = None,
        opening_tranche_block_reason: Optional[str] = None,
        minimum_order_quantity: float = 0.0,
        restore_minimum_quantity: Optional[float] = None,
        order_step_size: float = 0.0,
    ) -> None:
        self._external_target_quantity = max(0.0, float(quantity or 0))
        self._external_target_notional = max(
            0.0,
            float(target_notional or 0),
        )
        self._external_opening_tranche_quantity = min(
            self._external_target_quantity,
            max(0.0, float(opening_tranche_quantity or 0)),
        )
        self._external_opening_tranche_notional = max(
            0.0,
            float(
                self._external_opening_tranche_quantity
                * self.state.last_mark_price
                if opening_tranche_notional is None
                else opening_tranche_notional
            ),
        )
        self._external_opening_tranche_block_reason = (
            str(opening_tranche_block_reason or "").strip() or None
        )
        self._external_minimum_order_quantity = max(
            0.0,
            float(minimum_order_quantity or 0),
        )
        self._external_restore_minimum_order_quantity = max(
            self._external_minimum_order_quantity,
            float(restore_minimum_quantity or 0),
        )
        self._external_order_step_size = max(
            0.0,
            float(order_step_size or 0),
        )
        self.state.target_quantity = self._external_target_quantity
        self.persist()

    def set_leverage(self, leverage: float) -> None:
        self.leverage = max(1.0, float(leverage))

    def effective_hedge_quantity(self) -> float:
        return min(self.state.long_quantity, self.state.short_quantity)

    def exchange_ownership_established(self) -> bool:
        return bool(self._exchange_ownership_established)

    @staticmethod
    def _exchange_ownership_present(
        state: HedgeState,
        lifecycle: DryRunOrderLifecycle,
    ) -> bool:
        return bool(
            state.long_quantity > 1e-12
            or state.short_quantity > 1e-12
            or any(
                order.external_client_order_id or order.external_order_id
                for order in lifecycle.orders
            )
        )

    def _legacy_exchange_ownership_present(self) -> bool:
        return self._exchange_ownership_present(
            self.state,
            self.lifecycle,
        )

    def ensure_exchange_trade_sync_started(self, started_at_ms: int) -> None:
        if self.profit_bucket.exchange_trade_sync_started_at_ms > 0:
            return
        self.profit_bucket.exchange_trade_sync_started_at_ms = max(
            1,
            int(started_at_ms or 0),
        )
        self.persist()

    def exchange_trade_cursor(self, symbol: str) -> int:
        value = self.profit_bucket.exchange_trade_cursors.get(
            str(symbol or "").strip().upper()
        )
        return -1 if value is None else int(value)

    def apply_external_user_trades(self, trades: List[dict]) -> List[dict]:
        events: List[dict] = []
        started_at = self.profit_bucket.exchange_trade_sync_started_at_ms
        for trade in sorted(
            (item for item in trades if isinstance(item, dict)),
            key=lambda item: (
                int(item.get("time", 0) or 0),
                int(item.get("trade_id", 0) or 0),
            ),
        ):
            symbol = str(trade.get("symbol") or "").strip().upper()
            trade_id = max(0, int(trade.get("trade_id", 0) or 0))
            trade_time = max(0, int(trade.get("time", 0) or 0))
            if not symbol:
                continue
            cursor = self.exchange_trade_cursor(symbol)
            if trade_id <= cursor:
                continue
            self.profit_bucket.exchange_trade_cursors[symbol] = trade_id
            if started_at > 0 and trade_time > 0 and trade_time < started_at:
                continue
            realized = float(trade.get("realized_pnl", 0) or 0)
            commission = max(
                0.0,
                float(
                    trade.get(
                        "commission_value_usd",
                        trade.get("commission", 0),
                    )
                    or 0
                ),
            )
            net = realized - commission
            purpose = str(
                trade.get("purpose") or "external_fill"
            ).strip().lower()
            self.profit_bucket.exchange_realized_pnl_total += realized
            self.profit_bucket.exchange_commission_total += commission
            self.profit_bucket.exchange_trade_count += 1
            self.profit_bucket.realized_pnl_total += net
            if purpose == "take_profit":
                credited = self._apply_external_harvest_delta(net)
                self.profit_bucket.pending_harvest_net_credited += credited
                if self.state.phase in {
                    StrategyPhase.TAKING_PROFIT,
                    StrategyPhase.PROFIT_SWEEPING,
                    StrategyPhase.REBALANCING,
                }:
                    self.state.pending_harvest_trade_quantity += max(
                        0.0,
                        float(trade.get("quantity", 0) or 0),
                    )
            else:
                self.profit_bucket.unsettled_harvest_pnl += net
            events.append(
                {
                    "type": "exchange_trade",
                    "action": purpose,
                    "trade_id": trade_id,
                    "order_id": str(trade.get("order_id") or ""),
                    "symbol": symbol,
                    "position_side": str(
                        trade.get("position_side") or ""
                    ),
                    "side": str(trade.get("side") or ""),
                    "quantity": max(
                        0.0,
                        float(trade.get("quantity", 0) or 0),
                    ),
                    "price": max(
                        0.0,
                        float(trade.get("price", 0) or 0),
                    ),
                    "realized_pnl": realized,
                    "commission": commission,
                    "commission_asset": str(
                        trade.get("commission_asset") or ""
                    ),
                    "net_realized_pnl": net,
                    "harvest_credit": (
                        credited if purpose == "take_profit" else 0.0
                    ),
                    "maker": bool(trade.get("maker", False)),
                    "time": trade_time,
                    "simulated": False,
                }
            )
        self.persist()
        return events

    def _settle_external_profit_if_balanced(self) -> None:
        if not self.external_execution or not self._positions_are_balanced():
            return
        if self.state.phase in {
            StrategyPhase.TAKING_PROFIT,
            StrategyPhase.PROFIT_SWEEPING,
            StrategyPhase.REBALANCING,
            StrategyPhase.REPAIRING_HEDGE,
        }:
            return
        net = self.profit_bucket.unsettled_harvest_pnl
        if abs(net) <= 1e-12:
            return
        credited = self._apply_external_harvest_delta(0.0)
        if credited > 0:
            self.profit_bucket.harvest_count += 1
        self.profit_bucket.closed_loop_ready = (
            self.profit_bucket.harvest_buffer + 1e-12
            >= float(getattr(self.config, "MIN_SWEEP", 1.0))
        )
        self.profit_bucket.last_rebalance_action = "exchange_fill_reconciled"
        self.profit_bucket.sweep_block_reason = (
            None
            if self.profit_bucket.closed_loop_ready
            else "below_min_sweep"
        )

    def _apply_external_harvest_delta(self, net: float) -> float:
        combined = (
            float(net or 0)
            + self.profit_bucket.unsettled_harvest_pnl
        )
        self.profit_bucket.unsettled_harvest_pnl = 0.0
        if combined < 0:
            deduction = min(
                self.profit_bucket.harvest_buffer,
                -combined,
            )
            self.profit_bucket.harvest_buffer -= deduction
            self.profit_bucket.realized_pnl_available_for_deposit = max(
                0.0,
                (
                    self.profit_bucket.realized_pnl_available_for_deposit
                    - deduction
                ),
            )
            combined += deduction
            if combined < -1e-12:
                self.profit_bucket.unsettled_harvest_pnl = combined
            self.profit_bucket.closed_loop_ready = (
                self.profit_bucket.harvest_buffer + 1e-12
                >= float(getattr(self.config, "MIN_SWEEP", 1.0))
            )
            return 0.0
        credited = max(0.0, combined)
        self.profit_bucket.harvest_buffer += credited
        self.profit_bucket.realized_pnl_available_for_deposit += credited
        self.profit_bucket.closed_loop_ready = (
            self.profit_bucket.harvest_buffer + 1e-12
            >= float(getattr(self.config, "MIN_SWEEP", 1.0))
        )
        self.profit_bucket.sweep_block_reason = (
            None
            if self.profit_bucket.closed_loop_ready
            else "below_min_sweep"
        )
        return credited

    def settle_external_exit_pnl(self) -> Optional[dict]:
        if (
            not self.external_execution
            or self.lifecycle.active_orders
            or self.state.long_quantity > 1e-10
            or self.state.short_quantity > 1e-10
        ):
            return None
        pending = float(self.profit_bucket.unsettled_harvest_pnl or 0.0)
        if abs(pending) <= 1e-12:
            return None
        buffer_before = self.profit_bucket.harvest_buffer
        credited = self._apply_external_harvest_delta(0.0)
        residual_loss = max(
            0.0,
            -float(self.profit_bucket.unsettled_harvest_pnl or 0.0),
        )
        self.profit_bucket.unsettled_harvest_pnl = 0.0
        self.profit_bucket.closed_loop_ready = False
        self.profit_bucket.last_rebalance_action = (
            "customer_exit_pnl_settled"
        )
        self.profit_bucket.sweep_block_reason = "customer_exit_locked"
        self.persist()
        return {
            "type": "rwusd_exit",
            "action": "liquidation_pnl_settled",
            "net_realized_pnl": pending,
            "harvest_buffer_deduction": max(
                0.0,
                buffer_before - self.profit_bucket.harvest_buffer,
            ),
            "harvest_buffer_credit": credited,
            "capital_loss": residual_loss,
            "simulated": False,
        }

    def set_execution_cost_bps(self, maker_fee_bps: float) -> None:
        self._execution_cost_bps = max(
            0.0,
            float(maker_fee_bps or 0)
            + float(
                getattr(
                    self.config,
                    "MAKER_EXECUTION_BUFFER_BPS",
                    0.0,
                )
            ),
        )

    def execution_cost_bps(self) -> float:
        return self._execution_cost_bps

    def plan_pending_harvest_orders(self, mark_price: float) -> List[dict]:
        events: List[dict] = []
        self._plan_pending_harvest_orders(
            max(0.0, float(mark_price or 0)),
            events,
        )
        self.persist()
        return events

    def opening_imbalance_is_tolerated(self, difference: float) -> bool:
        return abs(float(difference or 0)) <= 1e-10

    def record_external_order_dust(
        self,
        order_id: str,
        error: object,
        *,
        minimum_quantity: float = 0.0,
        minimum_notional: float = 0.0,
        reference_price: float = 0.0,
        minimum_known: bool = False,
    ) -> List[dict]:
        order = self.lifecycle.get_order(order_id)
        if order is None:
            return []
        remaining = max(
            0.0,
            float(order.quantity)
            - float(order.cumulative_filled_quantity),
        )
        order.external_status = "DEFERRED_MINIMUM"
        order.external_error = str(error or "order_dust_below_minimum")
        order.status = "EXPIRED"
        order.deferred_minimum_quantity = max(
            remaining + 1e-12,
            float(minimum_quantity or 0),
        )
        order.deferred_minimum_notional = max(
            0.0,
            float(minimum_notional or 0),
        )
        order.deferred_reference_price = max(
            0.0,
            float(reference_price or order.price or 0),
        )
        order.deferred_cycle = self.lifecycle.cycle
        order.deferred_minimum_known = bool(minimum_known)
        if order.purpose in {"open_hedge"} | VECTOR_REPAIR_PURPOSES:
            if (
                order.purpose in VECTOR_REPAIR_PURPOSES
                and self.state.repair_deferred_side
                not in {None, order.position_side}
            ):
                self._clear_repair_deferred()
            had_deferred = self.state.repair_deferred_quantity > 1e-10
            self.state.repair_deferred_side = order.position_side
            self.state.repair_deferred_quantity = max(
                self.state.repair_deferred_quantity,
                remaining,
                abs(self.state.long_quantity - self.state.short_quantity),
            )
            self.state.repair_deferred_minimum_quantity = max(
                self.state.repair_deferred_minimum_quantity,
                remaining + 1e-12,
                float(minimum_quantity or 0),
            )
            self.state.repair_deferred_minimum_notional = max(
                self.state.repair_deferred_minimum_notional,
                float(minimum_notional or 0),
            )
            self.state.repair_deferred_reference_price = max(
                self.state.repair_deferred_reference_price,
                float(reference_price or order.price or 0),
            )
            self.state.repair_deferred_cycle = self.lifecycle.cycle
            self.state.repair_deferred_minimum_known = (
                (
                    self.state.repair_deferred_minimum_known
                    and bool(minimum_known)
                )
                if had_deferred
                else bool(minimum_known)
            )
        self.state.last_action = (
            "repair_dust_deferred"
            if order.purpose.startswith("hedge_repair")
            else "opening_dust_merge_next_pair"
        )
        deferred_quantity = max(
            0.0,
            float(order.deferred_minimum_quantity),
        )
        self.persist()
        return [
            {
                "type": "exchange_order_dust_deferred",
                "action": self.state.last_action,
                "order_id": order.order_id,
                "quantity": remaining,
                "minimum_quantity": (
                    self.state.repair_deferred_minimum_quantity
                    if order.purpose in {"open_hedge"} | VECTOR_REPAIR_PURPOSES
                    else deferred_quantity
                ),
                "minimum_notional": (
                    self.state.repair_deferred_minimum_notional
                    if order.purpose in {"open_hedge"} | VECTOR_REPAIR_PURPOSES
                    else float(order.deferred_minimum_notional)
                ),
                "minimum_known": (
                    self.state.repair_deferred_minimum_known
                    if order.purpose in {"open_hedge"} | VECTOR_REPAIR_PURPOSES
                    else order.deferred_minimum_known
                ),
                "error": order.external_error,
                "simulated": False,
            }
        ]

    def record_harvest_minimum_deferred(
        self,
        *,
        order_id: str,
        error: object,
        minimum_quantity: float,
        minimum_notional: float = 0.0,
        reference_price: float,
        minimum_known: bool = False,
    ) -> List[dict]:
        order = self.lifecycle.get_order(order_id)
        if order is None or order.purpose not in HARVEST_ORDER_PURPOSES:
            return []
        remaining = max(
            0.0,
            float(order.quantity)
            - float(order.cumulative_filled_quantity),
        )
        if remaining <= 1e-12:
            return []
        minimum = max(
            remaining + 1e-12,
            float(minimum_quantity or 0),
        )
        order.external_status = "DEFERRED_MINIMUM"
        order.external_error = str(
            error or "order_below_exchange_minimum"
        )
        order.status = "EXPIRED"
        order.deferred_minimum_quantity = minimum
        order.deferred_minimum_notional = max(
            0.0,
            float(minimum_notional or 0),
        )
        order.deferred_reference_price = max(
            0.0,
            float(reference_price or order.price or 0),
        )
        order.deferred_cycle = self.lifecycle.cycle
        order.deferred_minimum_known = bool(minimum_known)
        if order.purpose == "take_profit":
            if self.state.pending_restore_quantity > 1e-10:
                # Finish the current harvest loop at the quantity already
                # closed. The unfilled remainder stays deferred for a later
                # healthy window after this quantity has been restored.
                self.state.pending_harvest_quantity = (
                    self.state.pending_restore_quantity
                )
            else:
                self.state.pending_harvest_quantity = max(
                    self.state.pending_harvest_quantity,
                    remaining,
                )
            self.state.pending_rebalance_side = order.position_side
        carry_forwarded = (
            self._remember_deferred_restore_carry(
                order=order,
                remaining=remaining,
            )
            if order.purpose == "restore"
            else False
        )
        self.state.last_action = (
            "take_profit_minimum_deferred"
            if order.purpose == "take_profit"
            else "restore_minimum_deferred"
        )
        self.persist()
        return [
            {
                "type": (
                    "exchange_harvest_restore_dust_deferred"
                    if order.purpose == "restore"
                    else "exchange_harvest_minimum_deferred"
                ),
                "action": order.purpose,
                "order_id": order.order_id,
                "quantity": remaining,
                "minimum_quantity": minimum,
                "minimum_notional": order.deferred_minimum_notional,
                "minimum_known": order.deferred_minimum_known,
                "reference_price": order.deferred_reference_price,
                "carry_forwarded": carry_forwarded,
                "error": order.external_error,
                "simulated": False,
            }
        ]

    def defer_repair_retry(self, reason: object) -> None:
        cooldown = max(
            1,
            int(
                getattr(
                    self.config,
                    "REPAIR_RETRY_COOLDOWN_CYCLES",
                    3,
                )
            ),
        )
        self.state.repair_retry_after_cycle = self.lifecycle.cycle + cooldown
        self.state.phase = StrategyPhase.REPAIRING_HEDGE
        self.state.last_action = "hedge_repair_retry_deferred"
        self.state.last_error = str(reason or "hedge_repair_retry_deferred")
        self.persist()

    def record_opening_pair_failure(self, reason: object) -> dict:
        self.state.opening_pair_failure_count += 1
        self.state.opening_retry_after_cycle = (
            self.lifecycle.cycle
            + max(
                1,
                int(
                    getattr(
                        self.config,
                        "OPENING_RETRY_COOLDOWN_CYCLES",
                        12,
                    )
                ),
            )
        )
        maximum = max(
            1,
            int(
                getattr(
                    self.config,
                    "OPENING_MAX_PAIR_FAILURES",
                    3,
                )
            ),
        )
        has_position = (
            self.state.long_quantity > 1e-10
            or self.state.short_quantity > 1e-10
        )
        if self.state.opening_pair_failure_count >= maximum and has_position:
            self.state.phase = StrategyPhase.REPAIRING_HEDGE
            self.state.repair_retry_after_cycle = (
                self.lifecycle.cycle
                + max(
                    1,
                    int(
                        getattr(
                            self.config,
                            "REPAIR_RETRY_COOLDOWN_CYCLES",
                            3,
                        )
                    ),
                )
            )
            self.state.last_action = "repair_after_pair_failures"
        elif self.state.opening_pair_failure_count >= maximum:
            self.state.phase = StrategyPhase.SELECTING_SYMBOL
            self.state.last_action = "opening_pair_failures_retrying"
        else:
            self.state.last_action = "opening_pair_failure_cooldown"
        self.persist()
        return {
            "type": "exchange_opening_pair_failure",
            "action": self.state.last_action,
            "reason": str(reason or "opening_pair_failed"),
            "failure_count": self.state.opening_pair_failure_count,
            "retry_after_cycle": self.state.opening_retry_after_cycle,
            "simulated": False,
        }

    def plan_pending_risk_recovery(self, mark_price: float) -> List[dict]:
        events: List[dict] = []
        mark = max(
            0.0,
            float(mark_price or self.state.last_mark_price or 0),
        )
        if self.last_risk_decision.hard_risk:
            self.state.last_action = "hard_risk_reduction_pending"
            self.persist()
            return events
        if self.state.phase in {
            StrategyPhase.TAKING_PROFIT,
            StrategyPhase.PROFIT_SWEEPING,
            StrategyPhase.REBALANCING,
        }:
            self._plan_pending_harvest_orders(mark, events)
        elif not self.lifecycle.active_orders and not self._positions_are_balanced():
            self._quote_vector_repair(mark, events)
        self.persist()
        return events

    def force_runtime_risk_freeze(
        self,
        *,
        reason: object,
    ) -> List[dict]:
        self.last_risk_decision = RiskDecision(
            reason=str(reason or "runtime_account_guard"),
            block_expansion=True,
            block_harvest=True,
            block_subscription=True,
            hard_risk=True,
        )
        events: List[dict] = []
        self._enter_hard_risk_freeze(events, emit_transition=True)
        self.persist()
        return events

    def _clear_recovered_subscription_block(self) -> None:
        """Clear only a stale risk freeze after funding is fully idle."""
        if (
            self.profit_bucket.sweep_block_reason
            != "risk_subscription_frozen"
            or self.last_risk_decision.block_subscription
            or self.exit_state.reopen_locked
        ):
            return
        bucket = self.profit_bucket
        if any(
            (
                bucket.pending_subscription_stage,
                bucket.pending_subscription_asset,
                bucket.pending_subscription_kind,
                bucket.pending_source_asset,
                bucket.pending_conversion_quote_id,
                bucket.pending_conversion_order_id,
                bucket.pending_transfer_id,
                bucket.pending_capital_reference,
                bucket.pending_collateral_transfer_id,
            )
        ) or any(
            amount > 1e-10
            for amount in (
                bucket.pending_subscription_amount,
                bucket.pending_source_amount,
                bucket.pending_source_transfer_amount,
                bucket.pending_harvest_amount,
                bucket.pending_rwusd_amount,
                bucket.pending_collateral_balance_before,
            )
        ) or any(
            value not in (None, 0)
            for value in (
                bucket.pending_conversion_quote_expires_at_ms,
                bucket.pending_subscription_started_at_ms,
                bucket.pending_capital_contribution_amount,
            )
        ):
            return
        bucket.sweep_block_reason = None

    def sweep_shadow_rwusd(
        self,
        portfolio: PortfolioSnapshot,
    ) -> List[dict]:
        events: List[dict] = []
        self._simulate_profit_sweep(portfolio, events)
        self.persist()
        return events

    def live_subscription_amount(
        self,
        portfolio: PortfolioSnapshot,
    ) -> float:
        if not self.live_rwusd_execution:
            return 0.0
        if self.last_risk_decision.block_subscription:
            self.profit_bucket.sweep_block_reason = "risk_subscription_frozen"
            self.persist()
            return 0.0
        if self.state.phase in {
            StrategyPhase.OPENING_HEDGE,
            StrategyPhase.REPAIRING_HEDGE,
            StrategyPhase.FLATTENING,
            StrategyPhase.PAUSED,
        }:
            return 0.0
        if not self._harvest_subscription_phase_ready():
            return 0.0
        if not self.profit_bucket.closed_loop_ready:
            return 0.0
        if self.profit_bucket.pending_subscription_stage:
            return 0.0
        if self.harvest_transfer_retry_active():
            return 0.0
        if self.has_active_harvest_orders():
            self.profit_bucket.sweep_block_reason = (
                "harvest_waiting_for_position_action"
            )
            self.persist()
            return 0.0
        transferable = max(0.0, self.profit_bucket.harvest_buffer)
        minimum = float(getattr(self.config, "MIN_SWEEP", 1.0))
        if transferable + 1e-12 < minimum:
            self.profit_bucket.sweep_block_reason = "below_min_sweep"
            self.persist()
            return 0.0
        return max(0.0, transferable)

    def harvest_transfer_retry_active(self) -> bool:
        return int(self.profit_bucket.transfer_retry_after_ms or 0) > int(
            time.time() * 1000
        )

    def has_active_harvest_orders(self) -> bool:
        return any(
            order.purpose in HARVEST_ORDER_PURPOSES
            for order in self.lifecycle.active_orders
        )

    def defer_harvest_transfer(self, reason: object) -> None:
        schedule = tuple(
            getattr(
                self.config,
                "LIVE_HARVEST_TRANSFER_RETRY_BACKOFF_SEC",
                (1.0, 3.0, 10.0, 30.0),
            )
        )
        index = self.profit_bucket.transfer_retry_count
        delay_sec = max(1.0, float(schedule[min(index, len(schedule) - 1)]))
        self.profit_bucket.transfer_retry_count = min(index + 1, len(schedule))
        self.profit_bucket.transfer_retry_after_ms = int(
            (time.time() + delay_sec) * 1000
        )
        self.profit_bucket.transfer_retry_reason = str(
            reason or "transfer_retry_after_rejection"
        )
        self.profit_bucket.sweep_block_reason = (
            self.profit_bucket.transfer_retry_reason
        )
        self.persist()

    def clear_harvest_transfer_retry(self) -> None:
        self.profit_bucket.transfer_retry_after_ms = 0
        self.profit_bucket.transfer_retry_reason = None
        self.profit_bucket.transfer_retry_count = 0

    def _harvest_subscription_phase_ready(self) -> bool:
        if self.state.phase == StrategyPhase.HEDGED:
            return True
        if self.state.phase == StrategyPhase.PROFIT_SWEEPING:
            return self.state.pending_rebalance_side is not None
        if self.state.phase == StrategyPhase.REBALANCING:
            return self.state.pending_rebalance_side is not None
        if self.state.phase != StrategyPhase.TAKING_PROFIT:
            return False
        return (
            self.state.pending_rebalance_side is not None
            and self.state.pending_restore_quantity > 1e-10
        )

    def _profit_cash_reached_spot(self) -> bool:
        if (
            str(
                self.profit_bucket.pending_subscription_kind or ""
            ).strip().lower()
            != "harvest"
        ):
            return False
        return str(
            self.profit_bucket.pending_subscription_stage or ""
        ).upper() in PROFIT_CASH_SPOT_STAGES

    def _normalize_rwusd_source_asset(self, asset: object) -> str:
        configured = getattr(
            self.config,
            "RWUSD_SOURCE_ASSETS",
            ("USDC", "USDT"),
        )
        if isinstance(configured, str):
            configured = (configured,)
        allowed = {
            str(item or "").strip().upper()
            for item in configured or ()
            if str(item or "").strip().upper() in {"USDC", "USDT"}
        }
        normalized = str(asset or "").strip().upper()
        if not allowed or normalized not in allowed:
            raise RuntimeError(
                f"unsupported RWUSD subscription source asset={asset!r}"
            )
        return normalized

    def begin_live_subscription(
        self,
        *,
        amount: float,
        asset: str,
        started_at_ms: int,
        kind: str = "harvest",
        harvest_amount: float = 0.0,
        source_asset: Optional[str] = None,
        source_amount: Optional[float] = None,
        source_transfer_amount: Optional[float] = None,
        source_transfer_type: str = "portfolio_margin",
        source_collection_required: bool = False,
        capital_reference: Optional[str] = None,
    ) -> None:
        if not self.live_rwusd_execution:
            raise RuntimeError("live RWUSD subscription is disabled")
        if self.profit_bucket.pending_subscription_stage:
            raise RuntimeError("RWUSD subscription is already pending")
        value = max(0.0, float(amount or 0))
        if value <= 0:
            raise RuntimeError("RWUSD subscription amount must be positive")
        subscription_kind = str(kind or "").strip().lower()
        if subscription_kind not in {"initial", "harvest", "capital"}:
            raise RuntimeError(
                f"unsupported RWUSD subscription kind={kind!r}"
            )
        normalized_asset = self._normalize_rwusd_source_asset(asset)
        normalized_source_asset = self._normalize_rwusd_source_asset(
            source_asset or normalized_asset
        )
        source_value = max(
            0.0,
            float(value if source_amount is None else source_amount),
        )
        if source_value <= 0:
            raise RuntimeError("RWUSD source amount must be positive")
        transfer_amount = (
            source_value
            if source_transfer_amount is None
            else min(
                source_value,
                max(0.0, float(source_transfer_amount or 0)),
            )
        )
        self.profit_bucket.pending_subscription_amount = value
        self.profit_bucket.pending_subscription_asset = normalized_asset
        self.profit_bucket.pending_subscription_kind = subscription_kind
        self.profit_bucket.pending_source_asset = normalized_source_asset
        self.profit_bucket.pending_source_amount = source_value
        self.profit_bucket.pending_source_transfer_amount = transfer_amount
        self.profit_bucket.pending_conversion_quote_id = None
        self.profit_bucket.pending_conversion_order_id = None
        self.profit_bucket.pending_conversion_quote_expires_at_ms = 0
        transfer_type = str(source_transfer_type or "").strip().lower()
        if transfer_amount > 0 and transfer_type not in {
            "portfolio_margin",
            "funding",
        }:
            raise RuntimeError(
                f"unsupported RWUSD source transfer type={source_transfer_type!r}"
            )
        self.profit_bucket.pending_harvest_amount = min(
            source_value,
            max(0.0, float(harvest_amount or 0)),
        )
        self.profit_bucket.pending_subscription_stage = (
            "SOURCE_COLLECTION_PENDING"
            if transfer_amount > 0 and source_collection_required
            else (
                (
                    "FUNDING_TRANSFER_PENDING"
                    if transfer_type == "funding"
                    else "TRANSFER_PENDING"
                )
                if transfer_amount > 0 else "TRANSFERRED"
            )
        )
        self.profit_bucket.pending_subscription_started_at_ms = max(
            0,
            int(started_at_ms or 0),
        )
        self.profit_bucket.pending_transfer_id = None
        self.profit_bucket.pending_rwusd_amount = 0.0
        self.profit_bucket.pending_capital_contribution_amount = None
        self.profit_bucket.pending_capital_reference = (
            str(capital_reference or "").strip() or None
        )
        self.profit_bucket.pending_collateral_balance_before = 0.0
        self.profit_bucket.pending_collateral_transfer_id = None
        self.profit_bucket.sweep_block_reason = (
            "source_collection_pending"
            if transfer_amount > 0 and source_collection_required
            else (
                "transfer_pending"
                if transfer_amount > 0
                else "rwusd_subscribe_pending"
            )
        )
        self.persist()

    def mark_live_conversion_stage(
        self,
        stage: str,
        *,
        quote_id: Optional[str] = None,
        order_id: Optional[str] = None,
        subscription_amount: Optional[float] = None,
        quote_expires_at_ms: Optional[int] = None,
        block_reason: Optional[str] = None,
    ) -> None:
        if not self.profit_bucket.pending_subscription_stage:
            raise RuntimeError("no pending RWUSD subscription")
        self.profit_bucket.pending_subscription_stage = str(stage or "").upper()
        if quote_id is not None:
            self.profit_bucket.pending_conversion_quote_id = (
                str(quote_id or "") or None
            )
        if order_id is not None:
            self.profit_bucket.pending_conversion_order_id = (
                str(order_id or "") or None
            )
        if subscription_amount is not None:
            amount = max(0.0, float(subscription_amount or 0))
            if amount <= 0:
                raise RuntimeError("converted RWUSD source amount is missing")
            self.profit_bucket.pending_subscription_amount = amount
        if quote_expires_at_ms is not None:
            self.profit_bucket.pending_conversion_quote_expires_at_ms = max(
                0,
                int(quote_expires_at_ms or 0),
            )
        self.profit_bucket.sweep_block_reason = block_reason
        self.persist()

    def reset_live_conversion(self, reason: object) -> None:
        if not self.profit_bucket.pending_subscription_stage:
            raise RuntimeError("no pending RWUSD subscription")
        self.profit_bucket.pending_subscription_amount = max(
            0.0,
            self.profit_bucket.pending_source_amount,
        )
        self.profit_bucket.pending_conversion_quote_id = None
        self.profit_bucket.pending_conversion_order_id = None
        self.profit_bucket.pending_conversion_quote_expires_at_ms = 0
        self.profit_bucket.pending_subscription_stage = "TRANSFERRED"
        self.profit_bucket.sweep_block_reason = str(
            reason or "stablecoin_conversion_retry"
        )
        self.persist()

    def mark_live_subscription_stage(
        self,
        stage: str,
        *,
        transfer_id: Optional[str] = None,
        rwusd_amount: Optional[float] = None,
        collateral_balance_before: Optional[float] = None,
        collateral_transfer_id: Optional[str] = None,
        block_reason: Optional[str] = None,
    ) -> None:
        if not self.profit_bucket.pending_subscription_stage:
            raise RuntimeError("no pending RWUSD subscription")
        self.profit_bucket.pending_subscription_stage = str(stage or "").upper()
        if transfer_id:
            self.profit_bucket.pending_transfer_id = str(transfer_id)
        if rwusd_amount is not None:
            self.profit_bucket.pending_rwusd_amount = max(
                0.0,
                float(rwusd_amount or 0),
            )
        if collateral_balance_before is not None:
            self.profit_bucket.pending_collateral_balance_before = max(
                0.0,
                float(collateral_balance_before or 0),
            )
        if collateral_transfer_id:
            self.profit_bucket.pending_collateral_transfer_id = str(
                collateral_transfer_id
            )
        self.profit_bucket.sweep_block_reason = block_reason
        self.persist()

    def reset_pending_live_subscription(self, reason: object) -> None:
        self._clear_pending_live_subscription()
        self.profit_bucket.sweep_block_reason = str(
            reason or "live_subscription_failed"
        )
        self.persist()

    def _clear_pending_live_subscription(self) -> None:
        self.profit_bucket.pending_subscription_amount = 0.0
        self.profit_bucket.pending_subscription_asset = None
        self.profit_bucket.pending_subscription_kind = None
        self.profit_bucket.pending_source_asset = None
        self.profit_bucket.pending_source_amount = 0.0
        self.profit_bucket.pending_source_transfer_amount = 0.0
        self.profit_bucket.pending_conversion_quote_id = None
        self.profit_bucket.pending_conversion_order_id = None
        self.profit_bucket.pending_conversion_quote_expires_at_ms = 0
        self.profit_bucket.pending_harvest_amount = 0.0
        self.profit_bucket.pending_subscription_stage = None
        self.profit_bucket.pending_subscription_started_at_ms = 0
        self.profit_bucket.pending_transfer_id = None
        self.profit_bucket.pending_rwusd_amount = 0.0
        self.profit_bucket.pending_capital_contribution_amount = None
        self.profit_bucket.pending_capital_reference = None
        self.profit_bucket.pending_collateral_balance_before = 0.0
        self.profit_bucket.pending_collateral_transfer_id = None

    def confirm_live_subscription(
        self,
        *,
        rwusd_amount: float,
        collateralization_mode: str,
    ) -> dict:
        amount = max(
            0.0,
            self.profit_bucket.pending_subscription_amount,
        )
        asset = self._normalize_rwusd_source_asset(
            self.profit_bucket.pending_subscription_asset
        )
        source_asset = self._normalize_rwusd_source_asset(
            self.profit_bucket.pending_source_asset or asset
        )
        source_amount = max(
            0.0,
            self.profit_bucket.pending_source_amount or amount,
        )
        if amount <= 0:
            raise RuntimeError("pending RWUSD subscription amount is missing")
        received = max(
            0.0,
            float(
                rwusd_amount
                or self.profit_bucket.pending_rwusd_amount
                or 0
            ),
        )
        if received <= 0:
            raise RuntimeError("confirmed RWUSD subscription amount is missing")
        kind = str(
            self.profit_bucket.pending_subscription_kind or "harvest"
        ).strip().lower()
        harvest_amount = (
            min(
                source_amount,
                max(
                    0.0,
                    self.profit_bucket.pending_harvest_amount,
                ),
            )
            if kind == "harvest"
            else 0.0
        )
        if kind == "harvest" and harvest_amount <= 0:
            harvest_amount = source_amount
        capital_contribution_before = (
            self.profit_bucket.capital_contribution_total
        )
        capital_reference = (
            self.profit_bucket.pending_capital_reference
            if kind == "capital"
            else None
        )
        if kind == "capital" and capital_reference:
            self.profit_bucket.confirmed_capital_reference = (
                capital_reference
            )
            self.profit_bucket.confirmed_capital_contribution_before = (
                capital_contribution_before
            )
            self.profit_bucket.confirmed_capital_contribution_amount = (
                source_amount
            )
        self.profit_bucket.harvest_buffer = max(
            0.0,
            self.profit_bucket.harvest_buffer - harvest_amount,
        )
        self.profit_bucket.realized_pnl_available_for_deposit = max(
            0.0,
            (
                self.profit_bucket.realized_pnl_available_for_deposit
                - harvest_amount
            ),
        )
        self.profit_bucket.rwusd_principal += amount
        self.profit_bucket.rwusd_units += received
        if kind in {"initial", "capital"}:
            self.profit_bucket.capital_contribution_total += source_amount
        if kind == "initial":
            normalized_asset = str(source_asset or "").strip().upper()
            self.profit_bucket.initial_collateralization_source_amount += (
                source_amount
            )
            if normalized_asset:
                source_amounts = (
                    self.profit_bucket.initial_collateralization_source_amounts
                )
                source_amounts[normalized_asset] = (
                    source_amounts.get(normalized_asset, 0.0) + source_amount
                )
            self.profit_bucket.initial_collateralization_rwusd += received
        else:
            self.profit_bucket.deposit_count += 1
        self.profit_bucket.closed_loop_ready = (
            self.profit_bucket.harvest_buffer + 1e-12
            >= float(getattr(self.config, "MIN_SWEEP", 1.0))
        )
        self.profit_bucket.last_rebalance_action = {
            "initial": "initial_rwusd_subscribe",
            "capital": "capital_rwusd_subscribe",
        }.get(kind, "rwusd_subscribe")
        self.profit_bucket.sweep_block_reason = (
            "profit_remaining_after_subscription"
            if self.profit_bucket.closed_loop_ready
            else None
        )
        collateral_transfer_id = (
            self.profit_bucket.pending_collateral_transfer_id
        )
        normalized_collateralization_mode = str(
            collateralization_mode or ""
        ).strip().lower()
        if normalized_collateralization_mode not in {
            "automatic",
            "explicit_transfer",
        }:
            raise RuntimeError(
                "unsupported RWUSD collateralization mode"
            )
        self._clear_pending_live_subscription()
        self.state.last_action = "rwusd_collateralized"
        self.persist()
        return {
            "type": (
                "rwusd_transfer"
                if self.live_rwusd_execution
                else "simulated_transfer"
            ),
            "action": (
                "rwusd_auto_collateralized"
                if normalized_collateralization_mode == "automatic"
                else "rwusd_to_portfolio_margin"
            ),
            "asset": "RWUSD",
            "source_asset": source_asset,
            "source_amount": source_amount,
            "subscription_asset": asset,
            "amount": amount,
            "rwusd_amount": received,
            "subscription_kind": kind,
            "harvest_amount": harvest_amount,
            "capital_contribution_before": capital_contribution_before,
            "capital_reference": capital_reference,
            "collateral_transfer_id": collateral_transfer_id,
            "collateralization_mode": normalized_collateralization_mode,
            "collateral_transfer_performed": (
                normalized_collateralization_mode == "explicit_transfer"
            ),
            "collateralized": True,
            "simulated": False,
        }

    def complete_initial_collateralization(
        self,
        *,
        mode: str,
        rwusd_amount: float = 0.0,
    ) -> dict:
        normalized_mode = str(mode or "").strip()
        if not normalized_mode:
            raise RuntimeError(
                "initial RWUSD collateralization mode is required"
            )
        self.profit_bucket.initial_collateralization_completed = True
        self.profit_bucket.initial_collateralization_mode = normalized_mode
        if rwusd_amount > 0:
            self.profit_bucket.initial_collateralization_rwusd = max(
                self.profit_bucket.initial_collateralization_rwusd,
                float(rwusd_amount),
            )
            if (
                self.profit_bucket.capital_contribution_total <= 0
                and self.profit_bucket.initial_collateralization_source_amount
                <= 0
            ):
                uncollateralized_rwusd = max(
                    0.0,
                    self.profit_bucket.exchange_rwusd_amount
                    - float(rwusd_amount),
                )
                collateralized_interest = max(
                    0.0,
                    self.profit_bucket.exchange_rwusd_total_profit
                    - uncollateralized_rwusd,
                )
                adopted_principal = max(
                    0.0,
                    float(rwusd_amount)
                    - min(float(rwusd_amount), collateralized_interest),
                )
                self.profit_bucket.capital_contribution_total = (
                    adopted_principal
                )
                self.profit_bucket.rwusd_principal = max(
                    self.profit_bucket.rwusd_principal,
                    adopted_principal,
                )
        self.profit_bucket.sweep_block_reason = None
        self.profit_bucket.last_rebalance_action = (
            "initial_rwusd_collateralization_completed"
        )
        self.state.last_action = "initial_rwusd_collateral_ready"
        self.persist()
        return {
            "type": (
                "rwusd_transfer"
                if self.live_rwusd_execution
                else "simulated_transfer"
            ),
            "action": "initial_rwusd_collateral_ready",
            "asset": "RWUSD",
            "mode": normalized_mode,
            "amount": (
                self.profit_bucket.initial_collateralization_source_amount
            ),
            "source_amounts": dict(
                self.profit_bucket.initial_collateralization_source_amounts
            ),
            "rwusd_amount": (
                self.profit_bucket.initial_collateralization_rwusd
            ),
            "collateralized": True,
            "simulated": not self.live_rwusd_execution,
        }

    def complete_shadow_initial_collateralization(
        self,
        *,
        amount: float,
        mode: str,
        source_asset: str = "USDC",
    ) -> dict:
        value = max(0.0, float(amount or 0))
        if value <= 0:
            raise RuntimeError(
                "shadow initial RWUSD collateralization requires capital"
            )
        normalized_asset = self._normalize_rwusd_source_asset(source_asset)
        self.profit_bucket.rwusd_principal += value
        self.profit_bucket.rwusd_units += value
        self.profit_bucket.capital_contribution_total += value
        self.profit_bucket.initial_collateralization_source_amount += value
        source_amounts = (
            self.profit_bucket.initial_collateralization_source_amounts
        )
        source_amounts[normalized_asset] = (
            source_amounts.get(normalized_asset, 0.0) + value
        )
        self.profit_bucket.initial_collateralization_rwusd += value
        event = self.complete_initial_collateralization(
            mode=mode,
            rwusd_amount=self.profit_bucket.initial_collateralization_rwusd,
        )
        event.update(
            {
                "type": "simulated_transfer",
                "action": "initial_rwusd_shadow_collateralized",
                "source_asset": normalized_asset,
                "amount": value,
                "rwusd_amount": value,
                "simulated": True,
            }
        )
        return event

    def validate_official_capital_bootstrap_reset(self) -> None:
        if (
            self.state.long_quantity > 1e-12
            or self.state.short_quantity > 1e-12
            or self.lifecycle.active_orders
        ):
            raise RuntimeError(
                "RWUSD official capital migration requires a flat account"
            )
        if self.exit_state.active or self.exit_state.reopen_locked:
            raise RuntimeError(
                "RWUSD official capital migration is blocked during exit"
            )
        if self._confirmed_execution_history_established():
            raise RuntimeError(
                "RWUSD official capital migration cannot replace "
                "confirmed execution history"
            )

    def _confirmed_execution_history_established(self) -> bool:
        return bool(
            self.state.cycle_id > 0
            or self.lifecycle.orders
            or self.exchange_ownership_established()
            or self.profit_bucket.exchange_trade_count > 0
            or abs(self.profit_bucket.realized_pnl_total) > 1e-12
        )

    def validate_unfunded_primary_promotion(self) -> None:
        self.validate_official_capital_bootstrap_reset()
        bucket = self.profit_bucket
        if (
            bucket.capital_contribution_total > 1e-12
            or bucket.rwusd_principal > 1e-12
            or bucket.initial_collateralization_completed
            or bucket.pending_subscription_stage
        ):
            raise RuntimeError(
                "RWUSD primary promotion requires an unfunded engine"
            )

    def promote_unfunded_primary_symbol(self, symbol: str) -> None:
        target = str(symbol or "").strip().upper()
        if not target:
            raise ValueError("RWUSD primary promotion requires a symbol")
        self.validate_unfunded_primary_promotion()
        previous = self.state.selected_symbol
        self.state.selected_symbol = target
        self.state.last_action = "official_primary_symbol_promoted"
        try:
            self.persist()
        except Exception:
            self.state.selected_symbol = previous
            raise

    def reset_official_capital_bootstrap(self) -> None:
        self.validate_official_capital_bootstrap_reset()
        bucket = self.profit_bucket
        selected_symbol = self.state.selected_symbol
        last_mark_price = self.state.last_mark_price
        observed_equity = max(
            0.0,
            float(self.last_portfolio_snapshot.account_equity or 0),
        )
        self.state = HedgeState(
            phase=StrategyPhase.SELECTING_SYMBOL,
            selected_symbol=selected_symbol,
            last_mark_price=last_mark_price,
            peak_equity=observed_equity,
            last_action="official_capital_bootstrap_reset",
        )
        self.lifecycle = DryRunOrderLifecycle()
        self.profit_bucket = ProfitBucket(
            exchange_rwusd_amount=bucket.exchange_rwusd_amount,
            exchange_rwusd_total_profit=(
                bucket.exchange_rwusd_total_profit
            ),
            exchange_rwusd_annual_percentage_rate=(
                bucket.exchange_rwusd_annual_percentage_rate
            ),
            exchange_rwusd_rate_time_ms=(
                bucket.exchange_rwusd_rate_time_ms
            ),
            exchange_rwusd_collateral_amount=(
                bucket.exchange_rwusd_collateral_amount
            ),
            exchange_rwusd_collateral_equity=(
                bucket.exchange_rwusd_collateral_equity
            ),
            exchange_rwusd_transferable_amount=(
                bucket.exchange_rwusd_transferable_amount
            ),
            rwusd_interest_accrued=bucket.exchange_rwusd_total_profit,
            exchange_trade_sync_started_at_ms=int(time.time() * 1000),
        )
        self._pending_take_profit_gross = 0.0
        self._pending_take_profit_notional = 0.0
        self.persist()

    def reconcile_official_initial_principal(
        self,
        amount: float,
    ) -> dict:
        principal = float(amount or 0)
        if not math.isfinite(principal) or principal < 0:
            raise ValueError(
                "RWUSD detected initial principal must be nonnegative"
            )
        if not self.profit_bucket.initial_collateralization_completed:
            raise RuntimeError(
                "RWUSD initial principal requires completed collateralization"
            )
        if (
            self.profit_bucket.exchange_trade_count > 0
            or abs(self.profit_bucket.realized_pnl_total) > 1e-12
        ):
            raise RuntimeError(
                "RWUSD official initial principal cannot replace active history"
            )
        self.profit_bucket.capital_contribution_total = principal
        self.profit_bucket.rwusd_principal = principal
        self.profit_bucket.last_rebalance_action = (
            "official_initial_principal_reconciled"
        )
        self.state.last_action = "official_initial_principal_reconciled"
        self.persist()
        return {
            "type": "rwusd_capital_bootstrap",
            "action": "official_initial_principal_reconciled",
            "capital_contribution_amount": principal,
            "rwusd_principal": principal,
            "rwusd_interest": max(
                0.0,
                self.profit_bucket.exchange_rwusd_total_profit,
            ),
            "collateralized": True,
            "simulated": not self.live_rwusd_execution,
        }

    def adopt_official_capital_ledger(self, amount: float) -> dict:
        detected_capital = float(amount or 0)
        if not math.isfinite(detected_capital) or detected_capital < 0:
            raise ValueError(
                "RWUSD detected initial capital must be nonnegative"
            )
        if not self.profit_bucket.initial_collateralization_completed:
            raise RuntimeError(
                "RWUSD official capital adoption requires completed "
                "collateralization"
            )
        if not self._confirmed_execution_history_established():
            return self.reconcile_official_initial_principal(
                detected_capital
            )
        self.profit_bucket.last_rebalance_action = (
            "official_capital_ledger_adopted"
        )
        self.state.last_action = "official_capital_ledger_adopted"
        self.persist()
        return {
            "type": "rwusd_capital_bootstrap",
            "action": "official_capital_ledger_adopted",
            "detected_capital": detected_capital,
            "capital_contribution_amount": (
                self.profit_bucket.capital_contribution_total
            ),
            "rwusd_principal": self.profit_bucket.rwusd_principal,
            "historical_accounting_preserved": True,
            "collateralized": True,
            "simulated": not self.live_rwusd_execution,
        }

    def block_initial_collateralization(self, reason: object) -> None:
        self.profit_bucket.sweep_block_reason = str(
            reason or "initial_rwusd_collateralization_blocked"
        )
        self.state.last_action = "initial_rwusd_collateralization_waiting"
        self.persist()

    def begin_existing_rwusd_collateral_transfer(
        self,
        *,
        rwusd_amount: float,
        collateral_balance_before: float,
        started_at_ms: int,
        kind: str = "initial_existing_rwusd",
        capital_reference: Optional[str] = None,
    ) -> None:
        if self.profit_bucket.pending_subscription_stage:
            raise RuntimeError("RWUSD collateral transfer is already pending")
        amount = max(0.0, float(rwusd_amount or 0))
        if amount <= 0:
            raise RuntimeError("existing RWUSD collateral amount is missing")
        normalized_kind = str(kind or "").strip().lower()
        if normalized_kind not in {
            "initial_existing_rwusd",
            "capital_existing_rwusd",
        }:
            raise RuntimeError(
                f"unsupported existing RWUSD transfer kind={kind!r}"
            )
        self.profit_bucket.pending_subscription_kind = normalized_kind
        self.profit_bucket.pending_capital_reference = (
            str(capital_reference or "").strip() or None
        )
        self.profit_bucket.pending_subscription_stage = (
            "EXISTING_RWUSD_COLLATERAL_TRANSFER_PENDING"
        )
        self.profit_bucket.pending_subscription_started_at_ms = max(
            0,
            int(started_at_ms or 0),
        )
        self.profit_bucket.pending_rwusd_amount = amount
        collateralized_interest = max(
            0.0,
            self.profit_bucket.exchange_rwusd_collateral_amount
            - self.profit_bucket.rwusd_principal,
        )
        uncollateralized_interest = max(
            0.0,
            self.profit_bucket.exchange_rwusd_total_profit
            - collateralized_interest,
        )
        self.profit_bucket.pending_capital_contribution_amount = max(
            0.0,
            amount - min(amount, uncollateralized_interest),
        )
        self.profit_bucket.pending_collateral_balance_before = max(
            0.0,
            float(collateral_balance_before or 0),
        )
        self.profit_bucket.sweep_block_reason = (
            "existing_rwusd_collateral_transfer_pending"
        )
        self.persist()

    def confirm_existing_rwusd_collateral_transfer(self) -> dict:
        amount = max(0.0, self.profit_bucket.pending_rwusd_amount)
        if amount <= 0:
            raise RuntimeError("pending existing RWUSD amount is missing")
        transfer_id = self.profit_bucket.pending_collateral_transfer_id
        kind = str(
            self.profit_bucket.pending_subscription_kind or ""
        ).strip().lower()
        contribution_amount = (
            amount
            if self.profit_bucket.pending_capital_contribution_amount is None
            else self.profit_bucket.pending_capital_contribution_amount
        )
        capital_contribution_before = (
            self.profit_bucket.capital_contribution_total
        )
        capital_reference = (
            self.profit_bucket.pending_capital_reference
            if kind == "capital_existing_rwusd"
            else None
        )
        if kind == "capital_existing_rwusd" and capital_reference:
            self.profit_bucket.confirmed_capital_reference = (
                capital_reference
            )
            self.profit_bucket.confirmed_capital_contribution_before = (
                capital_contribution_before
            )
            self.profit_bucket.confirmed_capital_contribution_amount = (
                contribution_amount
            )
        self.profit_bucket.capital_contribution_total += contribution_amount
        self.profit_bucket.rwusd_principal += contribution_amount
        if kind == "initial_existing_rwusd":
            self.profit_bucket.initial_collateralization_existing_rwusd_amount += (
                amount
            )
            self.profit_bucket.initial_collateralization_rwusd += amount
        else:
            self.profit_bucket.deposit_count += 1
        self._clear_pending_live_subscription()
        self.profit_bucket.last_rebalance_action = (
            "existing_rwusd_collateralized"
        )
        self.profit_bucket.sweep_block_reason = None
        self.state.last_action = "existing_rwusd_collateralized"
        self.persist()
        return {
            "type": "rwusd_transfer",
            "action": "existing_rwusd_to_portfolio_margin",
            "asset": "RWUSD",
            "rwusd_amount": amount,
            "capital_contribution_amount": contribution_amount,
            "capital_contribution_before": capital_contribution_before,
            "subscription_kind": kind,
            "capital_reference": capital_reference,
            "collateral_transfer_id": transfer_id,
            "collateralized": True,
            "simulated": False,
        }

    def incremental_capital_reference(
        self,
        *,
        source_asset: str,
        amount: float,
        capital_contribution_before: float,
    ) -> str:
        return (
            "binance-incremental:"
            f"{self.account_name}:"
            f"{str(source_asset or '').strip().upper()}:"
            f"{float(amount or 0):.8f}:"
            f"{float(capital_contribution_before or 0):.8f}"
        )

    def acknowledge_confirmed_capital_reference(
        self,
        external_reference: str,
    ) -> None:
        reference = str(external_reference or "").strip()
        if (
            reference
            and self.profit_bucket.confirmed_capital_reference == reference
        ):
            self.profit_bucket.confirmed_capital_reference = None
            self.profit_bucket.confirmed_capital_contribution_before = 0.0
            self.profit_bucket.confirmed_capital_contribution_amount = 0.0
            self.persist()

    def record_direct_coin_capital(
        self,
        *,
        amount: float,
        source_asset: str,
        capital_contribution_before: Optional[float] = None,
    ) -> dict:
        value = max(0.0, float(amount or 0))
        asset = str(source_asset or "").strip().upper()
        if value <= 0 or not asset:
            raise RuntimeError("direct coin collateral capital is invalid")
        current = max(
            0.0,
            float(self.profit_bucket.capital_contribution_total or 0),
        )
        if capital_contribution_before is not None:
            before = max(0.0, float(capital_contribution_before or 0))
            expected = before + value
            if current > expected + 1e-8:
                raise RuntimeError(
                    "direct coin collateral capital contribution is ahead "
                    "of its persisted baseline"
                )
            if current >= expected - 1e-8:
                return {
                    "type": "coin_margined_capital",
                    "action": "incremental_coin_collateral_already_recorded",
                    "asset": asset,
                    "amount": value,
                    "capital_contribution_amount": 0.0,
                    "collateralized": True,
                    "simulated": False,
                }
        self.profit_bucket.capital_contribution_total += value
        self.profit_bucket.last_rebalance_action = (
            "coin_margined_incremental_collateral_confirmed"
        )
        self.state.last_action = (
            "coin_margined_incremental_collateral_confirmed"
        )
        self.persist()
        return {
            "type": "coin_margined_capital",
            "action": "incremental_coin_collateral_confirmed",
            "asset": asset,
            "amount": value,
            "capital_contribution_amount": value,
            "collateralized": True,
            "simulated": False,
        }

    def update_exchange_rwusd_snapshot(self, payload: object) -> None:
        data = payload if isinstance(payload, dict) else {}
        self.profit_bucket.exchange_rwusd_amount = max(
            0.0,
            float(data.get("rwusdAmount", 0) or 0),
        )
        total_profit = max(
            0.0,
            float(data.get("totalProfit", 0) or 0),
        )
        self.profit_bucket.exchange_rwusd_total_profit = total_profit
        if self.live_rwusd_execution:
            self.profit_bucket.rwusd_interest_accrued = total_profit
        self.persist()

    def update_exchange_rwusd_rate_snapshot(self, payload: object) -> None:
        data = payload if isinstance(payload, dict) else {}
        rows = [
            row
            for row in list(data.get("rows") or [])
            if isinstance(row, dict)
        ]
        if not rows:
            return
        latest = max(
            rows,
            key=lambda row: int(row.get("time", 0) or 0),
        )
        annual_rate = max(
            0.0,
            float(latest.get("annualPercentageRate", 0) or 0),
        )
        if annual_rate <= 0:
            return
        self.profit_bucket.exchange_rwusd_annual_percentage_rate = (
            annual_rate
        )
        self.profit_bucket.exchange_rwusd_rate_time_ms = max(
            0,
            int(latest.get("time", 0) or 0),
        )
        self.persist()

    def update_exchange_rwusd_collateral_snapshot(
        self,
        payload: object,
    ) -> None:
        data = payload if isinstance(payload, dict) else {}
        wallet_balance = max(
            0.0,
            float(data.get("wallet_balance", 0) or 0),
        )
        um_wallet_balance = max(
            0.0,
            float(data.get("um_wallet_balance", 0) or 0),
        )
        self.profit_bucket.exchange_rwusd_collateral_amount = (
            um_wallet_balance
        )
        self.profit_bucket.exchange_rwusd_collateral_equity = max(
            0.0,
            float(data.get("equity", um_wallet_balance) or um_wallet_balance),
        )
        self.persist()

    def update_exchange_rwusd_transferable_snapshot(
        self,
        payload: object,
    ) -> None:
        data = payload if isinstance(payload, dict) else {}
        self.profit_bucket.exchange_rwusd_transferable_amount = max(
            0.0,
            float(data.get("amount", 0) or 0),
        )
        self.persist()

    def block_live_subscription(self, reason: object) -> None:
        self.profit_bucket.sweep_block_reason = str(
            reason or "live_subscription_blocked"
        )
        self.persist()

    def _match_dry_run_orders(
        self,
        mark_price: float,
        events: List[dict],
        purposes: Optional[set[str]] = None,
    ) -> None:
        if mark_price <= 0 or not self.lifecycle.active_orders:
            return
        fills = self.lifecycle.match(
            mark_price=mark_price,
            fill_fraction=float(
                getattr(self.config, "DRY_RUN_FILL_FRACTION", 1.0)
            ),
            min_fill_quantity=float(
                getattr(self.config, "DRY_RUN_MIN_FILL_QUANTITY", 0.0)
            ),
            purposes=purposes,
        )
        for fill in fills:
            self._apply_fill(fill)
            events.append(fill.to_dict())
        self._sync_phase_after_fills(events)

    def assign_external_order(
        self,
        order_id: str,
        client_order_id: str,
        price: Optional[float] = None,
    ) -> bool:
        order = self.lifecycle.get_order(order_id)
        if order is None:
            return False
        normalized_client_id = str(client_order_id or "").strip()
        if not normalized_client_id:
            return False
        if (
            order.external_client_order_id
            and order.external_client_order_id != normalized_client_id
        ):
            raise RuntimeError(
                f"RWUSD external client order mismatch for {order.order_id}"
            )
        order.external_client_order_id = normalized_client_id
        self._exchange_ownership_established = True
        order.external_attempt_count += 1
        order.external_not_found_count = 0
        order.external_error = None
        order.external_submitted_cycle = self.lifecycle.cycle
        order.external_submitted_at_ms = int(time.time() * 1000)
        if price is not None and float(price or 0) > 0:
            order.price = float(price)
        self.persist()
        return True

    def set_external_order_price(
        self,
        order_id: str,
        price: float,
    ) -> None:
        order = self.lifecycle.get_order(order_id)
        value = max(0.0, float(price or 0))
        if order is None or value <= 0:
            return
        order.price = value
        self.persist()

    def set_external_order_quantity(
        self,
        order_id: str,
        quantity: float,
    ) -> None:
        order = self.lifecycle.get_order(order_id)
        normalized = max(0.0, float(quantity or 0))
        if order is None or normalized <= 0:
            return
        previous_quantity = order.quantity
        order.quantity = max(
            order.cumulative_filled_quantity,
            normalized,
        )
        if order.purpose == "take_profit":
            self.state.pending_harvest_quantity = max(
                self.state.pending_restore_quantity,
                self.state.pending_harvest_quantity
                + order.quantity
                - previous_quantity,
            )
        self.persist()

    def apply_external_order_update(
        self,
        *,
        order_id: str,
        external_order_id: Optional[str],
        external_status: str,
        cumulative_filled_quantity: float,
        average_fill_price: float,
    ) -> List[dict]:
        order = self.lifecycle.get_order(order_id)
        if order is None:
            return []
        status = str(external_status or "").strip().upper() or "NEW"
        cumulative = min(
            order.quantity,
            max(0.0, float(cumulative_filled_quantity or 0)),
        )
        previous = max(0.0, order.cumulative_filled_quantity)
        delta = max(0.0, cumulative - previous)
        self.lifecycle.bind_external_order_id(
            order,
            str(external_order_id or "") or order.external_order_id,
        )
        order.external_status = status
        order.external_error = None
        order.external_not_found_count = 0
        if status in EXTERNAL_ACTIVE_ORDER_STATUSES | {"FILLED"}:
            self.lifecycle.external_consecutive_rejection_count = 0
        if status in EXTERNAL_ACTIVE_ORDER_STATUSES:
            order.status = status
        elif status in EXTERNAL_TERMINAL_ORDER_STATUSES:
            order.status = status
        if status == "FILLED" and cumulative <= 0:
            order.status = "REJECTED"
            order.external_error = "filled_without_executed_quantity"
        order.cumulative_filled_quantity = cumulative

        events: List[dict] = []
        if delta > 1e-12:
            self.lifecycle.external_consecutive_maker_requote_count = 0
            fill = FillEvent(
                order_id=order.order_id,
                position_side=order.position_side,
                side=order.side,
                fill_quantity=delta,
                fill_price=max(
                    0.0,
                    float(average_fill_price or order.price or 0),
                ),
                reduce_only=order.reduce_only,
                purpose=order.purpose,
                status=order.status,
            )
            self._apply_fill(fill)
            fill_event = fill.to_dict()
            fill_event.update(
                {
                    "type": "exchange_fill",
                    "simulated": False,
                    "external_order_id": order.external_order_id,
                    "client_order_id": order.external_client_order_id,
                }
            )
            events.append(fill_event)
        if status == "EXPIRED_IN_MATCH":
            events.append(
                {
                    "type": "exchange_order_self_trade_expired",
                    "action": order.purpose,
                    "order_id": order.order_id,
                    "external_order_id": order.external_order_id,
                    "client_order_id": order.external_client_order_id,
                    "cumulative_filled_quantity": cumulative,
                    "simulated": False,
                }
            )
        self._sync_phase_after_fills(events)
        self.persist()
        return events

    def record_external_order_not_found(
        self,
        order_id: str,
        error: object,
    ) -> List[dict]:
        order = self.lifecycle.get_order(order_id)
        if order is None:
            return []
        order.external_not_found_count += 1
        confirmations = max(
            1,
            int(
                getattr(
                    self.config,
                    "EXTERNAL_ORDER_NOT_FOUND_CONFIRMATIONS",
                    3,
                )
            ),
        )
        if order.external_not_found_count < confirmations:
            return self.record_external_order_error(
                order_id,
                error,
                definitive_rejection=False,
            )
        return self.record_external_order_error(
            order_id,
            error,
            definitive_rejection=True,
        )

    def record_external_order_error(
        self,
        order_id: str,
        error: object,
        definitive_rejection: bool = False,
    ) -> List[dict]:
        order = self.lifecycle.get_order(order_id)
        if order is None:
            return []
        message = str(error or "unknown external execution error")
        order.external_error = message
        if not definitive_rejection:
            self.persist()
            return [
                {
                    "type": "exchange_order_pending_confirmation",
                    "action": order.purpose,
                    "order_id": order.order_id,
                    "client_order_id": order.external_client_order_id,
                    "error": message,
                    "simulated": False,
                }
            ]

        order.external_status = "REJECTED"
        order.status = "REJECTED"
        self.lifecycle.external_consecutive_rejection_count += 1
        self.lifecycle.timeout_requote_count += 1
        maximum = int(
            getattr(self.config, "EXTERNAL_MAX_SUBMISSION_ATTEMPTS", 3)
        )
        rejection_count = self.lifecycle.external_consecutive_rejection_count
        build_or_repair = order.purpose in {
            "open_hedge",
            "top_up_hedge",
            "hedge_repair",
        }
        if rejection_count >= maximum:
            if build_or_repair and (
                self.state.long_quantity > 0
                or self.state.short_quantity > 0
            ):
                if self._positions_are_balanced():
                    self.state.phase = StrategyPhase.HEDGED
                    self.state.last_action = "expansion_retry_deferred"
                else:
                    self.state.phase = StrategyPhase.REPAIRING_HEDGE
                    self.state.repair_retry_after_cycle = (
                        self.lifecycle.cycle
                        + max(
                            1,
                            int(
                                getattr(
                                    self.config,
                                    "REPAIR_RETRY_COOLDOWN_CYCLES",
                                    3,
                                )
                            ),
                        )
                    )
                    self.state.last_action = (
                        "repair_after_execution_rejections"
                    )
            elif self.state.long_quantity > 0 or self.state.short_quantity > 0:
                self.state.phase = (
                    StrategyPhase.HEDGED
                    if self._positions_are_balanced()
                    else StrategyPhase.REPAIRING_HEDGE
                )
                self.state.last_action = "exchange_execution_retrying"
            else:
                self.state.phase = StrategyPhase.SELECTING_SYMBOL
                self.state.last_action = "exchange_execution_retrying"
        self.persist()
        return [
            {
                "type": "exchange_order_rejected",
                "action": order.purpose,
                "order_id": order.order_id,
                "client_order_id": order.external_client_order_id,
                "error": message,
                "attempt_count": order.external_attempt_count,
                "consecutive_rejection_count": rejection_count,
                "simulated": False,
            }
        ]

    def record_external_maker_requote(
        self,
        *,
        order_id: str,
        error: object,
        external_order_id: Optional[str] = None,
    ) -> List[dict]:
        order = self.lifecycle.get_order(order_id)
        if order is None:
            return []
        self.lifecycle.bind_external_order_id(
            order,
            str(external_order_id or "") or order.external_order_id,
        )
        order.external_status = "EXPIRED"
        order.external_error = str(error or "maker_requote")
        order.status = "EXPIRED"
        self.lifecycle.expired_order_count += 1
        self.lifecycle.timeout_requote_count += 1
        self.lifecycle.external_consecutive_maker_requote_count += 1
        maximum = max(
            1,
            int(getattr(self.config, "MAKER_MAX_REQUOTES", 12)),
        )
        if (
            self.lifecycle.external_consecutive_maker_requote_count > maximum
        ):
            if order.purpose in HARVEST_ORDER_PURPOSES:
                self.state.last_action = "harvest_maker_requote"
            elif order.purpose == "hedge_repair":
                self.state.phase = StrategyPhase.REPAIRING_HEDGE
                self.state.last_action = "hedge_repair_maker_requote"
            elif not self._positions_are_balanced():
                self.state.phase = StrategyPhase.REPAIRING_HEDGE
                self.state.last_action = "repair_after_maker_requotes"
            elif self.state.long_quantity > 0 or self.state.short_quantity > 0:
                self.state.phase = StrategyPhase.HEDGED
                self.state.last_action = "maker_requote_expansion_paused"
            else:
                self.state.phase = StrategyPhase.SELECTING_SYMBOL
                self.state.last_action = "maker_requote_retrying"
        self.persist()
        return [
            {
                "type": "exchange_maker_requote",
                "action": order.purpose,
                "order_id": order.order_id,
                "client_order_id": order.external_client_order_id,
                "error": order.external_error,
                "requote_count": self.lifecycle.timeout_requote_count,
                "consecutive_requote_count": (
                    self.lifecycle.external_consecutive_maker_requote_count
                ),
                "simulated": False,
            }
        ]

    def reconcile_external_positions(
        self,
        *,
        symbol: Optional[str],
        long_quantity: float,
        long_entry_price: float,
        short_quantity: float,
        short_entry_price: float,
    ) -> None:
        long_qty = max(0.0, float(long_quantity or 0))
        short_qty = max(0.0, float(short_quantity or 0))
        normalized_symbol = str(symbol or "").strip().upper() or None
        self.state.long_quantity = long_qty
        self.state.long_entry_price = (
            max(0.0, float(long_entry_price or 0)) if long_qty > 0 else 0.0
        )
        self.state.short_quantity = short_qty
        self.state.short_entry_price = (
            max(0.0, float(short_entry_price or 0)) if short_qty > 0 else 0.0
        )
        if normalized_symbol:
            self.state.selected_symbol = normalized_symbol
        self._recover_pending_harvest_side()
        quantities_equal = self._positions_are_balanced()
        if not quantities_equal:
            self._align_pending_restore_side_to_gap()
        self._hydrate_deferred_restore_carry()
        if long_qty > 0 and short_qty > 0 and quantities_equal:
            if (
                self.state.phase == StrategyPhase.REBALANCING
                and self.state.pending_rebalance_side is not None
                and not self.lifecycle.active_orders
            ):
                self._complete_restore()
            elif (
                self.state.phase
                in {
                    StrategyPhase.TAKING_PROFIT,
                    StrategyPhase.PROFIT_SWEEPING,
                }
                and self.state.pending_rebalance_side is not None
            ):
                pass
            elif self.state.phase == StrategyPhase.FLATTENING:
                pass
            elif not self.lifecycle.active_orders:
                self.state.phase = StrategyPhase.HEDGED
                self.state.pending_rebalance_side = None
                self.state.pending_harvest_quantity = 0.0
                self.state.pending_restore_quantity = 0.0
                self._clear_deferred_restore_carry()
                self.state.profit_harvest_rsi_confirmed = False
                self.state.pending_harvest_trade_quantity = 0.0
                self.state.profit_sweep_started_at_ms = 0
                self.lifecycle.clear_deferred_minimum(purpose="restore")
                self._clear_repair_state()
                self._settle_external_profit_if_balanced()
        elif long_qty > 0 or short_qty > 0:
            if self.state.phase == StrategyPhase.FLATTENING:
                pass
            elif (
                self.state.phase
                in {
                    StrategyPhase.TAKING_PROFIT,
                    StrategyPhase.PROFIT_SWEEPING,
                    StrategyPhase.REBALANCING,
                }
                and self.state.pending_rebalance_side is None
            ):
                self.state.pending_harvest_quantity = 0.0
                self.state.pending_restore_quantity = 0.0
                self._clear_deferred_restore_carry()
                self.state.profit_harvest_rsi_confirmed = False
                self.lifecycle.clear_deferred_minimum(
                    purposes=HARVEST_ORDER_PURPOSES
                )
                self.state.phase = StrategyPhase.REPAIRING_HEDGE
                self.state.last_action = "harvest_context_missing_repair"
                self._clear_repair_state()
            elif self.state.phase not in {
                StrategyPhase.TAKING_PROFIT,
                StrategyPhase.PROFIT_SWEEPING,
                StrategyPhase.REBALANCING,
            }:
                self.state.phase = StrategyPhase.REPAIRING_HEDGE
                self.state.last_action = "hedge_gap_detected"
        elif not self.lifecycle.active_orders:
            if self.state.phase in {
                StrategyPhase.REPAIRING_HEDGE,
            }:
                self.state.phase = StrategyPhase.SELECTING_SYMBOL
                self.state.last_action = "hedge_repair_flat_retrying"
            elif self.state.phase not in {
                StrategyPhase.IDLE,
                StrategyPhase.SELECTING_SYMBOL,
                StrategyPhase.PAUSED,
            }:
                self.state.phase = StrategyPhase.SELECTING_SYMBOL
                self.state.pending_rebalance_side = None
                self.state.pending_harvest_trade_quantity = 0.0
                self.state.profit_sweep_started_at_ms = 0
                self.state.pending_restore_quantity = 0.0
                self.state.profit_harvest_rsi_confirmed = False
                self.state.opening_imbalance_tolerance = 0.0
                self.lifecycle.clear_deferred_minimum(
                    purposes=HARVEST_ORDER_PURPOSES
                )
                self._clear_repair_state()
        self.persist()

    def plan_external_manual_flatten(self, mark_price: float) -> List[dict]:
        mark = max(0.0, float(mark_price or self.state.last_mark_price or 0))
        if mark <= 0 and (
            self.state.long_quantity > 0 or self.state.short_quantity > 0
        ):
            raise RuntimeError(
                "RWUSD exchange execution cannot flatten without a valid mark price"
            )
        self.lifecycle.cancel_active()
        self.state.phase = StrategyPhase.FLATTENING
        self.state.last_action = "manual_exchange_flatten"
        events: List[dict] = []
        created: List[str] = []
        if self.state.long_quantity > 1e-10:
            order = self.lifecycle.create_order(
                position_side=PositionSide.LONG,
                side="SELL",
                quantity=self.state.long_quantity,
                price=mark,
                reduce_only=True,
                purpose="manual_flatten",
            )
            if order is not None:
                created.append(order.order_id)
        if self.state.short_quantity > 1e-10:
            order = self.lifecycle.create_order(
                position_side=PositionSide.SHORT,
                side="BUY",
                quantity=self.state.short_quantity,
                price=mark,
                reduce_only=True,
                purpose="manual_flatten",
            )
            if order is not None:
                created.append(order.order_id)
        if created:
            events.append(
                {
                    "type": "exchange_order_plan",
                    "action": "manual_flatten",
                    "order_ids": created,
                    "simulated": False,
                }
            )
        self.persist()
        return events

    def finalize_external_manual_flatten(self) -> bool:
        if self.state.long_quantity > 1e-10 or self.state.short_quantity > 1e-10:
            return False
        peak_equity = self.state.peak_equity
        self.state = HedgeState(
            peak_equity=peak_equity,
            last_action="manual_exchange_flatten_completed",
        )
        self.lifecycle = DryRunOrderLifecycle()
        self._pending_take_profit_gross = 0.0
        self._pending_take_profit_notional = 0.0
        self.persist()
        return True

    def _update_drawdown(self, equity: float) -> None:
        value = max(0.0, float(equity or 0))
        if self.state.peak_equity <= 0 or value > self.state.peak_equity:
            self.state.peak_equity = value
            self.state.current_drawdown = 0.0
            return
        if self.state.peak_equity > 0:
            self.state.current_drawdown = max(
                0.0,
                (self.state.peak_equity - value) / self.state.peak_equity,
            )

    def _hold_current_selector_during_active_cycle(self) -> None:
        current = self.state.selected_symbol
        if not current:
            return
        active = self.state.phase in {
            StrategyPhase.OPENING_HEDGE,
            StrategyPhase.REPAIRING_HEDGE,
            StrategyPhase.TAKING_PROFIT,
            StrategyPhase.PROFIT_SWEEPING,
            StrategyPhase.HEDGED,
            StrategyPhase.REBALANCING,
            StrategyPhase.FLATTENING,
        }
        if active:
            self.selector_snapshot.selected_symbol = current
            self.selector_snapshot.cooldown_symbol = current

    def _row_for_selected(self, rows: List[MarketRow]) -> Optional[MarketRow]:
        symbol = self.state.selected_symbol
        if not symbol:
            return None
        return next((row for row in rows if str(row.get("symbol")) == symbol), None)

    def _selected_mark(self, rows: List[MarketRow]) -> float:
        row = self._row_for_selected(rows)
        if row is None:
            return 0.0
        return max(0.0, float(row.get("close", 0) or 0))

    def _update_mark_and_pnl(self, rows: List[MarketRow]) -> None:
        mark = self._selected_mark(rows)
        if mark <= 0:
            return
        self.state.last_mark_price = mark
        self.state.long_unrealized_pnl = (
            (mark - self.state.long_entry_price) * self.state.long_quantity
            if self.state.long_quantity > 0 and self.state.long_entry_price > 0
            else 0.0
        )
        self.state.short_unrealized_pnl = (
            (self.state.short_entry_price - mark) * self.state.short_quantity
            if self.state.short_quantity > 0 and self.state.short_entry_price > 0
            else 0.0
        )

    def _start_selected_hedge(
        self,
        rows: List[MarketRow],
        minute_of_day: int,
        events: List[dict],
    ) -> None:
        selected = self.selector_snapshot.selected_symbol
        if not selected:
            self.state.phase = StrategyPhase.SELECTING_SYMBOL
            self.state.last_action = "selector_hold"
            return
        row = next((item for item in rows if str(item.get("symbol")) == selected), None)
        mark = float((row or {}).get("close", 0) or 0)
        if mark <= 0:
            self.state.phase = StrategyPhase.SELECTING_SYMBOL
            self.state.last_action = "selected_price_missing"
            return
        if not self._opening_market_gate(row):
            self.state.phase = StrategyPhase.SELECTING_SYMBOL
            self.state.last_action = "opening_blocked_by_volatility"
            return
        previous = self.state.selected_symbol
        self.state.selected_symbol = selected
        raw_target_quantity = (
            self.initial_capital
            * float(
                getattr(
                    self.config,
                    "TOTAL_MARGIN_FRACTION",
                    0.80,
                )
            )
            * 0.5
            * self.leverage
            / mark
        )
        if self.external_execution:
            if self._external_target_quantity <= 0:
                self.state.phase = StrategyPhase.SELECTING_SYMBOL
                self.state.last_action = "external_target_not_ready"
                return
            self.state.target_quantity = self._external_target_quantity
        else:
            self.state.target_quantity = raw_target_quantity
        self.state.phase = StrategyPhase.OPENING_HEDGE
        self.state.last_action = "open_hedge"
        self.state.opening_imbalance_tolerance = 0.0
        if previous != selected:
            self.state.last_symbol_switch_minute = minute_of_day
        events.append(
            {
                "type": "state_transition",
                "action": "open_hedge",
                "symbol": selected,
                "target_quantity": self.state.target_quantity,
                "simulated": True,
            }
        )
        self._quote_opening_hedge(mark, events, market_row=row)

    def _opening_market_gate(
        self,
        market_row: Optional[MarketRow] = None,
    ) -> bool:
        row = market_row or self._last_market_row
        if not isinstance(row, dict):
            return False
        return thirty_minute_row_volatility_allowed(
            row,
            maximum_range=float(
                getattr(self.config, "OPENING_VOLATILITY_MAX_RANGE", 0.015)
            ),
            atr_multiplier=float(
                getattr(
                    self.config,
                    "OPENING_VOLATILITY_ATR_MULTIPLIER",
                    1.50,
                )
            ),
            atr_key="thirty_minute_atr4",
        )

    def _quote_opening_hedge(
        self,
        mark: float,
        events: List[dict],
        *,
        market_row: Optional[MarketRow] = None,
    ) -> None:
        if mark <= 0 or self.state.target_quantity <= 0:
            return
        if not self._opening_market_gate(market_row):
            self.state.last_action = "opening_blocked_by_volatility"
            return
        if self.lifecycle.cycle < self.state.opening_retry_after_cycle:
            self.state.last_action = "opening_retry_cooldown"
            return
        if (
            self._positions_are_balanced()
            and min(self.state.long_quantity, self.state.short_quantity)
            > 1e-10
        ):
            self._complete_opening_hedge(events)
            return
        if self.last_risk_decision.block_expansion:
            self.state.last_action = "opening_pair_blocked_by_risk"
            return
        created = []
        tranche = (
            self._external_opening_tranche_quantity
            if self.external_execution
            else (
                self.state.target_quantity
                * float(
                    getattr(
                        self.config,
                        "OPENING_TRANCHE_FRACTION",
                        0.05,
                    )
                )
            )
        )
        if self.external_execution and tranche <= 1e-10:
            reason = (
                self._external_opening_tranche_block_reason
                or "projected_margin"
            )
            self.state.last_action = f"opening_pair_blocked_by_{reason}"
            return
        pair_deferred = self.lifecycle.deferred_minimum_summary(
            purposes={"open_hedge"},
        )
        if (
            pair_deferred["count"] > 0
            and self.lifecycle.cycle - pair_deferred["latest_cycle"]
            < self._minimum_retry_cooldown_cycles()
        ):
            self.state.last_action = "opening_pair_minimum_cooldown"
            return
        deferred_quantity = max(
            float(pair_deferred["residual_quantity"]),
            (
                self.state.repair_deferred_quantity
                if self.state.repair_deferred_side is not None
                or self.state.repair_deferred_quantity > 0
                else 0.0
            ),
        )
        if (
            self.state.long_quantity <= 1e-10
            and self.state.short_quantity <= 1e-10
            and deferred_quantity > 0
        ):
            minimum_quantity = max(
                float(pair_deferred["minimum_quantity"]),
                self.state.repair_deferred_minimum_quantity,
                (
                    float(pair_deferred["minimum_notional"]) / mark
                    if pair_deferred["minimum_notional"] > 0
                    else 0.0
                ),
                (
                    self.state.repair_deferred_minimum_notional / mark
                    if self.state.repair_deferred_minimum_notional > 0
                    else 0.0
                ),
            )
            minimum_known = (
                bool(pair_deferred["minimum_known"])
                if pair_deferred["count"] > 0
                else self.state.repair_deferred_minimum_known
            )
            if not minimum_known:
                required_growth = max(
                    minimum_quantity,
                    deferred_quantity
                    * (
                        1.0
                        + self._unknown_minimum_retry_growth_fraction()
                    ),
                )
                capacity = min(
                    max(0.0, self.state.target_quantity - self.state.long_quantity),
                    max(0.0, self.state.target_quantity - self.state.short_quantity),
                )
                if capacity + 1e-10 < required_growth:
                    self.state.last_action = (
                        "opening_pair_waiting_for_minimum_growth"
                    )
                    return
                tranche = max(tranche, required_growth)
            elif self.state.target_quantity + 1e-10 < minimum_quantity:
                self.state.last_action = (
                    "opening_pair_blocked_by_exchange_minimum"
                )
                return
            tranche = max(tranche, deferred_quantity, minimum_quantity)
        quantity = min(
            tranche,
            max(0.0, self.state.target_quantity - self.state.long_quantity),
            max(0.0, self.state.target_quantity - self.state.short_quantity),
        )
        for position_side, side in (
            (PositionSide.LONG, "BUY"),
            (PositionSide.SHORT, "SELL"),
        ):
            order = self.lifecycle.create_order(
                position_side=position_side,
                side=side,
                quantity=quantity,
                price=mark,
                reduce_only=False,
                purpose="open_hedge",
            )
            if order is not None:
                created.append(order.order_id)
        if created and not self.external_execution:
            self._clear_repair_deferred()
            events.append(
                {
                    "type": "simulated_order_plan",
                    "action": "open_hedge",
                    "order_ids": created,
                    "simulated": True,
                }
            )

    def _complete_opening_hedge(self, events: List[dict]) -> None:
        self.lifecycle.external_consecutive_maker_requote_count = 0
        self.state.opening_pair_failure_count = 0
        self.state.opening_retry_after_cycle = 0
        self.state.phase = StrategyPhase.HEDGED
        self.state.last_action = "hedged"
        self._clear_repair_state()
        events.append(
            {
                "type": "state_transition",
                "action": "hedged",
                "symbol": self.state.selected_symbol,
                "simulated": True,
            }
        )

    def _positions_are_balanced(self) -> bool:
        return self.opening_imbalance_is_tolerated(
            self.state.long_quantity - self.state.short_quantity
        )

    def _clear_repair_state(self) -> None:
        self.state.pending_repair_side = None
        self.state.pending_repair_quantity = 0.0
        self.state.repair_retry_after_cycle = 0
        self._clear_repair_deferred()

    def _clear_repair_deferred(self) -> None:
        self.state.repair_deferred_side = None
        self.state.repair_deferred_quantity = 0.0
        self.state.repair_deferred_minimum_quantity = 0.0
        self.state.repair_deferred_minimum_notional = 0.0
        self.state.repair_deferred_reference_price = 0.0
        self.state.repair_deferred_cycle = 0
        self.state.repair_deferred_minimum_known = False

    def _clear_deferred_restore_carry(self) -> None:
        self.state.deferred_restore_carry_side = None
        self.state.deferred_restore_carry_quantity = 0.0

    def _restore_carry_matches_current_gap(self) -> bool:
        side = self.state.deferred_restore_carry_side
        quantity = self.state.deferred_restore_carry_quantity
        if (
            side is None
            or quantity <= 1e-10
            or self.state.pending_rebalance_side != side
            or self.state.phase != StrategyPhase.REBALANCING
        ):
            return False
        return math.isclose(
            self._restore_gap(side),
            quantity,
            rel_tol=1e-6,
            abs_tol=1e-8,
        )

    def _hydrate_deferred_restore_carry(self) -> None:
        if (
            self.state.deferred_restore_carry_quantity > 1e-10
            or self.state.deferred_restore_carry_side is not None
        ):
            if not self._restore_carry_matches_current_gap():
                self._clear_deferred_restore_carry()
            return
        side = self.state.pending_rebalance_side
        if self.state.phase != StrategyPhase.REBALANCING or side is None:
            return
        deferred = self.lifecycle.deferred_minimum_summary(
            purposes={"restore"},
            position_side=side,
        )
        if deferred["count"] <= 0 or not deferred["minimum_known"]:
            return
        residual = max(0.0, float(deferred["residual_quantity"]))
        reference_price = max(
            self.state.last_mark_price,
            float(deferred["reference_price"]),
        )
        minimum = max(
            float(deferred["minimum_quantity"]),
            (
                float(deferred["minimum_notional"]) / reference_price
                if deferred["minimum_notional"] > 0
                and reference_price > 0
                else 0.0
            ),
        )
        if (
            residual <= 1e-10
            or residual + 1e-10 >= minimum
            or not math.isclose(
                self._restore_gap(side),
                residual,
                rel_tol=1e-6,
                abs_tol=1e-8,
            )
            ):
            return
        self.state.deferred_restore_carry_side = side
        self.state.deferred_restore_carry_quantity = residual

    def _remember_deferred_restore_carry(
        self,
        *,
        order: object,
        remaining: float,
    ) -> bool:
        if (
            self.last_risk_decision.hard_risk
            or self.state.phase != StrategyPhase.REBALANCING
            or self.state.pending_rebalance_side != order.position_side
        ):
            return False
        reference_price = max(
            0.0,
            float(order.deferred_reference_price or order.price or 0),
        )
        minimum = max(
            float(order.deferred_minimum_quantity),
            (
                float(order.deferred_minimum_notional) / reference_price
                if order.deferred_minimum_notional > 0
                and reference_price > 0
                else 0.0
            ),
        )
        if (
            not order.deferred_minimum_known
            or remaining <= 1e-10
            or remaining + 1e-10 >= minimum
            or not math.isclose(
                self._restore_gap(order.position_side),
                remaining,
                rel_tol=1e-6,
                abs_tol=1e-8,
            )
        ):
            return False
        self.state.deferred_restore_carry_side = order.position_side
        self.state.deferred_restore_carry_quantity = remaining
        return True

    def _deferred_restore_carry_ready(self) -> bool:
        return (
            self._restore_carry_matches_current_gap()
            and not self.last_risk_decision.hard_risk
            and not any(
                order.purpose in HARVEST_ORDER_PURPOSES
                for order in self.lifecycle.active_orders
            )
        )

    def clear_repair_deferred(self) -> None:
        self._clear_repair_deferred()
        self.persist()

    def clear_hedge_build_deferred(self) -> None:
        self.lifecycle.clear_deferred_minimum(
            purposes=PAIR_BUILD_PURPOSES,
        )
        self._clear_repair_deferred()
        self.persist()

    def _minimum_retry_cooldown_cycles(self) -> int:
        return max(
            1,
            int(
                getattr(
                    self.config,
                    "EXCHANGE_MINIMUM_RETRY_COOLDOWN_CYCLES",
                    5,
                )
            ),
        )

    def _unknown_minimum_retry_growth_fraction(self) -> float:
        return max(
            0.0,
            float(
                getattr(
                    self.config,
                    "EXCHANGE_MINIMUM_UNKNOWN_RETRY_GROWTH_FRACTION",
                    0.25,
                )
            ),
        )

    def _pair_build_retry_quantity(
        self,
        *,
        purpose: str,
        side: PositionSide,
        quantity: float,
        mark: float,
        available: float,
    ) -> tuple[float, bool]:
        requested = max(0.0, float(quantity or 0))
        capacity = max(0.0, float(available or 0))
        deferred = self.lifecycle.deferred_minimum_summary(
            purposes={purpose},
            position_side=side,
        )
        if deferred["count"] <= 0:
            return min(requested, capacity), False
        if (
            self.lifecycle.cycle - deferred["latest_cycle"]
            < self._minimum_retry_cooldown_cycles()
        ):
            return 0.0, True
        if not deferred["minimum_known"]:
            required = float(deferred["residual_quantity"]) * (
                1.0 + self._unknown_minimum_retry_growth_fraction()
            )
        else:
            required = max(
                float(deferred["minimum_quantity"]),
                (
                    float(deferred["minimum_notional"]) / mark
                    if deferred["minimum_notional"] > 0 and mark > 0
                    else 0.0
                ),
            )
        if capacity + 1e-10 < required:
            return 0.0, True
        return min(capacity, max(requested, required)), False

    def _repair_minimum_blocks_retry(
        self,
        *,
        side: PositionSide,
        quantity: float,
        mark: float,
    ) -> bool:
        state_deferred = self.state.repair_deferred_minimum_quantity > 0
        pair_deferred = self.lifecycle.deferred_minimum_summary(
            purposes={"top_up_hedge", HEDGE_REPAIR_PAIR_PURPOSE},
            position_side=side,
        )
        if not state_deferred and pair_deferred["count"] <= 0:
            return False
        if state_deferred and self.state.repair_deferred_side != side:
            self._clear_repair_deferred()
            state_deferred = False
        deferred_cycle = (
            self.state.repair_deferred_cycle
            if state_deferred
            else int(pair_deferred["latest_cycle"])
        )
        cooldown = self._minimum_retry_cooldown_cycles()
        if (
            self.lifecycle.cycle - deferred_cycle
            < cooldown
        ):
            self.state.last_action = "hedge_repair_minimum_cooldown"
            return True
        requested = max(0.0, float(quantity or 0))
        if state_deferred:
            minimum_known = self.state.repair_deferred_minimum_known
            residual = self.state.repair_deferred_quantity
            required = self.state.repair_deferred_minimum_quantity
            minimum_notional = self.state.repair_deferred_minimum_notional
        else:
            minimum_known = bool(pair_deferred["minimum_known"])
            residual = float(pair_deferred["residual_quantity"])
            required = float(pair_deferred["minimum_quantity"])
            minimum_notional = float(pair_deferred["minimum_notional"])
        if not minimum_known:
            required_growth = residual * (
                1.0 + self._unknown_minimum_retry_growth_fraction()
            )
            if requested <= required_growth + 1e-10:
                self.state.last_action = "hedge_repair_minimum_unknown_wait"
                return True
            return False
        if minimum_notional > 0 and mark > 0:
            required = max(
                required,
                minimum_notional / mark,
            )
        if requested <= max(residual, required) + 1e-12:
            self.state.last_action = "hedge_repair_minimum_wait"
            return True
        return False

    def _quote_deferred_repair_pair(
        self,
        *,
        missing_side: PositionSide,
        gap: float,
        mark: float,
        events: List[dict],
    ) -> bool:
        if gap <= 1e-10 or mark <= 0:
            return False
        if not self.last_risk_decision.allows_hedge_repair_pair:
            return False
        deferred_rows = [
            self.lifecycle.deferred_minimum_summary(
                purposes={"top_up_hedge", HEDGE_REPAIR_PAIR_PURPOSE},
                position_side=side,
            )
            for side in (PositionSide.LONG, PositionSide.SHORT)
        ]
        latest_cycle = max(
            self.state.repair_deferred_cycle,
            *(int(item["latest_cycle"]) for item in deferred_rows),
        )
        if (
            self.lifecycle.cycle - latest_cycle
            < self._minimum_retry_cooldown_cycles()
        ):
            return False

        requirements = []
        if self.state.repair_deferred_minimum_quantity > 0:
            requirement = max(
                self.state.repair_deferred_minimum_quantity,
                (
                    self.state.repair_deferred_minimum_notional / mark
                    if self.state.repair_deferred_minimum_notional > 0
                    else 0.0
                ),
            )
            if not self.state.repair_deferred_minimum_known:
                requirement = max(
                    requirement,
                    self.state.repair_deferred_quantity
                    * (1.0 + self._unknown_minimum_retry_growth_fraction()),
                )
            requirements.append(requirement)
        for deferred in deferred_rows:
            if deferred["count"] <= 0:
                continue
            requirement = max(
                float(deferred["minimum_quantity"]),
                (
                    float(deferred["minimum_notional"]) / mark
                    if deferred["minimum_notional"] > 0
                    else 0.0
                ),
            )
            if not deferred["minimum_known"]:
                requirement = max(
                    requirement,
                    float(deferred["residual_quantity"])
                    * (1.0 + self._unknown_minimum_retry_growth_fraction()),
                )
            requirements.append(requirement)
        common_quantity = max(requirements, default=0.0)
        if common_quantity <= 1e-10:
            return False

        long_add = common_quantity
        short_add = common_quantity
        if missing_side == PositionSide.LONG:
            long_add += gap
        else:
            short_add += gap
        target_cap = max(0.0, self.state.target_quantity)
        risk_cap = self.risk_target_quantity_cap()
        if risk_cap is not None:
            target_cap = min(target_cap, risk_cap)
        if (
            target_cap <= 0
            or self.state.long_quantity + long_add > target_cap + 1e-10
            or self.state.short_quantity + short_add > target_cap + 1e-10
        ):
            return False

        created: List[str] = []
        for position_side, side, quantity in (
            (PositionSide.LONG, "BUY", long_add),
            (PositionSide.SHORT, "SELL", short_add),
        ):
            order = self.lifecycle.create_order(
                position_side=position_side,
                side=side,
                quantity=quantity,
                price=mark,
                reduce_only=False,
                purpose=HEDGE_REPAIR_PAIR_PURPOSE,
            )
            if order is not None:
                created.append(order.order_id)
        if len(created) != 2:
            return False
        self.state.phase = StrategyPhase.REPAIRING_HEDGE
        self.state.pending_repair_side = missing_side
        self.state.pending_repair_quantity = gap
        self.state.last_action = "hedge_repair_minimum_pair"
        events.append(
            {
                "type": "simulated_order_plan",
                "action": self.state.last_action,
                "missing_side": missing_side.value,
                "gap_quantity": gap,
                "common_quantity": common_quantity,
                "long_quantity": long_add,
                "short_quantity": short_add,
                "order_ids": created,
                "simulated": not self.external_execution,
            }
        )
        return True

    def _quote_vector_repair(
        self,
        mark: float,
        events: List[dict],
    ) -> None:
        if mark <= 0 or self.lifecycle.active_orders:
            return
        long_quantity = self.state.long_quantity
        short_quantity = self.state.short_quantity
        difference = long_quantity - short_quantity
        if self.opening_imbalance_is_tolerated(difference):
            if long_quantity > 1e-10 and short_quantity > 1e-10:
                self.state.phase = StrategyPhase.HEDGED
                self.state.last_action = "hedge_repair_completed"
                self._clear_repair_state()
                self._settle_external_profit_if_balanced()
            return
        if self.lifecycle.cycle < self.state.repair_retry_after_cycle:
            self.state.phase = StrategyPhase.REPAIRING_HEDGE
            self.state.last_action = "hedge_repair_retry_cooldown"
            return

        missing_side = (
            PositionSide.LONG if difference < 0 else PositionSide.SHORT
        )
        missing_quantity = (
            long_quantity
            if missing_side == PositionSide.LONG
            else short_quantity
        )
        add_quantity = round(abs(difference), 12)
        risk_cap = self.risk_target_quantity_cap()
        if risk_cap is not None:
            add_quantity = min(
                add_quantity,
                max(0.0, risk_cap - missing_quantity),
            )
            if add_quantity <= 1e-10:
                self.state.phase = StrategyPhase.REPAIRING_HEDGE
                self.state.last_action = (
                    "degraded_excess_reduction_pending"
                )
                return
        minimum_order_quantity = (
            self._external_minimum_order_quantity
            if self.external_execution
            else 0.0
        )
        if (
            minimum_order_quantity > 0
            and add_quantity < minimum_order_quantity - 1e-10
        ):
            deferred = self.lifecycle.deferred_minimum_summary(
                purposes={"top_up_hedge", HEDGE_REPAIR_PAIR_PURPOSE},
            )
            latest_cycle = max(
                self.state.repair_deferred_cycle,
                int(deferred["latest_cycle"]),
            )
            if (
                latest_cycle > 0
                and self.lifecycle.cycle - latest_cycle
                < self._minimum_retry_cooldown_cycles()
            ):
                self.state.phase = StrategyPhase.REPAIRING_HEDGE
                self.state.last_action = "hedge_repair_minimum_cooldown"
                return
            self._quote_symmetric_top_up(
                mark,
                self.last_portfolio_snapshot,
                events,
                market_row=self._last_market_row,
                allow_imbalance=True,
            )
            if self.lifecycle.active_orders:
                return
            self.state.phase = StrategyPhase.REPAIRING_HEDGE
            self.state.last_action = "hedge_repair_waiting_for_target_headroom"
            return
        if self._repair_minimum_blocks_retry(
            side=missing_side,
            quantity=add_quantity,
            mark=mark,
        ):
            self.state.phase = StrategyPhase.REPAIRING_HEDGE
            self._quote_deferred_repair_pair(
                missing_side=missing_side,
                gap=abs(difference),
                mark=mark,
                events=events,
            )
            return
        created: List[str] = []
        if add_quantity > 1e-10:
            order = self.lifecycle.create_order(
                position_side=missing_side,
                side="BUY" if missing_side == PositionSide.LONG else "SELL",
                quantity=add_quantity,
                price=mark,
                reduce_only=False,
                purpose="hedge_repair",
            )
            if order is not None:
                created.append(order.order_id)
        if not created:
            self.state.last_action = "hedge_repair_waiting_for_valid_order"
            return
        if not self.external_execution:
            self._clear_repair_deferred()
        self.state.phase = StrategyPhase.REPAIRING_HEDGE
        self.state.pending_repair_side = missing_side
        self.state.pending_repair_quantity = add_quantity
        self.state.last_action = "hedge_repair_missing_side"
        events.append(
            {
                "type": "simulated_order_plan",
                "action": self.state.last_action,
                "missing_side": missing_side.value,
                "add_quantity": add_quantity,
                "order_ids": created,
                "simulated": True,
            }
        )

    def _quote_symmetric_top_up(
        self,
        mark: float,
        portfolio: PortfolioSnapshot,
        events: List[dict],
        market_row: Optional[MarketRow] = None,
        *,
        allow_imbalance: bool = False,
    ) -> None:
        repair_pair = allow_imbalance and not self._positions_are_balanced()
        if (
            mark <= 0
            or (
                self.last_risk_decision.block_expansion
                and not (
                    repair_pair
                    and self.last_risk_decision.allows_hedge_repair_pair
                )
            )
            or self.state.target_quantity <= 0
            or (
                not allow_imbalance
                and not self._positions_are_balanced()
            )
        ):
            return
        if not self._opening_market_gate(market_row):
            self.state.last_action = "top_up_blocked_by_volatility"
            return
        balanced = min(self.state.long_quantity, self.state.short_quantity)
        leader = max(self.state.long_quantity, self.state.short_quantity)
        difference = self.state.long_quantity - self.state.short_quantity
        gap = round(abs(difference), 12)
        target_cap = max(0.0, self.state.target_quantity)
        risk_cap = self.risk_target_quantity_cap()
        if risk_cap is not None:
            target_cap = min(target_cap, risk_cap)
        remaining = max(0.0, target_cap - balanced)
        rebalance_band = (
            self.state.target_quantity
            * float(
                getattr(
                    self.config,
                    "TARGET_REBALANCE_BAND_FRACTION",
                    0.05,
                )
            )
        )
        if not repair_pair and remaining <= rebalance_band + 1e-10:
            return
        tranche = (
            self._external_opening_tranche_quantity
            if self.external_execution
            else (
                max(balanced, self.state.target_quantity)
                * float(
                    getattr(
                        self.config,
                        "OPENING_TRANCHE_FRACTION",
                        0.05,
                    )
                )
            )
        )
        if self.external_execution and tranche <= 1e-10:
            reason = (
                self._external_opening_tranche_block_reason
                or "projected_margin"
            )
            self.state.last_action = f"top_up_blocked_by_{reason}"
            return
        minimum_order_quantity = (
            round(self._external_minimum_order_quantity, 12)
            if self.external_execution
            else 0.0
        )
        build_cap = target_cap
        common_quantity = 0.0
        if repair_pair:
            if (
                minimum_order_quantity <= 0
                or gap >= minimum_order_quantity - 1e-10
            ):
                return
            planned_final = min(
                target_cap,
                max(leader, balanced + tranche),
            )
            common_quantity = round(
                max(0.0, planned_final - leader),
                12,
            )
            if common_quantity < minimum_order_quantity - 1e-10:
                common_quantity = minimum_order_quantity
                final_quantity = leader + common_quantity
                build_cap = max(target_cap, final_quantity)
            else:
                final_quantity = planned_final
        else:
            final_quantity = min(
                tranche,
                max(0.0, target_cap - balanced),
            ) + balanced
            if final_quantity <= leader + 1e-10:
                return
            quantity = min(
                max(0.0, target_cap - balanced),
                tranche,
            )
        if risk_cap is not None and build_cap > risk_cap + 1e-10:
            self.state.last_action = "hedge_repair_waiting_for_target_headroom"
            return
        pair_purpose = (
            HEDGE_REPAIR_PAIR_PURPOSE
            if repair_pair
            else "top_up_hedge"
        )
        long_capacity = max(
            0.0,
            round(build_cap - self.state.long_quantity, 12),
        )
        short_capacity = max(
            0.0,
            round(build_cap - self.state.short_quantity, 12),
        )
        if repair_pair:
            if difference > 0:
                long_quantity = common_quantity
                short_quantity = round(common_quantity + gap, 12)
            else:
                long_quantity = round(common_quantity + gap, 12)
                short_quantity = common_quantity
        else:
            long_quantity = short_quantity = quantity
        long_quantity, long_blocked = self._pair_build_retry_quantity(
            purpose=pair_purpose,
            side=PositionSide.LONG,
            quantity=long_quantity,
            mark=mark,
            available=long_capacity,
        )
        short_quantity, short_blocked = self._pair_build_retry_quantity(
            purpose=pair_purpose,
            side=PositionSide.SHORT,
            quantity=short_quantity,
            mark=mark,
            available=short_capacity,
        )
        if long_blocked or short_blocked:
            self.state.last_action = "top_up_waiting_for_exchange_minimum"
            return
        projected_long = self.state.long_quantity + long_quantity
        projected_short = self.state.short_quantity + short_quantity
        pair_quantity = max(
            projected_long,
            projected_short,
        )
        if (
            pair_quantity <= leader + 1e-10
            or pair_quantity > build_cap + 1e-10
        ):
            self.state.last_action = "top_up_waiting_for_exchange_minimum"
            return
        if not math.isclose(
            projected_long,
            projected_short,
            rel_tol=1e-9,
            abs_tol=1e-10,
        ):
            self.state.last_action = "top_up_waiting_for_exchange_minimum"
            return
        created: List[str] = []
        for position_side, side, side_quantity in (
            (PositionSide.LONG, "BUY", long_quantity),
            (PositionSide.SHORT, "SELL", short_quantity),
        ):
            order = self.lifecycle.create_order(
                position_side=position_side,
                side=side,
                quantity=side_quantity,
                price=mark,
                reduce_only=False,
                purpose=pair_purpose,
            )
            if order is not None:
                created.append(order.order_id)
        if created and not self.external_execution:
            self.lifecycle.clear_deferred_minimum(purpose=pair_purpose)
        if created:
            if allow_imbalance and not self._positions_are_balanced():
                self.state.phase = StrategyPhase.REPAIRING_HEDGE
            self.state.last_action = (
                "hedge_repair_with_top_up"
                if pair_purpose == HEDGE_REPAIR_PAIR_PURPOSE
                else "top_up_hedge"
            )
            events.append(
                {
                    "type": "simulated_order_plan",
                    "action": self.state.last_action,
                    "quantity": max(long_quantity, short_quantity),
                    "long_add_quantity": long_quantity,
                    "short_add_quantity": short_quantity,
                    "previous_gap": abs(difference),
                    "target_quantity": self.state.target_quantity,
                    "effective_build_cap": build_cap,
                    "order_ids": created,
                    "simulated": True,
                }
            )

    def take_profit_restore_minimum_quantity(
        self,
        mark: float,
        *,
        side: Optional[PositionSide] = None,
    ) -> float:
        """Keep a take-profit large enough to leave a legal restore ticket."""
        if not math.isfinite(mark) or mark <= 0:
            return 0.0
        deferred = self.lifecycle.deferred_minimum_summary(
            purposes={"restore"},
            position_side=side,
        )
        minimum = max(
            self._external_restore_minimum_order_quantity,
            float(deferred["minimum_quantity"]),
            (
                float(deferred["minimum_notional"]) / mark
                if deferred["minimum_notional"] > 0
                else 0.0
            ),
        )
        step = max(0.0, self._external_order_step_size)
        if minimum <= 1e-10 or step <= 0:
            return max(0.0, minimum)
        minimum += step
        return max(0.0, math.ceil((minimum - 1e-12) / step) * step)

    def _maybe_take_profit(
        self,
        mark: float,
        portfolio: PortfolioSnapshot,
        events: List[dict],
        market_row: Optional[MarketRow] = None,
        only_side: Optional[PositionSide] = None,
    ) -> None:
        sweep_block_reason = str(
            self.profit_bucket.sweep_block_reason or ""
        ).strip()
        subscription_blocks_harvest = (
            self.profit_bucket.closed_loop_ready
            and not self.last_risk_decision.block_subscription
            and (
                not sweep_block_reason
                or not _is_profit_sweep_immediate_release_reason(
                    sweep_block_reason
                )
            )
        )
        if (
            mark <= 0
            or self.last_risk_decision.block_harvest
            or subscription_blocks_harvest
        ):
            if self.last_risk_decision.block_harvest:
                self.state.last_action = "take_profit_paused_by_risk"
            return
        candidates = [
            (
                PositionSide.LONG,
                self.state.long_quantity,
            ),
            (
                PositionSide.SHORT,
                self.state.short_quantity,
            ),
        ]
        if only_side is not None:
            candidates = [
                (side, quantity)
                for side, quantity in candidates
                if side == only_side
            ]
        deferred_by_side = {
            side: self.lifecycle.deferred_minimum_summary(
                purposes={"take_profit"},
                position_side=side,
            )
            for side, _ in candidates
        }
        deferred_candidates = [
            (side, position_quantity)
            for side, position_quantity in candidates
            if deferred_by_side[side]["count"] > 0
        ]
        if deferred_candidates:
            # A deferred minimum is one unfinished harvest intent. Do not
            # start a second side's harvest until this one is executable or
            # explicitly abandoned as no longer profitable.
            candidates = deferred_candidates
        harvest_limit = (
            min(self.state.long_quantity, self.state.short_quantity)
            * float(
                getattr(
                    self.config,
                    "HARVEST_TRANCHE_FRACTION",
                    0.05,
                )
            )
        )
        for side, position_quantity in candidates:
            deferred = deferred_by_side[side]
            retrying_deferred = deferred["count"] > 0
            deferred_quantity = float(deferred["residual_quantity"])
            minimum_quantity = float(deferred["minimum_quantity"])
            minimum_notional = float(deferred["minimum_notional"])
            aggregate_quantity = deferred_quantity
            if deferred["count"] > 0 and not deferred["minimum_known"]:
                aggregate_quantity += harvest_limit
            restore_floor = self.take_profit_restore_minimum_quantity(
                mark,
                side=side,
            )
            quantity = min(
                position_quantity,
                max(
                    harvest_limit,
                    aggregate_quantity,
                    minimum_quantity,
                    restore_floor,
                    (
                        minimum_notional / mark
                        if minimum_notional > 0 and mark > 0
                        else 0.0
                    ),
                ),
            )
            if (
                restore_floor > 0
                and position_quantity + 1e-10 < restore_floor
            ):
                self.state.last_action = "take_profit_waiting_restore_minimum"
                continue
            evaluation = self._evaluate_harvest(
                side=side,
                mark=mark,
                quantity=quantity,
                require_trigger=True,
                market_row=market_row,
                require_rsi_cross=not retrying_deferred,
            )
            if not evaluation["eligible"]:
                if retrying_deferred and evaluation["block_reason"] in {
                    "roe",
                    "net_profit",
                }:
                    self._abort_pending_take_profit(
                        events,
                        "deferred_take_profit_cleared_no_longer_profitable",
                    )
                    return
                if evaluation["block_reason"] == "net_profit":
                    self.state.last_action = "harvest_blocked_net"
                continue
            quantity = self._harvest_retry_quantity(
                purpose="take_profit",
                side=side,
                quantity=quantity,
                mark=mark,
            )
            if quantity <= 1e-10:
                continue
            self.state.phase = StrategyPhase.TAKING_PROFIT
            self.state.pending_rebalance_side = side
            self.state.pending_harvest_quantity = quantity
            self.state.pending_restore_quantity = 0.0
            self.state.pending_harvest_trade_quantity = 0.0
            self.state.profit_sweep_started_at_ms = 0
            self._pending_take_profit_gross = 0.0
            self._pending_take_profit_notional = 0.0
            order = self.lifecycle.create_order(
                position_side=side,
                side="SELL" if side == PositionSide.LONG else "BUY",
                quantity=quantity,
                price=mark,
                reduce_only=True,
                purpose="take_profit",
            )
            if order is not None:
                if only_side is not None:
                    self._clear_deferred_restore_carry()
                self.lifecycle.clear_deferred_minimum(
                    purpose="take_profit",
                    position_side=side,
                )
                self.state.profit_harvest_rsi_confirmed = True
                self.state.last_action = "take_profit"
                events.append(
                    {
                        "type": "simulated_order_plan",
                        "action": "take_profit",
                        "position_side": side.value,
                        "position_roe": evaluation["position_roe"],
                        "trigger_roe": evaluation["trigger_roe"],
                        "gross_pnl": evaluation["gross_pnl"],
                        "estimated_cost": evaluation["estimated_cost"],
                        "estimated_net_pnl": evaluation["net_pnl"],
                        "required_net_pnl": evaluation["required_net_pnl"],
                        "order_ids": [order.order_id],
                        "simulated": True,
                    }
                )
            return

    def _evaluate_harvest(
        self,
        *,
        side: PositionSide,
        mark: float,
        quantity: float,
        require_trigger: bool,
        market_row: Optional[MarketRow] = None,
        require_rsi_cross: bool = True,
    ) -> dict:
        entry_price = (
            self.state.long_entry_price
            if side == PositionSide.LONG
            else self.state.short_entry_price
        )
        pnl_per_unit = (
            mark - entry_price
            if side == PositionSide.LONG
            else entry_price - mark
        )
        trigger_roe = max(
            0.0,
            float(getattr(self.config, "HARVEST_TRIGGER_ROE", 1.50)),
        )
        position_roe = (
            (
                pnl_per_unit
                * max(0.0, quantity)
                / (
                    entry_price
                    * max(0.0, quantity)
                    / max(self.leverage, 1.0)
                )
                if quantity > 0
                else 0.0
            )
            if entry_price > 0
            else 0.0
        )
        gross = max(0.0, pnl_per_unit) * max(0.0, quantity)
        estimated_cost = (
            max(0.0, quantity)
            * max(0.0, mark)
            * self._execution_cost_bps
            / 10000.0
            * 2.0
        )
        net = gross - estimated_cost
        required_net = max(
            max(
                0.0,
                float(
                    getattr(
                        self.config,
                        "HARVEST_MIN_NET_USD",
                        1.0,
                    )
                ),
            ),
            estimated_cost
            * max(
                0.0,
                float(
                    getattr(
                        self.config,
                        "HARVEST_COST_MULTIPLIER",
                        3.0,
                    )
                ),
            ),
        )
        if quantity <= 1e-10 or mark <= 0 or entry_price <= 0:
            block_reason = "invalid_input"
        elif require_trigger and position_roe + 1e-12 < trigger_roe:
            block_reason = "roe"
        elif net + 1e-12 < required_net:
            block_reason = "net_profit"
        elif not self._harvest_market_gate(
            side=side,
            market_row=market_row,
            require_rsi_cross=require_rsi_cross,
        ):
            block_reason = self._harvest_market_block_reason(
                side=side,
                market_row=market_row,
                require_rsi_cross=require_rsi_cross,
            )
        else:
            block_reason = None
        return {
            "eligible": block_reason is None,
            "block_reason": block_reason,
            "position_roe": position_roe,
            "trigger_roe": trigger_roe,
            "gross_pnl": gross,
            "estimated_cost": estimated_cost,
            "net_pnl": net,
            "required_net_pnl": required_net,
        }

    def _harvest_market_gate(
        self,
        *,
        side: PositionSide,
        market_row: Optional[MarketRow],
        require_rsi_cross: bool,
    ) -> bool:
        row = market_row or self._last_market_row
        if not isinstance(row, dict):
            return False
        if not thirty_minute_row_volatility_allowed(
            row,
            maximum_range=float(
                getattr(self.config, "EXIT_VOLATILITY_MAX_RANGE", 0.01)
            ),
            atr_multiplier=float(
                getattr(
                    self.config,
                    "EXIT_VOLATILITY_ATR_MULTIPLIER",
                    1.25,
                )
            ),
            atr_key="thirty_minute_atr14",
        ):
            return False
        if not require_rsi_cross:
            return True
        try:
            rsi12 = float(row["thirty_minute_rsi12"])
            previous_rsi12 = float(
                row["thirty_minute_previous_rsi12"]
            )
        except (KeyError, TypeError, ValueError):
            return False
        if not math.isfinite(rsi12) or not math.isfinite(previous_rsi12):
            return False
        return (
            previous_rsi12 >= 60.0 and rsi12 < 60.0
            if side == PositionSide.LONG
            else previous_rsi12 <= 40.0 and rsi12 > 40.0
        )

    def _harvest_market_block_reason(
        self,
        *,
        side: PositionSide,
        market_row: Optional[MarketRow],
        require_rsi_cross: bool,
    ) -> str:
        row = market_row or self._last_market_row
        if not isinstance(row, dict):
            return "thirty_minute_data_unavailable"
        if not thirty_minute_row_volatility_allowed(
            row,
            maximum_range=float(
                getattr(self.config, "EXIT_VOLATILITY_MAX_RANGE", 0.01)
            ),
            atr_multiplier=float(
                getattr(
                    self.config,
                    "EXIT_VOLATILITY_ATR_MULTIPLIER",
                    1.25,
                )
            ),
            atr_key="thirty_minute_atr14",
        ):
            return str(
                row.get("thirty_minute_reject_reason")
                or "exit_volatility_blocked"
            )
        if not require_rsi_cross:
            return ""
        return (
            "rsi12_cross_down_60_required"
            if side == PositionSide.LONG
            else "rsi12_cross_up_40_required"
        )

    def _quote_pending_take_profit(
        self,
        mark: float,
        events: List[dict],
    ) -> None:
        side = self.state.pending_rebalance_side
        if side is None or mark <= 0:
            return
        if any(
            order.purpose == "take_profit"
            for order in self.lifecycle.active_orders
        ):
            return
        if self.last_risk_decision.block_harvest:
            self.state.last_action = "take_profit_paused_by_risk"
            return
        target_quantity = self.state.pending_harvest_quantity
        if target_quantity <= 1e-10:
            target_quantity = (
                self.state.pending_restore_quantity
                or self.state.target_quantity
            )
            self.state.pending_harvest_quantity = target_quantity
        quantity = max(
            0.0,
            target_quantity - self.state.pending_restore_quantity,
        )
        deferred = self.lifecycle.deferred_minimum_summary(
            purposes={"take_profit"},
            position_side=side,
        )
        aggregate_quantity = float(deferred["residual_quantity"])
        if deferred["count"] > 0 and not deferred["minimum_known"]:
            aggregate_quantity += (
                min(self.state.long_quantity, self.state.short_quantity)
                * float(
                    getattr(
                        self.config,
                        "HARVEST_TRANCHE_FRACTION",
                        0.05,
                    )
                )
            )
        available = (
            self.state.long_quantity
            if side == PositionSide.LONG
            else self.state.short_quantity
        )
        quantity = min(
            available,
            max(
                quantity,
                aggregate_quantity,
                float(deferred["minimum_quantity"]),
                self.take_profit_restore_minimum_quantity(
                    mark,
                    side=side,
                ),
                (
                    float(deferred["minimum_notional"]) / mark
                    if deferred["minimum_notional"] > 0 and mark > 0
                    else 0.0
                ),
            ),
        )
        self.state.pending_harvest_quantity = max(
            self.state.pending_harvest_quantity,
            self.state.pending_restore_quantity + quantity,
        )
        quantity = self._harvest_retry_quantity(
            purpose="take_profit",
            side=side,
            quantity=quantity,
            mark=mark,
        )
        if quantity <= 1e-10:
            if (
                deferred["count"] > 0
                and self.state.pending_restore_quantity <= 1e-10
            ):
                self.state.phase = (
                    StrategyPhase.HEDGED
                    if self._positions_are_balanced()
                    else StrategyPhase.REPAIRING_HEDGE
                )
                self.state.last_action = "take_profit_minimum_waiting"
            return
        restore_floor = self.take_profit_restore_minimum_quantity(
            mark,
            side=side,
        )
        if restore_floor > 0:
            if available + 1e-10 < restore_floor:
                self.state.last_action = "take_profit_waiting_restore_minimum"
                return
            quantity = max(quantity, restore_floor)
        self.state.pending_harvest_quantity = max(
            self.state.pending_harvest_quantity,
            self.state.pending_restore_quantity + quantity,
        )
        evaluation = self._evaluate_harvest(
            side=side,
            mark=mark,
            quantity=quantity,
            require_trigger=self.state.pending_restore_quantity <= 1e-10,
            require_rsi_cross=(
                not self.state.profit_harvest_rsi_confirmed
                and self.state.pending_restore_quantity <= 1e-10
            ),
        )
        if quantity > 1e-10 and not evaluation["eligible"]:
            if (
                self.state.profit_harvest_rsi_confirmed
                and evaluation["block_reason"]
                not in {"invalid_input", "roe", "net_profit"}
            ):
                self.state.last_action = "take_profit_waiting_market_gate"
                return
            self._abort_pending_take_profit(
                events,
                "take_profit_aborted_no_longer_profitable",
            )
            return
        order = self.lifecycle.create_order(
            position_side=side,
            side="SELL" if side == PositionSide.LONG else "BUY",
            quantity=quantity,
            price=mark,
            reduce_only=True,
            purpose="take_profit",
        )
        if order is not None:
            self.lifecycle.clear_deferred_minimum(
                purpose="take_profit",
                position_side=side,
            )
            self.state.last_action = "take_profit_requote"
            events.append(
                {
                    "type": "simulated_order_plan",
                    "action": "take_profit_requote",
                    "position_side": side.value,
                    "order_ids": [order.order_id],
                    "simulated": True,
                }
            )

    def _abort_pending_take_profit(
        self,
        events: List[dict],
        action: str,
    ) -> None:
        completed_quantity = max(
            0.0,
            self.state.pending_restore_quantity,
        )
        if completed_quantity > 1e-10:
            self.lifecycle.clear_deferred_minimum(
                purpose="take_profit",
            )
            self.state.pending_harvest_quantity = completed_quantity
        else:
            self.lifecycle.clear_deferred_minimum()
            self.state.pending_rebalance_side = None
            self.state.pending_harvest_quantity = 0.0
            self.state.pending_restore_quantity = 0.0
            self.state.profit_harvest_rsi_confirmed = False
            self.state.pending_harvest_trade_quantity = 0.0
            self.state.profit_sweep_started_at_ms = 0
            self.state.phase = (
                StrategyPhase.HEDGED
                if self._positions_are_balanced()
                else StrategyPhase.REPAIRING_HEDGE
            )
            self._pending_take_profit_gross = 0.0
            self._pending_take_profit_notional = 0.0
            self.profit_bucket.pending_harvest_net_credited = 0.0
        self.state.last_action = action
        events.append(
            {
                "type": "state_transition",
                "action": action,
                "completed_quantity": completed_quantity,
                "simulated": True,
            }
        )

    def _plan_pending_harvest_orders(
        self,
        mark: float,
        events: List[dict],
    ) -> None:
        if self.state.phase not in {
            StrategyPhase.TAKING_PROFIT,
            StrategyPhase.PROFIT_SWEEPING,
            StrategyPhase.REBALANCING,
        }:
            return
        if self.last_risk_decision.hard_risk:
            return
        self._sync_phase_after_fills(events)
        if self.state.phase == StrategyPhase.TAKING_PROFIT:
            self._quote_pending_take_profit(mark, events)
        elif (
            self.state.phase == StrategyPhase.REBALANCING
            and self._deferred_restore_carry_ready()
        ):
            carry_side = self.state.deferred_restore_carry_side
            self._maybe_take_profit(
                mark,
                self.last_portfolio_snapshot,
                events,
                market_row=self._last_market_row,
                only_side=carry_side,
            )
        if (
            self.state.phase == StrategyPhase.REBALANCING
            or (
                not self.live_rwusd_execution
                and self.state.phase
                in {
                    StrategyPhase.TAKING_PROFIT,
                    StrategyPhase.PROFIT_SWEEPING,
                }
            )
        ):
            self._quote_restore(mark, events)
        self._sync_phase_after_fills(events)

    def _harvest_retry_quantity(
        self,
        *,
        purpose: str,
        side: PositionSide,
        quantity: float,
        mark: float,
    ) -> float:
        requested = max(0.0, float(quantity or 0))
        deferred = self.lifecycle.deferred_minimum_summary(
            purposes={purpose},
            position_side=side,
        )
        available = (
            self.state.long_quantity
            if side == PositionSide.LONG
            else self.state.short_quantity
        )
        if purpose == "take_profit":
            restore_floor = self.take_profit_restore_minimum_quantity(
                mark,
                side=side,
            )
            if restore_floor > 0:
                if available + 1e-10 < restore_floor:
                    return 0.0
                requested = max(requested, restore_floor)
        if deferred["count"] <= 0:
            return requested
        cooldown = max(
            1,
            int(
                getattr(
                    self.config,
                    "EXCHANGE_MINIMUM_RETRY_COOLDOWN_CYCLES",
                    5,
                )
            ),
        )
        if self.lifecycle.cycle - deferred["latest_cycle"] < cooldown:
            return 0.0
        ready_quantity = (
            available
            if purpose == "take_profit"
            else max(0.0, float(quantity or 0))
        )
        if not deferred["minimum_known"]:
            growth_fraction = max(
                0.0,
                float(
                    getattr(
                        self.config,
                        "EXCHANGE_MINIMUM_UNKNOWN_RETRY_GROWTH_FRACTION",
                        0.25,
                    )
                ),
            )
            required_growth = float(deferred["residual_quantity"]) * (
                1.0 + growth_fraction
            )
            if ready_quantity <= required_growth + 1e-10:
                return 0.0
            candidate = max(requested, required_growth)
            return min(
                available if purpose == "take_profit" else ready_quantity,
                candidate,
            )
        minimum_quantity = max(
            float(deferred["minimum_quantity"]),
            (
                float(deferred["minimum_notional"]) / mark
                if deferred["minimum_notional"] > 0 and mark > 0
                else 0.0
            ),
        )
        if ready_quantity + 1e-10 < minimum_quantity:
            return 0.0
        return (
            min(available, max(requested, minimum_quantity))
            if purpose == "take_profit"
            else requested
        )

    def _restore_gap(self, side: PositionSide) -> float:
        difference = self.state.long_quantity - self.state.short_quantity
        return (
            max(0.0, -difference)
            if side == PositionSide.LONG
            else max(0.0, difference)
        )

    def _active_restore_coverage(self, side: PositionSide) -> float:
        return sum(
            max(
                0.0,
                float(order.quantity)
                - float(order.cumulative_filled_quantity),
            )
            for order in self.lifecycle.active_orders
            if (
                order.purpose == "restore"
                and order.position_side == side
                and not order.reduce_only
            )
        )

    def restore_overcoverage(self) -> Optional[dict]:
        if self.state.phase not in {
            StrategyPhase.TAKING_PROFIT,
            StrategyPhase.PROFIT_SWEEPING,
            StrategyPhase.REBALANCING,
        }:
            return None
        side = self.state.pending_rebalance_side
        if side is None:
            return None
        active = [
            order
            for order in self.lifecycle.active_orders
            if order.purpose == "restore" and not order.reduce_only
        ]
        if not active:
            return None
        gap = self._restore_gap(side)
        coverage = sum(
            max(
                0.0,
                float(order.quantity)
                - float(order.cumulative_filled_quantity),
            )
            for order in active
            if order.position_side == side
        )
        invalid_side = any(order.position_side != side for order in active)
        if not invalid_side and coverage <= gap + 1e-10:
            return None
        return {
            "reason": "restore_coverage_mismatch",
            "position_side": side.value,
            "gap_quantity": gap,
            "active_restore_coverage": coverage,
            "order_ids": [order.order_id for order in active],
        }

    def _quote_restore(self, mark: float, events: List[dict]) -> None:
        side = self.state.pending_rebalance_side
        if side is None or mark <= 0:
            self.state.last_action = "restore_wait"
            return
        gap = self._restore_gap(side)
        active_coverage = self._active_restore_coverage(side)
        quantity = max(0.0, gap - active_coverage)
        if quantity <= 1e-10:
            if not (
                self.last_risk_decision.block_harvest
                and any(
                    order.purpose == "take_profit"
                    for order in self.lifecycle.active_orders
                )
            ):
                self.state.last_action = "harvest_restore_dust_wait"
            return
        deferred = self.lifecycle.deferred_minimum_summary(
            purposes={"restore"},
            position_side=side,
        )
        active_harvest = self.has_active_harvest_orders()
        if deferred["count"] > 0 and not active_harvest:
            deferred_residual = max(
                0.0,
                float(deferred["residual_quantity"]),
            )
            deferred_minimum = max(
                float(deferred["minimum_quantity"]),
                (
                    float(deferred["minimum_notional"]) / mark
                    if deferred["minimum_notional"] > 0 and mark > 0
                    else 0.0
                ),
            )
            if (
                deferred["minimum_known"]
                and deferred_residual > 1e-10
                and deferred_residual + 1e-10 < deferred_minimum
                and math.isclose(
                    gap,
                    deferred_residual,
                    rel_tol=1e-6,
                    abs_tol=1e-8,
                )
            ):
                self.state.deferred_restore_carry_side = side
                self.state.deferred_restore_carry_quantity = gap
                self.state.last_action = "restore_minimum_waiting_for_harvest"
                self.persist()
                return
        if (
            deferred["count"] > 0
            and not active_harvest
            and self._restore_carry_matches_current_gap()
        ):
            self.state.last_action = "restore_minimum_waiting_for_harvest"
            return
        quantity = self._harvest_retry_quantity(
            purpose="restore",
            side=side,
            quantity=quantity,
            mark=mark,
        )
        if quantity <= 1e-10:
            return
        order = self.lifecycle.create_order(
            position_side=side,
            side="BUY" if side == PositionSide.LONG else "SELL",
            quantity=quantity,
            price=mark,
            reduce_only=False,
            purpose="restore",
        )
        if order is not None:
            self._clear_deferred_restore_carry()
            self.lifecycle.clear_deferred_minimum(
                purpose="restore",
                position_side=side,
            )
            self.state.last_action = "restore_now"
            events.append(
                {
                    "type": "simulated_order_plan",
                    "action": "restore_now",
                    "position_side": side.value,
                    "gap_quantity": gap,
                    "active_restore_coverage": active_coverage,
                    "quantity": quantity,
                    "order_ids": [order.order_id],
                    "simulated": True,
                }
            )

    def _recover_pending_harvest_side(self) -> None:
        if (
            self.state.phase
            not in {
                StrategyPhase.TAKING_PROFIT,
                StrategyPhase.PROFIT_SWEEPING,
                StrategyPhase.REBALANCING,
            }
            or self.state.pending_rebalance_side is not None
        ):
            return
        active = [
            order
            for order in self.lifecycle.active_orders
            if order.purpose in HARVEST_ORDER_PURPOSES
        ]
        sides = {order.position_side for order in active}
        if len(sides) != 1:
            return
        self.state.pending_rebalance_side = next(iter(sides))
        if (
            self.state.pending_restore_quantity > 1e-10
            or any(order.purpose == "take_profit" for order in active)
        ):
            self.state.profit_harvest_rsi_confirmed = True
        if self.state.pending_harvest_quantity <= 1e-10:
            take_profit_orders = [
                order
                for order in active
                if order.purpose == "take_profit"
            ]
            remaining_take_profit = sum(
                max(
                    0.0,
                    float(order.quantity)
                    - float(order.cumulative_filled_quantity),
                )
                for order in take_profit_orders
            )
            if take_profit_orders:
                self.state.pending_harvest_quantity = (
                    self.state.pending_restore_quantity
                    + remaining_take_profit
                )
            else:
                recovered_restore = max(
                    self.state.pending_restore_quantity,
                    self._restore_gap(self.state.pending_rebalance_side),
                    self._active_restore_coverage(
                        self.state.pending_rebalance_side
                    ),
                )
                self.state.pending_harvest_quantity = recovered_restore
                self.state.pending_restore_quantity = recovered_restore
                if self.state.phase == StrategyPhase.TAKING_PROFIT:
                    self.state.phase = StrategyPhase.REBALANCING
        self.state.last_action = "harvest_context_restored"

    def _align_pending_restore_side_to_gap(self) -> None:
        if self.state.phase not in {
            StrategyPhase.TAKING_PROFIT,
            StrategyPhase.PROFIT_SWEEPING,
            StrategyPhase.REBALANCING,
        }:
            return
        if self.state.pending_rebalance_side is None:
            return
        if any(
            order.purpose == "take_profit"
            for order in self.lifecycle.active_orders
        ):
            return
        difference = self.state.long_quantity - self.state.short_quantity
        if abs(difference) <= 1e-10:
            return
        missing_side = (
            PositionSide.LONG if difference < 0 else PositionSide.SHORT
        )
        if self.state.pending_rebalance_side == missing_side:
            return
        self.state.pending_rebalance_side = missing_side
        self.state.last_action = "restore_side_recalibrated"

    def _enter_hard_risk_freeze(
        self,
        events: List[dict],
        *,
        emit_transition: bool,
    ) -> None:
        canceled: List[str] = []
        if not self.external_execution:
            canceled = self.lifecycle.cancel_active_except(
                HARD_RISK_ALLOWED_PURPOSES
            )
        unsafe_active = any(
            order.purpose not in HARD_RISK_ALLOWED_PURPOSES
            for order in self.lifecycle.active_orders
        )
        if unsafe_active:
            self.state.last_action = "risk_waiting_order_cancel"
        else:
            self.state.last_action = "hard_risk_reduction_pending"
        if emit_transition or canceled:
            events.append(
                {
                    "type": "risk",
                    "action": "risk_freeze",
                    "reason": self.last_risk_decision.reason,
                    "canceled_order_ids": canceled,
                    "automatic_reduction": True,
                    "simulated": True,
                }
            )

    def _expire_and_requote(self, events: List[dict]) -> None:
        if self.external_execution:
            return
        expired = self.lifecycle.expire_orders_older_than(
            int(getattr(self.config, "DRY_RUN_ORDER_TIMEOUT_CYCLES", 0))
        )
        if not expired:
            return
        events.append(
            {
                "type": "simulated_order_timeout",
                "order_ids": [order.order_id for order in expired],
                "requote_count": self.lifecycle.timeout_requote_count,
                "simulated": True,
            }
        )
        maximum = int(getattr(self.config, "DRY_RUN_MAX_REQUOTES", 0))
        if maximum > 0 and self.lifecycle.timeout_requote_count > maximum:
            self.lifecycle.cancel_active()
            self.state.phase = StrategyPhase.SELECTING_SYMBOL
            self.state.last_action = "requote_limit_retrying"

    def _credit_simulated_harvest(
        self,
        *,
        realized_pnl: float,
        fill_notional: float,
    ) -> None:
        estimated_cost = (
            max(0.0, float(fill_notional or 0))
            * self._execution_cost_bps
            / 10000.0
            * 2.0
        )
        net = float(realized_pnl or 0) - estimated_cost
        self.profit_bucket.realized_pnl_total += net
        self._apply_external_harvest_delta(net)

    def _apply_fill(self, fill: FillEvent) -> None:
        quantity = max(0.0, fill.fill_quantity)
        price = max(0.0, fill.fill_price)
        if quantity <= 0 or price <= 0:
            return
        if fill.position_side == PositionSide.LONG:
            if fill.reduce_only:
                executable = min(quantity, self.state.long_quantity)
                realized = (price - self.state.long_entry_price) * executable
                self.state.long_quantity = max(0.0, self.state.long_quantity - executable)
                if fill.purpose == "take_profit":
                    self.state.profit_harvest_rsi_confirmed = True
                    if not self.external_execution:
                        self._pending_take_profit_gross += realized
                        self._credit_simulated_harvest(
                            realized_pnl=realized,
                            fill_notional=price * executable,
                        )
                    self._pending_take_profit_notional += price * executable
                    self.state.pending_restore_quantity += executable
                elif not self.external_execution:
                    self.profit_bucket.realized_pnl_total += realized
                if self.state.long_quantity <= 1e-10:
                    self.state.long_quantity = 0.0
                    self.state.long_entry_price = 0.0
            else:
                old_qty = self.state.long_quantity
                new_qty = old_qty + quantity
                self.state.long_entry_price = (
                    ((old_qty * self.state.long_entry_price) + (quantity * price)) / new_qty
                    if new_qty > 0
                    else 0.0
                )
                self.state.long_quantity = new_qty
        else:
            if fill.reduce_only:
                executable = min(quantity, self.state.short_quantity)
                realized = (self.state.short_entry_price - price) * executable
                self.state.short_quantity = max(0.0, self.state.short_quantity - executable)
                if fill.purpose == "take_profit":
                    self.state.profit_harvest_rsi_confirmed = True
                    if not self.external_execution:
                        self._pending_take_profit_gross += realized
                        self._credit_simulated_harvest(
                            realized_pnl=realized,
                            fill_notional=price * executable,
                        )
                    self._pending_take_profit_notional += price * executable
                    self.state.pending_restore_quantity += executable
                elif not self.external_execution:
                    self.profit_bucket.realized_pnl_total += realized
                if self.state.short_quantity <= 1e-10:
                    self.state.short_quantity = 0.0
                    self.state.short_entry_price = 0.0
            else:
                old_qty = self.state.short_quantity
                new_qty = old_qty + quantity
                self.state.short_entry_price = (
                    ((old_qty * self.state.short_entry_price) + (quantity * price)) / new_qty
                    if new_qty > 0
                    else 0.0
                )
                self.state.short_quantity = new_qty

    def _sync_phase_after_fills(self, events: List[dict]) -> None:
        if self.state.phase == StrategyPhase.TAKING_PROFIT:
            side = self.state.pending_rebalance_side
            target = (
                self.state.pending_harvest_quantity
                if self.state.pending_harvest_quantity > 1e-10
                else self.state.pending_restore_quantity
            )
            harvest_completed = (
                target > 1e-10
                and self.state.pending_restore_quantity + 1e-10 >= target
                and not any(
                    order.purpose == "take_profit"
                    for order in self.lifecycle.active_orders
                )
            )
            if not harvest_completed:
                return
            net = 0.0
            if not self.external_execution:
                estimated_cost = (
                    self._pending_take_profit_notional
                    * self._execution_cost_bps
                    / 10000.0
                    * 2.0
                )
                net = self._pending_take_profit_gross - estimated_cost
                self.profit_bucket.harvest_count += 1
                self.state.pending_harvest_trade_quantity = (
                    self.state.pending_restore_quantity
                )
            if not self.external_execution:
                self.profit_bucket.closed_loop_ready = (
                    self.profit_bucket.harvest_buffer + 1e-12
                    >= float(getattr(self.config, "MIN_SWEEP", 1.0))
                )
            self.profit_bucket.last_rebalance_action = "take_profit"
            self.profit_bucket.sweep_block_reason = (
                "official_profit_reconciliation_pending"
                if (
                    self.external_execution
                    and self.state.pending_harvest_trade_quantity + 1e-10
                    < self.state.pending_restore_quantity
                )
                else "profit_sweep_pending"
            )
            self.state.take_profit_count += 1
            self.state.phase = StrategyPhase.PROFIT_SWEEPING
            self.state.profit_sweep_started_at_ms = int(
                datetime.now(timezone.utc).timestamp() * 1000
            )
            self.state.last_action = "profit_sweep_started"
            events.append(
                {
                    "type": "simulated_harvest",
                    "action": "profit_sweep_started",
                    "position_side": side.value if side else None,
                    "net_pnl": net if not self.external_execution else None,
                    "official_trade_reconciliation_pending": (
                        self.external_execution
                    ),
                    "simulated": not self.external_execution,
                }
            )
            self._pending_take_profit_gross = 0.0
            self._pending_take_profit_notional = 0.0

        if self.lifecycle.active_orders:
            return
        if self.state.phase == StrategyPhase.OPENING_HEDGE:
            if (
                self._positions_are_balanced()
                and min(
                    self.state.long_quantity,
                    self.state.short_quantity,
                )
                > 1e-10
            ):
                self._complete_opening_hedge(events)
            elif not self._positions_are_balanced():
                self.state.phase = StrategyPhase.REPAIRING_HEDGE
                self.state.last_action = "hedge_gap_detected"
        elif self.state.phase == StrategyPhase.REPAIRING_HEDGE:
            if self._positions_are_balanced():
                if (
                    self.state.long_quantity <= 1e-10
                    and self.state.short_quantity <= 1e-10
                ):
                    self.state.phase = StrategyPhase.SELECTING_SYMBOL
                    self.state.last_action = "hedge_repair_flat_retrying"
                else:
                    self.state.phase = StrategyPhase.HEDGED
                    self.state.last_action = "hedge_repair_completed"
                    self._clear_repair_state()
                    self._settle_external_profit_if_balanced()
        elif self.state.phase == StrategyPhase.REBALANCING:
            side = self.state.pending_rebalance_side
            restored = (
                side is not None
                and self._positions_are_balanced()
            )
            if restored:
                self._complete_restore()
                events.append(
                    {
                        "type": "state_transition",
                        "action": "restore_completed",
                        "symbol": self.state.selected_symbol,
                        "simulated": True,
                    }
                )

    def finalize_profit_sweep(self) -> List[dict]:
        if self.state.phase != StrategyPhase.PROFIT_SWEEPING:
            return []

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        started_at_ms = max(0, int(self.state.profit_sweep_started_at_ms or 0))
        timeout_ms = max(
            1,
            int(
                float(
                    getattr(
                        self.config,
                        "PROFIT_SWEEP_MAX_WAIT_SEC",
                        90.0,
                    )
                )
                * 1000
            ),
        )
        timed_out = (
            started_at_ms > 0
            and now_ms >= started_at_ms + timeout_ms
        )
        trade_reconciliation_pending = (
            self.external_execution
            and self.state.pending_harvest_trade_quantity + 1e-10
            < self.state.pending_restore_quantity
        )
        subscription_pending = bool(
            self.live_rwusd_execution
            and self.profit_bucket.pending_subscription_stage
        )
        cash_reached_spot = self._profit_cash_reached_spot()
        transferable = max(0.0, self.profit_bucket.harvest_buffer)
        minimum = float(getattr(self.config, "MIN_SWEEP", 1.0))
        sweepable = (
            self.profit_bucket.closed_loop_ready
            and transferable + 1e-12 >= minimum
        )
        block_reason = str(
            self.profit_bucket.sweep_block_reason or ""
        ).strip()
        immediate_release = _is_profit_sweep_immediate_release_reason(
            block_reason
        )
        if self.last_risk_decision.hard_risk:
            release_reason = "hard_risk_restore_required"
        elif not trade_reconciliation_pending and cash_reached_spot:
            release_reason = "profit_cash_transferred_to_spot"
        elif (
            not trade_reconciliation_pending
            and not subscription_pending
            and not sweepable
        ):
            release_reason = "profit_sweep_completed"
        elif immediate_release and not subscription_pending:
            release_reason = block_reason
        elif timed_out and not subscription_pending:
            release_reason = "profit_sweep_timeout"
        else:
            self.profit_bucket.sweep_block_reason = (
                "official_profit_reconciliation_pending"
                if trade_reconciliation_pending
                else (
                    block_reason
                    or "rwusd_subscription_pending"
                )
            )
            self.persist()
            return []

        self.state.phase = StrategyPhase.REBALANCING
        self.state.last_action = "profit_sweep_released"
        self.state.profit_sweep_started_at_ms = 0
        if self.profit_bucket.pending_harvest_net_credited > 0:
            self.profit_bucket.harvest_count += 1
        self.profit_bucket.pending_harvest_net_credited = 0.0
        self.profit_bucket.last_rebalance_action = "profit_sweep_completed"
        self.profit_bucket.sweep_block_reason = release_reason
        self.persist()
        return [
            {
                "type": "state_transition",
                "action": "profit_sweep_released",
                "reason": release_reason,
                "harvest_buffer": transferable,
                "pending_subscription_stage": (
                    self.profit_bucket.pending_subscription_stage
                ),
                "pending_harvest_quantity": (
                    self.state.pending_harvest_quantity
                ),
                "pending_harvest_trade_quantity": (
                    self.state.pending_harvest_trade_quantity
                ),
                "pending_restore_quantity": (
                    self.state.pending_restore_quantity
                ),
                "simulated": not self.external_execution,
            }
        ]

    def _complete_restore(self) -> None:
        self.state.restore_count += 1
        self.lifecycle.clear_deferred_minimum(purpose="restore")
        self._clear_deferred_restore_carry()
        self.state.pending_rebalance_side = None
        self.state.pending_harvest_quantity = 0.0
        self.state.pending_restore_quantity = 0.0
        self.state.profit_harvest_rsi_confirmed = False
        self.state.pending_harvest_trade_quantity = 0.0
        self.state.profit_sweep_started_at_ms = 0
        self.state.phase = StrategyPhase.HEDGED
        self.state.last_action = "restore_completed"
        if self.external_execution:
            self._settle_external_profit_if_balanced()
        else:
            self.profit_bucket.closed_loop_ready = True
        self.profit_bucket.last_rebalance_action = "restore_now"
        self.profit_bucket.sweep_block_reason = (
            None
            if self.profit_bucket.closed_loop_ready
            else "awaiting_official_profit"
        )

    def _clear_harvest_task_state(self) -> None:
        self.lifecycle.clear_deferred_minimum(
            purposes=HARVEST_ORDER_PURPOSES,
        )
        self._clear_deferred_restore_carry()
        self.state.pending_rebalance_side = None
        self.state.pending_harvest_quantity = 0.0
        self.state.pending_restore_quantity = 0.0
        self.state.profit_harvest_rsi_confirmed = False
        self.state.pending_harvest_trade_quantity = 0.0
        self.state.profit_sweep_started_at_ms = 0
        self._pending_take_profit_gross = 0.0
        self._pending_take_profit_notional = 0.0

    def _simulate_profit_sweep(
        self,
        portfolio: PortfolioSnapshot,
        events: List[dict],
    ) -> None:
        if self.last_risk_decision.block_subscription:
            self.profit_bucket.sweep_block_reason = (
                "risk_subscription_frozen"
            )
            return
        if (
            not self.profit_bucket.closed_loop_ready
            or not self._harvest_subscription_phase_ready()
        ):
            return
        transferable = max(0.0, self.profit_bucket.harvest_buffer)
        if transferable < float(getattr(self.config, "MIN_SWEEP", 50.0)):
            self.profit_bucket.sweep_block_reason = "below_min_sweep"
            return
        self.profit_bucket.harvest_buffer -= transferable
        self.profit_bucket.realized_pnl_available_for_deposit -= transferable
        self.profit_bucket.rwusd_principal += transferable
        self.profit_bucket.rwusd_redeemable += transferable
        self.profit_bucket.deposit_count += 1
        self.profit_bucket.closed_loop_ready = False
        self.profit_bucket.last_rebalance_action = "sweep"
        self.profit_bucket.sweep_block_reason = None
        self.state.last_action = "rwusd_sweep"
        events.append(
            {
                "type": "simulated_transfer",
                "action": "rwusd_subscribe",
                "amount": transferable,
                "rwusd_shadow": True,
                "rwusd_testnet_supported": False,
                "simulated": True,
            }
        )

    def _simulate_redeem_if_needed(
        self,
        portfolio: PortfolioSnapshot,
        decision: RiskDecision,
        events: List[dict],
    ) -> None:
        if not decision.should_redeem_topup:
            return
        requested = 0.0
        if portfolio.uni_mmr < float(getattr(self.config, "REDEEM_UNIMMR", 0.0)):
            requested = max(
                requested,
                min(
                    self.profit_bucket.rwusd_redeemable,
                    portfolio.account_equity * 0.05,
                ),
            )
        amount = min(requested, self.profit_bucket.rwusd_redeemable)
        if amount < float(getattr(self.config, "MIN_REDEEM", 50.0)):
            return
        self.profit_bucket.rwusd_principal = max(
            0.0,
            self.profit_bucket.rwusd_principal - amount,
        )
        self.profit_bucket.rwusd_redeemable = max(
            0.0,
            self.profit_bucket.rwusd_redeemable - amount,
        )
        self.profit_bucket.redeem_count += 1
        self.profit_bucket.last_rebalance_action = "redeem_topup"
        events.append(
            {
                "type": "simulated_transfer",
                "action": "rwusd_redeem_topup",
                "amount": amount,
                "rwusd_shadow": True,
                "rwusd_testnet_supported": False,
                "simulated": True,
            }
        )

    def manual_flatten(self, mark_price: float) -> dict:
        mark = max(0.0, float(mark_price or self.state.last_mark_price or 0))
        if mark <= 0 and (
            self.state.long_quantity > 0 or self.state.short_quantity > 0
        ):
            return {
                "success": False,
                "message": "RWUSD dry-run cannot flatten without a valid mark price",
            }
        self.lifecycle.cancel_active()
        realized = 0.0
        if self.state.long_quantity > 0:
            realized += (
                mark - self.state.long_entry_price
            ) * self.state.long_quantity
        if self.state.short_quantity > 0:
            realized += (
                self.state.short_entry_price - mark
            ) * self.state.short_quantity
        self.profit_bucket.realized_pnl_total += realized
        self.state = HedgeState(
            peak_equity=self.state.peak_equity,
            last_action="manual_simulated_flatten",
        )
        self.lifecycle = DryRunOrderLifecycle()
        self._pending_take_profit_gross = 0.0
        self._pending_take_profit_notional = 0.0
        self.persist()
        return {
            "success": True,
            "message": (
                "RWUSD dry-run simulated hedge has been flattened; "
                "no exchange order or RWUSD transfer was submitted"
            ),
            "simulated": True,
            "realized_pnl": realized,
        }

    def get_status(self) -> dict:
        active_count, terminal_count = self.lifecycle.counts()
        total_unrealized = (
            self.state.long_unrealized_pnl + self.state.short_unrealized_pnl
        )
        gross_position_notional = (
            self.state.long_quantity + self.state.short_quantity
        ) * self.state.last_mark_price
        total_unrealized_rate = (
            total_unrealized / gross_position_notional * 100.0
            if gross_position_notional > 0
            else 0.0
        )
        target_completion_ratio = (
            self.effective_hedge_quantity() / self.state.target_quantity
            if self.state.target_quantity > 0
            else 0.0
        )
        harvest_quantity = (
            min(self.state.long_quantity, self.state.short_quantity)
            * max(
                0.0,
                float(
                    getattr(
                        self.config,
                        "HARVEST_TRANCHE_FRACTION",
                        0.05,
                    )
                ),
            )
        )
        long_harvest = self._evaluate_harvest(
            side=PositionSide.LONG,
            mark=self.state.last_mark_price,
            quantity=harvest_quantity,
            require_trigger=True,
        )
        short_harvest = self._evaluate_harvest(
            side=PositionSide.SHORT,
            mark=self.state.last_mark_price,
            quantity=harvest_quantity,
            require_trigger=True,
        )
        orders = {
            "cycle": self.lifecycle.cycle,
            "active_count": active_count,
            "terminal_count": terminal_count,
            "expired_order_count": self.lifecycle.expired_order_count,
            "timeout_requote_count": self.lifecycle.timeout_requote_count,
            "external_consecutive_rejection_count": (
                self.lifecycle.external_consecutive_rejection_count
            ),
            "external_consecutive_maker_requote_count": (
                self.lifecycle.external_consecutive_maker_requote_count
            ),
            "active": [
                order.to_dict() for order in self.lifecycle.active_orders
            ],
        }
        simulated_equity = (
            self.initial_capital
            + self.profit_bucket.realized_pnl_total
            + total_unrealized
            + self.profit_bucket.rwusd_interest_accrued
        )
        portfolio_margin_equity = max(
            0.0,
            float(self.last_portfolio_snapshot.account_equity or 0),
        )
        rwusd_uncollateralized = max(
            0.0,
            self.profit_bucket.exchange_rwusd_transferable_amount,
        )
        if (
            self.exit_state.stage == RWUSDExitStage.REDEEM_PENDING
            and self.exit_state.pending_redemption_submission_confirmed
        ):
            rwusd_uncollateralized = min(
                rwusd_uncollateralized,
                max(
                    0.0,
                    self.exit_state.pending_rwusd_balance_before
                    - self.exit_state.pending_redemption_amount,
                ),
            )
        exit_redemption_receivable = max(
            0.0,
            self.exit_state.expected_usdc - self.exit_state.confirmed_usdc,
        )
        exit_spot_usdc = (
            max(0.0, self.exit_state.spot_usdc_balance)
            if self.exit_state.redemption_requested_at_ms > 0
            else 0.0
        )
        strategy_equity = (
            portfolio_margin_equity
            + rwusd_uncollateralized
            + exit_redemption_receivable
            + exit_spot_usdc
            if self.live_rwusd_execution
            else simulated_equity
        )
        return {
            "mode": self.execution_environment,
            "execution_enabled": self.external_execution,
            "mainnet_execution_enabled": (
                self.external_execution
                and self.execution_environment
                == "binance_portfolio_margin_live"
            ),
            "rwusd_transfers_simulated": not self.live_rwusd_execution,
            "rwusd_shadow": not self.live_rwusd_execution,
            "rwusd_testnet_supported": False,
            "execution_boundary": (
                "Binance Portfolio Margin mainnet uses one BTC or ETH asset "
                "as direct collateral for its corresponding USDC hedge; "
                "confirmed Spot profit releases hedge restore before "
                "asynchronous RWUSD subscription and collateral confirmation"
                if self.live_rwusd_execution
                else (
                    "Binance USD-M Futures Demo requires shadow RWUSD "
                    "collateralization before USDC Maker orders; Portfolio "
                    "Margin and RWUSD transfers remain simulated"
                )
                if self.external_execution
                else (
                    "market/account reads only; orders and RWUSD transfers "
                    "are simulated"
                )
            ),
            "phase": self.state.phase.value,
            "selected_symbol": self.state.selected_symbol,
            "symbol": self.state.selected_symbol or "BTCUSDC",
            "selector": {
                "selected_symbol": self.selector_snapshot.selected_symbol,
                "cooldown_symbol": self.selector_snapshot.cooldown_symbol,
                "scores": [
                    item.to_dict() for item in self.selector_snapshot.scores
                ],
            },
            "hedge": {
                "requested_target_quantity": self.state.target_quantity,
                "target_quantity": self.state.target_quantity,
                "effective_hedge_quantity": self.effective_hedge_quantity(),
                "target_completion_ratio": target_completion_ratio,
                "hedge_gap_quantity": abs(
                    self.state.long_quantity - self.state.short_quantity
                ),
                "target_notional_per_side": (
                    self._external_target_notional
                    or (
                        self.state.target_quantity
                        * self.state.last_mark_price
                    )
                ),
                "opening_tranche_quantity": (
                    self._external_opening_tranche_quantity
                ),
                "opening_tranche_notional": (
                    self._external_opening_tranche_notional
                    or (
                        self._external_opening_tranche_quantity
                        * self.state.last_mark_price
                    )
                ),
                "opening_tranche_block_reason": (
                    self._external_opening_tranche_block_reason
                ),
                "long_quantity": self.state.long_quantity,
                "short_quantity": self.state.short_quantity,
                "long_entry_price": self.state.long_entry_price,
                "short_entry_price": self.state.short_entry_price,
                "long_unrealized_pnl": self.state.long_unrealized_pnl,
                "short_unrealized_pnl": self.state.short_unrealized_pnl,
                "long_roe_percent": (
                    long_harvest["position_roe"] * 100.0
                ),
                "short_roe_percent": (
                    short_harvest["position_roe"] * 100.0
                ),
                "total_unrealized_pnl": total_unrealized,
                "gross_position_notional": gross_position_notional,
                "total_unrealized_pnl_rate": total_unrealized_rate,
                "pending_rebalance_side": (
                    self.state.pending_rebalance_side.value
                    if self.state.pending_rebalance_side is not None
                    else None
                ),
                "pending_harvest_quantity": (
                    self.state.pending_harvest_quantity
                ),
                "pending_restore_quantity": (
                    self.state.pending_restore_quantity
                ),
                "deferred_restore_carry_side": (
                    self.state.deferred_restore_carry_side.value
                    if self.state.deferred_restore_carry_side is not None
                    else None
                ),
                "deferred_restore_carry_quantity": (
                    self.state.deferred_restore_carry_quantity
                ),
                "profit_harvest_rsi_confirmed": (
                    self.state.profit_harvest_rsi_confirmed
                ),
                "pending_harvest_trade_quantity": (
                    self.state.pending_harvest_trade_quantity
                ),
                "profit_sweep_started_at_ms": (
                    self.state.profit_sweep_started_at_ms
                ),
                "pending_repair_side": (
                    self.state.pending_repair_side.value
                    if self.state.pending_repair_side is not None
                    else None
                ),
                "pending_repair_quantity": (
                    self.state.pending_repair_quantity
                ),
                "repair_retry_after_cycle": (
                    self.state.repair_retry_after_cycle
                ),
                "take_profit_count": self.state.take_profit_count,
                "restore_count": self.state.restore_count,
                "opening_pair_failure_count": (
                    self.state.opening_pair_failure_count
                ),
                "opening_retry_after_cycle": (
                    self.state.opening_retry_after_cycle
                ),
                "opening_imbalance_tolerance": (
                    self.state.opening_imbalance_tolerance
                ),
                "repair_deferred_side": (
                    self.state.repair_deferred_side.value
                    if self.state.repair_deferred_side is not None
                    else None
                ),
                "repair_deferred_quantity": (
                    self.state.repair_deferred_quantity
                ),
                "repair_deferred_minimum_quantity": (
                    self.state.repair_deferred_minimum_quantity
                ),
                "repair_deferred_minimum_notional": (
                    self.state.repair_deferred_minimum_notional
                ),
                "repair_deferred_minimum_known": (
                    self.state.repair_deferred_minimum_known
                ),
                "risk_degraded": self.state.risk_degraded,
                "risk_gross_cap_quantity": (
                    self.state.risk_gross_cap_quantity
                ),
                "hard_risk_reduction_count": (
                    self.state.hard_risk_reduction_count
                ),
                "hard_risk_episode_reduction_count": (
                    self.state.hard_risk_episode_reduction_count
                ),
            },
            "harvest": {
                "trigger_roe": float(
                    getattr(
                        self.config,
                        "HARVEST_TRIGGER_ROE",
                        0.05,
                    )
                ),
                "trigger_roe_percent": float(
                    getattr(
                        self.config,
                        "HARVEST_TRIGGER_ROE",
                        0.05,
                    )
                )
                * 100.0,
                "tranche_fraction": float(
                    getattr(
                        self.config,
                        "HARVEST_TRANCHE_FRACTION",
                        0.05,
                    )
                ),
                "minimum_net_usd": float(
                    getattr(
                        self.config,
                        "HARVEST_MIN_NET_USD",
                        1.0,
                    )
                ),
                "cost_multiplier": float(
                    getattr(
                        self.config,
                        "HARVEST_COST_MULTIPLIER",
                        3.0,
                    )
                ),
                "next_slice_quantity": harvest_quantity,
                "long": long_harvest,
                "short": short_harvest,
            },
            "risk": {
                **self.last_risk_decision.to_dict(),
                "automatic_loss_side_reduction": (
                    "hard_risk_only"
                ),
                "risk_degraded": self.state.risk_degraded,
                "gross_cap_quantity": self.state.risk_gross_cap_quantity,
                "recovery_healthy_since_ms": (
                    self.state.risk_recovery_healthy_since_ms
                ),
                "current_drawdown": self.state.current_drawdown,
                "peak_equity": self.state.peak_equity,
                "snapshot": self.last_portfolio_snapshot.to_dict(),
            },
            "valuation": {
                "portfolio_margin_equity": portfolio_margin_equity,
                "rwusd_uncollateralized": rwusd_uncollateralized,
                "rwusd_transferable_amount": (
                    self.profit_bucket.exchange_rwusd_transferable_amount
                ),
                "exit_redemption_receivable": (
                    exit_redemption_receivable
                ),
                "exit_redeemed_usdc": max(
                    0.0,
                    self.exit_state.confirmed_usdc,
                ),
                "exit_spot_usdc": exit_spot_usdc,
                "exit_redemption_fee_usdc": (
                    self.exit_state.redemption_fee_usdc
                ),
                "strategy_equity": strategy_equity,
                "source": (
                    "binance_pm_plus_official_transferable_rwusd"
                    if self.live_rwusd_execution
                    else "rwusd_shadow_ledger"
                ),
            },
            "exit": self.exit_status(),
            "profit_bucket": self.profit_bucket.to_dict(),
            "execution_cost_bps_per_order": self._execution_cost_bps,
            "orders": orders,
            "dry_run_orders": orders,
            "simulated_equity": simulated_equity,
            "last_mark_price": self.state.last_mark_price,
            "last_action": self.state.last_action,
            "last_error": self.state.last_error,
            "last_step_at": self._last_step_at,
        }
