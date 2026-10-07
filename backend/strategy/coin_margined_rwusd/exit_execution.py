"""Exchange advancement for RWUSD liquidation, redemption, and risk reduction."""

from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass, replace
from typing import Mapping, Optional

from backend.strategy.coin_margined_rwusd.exit import (
    MONEY_EPSILON,
    QUANTITY_EPSILON,
    RWUSDExitStage,
    build_reduction_plan,
    hard_risk_reduction_fraction,
)
from backend.strategy.coin_margined_rwusd.exit_orders import RWUSDCustomerExitOrders
from backend.strategy.coin_margined_rwusd.exit_settlement import RWUSDExitSettlement


@dataclass(frozen=True)
class RedemptionBatch:
    redemption_type: str
    amount: float
    expected_usdc: float
    expected_fee_usdc: float
    reason: str = ""

    @property
    def executable(self) -> bool:
        return self.amount > MONEY_EPSILON


class RWUSDExitExecution:
    """Advance one persistent exit state without blocking the trading loop."""

    def __init__(
        self,
        *,
        owner: object,
        client: object,
        engine: object,
        config: object,
    ) -> None:
        self.owner = owner
        self.client = client
        self.engine = engine
        self.config = config
        self._customer_orders = RWUSDCustomerExitOrders(
            owner=owner,
            engine=engine,
            config=config,
        )
        self._settlement = RWUSDExitSettlement(
            owner=owner,
            client=client,
            engine=engine,
            config=config,
        )

    async def advance_customer_exit(
        self,
        *,
        finalize_flat: bool = True,
    ) -> list[dict]:
        now_ms = int(time.time() * 1000)
        state = self.engine.exit_state
        if state.retry_after_ms > now_ms:
            return []
        try:
            if state.stage == RWUSDExitStage.LIQUIDATING:
                return await self._advance_liquidation(
                    now_ms,
                    finalize_flat=finalize_flat,
                )
            if state.stage in {
                RWUSDExitStage.REDEEMING,
                RWUSDExitStage.REDEEM_PENDING,
            }:
                return await self._advance_redemption(now_ms)
            return []
        except Exception as exc:
            state.record_failure(
                now_ms=now_ms,
                error=exc,
                base_retry_ms=self._milliseconds("EXIT_RETRY_BASE_SEC", 2.0),
                maximum_retry_ms=self._milliseconds(
                    "EXIT_RETRY_MAX_SEC",
                    60.0,
                ),
            )
            self.engine.state.last_error = state.last_error
            self.engine.persist()
            return [
                {
                    "type": "rwusd_exit",
                    "action": "exit_retry_scheduled",
                    "stage": state.stage.value,
                    "reason": state.last_error,
                    "retry_after_ms": state.retry_after_ms,
                    "simulated": False,
                }
            ]

    async def advance_hard_risk_reduction(self) -> list[dict]:
        decision = self.engine.last_risk_decision
        cap = self.engine.risk_target_quantity_cap()
        larger_side = max(
            self.engine.state.long_quantity,
            self.engine.state.short_quantity,
        )
        degraded_excess = (
            cap is not None and larger_side > cap + 1e-10
        )
        if self.engine.exit_state.reopen_locked:
            return []
        if not decision.hard_risk and not degraded_excess:
            return []

        events = (
            await self.owner._cancel_managed_active_orders(
                allowed_purposes={"hard_risk_reduction"},
            )
            if degraded_excess and not decision.hard_risk
            else await self.owner.cancel_risk_blocked_orders(
                reference_price=self.engine.state.last_mark_price,
            )
        )
        await self.owner.reconcile_positions()
        if self.engine.lifecycle.active_orders:
            return events

        symbol = self._selected_symbol()
        if not symbol:
            return events
        order_book = await self.client.get_order_book(symbol, limit=100)
        long_quantity = self.engine.state.long_quantity
        short_quantity = self.engine.state.short_quantity
        imbalance = long_quantity - short_quantity
        asymmetric_repair = abs(imbalance) > QUANTITY_EPSILON
        total_quantity = (
            long_quantity + short_quantity
        )
        fraction = (
            min(
                1.0,
                (
                    max(
                        0.0,
                        self.engine.state.long_quantity - float(cap or 0),
                    )
                    + max(
                        0.0,
                        self.engine.state.short_quantity - float(cap or 0),
                    )
                )
                / max(MONEY_EPSILON, total_quantity),
            )
            if degraded_excess
            and not decision.hard_risk
            else hard_risk_reduction_fraction(
                uni_mmr=self.engine.last_portfolio_snapshot.uni_mmr,
                target_uni_mmr=float(
                    getattr(
                        self.config,
                        "HARD_RISK_TARGET_UNIMMR",
                        5.0,
                    )
                ),
                minimum_fraction=float(
                    getattr(
                        self.config,
                        "HARD_RISK_MIN_REDUCTION_FRACTION",
                        0.10,
                    )
                ),
                maximum_fraction=float(
                    getattr(
                        self.config,
                        "HARD_RISK_MAX_REDUCTION_FRACTION",
                        0.25,
                    )
                ),
            )
        )
        plan_long_quantity = long_quantity
        plan_short_quantity = short_quantity
        if asymmetric_repair:
            # A partial bilateral reduction can leave one side larger. Repair
            # only that excess before starting another bilateral batch.
            if imbalance > 0:
                plan_short_quantity = 0.0
            else:
                plan_long_quantity = 0.0
            fraction = min(
                1.0,
                abs(imbalance)
                / max(
                    MONEY_EPSILON,
                    max(plan_long_quantity, plan_short_quantity),
                ),
            )
        plan = build_reduction_plan(
            long_quantity=plan_long_quantity,
            short_quantity=plan_short_quantity,
            order_book=order_book,
            desired_fraction=fraction,
            max_slippage_bps=float(
                getattr(
                    self.config,
                    "HARD_RISK_MAX_SLIPPAGE_BPS",
                    5.0,
                )
            ),
            depth_utilization=float(
                getattr(
                    self.config,
                    "HARD_RISK_DEPTH_UTILIZATION",
                    0.10,
                )
            ),
            max_notional_per_side=float(
                getattr(
                    self.config,
                    "HARD_RISK_MAX_BATCH_NOTIONAL_PER_SIDE",
                    100_000.0,
                )
            ),
        )
        if not plan.executable:
            if (
                long_quantity > 1e-12
                or short_quantity > 1e-12
            ):
                raise RuntimeError(
                    "hard_risk_reduction_blocked:"
                    f"{'asymmetric_pair_repair' if asymmetric_repair else plan.reason}"
                )
            return events

        before_long = self.engine.state.long_quantity
        before_short = self.engine.state.short_quantity
        events.extend(
            self.engine.plan_external_reduction(
                plan=plan,
                purpose="hard_risk_reduction",
                mark_price=self._mark_price(order_book),
            )
        )
        events.extend(
            await self.owner.execute_pending_orders(
                reference_price=self._mark_price(order_book),
            )
        )
        await self.owner.reconcile_positions()
        reduced_quantity = (
            before_long
            + before_short
            - self.engine.state.long_quantity
            - self.engine.state.short_quantity
        )
        if (
            reduced_quantity <= QUANTITY_EPSILON
            and not self.engine.lifecycle.active_orders
        ):
            raise RuntimeError(
                "hard_risk_reduction_confirmed_without_fill"
            )
        if not self.engine.lifecycle.active_orders:
            self.engine.finalize_external_reduction(
                "hard_risk_reduction"
            )
        return events

    async def _advance_liquidation(
        self,
        now_ms: int,
        *,
        finalize_flat: bool,
    ) -> list[dict]:
        await self.owner.validate_runtime_isolation()
        events = await self.owner._cancel_managed_active_orders(
            allowed_purposes={
                "customer_exit",
                "customer_exit_residual",
            },
        )
        await self.owner.reconcile_positions()
        events.extend(await self._customer_orders.reconcile(now_ms))
        await self.owner.reconcile_positions()
        pending_exit_orders = [
            order
            for order in self.engine.lifecycle.active_orders
            if (
                order.purpose
                in {"customer_exit", "customer_exit_residual"}
                and not order.external_client_order_id
            )
        ]
        if pending_exit_orders:
            symbol = self._selected_symbol()
            if not symbol:
                raise RuntimeError("customer_exit_symbol_unavailable")
            order_book = await self.client.get_order_book(symbol, limit=100)
            events.extend(
                await self.owner.execute_pending_orders(
                    reference_price=self._mark_price(order_book),
                )
            )
            await self.owner.reconcile_positions()
        if self.engine.lifecycle.active_orders:
            return events

        symbol = self._selected_symbol()
        if not symbol:
            raise RuntimeError("customer_exit_symbol_unavailable")
        open_orders = list(
            await self.client.get_um_open_orders(symbol=symbol) or []
        )
        if open_orders:
            raise RuntimeError(
                "customer_exit_waiting_for_order_cancellation"
            )
        if self._is_flat():
            if not finalize_flat:
                return events
            events.extend(await self._complete_customer_liquidation())
            return events

        order_book = await self.client.get_order_book(symbol, limit=100)
        mark_price = self._mark_price(order_book)
        plan = build_reduction_plan(
            long_quantity=self.engine.state.long_quantity,
            short_quantity=self.engine.state.short_quantity,
            order_book=order_book,
            desired_fraction=float(
                getattr(self.config, "EXIT_BATCH_FRACTION", 0.10)
            ),
            max_slippage_bps=float(
                getattr(self.config, "EXIT_MAX_SLIPPAGE_BPS", 8.0)
            ),
            depth_utilization=float(
                getattr(self.config, "EXIT_DEPTH_UTILIZATION", 0.20)
            ),
            max_notional_per_side=float(
                getattr(
                    self.config,
                    "EXIT_MAX_BATCH_NOTIONAL_PER_SIDE",
                    250_000.0,
                )
            ),
        )
        if not plan.executable:
            raise RuntimeError(f"customer_exit_blocked:{plan.reason}")
        limit_plan, residual_plan = await self._customer_orders.partition_plan(
            symbol=symbol,
            plan=plan,
            long_position=self.engine.state.long_quantity,
            short_position=self.engine.state.short_quantity,
            order_book=order_book,
        )
        purpose = "customer_exit"
        if limit_plan.executable:
            plan = limit_plan
        elif residual_plan.executable:
            plan = residual_plan
            purpose = "customer_exit_residual"
        else:
            raise RuntimeError("customer_exit_official_order_unavailable")

        before_long = self.engine.state.long_quantity
        before_short = self.engine.state.short_quantity
        planned = self.engine.plan_external_reduction(
            plan=plan,
            purpose=purpose,
            mark_price=mark_price,
        )
        if planned:
            self.engine.exit_state.liquidation_batch_count += 1
            self.engine.exit_state.liquidation_order_count += len(
                planned[0].get("order_ids") or []
            )
            self.engine.exit_state.record_progress(now_ms)
            self.engine.persist()
            events.extend(planned)
        events.extend(
            await self.owner.execute_pending_orders(
                reference_price=mark_price,
            )
        )
        await self.owner.reconcile_positions()
        reduced_quantity = (
            before_long
            + before_short
            - self.engine.state.long_quantity
            - self.engine.state.short_quantity
        )
        if (
            reduced_quantity <= 1e-12
            and not self.engine.lifecycle.active_orders
        ):
            raise RuntimeError("customer_exit_batch_confirmed_without_fill")
        if (
            not self.engine.lifecycle.active_orders
            and self._is_flat()
            and finalize_flat
        ):
            events.extend(await self._complete_customer_liquidation())
        return events

    async def _complete_customer_liquidation(self) -> list[dict]:
        events = list(await self.owner.reconcile_user_trades())
        self.engine.finalize_external_reduction("customer_exit")
        pnl_event = self.engine.settle_external_exit_pnl()
        if pnl_event is not None:
            events.append(pnl_event)
        events.append(
            {
                "type": "rwusd_exit",
                "action": "liquidation_complete",
                "stage": self.engine.exit_state.stage.value,
                "simulated": False,
            }
        )
        return events

    async def _advance_redemption(self, now_ms: int) -> list[dict]:
        if not self.owner.live_rwusd:
            raise RuntimeError(
                "RWUSD redemption is available only on mainnet"
            )
        await self._assert_exchange_flat()
        if self.engine.exit_state.stage == RWUSDExitStage.REDEEM_PENDING:
            return await self._reconcile_pending_redemption(now_ms)

        if (
            self.engine.exit_state.pending_collateral_transfer_amount
            > MONEY_EPSILON
        ):
            self.engine.exit_state.clear_zero_balance()
            return await self._reconcile_pending_collateral_transfer(now_ms)

        snapshot, events, handled = await self._settlement.prepare(now_ms)
        if handled:
            return events
        if snapshot is None:
            raise RuntimeError("customer_exit_usdc_snapshot_unavailable")

        transferable = await self.client.get_rwusd_exit_transferable_balance()
        transferable_amount = _floor_asset_amount(
            float((transferable or {}).get("amount", 0) or 0)
        )
        dust = self._rwusd_dust()
        if transferable_amount > dust:
            state = self.engine.exit_state
            state.clear_zero_balance()
            state.begin_collateral_transfer(
                now_ms=now_ms,
                amount=transferable_amount,
                balance_before=transferable_amount,
            )
            self.engine.persist()
            try:
                await self.client.transfer_rwusd_from_portfolio_margin(
                    amount=transferable_amount,
                )
            except Exception as exc:
                state.record_failure(
                    now_ms=now_ms,
                    error=f"rwusd_collateral_return_uncertain:{exc}",
                    base_retry_ms=self._milliseconds(
                        "EXIT_REDEMPTION_HISTORY_POLL_SEC",
                        15.0,
                    ),
                    maximum_retry_ms=self._milliseconds(
                        "EXIT_REDEMPTION_HISTORY_POLL_SEC",
                        15.0,
                    ),
                )
                self.engine.persist()
                return events + [
                    {
                        "type": "rwusd_exit",
                        "action": "collateral_return_uncertain",
                        "amount": transferable_amount,
                        "retry_after_ms": state.retry_after_ms,
                        "reason": str(exc),
                        "simulated": False,
                    }
                ]
            state.confirm_collateral_transfer_submission(now_ms)
            self._defer(
                now_ms,
                self._milliseconds(
                    "EXIT_REDEMPTION_HISTORY_POLL_SEC",
                    15.0,
                ),
                None,
            )
            self.engine.persist()
            return events + [
                {
                    "type": "rwusd_exit",
                    "action": "collateral_return_submitted",
                    "amount": transferable_amount,
                    "stage": state.stage.value,
                    "simulated": False,
                }
            ]

        account, earn_balance, account_info = await asyncio.gather(
            self.client.get_rwusd_account(),
            self.client.get_rwusd_transferable_balance(),
            self.client.get_account_info(),
        )
        self.engine.update_exchange_rwusd_snapshot(account)
        rwusd_amount = _floor_asset_amount(
            float((account or {}).get("rwusdAmount", 0) or 0)
        )
        redeemable_amount = _floor_asset_amount(
            float((earn_balance or {}).get("amount", 0) or 0)
        )
        self._apply_exit_valuation(account_info, earn_balance)
        state = self.engine.exit_state
        if redeemable_amount <= dust and rwusd_amount <= dust:
            final_events, final_handled = (
                await self._settlement.advance_final_cash(
                    now_ms=now_ms,
                    snapshot=snapshot,
                )
            )
            events.extend(final_events)
            if final_handled:
                return events
            state.mark_zero_balance(now_ms)
            stable_ms = self._milliseconds("EXIT_ZERO_CONFIRM_SEC", 30.0)
            if now_ms - state.zero_balance_since_ms >= stable_ms:
                state.mark_complete(now_ms)
                self.engine.state.last_action = "customer_exit_complete"
                self.engine.persist()
                return events + [
                    {
                        "type": "rwusd_exit",
                        "action": "redemption_complete",
                        "stage": state.stage.value,
                        "confirmed_usdc": state.confirmed_usdc,
                        "spot_usdc": state.spot_usdc_balance,
                        "net_usdc_after_liabilities": (
                            state.spot_usdc_balance
                            + state.signed_usdc_wallet_balance
                        ),
                        "simulated": False,
                    }
                ]
            self._defer(
                now_ms,
                min(5_000, stable_ms),
                "rwusd_zero_balance_confirmation",
            )
            return events

        state.clear_zero_balance()
        if redeemable_amount <= dust:
            self._defer(
                now_ms,
                self._milliseconds(
                    "EXIT_REDEMPTION_HISTORY_POLL_SEC",
                    15.0,
                ),
                "rwusd_collateral_locked_for_settlement",
            )
            self.engine.persist()
            return events

        quota = await self.client.get_rwusd_quota()
        batch = select_redemption_batch(
            rwusd_amount=redeemable_amount,
            quota=quota,
            preference=getattr(
                self.config,
                "EXIT_REDEMPTION_PREFERENCE",
                ("STANDARD", "FAST"),
            ),
        )
        if not batch.executable:
            self._defer(
                now_ms,
                self._milliseconds(
                    "EXIT_REDEMPTION_QUOTA_RETRY_SEC",
                    5 * 60.0,
                ),
                batch.reason or "rwusd_redemption_quota_unavailable",
            )
            return events

        recent_history = await self.client.get_rwusd_redemption_history(
            start_time_ms=max(1, now_ms - 60_000),
            end_time_ms=now_ms + 60_000,
        )
        state.last_redemption_history_time_ms = max(
            state.last_redemption_history_time_ms,
            _latest_redemption_history_time(recent_history),
        )
        state.mark_redemption_pending(
            now_ms=now_ms,
            redemption_type=batch.redemption_type,
            amount=batch.amount,
            expected_usdc=batch.expected_usdc,
            fee_usdc=batch.expected_fee_usdc,
            rwusd_balance_before=rwusd_amount,
            spot_usdc_baseline=snapshot.spot_total,
        )
        self.engine.persist()
        try:
            response = await self.client.redeem_rwusd(
                amount=batch.amount,
                redeem_type=batch.redemption_type,
            )
        except Exception as exc:
            state.record_failure(
                now_ms=now_ms,
                error=f"rwusd_redemption_submission_uncertain:{exc}",
                base_retry_ms=self._milliseconds(
                    "EXIT_REDEMPTION_HISTORY_POLL_SEC",
                    15.0,
                ),
                maximum_retry_ms=self._milliseconds(
                    "EXIT_REDEMPTION_HISTORY_POLL_SEC",
                    15.0,
                ),
            )
            self.engine.persist()
            return events + [
                {
                    "type": "rwusd_exit",
                    "action": "redemption_submission_uncertain",
                    "redemption_type": batch.redemption_type,
                    "amount": batch.amount,
                    "retry_after_ms": state.retry_after_ms,
                    "reason": str(exc),
                    "simulated": False,
                }
            ]

        expected_usdc, fee_usdc, arrival_time_ms = _redemption_terms(
            response,
            amount=batch.amount,
            fallback_expected_usdc=batch.expected_usdc,
            fallback_fee_usdc=batch.expected_fee_usdc,
        )
        state.confirm_redemption_submission(
            now_ms,
            expected_usdc=expected_usdc,
            fee_usdc=fee_usdc,
            arrival_time_ms=arrival_time_ms,
        )
        self._defer(
            now_ms,
            self._redemption_poll_delay_ms(now_ms, arrival_time_ms),
            None,
        )
        self.engine.persist()
        return events + [
            {
                "type": "rwusd_exit",
                "action": "redemption_submitted",
                "redemption_type": batch.redemption_type,
                "amount": batch.amount,
                "expected_usdc": expected_usdc,
                "expected_fee_usdc": fee_usdc,
                "arrival_time_ms": arrival_time_ms,
                "simulated": False,
            }
        ]

    async def _reconcile_pending_collateral_transfer(
        self,
        now_ms: int,
    ) -> list[dict]:
        state = self.engine.exit_state
        transferable = await self.client.get_rwusd_exit_transferable_balance()
        current = _floor_asset_amount(
            float((transferable or {}).get("amount", 0) or 0)
        )
        expected_after = max(
            0.0,
            state.pending_collateral_transfer_balance_before
            - state.pending_collateral_transfer_amount,
        )
        if current <= expected_after + MONEY_EPSILON:
            amount = state.confirm_collateral_transfer(now_ms)
            account_info, earn_balance = await asyncio.gather(
                self.client.get_account_info(),
                self.client.get_rwusd_transferable_balance(),
            )
            self._apply_exit_valuation(account_info, earn_balance)
            self.engine.persist()
            return [
                {
                    "type": "rwusd_exit",
                    "action": "collateral_return_confirmed",
                    "amount": amount,
                    "stage": state.stage.value,
                    "simulated": False,
                }
            ]

        uncertain_timeout_ms = self._milliseconds(
            "EXIT_REDEMPTION_UNCERTAIN_RETRY_SEC",
            10 * 60.0,
        )
        if (
            not state.pending_collateral_transfer_submission_confirmed
            and now_ms - state.pending_collateral_transfer_started_at_ms
            >= uncertain_timeout_ms
        ):
            amount = state.pending_collateral_transfer_amount
            state.abandon_unconfirmed_collateral_transfer(now_ms)
            self.engine.persist()
            return [
                {
                    "type": "rwusd_exit",
                    "action": (
                        "collateral_return_unconfirmed_released_for_retry"
                    ),
                    "amount": amount,
                    "simulated": False,
                }
            ]

        self._defer(
            now_ms,
            self._milliseconds(
                "EXIT_REDEMPTION_HISTORY_POLL_SEC",
                15.0,
            ),
            (
                "rwusd_collateral_return_confirmation_pending"
                if state.pending_collateral_transfer_submission_confirmed
                else "rwusd_collateral_return_submission_pending"
            ),
        )
        self.engine.persist()
        return []

    async def _reconcile_pending_redemption(
        self,
        now_ms: int,
    ) -> list[dict]:
        state = self.engine.exit_state
        start_ms = max(
            1,
            state.pending_redemption_started_at_ms - 60_000,
        )
        (
            history,
            account,
            spot,
            earn_balance,
            account_info,
        ) = await asyncio.gather(
            self.client.get_rwusd_redemption_history(
                start_time_ms=start_ms,
                end_time_ms=now_ms + 60_000,
            ),
            self.client.get_rwusd_account(),
            self.client.get_spot_asset_balance("USDC"),
            self.client.get_rwusd_transferable_balance(),
            self.client.get_account_info(),
        )
        self.engine.update_exchange_rwusd_snapshot(account)
        state.spot_usdc_balance = _spot_asset_total(spot)
        row = _matching_redemption_row(
            history,
            amount=state.pending_redemption_amount,
            started_at_ms=state.pending_redemption_started_at_ms,
            after_time_ms=state.last_redemption_history_time_ms,
            matched_time_ms=state.pending_redemption_history_time_ms,
        )
        current_rwusd = _floor_asset_amount(
            float((account or {}).get("rwusdAmount", 0) or 0)
        )
        self._apply_exit_valuation(account_info, earn_balance)
        balance_decreased = (
            current_rwusd
            <= state.pending_rwusd_balance_before
            - state.pending_redemption_amount
            + MONEY_EPSILON
        )
        row_terms = (
            _redemption_terms(
                row,
                amount=state.pending_redemption_amount,
                fallback_expected_usdc=state.pending_expected_usdc,
                fallback_fee_usdc=state.pending_fee_usdc,
            )
            if row is not None
            else None
        )
        row_time_ms = (
            int(row.get("time", 0) or 0)
            if row is not None
            else None
        )
        if row is not None and _redemption_row_failed(row):
            if balance_decreased:
                raise RuntimeError(
                    "RWUSD redemption history failed after balance decreased"
                )
            status = str(row.get("status") or "").strip().upper()
            state.fail_pending_redemption(
                now_ms,
                f"rwusd_redemption_failed:{status or 'UNKNOWN'}",
            )
            self._defer(
                now_ms,
                self._milliseconds(
                    "EXIT_REDEMPTION_QUOTA_RETRY_SEC",
                    5 * 60.0,
                ),
                state.last_error,
            )
            self.engine.persist()
            return [
                {
                    "type": "rwusd_exit",
                    "action": "redemption_failed",
                    "status": status,
                    "retry_after_ms": state.retry_after_ms,
                    "simulated": False,
                }
            ]
        if (
            not state.pending_redemption_submission_confirmed
            and (
                balance_decreased
                or (
                    row is not None
                    and _redemption_row_acknowledged(row)
                )
            )
        ):
            state.confirm_redemption_submission(
                now_ms,
                expected_usdc=(
                    row_terms[0] if row_terms is not None else None
                ),
                fee_usdc=(
                    row_terms[1] if row_terms is not None else None
                ),
                arrival_time_ms=(
                    row_terms[2] if row_terms is not None else None
                ),
                history_time_ms=row_time_ms,
            )
        elif (
            row_terms is not None
            and row is not None
            and _redemption_row_acknowledged(row)
        ):
            state.confirm_redemption_submission(
                now_ms,
                expected_usdc=row_terms[0],
                fee_usdc=row_terms[1],
                arrival_time_ms=row_terms[2],
                history_time_ms=row_time_ms,
            )

        if row is not None and _redemption_row_succeeded(row):
            receive_amount = row_terms[0] if row_terms is not None else 0.0
            arrival_ms = row_terms[2] if row_terms is not None else 0
            spot_total = _spot_asset_total(spot)
            spot_arrived = (
                receive_amount > 0
                and spot_total + MONEY_EPSILON
                >= state.spot_usdc_baseline + receive_amount
            )
            if (
                state.pending_redemption_submission_confirmed
                and arrival_ms <= now_ms
                and spot_arrived
            ):
                redemption_type = state.pending_redemption_type
                redeemed_amount = state.pending_redemption_amount
                state.confirm_pending_redemption(
                    now_ms=now_ms,
                    confirmed_usdc=receive_amount,
                )
                self.engine.state.last_action = (
                    "customer_redemption_batch_confirmed"
                )
                self.engine.persist()
                return [
                    {
                        "type": "rwusd_exit",
                        "action": "redemption_confirmed",
                        "redemption_type": redemption_type,
                        "amount": redeemed_amount,
                        "confirmed_usdc": receive_amount,
                        "stage": state.stage.value,
                        "simulated": False,
                    }
                ]
            if arrival_ms > now_ms:
                self._defer(
                    now_ms,
                    self._redemption_poll_delay_ms(now_ms, arrival_ms),
                    "rwusd_redemption_arrival_pending",
                )
                self.engine.persist()
                return []

        uncertain_timeout_ms = self._milliseconds(
            "EXIT_REDEMPTION_UNCERTAIN_RETRY_SEC",
            10 * 60.0,
        )
        if (
            not state.pending_redemption_submission_confirmed
            and not balance_decreased
            and row is None
            and now_ms - state.pending_redemption_started_at_ms
            >= uncertain_timeout_ms
        ):
            amount = state.pending_redemption_amount
            state.abandon_unconfirmed_redemption(now_ms)
            self.engine.persist()
            return [
                {
                    "type": "rwusd_exit",
                    "action": "redemption_unconfirmed_released_for_retry",
                    "amount": amount,
                    "simulated": False,
                }
            ]

        self._defer(
            now_ms,
            self._milliseconds(
                "EXIT_REDEMPTION_HISTORY_POLL_SEC",
                15.0,
            ),
            (
                "rwusd_redemption_arrival_pending"
                if state.pending_redemption_submission_confirmed
                else "rwusd_redemption_submission_confirmation_pending"
            ),
        )
        self.engine.persist()
        return []

    def _redemption_poll_delay_ms(
        self,
        now_ms: int,
        arrival_time_ms: int,
    ) -> int:
        base_delay = self._milliseconds(
            "EXIT_REDEMPTION_HISTORY_POLL_SEC",
            15.0,
        )
        arrival_delay = max(
            0,
            int(arrival_time_ms or 0) - max(1, int(now_ms)),
        )
        return max(base_delay, arrival_delay)

    async def _assert_exchange_flat(self) -> None:
        await self.owner.validate_runtime_isolation()
        await self.owner.reconcile_positions()
        if not self._is_flat():
            raise RuntimeError(
                "RWUSD redemption blocked until positions are flat"
            )
        if self.engine.lifecycle.active_orders:
            raise RuntimeError(
                "RWUSD redemption blocked until managed orders are terminal"
            )
        if list(await self.client.get_um_open_orders() or []):
            raise RuntimeError(
                "RWUSD redemption blocked until exchange orders are zero"
            )

    def _is_flat(self) -> bool:
        return (
            self.engine.state.long_quantity <= 1e-10
            and self.engine.state.short_quantity <= 1e-10
        )

    def _selected_symbol(self) -> str:
        return str(
            self.engine.state.selected_symbol or ""
        ).strip().upper()

    def _mark_price(self, order_book: Mapping[str, object]) -> float:
        bids = list(order_book.get("bids") or [])
        asks = list(order_book.get("asks") or [])
        best_bid = float(bids[0][0]) if bids else 0.0
        best_ask = float(asks[0][0]) if asks else 0.0
        if best_bid > 0 and best_ask > 0:
            return (best_bid + best_ask) / 2.0
        mark = max(0.0, float(self.engine.state.last_mark_price or 0))
        if mark <= 0:
            raise RuntimeError("RWUSD reduction mark price is unavailable")
        return mark

    def _rwusd_dust(self) -> float:
        return max(
            MONEY_EPSILON,
            float(getattr(self.config, "EXIT_RWUSD_DUST", MONEY_EPSILON)),
        )

    def _apply_exit_valuation(
        self,
        account_info: object,
        earn_balance: object,
    ) -> None:
        account = account_info if isinstance(account_info, Mapping) else {}
        snapshot = self.engine.last_portfolio_snapshot
        self.engine.last_portfolio_snapshot = replace(
            snapshot,
            account_equity=max(
                0.0,
                float(account.get("equity", snapshot.account_equity) or 0),
            ),
            available_balance=max(
                0.0,
                float(
                    account.get(
                        "available",
                        snapshot.available_balance,
                    )
                    or 0
                ),
            ),
            withdrawable_balance=max(
                0.0,
                float(
                    account.get(
                        "withdrawable",
                        snapshot.withdrawable_balance,
                    )
                    or 0
                ),
            ),
        )
        self.engine.update_exchange_rwusd_transferable_snapshot(earn_balance)

    def _defer(
        self,
        now_ms: int,
        delay_ms: int,
        reason: Optional[str],
    ) -> None:
        state = self.engine.exit_state
        state.retry_after_ms = max(1, int(now_ms)) + max(1, int(delay_ms))
        state.last_error = reason
        self.engine.state.last_error = reason

    def _milliseconds(self, name: str, fallback_sec: float) -> int:
        return max(
            1,
            int(
                max(
                    0.001,
                    float(getattr(self.config, name, fallback_sec)),
                )
                * 1000
            ),
        )


def select_redemption_batch(
    *,
    rwusd_amount: float,
    quota: object,
    preference: object,
) -> RedemptionBatch:
    data = quota if isinstance(quota, Mapping) else {}
    amount = _floor_asset_amount(rwusd_amount)
    if data.get("redeemEnable") is False:
        return RedemptionBatch(
            redemption_type="",
            amount=0.0,
            expected_usdc=0.0,
            expected_fee_usdc=0.0,
            reason="rwusd_redemption_disabled",
        )
    configured = (
        (preference,)
        if isinstance(preference, str)
        else tuple(preference or ())
    )
    ordered = []
    for item in configured + ("STANDARD", "FAST"):
        normalized = str(item or "").strip().upper()
        if normalized in {"STANDARD", "FAST"} and normalized not in ordered:
            ordered.append(normalized)

    routes = []
    for redemption_type in ordered:
        details = _redemption_quota_details(data, redemption_type)
        if details is None:
            continue
        left_quota, fee_rate, minimum, free_quota = details
        routes.append(
            (
                redemption_type,
                left_quota,
                fee_rate,
                minimum,
                free_quota,
            )
        )
    minimums = [route[3] for route in routes]
    minimum_remaining = min(minimums) if minimums else 0.0
    for redemption_type, left_quota, fee_rate, minimum, free_quota in routes:
        if amount + MONEY_EPSILON < minimum:
            continue
        batch_amount = _floor_asset_amount(min(amount, left_quota))
        if batch_amount + MONEY_EPSILON < minimum:
            continue
        remaining = _floor_asset_amount(amount - batch_amount)
        if (
            remaining > MONEY_EPSILON
            and remaining + MONEY_EPSILON < minimum_remaining
        ):
            batch_amount = _floor_asset_amount(amount - minimum_remaining)
            if (
                batch_amount + MONEY_EPSILON < minimum
                or batch_amount > left_quota + MONEY_EPSILON
            ):
                continue
        fee_base = batch_amount
        if redemption_type == "FAST":
            fee_base = max(0.0, batch_amount - free_quota)
        fee = min(batch_amount, fee_base * fee_rate)
        return RedemptionBatch(
            redemption_type=redemption_type,
            amount=batch_amount,
            expected_usdc=max(0.0, batch_amount - fee),
            expected_fee_usdc=fee,
        )
    if minimums and amount + MONEY_EPSILON < min(minimums):
        return RedemptionBatch(
            redemption_type="",
            amount=0.0,
            expected_usdc=0.0,
            expected_fee_usdc=0.0,
            reason=f"rwusd_balance_below_redeem_minimum:{amount}",
        )
    return RedemptionBatch(
        redemption_type="",
        amount=0.0,
        expected_usdc=0.0,
        expected_fee_usdc=0.0,
        reason=(
            "rwusd_redemption_daily_quota_exhausted"
            if routes
            else "rwusd_redemption_quota_invalid"
        ),
    )


def _redemption_terms(
    payload: object,
    *,
    amount: float,
    fallback_expected_usdc: float,
    fallback_fee_usdc: float,
) -> tuple[float, float, int]:
    data = payload if isinstance(payload, Mapping) else {}
    redeemed_amount = max(0.0, float(amount or 0))
    expected = _first_numeric(data, ("receiveAmount",))
    fee = _first_numeric(data, ("fee",))
    arrival_time_ms = _first_nonnegative_int(data, ("arrivalTime",))
    if expected is None:
        expected = max(0.0, float(fallback_expected_usdc or 0))
    if fee is None:
        fee = max(0.0, float(fallback_fee_usdc or 0))
    if (
        expected > redeemed_amount + MONEY_EPSILON
        or fee > redeemed_amount + MONEY_EPSILON
        or expected + fee > redeemed_amount + MONEY_EPSILON
    ):
        raise RuntimeError("RWUSD redemption terms exceed submitted amount")
    return expected, fee, arrival_time_ms or 0


def _redemption_quota_details(
    quota: Mapping[str, object],
    redemption_type: str,
) -> Optional[tuple[float, float, float, float]]:
    prefix = redemption_type.lower()
    nested = quota.get(f"{prefix}RedemptionQuota")
    details = nested if isinstance(nested, Mapping) else {}
    left_quota = _first_numeric(
        details,
        ("leftQuota", "available", "quota"),
    )
    fee_rate = _first_numeric(
        details,
        ("fee", "dailyRedemptionFeeRate", "redemptionFeeRate", "feeRate"),
    )
    minimum = _first_numeric(
        details,
        ("minimum", "minimumRedemptionAmount", "redeemMinAmount"),
    )
    if left_quota is None:
        left_quota = _first_numeric(
            quota,
            (f"{prefix}RedemptionLeftQuota",),
        )
    if fee_rate is None:
        fee_rate = _first_numeric(
            quota,
            (f"{prefix}RedemptionFeeRate",),
        )
    if minimum is None:
        minimum = _first_numeric(quota, ("redeemMinAmount",))
    if (
        left_quota is None
        or fee_rate is None
        or fee_rate > 1.0
        or minimum is None
    ):
        return None
    free_quota = (
        _first_numeric(
            details,
            ("freeQuota", "leftFreeQuota"),
        )
        if redemption_type == "FAST"
        else 0.0
    )
    if redemption_type == "FAST" and free_quota is None:
        free_quota = _first_numeric(
            quota,
            ("fastRedemptionLeftFreeQuota",),
        )
    return (
        max(0.0, left_quota),
        max(0.0, fee_rate),
        max(MONEY_EPSILON, minimum),
        max(0.0, free_quota or 0.0),
    )


def _first_numeric(
    payload: Mapping[str, object],
    keys: tuple[str, ...],
) -> Optional[float]:
    for key in keys:
        if key not in payload or payload.get(key) in (None, ""):
            continue
        try:
            value = float(payload[key])
        except (TypeError, ValueError):
            return None
        if math.isfinite(value) and value >= 0:
            return value
        return None
    return None


def _first_nonnegative_int(
    payload: Mapping[str, object],
    keys: tuple[str, ...],
) -> Optional[int]:
    for key in keys:
        if key not in payload or payload.get(key) in (None, ""):
            continue
        value = payload.get(key)
        if isinstance(value, bool):
            return None
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return None
        return parsed if parsed >= 0 else None
    return None


def _matching_redemption_row(
    payload: object,
    *,
    amount: float,
    started_at_ms: int,
    after_time_ms: int = 0,
    matched_time_ms: int = 0,
) -> Optional[dict]:
    data = payload if isinstance(payload, Mapping) else {}
    candidates = []
    for row in list(data.get("rows") or []):
        if not isinstance(row, Mapping):
            continue
        asset = str(row.get("asset") or "").strip().upper()
        receive_asset = str(
            row.get("receiveAsset") or ""
        ).strip().upper()
        if asset != "RWUSD":
            continue
        if receive_asset != "USDC":
            continue
        try:
            row_amount = float(row.get("amount", 0) or 0)
            row_time = int(row.get("time", 0) or 0)
        except (TypeError, ValueError):
            continue
        if not math.isclose(
            row_amount,
            amount,
            rel_tol=0.0,
            abs_tol=MONEY_EPSILON,
        ):
            continue
        if row_time <= max(0, int(after_time_ms)):
            continue
        if matched_time_ms > 0 and row_time != int(matched_time_ms):
            continue
        if row_time + 5_000 < started_at_ms:
            continue
        candidates.append(dict(row))
    if not candidates:
        return None
    return min(
        candidates,
        key=lambda row: abs(
            int(row.get("time", 0) or 0) - int(started_at_ms)
        ),
    )


def _latest_redemption_history_time(payload: object) -> int:
    data = payload if isinstance(payload, Mapping) else {}
    latest = 0
    for row in list(data.get("rows") or []):
        if not isinstance(row, Mapping):
            continue
        if str(row.get("asset") or "").strip().upper() != "RWUSD":
            continue
        if (
            str(row.get("receiveAsset") or "").strip().upper()
            != "USDC"
        ):
            continue
        try:
            latest = max(latest, int(row.get("time", 0) or 0))
        except (TypeError, ValueError):
            continue
    return latest


def _redemption_row_succeeded(row: Mapping[str, object]) -> bool:
    return str(row.get("status") or "").strip().upper() == "SUCCESS"


def _redemption_row_acknowledged(row: Mapping[str, object]) -> bool:
    return str(row.get("status") or "").strip().upper() in {
        "PENDING",
        "PROCESSING",
        "SUCCESS",
    }


def _redemption_row_failed(row: Mapping[str, object]) -> bool:
    return str(row.get("status") or "").strip().upper() in {
        "FAIL",
        "FAILED",
        "REJECTED",
    }


def _spot_asset_total(payload: object) -> float:
    data = payload if isinstance(payload, Mapping) else {}
    if data.get("total") not in (None, ""):
        return max(0.0, float(data.get("total") or 0))
    return max(
        0.0,
        float(data.get("free", 0) or 0)
        + float(data.get("locked", 0) or 0),
    )


def _floor_asset_amount(value: float) -> float:
    amount = max(0.0, float(value or 0))
    return math.floor((amount + 1e-12) * 100_000_000) / 100_000_000
