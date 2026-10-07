"""Official-order handling for staged RWUSD customer liquidation."""

from __future__ import annotations

from dataclasses import replace
from typing import Mapping

from backend.strategy.coin_margined_rwusd.exit import (
    QUANTITY_EPSILON,
    ReductionPlan,
)


class RWUSDCustomerExitOrders:
    """Reconcile, cancel, and reprice persistent customer GTX orders."""

    def __init__(
        self,
        *,
        owner: object,
        engine: object,
        config: object,
    ) -> None:
        self.owner = owner
        self.engine = engine
        self.config = config

    async def reconcile(self, now_ms: int) -> list[dict]:
        self.engine.lifecycle.advance_cycle()
        self.engine.persist()
        events = await self.owner.reconcile_active_orders()
        active = [
            order
            for order in self.engine.lifecycle.active_orders
            if order.purpose == "customer_exit"
        ]
        timeout_ms = int(
            max(
                1.0,
                float(
                    getattr(
                        self.config,
                        "EXIT_LIMIT_REPRICE_SEC",
                        15.0,
                    )
                ),
            )
            * 1000
        )
        due = [
            order
            for order in active
            if (
                order.external_order_id
                and order.external_submitted_at_ms > 0
                and now_ms - order.external_submitted_at_ms >= timeout_ms
            )
        ]
        if not due:
            return events
        events.append(
            {
                "type": "rwusd_exit",
                "action": "limit_reprice_requested",
                "order_ids": [order.order_id for order in due],
                "simulated": False,
            }
        )
        events.extend(await self.owner._cancel_orders(due))
        await self.owner.reconcile_positions()
        return events

    async def partition_plan(
        self,
        *,
        symbol: str,
        plan: ReductionPlan,
        long_position: float,
        short_position: float,
        order_book: Mapping[str, object],
    ) -> tuple[ReductionPlan, ReductionPlan]:
        bids = list(order_book.get("bids") or [])
        asks = list(order_book.get("asks") or [])
        if not bids or not asks:
            raise RuntimeError("customer_exit_order_book_unavailable")
        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
        long_limit, long_residual = await self._partition_side(
            symbol=symbol,
            planned=plan.long_quantity,
            position=long_position,
            limit_price=best_ask,
            market_price=best_bid,
            safety_cap=min(
                max(0.0, float(long_position)),
                max(0.0, float(plan.bid_depth_quantity)),
                self._notional_quantity_cap(best_bid),
            ),
        )
        short_limit, short_residual = await self._partition_side(
            symbol=symbol,
            planned=plan.short_quantity,
            position=short_position,
            limit_price=best_bid,
            market_price=best_ask,
            safety_cap=min(
                max(0.0, float(short_position)),
                max(0.0, float(plan.ask_depth_quantity)),
                self._notional_quantity_cap(best_ask),
            ),
        )
        return (
            replace(
                plan,
                long_quantity=long_limit,
                short_quantity=short_limit,
            ),
            ReductionPlan(
                long_quantity=long_residual,
                short_quantity=short_residual,
                desired_fraction=plan.desired_fraction,
                reason="official_limit_minimum_residual",
            ),
        )

    async def _partition_side(
        self,
        *,
        symbol: str,
        planned: float,
        position: float,
        limit_price: float,
        market_price: float,
        safety_cap: float,
    ) -> tuple[float, float]:
        planned_quantity = max(0.0, float(planned))
        position_quantity = max(0.0, float(position))
        if planned_quantity <= QUANTITY_EPSILON:
            return 0.0, 0.0
        minimum = await self.owner.client.get_um_minimum_limit_order_quantity(
            symbol,
            limit_price,
        )
        if not minimum.get("success"):
            raise RuntimeError(
                "customer_exit_official_limit_rules_unavailable:"
                f"{minimum.get('error') or 'unknown'}"
            )
        minimum_quantity = max(
            0.0,
            float(minimum.get("quantity", 0) or 0),
        )
        minimum_notional = max(
            0.0,
            float(minimum.get("notional", 0) or 0),
        )
        position_is_subminimum = (
            position_quantity + QUANTITY_EPSILON < minimum_quantity
            or (
                position_quantity * limit_price + 1e-8
                < minimum_notional
            )
        )
        if position_is_subminimum:
            if (
                position_quantity
                > max(0.0, float(safety_cap)) + QUANTITY_EPSILON
            ):
                raise RuntimeError(
                    "customer_exit_market_residual_exceeds_safety_cap"
                )
            normalized = await (
                self.owner.client.normalize_um_market_close_quantity(
                    symbol,
                    position_quantity,
                    market_price,
                )
            )
            if not normalized.get("success"):
                raise RuntimeError(
                    "customer_exit_market_residual_unavailable:"
                    f"{normalized.get('error') or 'unknown'}"
                )
            market_quantity = max(
                0.0,
                float(normalized.get("quantity", 0) or 0),
            )
            if (
                market_quantity <= QUANTITY_EPSILON
                or position_quantity - market_quantity > QUANTITY_EPSILON
            ):
                raise RuntimeError(
                    "customer_exit_market_residual_cannot_close_full_position"
                )
            return 0.0, min(position_quantity, market_quantity)
        executable = max(planned_quantity, minimum_quantity)
        if executable > max(0.0, float(safety_cap)) + QUANTITY_EPSILON:
            return 0.0, 0.0
        return min(position_quantity, executable), 0.0

    def _notional_quantity_cap(self, price: float) -> float:
        maximum = max(
            0.0,
            float(
                getattr(
                    self.config,
                    "EXIT_MAX_BATCH_NOTIONAL_PER_SIDE",
                    250_000.0,
                )
            ),
        )
        return maximum / price if maximum > 0 and price > 0 else float("inf")
