"""Execution-neutral order lifecycle for RWUSD dry-run and exchange modes."""

from __future__ import annotations

from dataclasses import dataclass
import time
from typing import List, Optional, Set, Tuple

from backend.strategy.coin_margined_rwusd.models import PositionSide, parse_bool


ACTIVE_STATUSES = {"NEW", "PARTIALLY_FILLED"}
MAX_RETAINED_TERMINAL_ORDERS = 2000


@dataclass
class DryRunOrder:
    order_id: str
    position_side: PositionSide
    side: str
    quantity: float
    price: float
    reduce_only: bool
    purpose: str
    cumulative_filled_quantity: float = 0.0
    status: str = "NEW"
    created_cycle: int = 0
    external_client_order_id: Optional[str] = None
    external_order_id: Optional[str] = None
    external_status: Optional[str] = None
    external_error: Optional[str] = None
    external_attempt_count: int = 0
    external_not_found_count: int = 0
    external_submitted_cycle: Optional[int] = None
    created_at_ms: int = 0
    external_submitted_at_ms: int = 0
    order_type: str = "LIMIT"
    time_in_force: str = "GTX"
    maker_only: bool = True
    deferred_minimum_quantity: float = 0.0
    deferred_minimum_notional: float = 0.0
    deferred_reference_price: float = 0.0
    deferred_cycle: int = 0
    deferred_minimum_known: bool = False

    def to_dict(self) -> dict:
        return {
            "order_id": self.order_id,
            "position_side": self.position_side.value,
            "side": self.side,
            "quantity": self.quantity,
            "price": self.price,
            "reduce_only": self.reduce_only,
            "purpose": self.purpose,
            "cumulative_filled_quantity": self.cumulative_filled_quantity,
            "status": self.status,
            "created_cycle": self.created_cycle,
            "external_client_order_id": self.external_client_order_id,
            "external_order_id": self.external_order_id,
            "external_status": self.external_status,
            "external_error": self.external_error,
            "external_attempt_count": self.external_attempt_count,
            "external_not_found_count": self.external_not_found_count,
            "external_submitted_cycle": self.external_submitted_cycle,
            "created_at_ms": self.created_at_ms,
            "external_submitted_at_ms": self.external_submitted_at_ms,
            "order_type": self.order_type,
            "time_in_force": self.time_in_force,
            "maker_only": self.maker_only,
            "deferred_minimum_quantity": self.deferred_minimum_quantity,
            "deferred_minimum_notional": self.deferred_minimum_notional,
            "deferred_reference_price": self.deferred_reference_price,
            "deferred_cycle": self.deferred_cycle,
            "deferred_minimum_known": self.deferred_minimum_known,
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "DryRunOrder":
        return cls(
            order_id=str(payload.get("order_id") or ""),
            position_side=PositionSide(str(payload.get("position_side") or "LONG")),
            side=str(payload.get("side") or "BUY"),
            quantity=max(0.0, float(payload.get("quantity", 0) or 0)),
            price=max(0.0, float(payload.get("price", 0) or 0)),
            reduce_only=parse_bool(payload.get("reduce_only", False)),
            purpose=str(payload.get("purpose") or "open_hedge"),
            cumulative_filled_quantity=max(
                0.0,
                float(payload.get("cumulative_filled_quantity", 0) or 0),
            ),
            status=str(payload.get("status") or "NEW"),
            created_cycle=max(0, int(payload.get("created_cycle", 0) or 0)),
            external_client_order_id=(
                str(payload.get("external_client_order_id") or "") or None
            ),
            external_order_id=str(payload.get("external_order_id") or "") or None,
            external_status=str(payload.get("external_status") or "") or None,
            external_error=str(payload.get("external_error") or "") or None,
            external_attempt_count=max(
                0,
                int(payload.get("external_attempt_count", 0) or 0),
            ),
            external_not_found_count=max(
                0,
                int(payload.get("external_not_found_count", 0) or 0),
            ),
            external_submitted_cycle=(
                None
                if payload.get("external_submitted_cycle") is None
                else max(
                    0,
                    int(payload.get("external_submitted_cycle", 0) or 0),
                )
            ),
            created_at_ms=max(
                0,
                int(payload.get("created_at_ms", 0) or 0),
            ),
            external_submitted_at_ms=max(
                0,
                int(payload.get("external_submitted_at_ms", 0) or 0),
            ),
            order_type=str(payload.get("order_type") or "LIMIT").upper(),
            time_in_force=str(
                (
                    payload["time_in_force"]
                    if "time_in_force" in payload
                    else "GTX"
                )
                or ""
            ).upper(),
            maker_only=parse_bool(payload.get("maker_only", True), default=True),
            deferred_minimum_quantity=max(
                0.0,
                float(payload.get("deferred_minimum_quantity", 0) or 0),
            ),
            deferred_minimum_notional=max(
                0.0,
                float(payload.get("deferred_minimum_notional", 0) or 0),
            ),
            deferred_reference_price=max(
                0.0,
                float(payload.get("deferred_reference_price", 0) or 0),
            ),
            deferred_cycle=max(
                0,
                int(payload.get("deferred_cycle", 0) or 0),
            ),
            deferred_minimum_known=parse_bool(
                payload.get("deferred_minimum_known", False)
            ),
        )


@dataclass
class FillEvent:
    order_id: str
    position_side: PositionSide
    side: str
    fill_quantity: float
    fill_price: float
    reduce_only: bool
    purpose: str
    status: str

    def to_dict(self) -> dict:
        return {
            "type": "simulated_fill",
            "order_id": self.order_id,
            "position_side": self.position_side.value,
            "side": self.side,
            "quantity": self.fill_quantity,
            "price": self.fill_price,
            "reduce_only": self.reduce_only,
            "purpose": self.purpose,
            "status": self.status,
            "simulated": True,
        }


class DryRunOrderLifecycle:
    def __init__(self) -> None:
        self.orders: List[DryRunOrder] = []
        self._order_index: dict[str, DryRunOrder] = {}
        self._external_order_index: dict[str, DryRunOrder] = {}
        self.next_order_number = 1
        self.cycle = 0
        self.expired_order_count = 0
        self.timeout_requote_count = 0
        self.external_consecutive_rejection_count = 0
        self.external_consecutive_maker_requote_count = 0
        self.pruned_terminal_order_count = 0

    @property
    def active_orders(self) -> List[DryRunOrder]:
        return [order for order in self.orders if order.status in ACTIVE_STATUSES]

    def get_order(self, order_id: str) -> Optional[DryRunOrder]:
        return self._order_index.get(str(order_id or ""))

    def get_order_by_external_id(
        self,
        external_order_id: object,
    ) -> Optional[DryRunOrder]:
        return self._external_order_index.get(str(external_order_id or ""))

    def deferred_minimum_summary(
        self,
        *,
        purposes: Set[str],
        position_side: Optional[PositionSide] = None,
    ) -> dict:
        normalized_purposes = {
            str(purpose or "").strip() for purpose in purposes
        }
        entries = [
            order
            for order in self.orders
            if (
                order.purpose in normalized_purposes
                and (
                    position_side is None
                    or order.position_side == position_side
                )
                and order.deferred_minimum_quantity > 0
            )
        ]
        if not entries:
            return {
                "count": 0,
                "residual_quantity": 0.0,
                "minimum_quantity": 0.0,
                "minimum_notional": 0.0,
                "reference_price": 0.0,
                "latest_cycle": 0,
                "minimum_known": False,
            }
        latest = max(
            entries,
            key=lambda order: (
                order.deferred_cycle,
                order.created_cycle,
                order.order_id,
            ),
        )
        return {
            "count": len(entries),
            # Deferred rows are retry attempts for one state-machine intent.
            # Keep the largest requirement so a later retry cannot shrink
            # below an earlier rejected ticket.
            "residual_quantity": max(
                max(
                    0.0,
                    float(order.quantity)
                    - float(order.cumulative_filled_quantity),
                )
                for order in entries
            ),
            "minimum_quantity": max(
                float(order.deferred_minimum_quantity)
                for order in entries
            ),
            "minimum_notional": max(
                float(order.deferred_minimum_notional)
                for order in entries
            ),
            "reference_price": float(latest.deferred_reference_price),
            "latest_cycle": latest.deferred_cycle,
            "minimum_known": all(
                bool(order.deferred_minimum_known) for order in entries
            ),
        }

    def clear_deferred_minimum(
        self,
        *,
        purpose: Optional[str] = None,
        position_side: Optional[PositionSide] = None,
        purposes: Optional[Set[str]] = None,
    ) -> None:
        normalized_purpose = (
            str(purpose or "").strip() if purpose is not None else None
        )
        normalized_purposes = (
            {str(item or "").strip() for item in purposes}
            if purposes is not None
            else None
        )
        for order in self.orders:
            if order.deferred_minimum_quantity <= 0:
                continue
            if (
                normalized_purpose is not None
                and order.purpose != normalized_purpose
            ):
                continue
            if (
                normalized_purposes is not None
                and order.purpose not in normalized_purposes
            ):
                continue
            if (
                position_side is not None
                and order.position_side != position_side
            ):
                continue
            order.deferred_minimum_quantity = 0.0
            order.deferred_minimum_notional = 0.0
            order.deferred_reference_price = 0.0
            order.deferred_cycle = 0
            order.deferred_minimum_known = False

    def bind_external_order_id(
        self,
        order: DryRunOrder,
        external_order_id: object,
    ) -> None:
        previous = str(order.external_order_id or "")
        if previous and self._external_order_index.get(previous) is order:
            self._external_order_index.pop(previous, None)
        normalized = str(external_order_id or "")
        order.external_order_id = normalized or None
        if normalized:
            self._external_order_index[normalized] = order

    def _index_order(self, order: DryRunOrder) -> None:
        self._order_index[order.order_id] = order
        if order.external_order_id:
            self._external_order_index[str(order.external_order_id)] = order

    def _rebuild_indexes(self) -> None:
        self._order_index = {}
        self._external_order_index = {}
        for order in self.orders:
            self._index_order(order)

    def _prune_terminal_orders(self) -> None:
        terminal = [
            order for order in self.orders
            if order.status not in ACTIVE_STATUSES
        ]
        excess = len(terminal) - MAX_RETAINED_TERMINAL_ORDERS
        if excess <= 0:
            return
        retained = {
            id(order)
            for order in terminal[-MAX_RETAINED_TERMINAL_ORDERS:]
        }
        self.orders = [
            order
            for order in self.orders
            if order.status in ACTIVE_STATUSES or id(order) in retained
        ]
        self.pruned_terminal_order_count += excess
        self._rebuild_indexes()

    def advance_cycle(self) -> None:
        self.cycle += 1

    def cancel_active(self) -> List[str]:
        canceled: List[str] = []
        for order in self.active_orders:
            order.status = "CANCELED"
            canceled.append(order.order_id)
        return canceled

    def cancel_active_except(self, allowed_purposes: Set[str]) -> List[str]:
        canceled: List[str] = []
        for order in self.active_orders:
            if order.purpose in allowed_purposes:
                continue
            order.status = "CANCELED"
            canceled.append(order.order_id)
        return canceled

    def create_order(
        self,
        *,
        position_side: PositionSide,
        side: str,
        quantity: float,
        price: float,
        reduce_only: bool,
        purpose: str,
        order_type: str = "LIMIT",
        time_in_force: str = "GTX",
        maker_only: bool = True,
    ) -> Optional[DryRunOrder]:
        if quantity <= 1e-12 or price <= 0:
            return None
        order = DryRunOrder(
            order_id=f"rwusd-dry-{self.next_order_number}",
            position_side=position_side,
            side=str(side).upper(),
            quantity=float(quantity),
            price=float(price),
            reduce_only=bool(reduce_only),
            purpose=str(purpose),
            created_cycle=self.cycle,
            created_at_ms=int(time.time() * 1000),
            order_type=str(order_type or "LIMIT").upper(),
            time_in_force=str(
                "GTX" if time_in_force is None else time_in_force
            ).upper(),
            maker_only=bool(maker_only),
        )
        self.next_order_number += 1
        self.orders.append(order)
        self._index_order(order)
        return order

    def expire_orders_older_than(self, max_age_cycles: int) -> List[DryRunOrder]:
        if max_age_cycles <= 0:
            return []
        expired: List[DryRunOrder] = []
        for order in self.active_orders:
            if self.cycle - order.created_cycle < max_age_cycles:
                continue
            order.status = "EXPIRED"
            expired.append(order)
        if expired:
            self.expired_order_count += len(expired)
            self.timeout_requote_count += 1
        return expired

    def match(
        self,
        *,
        mark_price: float,
        fill_fraction: float,
        min_fill_quantity: float,
        purposes: Optional[Set[str]] = None,
    ) -> List[FillEvent]:
        if mark_price <= 0:
            return []
        fraction = min(1.0, max(1e-9, float(fill_fraction)))
        minimum = max(0.0, float(min_fill_quantity))
        fills: List[FillEvent] = []
        for order in list(self.active_orders):
            if purposes is not None and order.purpose not in purposes:
                continue
            if not self._is_marketable(order, mark_price):
                continue
            remaining = max(0.0, order.quantity - order.cumulative_filled_quantity)
            if remaining <= 1e-12:
                order.status = "FILLED"
                continue
            fill_qty = min(remaining, max(remaining * fraction, minimum))
            if fill_qty <= 1e-12:
                continue
            order.cumulative_filled_quantity += fill_qty
            if order.quantity - order.cumulative_filled_quantity <= 1e-10:
                order.cumulative_filled_quantity = order.quantity
                order.status = "FILLED"
            else:
                order.status = "PARTIALLY_FILLED"
            fills.append(
                FillEvent(
                    order_id=order.order_id,
                    position_side=order.position_side,
                    side=order.side,
                    fill_quantity=fill_qty,
                    fill_price=float(mark_price),
                    reduce_only=order.reduce_only,
                    purpose=order.purpose,
                    status=order.status,
                )
            )
        return fills

    @staticmethod
    def _is_marketable(order: DryRunOrder, mark_price: float) -> bool:
        return mark_price <= order.price if order.side == "BUY" else mark_price >= order.price

    def active_purpose(self) -> Optional[str]:
        active = self.active_orders
        return active[0].purpose if active else None

    def counts(self) -> Tuple[int, int]:
        active = len(self.active_orders)
        terminal = (
            self.pruned_terminal_order_count
            + len(self.orders)
            - active
        )
        return active, terminal

    def to_dict(self) -> dict:
        self._prune_terminal_orders()
        return {
            "next_order_number": self.next_order_number,
            "cycle": self.cycle,
            "expired_order_count": self.expired_order_count,
            "timeout_requote_count": self.timeout_requote_count,
            "external_consecutive_rejection_count": (
                self.external_consecutive_rejection_count
            ),
            "external_consecutive_maker_requote_count": (
                self.external_consecutive_maker_requote_count
            ),
            "pruned_terminal_order_count": (
                self.pruned_terminal_order_count
            ),
            "orders": [order.to_dict() for order in self.orders],
        }

    @classmethod
    def from_dict(cls, payload: object) -> "DryRunOrderLifecycle":
        data = payload if isinstance(payload, dict) else {}
        lifecycle = cls()
        lifecycle.next_order_number = max(1, int(data.get("next_order_number", 1) or 1))
        lifecycle.cycle = max(0, int(data.get("cycle", 0) or 0))
        lifecycle.expired_order_count = max(
            0,
            int(data.get("expired_order_count", 0) or 0),
        )
        lifecycle.timeout_requote_count = max(
            0,
            int(data.get("timeout_requote_count", 0) or 0),
        )
        lifecycle.external_consecutive_rejection_count = max(
            0,
            int(data.get("external_consecutive_rejection_count", 0) or 0),
        )
        lifecycle.external_consecutive_maker_requote_count = max(
            0,
            int(
                data.get(
                    "external_consecutive_maker_requote_count",
                    0,
                )
                or 0
            ),
        )
        lifecycle.pruned_terminal_order_count = max(
            0,
            int(data.get("pruned_terminal_order_count", 0) or 0),
        )
        raw_orders = data.get("orders", [])
        if isinstance(raw_orders, list):
            for item in raw_orders:
                if not isinstance(item, dict):
                    continue
                try:
                    order = DryRunOrder.from_dict(item)
                except (TypeError, ValueError):
                    continue
                lifecycle.orders.append(order)
                lifecycle._index_order(order)
        lifecycle._prune_terminal_orders()
        return lifecycle
