"""USDC liability repayment and final cash settlement for RWUSD exits."""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from typing import Optional

from backend.strategy.coin_margined_rwusd.exit import MONEY_EPSILON


SPOT_TO_PM = "SPOT_TO_PM"
PM_TO_SPOT = "PM_TO_SPOT"
_TRANSFER_TYPES = {
    SPOT_TO_PM: ("MAIN_PORTFOLIO_MARGIN", "MAIN_MARGIN"),
    PM_TO_SPOT: ("PORTFOLIO_MARGIN_MAIN", "MARGIN_MAIN"),
}


@dataclass(frozen=True)
class USDCSettlementSnapshot:
    account_type: str
    total_wallet_balance: float
    negative_balance: float
    um_wallet_balance: float
    cm_wallet_balance: float
    cross_margin_free: float
    cross_margin_borrowed: float
    cross_margin_interest: float
    spot_free: float
    spot_total: float

    @classmethod
    def from_payloads(
        cls,
        balance: object,
        spot: object,
    ) -> "USDCSettlementSnapshot":
        asset = balance if isinstance(balance, dict) else {}
        spot_asset = spot if isinstance(spot, dict) else {}
        spot_free = max(
            0.0,
            _finite_number(spot_asset.get("free", 0), "spot.free"),
        )
        spot_total_value = spot_asset.get("total")
        if spot_total_value is None or spot_total_value == "":
            spot_total_value = spot_free + _finite_number(
                spot_asset.get("locked", 0),
                "spot.locked",
            )
        spot_total = max(
            spot_free,
            _finite_number(
                spot_total_value,
                "spot.total",
            ),
        )
        numbers = {
            field: _finite_number(asset.get(field, 0), field)
            for field in (
                "total_wallet_balance",
                "negative_balance",
                "um_wallet_balance",
                "cm_wallet_balance",
                "cross_margin_free",
                "cross_margin_borrowed",
                "cross_margin_interest",
            )
        }
        return cls(
            account_type=str(
                asset.get("account_type") or ""
            ).strip().lower(),
            total_wallet_balance=numbers["total_wallet_balance"],
            negative_balance=numbers["negative_balance"],
            um_wallet_balance=numbers["um_wallet_balance"],
            cm_wallet_balance=numbers["cm_wallet_balance"],
            cross_margin_free=max(0.0, numbers["cross_margin_free"]),
            cross_margin_borrowed=max(0.0, numbers["cross_margin_borrowed"]),
            cross_margin_interest=max(0.0, numbers["cross_margin_interest"]),
            spot_free=spot_free,
            spot_total=spot_total,
        )

    @property
    def futures_negative(self) -> float:
        return max(0.0, -self.um_wallet_balance) + max(
            0.0,
            -self.cm_wallet_balance,
        )

    @property
    def cross_margin_debt(self) -> float:
        return self.cross_margin_borrowed + self.cross_margin_interest

    @property
    def negative_balance_amount(self) -> float:
        return abs(self.negative_balance)

    def debt_free(self, dust: float) -> bool:
        return (
            self.total_wallet_balance >= -dust
            and self.negative_balance_amount <= dust
            and self.futures_negative <= dust
            and self.cross_margin_debt <= dust
        )


class RWUSDExitSettlement:
    """Advance one restart-safe USDC settlement action per exit cycle."""

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

    async def prepare(
        self,
        now_ms: int,
    ) -> tuple[USDCSettlementSnapshot | None, list[dict], bool]:
        if self.engine.exit_state.pending_cash_transfer_amount > MONEY_EPSILON:
            self.engine.exit_state.clear_zero_balance()
            return (
                None,
                await self._reconcile_pending_cash_transfer(now_ms),
                True,
            )

        events = list(await self.owner.reconcile_user_trades())
        pnl_event = self.engine.settle_external_exit_pnl()
        if pnl_event is not None:
            events.append(pnl_event)

        snapshot = await self._load_snapshot()
        dust = self._usdc_dust()
        if (
            not snapshot.debt_free(dust)
            or abs(snapshot.total_wallet_balance) > dust
        ):
            self.engine.exit_state.clear_zero_balance()
        self._record_snapshot(snapshot)
        repayment_source = self._repayment_source(snapshot, dust)
        if repayment_source is not None:
            events.extend(
                await self._repay_futures_negative(
                    now_ms,
                    source=repayment_source,
                )
            )
            return snapshot, events, True

        if snapshot.futures_negative > dust:
            funding_events, funding_submitted = (
                await self._fund_debt_or_wait(
                    now_ms=now_ms,
                    required=snapshot.futures_negative,
                    available=snapshot.cross_margin_free,
                    spot_free=snapshot.spot_free,
                    wait_action=(
                        "awaiting_redemption_cash_for_futures_debt"
                    ),
                )
            )
            events.extend(funding_events)
            if funding_submitted:
                return snapshot, events, True
            return snapshot, events, False

        if snapshot.cross_margin_debt > dust:
            if snapshot.account_type not in {
                "pm_2",
                "portfolio_margin_classic",
            }:
                self.engine.exit_state.settlement_action = (
                    "pm_pro_margin_debt_requires_manual_repayment"
                )
                self.engine.persist()
                return snapshot, events, False
            if (
                snapshot.cross_margin_free + dust
                >= snapshot.cross_margin_debt
            ):
                events.extend(
                    await self._repay_margin_debt(
                        now_ms,
                        amount=snapshot.cross_margin_debt,
                    )
                )
                return snapshot, events, True
            funding_events, funding_submitted = (
                await self._fund_debt_or_wait(
                    now_ms=now_ms,
                    required=snapshot.cross_margin_debt,
                    available=snapshot.cross_margin_free,
                    spot_free=snapshot.spot_free,
                    wait_action=(
                        "awaiting_redemption_cash_for_margin_debt"
                    ),
                )
            )
            events.extend(funding_events)
            if funding_submitted:
                return snapshot, events, True
            return snapshot, events, False

        unfunded_negative = max(
            snapshot.negative_balance_amount,
            -snapshot.total_wallet_balance,
        )
        if unfunded_negative > dust:
            funding_events, funding_submitted = (
                await self._fund_debt_or_wait(
                    now_ms=now_ms,
                    required=unfunded_negative,
                    available=0.0,
                    spot_free=snapshot.spot_free,
                    wait_action=(
                        "awaiting_redemption_cash_for_negative_balance"
                    ),
                )
            )
            events.extend(funding_events)
            return snapshot, events, funding_submitted
        return snapshot, events, False

    async def advance_final_cash(
        self,
        *,
        now_ms: int,
        snapshot: USDCSettlementSnapshot,
    ) -> tuple[list[dict], bool]:
        dust = self._usdc_dust()
        if not snapshot.debt_free(dust):
            if (
                snapshot.cross_margin_debt > dust
                and snapshot.account_type
                not in {"pm_2", "portfolio_margin_classic"}
            ):
                raise RuntimeError(
                    "customer_exit_pm_pro_margin_debt_requires_manual_"
                    f"repayment:{snapshot.cross_margin_debt}"
                )
            if snapshot.total_wallet_balance < -dust:
                raise RuntimeError(
                    "customer_exit_usdc_negative_balance_unfunded:"
                    f"{-snapshot.total_wallet_balance}"
                )
            raise RuntimeError("customer_exit_usdc_liability_not_settled")
        if snapshot.total_wallet_balance <= dust:
            self.engine.exit_state.settlement_action = "settlement_complete"
            self.engine.persist()
            return [], False
        if snapshot.cross_margin_free <= dust:
            await self.client.collect_pm_asset(asset="USDC")
            self.engine.exit_state.settlement_action = "collecting_pm_usdc"
            self._defer(now_ms, self._poll_delay_ms(), None)
            self.engine.persist()
            return [
                {
                    "type": "rwusd_exit",
                    "action": "pm_usdc_collection_submitted",
                    "amount": snapshot.total_wallet_balance,
                    "simulated": False,
                }
            ], True

        maximum = max(
            0.0,
            float(await self.client.get_margin_asset_max_withdraw("USDC") or 0),
        )
        amount = self._floor_asset_amount(
            min(
                snapshot.total_wallet_balance,
                snapshot.cross_margin_free,
                maximum,
            )
        )
        if amount <= dust:
            self.engine.exit_state.settlement_action = (
                "pm_usdc_withdrawal_waiting"
            )
            self._defer(
                now_ms,
                self._poll_delay_ms(),
                "customer_exit_pm_usdc_not_withdrawable",
            )
            self.engine.persist()
            return [], True
        return (
            await self._submit_cash_transfer(
                now_ms=now_ms,
                direction=PM_TO_SPOT,
                amount=amount,
            ),
            True,
        )

    async def refresh_snapshot(self) -> USDCSettlementSnapshot:
        snapshot = await self._load_snapshot()
        self._record_snapshot(snapshot)
        return snapshot

    async def _load_snapshot(self) -> USDCSettlementSnapshot:
        balance, spot = await asyncio.gather(
            self.client.get_margin_asset_settlement("USDC"),
            self.client.get_spot_asset_balance("USDC"),
        )
        return USDCSettlementSnapshot.from_payloads(balance, spot)

    def _record_snapshot(self, snapshot: USDCSettlementSnapshot) -> None:
        self.engine.exit_state.update_settlement_snapshot(
            signed_wallet_balance=snapshot.total_wallet_balance,
            negative_balance=snapshot.negative_balance_amount,
            futures_negative=snapshot.futures_negative,
            cross_margin_debt=snapshot.cross_margin_debt,
            spot_usdc=snapshot.spot_total,
        )
        self.engine.persist()

    @staticmethod
    def _repayment_source(
        snapshot: USDCSettlementSnapshot,
        dust: float,
    ) -> str | None:
        debt = snapshot.futures_negative
        if debt <= dust:
            return None
        if snapshot.account_type in {
            "pm_2",
            "portfolio_margin_classic",
        }:
            return (
                "CLASSIC"
                if snapshot.cross_margin_free + dust >= debt
                else None
            )
        if snapshot.account_type in {
            "pm_1",
            "pm_3",
            "portfolio_margin_pro",
            "portfolio_margin_pro_span",
        }:
            if snapshot.cross_margin_free + dust >= debt:
                return "MARGIN"
            if snapshot.spot_free + dust >= debt:
                return "SPOT"
            return None
        raise RuntimeError(
            "customer_exit_portfolio_margin_account_type_unsupported:"
            f"{snapshot.account_type or 'missing'}"
        )

    async def _repay_futures_negative(
        self,
        now_ms: int,
        *,
        source: str,
    ) -> list[dict]:
        request_source = None if source == "CLASSIC" else source
        try:
            await self.client.repay_futures_negative_balance(
                source=request_source
            )
        except Exception as exc:
            self._defer(
                now_ms,
                self._poll_delay_ms(),
                f"futures_negative_balance_repay_uncertain:{exc}",
            )
            self.engine.persist()
            return [
                {
                    "type": "rwusd_exit",
                    "action": "futures_negative_balance_repay_uncertain",
                    "source": source,
                    "reason": str(exc),
                    "simulated": False,
                }
            ]
        self.engine.exit_state.record_futures_repay(now_ms)
        self._defer(now_ms, self._poll_delay_ms(), None)
        self.engine.persist()
        return [
            {
                "type": "rwusd_exit",
                "action": "futures_negative_balance_repay_submitted",
                "source": source,
                "simulated": False,
            }
        ]

    async def _repay_margin_debt(
        self,
        now_ms: int,
        *,
        amount: float,
    ) -> list[dict]:
        repayment = self._floor_asset_amount(amount)
        if repayment <= self._usdc_dust():
            return []
        try:
            response = await self.client.repay_margin_debt(
                asset="USDC",
                amount=repayment,
            )
        except Exception as exc:
            self._defer(
                now_ms,
                self._poll_delay_ms(),
                f"margin_debt_repay_uncertain:{exc}",
            )
            self.engine.persist()
            return [
                {
                    "type": "rwusd_exit",
                    "action": "margin_debt_repay_uncertain",
                    "amount": repayment,
                    "reason": str(exc),
                    "simulated": False,
                }
            ]
        self.engine.exit_state.settlement_action = "margin_debt_repaid"
        self._defer(now_ms, self._poll_delay_ms(), None)
        self.engine.persist()
        return [
            {
                "type": "rwusd_exit",
                "action": "margin_debt_repay_submitted",
                "amount": repayment,
                "transaction_id": str(
                    (response or {}).get("tranId")
                    or (response or {}).get("clientTranId")
                    or ""
                ),
                "simulated": False,
            }
        ]

    async def _fund_debt_or_wait(
        self,
        *,
        now_ms: int,
        required: float,
        available: float,
        spot_free: float,
        wait_action: str,
    ) -> tuple[list[dict], bool]:
        amount = self._floor_asset_amount(
            min(
                max(0.0, float(required) - float(available)),
                max(0.0, float(spot_free)),
            )
        )
        if amount > self._usdc_dust():
            return (
                await self._submit_cash_transfer(
                    now_ms=now_ms,
                    direction=SPOT_TO_PM,
                    amount=amount,
                ),
                True,
            )
        self.engine.exit_state.settlement_action = wait_action
        self.engine.persist()
        return [], False

    async def _submit_cash_transfer(
        self,
        *,
        now_ms: int,
        direction: str,
        amount: float,
    ) -> list[dict]:
        state = self.engine.exit_state
        history_loader = self._cash_transfer_history_loader(direction)
        history = await history_loader(
            start_time_ms=max(1, now_ms - 60_000),
            end_time_ms=now_ms + 1_000,
        )
        state.last_cash_transfer_history_time_ms = max(
            state.last_cash_transfer_history_time_ms,
            _latest_cash_transfer_time(history, direction=direction),
        )
        state.begin_cash_transfer(
            now_ms=now_ms,
            direction=direction,
            amount=amount,
        )
        self.engine.persist()
        try:
            response = (
                await self.client.transfer_spot_to_pm(
                    asset="USDC",
                    amount=amount,
                )
                if direction == SPOT_TO_PM
                else await self.client.transfer_pm_to_spot(
                    asset="USDC",
                    amount=amount,
                )
            )
        except Exception as exc:
            self._defer(
                now_ms,
                self._poll_delay_ms(),
                f"cash_transfer_uncertain:{direction}:{exc}",
            )
            self.engine.persist()
            return [
                {
                    "type": "rwusd_exit",
                    "action": "cash_transfer_uncertain",
                    "direction": direction,
                    "amount": amount,
                    "reason": str(exc),
                    "simulated": False,
                }
            ]
        state.confirm_cash_transfer_submission(
            now_ms,
            transfer_id=(
                (response or {}).get("tranId")
                or (response or {}).get("clientTranId")
            ),
        )
        self._defer(now_ms, self._poll_delay_ms(), None)
        self.engine.persist()
        return [
            {
                "type": "rwusd_exit",
                "action": "cash_transfer_submitted",
                "direction": direction,
                "amount": amount,
                "transfer_id": state.pending_cash_transfer_id,
                "simulated": False,
            }
        ]

    async def _reconcile_pending_cash_transfer(
        self,
        now_ms: int,
    ) -> list[dict]:
        state = self.engine.exit_state
        direction = str(
            state.pending_cash_transfer_direction or ""
        ).strip().upper()
        history_loader = self._cash_transfer_history_loader(direction)
        history = await history_loader(
            start_time_ms=max(
                1,
                state.pending_cash_transfer_started_at_ms - 60_000,
            ),
            end_time_ms=now_ms + 60_000,
        )
        matched = _matching_cash_transfer_row(
            history,
            direction=direction,
            amount=state.pending_cash_transfer_amount,
            started_at_ms=state.pending_cash_transfer_started_at_ms,
            after_time_ms=state.last_cash_transfer_history_time_ms,
            transfer_id=state.pending_cash_transfer_id,
        )
        if matched is not None:
            status = str(matched.get("status") or "").strip().upper()
            if status == "CONFIRMED":
                return self._confirm_cash_transfer(
                    now_ms=now_ms,
                    snapshot=await self._load_snapshot(),
                    history_time_ms=int(
                        matched.get("timestamp", 0) or 0
                    ),
                    confirmation_source="history",
                )
            if status in {"FAILED", "FAIL", "REJECTED"}:
                reason = f"cash_transfer_failed:{direction}:{status}"
                state.fail_pending_cash_transfer(now_ms, reason)
                self._defer(now_ms, self._poll_delay_ms(), reason)
                self.engine.persist()
                return [
                    {
                        "type": "rwusd_exit",
                        "action": "cash_transfer_failed",
                        "direction": direction,
                        "amount": float(
                            matched.get("amount", 0) or 0
                        ),
                        "status": status,
                        "simulated": False,
                    }
                ]

        if matched is None:
            snapshot = await self._load_snapshot()
            if _cash_transfer_balances_confirmed(
                snapshot,
                direction=direction,
                amount=state.pending_cash_transfer_amount,
                pm_balance_before=state.signed_usdc_wallet_balance,
                spot_balance_before=state.spot_usdc_balance,
                tolerance=self._usdc_dust(),
            ):
                return self._confirm_cash_transfer(
                    now_ms=now_ms,
                    snapshot=snapshot,
                    history_time_ms=None,
                    confirmation_source="balance",
                )

        timeout_ms = self._milliseconds(
            "EXIT_REDEMPTION_UNCERTAIN_RETRY_SEC",
            10 * 60.0,
        )
        if (
            not state.pending_cash_transfer_submission_confirmed
            and now_ms - state.pending_cash_transfer_started_at_ms
            >= timeout_ms
        ):
            amount = state.pending_cash_transfer_amount
            state.abandon_unconfirmed_cash_transfer(now_ms)
            self.engine.persist()
            return [
                {
                    "type": "rwusd_exit",
                    "action": "cash_transfer_unconfirmed_released_for_retry",
                    "direction": direction,
                    "amount": amount,
                    "simulated": False,
                }
            ]
        self._defer(
            now_ms,
            self._poll_delay_ms(),
            "cash_transfer_confirmation_pending",
        )
        self.engine.persist()
        return []

    def _confirm_cash_transfer(
        self,
        *,
        now_ms: int,
        snapshot: USDCSettlementSnapshot,
        history_time_ms: int | None,
        confirmation_source: str,
    ) -> list[dict]:
        confirmed_direction, confirmed_amount = (
            self.engine.exit_state.confirm_cash_transfer(
                now_ms,
                history_time_ms=history_time_ms,
            )
        )
        self._record_snapshot(snapshot)
        return [
            {
                "type": "rwusd_exit",
                "action": "cash_transfer_confirmed",
                "direction": confirmed_direction,
                "amount": confirmed_amount,
                "confirmation_source": confirmation_source,
                "simulated": False,
            }
        ]

    def _cash_transfer_history_loader(self, direction: str):
        if direction == SPOT_TO_PM:
            return self.client.get_spot_to_pm_transfer_history
        if direction == PM_TO_SPOT:
            return self.client.get_pm_to_spot_transfer_history
        raise RuntimeError(
            f"RWUSD cash transfer direction is invalid: {direction}"
        )

    def _usdc_dust(self) -> float:
        return max(
            MONEY_EPSILON,
            float(getattr(self.config, "EXIT_USDC_DUST", 1e-6)),
        )

    def _poll_delay_ms(self) -> int:
        return self._milliseconds(
            "EXIT_REDEMPTION_HISTORY_POLL_SEC",
            15.0,
        )

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

    @staticmethod
    def _floor_asset_amount(amount: float) -> float:
        return math.floor(max(0.0, float(amount)) * 1e8) / 1e8


def _matching_cash_transfer_row(
    payload: object,
    *,
    direction: str,
    amount: float,
    started_at_ms: int,
    after_time_ms: int,
    transfer_id: object = None,
) -> Optional[dict]:
    expected_types = set(
        _TRANSFER_TYPES.get(str(direction or "").upper(), ())
    )
    expected_id = str(transfer_id or "").strip()
    data = payload if isinstance(payload, dict) else {}
    rows = list(data.get("rows") or [])
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_time = int(row.get("timestamp", 0) or 0)
        if (
            str(row.get("type") or "").upper() not in expected_types
            or str(row.get("asset") or "").upper() != "USDC"
            or row_time < max(1, int(started_at_ms)) - 60_000
            or not math.isclose(
                float(row.get("amount", 0) or 0),
                float(amount),
                rel_tol=0.0,
                abs_tol=MONEY_EPSILON,
            )
        ):
            continue
        row_id = str(
            row.get("tranId") or row.get("clientTranId") or ""
        ).strip()
        if expected_id:
            if row_id == expected_id:
                return row
            continue
        if row_time <= max(0, int(after_time_ms)):
            continue
        return row
    return None


def _cash_transfer_balances_confirmed(
    snapshot: USDCSettlementSnapshot,
    *,
    direction: str,
    amount: float,
    pm_balance_before: float,
    spot_balance_before: float,
    tolerance: float,
) -> bool:
    expected = max(0.0, float(amount))
    allowed = max(MONEY_EPSILON, float(tolerance))
    pm_change = snapshot.total_wallet_balance - pm_balance_before
    spot_change = snapshot.spot_total - spot_balance_before
    if direction == SPOT_TO_PM:
        return spot_change <= -expected + allowed and pm_change >= (
            expected - allowed
        )
    if direction == PM_TO_SPOT:
        return pm_change <= -expected + allowed and spot_change >= (
            expected - allowed
        )
    return False


def _finite_number(value: object, field: str) -> float:
    try:
        parsed = float(value or 0)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"{field} must be finite") from exc
    if not math.isfinite(parsed):
        raise RuntimeError(f"{field} must be finite")
    return parsed


def _latest_cash_transfer_time(
    payload: object,
    *,
    direction: str,
) -> int:
    expected_types = set(
        _TRANSFER_TYPES.get(str(direction or "").upper(), ())
    )
    data = payload if isinstance(payload, dict) else {}
    return max(
        (
            int(row.get("timestamp", 0) or 0)
            for row in list(data.get("rows") or [])
            if isinstance(row, dict)
            and str(row.get("type") or "").upper() in expected_types
            and str(row.get("asset") or "").upper() == "USDC"
        ),
        default=0,
    )
