"""Persistent RWUSD exit state and depth-aware reduction sizing."""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum
from typing import Iterable, Mapping, Sequence


EXIT_STATE_VERSION = 5
PREVIOUS_EXIT_STATE_VERSION = 4
MONEY_EPSILON = 1e-8
QUANTITY_EPSILON = 1e-12

_V5_SETTLEMENT_FIELDS = {
    "pending_cash_transfer_direction",
    "pending_cash_transfer_amount",
    "pending_cash_transfer_started_at_ms",
    "pending_cash_transfer_submission_confirmed",
    "pending_cash_transfer_id",
    "last_cash_transfer_history_time_ms",
    "signed_usdc_wallet_balance",
    "negative_balance_usdc",
    "futures_negative_usdc",
    "cross_margin_debt_usdc",
    "spot_usdc_balance",
    "settlement_action",
}


class RWUSDExitStage(str, Enum):
    INACTIVE = "INACTIVE"
    LIQUIDATING = "LIQUIDATING"
    READY_TO_REDEEM = "READY_TO_REDEEM"
    REDEEMING = "REDEEMING"
    REDEEM_PENDING = "REDEEM_PENDING"
    COMPLETE = "COMPLETE"


@dataclass
class RWUSDExitState:
    stage: RWUSDExitStage = RWUSDExitStage.INACTIVE
    reopen_locked: bool = False
    redemption_required: bool = False
    liquidation_requested_at_ms: int = 0
    redemption_requested_at_ms: int = 0
    completed_at_ms: int = 0
    last_progress_at_ms: int = 0
    retry_after_ms: int = 0
    failure_count: int = 0
    last_error: str | None = None
    liquidation_batch_count: int = 0
    liquidation_order_count: int = 0
    collateral_transfer_count: int = 0
    collateral_transferred_rwusd: float = 0.0
    pending_collateral_transfer_amount: float = 0.0
    pending_collateral_transfer_balance_before: float = 0.0
    pending_collateral_transfer_started_at_ms: int = 0
    pending_collateral_transfer_submission_confirmed: bool = False
    redemption_count: int = 0
    redeemed_rwusd: float = 0.0
    redemption_fee_usdc: float = 0.0
    expected_usdc: float = 0.0
    confirmed_usdc: float = 0.0
    pending_redemption_type: str | None = None
    pending_redemption_amount: float = 0.0
    pending_redemption_started_at_ms: int = 0
    pending_redemption_submission_confirmed: bool = False
    pending_rwusd_balance_before: float = 0.0
    pending_expected_usdc: float = 0.0
    pending_fee_usdc: float = 0.0
    pending_arrival_time_ms: int = 0
    pending_redemption_history_time_ms: int = 0
    last_redemption_history_time_ms: int = 0
    spot_usdc_baseline: float = 0.0
    pending_cash_transfer_direction: str | None = None
    pending_cash_transfer_amount: float = 0.0
    pending_cash_transfer_started_at_ms: int = 0
    pending_cash_transfer_submission_confirmed: bool = False
    pending_cash_transfer_id: str | None = None
    last_cash_transfer_history_time_ms: int = 0
    signed_usdc_wallet_balance: float = 0.0
    negative_balance_usdc: float = 0.0
    futures_negative_usdc: float = 0.0
    cross_margin_debt_usdc: float = 0.0
    spot_usdc_balance: float = 0.0
    settlement_action: str | None = None
    zero_balance_since_ms: int = 0

    @property
    def active(self) -> bool:
        return self.stage not in {
            RWUSDExitStage.INACTIVE,
            RWUSDExitStage.COMPLETE,
        }

    @property
    def redemption_requested(self) -> bool:
        return self.stage in {
            RWUSDExitStage.REDEEMING,
            RWUSDExitStage.REDEEM_PENDING,
        }

    def status_payload(self, *, restore_blocked: bool = False) -> dict:
        payload = self.to_dict()
        payload.update(
            {
                "active": self.active,
                "worker_required": self.stage
                in {
                    RWUSDExitStage.LIQUIDATING,
                    RWUSDExitStage.REDEEMING,
                    RWUSDExitStage.REDEEM_PENDING,
                },
                "can_start_liquidation": (
                    self.stage == RWUSDExitStage.INACTIVE
                ),
                "can_resume_liquidation": (
                    self.stage == RWUSDExitStage.LIQUIDATING
                ),
                "can_redeem": (
                    self.stage == RWUSDExitStage.READY_TO_REDEEM
                    and self.redemption_required
                ),
                "can_resume_redemption": self.stage
                in {
                    RWUSDExitStage.REDEEMING,
                    RWUSDExitStage.REDEEM_PENDING,
                },
                "can_explicitly_restart": (
                    self.stage == RWUSDExitStage.COMPLETE
                ),
                "net_usdc_after_liabilities": (
                    self.spot_usdc_balance
                    + self.signed_usdc_wallet_balance
                ),
            }
        )
        if restore_blocked:
            payload.update(
                {
                    "stage": "STATE_RESTORE_BLOCKED",
                    "reopen_locked": True,
                    "active": False,
                    "worker_required": False,
                    "can_start_liquidation": False,
                    "can_resume_liquidation": False,
                    "can_redeem": False,
                    "can_resume_redemption": False,
                    "can_explicitly_restart": False,
                }
            )
        return payload

    def request_liquidation(
        self,
        now_ms: int,
        *,
        redemption_required: bool = True,
    ) -> None:
        if self.active:
            return
        self.stage = RWUSDExitStage.LIQUIDATING
        self.reopen_locked = True
        self.redemption_required = bool(redemption_required)
        self.liquidation_requested_at_ms = max(1, int(now_ms))
        self.redemption_requested_at_ms = 0
        self.completed_at_ms = 0
        self.last_progress_at_ms = self.liquidation_requested_at_ms
        self.retry_after_ms = 0
        self.failure_count = 0
        self.last_error = None

    def mark_flat(self, now_ms: int) -> None:
        if self.stage != RWUSDExitStage.LIQUIDATING:
            raise RuntimeError(
                "RWUSD flat confirmation requires liquidation stage"
            )
        self.reopen_locked = True
        self.last_progress_at_ms = max(1, int(now_ms))
        self.retry_after_ms = 0
        self.failure_count = 0
        self.last_error = None
        if self.redemption_required:
            self.stage = RWUSDExitStage.READY_TO_REDEEM
            return
        self.stage = RWUSDExitStage.COMPLETE
        self.completed_at_ms = self.last_progress_at_ms

    def request_redemption(self, now_ms: int) -> None:
        if (
            self.stage != RWUSDExitStage.READY_TO_REDEEM
            or not self.redemption_required
        ):
            raise RuntimeError(
                "RWUSD redemption requires confirmed flat positions and orders"
            )
        self.stage = RWUSDExitStage.REDEEMING
        self.reopen_locked = True
        self.redemption_requested_at_ms = max(1, int(now_ms))
        self.last_progress_at_ms = self.redemption_requested_at_ms
        self.retry_after_ms = 0
        self.failure_count = 0
        self.last_error = None

    def begin_collateral_transfer(
        self,
        *,
        now_ms: int,
        amount: float,
        balance_before: float,
    ) -> None:
        if self.stage != RWUSDExitStage.REDEEMING:
            raise RuntimeError(
                "RWUSD collateral return requires redemption stage"
            )
        if self.pending_collateral_transfer_amount > MONEY_EPSILON:
            raise RuntimeError("RWUSD collateral return is already pending")
        transfer_amount = max(0.0, float(amount))
        before = max(0.0, float(balance_before))
        if (
            transfer_amount <= MONEY_EPSILON
            or before + MONEY_EPSILON < transfer_amount
        ):
            raise RuntimeError("RWUSD collateral return amount is invalid")
        self.pending_collateral_transfer_amount = transfer_amount
        self.pending_collateral_transfer_balance_before = before
        self.pending_collateral_transfer_started_at_ms = max(1, int(now_ms))
        self.pending_collateral_transfer_submission_confirmed = False
        self.last_progress_at_ms = self.pending_collateral_transfer_started_at_ms
        self.retry_after_ms = 0
        self.failure_count = 0
        self.last_error = None

    def confirm_collateral_transfer_submission(self, now_ms: int) -> None:
        if self.pending_collateral_transfer_amount <= MONEY_EPSILON:
            raise RuntimeError("RWUSD collateral return is not pending")
        self.pending_collateral_transfer_submission_confirmed = True
        self.last_progress_at_ms = max(1, int(now_ms))
        self.retry_after_ms = 0
        self.failure_count = 0
        self.last_error = None

    def confirm_collateral_transfer(self, now_ms: int) -> float:
        amount = self.pending_collateral_transfer_amount
        if amount <= MONEY_EPSILON:
            raise RuntimeError("RWUSD collateral return is not pending")
        self.collateral_transfer_count += 1
        self.collateral_transferred_rwusd += amount
        self._clear_pending_collateral_transfer()
        self.record_progress(now_ms)
        return amount

    def abandon_unconfirmed_collateral_transfer(self, now_ms: int) -> None:
        if (
            self.pending_collateral_transfer_amount <= MONEY_EPSILON
            or self.pending_collateral_transfer_submission_confirmed
        ):
            raise RuntimeError(
                "only an unconfirmed RWUSD collateral return can be abandoned"
            )
        self._clear_pending_collateral_transfer()
        self.record_progress(now_ms)

    def mark_redemption_pending(
        self,
        *,
        now_ms: int,
        redemption_type: str,
        amount: float,
        expected_usdc: float,
        fee_usdc: float,
        rwusd_balance_before: float,
        spot_usdc_baseline: float,
        arrival_time_ms: int = 0,
    ) -> None:
        if self.stage != RWUSDExitStage.REDEEMING:
            raise RuntimeError(
                "RWUSD redemption submission requires redemption stage"
            )
        self.stage = RWUSDExitStage.REDEEM_PENDING
        self.pending_redemption_type = str(redemption_type).strip().upper()
        self.pending_redemption_amount = max(0.0, float(amount))
        self.pending_redemption_started_at_ms = max(1, int(now_ms))
        self.pending_redemption_submission_confirmed = False
        self.pending_rwusd_balance_before = max(
            0.0,
            float(rwusd_balance_before),
        )
        self.pending_expected_usdc = max(0.0, float(expected_usdc))
        self.pending_fee_usdc = max(0.0, float(fee_usdc))
        self.pending_arrival_time_ms = max(0, int(arrival_time_ms))
        self.spot_usdc_baseline = max(0.0, float(spot_usdc_baseline))
        self.zero_balance_since_ms = 0
        self.last_progress_at_ms = self.pending_redemption_started_at_ms
        self.retry_after_ms = 0
        self.failure_count = 0
        self.last_error = None

    def confirm_redemption_submission(
        self,
        now_ms: int,
        *,
        expected_usdc: float | None = None,
        fee_usdc: float | None = None,
        arrival_time_ms: int | None = None,
        history_time_ms: int | None = None,
    ) -> None:
        if self.stage != RWUSDExitStage.REDEEM_PENDING:
            raise RuntimeError("RWUSD redemption is not pending")
        previous_expected = self.pending_expected_usdc
        previous_fee = self.pending_fee_usdc
        if expected_usdc is not None:
            self.pending_expected_usdc = max(0.0, float(expected_usdc))
        if fee_usdc is not None:
            self.pending_fee_usdc = max(0.0, float(fee_usdc))
        if arrival_time_ms is not None:
            self.pending_arrival_time_ms = max(0, int(arrival_time_ms))
        if history_time_ms is not None:
            matched_time = max(0, int(history_time_ms))
            if matched_time <= self.last_redemption_history_time_ms:
                raise RuntimeError("RWUSD redemption history row is stale")
            if (
                self.pending_redemption_history_time_ms > 0
                and matched_time != self.pending_redemption_history_time_ms
            ):
                raise RuntimeError("RWUSD redemption history row changed")
            self.pending_redemption_history_time_ms = matched_time
        if self.pending_redemption_submission_confirmed:
            self.expected_usdc = max(
                0.0,
                self.expected_usdc
                - previous_expected
                + self.pending_expected_usdc,
            )
            self.redemption_fee_usdc = max(
                0.0,
                self.redemption_fee_usdc
                - previous_fee
                + self.pending_fee_usdc,
            )
        else:
            self.pending_redemption_submission_confirmed = True
            self.expected_usdc += self.pending_expected_usdc
            self.redemption_fee_usdc += self.pending_fee_usdc
        self.last_progress_at_ms = max(1, int(now_ms))
        self.retry_after_ms = 0
        self.failure_count = 0
        self.last_error = None

    def abandon_unconfirmed_redemption(self, now_ms: int) -> None:
        if (
            self.stage != RWUSDExitStage.REDEEM_PENDING
            or self.pending_redemption_submission_confirmed
        ):
            raise RuntimeError(
                "only an unconfirmed RWUSD redemption can be abandoned"
            )
        self._clear_pending_redemption()
        self.stage = RWUSDExitStage.REDEEMING
        self.last_progress_at_ms = max(1, int(now_ms))
        self.retry_after_ms = 0
        self.failure_count = 0
        self.last_error = None

    def fail_pending_redemption(self, now_ms: int, error: object) -> None:
        if self.stage != RWUSDExitStage.REDEEM_PENDING:
            raise RuntimeError("RWUSD redemption is not pending")
        if self.pending_redemption_submission_confirmed:
            self.expected_usdc = max(
                0.0,
                self.expected_usdc - self.pending_expected_usdc,
            )
            self.redemption_fee_usdc = max(
                0.0,
                self.redemption_fee_usdc - self.pending_fee_usdc,
            )
        self._clear_pending_redemption()
        self.stage = RWUSDExitStage.REDEEMING
        self.last_progress_at_ms = max(1, int(now_ms))
        self.retry_after_ms = 0
        self.failure_count = 0
        self.last_error = str(error or "rwusd_redemption_failed")

    def confirm_pending_redemption(
        self,
        *,
        now_ms: int,
        confirmed_usdc: float,
    ) -> None:
        if (
            self.stage != RWUSDExitStage.REDEEM_PENDING
            or not self.pending_redemption_submission_confirmed
        ):
            raise RuntimeError(
                "RWUSD redemption submission is not confirmed"
            )
        settled_usdc = max(0.0, float(confirmed_usdc))
        actual_fee = max(
            0.0,
            self.pending_redemption_amount - settled_usdc,
        )
        self.expected_usdc = max(
            0.0,
            self.expected_usdc
            - self.pending_expected_usdc
            + settled_usdc,
        )
        self.redemption_fee_usdc = max(
            0.0,
            self.redemption_fee_usdc
            - self.pending_fee_usdc
            + actual_fee,
        )
        self.redemption_count += 1
        self.redeemed_rwusd += self.pending_redemption_amount
        self.confirmed_usdc += settled_usdc
        self.last_redemption_history_time_ms = max(
            self.last_redemption_history_time_ms,
            self.pending_redemption_history_time_ms,
        )
        self._clear_pending_redemption()
        self.stage = RWUSDExitStage.REDEEMING
        self.last_progress_at_ms = max(1, int(now_ms))
        self.retry_after_ms = 0
        self.failure_count = 0
        self.last_error = None

    def update_settlement_snapshot(
        self,
        *,
        signed_wallet_balance: float,
        negative_balance: float,
        futures_negative: float,
        cross_margin_debt: float,
        spot_usdc: float,
        action: str | None = None,
    ) -> None:
        self.signed_usdc_wallet_balance = float(signed_wallet_balance)
        self.negative_balance_usdc = max(0.0, float(negative_balance))
        self.futures_negative_usdc = max(0.0, float(futures_negative))
        self.cross_margin_debt_usdc = max(
            0.0,
            float(cross_margin_debt),
        )
        self.spot_usdc_balance = max(0.0, float(spot_usdc))
        if action is not None:
            self.settlement_action = _optional_text(action)

    def begin_cash_transfer(
        self,
        *,
        now_ms: int,
        direction: str,
        amount: float,
    ) -> None:
        if self.stage != RWUSDExitStage.REDEEMING:
            raise RuntimeError(
                "RWUSD cash settlement requires redemption stage"
            )
        if self.pending_cash_transfer_amount > MONEY_EPSILON:
            raise RuntimeError("RWUSD cash transfer is already pending")
        normalized_direction = str(direction or "").strip().upper()
        if normalized_direction not in {"SPOT_TO_PM", "PM_TO_SPOT"}:
            raise RuntimeError("RWUSD cash transfer direction is invalid")
        transfer_amount = max(0.0, float(amount))
        if transfer_amount <= MONEY_EPSILON:
            raise RuntimeError("RWUSD cash transfer amount is invalid")
        self.pending_cash_transfer_direction = normalized_direction
        self.pending_cash_transfer_amount = transfer_amount
        self.pending_cash_transfer_started_at_ms = max(1, int(now_ms))
        self.pending_cash_transfer_submission_confirmed = False
        self.pending_cash_transfer_id = None
        self.settlement_action = normalized_direction.lower()
        self.record_progress(now_ms)

    def confirm_cash_transfer_submission(
        self,
        now_ms: int,
        *,
        transfer_id: object = None,
    ) -> None:
        if self.pending_cash_transfer_amount <= MONEY_EPSILON:
            raise RuntimeError("RWUSD cash transfer is not pending")
        self.pending_cash_transfer_submission_confirmed = True
        normalized_id = str(transfer_id or "").strip()
        self.pending_cash_transfer_id = normalized_id or None
        self.record_progress(now_ms)

    def confirm_cash_transfer(
        self,
        now_ms: int,
        *,
        history_time_ms: int | None = None,
    ) -> tuple[str, float]:
        direction = str(
            self.pending_cash_transfer_direction or ""
        ).strip().upper()
        amount = self.pending_cash_transfer_amount
        if (
            direction not in {"SPOT_TO_PM", "PM_TO_SPOT"}
            or amount <= MONEY_EPSILON
        ):
            raise RuntimeError("RWUSD cash transfer confirmation is invalid")
        if history_time_ms is not None:
            matched_time = max(0, int(history_time_ms))
            if (
                matched_time <= 0
                or (
                    self.pending_cash_transfer_id is None
                    and matched_time <= self.last_cash_transfer_history_time_ms
                )
            ):
                raise RuntimeError(
                    "RWUSD cash transfer confirmation is invalid"
                )
            self.last_cash_transfer_history_time_ms = max(
                self.last_cash_transfer_history_time_ms,
                matched_time,
            )
        self._clear_pending_cash_transfer()
        self.record_progress(now_ms)
        return direction, amount

    def abandon_unconfirmed_cash_transfer(self, now_ms: int) -> None:
        if (
            self.pending_cash_transfer_amount <= MONEY_EPSILON
            or self.pending_cash_transfer_submission_confirmed
        ):
            raise RuntimeError(
                "only an unconfirmed RWUSD cash transfer can be abandoned"
            )
        self._clear_pending_cash_transfer()
        self.record_progress(now_ms)

    def fail_pending_cash_transfer(
        self,
        now_ms: int,
        error: object,
    ) -> None:
        if self.pending_cash_transfer_amount <= MONEY_EPSILON:
            raise RuntimeError("RWUSD cash transfer is not pending")
        self._clear_pending_cash_transfer()
        self.record_progress(now_ms)
        self.last_error = str(error or "rwusd_cash_transfer_failed")

    def record_futures_repay(self, now_ms: int) -> None:
        self.settlement_action = "futures_negative_balance_repaid"
        self.record_progress(now_ms)

    def mark_complete(self, now_ms: int) -> None:
        if self.stage != RWUSDExitStage.REDEEMING:
            raise RuntimeError(
                "RWUSD exit completion requires redemption stage"
            )
        if self.pending_collateral_transfer_amount > MONEY_EPSILON:
            raise RuntimeError(
                "RWUSD exit cannot complete with collateral return pending"
            )
        if self.pending_cash_transfer_amount > MONEY_EPSILON:
            raise RuntimeError(
                "RWUSD exit cannot complete with cash transfer pending"
            )
        self.stage = RWUSDExitStage.COMPLETE
        self.reopen_locked = True
        self.completed_at_ms = max(1, int(now_ms))
        self.last_progress_at_ms = self.completed_at_ms
        self.retry_after_ms = 0
        self.failure_count = 0
        self.last_error = None
        self._clear_pending_redemption()
        self.zero_balance_since_ms = 0

    def clear_for_explicit_restart(self) -> None:
        if self.stage != RWUSDExitStage.COMPLETE:
            raise RuntimeError("RWUSD exit is not complete")
        self.__dict__.update(RWUSDExitState().__dict__)

    def record_progress(self, now_ms: int) -> None:
        self.last_progress_at_ms = max(1, int(now_ms))
        self.retry_after_ms = 0
        self.failure_count = 0
        self.last_error = None

    def record_failure(
        self,
        *,
        now_ms: int,
        error: object,
        base_retry_ms: int,
        maximum_retry_ms: int,
    ) -> None:
        self.failure_count += 1
        delay = min(
            max(1, int(maximum_retry_ms)),
            max(1, int(base_retry_ms)) * (2 ** min(self.failure_count - 1, 6)),
        )
        self.retry_after_ms = max(1, int(now_ms)) + delay
        self.last_error = str(error or "rwusd_exit_failed")

    def mark_zero_balance(self, now_ms: int) -> None:
        if self.zero_balance_since_ms <= 0:
            self.zero_balance_since_ms = max(1, int(now_ms))

    def clear_zero_balance(self) -> None:
        self.zero_balance_since_ms = 0

    def to_dict(self) -> dict:
        return {
            "version": EXIT_STATE_VERSION,
            "stage": self.stage.value,
            "reopen_locked": self.reopen_locked,
            "redemption_required": self.redemption_required,
            "liquidation_requested_at_ms": self.liquidation_requested_at_ms,
            "redemption_requested_at_ms": self.redemption_requested_at_ms,
            "completed_at_ms": self.completed_at_ms,
            "last_progress_at_ms": self.last_progress_at_ms,
            "retry_after_ms": self.retry_after_ms,
            "failure_count": self.failure_count,
            "last_error": self.last_error,
            "liquidation_batch_count": self.liquidation_batch_count,
            "liquidation_order_count": self.liquidation_order_count,
            "collateral_transfer_count": self.collateral_transfer_count,
            "collateral_transferred_rwusd": (
                self.collateral_transferred_rwusd
            ),
            "pending_collateral_transfer_amount": (
                self.pending_collateral_transfer_amount
            ),
            "pending_collateral_transfer_balance_before": (
                self.pending_collateral_transfer_balance_before
            ),
            "pending_collateral_transfer_started_at_ms": (
                self.pending_collateral_transfer_started_at_ms
            ),
            "pending_collateral_transfer_submission_confirmed": (
                self.pending_collateral_transfer_submission_confirmed
            ),
            "redemption_count": self.redemption_count,
            "redeemed_rwusd": self.redeemed_rwusd,
            "redemption_fee_usdc": self.redemption_fee_usdc,
            "expected_usdc": self.expected_usdc,
            "confirmed_usdc": self.confirmed_usdc,
            "pending_redemption_type": self.pending_redemption_type,
            "pending_redemption_amount": self.pending_redemption_amount,
            "pending_redemption_started_at_ms": (
                self.pending_redemption_started_at_ms
            ),
            "pending_redemption_submission_confirmed": (
                self.pending_redemption_submission_confirmed
            ),
            "pending_rwusd_balance_before": (
                self.pending_rwusd_balance_before
            ),
            "pending_expected_usdc": self.pending_expected_usdc,
            "pending_fee_usdc": self.pending_fee_usdc,
            "pending_arrival_time_ms": self.pending_arrival_time_ms,
            "pending_redemption_history_time_ms": (
                self.pending_redemption_history_time_ms
            ),
            "last_redemption_history_time_ms": (
                self.last_redemption_history_time_ms
            ),
            "spot_usdc_baseline": self.spot_usdc_baseline,
            "pending_cash_transfer_direction": (
                self.pending_cash_transfer_direction
            ),
            "pending_cash_transfer_amount": (
                self.pending_cash_transfer_amount
            ),
            "pending_cash_transfer_started_at_ms": (
                self.pending_cash_transfer_started_at_ms
            ),
            "pending_cash_transfer_submission_confirmed": (
                self.pending_cash_transfer_submission_confirmed
            ),
            "pending_cash_transfer_id": self.pending_cash_transfer_id,
            "last_cash_transfer_history_time_ms": (
                self.last_cash_transfer_history_time_ms
            ),
            "signed_usdc_wallet_balance": self.signed_usdc_wallet_balance,
            "negative_balance_usdc": self.negative_balance_usdc,
            "futures_negative_usdc": self.futures_negative_usdc,
            "cross_margin_debt_usdc": self.cross_margin_debt_usdc,
            "spot_usdc_balance": self.spot_usdc_balance,
            "settlement_action": self.settlement_action,
            "zero_balance_since_ms": self.zero_balance_since_ms,
        }

    @classmethod
    def from_dict(cls, payload: object) -> "RWUSDExitState":
        if payload is None:
            return cls()
        if not isinstance(payload, dict):
            raise RuntimeError("RWUSD exit state is not an object")
        version = int(payload.get("version", 0) or 0)
        if version == PREVIOUS_EXIT_STATE_VERSION:
            legacy_required = set(cls().to_dict()).difference(
                _V5_SETTLEMENT_FIELDS
            )
            missing = sorted(legacy_required.difference(payload))
            if missing:
                raise RuntimeError(
                    "RWUSD exit state is missing fields: " + ",".join(missing)
                )
            payload = {
                **cls().to_dict(),
                **payload,
                "version": EXIT_STATE_VERSION,
            }
        elif version != EXIT_STATE_VERSION:
            raise RuntimeError("unsupported RWUSD exit state version")
        required = set(cls().to_dict())
        missing = sorted(required.difference(payload))
        if missing:
            raise RuntimeError(
                "RWUSD exit state is missing fields: " + ",".join(missing)
            )
        try:
            stage = RWUSDExitStage(str(payload.get("stage") or ""))
            state = cls(
                stage=stage,
                reopen_locked=_required_bool(payload, "reopen_locked"),
                redemption_required=_required_bool(
                    payload,
                    "redemption_required",
                ),
                liquidation_requested_at_ms=_non_negative_int(
                    payload, "liquidation_requested_at_ms"
                ),
                redemption_requested_at_ms=_non_negative_int(
                    payload, "redemption_requested_at_ms"
                ),
                completed_at_ms=_non_negative_int(
                    payload, "completed_at_ms"
                ),
                last_progress_at_ms=_non_negative_int(
                    payload, "last_progress_at_ms"
                ),
                retry_after_ms=_non_negative_int(payload, "retry_after_ms"),
                failure_count=_non_negative_int(payload, "failure_count"),
                last_error=_optional_text(payload.get("last_error")),
                liquidation_batch_count=_non_negative_int(
                    payload, "liquidation_batch_count"
                ),
                liquidation_order_count=_non_negative_int(
                    payload, "liquidation_order_count"
                ),
                collateral_transfer_count=_non_negative_int(
                    payload, "collateral_transfer_count"
                ),
                collateral_transferred_rwusd=_non_negative_float(
                    payload, "collateral_transferred_rwusd"
                ),
                pending_collateral_transfer_amount=_non_negative_float(
                    payload, "pending_collateral_transfer_amount"
                ),
                pending_collateral_transfer_balance_before=(
                    _non_negative_float(
                        payload,
                        "pending_collateral_transfer_balance_before",
                    )
                ),
                pending_collateral_transfer_started_at_ms=_non_negative_int(
                    payload,
                    "pending_collateral_transfer_started_at_ms",
                ),
                pending_collateral_transfer_submission_confirmed=(
                    _required_bool(
                        payload,
                        "pending_collateral_transfer_submission_confirmed",
                    )
                ),
                redemption_count=_non_negative_int(
                    payload, "redemption_count"
                ),
                redeemed_rwusd=_non_negative_float(
                    payload, "redeemed_rwusd"
                ),
                redemption_fee_usdc=_non_negative_float(
                    payload, "redemption_fee_usdc"
                ),
                expected_usdc=_non_negative_float(payload, "expected_usdc"),
                confirmed_usdc=_non_negative_float(
                    payload, "confirmed_usdc"
                ),
                pending_redemption_type=_optional_text(
                    payload.get("pending_redemption_type")
                ),
                pending_redemption_amount=_non_negative_float(
                    payload, "pending_redemption_amount"
                ),
                pending_redemption_started_at_ms=_non_negative_int(
                    payload, "pending_redemption_started_at_ms"
                ),
                pending_redemption_submission_confirmed=_required_bool(
                    payload,
                    "pending_redemption_submission_confirmed",
                ),
                pending_rwusd_balance_before=_non_negative_float(
                    payload, "pending_rwusd_balance_before"
                ),
                pending_expected_usdc=_non_negative_float(
                    payload, "pending_expected_usdc"
                ),
                pending_fee_usdc=_non_negative_float(
                    payload, "pending_fee_usdc"
                ),
                pending_arrival_time_ms=_non_negative_int(
                    payload, "pending_arrival_time_ms"
                ),
                pending_redemption_history_time_ms=_non_negative_int(
                    payload,
                    "pending_redemption_history_time_ms",
                ),
                last_redemption_history_time_ms=_non_negative_int(
                    payload,
                    "last_redemption_history_time_ms",
                ),
                spot_usdc_baseline=_non_negative_float(
                    payload, "spot_usdc_baseline"
                ),
                pending_cash_transfer_direction=_optional_text(
                    payload.get("pending_cash_transfer_direction")
                ),
                pending_cash_transfer_amount=_non_negative_float(
                    payload, "pending_cash_transfer_amount"
                ),
                pending_cash_transfer_started_at_ms=_non_negative_int(
                    payload, "pending_cash_transfer_started_at_ms"
                ),
                pending_cash_transfer_submission_confirmed=_required_bool(
                    payload,
                    "pending_cash_transfer_submission_confirmed",
                ),
                pending_cash_transfer_id=_optional_text(
                    payload.get("pending_cash_transfer_id")
                ),
                last_cash_transfer_history_time_ms=_non_negative_int(
                    payload,
                    "last_cash_transfer_history_time_ms",
                ),
                signed_usdc_wallet_balance=_finite_float(
                    payload, "signed_usdc_wallet_balance"
                ),
                negative_balance_usdc=_non_negative_float(
                    payload, "negative_balance_usdc"
                ),
                futures_negative_usdc=_non_negative_float(
                    payload, "futures_negative_usdc"
                ),
                cross_margin_debt_usdc=_non_negative_float(
                    payload, "cross_margin_debt_usdc"
                ),
                spot_usdc_balance=_non_negative_float(
                    payload, "spot_usdc_balance"
                ),
                settlement_action=_optional_text(
                    payload.get("settlement_action")
                ),
                zero_balance_since_ms=_non_negative_int(
                    payload, "zero_balance_since_ms"
                ),
            )
        except (TypeError, ValueError) as exc:
            raise RuntimeError("RWUSD exit state is invalid") from exc
        state._validate_invariants()
        return state

    def _validate_invariants(self) -> None:
        if self.stage == RWUSDExitStage.INACTIVE and self.reopen_locked:
            raise RuntimeError("inactive RWUSD exit cannot lock reopening")
        if (
            self.stage == RWUSDExitStage.INACTIVE
            and self.redemption_required
        ):
            raise RuntimeError(
                "inactive RWUSD exit cannot require redemption"
            )
        if self.active and not self.reopen_locked:
            raise RuntimeError("active RWUSD exit must lock reopening")
        if (
            self.stage == RWUSDExitStage.COMPLETE
            and not self.reopen_locked
        ):
            raise RuntimeError("completed RWUSD exit must lock reopening")
        if self.stage == RWUSDExitStage.INACTIVE:
            if any(
                (
                    self.liquidation_requested_at_ms,
                    self.redemption_requested_at_ms,
                    self.completed_at_ms,
                    self.last_progress_at_ms,
                )
            ):
                raise RuntimeError(
                    "inactive RWUSD exit contains lifecycle timestamps"
                )
        else:
            if (
                self.liquidation_requested_at_ms <= 0
                or self.last_progress_at_ms <= 0
            ):
                raise RuntimeError(
                    "active RWUSD exit is missing liquidation timestamps"
                )
            redemption_started = self.stage in {
                RWUSDExitStage.REDEEMING,
                RWUSDExitStage.REDEEM_PENDING,
            } or (
                self.stage == RWUSDExitStage.COMPLETE
                and self.redemption_required
            )
            if (
                redemption_started
                != (self.redemption_requested_at_ms > 0)
            ):
                raise RuntimeError(
                    "RWUSD exit redemption timestamp does not match stage"
                )
            completed = self.stage == RWUSDExitStage.COMPLETE
            if completed != (self.completed_at_ms > 0):
                raise RuntimeError(
                    "RWUSD exit completion timestamp does not match stage"
                )
            if (
                self.stage
                in {
                    RWUSDExitStage.READY_TO_REDEEM,
                    RWUSDExitStage.REDEEMING,
                    RWUSDExitStage.REDEEM_PENDING,
                }
                and not self.redemption_required
            ):
                raise RuntimeError(
                    "RWUSD redemption stage does not require redemption"
                )
        collateral_pending = (
            self.pending_collateral_transfer_amount > MONEY_EPSILON
        )
        if collateral_pending and (
            self.stage != RWUSDExitStage.REDEEMING
            or self.pending_collateral_transfer_started_at_ms <= 0
            or self.pending_collateral_transfer_balance_before
            + MONEY_EPSILON
            < self.pending_collateral_transfer_amount
        ):
            raise RuntimeError(
                "pending RWUSD collateral return state is incomplete"
            )
        if not collateral_pending and (
            self.pending_collateral_transfer_balance_before > MONEY_EPSILON
            or self.pending_collateral_transfer_started_at_ms > 0
            or self.pending_collateral_transfer_submission_confirmed
        ):
            raise RuntimeError(
                "RWUSD collateral return pending fields leaked stages"
            )
        if (
            self.stage == RWUSDExitStage.REDEEM_PENDING
            and (
                not self.pending_redemption_type
                or self.pending_redemption_amount <= 0
                or self.pending_redemption_started_at_ms <= 0
                or self.pending_rwusd_balance_before
                + MONEY_EPSILON
                < self.pending_redemption_amount
            )
        ):
            raise RuntimeError("pending RWUSD redemption state is incomplete")
        if (
            self.stage != RWUSDExitStage.REDEEM_PENDING
            and (
                self.pending_redemption_type is not None
                or self.pending_redemption_amount > MONEY_EPSILON
                or self.pending_redemption_started_at_ms > 0
                or self.pending_redemption_submission_confirmed
                or self.pending_rwusd_balance_before > MONEY_EPSILON
                or self.pending_expected_usdc > MONEY_EPSILON
                or self.pending_fee_usdc > MONEY_EPSILON
                or self.pending_arrival_time_ms > 0
                or self.pending_redemption_history_time_ms > 0
                or self.spot_usdc_baseline > MONEY_EPSILON
            )
        ):
            raise RuntimeError("RWUSD redemption pending fields leaked stages")
        if (
            self.pending_redemption_submission_confirmed
            and self.pending_expected_usdc <= 0
        ):
            raise RuntimeError(
                "confirmed RWUSD redemption has no expected USDC"
            )
        if (
            self.pending_redemption_history_time_ms > 0
            and self.pending_redemption_history_time_ms
            <= self.last_redemption_history_time_ms
        ):
            raise RuntimeError("pending RWUSD redemption history row is stale")
        cash_pending = self.pending_cash_transfer_amount > MONEY_EPSILON
        if cash_pending:
            if (
                self.stage != RWUSDExitStage.REDEEMING
                or self.pending_cash_transfer_direction
                not in {"SPOT_TO_PM", "PM_TO_SPOT"}
                or self.pending_cash_transfer_started_at_ms <= 0
            ):
                raise RuntimeError(
                    "pending RWUSD cash transfer state is incomplete"
                )
        elif (
            self.pending_cash_transfer_direction is not None
            or self.pending_cash_transfer_started_at_ms > 0
            or self.pending_cash_transfer_submission_confirmed
            or self.pending_cash_transfer_id is not None
        ):
            raise RuntimeError(
                "RWUSD cash transfer pending fields leaked stages"
            )

    def _clear_pending_redemption(self) -> None:
        self.pending_redemption_type = None
        self.pending_redemption_amount = 0.0
        self.pending_redemption_started_at_ms = 0
        self.pending_redemption_submission_confirmed = False
        self.pending_rwusd_balance_before = 0.0
        self.pending_expected_usdc = 0.0
        self.pending_fee_usdc = 0.0
        self.pending_arrival_time_ms = 0
        self.pending_redemption_history_time_ms = 0
        self.spot_usdc_baseline = 0.0

    def _clear_pending_collateral_transfer(self) -> None:
        self.pending_collateral_transfer_amount = 0.0
        self.pending_collateral_transfer_balance_before = 0.0
        self.pending_collateral_transfer_started_at_ms = 0
        self.pending_collateral_transfer_submission_confirmed = False

    def _clear_pending_cash_transfer(self) -> None:
        self.pending_cash_transfer_direction = None
        self.pending_cash_transfer_amount = 0.0
        self.pending_cash_transfer_started_at_ms = 0
        self.pending_cash_transfer_submission_confirmed = False
        self.pending_cash_transfer_id = None


@dataclass(frozen=True)
class ReductionPlan:
    long_quantity: float = 0.0
    short_quantity: float = 0.0
    desired_fraction: float = 0.0
    bid_depth_quantity: float = 0.0
    ask_depth_quantity: float = 0.0
    reason: str = ""

    @property
    def executable(self) -> bool:
        return (
            self.long_quantity > QUANTITY_EPSILON
            or self.short_quantity > QUANTITY_EPSILON
        )


def hard_risk_reduction_fraction(
    *,
    uni_mmr: float,
    target_uni_mmr: float,
    minimum_fraction: float,
    maximum_fraction: float,
) -> float:
    minimum = min(1.0, max(0.0, float(minimum_fraction)))
    maximum = min(1.0, max(minimum, float(maximum_fraction)))
    current = max(0.0, float(uni_mmr))
    target = max(MONEY_EPSILON, float(target_uni_mmr))
    required = 1.0 - min(1.0, current / target)
    return min(maximum, max(minimum, required))


def build_reduction_plan(
    *,
    long_quantity: float,
    short_quantity: float,
    order_book: Mapping[str, object],
    desired_fraction: float,
    max_slippage_bps: float,
    depth_utilization: float,
    max_notional_per_side: float,
) -> ReductionPlan:
    long_qty = max(0.0, float(long_quantity))
    short_qty = max(0.0, float(short_quantity))
    fraction = min(1.0, max(0.0, float(desired_fraction)))
    if fraction <= 0 or long_qty + short_qty <= QUANTITY_EPSILON:
        return ReductionPlan(reason="no_position")

    bids = _levels(order_book.get("bids"))
    asks = _levels(order_book.get("asks"))
    if not bids or not asks:
        return ReductionPlan(
            desired_fraction=fraction,
            reason="order_book_unavailable",
        )
    best_bid = bids[0][0]
    best_ask = asks[0][0]
    slippage = max(0.0, float(max_slippage_bps)) / 10000.0
    utilization = min(1.0, max(0.0, float(depth_utilization)))
    bid_depth = _depth_quantity(
        bids,
        lower_bound=best_bid * (1.0 - slippage),
    ) * utilization
    ask_depth = _depth_quantity(
        asks,
        upper_bound=best_ask * (1.0 + slippage),
    ) * utilization
    maximum_notional = max(0.0, float(max_notional_per_side))
    max_long = min(
        long_qty,
        bid_depth,
        maximum_notional / best_bid if maximum_notional > 0 else long_qty,
    )
    max_short = min(
        short_qty,
        ask_depth,
        maximum_notional / best_ask if maximum_notional > 0 else short_qty,
    )
    desired_total = (long_qty + short_qty) * fraction

    close_long = 0.0
    close_short = 0.0
    if long_qty > short_qty:
        close_long = min(max_long, long_qty - short_qty, desired_total)
    elif short_qty > long_qty:
        close_short = min(max_short, short_qty - long_qty, desired_total)
    remaining = max(0.0, desired_total - close_long - close_short)
    pair_quantity = min(
        max(0.0, max_long - close_long),
        max(0.0, max_short - close_short),
        remaining / 2.0,
    )
    close_long += pair_quantity
    close_short += pair_quantity

    if close_long <= QUANTITY_EPSILON and close_short <= QUANTITY_EPSILON:
        reason = "depth_or_notional_limit"
    elif abs(close_long - close_short) > QUANTITY_EPSILON:
        reason = "reduce_excess_then_pair"
    else:
        reason = "reduce_balanced_pair"
    return ReductionPlan(
        long_quantity=close_long,
        short_quantity=close_short,
        desired_fraction=fraction,
        bid_depth_quantity=bid_depth,
        ask_depth_quantity=ask_depth,
        reason=reason,
    )


def _levels(payload: object) -> list[tuple[float, float]]:
    output: list[tuple[float, float]] = []
    for row in payload if isinstance(payload, Sequence) else ():
        if not isinstance(row, Sequence) or len(row) < 2:
            continue
        try:
            price = float(row[0])
            quantity = float(row[1])
        except (TypeError, ValueError):
            continue
        if math.isfinite(price) and math.isfinite(quantity):
            if price > 0 and quantity > 0:
                output.append((price, quantity))
    return output


def _depth_quantity(
    levels: Iterable[tuple[float, float]],
    *,
    lower_bound: float = 0.0,
    upper_bound: float = math.inf,
) -> float:
    return sum(
        quantity
        for price, quantity in levels
        if lower_bound <= price <= upper_bound
    )


def _required_bool(payload: Mapping[str, object], key: str) -> bool:
    value = payload.get(key)
    if not isinstance(value, bool):
        raise TypeError(f"{key} must be bool")
    return value


def _non_negative_int(payload: Mapping[str, object], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool):
        raise TypeError(f"{key} must be int")
    parsed = int(value)
    if parsed < 0:
        raise ValueError(f"{key} must be non-negative")
    return parsed


def _non_negative_float(payload: Mapping[str, object], key: str) -> float:
    value = payload.get(key)
    if isinstance(value, bool):
        raise TypeError(f"{key} must be numeric")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0:
        raise ValueError(f"{key} must be finite and non-negative")
    return parsed


def _finite_float(payload: Mapping[str, object], key: str) -> float:
    value = payload.get(key)
    if isinstance(value, bool):
        raise TypeError(f"{key} must be numeric")
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"{key} must be finite")
    return parsed


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError("optional text field must be string or null")
    return value or None
