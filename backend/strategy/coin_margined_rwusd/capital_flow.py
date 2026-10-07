"""Restart-safe Binance deposit history for RWUSD capital registration."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Iterable, Mapping, Optional, Sequence

from backend.strategy.coin_margined_rwusd.portfolio import MONEY_QUANTUM


MAX_DEPOSIT_HISTORY_WINDOW_MS = 89 * 24 * 60 * 60 * 1000
SUCCESS_STATUS = 1
TERMINAL_STATUSES = frozenset({7})
# Binance's SDK enum includes 2 without documented success/terminal semantics.
UNRESOLVED_STATUSES = frozenset({0, 2, 6, 8})
SUPPORTED_STATUSES = (
    frozenset({SUCCESS_STATUS}) | TERMINAL_STATUSES | UNRESOLVED_STATUSES
)


@dataclass(frozen=True)
class ConfirmedExternalDeposit:
    external_reference: str
    asset: str
    amount: Decimal
    completed_at_ms: int
    transaction_id: str
    deposit_id: str
    network: str
    wallet_type: int

    def to_dict(self) -> dict:
        return {
            "external_reference": self.external_reference,
            "asset": self.asset,
            "amount": str(self.amount),
            "completed_at_ms": self.completed_at_ms,
            "transaction_id": self.transaction_id,
            "deposit_id": self.deposit_id,
            "network": self.network,
            "wallet_type": self.wallet_type,
        }


@dataclass(frozen=True)
class UnresolvedExternalDeposit:
    external_reference: str
    asset: str
    inserted_at_ms: int
    status: int
    transaction_id: str
    deposit_id: str

    def to_dict(self) -> dict:
        return {
            "external_reference": self.external_reference,
            "asset": self.asset,
            "inserted_at_ms": self.inserted_at_ms,
            "status": self.status,
            "transaction_id": self.transaction_id,
            "deposit_id": self.deposit_id,
        }


@dataclass(frozen=True)
class ExternalDepositHistoryBatch:
    confirmed: tuple[ConfirmedExternalDeposit, ...] = ()
    unresolved: tuple[UnresolvedExternalDeposit, ...] = ()
    terminal_references: tuple[str, ...] = ()

    def to_dict(self) -> dict:
        return {
            "confirmed": [item.to_dict() for item in self.confirmed],
            "unresolved": [item.to_dict() for item in self.unresolved],
            "terminal_references": list(self.terminal_references),
        }


def confirmed_external_deposits(
    payload: object,
    *,
    allowed_assets: Iterable[str],
    completed_after_ms: int,
    completed_through_ms: Optional[int] = None,
) -> list[ConfirmedExternalDeposit]:
    rows = payload if isinstance(payload, list) else []
    successful_rows = [
        row
        for row in rows
        if isinstance(row, Mapping)
        and _status(row.get("status")) == SUCCESS_STATUS
    ]
    return list(
        external_deposit_history(
            successful_rows,
            allowed_assets=allowed_assets,
            observed_after_ms=completed_after_ms,
            observed_through_ms=completed_through_ms,
        ).confirmed
    )


def external_deposit_history(
    payload: object,
    *,
    allowed_assets: Iterable[str],
    observed_after_ms: int,
    observed_through_ms: Optional[int] = None,
    unresolved_after_ms: Optional[int] = None,
) -> ExternalDepositHistoryBatch:
    allowed = {
        str(asset or "").strip().upper()
        for asset in allowed_assets
        if str(asset or "").strip()
    }
    rows = payload if isinstance(payload, list) else []
    unresolved_after = (
        int(observed_after_ms)
        if unresolved_after_ms is None
        else int(unresolved_after_ms)
    )
    confirmed: dict[str, ConfirmedExternalDeposit] = {}
    unresolved: dict[str, UnresolvedExternalDeposit] = {}
    terminal: set[str] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        asset = str(row.get("coin") or "").strip().upper()
        if asset not in allowed:
            continue
        status = _status(row.get("status"))
        if status is None:
            raise ValueError(f"Binance deposit status is invalid for {asset}")
        if status not in SUPPORTED_STATUSES:
            raise ValueError(
                f"Binance deposit status is unsupported for {asset}: {status}"
            )
        if status == SUCCESS_STATUS:
            completed_at = _positive_int(
                row.get("completeTime") or row.get("insertTime")
            )
            if not _within_scan(
                completed_at,
                observed_after_ms,
                observed_through_ms,
            ):
                continue
            reference, deposit_id, transaction_id = _deposit_identity(
                row,
                asset,
            )
            amount = _positive_decimal(row.get("amount"))
            if amount <= 0:
                raise ValueError(
                    f"Binance confirmed deposit amount is invalid: {reference}"
                )
            item = ConfirmedExternalDeposit(
                external_reference=reference,
                asset=asset,
                amount=amount,
                completed_at_ms=completed_at,
                transaction_id=transaction_id,
                deposit_id=deposit_id,
                network=str(row.get("network") or "").strip().upper(),
                wallet_type=_nonnegative_int(row.get("walletType")),
            )
            _insert_unique(confirmed, reference, item)
            continue

        inserted_at = _positive_int(row.get("insertTime"))
        if not _within_scan(
            inserted_at,
            unresolved_after,
            observed_through_ms,
        ):
            continue
        reference, deposit_id, transaction_id = _deposit_identity(row, asset)
        if status in TERMINAL_STATUSES:
            terminal.add(reference)
            continue
        item = UnresolvedExternalDeposit(
            external_reference=reference,
            asset=asset,
            inserted_at_ms=inserted_at,
            status=status,
            transaction_id=transaction_id,
            deposit_id=deposit_id,
        )
        _insert_unique(unresolved, reference, item)

    for reference in confirmed:
        unresolved.pop(reference, None)
        terminal.discard(reference)
    for reference in terminal:
        unresolved.pop(reference, None)
    return ExternalDepositHistoryBatch(
        confirmed=tuple(
            sorted(
                confirmed.values(),
                key=lambda item: (
                    item.completed_at_ms,
                    item.external_reference,
                ),
            )
        ),
        unresolved=tuple(
            sorted(
                unresolved.values(),
                key=lambda item: (
                    item.inserted_at_ms,
                    item.external_reference,
                ),
            )
        ),
        terminal_references=tuple(sorted(terminal)),
    )


async def fetch_external_deposit_history(
    client: object,
    *,
    allowed_assets: Iterable[str],
    time_ranges: Iterable[Sequence[int]],
    page_size: int = 1000,
    maximum_pages: int = 20,
    confirmed_after_ms: Optional[int] = None,
    unresolved_after_ms: Optional[int] = None,
) -> ExternalDepositHistoryBatch:
    ranges = _normalized_time_ranges(time_ranges)
    size = min(1000, max(1, int(page_size)))
    pages = max(1, int(maximum_pages))
    rows: list[object] = []
    loader = getattr(client, "get_deposit_history", None)
    if loader is None:
        raise RuntimeError("Binance deposit history interface is unavailable")
    for scan_start, scan_end in ranges:
        window_start = scan_start
        while window_start <= scan_end:
            window_end = min(
                scan_end,
                window_start + MAX_DEPOSIT_HISTORY_WINDOW_MS,
            )
            offset = 0
            for _ in range(pages):
                payload = await loader(
                    start_time_ms=window_start,
                    end_time_ms=window_end,
                    offset=offset,
                    limit=size,
                )
                if not isinstance(payload, list):
                    raise RuntimeError(
                        "Binance deposit history returned invalid "
                        "pagination data"
                    )
                rows.extend(payload)
                if len(payload) < size:
                    break
                offset += len(payload)
            else:
                raise RuntimeError(
                    "Binance deposit history exceeded the configured page "
                    "limit"
                )
            window_start = window_end + 1
    return external_deposit_history(
        rows,
        allowed_assets=allowed_assets,
        observed_after_ms=(
            ranges[0][0] - 1
            if confirmed_after_ms is None
            else int(confirmed_after_ms)
        ),
        observed_through_ms=max(end for _, end in ranges),
        unresolved_after_ms=unresolved_after_ms,
    )


def _normalized_time_ranges(
    time_ranges: Iterable[Sequence[int]],
) -> list[tuple[int, int]]:
    parsed: list[tuple[int, int]] = []
    for value in time_ranges:
        try:
            start, end = value
            start_ms = int(start)
            end_ms = int(end)
        except (TypeError, ValueError) as exc:
            raise ValueError("deposit history requires valid time ranges") from exc
        if start_ms <= 0 or end_ms < start_ms:
            raise ValueError("deposit history requires valid time ranges")
        parsed.append((start_ms, end_ms))
    if not parsed:
        raise ValueError("deposit history requires at least one time range")
    merged: list[tuple[int, int]] = []
    for start_ms, end_ms in sorted(parsed):
        if merged and start_ms <= merged[-1][1] + 1:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end_ms))
        else:
            merged.append((start_ms, end_ms))
    return merged


def _deposit_identity(
    row: Mapping[str, object],
    asset: str,
) -> tuple[str, str, str]:
    deposit_id = str(row.get("id") or "").strip()
    transaction_id = str(row.get("txId") or "").strip()
    identity = deposit_id or transaction_id
    if not identity:
        raise ValueError(f"Binance deposit identity is missing for {asset}")
    return (
        f"binance-deposit:{asset}:{identity}",
        deposit_id,
        transaction_id,
    )


def _insert_unique(target: dict, key: str, value: object) -> None:
    existing = target.get(key)
    if existing is not None and existing != value:
        raise RuntimeError(
            "Binance deposit history contains a conflicting reference"
        )
    target[key] = value


def _within_scan(
    timestamp_ms: int,
    after_ms: int,
    through_ms: Optional[int],
) -> bool:
    return (
        timestamp_ms > int(after_ms)
        and (
            through_ms is None
            or timestamp_ms <= int(through_ms)
        )
    )


def _status(value: object) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _positive_decimal(value: object) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")
    if not parsed.is_finite() or parsed <= 0:
        return Decimal("0")
    return parsed.quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)


def _positive_int(value: object) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _nonnegative_int(value: object) -> int:
    return _positive_int(value)
