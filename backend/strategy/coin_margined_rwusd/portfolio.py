"""Minimal external-capital ledger for multi-symbol RWUSD allocation."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Dict, Iterable, Mapping, Optional


MONEY_QUANTUM = Decimal("0.00000001")
ZERO = Decimal("0")


def money(value: object) -> Decimal:
    try:
        parsed = Decimal(str(0 if value is None else value))
    except (InvalidOperation, TypeError, ValueError):
        return ZERO
    if not parsed.is_finite() or parsed <= 0:
        return ZERO
    return parsed.quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)


def money_text(value: Decimal) -> str:
    return format(
        value.quantize(MONEY_QUANTUM, rounding=ROUND_DOWN),
        "f",
    )


def nonnegative_money(value: object, field: str) -> Decimal:
    try:
        parsed = Decimal(str(0 if value is None else value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field} is invalid") from exc
    if not parsed.is_finite() or parsed < 0:
        raise ValueError(f"{field} must be nonnegative")
    return parsed.quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)


def _allocation_map(value: object) -> Dict[str, Decimal]:
    if not isinstance(value, Mapping):
        raise ValueError("capital allocations must be an object")
    normalized: Dict[str, Decimal] = {}
    for raw_symbol, raw_amount in value.items():
        symbol = str(raw_symbol or "").strip().upper()
        amount = money(raw_amount)
        if not symbol:
            raise ValueError("capital allocation symbol is required")
        if amount <= 0:
            raise ValueError(
                f"capital allocation must be positive for {symbol}"
            )
        if symbol in normalized:
            raise ValueError(f"duplicate capital allocation symbol: {symbol}")
        normalized[symbol] = amount
    return normalized


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_timestamp(value: object, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} is required")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} is invalid") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat()


@dataclass
class CapitalLot:
    lot_id: str
    external_reference: str
    source_asset: str
    amount: Decimal
    created_at: str
    collateralized_amount: Decimal = ZERO
    minimum_reserve: Decimal = ZERO
    capital_contribution_before: Decimal = ZERO
    allocations: Dict[str, Decimal] = field(default_factory=dict)

    @property
    def allocated_total(self) -> Decimal:
        return sum(self.allocations.values(), ZERO).quantize(
            MONEY_QUANTUM,
            rounding=ROUND_DOWN,
        )

    @property
    def reserve_amount(self) -> Decimal:
        return max(ZERO, self.amount - self.allocated_total).quantize(
            MONEY_QUANTUM,
            rounding=ROUND_DOWN,
        )

    @property
    def allocatable_amount(self) -> Decimal:
        return max(
            ZERO,
            self.amount - self.minimum_reserve - self.allocated_total,
        ).quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)

    @property
    def pending_collateralization(self) -> Decimal:
        return max(
            ZERO,
            self.amount - self.collateralized_amount,
        ).quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)

    @property
    def deployed_allocations(self) -> Dict[str, Decimal]:
        allocated = self.allocated_total
        if allocated <= 0 or self.collateralized_amount <= 0:
            return {}
        deployed_total = min(allocated, self.collateralized_amount)
        if deployed_total == allocated:
            return dict(self.allocations)
        allocated_units = int(allocated / MONEY_QUANTUM)
        deployed_units = int(deployed_total / MONEY_QUANTUM)
        base_units: Dict[str, int] = {}
        remainders: Dict[str, int] = {}
        for symbol, amount in self.allocations.items():
            amount_units = int(amount / MONEY_QUANTUM)
            numerator = amount_units * deployed_units
            base_units[symbol], remainders[symbol] = divmod(
                numerator,
                allocated_units,
            )
        residual_units = deployed_units - sum(base_units.values())
        ranked = sorted(
            base_units,
            key=lambda symbol: (-remainders[symbol], symbol),
        )
        if residual_units > len(ranked):
            raise RuntimeError("capital deployment apportionment is invalid")
        for symbol in ranked[:residual_units]:
            base_units[symbol] += 1
        return {
            symbol: MONEY_QUANTUM * units
            for symbol, units in base_units.items()
            if units > 0
        }

    def to_dict(self) -> dict:
        return {
            "lot_id": self.lot_id,
            "external_reference": self.external_reference,
            "source_asset": self.source_asset,
            "amount": money_text(self.amount),
            "created_at": self.created_at,
            "collateralized_amount": money_text(
                self.collateralized_amount
            ),
            "minimum_reserve": money_text(self.minimum_reserve),
            "capital_contribution_before": money_text(
                self.capital_contribution_before
            ),
            "allocations": {
                symbol: money_text(amount)
                for symbol, amount in self.allocations.items()
            },
        }

    @classmethod
    def from_dict(cls, payload: object) -> "CapitalLot":
        if not isinstance(payload, Mapping):
            raise ValueError("capital lot must be an object")
        data = payload
        required = {
            "lot_id",
            "external_reference",
            "source_asset",
            "amount",
            "created_at",
            "collateralized_amount",
            "minimum_reserve",
            "allocations",
        }
        missing = sorted(required.difference(data))
        if missing:
            raise ValueError(
                "capital lot is missing fields: " + ",".join(missing)
            )
        amount = money(data.get("amount"))
        lot_id = str(data.get("lot_id") or "").strip()
        external_reference = str(
            data.get("external_reference") or ""
        ).strip()
        source_asset = str(data.get("source_asset") or "").strip().upper()
        created_at = _utc_timestamp(
            data.get("created_at"),
            "capital lot creation time",
        )
        if amount <= 0:
            raise ValueError("capital lot amount must be positive")
        if not lot_id:
            raise ValueError("capital lot id is required")
        if not external_reference:
            raise ValueError("capital lot external reference is required")
        if not source_asset:
            raise ValueError("capital lot source asset is required")
        collateralized = nonnegative_money(
            data.get("collateralized_amount"),
            "collateralized amount",
        )
        minimum_reserve = nonnegative_money(
            data.get("minimum_reserve"),
            "minimum reserve",
        )
        capital_contribution_before = nonnegative_money(
            data.get("capital_contribution_before", 0),
            "capital contribution before",
        )
        return cls(
            lot_id=lot_id,
            external_reference=external_reference,
            source_asset=source_asset,
            amount=amount,
            created_at=created_at,
            collateralized_amount=collateralized,
            minimum_reserve=minimum_reserve,
            capital_contribution_before=capital_contribution_before,
            allocations=_allocation_map(data.get("allocations")),
        )


@dataclass
class PortfolioLedger:
    capital_lots: Dict[str, CapitalLot] = field(default_factory=dict)
    next_capital_number: int = 1

    @property
    def total_capital(self) -> Decimal:
        return sum(
            (lot.amount for lot in self.capital_lots.values()),
            ZERO,
        ).quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)

    @property
    def allocated_capital(self) -> Dict[str, Decimal]:
        return self._sum_by_symbol("allocations")

    @property
    def deployed_capital(self) -> Dict[str, Decimal]:
        return self._sum_by_symbol("deployed_allocations")

    @property
    def reserve_capital(self) -> Decimal:
        return sum(
            (lot.reserve_amount for lot in self.capital_lots.values()),
            ZERO,
        ).quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)

    @property
    def allocatable_reserve(self) -> Decimal:
        return sum(
            (lot.allocatable_amount for lot in self.capital_lots.values()),
            ZERO,
        ).quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)

    @property
    def pending_collateralization(self) -> Decimal:
        return sum(
            (
                lot.pending_collateralization
                for lot in self.capital_lots.values()
            ),
            ZERO,
        ).quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)

    def capital_lot_by_reference(
        self,
        external_reference: object,
    ) -> Optional[CapitalLot]:
        reference = str(external_reference or "").strip()
        if not reference:
            return None
        return next(
            (
                lot
                for lot in self.capital_lots.values()
                if lot.external_reference == reference
            ),
            None,
        )

    def register_capital(
        self,
        *,
        amount: object,
        source_asset: str,
        external_reference: str,
        created_at: Optional[str] = None,
        collateralized: bool = True,
        capital_contribution_before: object = ZERO,
    ) -> CapitalLot:
        value = money(amount)
        reference = str(external_reference or "").strip()
        asset = str(source_asset or "").strip().upper()
        if value <= 0:
            raise ValueError("capital amount must be positive")
        if not asset:
            raise ValueError("capital source asset is required")
        if not reference:
            raise ValueError("external capital reference is required")
        existing = self.capital_lot_by_reference(reference)
        if existing is not None:
            baseline = nonnegative_money(
                capital_contribution_before,
                "capital contribution before",
            )
            if (
                existing.amount != value
                or existing.source_asset != asset
                or existing.capital_contribution_before != baseline
            ):
                raise ValueError(
                    "external capital reference conflicts with existing lot"
                )
            return existing
        lot = CapitalLot(
            lot_id=self._next_lot_id(),
            external_reference=reference,
            source_asset=asset,
            amount=value,
            created_at=(
                _utc_timestamp(created_at, "capital creation time")
                if created_at is not None
                else _utc_now()
            ),
            collateralized_amount=value if collateralized else ZERO,
            capital_contribution_before=nonnegative_money(
                capital_contribution_before,
                "capital contribution before",
            ),
        )
        self.capital_lots[lot.lot_id] = lot
        return lot

    def allocate_lot(
        self,
        lot_id: str,
        allocations: Mapping[str, object],
        *,
        minimum_reserve: object = ZERO,
    ) -> Dict[str, Decimal]:
        lot = self.capital_lots.get(str(lot_id or ""))
        if lot is None:
            raise KeyError(f"unknown capital lot: {lot_id}")
        normalized = self._normalize_allocations(allocations)
        allocated = sum(normalized.values(), ZERO)
        reserve = min(
            lot.amount,
            nonnegative_money(minimum_reserve, "minimum reserve"),
        )
        if allocated + reserve > lot.amount:
            raise ValueError(
                "capital allocation and reserve exceed lot amount"
            )
        lot.allocations = normalized
        lot.minimum_reserve = reserve
        return dict(normalized)

    def add_allocations(
        self,
        allocations: Mapping[str, object],
    ) -> Dict[str, Decimal]:
        remaining = self._normalize_allocations(allocations)
        if sum(remaining.values(), ZERO) > self.allocatable_reserve:
            raise ValueError("allocation exceeds allocatable reserve")
        applied = {symbol: ZERO for symbol in remaining}
        for lot in sorted(
            self.capital_lots.values(),
            key=lambda item: (item.created_at, item.lot_id),
        ):
            available = lot.allocatable_amount
            if available <= 0:
                continue
            for symbol in list(remaining):
                addition = min(available, remaining[symbol])
                if addition <= 0:
                    continue
                lot.allocations[symbol] = (
                    lot.allocations.get(symbol, ZERO) + addition
                ).quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)
                applied[symbol] += addition
                remaining[symbol] -= addition
                available -= addition
                if remaining[symbol] <= 0:
                    remaining.pop(symbol)
                if available <= 0:
                    break
            if not remaining:
                break
        if remaining:
            raise RuntimeError("capital reserve allocation was incomplete")
        return {
            symbol: amount.quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)
            for symbol, amount in applied.items()
            if amount > 0
        }

    def release_allocations(
        self,
        symbols: Iterable[str],
    ) -> Dict[str, Decimal]:
        targets = {
            str(symbol or "").strip().upper()
            for symbol in symbols
            if str(symbol or "").strip()
        }
        released: Dict[str, Decimal] = {}
        if not targets:
            return released
        for lot in self.capital_lots.values():
            for symbol in list(lot.allocations):
                if symbol not in targets:
                    continue
                amount = lot.allocations.pop(symbol)
                released[symbol] = (
                    released.get(symbol, ZERO) + amount
                ).quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)
        return released

    def release_excess_allocations(
        self,
        maximum_allocations: Mapping[str, object],
    ) -> Dict[str, Decimal]:
        limits = {
            str(symbol or "").strip().upper(): nonnegative_money(
                amount,
                "maximum allocation",
            )
            for symbol, amount in maximum_allocations.items()
            if str(symbol or "").strip()
        }
        released: Dict[str, Decimal] = {}
        current = self.allocated_capital
        for symbol, amount in current.items():
            limit = limits.get(symbol)
            if limit is None or amount <= limit:
                continue
            remaining = amount - limit
            for lot in reversed(
                sorted(
                    self.capital_lots.values(),
                    key=lambda item: (item.created_at, item.lot_id),
                )
            ):
                lot_amount = lot.allocations.get(symbol, ZERO)
                if lot_amount <= ZERO:
                    continue
                reduction = min(lot_amount, remaining)
                updated = lot_amount - reduction
                if updated > ZERO:
                    lot.allocations[symbol] = updated.quantize(
                        MONEY_QUANTUM,
                        rounding=ROUND_DOWN,
                    )
                else:
                    lot.allocations.pop(symbol, None)
                released[symbol] = (
                    released.get(symbol, ZERO) + reduction
                ).quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)
                remaining -= reduction
                if remaining <= ZERO:
                    break
            if remaining > ZERO:
                raise RuntimeError(
                    f"allocation release was incomplete for {symbol}"
                )
        return released

    def mark_capital_collateralized(
        self,
        lot_id: str,
        amount: object,
    ) -> Decimal:
        lot = self.capital_lots.get(str(lot_id or ""))
        if lot is None:
            raise KeyError(f"unknown capital lot: {lot_id}")
        addition = min(
            lot.pending_collateralization,
            nonnegative_money(amount, "collateralized amount"),
        )
        if addition <= 0:
            return ZERO.quantize(MONEY_QUANTUM)
        lot.collateralized_amount = min(
            lot.amount,
            lot.collateralized_amount + addition,
        ).quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)
        return addition.quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)

    def reset_collateralization(self) -> Decimal:
        reset = sum(
            (
                lot.collateralized_amount
                for lot in self.capital_lots.values()
            ),
            ZERO,
        ).quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)
        for lot in self.capital_lots.values():
            lot.collateralized_amount = ZERO
        return reset

    def validate(self) -> None:
        references: set[str] = set()
        for lot_id, lot in self.capital_lots.items():
            if not lot.lot_id or lot.amount <= 0:
                raise ValueError("capital ledger contains an invalid lot")
            if lot_id != lot.lot_id:
                raise ValueError("capital ledger lot key does not match lot id")
            if (
                not lot.external_reference
                or lot.external_reference in references
            ):
                raise ValueError(
                    "capital ledger contains duplicate external references"
                )
            references.add(lot.external_reference)
            if lot.collateralized_amount > lot.amount:
                raise ValueError(
                    "collateralized principal exceeds capital lot"
                )
            if (
                lot.minimum_reserve > lot.amount
                or lot.allocated_total + lot.minimum_reserve > lot.amount
            ):
                raise ValueError(
                    "capital allocation and reserve exceed capital lot"
                )

    def to_dict(self) -> dict:
        return {
            "capital_lots": {
                lot_id: lot.to_dict()
                for lot_id, lot in self.capital_lots.items()
            },
            "next_capital_number": self.next_capital_number,
            "total_capital": money_text(self.total_capital),
            "allocated_capital": {
                symbol: money_text(amount)
                for symbol, amount in self.allocated_capital.items()
            },
            "deployed_capital": {
                symbol: money_text(amount)
                for symbol, amount in self.deployed_capital.items()
            },
            "reserve_capital": money_text(self.reserve_capital),
            "allocatable_reserve": money_text(self.allocatable_reserve),
            "pending_collateralization": money_text(
                self.pending_collateralization
            ),
        }

    @classmethod
    def from_dict(cls, payload: object) -> "PortfolioLedger":
        if not isinstance(payload, Mapping):
            raise ValueError("capital ledger must be an object")
        data = payload
        lots = data.get("capital_lots")
        if not isinstance(lots, Mapping):
            raise ValueError("capital ledger lots must be an object")
        restored_lots: Dict[str, CapitalLot] = {}
        for lot_id, lot_payload in lots.items():
            if not isinstance(lot_payload, Mapping):
                raise ValueError(
                    f"capital ledger lot is invalid: {lot_id}"
                )
            restored_lots[str(lot_id)] = CapitalLot.from_dict(lot_payload)
        try:
            next_capital_number = int(
                data.get("next_capital_number", 1) or 1
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "capital ledger next number is invalid"
            ) from exc
        ledger = cls(
            capital_lots=restored_lots,
            next_capital_number=max(1, next_capital_number),
        )
        ledger.validate()
        return ledger

    def _next_lot_id(self) -> str:
        number = max(1, self.next_capital_number)
        while f"capital-{number}" in self.capital_lots:
            number += 1
        self.next_capital_number = number + 1
        return f"capital-{number}"

    def _sum_by_symbol(self, attribute: str) -> Dict[str, Decimal]:
        totals: Dict[str, Decimal] = {}
        for lot in self.capital_lots.values():
            values = getattr(lot, attribute)
            for symbol, amount in values.items():
                totals[symbol] = totals.get(symbol, ZERO) + amount
        return {
            symbol: amount.quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)
            for symbol, amount in totals.items()
            if amount > 0
        }

    @staticmethod
    def _normalize_allocations(
        allocations: Mapping[str, object],
    ) -> Dict[str, Decimal]:
        return _allocation_map(allocations)
