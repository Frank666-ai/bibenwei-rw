"""Restart-safe planning for confirmed external RWUSD capital."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from typing import Iterator, Iterable, Mapping, Optional

from backend.strategy.coin_margined_rwusd.allocator import (
    AllocationResult,
    RWUSDCapitalAllocator,
)
from backend.strategy.coin_margined_rwusd.capital_bootstrap import OfficialCapitalSnapshot
from backend.strategy.coin_margined_rwusd.capital_flow import (
    ConfirmedExternalDeposit,
    ExternalDepositHistoryBatch,
    UnresolvedExternalDeposit,
)
from backend.strategy.coin_margined_rwusd.persistence import (
    PORTFOLIO_STATE_SUFFIX,
    RWUSDStateStore,
)
from backend.strategy.coin_margined_rwusd.portfolio import (
    CapitalLot,
    MONEY_QUANTUM,
    PortfolioLedger,
    ZERO,
    money,
    nonnegative_money,
)


PORTFOLIO_STATE_VERSION = 18
LEGACY_PORTFOLIO_STATE_VERSIONS = frozenset({16, 17})
PORTFOLIO_STORE_SUFFIX = PORTFOLIO_STATE_SUFFIX
CONFIGURED_CAPITAL_PREFIX = "configured-initial:"


class RWUSDPortfolioCoordinator:
    """Own account capital allocation across executable symbol lanes."""

    def __init__(
        self,
        *,
        account_name: str,
        config: object,
        execution_environment: str = "dry_run",
        live_rwusd_execution: bool = False,
        state_store: Optional[RWUSDStateStore] = None,
    ) -> None:
        self.account_name = str(account_name)
        self.config = config
        self.execution_environment = str(
            execution_environment or "dry_run"
        ).strip()
        self.live_rwusd_execution = bool(live_rwusd_execution)
        self.state_store = state_store or RWUSDStateStore(
            f"{self.account_name}{PORTFOLIO_STORE_SUFFIX}"
        )
        self.ledger = PortfolioLedger()
        self.last_allocation: Optional[AllocationResult] = None
        self.deposit_detection_started_at_ms = 0
        self.deposit_scan_completed_through_ms = 0
        self.unresolved_deposit_inserted_at_ms: dict[str, int] = {}
        self.capital_bootstrap_source = ""
        self.capital_bootstrap_observed_at_ms = 0
        self.initial_collateralization_target = ZERO
        self.configured_capital_migration_required = False
        self.configured_capital_migration_symbols: tuple[str, ...] = ()
        self._last_persisted_payload: Optional[dict] = None
        self.allocator = self._build_allocator()

    def _build_allocator(self) -> RWUSDCapitalAllocator:
        config = self.config
        return RWUSDCapitalAllocator(
            margin_fraction=float(
                getattr(config, "TOTAL_MARGIN_FRACTION", 0.80)
            ),
            minimum_daily_return=float(
                getattr(config, "ALLOCATION_MIN_DAILY_RETURN", 0.10)
            ),
            maximum_spread_bps=float(
                getattr(config, "ALLOCATION_MAX_SPREAD_BPS", 5.0)
            ),
            official_capacity_haircut=float(
                getattr(
                    config,
                    "ALLOCATION_OFFICIAL_CAPACITY_HAIRCUT",
                    0.75,
                )
            ),
            liquidity_reserve_fraction=float(
                getattr(config, "ALLOCATION_LIQUIDITY_RESERVE", 0.05)
            ),
            maximum_symbol_fraction=float(
                getattr(config, "ALLOCATION_MAX_SYMBOL_FRACTION", 0.25)
            ),
            target_weights=getattr(
                config,
                "ALLOCATION_TARGET_WEIGHTS",
                {},
            ),
            maximum_symbol_fractions=getattr(
                config,
                "ALLOCATION_MAX_SYMBOL_FRACTIONS",
                {},
            ),
            core_symbols=getattr(
                config,
                "ALLOCATION_CORE_SYMBOLS",
                (),
            ),
            appreciation_exempt_symbols=getattr(
                config,
                "ALLOCATION_APPRECIATION_EXEMPT_SYMBOLS",
                (),
            ),
            minimum_core_fraction=float(
                getattr(
                    config,
                    "ALLOCATION_MIN_CORE_FRACTION",
                    0.0,
                )
            ),
            maximum_correlation_cluster_fraction=float(
                getattr(
                    config,
                    "ALLOCATION_MAX_CORRELATION_CLUSTER_FRACTION",
                    0.50,
                )
            ),
            correlation_threshold=float(
                getattr(config, "ALLOCATION_CORRELATION_THRESHOLD", 0.80)
            ),
        )

    def restore(self) -> bool:
        loader = getattr(
            self.state_store,
            "load_strict",
            self.state_store.load,
        )
        payload = loader()
        if not payload:
            return False
        version = int(payload.get("version", 0) or 0)
        if version not in {
            *LEGACY_PORTFOLIO_STATE_VERSIONS,
            PORTFOLIO_STATE_VERSION,
        }:
            raise RuntimeError(
                f"unsupported RWUSD portfolio state version: {version}"
            )
        required = {
            "version",
            "mode",
            "execution_wired",
            "portfolio",
            "deposit_detection_started_at_ms",
            "deposit_scan_completed_through_ms",
            "unresolved_deposit_inserted_at_ms",
        }
        if version >= PORTFOLIO_STATE_VERSION:
            required.update(
                {
                    "capital_bootstrap_source",
                    "capital_bootstrap_observed_at_ms",
                    "initial_collateralization_target",
                }
            )
        missing = sorted(required.difference(payload))
        if missing:
            raise RuntimeError(
                "RWUSD portfolio state is missing fields: "
                + ",".join(missing)
            )
        persisted_mode = str(payload.get("mode") or "").strip()
        if persisted_mode != self.execution_environment:
            raise RuntimeError("RWUSD portfolio state belongs to another mode")
        expected_execution_wired = version >= 17
        if payload.get("execution_wired") is not expected_execution_wired:
            raise RuntimeError("RWUSD portfolio execution state is invalid")
        self.ledger = PortfolioLedger.from_dict(payload.get("portfolio"))
        self.deposit_detection_started_at_ms = max(
            0,
            int(payload.get("deposit_detection_started_at_ms", 0) or 0),
        )
        self.deposit_scan_completed_through_ms = max(
            self.deposit_detection_started_at_ms,
            int(
                payload.get(
                    "deposit_scan_completed_through_ms",
                    self.deposit_detection_started_at_ms,
                )
                or 0
            ),
        )
        self.unresolved_deposit_inserted_at_ms = (
            self._restore_unresolved_deposits(
                payload.get("unresolved_deposit_inserted_at_ms"),
                baseline_ms=self.deposit_detection_started_at_ms,
                completed_through_ms=self.deposit_scan_completed_through_ms,
            )
        )
        if self._configured_capital_lots_present():
            if self.execution_environment == "local_dry_run":
                raise RuntimeError(
                    "configured RWUSD capital is unsupported in portfolio state"
                )
            references = {
                lot.external_reference
                for lot in self.ledger.capital_lots.values()
            }
            if any(
                not reference.startswith(CONFIGURED_CAPITAL_PREFIX)
                for reference in references
            ):
                raise RuntimeError(
                    "RWUSD configured capital is mixed with confirmed capital"
                )
            self.configured_capital_migration_symbols = tuple(
                sorted(self.ledger.allocated_capital)
            )
            self.ledger = PortfolioLedger()
            self.capital_bootstrap_source = ""
            self.capital_bootstrap_observed_at_ms = 0
            self.initial_collateralization_target = ZERO
            self.configured_capital_migration_required = True
            self._last_persisted_payload = None
            return False
        self.capital_bootstrap_source = str(
            payload.get("capital_bootstrap_source") or ""
        ).strip()
        if (
            version in LEGACY_PORTFOLIO_STATE_VERSIONS
            and not self.capital_bootstrap_source
        ):
            self.capital_bootstrap_source = f"legacy_portfolio_v{version}"
        self.capital_bootstrap_observed_at_ms = max(
            0,
            int(payload.get("capital_bootstrap_observed_at_ms", 0) or 0),
        )
        self.initial_collateralization_target = (
            nonnegative_money(
                payload.get(
                    "initial_collateralization_target",
                    self.ledger.total_capital,
                ),
                "initial collateralization target",
            )
        )
        if self.initial_collateralization_target < self.ledger.total_capital:
            raise RuntimeError(
                "RWUSD initial collateralization target is below capital"
            )
        if (
            (
                self.ledger.capital_lots
                or self.initial_collateralization_target > ZERO
            )
            and not self.capital_bootstrap_source
        ):
            raise RuntimeError(
                "RWUSD portfolio capital bootstrap source is missing"
            )
        self._last_persisted_payload = (
            payload
            if version == PORTFOLIO_STATE_VERSION
            else None
        )
        if version != PORTFOLIO_STATE_VERSION:
            self.persist()
        return True

    def export_payload(self) -> dict:
        return {
            "version": PORTFOLIO_STATE_VERSION,
            "mode": self.execution_environment,
            "execution_wired": True,
            "portfolio": self.ledger.to_dict(),
            "deposit_detection_started_at_ms": (
                self.deposit_detection_started_at_ms
            ),
            "deposit_scan_completed_through_ms": (
                self.deposit_scan_completed_through_ms
            ),
            "unresolved_deposit_inserted_at_ms": dict(
                sorted(self.unresolved_deposit_inserted_at_ms.items())
            ),
            "capital_bootstrap_source": self.capital_bootstrap_source,
            "capital_bootstrap_observed_at_ms": (
                self.capital_bootstrap_observed_at_ms
            ),
            "initial_collateralization_target": str(
                self.initial_collateralization_target
            ),
        }

    def persist(self) -> None:
        payload = self.export_payload()
        if payload == self._last_persisted_payload:
            return
        self.state_store.save(payload)
        self._last_persisted_payload = payload

    def register_external_capital(
        self,
        *,
        amount: object,
        source_asset: str,
        external_reference: str,
        rows: Iterable[Mapping[str, object]],
        created_at: Optional[str] = None,
        collateralized: bool,
        capital_contribution_before: object = ZERO,
        fixed_symbol: Optional[str] = None,
    ) -> tuple[CapitalLot, AllocationResult, bool]:
        existing = self.ledger.capital_lot_by_reference(external_reference)
        if existing is not None:
            lot = self.ledger.register_capital(
                amount=amount,
                source_asset=source_asset,
                external_reference=external_reference,
                created_at=created_at,
                collateralized=collateralized,
                capital_contribution_before=capital_contribution_before,
            )
            return lot, self._existing_allocation(lot), False
        with self._atomic_ledger_update():
            lot = self.ledger.register_capital(
                amount=amount,
                source_asset=source_asset,
                external_reference=external_reference,
                created_at=created_at,
                collateralized=collateralized,
                capital_contribution_before=capital_contribution_before,
            )
            target = str(fixed_symbol or "").strip().upper()
            result = (
                AllocationResult(
                    requested_capital=lot.amount,
                    deployable_capital=lot.amount,
                    allocated_capital={target: lot.amount},
                    reserve_capital=ZERO,
                    candidates=[],
                )
                if target
                else self.allocator.allocate(
                    amount=lot.amount,
                    rows=rows,
                    current_allocated_capital=self.ledger.allocated_capital,
                    current_total_capital=self.ledger.total_capital - lot.amount,
                )
            )
            self.ledger.allocate_lot(
                lot.lot_id,
                result.allocated_capital,
                minimum_reserve=(
                    result.requested_capital - result.deployable_capital
                ),
            )
            self.initial_collateralization_target += lot.amount
            self.last_allocation = result
        return lot, result, True

    @staticmethod
    def _existing_allocation(lot: CapitalLot) -> AllocationResult:
        return AllocationResult(
            requested_capital=lot.amount,
            deployable_capital=max(
                ZERO,
                lot.amount - lot.minimum_reserve,
            ),
            allocated_capital=dict(lot.allocations),
            reserve_capital=lot.reserve_amount,
            candidates=[],
        )

    def start_deposit_detection(self, started_at_ms: int) -> int:
        if self.deposit_detection_started_at_ms <= 0:
            value = int(started_at_ms or 0)
            if value <= 0:
                raise ValueError(
                    "RWUSD deposit detection requires a positive baseline"
                )
            with self._atomic_cursor_update():
                self.deposit_detection_started_at_ms = value
                self.deposit_scan_completed_through_ms = value
        return self.deposit_detection_started_at_ms

    def deposit_scan_ranges(
        self,
        completed_through_ms: int,
    ) -> list[tuple[int, int]]:
        baseline = self._deposit_detection_baseline()
        scan_end = int(completed_through_ms or 0)
        if scan_end < max(baseline, self.deposit_scan_completed_through_ms):
            raise ValueError("RWUSD deposit scan end precedes its cursor")
        completed = max(baseline, self.deposit_scan_completed_through_ms)
        overlap_ms = int(
            max(
                0.0,
                float(
                    getattr(
                        self.config,
                        "DEPOSIT_SCAN_OVERLAP_SEC",
                        10 * 60.0,
                    )
                ),
            )
            * 1000
        )
        recent_start = max(baseline, completed - overlap_ms)
        ranges = [(recent_start, scan_end)]
        ranges.extend(
            (
                max(1, inserted_at - 1),
                min(scan_end, inserted_at + 1),
            )
            for inserted_at in set(
                self.unresolved_deposit_inserted_at_ms.values()
            )
            if inserted_at < recent_start
        )
        merged: list[tuple[int, int]] = []
        for start_ms, end_ms in sorted(ranges):
            if merged and start_ms <= merged[-1][1] + 1:
                merged[-1] = (
                    merged[-1][0],
                    max(merged[-1][1], end_ms),
                )
            else:
                merged.append((start_ms, end_ms))
        return merged

    def record_deposit_scan_completed(
        self,
        completed_through_ms: int,
        *,
        unresolved_deposits: Iterable[UnresolvedExternalDeposit] = (),
        resolved_references: Iterable[str] = (),
    ) -> None:
        baseline = self._deposit_detection_baseline()
        value = max(
            baseline,
            int(completed_through_ms or 0),
        )
        with self._atomic_cursor_update():
            for deposit in unresolved_deposits:
                reference = str(
                    deposit.external_reference or ""
                ).strip()
                inserted_at = int(deposit.inserted_at_ms or 0)
                if (
                    not reference
                    or inserted_at <= 0
                    or inserted_at > value
                ):
                    raise ValueError(
                        "RWUSD unresolved deposit cursor is invalid"
                    )
                previous = self.unresolved_deposit_inserted_at_ms.get(
                    reference
                )
                self.unresolved_deposit_inserted_at_ms[reference] = (
                    min(previous, inserted_at)
                    if previous is not None
                    else inserted_at
                )
            for reference in resolved_references:
                self.unresolved_deposit_inserted_at_ms.pop(
                    str(reference or "").strip(),
                    None,
                )
            self.deposit_scan_completed_through_ms = max(
                self.deposit_scan_completed_through_ms,
                value,
            )

    def _deposit_detection_baseline(self) -> int:
        if self.deposit_detection_started_at_ms <= 0:
            raise RuntimeError("RWUSD deposit detection is not initialized")
        return self.deposit_detection_started_at_ms

    def register_confirmed_deposits(
        self,
        *,
        deposits: Iterable[ConfirmedExternalDeposit],
        rows: Iterable[Mapping[str, object]],
    ) -> list[dict]:
        market_rows = list(rows)
        events: list[dict] = []
        for deposit in deposits:
            created_at = datetime.fromtimestamp(
                deposit.completed_at_ms / 1000,
                tz=timezone.utc,
            ).isoformat()
            lot, allocation, created = self.register_external_capital(
                amount=deposit.amount,
                source_asset=deposit.asset,
                external_reference=deposit.external_reference,
                created_at=created_at,
                rows=market_rows,
                collateralized=False,
            )
            events.append(
                {
                    "type": "rwusd_external_capital",
                    "action": (
                        "registered_and_planned"
                        if created
                        else "duplicate_ignored"
                    ),
                    "lot_id": lot.lot_id,
                    "external_reference": deposit.external_reference,
                    "asset": deposit.asset,
                    "amount": str(deposit.amount),
                    "completed_at_ms": deposit.completed_at_ms,
                    "allocation": allocation.to_dict(),
                    "execution_wired": True,
                }
            )
        return events

    def complete_deposit_scan(
        self,
        *,
        history: ExternalDepositHistoryBatch,
        rows: Iterable[Mapping[str, object]],
        completed_through_ms: int,
    ) -> list[dict]:
        events = self.register_confirmed_deposits(
            deposits=history.confirmed,
            rows=rows,
        )
        resolved = {
            deposit.external_reference for deposit in history.confirmed
        }
        resolved.update(history.terminal_references)
        self.record_deposit_scan_completed(
            completed_through_ms,
            unresolved_deposits=history.unresolved,
            resolved_references=resolved,
        )
        return events

    def bootstrap_capital(
        self,
        *,
        snapshot: OfficialCapitalSnapshot,
        rows: Iterable[Mapping[str, object]],
        source: str,
        fixed_symbol: Optional[str] = None,
    ) -> Optional[AllocationResult]:
        if snapshot.environment != self.execution_environment:
            raise RuntimeError(
                "RWUSD capital snapshot belongs to another environment"
            )
        if self.ledger.capital_lots:
            return None
        components = tuple(snapshot.components)
        if any(component.amount <= ZERO for component in components):
            raise ValueError("RWUSD detected capital must be positive")
        references = [
            component.external_reference for component in components
        ]
        if len(references) != len(set(references)):
            raise ValueError("RWUSD detected capital references are duplicated")
        market_rows = list(rows)
        result: Optional[AllocationResult] = None
        replaces_empty_official_snapshot = bool(
            self.live_rwusd_execution
            and self.capital_bootstrap_source
            and not self.ledger.capital_lots
            and components
        )
        with self._atomic_ledger_update():
            self.ledger = PortfolioLedger()
            for component in components:
                self.ledger.register_capital(
                    amount=component.amount,
                    source_asset=component.source_asset,
                    external_reference=component.external_reference,
                    collateralized=False,
                )
            if self.ledger.total_capital > ZERO:
                target = str(fixed_symbol or "").strip().upper()
                result = (
                    AllocationResult(
                        requested_capital=self.ledger.total_capital,
                        deployable_capital=self.ledger.total_capital,
                        allocated_capital={
                            target: self.ledger.total_capital
                        },
                        reserve_capital=ZERO,
                        candidates=[],
                    )
                    if target
                    else self.allocator.allocate(
                        amount=self.ledger.total_capital,
                        rows=market_rows,
                        current_allocated_capital={},
                        current_total_capital=ZERO,
                    )
                )
                self._apply_bootstrap_allocation(result)
            self.last_allocation = result
            self.capital_bootstrap_source = str(source or "").strip()
            if not self.capital_bootstrap_source:
                raise ValueError("RWUSD capital bootstrap source is required")
            self.capital_bootstrap_observed_at_ms = max(
                1,
                int(snapshot.observed_at_ms or 0),
            )
            self.initial_collateralization_target = nonnegative_money(
                snapshot.collateralization_target,
                "initial collateralization target",
            )
            if (
                self.initial_collateralization_target
                < self.ledger.total_capital
            ):
                raise RuntimeError(
                    "RWUSD collateralization target is below detected capital"
                )
            if (
                self.live_rwusd_execution
                and (
                    self.deposit_detection_started_at_ms <= 0
                    or replaces_empty_official_snapshot
                )
            ):
                self.deposit_detection_started_at_ms = (
                    self.capital_bootstrap_observed_at_ms
                )
                self.deposit_scan_completed_through_ms = (
                    self.capital_bootstrap_observed_at_ms
                )
                self.unresolved_deposit_inserted_at_ms = {}
            self.configured_capital_migration_required = False
            self.configured_capital_migration_symbols = ()
        return result

    def _apply_bootstrap_allocation(
        self,
        result: AllocationResult,
    ) -> None:
        minimum_reserve = max(
            ZERO,
            result.requested_capital - result.deployable_capital,
        )
        remaining_reserve = minimum_reserve
        for lot in sorted(
            self.ledger.capital_lots.values(),
            key=lambda item: (item.created_at, item.lot_id),
        ):
            reserve = min(lot.amount, remaining_reserve)
            lot.minimum_reserve = reserve
            remaining_reserve -= reserve
            if remaining_reserve <= ZERO:
                break
        if remaining_reserve > ZERO:
            raise RuntimeError(
                "RWUSD bootstrap reserve allocation was incomplete"
            )
        if result.allocated_capital:
            self.ledger.add_allocations(result.allocated_capital)

    def adopt_legacy_capital(
        self,
        *,
        symbol: str,
        amount: object,
        source_asset: str = "USDC",
    ) -> Optional[CapitalLot]:
        target = str(symbol or "").strip().upper()
        if self.ledger.capital_lots:
            return None
        if not target:
            raise ValueError("RWUSD legacy capital requires a symbol")
        with self._atomic_ledger_update():
            lot = self.ledger.register_capital(
                amount=amount,
                source_asset=source_asset,
                external_reference=(
                    f"legacy-execution:{self.account_name}:{target}:v17"
                ),
                collateralized=True,
            )
            self.ledger.allocate_lot(
                lot.lot_id,
                {target: lot.amount},
            )
            self.capital_bootstrap_source = "legacy_confirmed_execution"
            self.capital_bootstrap_observed_at_ms = 0
            self.initial_collateralization_target = self.ledger.total_capital
        return lot

    def allocate_reserve(
        self,
        *,
        rows: Iterable[Mapping[str, object]],
    ) -> Optional[AllocationResult]:
        amount = self.ledger.allocatable_reserve
        if amount <= 0:
            return None
        result = self.allocator.allocate(
            amount=amount,
            rows=rows,
            current_allocated_capital=self.ledger.allocated_capital,
            current_total_capital=self.ledger.total_capital,
            capital_base_override=self.ledger.total_capital,
            liquidity_reserve_fraction=0.0,
        )
        with self._atomic_ledger_update():
            if result.allocated_capital:
                self.ledger.add_allocations(result.allocated_capital)
            self.last_allocation = result
        return result

    def reconcile_ignored_symbol_allocations(
        self,
        *,
        ignored_symbols: Iterable[str],
        positions: Iterable[Mapping[str, object]],
        open_orders: Iterable[Mapping[str, object]],
    ) -> Optional[dict]:
        ignored = {
            str(symbol or "").strip().upper()
            for symbol in ignored_symbols
            if str(symbol or "").strip()
        }
        retired = sorted(
            symbol
            for symbol in ignored
            if self.ledger.allocated_capital.get(symbol, ZERO) > ZERO
        )
        if not retired or self.ledger.pending_collateralization > ZERO:
            return None

        active_position_symbols: set[str] = set()
        for position in positions:
            symbol = str(position.get("symbol") or "").strip().upper()
            if not symbol:
                return None
            try:
                size = abs(float(position.get("size", 0) or 0))
            except (TypeError, ValueError):
                return None
            if size > 1e-12:
                active_position_symbols.add(symbol)

        active_order_symbols: set[str] = set()
        for order in open_orders:
            symbol = str(order.get("symbol") or "").strip().upper()
            if not symbol:
                return None
            active_order_symbols.add(symbol)

        if active_position_symbols.intersection(retired):
            return None
        if active_order_symbols.intersection(retired):
            return None

        total_capital = self.ledger.total_capital
        maximum_allocations = {
            symbol: money(
                total_capital * Decimal(str(fraction))
            )
            for (
                symbol,
                fraction,
            ) in self.allocator.maximum_symbol_fractions.items()
        }
        with self._atomic_ledger_update():
            released = self.ledger.release_allocations(retired)
            capped = self.ledger.release_excess_allocations(
                maximum_allocations
            )
            self.last_allocation = None
        released_total = sum(
            [*released.values(), *capped.values()],
            ZERO,
        ).quantize(MONEY_QUANTUM)
        return {
            "type": "rwusd_capital_allocation",
            "action": "legacy_allocations_released",
            "symbols": retired,
            "released_allocated_capital": {
                symbol: str(amount)
                for symbol, amount in sorted(released.items())
            },
            "released_over_target_capital": {
                symbol: str(amount)
                for symbol, amount in sorted(capped.items())
            },
            "released_total": str(released_total),
            "reserve_capital": str(self.ledger.reserve_capital),
            "allocatable_reserve": str(self.ledger.allocatable_reserve),
            "positions_confirmed_flat": True,
            "open_orders_confirmed_zero": True,
            "execution_wired": True,
        }

    def mark_capital_collateralized(
        self,
        lot_id: str,
        amount: object,
    ) -> Decimal:
        with self._atomic_ledger_update():
            applied = self.ledger.mark_capital_collateralized(lot_id, amount)
        return applied

    def mark_pending_capital_collateralized(
        self,
        amount: object,
        *,
        source_asset: Optional[str] = None,
        exclude_prefixes: Iterable[str] = (),
    ) -> Decimal:
        remaining = nonnegative_money(
            amount,
            "pending collateralized amount",
        )
        asset = str(source_asset or "").strip().upper()
        prefixes = tuple(
            str(prefix or "").strip()
            for prefix in exclude_prefixes
            if str(prefix or "").strip()
        )
        applied = ZERO
        with self._atomic_ledger_update():
            for lot in sorted(
                self.ledger.capital_lots.values(),
                key=lambda item: (item.created_at, item.lot_id),
            ):
                if remaining <= ZERO:
                    break
                if (
                    asset
                    and lot.source_asset != asset
                ) or (
                    prefixes
                    and lot.external_reference.startswith(prefixes)
                ):
                    continue
                addition = self.ledger.mark_capital_collateralized(
                    lot.lot_id,
                    remaining,
                )
                remaining -= addition
                applied += addition
        return applied

    def mark_incremental_capital_collateralized(
        self,
        *,
        amount: object,
        source_asset: str,
        external_reference: str,
    ) -> Decimal:
        value = nonnegative_money(
            amount,
            "incremental capital amount",
        )
        if value <= ZERO:
            return ZERO
        reference = str(external_reference or "").strip()
        if not reference.startswith("binance-incremental:"):
            return ZERO
        existing = self.ledger.capital_lot_by_reference(reference)
        if existing is None:
            return ZERO
        asset = str(source_asset or "").strip().upper()
        if existing.source_asset != asset or existing.amount != value:
            raise RuntimeError(
                "incremental capital confirmation conflicts with the "
                "referenced portfolio lot"
            )
        if existing.pending_collateralization <= 0:
            return value
        return self.mark_capital_collateralized(
            existing.lot_id,
            value,
        )

    def discard_unconfirmed_incremental_capital(
        self,
        observed_capital_contribution: object,
    ) -> Decimal:
        observed = nonnegative_money(
            observed_capital_contribution,
            "observed capital contribution",
        )
        discarded = ZERO
        with self._atomic_ledger_update():
            for lot_id, lot in list(self.ledger.capital_lots.items()):
                reference = str(lot.external_reference or "").strip()
                expected = lot.capital_contribution_before + lot.amount
                if (
                    not reference.startswith("binance-incremental:")
                    or lot.collateralized_amount <= ZERO
                    or observed >= expected
                ):
                    continue
                discarded += lot.amount
                del self.ledger.capital_lots[lot_id]
            self.initial_collateralization_target = max(
                self.ledger.total_capital,
                self.initial_collateralization_target - discarded,
            )
        return discarded.quantize(MONEY_QUANTUM)

    def reconcile_pending_incremental_capital(
        self,
        observed_capital_contribution: object,
        *,
        external_reference: str,
        capital_contribution_before: object,
        capital_contribution_amount: object,
    ) -> Decimal:
        observed = nonnegative_money(
            observed_capital_contribution,
            "observed capital contribution",
        )
        baseline = nonnegative_money(
            capital_contribution_before,
            "confirmed capital contribution before",
        )
        amount = nonnegative_money(
            capital_contribution_amount,
            "confirmed capital contribution amount",
        )
        reference = str(external_reference or "").strip()
        lot = self.ledger.capital_lot_by_reference(reference)
        if (
            lot is None
            or not reference.startswith("binance-incremental:")
            or lot.capital_contribution_before != baseline
            or lot.amount != amount
            or observed < baseline + amount
            or lot.pending_collateralization <= ZERO
        ):
            return ZERO
        return self.mark_incremental_capital_collateralized(
            amount=lot.amount,
            source_asset=lot.source_asset,
            external_reference=reference,
        )

    def symbol_capital(
        self,
        symbol: str,
        *,
        deployed: bool = True,
    ) -> Decimal:
        target = str(symbol or "").strip().upper()
        values = (
            self.ledger.deployed_capital
            if deployed
            else self.ledger.allocated_capital
        )
        return values.get(target, ZERO)

    @contextmanager
    def _atomic_ledger_update(self) -> Iterator[None]:
        ledger_before = self.ledger.to_dict()
        allocation_before = self.last_allocation
        bootstrap_source_before = self.capital_bootstrap_source
        bootstrap_observed_before = self.capital_bootstrap_observed_at_ms
        collateralization_target_before = (
            self.initial_collateralization_target
        )
        detection_started_before = self.deposit_detection_started_at_ms
        scan_completed_before = self.deposit_scan_completed_through_ms
        unresolved_before = dict(
            self.unresolved_deposit_inserted_at_ms
        )
        migration_required_before = (
            self.configured_capital_migration_required
        )
        migration_symbols_before = self.configured_capital_migration_symbols
        try:
            yield
            self.persist()
        except Exception:
            self.ledger = PortfolioLedger.from_dict(ledger_before)
            self.last_allocation = allocation_before
            self.capital_bootstrap_source = bootstrap_source_before
            self.capital_bootstrap_observed_at_ms = bootstrap_observed_before
            self.initial_collateralization_target = (
                collateralization_target_before
            )
            self.deposit_detection_started_at_ms = detection_started_before
            self.deposit_scan_completed_through_ms = scan_completed_before
            self.unresolved_deposit_inserted_at_ms = unresolved_before
            self.configured_capital_migration_required = (
                migration_required_before
            )
            self.configured_capital_migration_symbols = (
                migration_symbols_before
            )
            raise

    @contextmanager
    def _atomic_cursor_update(self) -> Iterator[None]:
        started_before = self.deposit_detection_started_at_ms
        completed_before = self.deposit_scan_completed_through_ms
        unresolved_before = dict(
            self.unresolved_deposit_inserted_at_ms
        )
        try:
            yield
            self.persist()
        except Exception:
            self.deposit_detection_started_at_ms = started_before
            self.deposit_scan_completed_through_ms = completed_before
            self.unresolved_deposit_inserted_at_ms = unresolved_before
            raise

    def pending_capital_collateralization(
        self,
        *,
        reference_prefix: Optional[str] = None,
        exclude_prefixes: Iterable[str] = (),
    ) -> Decimal:
        prefix = str(reference_prefix or "").strip()
        excluded = tuple(
            str(item or "").strip()
            for item in exclude_prefixes
            if str(item or "").strip()
        )
        return sum(
            (
                lot.pending_collateralization
                for lot in self.ledger.capital_lots.values()
                if (
                    (not prefix or lot.external_reference.startswith(prefix))
                    and not (
                        excluded
                        and lot.external_reference.startswith(excluded)
                    )
                )
            ),
            ZERO,
        ).quantize(MONEY_QUANTUM)

    def reset_collateralization_for_restart(self) -> Decimal:
        with self._atomic_ledger_update():
            reset = self.ledger.reset_collateralization()
        return reset

    def reset_for_official_restart(self) -> Decimal:
        previous_capital = self.ledger.total_capital
        with self._atomic_ledger_update():
            self.ledger = PortfolioLedger()
            self.last_allocation = None
            self.deposit_detection_started_at_ms = 0
            self.deposit_scan_completed_through_ms = 0
            self.unresolved_deposit_inserted_at_ms = {}
            self.capital_bootstrap_source = ""
            self.capital_bootstrap_observed_at_ms = 0
            self.initial_collateralization_target = ZERO
            self.configured_capital_migration_required = False
            self.configured_capital_migration_symbols = ()
        return previous_capital

    def active_symbols(self) -> list[str]:
        return sorted(self.ledger.deployed_capital)

    def planned_symbols(self) -> list[str]:
        return sorted(self.ledger.allocated_capital)

    def get_status(self) -> dict:
        return {
            "version": PORTFOLIO_STATE_VERSION,
            "mode": self.execution_environment,
            "execution_wired": True,
            "active_symbols": self.active_symbols(),
            "planned_symbols": self.planned_symbols(),
            "portfolio": self.ledger.to_dict(),
            "last_allocation": (
                self.last_allocation.to_dict()
                if self.last_allocation is not None
                else None
            ),
            "deposit_detection_started_at_ms": (
                self.deposit_detection_started_at_ms
            ),
            "deposit_scan_completed_through_ms": (
                self.deposit_scan_completed_through_ms
            ),
            "unresolved_deposit_inserted_at_ms": dict(
                sorted(self.unresolved_deposit_inserted_at_ms.items())
            ),
            "capital_bootstrap_source": self.capital_bootstrap_source,
            "capital_bootstrap_observed_at_ms": (
                self.capital_bootstrap_observed_at_ms
            ),
            "initial_collateralization_target": str(
                self.initial_collateralization_target
            ),
            "configured_capital_migration_required": (
                self.configured_capital_migration_required
            ),
        }

    def _configured_capital_lots_present(self) -> bool:
        return any(
            lot.external_reference.startswith(CONFIGURED_CAPITAL_PREFIX)
            for lot in self.ledger.capital_lots.values()
        )

    @staticmethod
    def _restore_unresolved_deposits(
        payload: object,
        *,
        baseline_ms: int,
        completed_through_ms: int,
    ) -> dict[str, int]:
        if not isinstance(payload, Mapping):
            raise ValueError(
                "RWUSD unresolved deposit state must be an object"
            )
        restored: dict[str, int] = {}
        for raw_reference, raw_inserted_at in payload.items():
            reference = str(raw_reference or "").strip()
            try:
                inserted_at = int(raw_inserted_at)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "RWUSD unresolved deposit timestamp is invalid"
                ) from exc
            if (
                not reference
                or baseline_ms <= 0
                or inserted_at <= 0
                or inserted_at > completed_through_ms
            ):
                raise ValueError(
                    "RWUSD unresolved deposit state is invalid"
                )
            restored[reference] = inserted_at
        return restored
