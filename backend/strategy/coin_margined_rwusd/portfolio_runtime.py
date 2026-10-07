"""Account-level orchestration for independent RWUSD symbol lanes."""

from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Iterable, List, Mapping, Optional

from backend.strategy.coin_margined_rwusd.capital_bootstrap import (
    OfficialCapitalSnapshot,
    detect_official_capital,
    simulation_capital_snapshot,
)
from backend.strategy.coin_margined_rwusd.capital_flow import (
    ExternalDepositHistoryBatch,
    MAX_DEPOSIT_HISTORY_WINDOW_MS,
    fetch_external_deposit_history,
)
from backend.strategy.coin_margined_rwusd.coordinator import RWUSDPortfolioCoordinator
from backend.strategy.coin_margined_rwusd.engine import RWUSDEngine
from backend.strategy.coin_margined_rwusd.execution import BinanceRWUSDExecutor
from backend.strategy.coin_margined_rwusd.exit import RWUSDExitStage
from backend.strategy.coin_margined_rwusd.models import StrategyPhase
from backend.strategy.coin_margined_rwusd.persistence import (
    LANE_STATE_PREFIX,
    RWUSDStateStore,
    rwusd_runtime_lane_symbols,
)
from backend.strategy.coin_margined_rwusd.portfolio import ZERO, nonnegative_money
from backend.strategy.coin_margined_rwusd.preflight import (
    RWUSDPreflightSnapshots,
    load_rwusd_preflight_snapshots,
)
from backend.strategy.coin_margined_rwusd.runtime import RWUSDExecutionOwnership


LANE_STATE_SUFFIX = LANE_STATE_PREFIX


@dataclass
class RWUSDSymbolLane:
    symbol: str
    engine: RWUSDEngine
    executor: Optional[BinanceRWUSDExecutor]
    primary: bool = False
    prepared: bool = False


class RWUSDPortfolioRuntime:
    """Coordinate capital once while reusing one engine per symbol."""

    def __init__(
        self,
        *,
        account_name: str,
        simulation_capital: float,
        config: object,
        client: object,
        execution_environment: str,
        execution_enabled: bool,
        live_rwusd_execution: bool,
        total_margin_fraction: float,
        primary_engine: RWUSDEngine,
        primary_executor: Optional[BinanceRWUSDExecutor],
    ) -> None:
        self.account_name = str(account_name)
        self.simulation_capital = max(
            0.0,
            float(simulation_capital or 0),
        )
        self.config = config
        self.client = client
        self.execution_environment = str(execution_environment)
        self.execution_enabled = bool(execution_enabled)
        self.live_rwusd_execution = bool(live_rwusd_execution)
        self.total_margin_fraction = float(total_margin_fraction)
        self.coordinator = RWUSDPortfolioCoordinator(
            account_name=self.account_name,
            config=self.config,
            execution_environment=self.execution_environment,
            live_rwusd_execution=self.live_rwusd_execution,
        )
        self.ownership = RWUSDExecutionOwnership()
        self.primary_engine = primary_engine
        self.primary_executor = primary_executor
        self.primary_symbol = ""
        self._lanes: Dict[str, RWUSDSymbolLane] = {}
        self._last_deposit_scan_monotonic = 0.0
        self._last_official_bootstrap_monotonic = 0.0
        self._last_reserve_allocation_monotonic = 0.0
        self._initial_capital_reconciled = False
        self._legacy_official_adoption_required = False
        self._pending_official_snapshot: Optional[
            OfficialCapitalSnapshot
        ] = None
        self._persisted_lane_symbols = set(
            rwusd_runtime_lane_symbols(self.account_name)
        )
        self._coordinator_restored: Optional[bool] = None

    def restore(self) -> bool:
        if self._coordinator_restored is None:
            self._coordinator_restored = self.coordinator.restore()
        return self._coordinator_restored

    def initial_allocation_required(self) -> bool:
        return bool(
            not self.restore()
            and not self._legacy_state_present()
            and self.execution_environment == "local_dry_run"
            and not self.execution_enabled
            and self.simulation_capital > 0
        )

    def initialize(
        self,
        *,
        allocation_rows: Iterable[Mapping[str, object]],
        fallback_symbol: str,
    ) -> None:
        restored = self.restore()
        self.ownership.set_ignored_symbols(
            self._historical_ignored_symbols()
        )
        supported = self._supported_symbols()
        legacy_symbol = str(
            self.primary_engine.state.selected_symbol
            or fallback_symbol
            or ""
        ).strip().upper()
        if (
            self.primary_engine.state.selected_symbol
            and legacy_symbol not in supported
        ):
            raise RuntimeError(
                "RWUSD primary state uses unsupported symbol: "
                f"{legacy_symbol}; frontend cleanup is required"
            )
        legacy_state = self._legacy_state_present()
        if (
            not restored
            and not self.coordinator.configured_capital_migration_required
        ):
            if (
                legacy_state
                and self.execution_environment == "binance_futures_demo"
            ):
                self._legacy_official_adoption_required = True
            elif legacy_state:
                self.coordinator.adopt_legacy_capital(
                    symbol=legacy_symbol,
                    amount=self._legacy_capital_basis(),
                )
            elif (
                self.execution_environment == "local_dry_run"
                and not self.execution_enabled
                and self.simulation_capital > 0
            ):
                self.coordinator.bootstrap_capital(
                    snapshot=simulation_capital_snapshot(
                        account_name=self.account_name,
                        amount=self.simulation_capital,
                    ),
                    rows=allocation_rows,
                    source="local_dry_run_simulation",
                )
        self.primary_symbol = (
            legacy_symbol
            if legacy_state
            else self._preferred_primary_symbol(fallback_symbol)
        )
        if not self.primary_symbol:
            raise RuntimeError("RWUSD portfolio has no primary symbol")
        if (
            self.primary_engine.state.selected_symbol
            and self.primary_engine.state.selected_symbol
            != self.primary_symbol
            and legacy_state
        ):
            raise RuntimeError("RWUSD legacy primary symbol changed")
        if self.primary_symbol in self._persisted_lane_symbols:
            raise RuntimeError(
                "RWUSD primary symbol has conflicting lane state: "
                f"{self.primary_symbol}"
            )
        self.primary_engine.state.selected_symbol = self.primary_symbol
        self.primary_engine.persist()
        self._register_primary_lane()
        self.ensure_lanes()

    def _legacy_state_present(self) -> bool:
        return bool(
            self.primary_engine.exchange_ownership_established()
            or self.primary_engine.state.cycle_id > 0
            or self.primary_engine.profit_bucket.capital_contribution_total > 0
            or self.primary_engine.profit_bucket.initial_collateralization_completed
        )

    def _legacy_capital_basis(self) -> float:
        bucket = self.primary_engine.profit_bucket
        confirmed_capital = max(
            0.0,
            float(bucket.capital_contribution_total or 0),
        )
        if confirmed_capital > 0:
            return confirmed_capital
        source_capital = max(
            0.0,
            float(bucket.initial_collateralization_source_amount or 0),
        )
        if source_capital > 0:
            return source_capital
        confirmed_principal = max(
            0.0,
            float(bucket.rwusd_principal or 0),
        )
        if confirmed_principal > 0:
            return confirmed_principal
        if not self.execution_enabled and self.simulation_capital > 0:
            return self.simulation_capital
        raise RuntimeError(
            "RWUSD live legacy capital basis is unavailable; "
            "configured capital cannot replace confirmed principal"
        )

    def _preferred_primary_symbol(self, fallback_symbol: str) -> str:
        supported = self._supported_symbols()
        values = {
            symbol: amount
            for symbol, amount in self.coordinator.ledger.allocated_capital.items()
            if symbol in supported
        }
        if values:
            return max(
                values,
                key=lambda symbol: (values[symbol], symbol == "BTCUSDC"),
            )
        fallback = str(fallback_symbol or "").strip().upper()
        candidates = tuple(
            str(symbol or "").strip().upper()
            for symbol in getattr(self.config, "ALLOCATION_SYMBOLS", ())
        )
        return fallback if fallback in candidates else "BTCUSDC"

    def _supported_symbols(self) -> set[str]:
        return {
            str(symbol or "").strip().upper()
            for symbol in getattr(self.config, "ALLOCATION_SYMBOLS", ())
            if str(symbol or "").strip()
        }

    def _historical_ignored_symbols(self) -> set[str]:
        symbols = set(self._persisted_lane_symbols)
        symbols.update(self.coordinator.planned_symbols())
        symbols.update(self.coordinator.configured_capital_migration_symbols)
        return symbols - self._supported_symbols()

    def _register_primary_lane(self) -> None:
        if self.primary_executor is not None:
            self.primary_executor.set_ownership_provider(
                self.ownership.snapshot
            )
        lane = RWUSDSymbolLane(
            symbol=self.primary_symbol,
            engine=self.primary_engine,
            executor=self.primary_executor,
            primary=True,
        )
        self._lanes[self.primary_symbol] = lane
        self.ownership.register(self.primary_symbol, self.primary_engine)
        self._refresh_lane_budget(lane)

    def ensure_lanes(self) -> List[RWUSDSymbolLane]:
        supported = self._supported_symbols()
        symbols = set(self.coordinator.planned_symbols())
        symbols.update(
            self.coordinator.configured_capital_migration_symbols
        )
        symbols.update(self._persisted_lane_symbols)
        symbols.add(self.primary_symbol)
        created: List[RWUSDSymbolLane] = []
        for symbol in sorted(symbols):
            if (
                not symbol
                or symbol not in supported
                or symbol in self._lanes
            ):
                continue
            leverage = int(
                getattr(self.config, "SYMBOL_TARGET_LEVERAGE", {}).get(
                    symbol,
                    getattr(self.config, "SIM_LEVERAGE", 50),
                )
            )
            lane_name = (
                f"{self.account_name}{LANE_STATE_SUFFIX}{symbol.lower()}"
            )
            engine = RWUSDEngine(
                account_name=lane_name,
                initial_capital=0.0,
                config=self.config,
                external_execution=self.execution_enabled,
                execution_environment=self.execution_environment,
                live_rwusd_execution=self.live_rwusd_execution,
                leverage=(
                    leverage
                    if not bool(getattr(self.client, "is_testnet", False))
                    else leverage
                ),
                state_store=RWUSDStateStore(lane_name),
            )
            restored = engine.restore()
            if restored and engine.state.selected_symbol != symbol:
                raise RuntimeError(
                    f"RWUSD lane state symbol mismatch: {symbol}"
                )
            engine.state.selected_symbol = symbol
            engine.persist()
            executor = (
                BinanceRWUSDExecutor(
                    account_name=lane_name,
                    client=self.client,
                    engine=engine,
                    config=self.config,
                    total_margin_fraction=self.total_margin_fraction,
                    leverage=leverage,
                    ownership_provider=self.ownership.snapshot,
                    capital_budget=float(
                        self.coordinator.symbol_capital(symbol)
                    ),
                )
                if self.execution_enabled
                else None
            )
            lane = RWUSDSymbolLane(
                symbol=symbol,
                engine=engine,
                executor=executor,
            )
            self._lanes[symbol] = lane
            self.ownership.register(symbol, engine)
            created.append(lane)
        for lane in self._lanes.values():
            self._refresh_lane_budget(lane)
        return created

    def official_capital_bootstrap_required(self) -> bool:
        if not self.execution_enabled:
            return False
        if (
            self._legacy_official_adoption_required
            or self.coordinator.configured_capital_migration_required
        ):
            return True
        if self.coordinator.ledger.capital_lots:
            return False
        return True

    def official_capital_bootstrap_due(self) -> bool:
        if not self.official_capital_bootstrap_required():
            return False
        interval = max(
            5.0,
            float(getattr(self.config, "CAPITAL_SCAN_INTERVAL_SEC", 30.0)),
        )
        return bool(
            self._last_official_bootstrap_monotonic <= 0
            or time.monotonic() - self._last_official_bootstrap_monotonic
            >= interval
        )

    async def detect_official_capital(
        self,
    ) -> Optional[OfficialCapitalSnapshot]:
        if not self.official_capital_bootstrap_due():
            return None
        try:
            snapshot = await self._load_official_capital_snapshot()
            pending = self._pending_official_snapshot
            if (
                pending is None
                or pending.stability_key != snapshot.stability_key
            ):
                self._pending_official_snapshot = snapshot
                return None
            self._pending_official_snapshot = None
            return snapshot
        except Exception:
            self._pending_official_snapshot = None
            raise
        finally:
            self._last_official_bootstrap_monotonic = time.monotonic()

    async def prime_official_capital_snapshot(self) -> None:
        if (
            not self.official_capital_bootstrap_required()
            or self._pending_official_snapshot is not None
        ):
            return
        self._pending_official_snapshot = (
            await self._load_official_capital_snapshot()
        )

    async def _load_official_capital_snapshot(
        self,
    ) -> OfficialCapitalSnapshot:
        return await detect_official_capital(
            self.client,
            account_name=self.account_name,
            execution_environment=self.execution_environment,
            source_assets=getattr(
                self.config,
                "RWUSD_SOURCE_ASSETS",
                ("USDC", "USDT"),
            ),
        )

    def complete_official_capital_bootstrap(
        self,
        *,
        snapshot: OfficialCapitalSnapshot,
        allocation_rows: Iterable[Mapping[str, object]],
    ) -> List[dict]:
        migration_required = bool(
            self.coordinator.configured_capital_migration_required
        )
        legacy_adoption = self._legacy_official_adoption_required
        primary_before = self.primary_symbol
        if (
            not migration_required
            and not legacy_adoption
            and not snapshot.components
            and self.coordinator.capital_bootstrap_source
        ):
            return []
        if migration_required:
            for lane in self.lanes():
                lane.engine.validate_official_capital_bootstrap_reset()
            for lane in self.lanes():
                lane.engine.reset_official_capital_bootstrap()
                lane.prepared = False
                if lane.executor is not None:
                    lane.executor.reset_for_new_cycle()
        elif not legacy_adoption and snapshot.components:
            for lane in self.lanes():
                lane.engine.validate_unfunded_primary_promotion()
        result = self.coordinator.bootstrap_capital(
            snapshot=snapshot,
            rows=allocation_rows,
            source=(
                "binance_futures_demo_wallet"
                if self.execution_environment == "binance_futures_demo"
                else "binance_mainnet_official_assets"
            ),
            fixed_symbol=(
                self.primary_symbol if legacy_adoption else None
            ),
        )
        self._legacy_official_adoption_required = False
        if (
            result is not None
            and not migration_required
            and not legacy_adoption
            and len(self._lanes) == 1
        ):
            self._promote_fresh_primary(result.allocated_capital)
        self.ensure_lanes()
        return [
            {
                "type": "rwusd_capital_bootstrap",
                "action": (
                    "official_capital_migrated"
                    if migration_required
                    else (
                        "official_capital_adopted"
                        if legacy_adoption
                        else "official_capital_detected"
                    )
                ),
                "snapshot": snapshot.to_dict(),
                "allocation": (
                    result.to_dict() if result is not None else None
                ),
                "primary_symbol_before": primary_before,
                "primary_symbol_after": self.primary_symbol,
                "execution_wired": True,
            }
        ]

    def _promote_fresh_primary(
        self,
        allocated_capital: Mapping[str, Decimal],
    ) -> None:
        if not allocated_capital:
            return
        target = max(
            allocated_capital,
            key=lambda symbol: (
                allocated_capital[symbol],
                symbol == "BTCUSDC",
            ),
        )
        if target == self.primary_symbol:
            return
        if target in self._lanes or target in self._persisted_lane_symbols:
            raise RuntimeError(
                "RWUSD official primary promotion conflicts with existing "
                f"lane state: {target}"
            )
        lane = self._lanes.get(self.primary_symbol)
        if lane is None or not lane.primary:
            raise RuntimeError("RWUSD primary lane is unavailable")
        self.primary_engine.promote_unfunded_primary_symbol(target)
        old_symbol = self.primary_symbol
        self.ownership.unregister(old_symbol)
        self._lanes.pop(old_symbol)
        self.primary_symbol = target
        lane.symbol = target
        lane.prepared = False
        if lane.executor is not None:
            lane.executor.reset_for_new_cycle()
        self._lanes[target] = lane
        self.ownership.register(target, lane.engine)

    async def advance_capital_sources(
        self,
        *,
        allocation_market: object,
        now_ms: int,
    ) -> tuple[List[dict], List[Mapping[str, object]]]:
        events: List[dict] = []
        latest_rows: List[Mapping[str, object]] = []
        if not self.execution_enabled:
            return events, latest_rows
        if self.primary_executor is None:
            raise RuntimeError(
                "RWUSD capital source executor is unavailable"
            )
        bootstrap_due = self.official_capital_bootstrap_due()
        deposit_due = self.deposit_scan_due()
        if not bootstrap_due and not deposit_due:
            return events, latest_rows
        account_preflight = (
            await self.primary_executor.validate_runtime_isolation(
                expected_symbol=self.primary_symbol,
            )
        )
        release_event = (
            self.coordinator.reconcile_ignored_symbol_allocations(
                ignored_symbols=self.ownership.snapshot().ignored_symbols,
                positions=account_preflight.positions,
                open_orders=account_preflight.open_orders,
            )
        )
        if release_event is not None:
            events.append(release_event)
            self._last_reserve_allocation_monotonic = 0.0
            self.ensure_lanes()
        snapshot = (
            await self.detect_official_capital()
            if bootstrap_due
            else None
        )
        if snapshot is not None:
            if snapshot.external_principal > ZERO:
                latest_rows = list(await allocation_market.build())
            events.extend(
                self.complete_official_capital_bootstrap(
                    snapshot=snapshot,
                    allocation_rows=latest_rows,
                )
            )
        scan_now_ms = max(
            int(now_ms or 0),
            int(time.time() * 1000),
            self.coordinator.deposit_detection_started_at_ms,
        )
        scan = (
            await self.scan_external_deposits(now_ms=scan_now_ms)
            if deposit_due
            else None
        )
        if scan is not None:
            history, completed_through_ms = scan
            deposit_rows = (
                list(await allocation_market.build())
                if history.confirmed
                else []
            )
            if deposit_rows:
                latest_rows = deposit_rows
            events.extend(
                self.complete_deposit_scan(
                    history=history,
                    allocation_rows=deposit_rows,
                    completed_through_ms=completed_through_ms,
                )
            )
        return events, latest_rows

    async def prepare_lanes(
        self,
        *,
        rows: Iterable[Mapping[str, object]],
        portfolio: object,
        preflight_snapshots: Optional[
            RWUSDPreflightSnapshots
        ] = None,
    ) -> List[dict]:
        reference_prices = {
            str(row.get("symbol") or "").strip().upper(): float(
                row.get("close", 0) or 0
            )
            for row in rows
        }
        pending_lanes = [
            lane
            for lane in self.lanes()
            if (
                lane.executor is not None
                and not lane.prepared
                and self._lane_preparation_required(lane)
            )
        ]
        if not pending_lanes:
            return []
        if preflight_snapshots is None:
            preflight_snapshots = await self.preflight_account_writes(
                portfolio=portfolio
            )
        account_preflight, mainnet_preflight = preflight_snapshots
        events: List[dict] = []
        for lane in pending_lanes:
            if lane.executor is None:
                continue
            operational_preflight = (
                await lane.executor.build_operational_preflight(
                    mainnet_preflight=mainnet_preflight,
                )
            )
            events.extend(
                self.tag_events(
                    lane,
                    await lane.executor.prepare_symbol(
                        symbol=lane.symbol,
                        reference_price=reference_prices.get(
                            lane.symbol,
                            0.0,
                        ),
                        portfolio=portfolio,
                        reference_prices=reference_prices,
                        account_preflight=account_preflight,
                        operational_preflight=operational_preflight,
                    ),
                )
            )
            lane.prepared = True
        return events

    async def preflight_account_writes(
        self,
        *,
        portfolio: object,
    ) -> RWUSDPreflightSnapshots:
        if self.primary_executor is None:
            raise RuntimeError("RWUSD primary executor is unavailable")
        account_preflight, mainnet_preflight = (
            await load_rwusd_preflight_snapshots(
                self.client,
                include_mainnet=self.live_rwusd_execution,
            )
        )
        self.primary_executor.validate_preparation(
            symbol=self.primary_symbol,
            portfolio=portfolio,
            account_preflight=account_preflight,
        )
        if mainnet_preflight is not None:
            mainnet_preflight.validate_security()
        account_preflight = await self.primary_executor.ensure_hedge_mode(
            account_preflight
        )
        return account_preflight, mainnet_preflight

    async def inspect_lanes_read_only(
        self,
        *,
        portfolio: object,
    ) -> List[dict]:
        lanes = [
            lane
            for lane in self.lanes()
            if lane.executor is not None
        ]
        if not lanes:
            return []
        account_preflight, mainnet_preflight = (
            await load_rwusd_preflight_snapshots(
                self.client,
                include_mainnet=self.live_rwusd_execution,
            )
        )
        events: List[dict] = []
        for lane in lanes:
            if lane.executor is None:
                continue
            events.extend(
                self.tag_events(
                    lane,
                    await lane.executor.inspect_read_only(
                        symbol=lane.symbol,
                        portfolio=portfolio,
                        account_preflight=account_preflight,
                        mainnet_preflight=mainnet_preflight,
                    ),
                )
            )
        return events

    def preparation_required(self) -> bool:
        return any(
            not lane.prepared and self._lane_preparation_required(lane)
            for lane in self.lanes()
        )

    def _lane_preparation_required(self, lane: RWUSDSymbolLane) -> bool:
        return bool(
            lane.executor is not None
            and (
                self._lane_capital_basis(lane) > 0
                or lane.engine.exchange_ownership_established()
            )
        )

    def _refresh_lane_budget(self, lane: RWUSDSymbolLane) -> None:
        capital = self._lane_capital_basis(lane)
        lane.engine.initial_capital = capital
        if lane.executor is not None:
            lane.executor.set_capital_budget(capital)

    def _lane_capital_basis(self, lane: RWUSDSymbolLane) -> float:
        external_capital = float(
            self.coordinator.symbol_capital(lane.symbol)
        )
        bucket = lane.engine.profit_bucket
        reinvested_profit = max(
            0.0,
            float(bucket.rwusd_principal or 0)
            - float(bucket.capital_contribution_total or 0),
        )
        return external_capital + reinvested_profit

    def lanes(self) -> List[RWUSDSymbolLane]:
        weights = getattr(self.config, "ALLOCATION_TARGET_WEIGHTS", {})
        return sorted(
            self._lanes.values(),
            key=lambda lane: (
                not lane.primary,
                -float(weights.get(lane.symbol, 0) or 0),
                lane.symbol,
            ),
        )

    @staticmethod
    def tag_events(
        lane: RWUSDSymbolLane,
        events: Iterable[Mapping[str, object]],
    ) -> List[dict]:
        tagged: List[dict] = []
        for event in events:
            payload = dict(event)
            payload.setdefault("symbol", lane.symbol)
            tagged.append(payload)
        return tagged

    def active_lanes(self) -> List[RWUSDSymbolLane]:
        active = set(self.coordinator.active_symbols())
        return [
            lane
            for lane in self.lanes()
            if lane.symbol in active
            or lane.engine.exchange_ownership_established()
        ]

    def market_symbols(self) -> List[str]:
        active = {lane.symbol for lane in self.active_lanes()}
        return [
            lane.symbol
            for lane in self.lanes()
            if (
                lane.primary
                or lane.symbol in active
                or self._lane_preparation_required(lane)
            )
        ]

    def mark_initial_collateralized(self, amount: float) -> Decimal:
        return self.coordinator.mark_pending_capital_collateralized(amount)

    def inherit_initial_collateralization(self) -> List[dict]:
        if not (
            self.primary_engine.profit_bucket
            .initial_collateralization_completed
        ):
            return []
        events: List[dict] = []
        for lane in self.lanes():
            if lane.primary:
                continue
            if (
                lane.engine.profit_bucket
                .initial_collateralization_completed
            ):
                continue
            events.extend(
                self.tag_events(
                    lane,
                    [
                        lane.engine.complete_initial_collateralization(
                            mode="portfolio_controller_ready"
                        )
                    ],
                )
            )
        return events

    def pending_subscription_lane(self) -> Optional[RWUSDSymbolLane]:
        pending = [
            lane
            for lane in self.lanes()
            if lane.engine.profit_bucket.pending_subscription_stage
        ]
        if len(pending) > 1:
            raise RuntimeError(
                "RWUSD account has multiple pending subscription lanes: "
                + ",".join(lane.symbol for lane in pending)
            )
        return pending[0] if pending else None

    def harvest_subscription_lane(
        self,
        portfolio: object,
    ) -> Optional[RWUSDSymbolLane]:
        if self.pending_subscription_lane() is not None:
            return None
        candidates = []
        for lane in self.active_lanes():
            if lane.executor is None:
                continue
            amount = lane.engine.live_subscription_amount(portfolio)
            if amount <= 0:
                continue
            started_at_ms = max(
                0,
                int(lane.engine.state.profit_sweep_started_at_ms or 0),
            )
            candidates.append((lane, amount, started_at_ms))
        if not candidates:
            return None
        return min(
            candidates,
            key=lambda item: (
                item[2] <= 0,
                item[2] if item[2] > 0 else 0,
                -item[1],
                item[0].symbol,
            ),
        )[0]

    def _account_harvest_margin_reserves(self) -> Dict[str, float]:
        configured = getattr(
            self.config,
            "RWUSD_SOURCE_ASSETS",
            ("USDC", "USDT"),
        )
        if isinstance(configured, str):
            configured = (configured,)
        assets = tuple(
            str(asset or "").strip().upper()
            for asset in configured or ()
            if str(asset or "").strip().upper() in {"USDC", "USDT"}
        ) or ("USDC",)
        reserves: Dict[str, float] = {}
        for lane in self.lanes():
            bucket = lane.engine.profit_bucket
            amount = max(
                0.0,
                float(bucket.harvest_buffer or 0),
                float(
                    bucket.realized_pnl_available_for_deposit or 0
                ),
            ) + max(0.0, float(bucket.unsettled_harvest_pnl or 0))
            if amount <= 0:
                continue
            symbol = str(
                lane.engine.state.selected_symbol or lane.symbol
            ).strip().upper()
            asset = next(
                (candidate for candidate in assets if symbol.endswith(candidate)),
                assets[0],
            )
            reserves[asset] = reserves.get(asset, 0.0) + amount
        return reserves

    async def advance_account_subscription(
        self,
        portfolio: object,
        *,
        allow_new_capital: bool,
    ) -> tuple[List[dict], bool]:
        lane = self.pending_subscription_lane()
        if lane is not None:
            if lane.executor is None:
                raise RuntimeError(
                    "RWUSD pending subscription executor is unavailable"
                )
            pending_kind = str(
                lane.engine.profit_bucket.pending_subscription_kind
                or "harvest"
            ).strip().lower()
            events = self.tag_events(
                lane,
                await lane.executor.execute_live_subscription(
                    portfolio,
                    kind=(
                        "capital"
                        if pending_kind in {
                            "capital",
                            "capital_existing_rwusd",
                        }
                        else pending_kind
                    ),
                    requested_amount=0.0,
                ),
            )
            self.apply_capital_events(events)
            return events, True
        if self.harvest_subscription_lane(portfolio) is not None:
            return await self.advance_harvest_subscription(portfolio), True
        pending_capital = self.pending_external_capital_subscription()
        if not allow_new_capital or pending_capital <= 0:
            return [], False
        primary = next(lane for lane in self.lanes() if lane.primary)
        if primary.executor is None:
            raise RuntimeError("RWUSD primary executor is unavailable")
        events = self.tag_events(
            primary,
            await primary.executor.execute_live_subscription(
                portfolio,
                kind="capital",
                requested_amount=float(pending_capital),
            ),
        )
        self.apply_capital_events(events)
        return events, True

    async def _register_incremental_capital_intent(
        self,
        *,
        amount: float,
        source_asset: str,
        capital_contribution_before: float,
        allocation_market: object,
    ) -> Optional[dict]:
        value = max(0.0, float(amount or 0))
        asset = str(source_asset or "").strip().upper()
        if value <= 0 or not asset:
            return None
        primary = next(
            lane for lane in self.lanes() if lane.primary
        )
        reference = primary.engine.incremental_capital_reference(
            source_asset=asset,
            amount=value,
            capital_contribution_before=capital_contribution_before,
        )
        existing = self.coordinator.ledger.capital_lot_by_reference(
            reference
        )
        if existing is not None:
            return {
                "type": "rwusd_external_capital",
                "action": "incremental_capital_duplicate_ignored",
                "lot_id": existing.lot_id,
                "external_reference": reference,
                "asset": asset,
                "amount": value,
                "execution_wired": True,
            }
        rows = list(await allocation_market.build())
        lot, allocation, _ = self.coordinator.register_external_capital(
            amount=value,
            source_asset=asset,
            external_reference=reference,
            rows=rows,
            collateralized=False,
            capital_contribution_before=capital_contribution_before,
        )
        self.ensure_lanes()
        return {
            "type": "rwusd_external_capital",
            "action": "incremental_capital_registered_pending",
            "lot_id": lot.lot_id,
            "external_reference": reference,
            "asset": asset,
            "amount": value,
            "allocation": allocation.to_dict(),
            "capital_contribution_before": capital_contribution_before,
            "execution_wired": True,
        }

    async def advance_incremental_capital(
        self,
        portfolio: object,
        *,
        allocation_market: object,
        allow_new_capital: bool,
    ) -> tuple[List[dict], bool]:
        """Run the live incremental path before any exchange write."""
        if not self.live_rwusd_execution or not allow_new_capital:
            return [], False
        bucket = self.primary_engine.profit_bucket
        if self.pending_subscription_lane() is not None:
            return [], False
        discarded = (
            self.coordinator.discard_unconfirmed_incremental_capital(
                bucket.capital_contribution_total
            )
        )
        events: List[dict] = []
        if discarded > ZERO:
            for lane in self._lanes.values():
                self._refresh_lane_budget(lane)
            events.append(
                {
                    "type": "rwusd_capital_reconciliation",
                    "action": "incremental_capital_discarded_unconfirmed",
                    "amount": float(discarded),
                    "reason": "capital_contribution_not_confirmed",
                    "execution_wired": True,
                }
            )
        confirmed_reference = str(
            bucket.confirmed_capital_reference or ""
        ).strip()
        reconciled = (
            self.coordinator.reconcile_pending_incremental_capital(
                bucket.capital_contribution_total,
                external_reference=confirmed_reference,
                capital_contribution_before=(
                    bucket.confirmed_capital_contribution_before
                ),
                capital_contribution_amount=(
                    bucket.confirmed_capital_contribution_amount
                ),
            )
            if confirmed_reference
            else 0
        )
        if confirmed_reference:
            lot = self.coordinator.ledger.capital_lot_by_reference(
                confirmed_reference
            )
            if lot is None:
                raise RuntimeError(
                    "RWUSD confirmed incremental capital cannot be "
                    "found in the portfolio ledger"
                )
            confirmed_baseline = nonnegative_money(
                bucket.confirmed_capital_contribution_before,
                "confirmed capital contribution before",
            )
            confirmed_amount = nonnegative_money(
                bucket.confirmed_capital_contribution_amount,
                "confirmed capital contribution amount",
            )
            if (
                lot.capital_contribution_before != confirmed_baseline
                or lot.amount != confirmed_amount
            ):
                raise RuntimeError(
                    "RWUSD confirmed incremental capital conflicts with "
                    "its persisted portfolio lot"
                )
            if lot.pending_collateralization > 0 and reconciled <= 0:
                raise RuntimeError(
                    "RWUSD confirmed incremental capital cannot be "
                    "reconciled with its persisted portfolio lot"
                )
            self.primary_engine.acknowledge_confirmed_capital_reference(
                confirmed_reference
            )
            if reconciled > 0:
                for lane in self._lanes.values():
                    self._refresh_lane_budget(lane)
        account_harvest_reserves = (
            self._account_harvest_margin_reserves()
        )
        if self.harvest_subscription_lane(portfolio) is not None:
            return events, False
        if self.pending_external_capital_subscription() > 0:
            return events, False
        primary = next(
            lane for lane in self.lanes() if lane.primary
        )
        if primary.executor is None:
            raise RuntimeError(
                "RWUSD incremental capital executor is unavailable"
            )
        subscription_events = self.tag_events(
            primary,
            await primary.executor.process_incremental_collateralization(
                portfolio,
                capital_intent_callback=(
                    lambda **kwargs: (
                        self._register_incremental_capital_intent(
                            allocation_market=allocation_market,
                            **kwargs,
                        )
                    )
                ),
                margin_reserves=account_harvest_reserves,
            ),
        )
        events.extend(subscription_events)
        self.apply_capital_events(events)
        return events, bool(
            primary.engine.profit_bucket.pending_subscription_stage
            or any(
                event.get("type") in {
                    "rwusd_external_capital",
                    "rwusd_subscription",
                    "rwusd_transfer",
                }
                for event in events
            )
        )

    async def advance_harvest_subscription(
        self,
        portfolio: object,
    ) -> List[dict]:
        lane = self.harvest_subscription_lane(portfolio)
        if lane is None:
            return []
        if lane.executor is None:
            raise RuntimeError(
                "RWUSD harvest subscription executor is unavailable"
            )
        return self.tag_events(
            lane,
            await lane.executor.execute_live_subscription(portfolio),
        )

    def deposit_scan_due(self) -> bool:
        if not self.live_rwusd_execution:
            return False
        interval = max(
            5.0,
            float(getattr(self.config, "CAPITAL_SCAN_INTERVAL_SEC", 30.0)),
        )
        return (
            self._last_deposit_scan_monotonic <= 0
            or time.monotonic() - self._last_deposit_scan_monotonic
            >= interval
        )

    async def scan_external_deposits(
        self,
        *,
        now_ms: int,
    ) -> Optional[tuple[ExternalDepositHistoryBatch, int]]:
        if not self.deposit_scan_due():
            return None
        if (
            not self.coordinator.capital_bootstrap_source
            and not self.coordinator.ledger.capital_lots
        ):
            return None
        self._last_deposit_scan_monotonic = time.monotonic()
        if self.coordinator.deposit_detection_started_at_ms <= 0:
            self.coordinator.start_deposit_detection(now_ms)
            return None
        ranges = self.coordinator.deposit_scan_ranges(now_ms)
        baseline = self.coordinator.deposit_detection_started_at_ms
        initial_scan = (
            self.coordinator.deposit_scan_completed_through_ms <= baseline
        )
        confirmed_after_ms: Optional[int] = None
        unresolved_after_ms: Optional[int] = None
        if initial_scan:
            lookback_start = max(
                1,
                baseline - MAX_DEPOSIT_HISTORY_WINDOW_MS,
            )
            ranges = [(lookback_start, now_ms)]
            confirmed_after_ms = baseline
            unresolved_after_ms = lookback_start - 1
        history = await fetch_external_deposit_history(
            self.client,
            allowed_assets=getattr(
                self.config,
                "RWUSD_SOURCE_ASSETS",
                ("USDC", "USDT"),
            ),
            time_ranges=ranges,
            confirmed_after_ms=confirmed_after_ms,
            unresolved_after_ms=unresolved_after_ms,
        )
        return history, now_ms

    def complete_deposit_scan(
        self,
        *,
        history: ExternalDepositHistoryBatch,
        allocation_rows: Iterable[Mapping[str, object]],
        completed_through_ms: int,
    ) -> List[dict]:
        events = self.coordinator.complete_deposit_scan(
            history=history,
            rows=allocation_rows,
            completed_through_ms=completed_through_ms,
        )
        self.ensure_lanes()
        return events

    def pending_capital_subscription(self) -> float:
        return float(self.coordinator.pending_capital_collateralization())

    def pending_external_capital_subscription(self) -> float:
        return float(
            self.coordinator.pending_capital_collateralization(
                exclude_prefixes=("binance-incremental:",),
            )
        )

    def initial_collateralization_request(self) -> float:
        if (
            self.primary_engine.profit_bucket
            .initial_collateralization_completed
        ):
            return 0.0
        return float(self.coordinator.initial_collateralization_target)

    def apply_capital_events(
        self,
        events: Iterable[Mapping[str, object]],
    ) -> None:
        for event in events:
            if not event.get("collateralized"):
                continue
            kind = str(
                event.get("subscription_kind") or ""
            ).strip().lower()
            if kind not in {
                "initial",
                "initial_existing_rwusd",
                "capital",
                "capital_existing_rwusd",
            } and event.get("action") != "initial_rwusd_collateral_ready":
                continue
            amount_field = (
                "rwusd_amount"
                if kind in {
                    "initial_existing_rwusd",
                    "capital_existing_rwusd",
                }
                or event.get("action")
                == "existing_rwusd_to_portfolio_margin"
                or event.get("action")
                == "initial_rwusd_collateral_ready"
                else "capital_contribution_amount"
            )
            amount = max(
                0.0,
                float(
                    event.get(amount_field)
                    or event.get("source_amount")
                    or event.get("amount")
                    or 0
                ),
            )
            if amount > 0:
                if kind in {"capital", "capital_existing_rwusd"}:
                    contribution = max(
                        0.0,
                        float(
                            (
                                event.get("source_amount") or 0
                                if kind == "capital"
                                else (
                                    event.get("capital_contribution_amount")
                                    or 0
                                )
                            )
                        ),
                    )
                    capital_reference = str(
                        event.get("capital_reference") or ""
                    ).strip()
                    if capital_reference.startswith(
                        "binance-incremental:"
                    ):
                        source_asset = str(
                            event.get("source_asset")
                            or event.get("asset")
                            or ""
                        )
                        applied = (
                            self.coordinator
                            .mark_incremental_capital_collateralized(
                                amount=contribution,
                                source_asset=source_asset,
                                external_reference=capital_reference,
                            )
                        )
                    else:
                        applied = (
                            self.coordinator
                            .mark_pending_capital_collateralized(
                                contribution,
                                source_asset=str(
                                    event.get("source_asset") or ""
                                ),
                                exclude_prefixes=(
                                    "binance-incremental:",
                                ),
                            )
                        )
                    if float(applied) + 1e-8 < contribution:
                        raise RuntimeError(
                            "RWUSD confirmed incremental capital has no "
                            "matching pending portfolio lot"
                        )
                    if capital_reference.startswith(
                        "binance-incremental:"
                    ):
                        primary = next(
                            lane
                            for lane in self.lanes()
                            if lane.primary
                        )
                        primary.engine.acknowledge_confirmed_capital_reference(
                            capital_reference
                        )
                else:
                    self.coordinator.mark_pending_capital_collateralized(
                        amount
                    )
        for lane in self._lanes.values():
            self._refresh_lane_budget(lane)

    def finalize_initial_capital_accounting(self) -> List[dict]:
        if (
            self._initial_capital_reconciled
            or not self.primary_engine.profit_bucket
            .initial_collateralization_completed
        ):
            return []
        events: List[dict] = []
        official_bootstrap = self.coordinator.capital_bootstrap_source in {
            "binance_futures_demo_wallet",
            "binance_mainnet_official_assets",
        }
        bucket = self.primary_engine.profit_bucket
        confirmed_capital = max(
            0.0,
            float(bucket.capital_contribution_total or 0),
            float(
                bucket.initial_collateralization_source_amount
                + bucket.initial_collateralization_existing_rwusd_amount
            ),
            float(bucket.initial_collateralization_rwusd or 0),
        )
        if official_bootstrap:
            principal = (
                float(self.coordinator.ledger.total_capital)
                if not self.live_rwusd_execution
                else confirmed_capital
            )
            events.append(
                self.primary_engine.adopt_official_capital_ledger(
                    principal
                )
            )
            confirmed_capital = principal
        already_collateralized = sum(
            (
                lot.collateralized_amount
                for lot in self.coordinator.ledger.capital_lots.values()
                if not lot.external_reference.startswith(
                    "binance-incremental:"
                )
            ),
            ZERO,
        )
        shortfall = max(
            ZERO,
            nonnegative_money(
                confirmed_capital,
                "confirmed initial capital",
            )
            - already_collateralized,
        )
        if shortfall > ZERO:
            self.coordinator.mark_pending_capital_collateralized(
                shortfall,
                exclude_prefixes=("binance-incremental:",),
            )
        self._initial_capital_reconciled = True
        for lane in self._lanes.values():
            self._refresh_lane_budget(lane)
        return events

    def reserve_allocation_due(self) -> bool:
        if self.coordinator.ledger.allocatable_reserve <= 0:
            return False
        interval = max(
            60.0,
            float(
                getattr(
                    self.config,
                    "ALLOCATION_RESERVE_REEVALUATE_SEC",
                    15 * 60.0,
                )
            ),
        )
        return (
            self._last_reserve_allocation_monotonic <= 0
            or time.monotonic()
            - self._last_reserve_allocation_monotonic
            >= interval
        )

    def allocate_reserve(
        self,
        allocation_rows: Iterable[Mapping[str, object]],
    ) -> Optional[dict]:
        self._last_reserve_allocation_monotonic = time.monotonic()
        result = self.coordinator.allocate_reserve(rows=allocation_rows)
        if result is None:
            return None
        self.ensure_lanes()
        return {
            "type": "rwusd_capital_allocation",
            "action": "reserve_reallocated",
            "allocation": result.to_dict(),
            "execution_wired": True,
        }

    def request_customer_exit(self, now_ms: int) -> List[dict]:
        events: List[dict] = []
        for lane in self.lanes():
            events.extend(
                self.tag_events(
                    lane,
                    [
                        lane.engine.request_customer_exit(
                            now_ms=now_ms,
                            redemption_required=(
                                lane.primary
                                and self.live_rwusd_execution
                            ),
                        )
                    ],
                )
            )
        return events

    async def advance_customer_exit(self) -> List[dict]:
        events: List[dict] = []
        lanes = self.lanes()
        secondary = [lane for lane in lanes if not lane.primary]
        primary = next((lane for lane in lanes if lane.primary), None)
        for lane in [*secondary, *([primary] if primary is not None else [])]:
            if (
                lane.executor is not None
                and lane.engine.exit_worker_required()
            ):
                finalize_flat = (
                    not lane.primary
                    or all(
                        item.engine.exit_state.stage
                        == RWUSDExitStage.COMPLETE
                        for item in secondary
                    )
                )
                events.extend(
                    self.tag_events(
                        lane,
                        await lane.executor.advance_customer_exit(
                            finalize_flat=finalize_flat
                        ),
                    )
                )
        return events

    def exit_worker_required(self) -> bool:
        return any(
            lane.engine.exit_worker_required()
            for lane in self.lanes()
        )

    def next_exit_retry_after_ms(self, now_ms: int) -> int:
        effective_now = max(0, int(now_ms or 0))
        retry_times = [
            max(
                0,
                int(lane.engine.exit_state.retry_after_ms or 0),
            )
            for lane in self.lanes()
            if lane.engine.exit_worker_required()
        ]
        if not retry_times or any(
            retry_after <= effective_now
            for retry_after in retry_times
        ):
            return effective_now
        return min(retry_times)

    def can_redeem(self) -> bool:
        return bool(
            self.primary_engine.exit_state.stage
            == RWUSDExitStage.READY_TO_REDEEM
            and all(
                lane.primary
                or lane.engine.exit_state.stage == RWUSDExitStage.COMPLETE
                for lane in self.lanes()
            )
        )

    def request_customer_redemption(self, now_ms: int) -> dict:
        if not self.can_redeem():
            raise RuntimeError(
                "RWUSD redemption requires every symbol lane to be flat"
            )
        return self.primary_engine.request_customer_redemption(now_ms)

    def clear_completed_exit_for_restart(self) -> bool:
        lanes = self.lanes()
        if not all(
            lane.engine.exit_state.stage == RWUSDExitStage.COMPLETE
            or lane.engine.official_restart_rearmed()
            for lane in lanes
        ):
            return False
        if self.execution_enabled:
            self.coordinator.reset_for_official_restart()
            self._pending_official_snapshot = None
            self._last_official_bootstrap_monotonic = 0.0
            self._last_deposit_scan_monotonic = 0.0
            self._last_reserve_allocation_monotonic = 0.0
            self._legacy_official_adoption_required = False
        else:
            self.coordinator.reset_collateralization_for_restart()
        for lane in lanes:
            if (
                lane.engine.exit_state.stage == RWUSDExitStage.COMPLETE
                and not lane.engine.clear_completed_exit_for_restart()
            ):
                return False
            lane.engine.state.selected_symbol = lane.symbol
            lane.engine.state.phase = StrategyPhase.SELECTING_SYMBOL
            lane.engine.persist()
            lane.prepared = False
            if lane.executor is not None:
                lane.executor.reset_for_new_cycle()
        self._initial_capital_reconciled = False
        for lane in lanes:
            self._refresh_lane_budget(lane)
        return True

    def account_profit_bucket(self) -> dict:
        lanes = self.lanes()
        primary = self.primary_engine.profit_bucket.to_dict()
        if not lanes:
            return primary
        float_totals = {
            "realized_pnl_total",
            "realized_pnl_available_for_deposit",
            "harvest_buffer",
            "capital_contribution_total",
            "rwusd_principal",
            "rwusd_redeemable",
            "rwusd_units",
            "exchange_realized_pnl_total",
            "exchange_commission_total",
            "unsettled_harvest_pnl",
            "pending_harvest_net_credited",
        }
        integer_totals = {
            "harvest_count",
            "deposit_count",
            "redeem_count",
            "exchange_trade_count",
        }
        for field in float_totals:
            primary[field] = sum(
                float(
                    getattr(lane.engine.profit_bucket, field, 0.0)
                    or 0.0
                )
                for lane in lanes
            )
        for field in integer_totals:
            primary[field] = sum(
                int(
                    getattr(lane.engine.profit_bucket, field, 0)
                    or 0
                )
                for lane in lanes
            )
        primary["closed_loop_ready"] = any(
            lane.engine.profit_bucket.closed_loop_ready
            for lane in lanes
        )
        pending = [
            lane
            for lane in lanes
            if lane.engine.profit_bucket.pending_subscription_stage
        ]
        primary["pending_subscription_symbol"] = (
            pending[0].symbol if len(pending) == 1 else None
        )
        primary["pending_subscription_conflict"] = (
            [lane.symbol for lane in pending]
            if len(pending) > 1
            else []
        )
        status_lane = pending[0] if len(pending) == 1 else None
        if status_lane is None and not pending:
            harvest_lanes = [
                lane
                for lane in lanes
                if float(
                    lane.engine.profit_bucket.harvest_buffer or 0
                )
                > 1e-12
            ]
            if harvest_lanes:
                status_lane = min(
                    harvest_lanes,
                    key=lambda lane: (
                        int(
                            lane.engine.state.profit_sweep_started_at_ms
                            or 0
                        )
                        <= 0,
                        int(
                            lane.engine.state.profit_sweep_started_at_ms
                            or 0
                        ),
                        -float(
                            lane.engine.profit_bucket.harvest_buffer or 0
                        ),
                        lane.symbol,
                    ),
                )
        primary["sweep_block_symbol"] = (
            status_lane.symbol if status_lane is not None else None
        )
        if status_lane is not None:
            source = status_lane.engine.profit_bucket.to_dict()
            primary["sweep_block_reason"] = source.get(
                "sweep_block_reason"
            )
            primary["last_rebalance_action"] = source.get(
                "last_rebalance_action"
            )
        if (
            len(pending) == 1
            and status_lane is not None
            and not status_lane.primary
        ):
            for field, value in source.items():
                if field.startswith("pending_"):
                    primary[field] = value
        return primary

    def exit_status(self) -> dict:
        payload = self.primary_engine.exit_status()
        lane_status = {
            lane.symbol: lane.engine.exit_status()
            for lane in self.lanes()
        }
        if any(
            status["stage"] == RWUSDExitStage.LIQUIDATING.value
            for status in lane_status.values()
        ):
            payload.update(
                {
                    "stage": RWUSDExitStage.LIQUIDATING.value,
                    "can_redeem": False,
                    "can_resume_liquidation": True,
                }
            )
        payload["can_redeem"] = self.can_redeem()
        payload["liquidation_batch_count"] = sum(
            int(status.get("liquidation_batch_count", 0) or 0)
            for status in lane_status.values()
        )
        payload["liquidation_order_count"] = sum(
            int(status.get("liquidation_order_count", 0) or 0)
            for status in lane_status.values()
        )
        lane_errors = {
            symbol: str(status.get("last_error") or "")
            for symbol, status in lane_status.items()
            if status.get("last_error")
        }
        payload["lane_errors"] = lane_errors
        if lane_errors:
            payload["last_error"] = "; ".join(
                f"{symbol}:{error}"
                for symbol, error in sorted(lane_errors.items())
            )
        payload["lanes"] = lane_status
        return payload

    def status(self) -> dict:
        deployed = self.coordinator.ledger.deployed_capital
        deployed_total = sum(deployed.values(), Decimal("0"))
        core_deployed = sum(
            (
                deployed.get("BTCUSDC", Decimal("0")),
                deployed.get("ETHUSDC", Decimal("0")),
            ),
            Decimal("0"),
        )
        return {
            **self.coordinator.get_status(),
            "primary_symbol": self.primary_symbol,
            "official_capital_snapshot_pending": (
                self._pending_official_snapshot is not None
            ),
            "ownership": self.ownership.status(),
            "lane_count": len(self._lanes),
            "active_lane_count": len(self.active_lanes()),
            "core_deployed_fraction": (
                float(core_deployed / deployed_total)
                if deployed_total > 0
                else 0.0
            ),
            "lanes": {
                lane.symbol: {
                    "primary": lane.primary,
                    "prepared": lane.prepared,
                    "allocated_capital": str(
                        self.coordinator.symbol_capital(
                            lane.symbol,
                            deployed=False,
                        )
                    ),
                    "deployed_capital": str(
                        self.coordinator.symbol_capital(lane.symbol)
                    ),
                    "reinvested_profit_capital": max(
                        0.0,
                        float(lane.engine.profit_bucket.rwusd_principal or 0)
                        - float(
                            lane.engine.profit_bucket
                            .capital_contribution_total
                            or 0
                        ),
                    ),
                    "capital_basis": self._lane_capital_basis(lane),
                    "phase": lane.engine.state.phase.value,
                    "long_quantity": lane.engine.state.long_quantity,
                    "short_quantity": lane.engine.state.short_quantity,
                }
                for lane in self.lanes()
            },
        }
