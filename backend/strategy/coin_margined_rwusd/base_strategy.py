"""DBRSI adapter for isolated RWUSD Testnet and Mainnet execution."""

from __future__ import annotations

import asyncio
import math
import time
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Tuple

from backend.config.coin_margined_rwusd_strategy_config import CoinMarginedRWUSDStrategyConfig
from backend.strategy.coin_margined_rwusd.allocation_market import RWUSDAllocationMarket
from backend.strategy.coin_margined_rwusd.binance_view import (
    BinanceRWUSDExecutionView,
    ReadOnlyBinanceView,
)
from backend.strategy.coin_margined_rwusd.engine import RWUSDEngine
from backend.strategy.coin_margined_rwusd.exit import RWUSDExitStage
from backend.strategy.coin_margined_rwusd.market_runtime import RWUSDMarketRuntime
from backend.strategy.coin_margined_rwusd.models import (
    MarketRow,
    PortfolioSnapshot,
    StrategyPhase,
    parse_bool,
)
from backend.strategy.coin_margined_rwusd.execution import BinanceRWUSDExecutor
from backend.strategy.coin_margined_rwusd.portfolio_runtime import RWUSDPortfolioRuntime
from backend.strategy.coin_margined_rwusd.preflight import RWUSDPreflightSnapshots
from backend.strategy.coin_margined_rwusd.yield_projection import (
    build_rwusd_yield_projection,
)

_ReadOnlyBinanceView = ReadOnlyBinanceView
_BinanceRWUSDExecutionView = BinanceRWUSDExecutionView


class CoinMarginedRWUSDBaseStrategy:
    strategy_type = "coin_margined_rwusd_strategy"

    def __init__(
        self,
        account_name: str,
        binance_client: object,
        total_capital: float,
        position_margin_percent: Optional[float] = None,
        leverage: Optional[int] = None,
        harvest_min_net_usd: Optional[float] = None,
        force_dry_run: bool = False,
        symbol: str = "BTCUSDC",
        on_signal_callback: Optional[Callable] = None,
        on_trade_callback: Optional[Callable] = None,
        on_error_callback: Optional[Callable] = None,
    ) -> None:
        self.account_name = account_name
        self.strategy_config = CoinMarginedRWUSDStrategyConfig
        if harvest_min_net_usd is not None:
            try:
                account_harvest_minimum = float(harvest_min_net_usd)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "RWUSD harvest minimum must be a positive number"
                ) from exc
            if (
                not math.isfinite(account_harvest_minimum)
                or account_harvest_minimum <= 0.0
            ):
                raise ValueError(
                    "RWUSD harvest minimum must be a positive number"
                )
            self.strategy_config = type(
                "AccountCoinMarginedRWUSDStrategyConfig",
                (CoinMarginedRWUSDStrategyConfig,),
                {"HARVEST_MIN_NET_USD": account_harvest_minimum},
            )
        read_only_client = ReadOnlyBinanceView(binance_client)
        self.is_testnet = read_only_client.is_testnet
        self.force_dry_run = bool(force_dry_run)
        self.execution_enabled = bool(
            not self.force_dry_run
            and (
                CoinMarginedRWUSDStrategyConfig.TESTNET_EXECUTION_ENABLED
                if self.is_testnet
                else CoinMarginedRWUSDStrategyConfig.MAINNET_EXECUTION_ENABLED
            )
        )
        execution_client = (
            BinanceRWUSDExecutionView(binance_client)
            if self.execution_enabled
            else None
        )
        self.client = execution_client or read_only_client
        self.execution_mode = (
            "binance_futures_demo"
            if self.is_testnet and self.execution_enabled
            else (
                "binance_portfolio_margin_live"
                if self.execution_enabled
                else "local_dry_run"
            )
        )
        requested_simulation_capital = max(
            0.0,
            float(total_capital or 0),
        )
        if not self.execution_enabled and requested_simulation_capital <= 0:
            requested_simulation_capital = float(
                CoinMarginedRWUSDStrategyConfig.DRY_RUN_INITIAL_CAPITAL
            )
        self.total_capital = (
            0.0 if self.execution_enabled else requested_simulation_capital
        )
        self.position_margin_percent = float(
            CoinMarginedRWUSDStrategyConfig.TOTAL_MARGIN_FRACTION
            if position_margin_percent is None
            else position_margin_percent
        )
        self.target_leverage = int(
            CoinMarginedRWUSDStrategyConfig.SIM_LEVERAGE
            if leverage is None
            else leverage
        )
        if not 0 < self.position_margin_percent <= 0.80:
            raise ValueError(
                "RWUSD total margin percent must be within (0%, 80%]"
            )
        if not 1 <= self.target_leverage <= 50:
            raise ValueError("RWUSD leverage must be within [1x, 50x]")
        initial_leverage = (
            self.target_leverage
            if self.is_testnet
            else min(self.target_leverage, 20)
        )
        self.symbol = (
            str(symbol or "").strip().upper()
            if str(symbol or "").strip().upper() in CoinMarginedRWUSDStrategyConfig.CANDIDATE_SYMBOLS
            else "BTCUSDC"
        )
        self.on_signal = on_signal_callback
        self.on_trade = on_trade_callback
        self.on_error = on_error_callback
        self.is_running = False
        self._task: Optional[asyncio.Task] = None
        self._exit_task: Optional[asyncio.Task] = None
        self._control_lock: Optional[asyncio.Lock] = None
        self._control_lock_loop: Optional[asyncio.AbstractEventLoop] = None
        self._operation_lock: Optional[asyncio.Lock] = None
        self._operation_lock_loop: Optional[asyncio.AbstractEventLoop] = None
        self._last_cycle_monotonic = 0.0
        self._last_error: Optional[str] = None
        self._operational_prepared = False
        self.engine = RWUSDEngine(
            account_name=account_name,
            initial_capital=self.total_capital,
            config=self.strategy_config,
            external_execution=self.execution_enabled,
            execution_environment=self.execution_mode,
            live_rwusd_execution=(
                self.execution_enabled and not self.is_testnet
            ),
            leverage=initial_leverage,
        )
        self.executor: Optional[BinanceRWUSDExecutor] = None
        if execution_client is not None:
            self.executor = BinanceRWUSDExecutor(
                account_name=account_name,
                client=execution_client,
                engine=self.engine,
                config=self.strategy_config,
                total_margin_fraction=self.position_margin_percent,
                leverage=self.target_leverage,
            )
        self.portfolio_runtime = RWUSDPortfolioRuntime(
            account_name=self.account_name,
            simulation_capital=self.total_capital,
            config=self.strategy_config,
            client=self.client,
            execution_environment=self.execution_mode,
            execution_enabled=self.executor is not None,
            live_rwusd_execution=(
                self.executor is not None and not self.is_testnet
            ),
            total_margin_fraction=self.position_margin_percent,
            primary_engine=self.engine,
            primary_executor=self.executor,
        )
        self.allocation_market = RWUSDAllocationMarket(
            client=self.client,
            config=self.strategy_config,
            execution_enabled=self.executor is not None,
        )
        self.market_runtime = RWUSDMarketRuntime(
            client=self.client,
            config=self.strategy_config,
        )

    async def initialize(self) -> bool:
        if str(getattr(self.client, "exchange", "") or "").strip().lower() != "binance":
            await self._emit_error("RWUSD strategy only supports Binance")
            return False
        self.engine.restore()
        try:
            rows, portfolio = await self._build_runtime_snapshot()
            self.engine.observe(
                rows=rows,
                portfolio=portfolio,
                now=datetime.now(timezone.utc),
            )
            self._last_cycle_monotonic = time.monotonic()
            self.symbol = (
                self.engine.state.selected_symbol
                or self.engine.selector_snapshot.selected_symbol
                or self.symbol
            )
            self.portfolio_runtime.restore()
            allocation_rows = (
                await self.allocation_market.build()
                if self.portfolio_runtime.initial_allocation_required()
                else []
            )
            self.portfolio_runtime.initialize(
                allocation_rows=allocation_rows,
                fallback_symbol=self.symbol,
            )
            await self.portfolio_runtime.prime_official_capital_snapshot()
            self.symbol = self.portfolio_runtime.primary_symbol
            row_by_symbol = {
                str(row.get("symbol") or "").strip().upper(): row
                for row in rows
            }
            for lane in self.portfolio_runtime.lanes():
                row = row_by_symbol.get(lane.symbol)
                if row is None:
                    continue
                lane.engine.observe(
                    rows=[row],
                    portfolio=portfolio,
                    now=datetime.now(timezone.utc),
                )
            events = await self.portfolio_runtime.inspect_lanes_read_only(
                portfolio=portfolio
            )
            if events:
                await self._emit_events(events)
            self._last_error = None
            self.engine.state.last_error = None
            self.engine.persist()
            await self._emit_signal(
                "initialized",
                (
                    (
                        "RWUSD Binance Futures Demo initialized; USDC "
                        "LIMIT GTX orders enabled after shadow RWUSD "
                        "collateralization"
                        if self.is_testnet
                        else (
                            "RWUSD Binance Portfolio Margin mainnet read-only "
                            "initialization completed; account writes begin "
                            "only after live start confirmation"
                        )
                    )
                    if self.executor is not None
                    else (
                        "RWUSD local dry-run initialized; orders and transfers "
                        "remain simulated"
                    )
                ),
            )
            return True
        except Exception as exc:
            self._last_error = str(exc)
            self.engine.state.last_error = self._last_error
            self.engine.persist()
            await self._emit_error(f"RWUSD initialization failed: {exc}")
            return False

    def _loop_lock(
        self,
        lock_attribute: str,
        loop_attribute: str,
    ) -> asyncio.Lock:
        loop = asyncio.get_running_loop()
        lock = getattr(self, lock_attribute)
        lock_loop = getattr(self, loop_attribute)
        if lock is None or lock_loop is not loop:
            if lock is not None and lock.locked():
                raise RuntimeError(
                    "RWUSD operation cannot move across active event loops"
                )
            lock = asyncio.Lock()
            setattr(self, lock_attribute, lock)
            setattr(self, loop_attribute, loop)
        return lock

    def _current_control_lock(self) -> asyncio.Lock:
        return self._loop_lock(
            "_control_lock",
            "_control_lock_loop",
        )

    def _current_operation_lock(self) -> asyncio.Lock:
        return self._loop_lock(
            "_operation_lock",
            "_operation_lock_loop",
        )

    async def start(self) -> None:
        async with self._current_control_lock():
            if self.is_running:
                return
            if any(
                lane.engine.exit_state.active
                for lane in self.portfolio_runtime.lanes()
            ):
                raise RuntimeError(
                    "RWUSD customer exit is active and reopening is locked"
                )
            if any(
                lane.engine.exit_state.reopen_locked
                for lane in self.portfolio_runtime.lanes()
            ):
                if not (
                    self.portfolio_runtime.clear_completed_exit_for_restart()
                ):
                    raise RuntimeError(
                        "RWUSD customer exit must complete before reopening"
                    )
                self._operational_prepared = False
                self.market_runtime.clear()
                if self.executor is not None:
                    for lane in self.portfolio_runtime.lanes():
                        if lane.executor is not None:
                            lane.executor.reset_for_new_cycle()
            try:
                async with self._current_operation_lock():
                    rows, portfolio = await self._build_runtime_snapshot()
                    self.engine.observe(
                        rows=rows,
                        portfolio=portfolio,
                        now=datetime.now(timezone.utc),
                    )
                    events: List[dict] = []
                    capital_events, _ = await (
                        self.portfolio_runtime.advance_capital_sources(
                            allocation_market=self.allocation_market,
                            now_ms=int(time.time() * 1000),
                        )
                    )
                    events.extend(capital_events)
                    if not (
                        self.portfolio_runtime
                        .official_capital_bootstrap_required()
                    ):
                        primary_lane = next(
                            lane
                            for lane in self.portfolio_runtime.lanes()
                            if lane.primary
                        )
                        if (
                            primary_lane.engine.profit_bucket
                            .initial_collateralization_completed
                            or primary_lane.engine
                            .exchange_ownership_established()
                        ):
                            events.extend(
                                await self._prepare_operational_start(
                                    rows,
                                    portfolio,
                                )
                            )
                        else:
                            self._validate_execution_portfolio(
                                portfolio,
                                allow_unfunded_initial=True,
                            )
                            if self.executor is not None:
                                await (
                                    self.portfolio_runtime
                                    .preflight_account_writes(
                                        portfolio=portfolio
                                    )
                                )
                    await self._emit_events(events)
            except Exception as exc:
                self._operational_prepared = False
                self._last_error = str(exc)
                self.engine.state.last_error = self._last_error
                self.engine.persist()
                await self._emit_error(f"RWUSD start preflight failed: {exc}")
                raise
            self._last_error = None
            self._last_cycle_monotonic = time.monotonic()
            self.is_running = True
            self._task = asyncio.create_task(self._main_loop())
            await self._emit_signal(
                "started",
                (
                    (
                        "RWUSD Binance Futures Demo started; USDC Maker orders "
                        "start only after shadow RWUSD collateralization"
                        if self.is_testnet
                        else (
                            "Coin-margined RWUSD Portfolio Margin started; "
                            "single BTC/ETH direct collateral, matching USDC "
                            "Maker hedge; Spot profit releases restore while "
                            "RWUSD subscription continues asynchronously"
                        )
                    )
                    if self.executor is not None
                    else (
                        "RWUSD local dry-run started; no exchange order can be "
                        "submitted"
                    )
                ),
            )

    async def stop(self) -> None:
        async with self._current_control_lock():
            self.is_running = False
            task = self._task
            self._task = None
            if task is not None:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            exit_task = self._exit_task
            self._exit_task = None
            if exit_task is not None:
                exit_task.cancel()
                try:
                    await exit_task
                except asyncio.CancelledError:
                    pass
            self._operational_prepared = False
            for lane in self.portfolio_runtime.lanes():
                lane.engine.persist()
            await self._emit_signal(
                "stopped",
                (
                    (
                        "RWUSD Binance Futures Demo stopped"
                        if self.is_testnet
                        else "RWUSD Binance Portfolio Margin mainnet stopped"
                    )
                    if self.executor is not None
                    else "RWUSD local dry-run stopped"
                ),
            )

    async def _main_loop(self) -> None:
        interval = max(1.0, float(CoinMarginedRWUSDStrategyConfig.POLL_INTERVAL_SEC))
        fast_interval = max(
            0.5,
            float(
                getattr(
                    self.strategy_config,
                    "PROFIT_SWEEP_FAST_POLL_INTERVAL_SEC",
                    1.0,
                )
            ),
        )
        while self.is_running:
            full_started_at = time.monotonic()
            await self._run_scheduled_cycle(fast=False)
            next_full_at = full_started_at + interval
            next_fast_at = time.monotonic() + fast_interval
            while self.is_running:
                now = time.monotonic()
                if now >= next_full_at:
                    break
                fast_active = self._fast_profit_progress_required()
                due_at = (
                    min(next_full_at, next_fast_at)
                    if fast_active
                    else next_full_at
                )
                await asyncio.sleep(max(0.1, due_at - now))
                now = time.monotonic()
                if now >= next_full_at:
                    break
                if fast_active and now >= next_fast_at:
                    await self._run_scheduled_cycle(fast=True)
                    next_fast_at = max(
                        next_fast_at + fast_interval,
                        time.monotonic(),
                    )

    async def _run_scheduled_cycle(self, *, fast: bool) -> None:
        try:
            if fast:
                await self._run_fast_profit_progress_cycle()
            else:
                await self._run_cycle()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._last_error = str(exc)
            self.engine.state.last_error = self._last_error
            self.engine.persist()
            await self._emit_error(
                f"RWUSD {self.execution_mode} "
                f"{'fast ' if fast else ''}cycle failed: {exc}"
            )

    def _fast_profit_progress_required(self) -> bool:
        if not self.portfolio_runtime.live_rwusd_execution:
            return False
        transient_reasons = {
            "",
            "profit_sweep_pending",
            "official_profit_reconciliation_pending",
            "rwusd_subscription_pending",
        }
        for lane in self.portfolio_runtime.active_lanes():
            bucket = lane.engine.profit_bucket
            if bucket.pending_subscription_stage:
                return True
            if (
                lane.engine.state.phase == StrategyPhase.PROFIT_SWEEPING
                and str(bucket.sweep_block_reason or "").strip()
                in transient_reasons
            ):
                return True
        return False

    async def _run_fast_profit_progress_cycle(self) -> None:
        async with self._current_operation_lock():
            events: List[dict] = []
            portfolio = await self._build_portfolio_snapshot()
            for lane in self.portfolio_runtime.active_lanes():
                if lane.executor is None:
                    continue
                bucket = lane.engine.profit_bucket
                if not (
                    bucket.pending_subscription_stage
                    or lane.engine.state.phase
                    == StrategyPhase.PROFIT_SWEEPING
                ):
                    continue
                events.extend(
                    self.portfolio_runtime.tag_events(
                        lane,
                        await lane.executor.reconcile_active_orders(),
                    )
                )

            if self.portfolio_runtime.pending_subscription_lane() is not None:
                subscription_events, _ = await (
                    self.portfolio_runtime.advance_account_subscription(
                        portfolio,
                        allow_new_capital=False,
                    )
                )
                events.extend(subscription_events)
            else:
                events.extend(
                    await self.portfolio_runtime.advance_harvest_subscription(
                        portfolio
                    )
                )

            release_events = await self._release_profit_sweeps()
            events.extend(release_events)
            events.extend(
                await self._continue_released_harvest_subscription(
                    portfolio,
                    release_events,
                )
            )

            self._last_error = None
            for lane in self.portfolio_runtime.lanes():
                lane.engine.state.last_error = None
                lane.engine.persist()
            await self._emit_events(events)

    async def _submit_released_restore(self, lane) -> List[dict]:
        if lane.executor is None:
            return []
        reference_price = lane.engine.state.last_mark_price
        return self.portfolio_runtime.tag_events(
            lane,
            await lane.executor.execute_pending_orders(
                reference_price=reference_price,
            ),
        )

    async def _release_profit_sweeps(self) -> List[dict]:
        events: List[dict] = []
        for lane in self.portfolio_runtime.active_lanes():
            sweep_events = lane.engine.finalize_profit_sweep()
            events.extend(
                self.portfolio_runtime.tag_events(lane, sweep_events)
            )
            if (
                sweep_events
                and lane.engine.state.phase == StrategyPhase.REBALANCING
                and lane.executor is not None
            ):
                events.extend(await self._submit_released_restore(lane))
        return events

    async def _continue_released_harvest_subscription(
        self,
        portfolio: PortfolioSnapshot,
        release_events: List[dict],
    ) -> List[dict]:
        if not any(
            event.get("action") == "profit_sweep_released"
            and event.get("reason") == "profit_cash_transferred_to_spot"
            for event in release_events
        ):
            return []
        events, _ = await self.portfolio_runtime.advance_account_subscription(
            portfolio,
            allow_new_capital=False,
        )
        return events

    async def _exit_loop(self) -> None:
        interval = max(
            0.5,
            float(CoinMarginedRWUSDStrategyConfig.EXIT_POLL_INTERVAL_SEC),
        )
        try:
            while self.portfolio_runtime.exit_worker_required():
                started = time.monotonic()
                async with self._current_operation_lock():
                    events = await (
                        self.portfolio_runtime.advance_customer_exit()
                    )
                    try:
                        await self._emit_events(events)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        print(
                            f"[RWUSD][{self.account_name}][warning] "
                            f"exit event callback failed: {exc}"
                        )
                now_ms = int(time.time() * 1000)
                retry_after_ms = (
                    self.portfolio_runtime.next_exit_retry_after_ms(now_ms)
                )
                retry_delay = max(
                    0.0,
                    (retry_after_ms - now_ms) / 1000.0,
                )
                delay = max(
                    0.1,
                    interval - (time.monotonic() - started),
                    retry_delay,
                )
                await asyncio.sleep(delay)
        except asyncio.CancelledError:
            raise
        finally:
            self._exit_task = None

    async def _run_cycle(self) -> None:
        async with self._current_operation_lock():
            await self._run_cycle_once()

    async def _run_cycle_once(self) -> None:
        events: List[dict] = []
        account_subscription_advanced = False
        rows, portfolio = await self._build_runtime_snapshot()
        if self.executor is not None:
            capital_events, _ = await (
                self.portfolio_runtime.advance_capital_sources(
                    allocation_market=self.allocation_market,
                    now_ms=int(time.time() * 1000),
                )
            )
            events.extend(capital_events)
            if (
                self.portfolio_runtime
                .official_capital_bootstrap_required()
            ):
                self._last_error = None
                self.engine.state.last_error = None
                self.engine.persist()
                await self._emit_events(events)
                return
            rows = await self._ensure_lane_market_rows(rows)
            if self._operational_prepared:
                for lane in self.portfolio_runtime.lanes():
                    if lane.executor is None:
                        continue
                    events.extend(
                        self.portfolio_runtime.tag_events(
                            lane,
                            await lane.executor.reconcile_active_orders(),
                        )
                    )
                try:
                    self._validate_execution_portfolio(
                        portfolio,
                        allow_unfunded_initial=True,
                    )
                except RuntimeError as exc:
                    for lane in self.portfolio_runtime.lanes():
                        if lane.executor is None:
                            continue
                        events.extend(
                            self.portfolio_runtime.tag_events(
                                lane,
                                await lane.executor.enforce_runtime_account_guard(
                                    reason=str(exc),
                                    reference_price=(
                                        lane.engine.state.last_mark_price
                                    ),
                                ),
                            )
                        )
                    self._last_error = str(exc)
                    self.engine.state.last_error = self._last_error
                    self.engine.persist()
                    await self._emit_events(events)
                    raise
            else:
                self._validate_execution_portfolio(
                    portfolio,
                    allow_unfunded_initial=True,
                )
            primary_lane = next(
                lane
                for lane in self.portfolio_runtime.lanes()
                if lane.primary
            )
            if primary_lane.executor is None:
                raise RuntimeError("RWUSD primary executor is unavailable")
            preflight_snapshots: Optional[RWUSDPreflightSnapshots] = None
            if (
                not primary_lane.engine.profit_bucket
                .initial_collateralization_completed
                or self.portfolio_runtime.preparation_required()
            ):
                preflight_snapshots = await (
                    self.portfolio_runtime.preflight_account_writes(
                        portfolio=portfolio
                    )
                )
            initial_ready, initial_events = await (
                primary_lane.executor.ensure_initial_collateralization(
                    portfolio,
                    requested_amount=(
                        self.portfolio_runtime
                        .initial_collateralization_request()
                    ),
                )
            )
            initial_events = self.portfolio_runtime.tag_events(
                primary_lane,
                initial_events,
            )
            events.extend(initial_events)
            account_subscription_advanced = bool(initial_events)
            self.portfolio_runtime.apply_capital_events(initial_events)
            if account_subscription_advanced:
                preflight_snapshots = None
            if not initial_ready:
                self._last_error = None
                self.engine.state.last_error = None
                self.engine.persist()
                await self._emit_events(events)
                return
            events.extend(
                self.portfolio_runtime.finalize_initial_capital_accounting()
            )
            events.extend(
                self.portfolio_runtime.inherit_initial_collateralization()
            )
            await primary_lane.executor.refresh_live_rwusd_observation()
            self.portfolio_runtime.ensure_lanes()
            rows = await self._ensure_lane_market_rows(rows)
            if self.portfolio_runtime.preparation_required():
                events.extend(
                    await self._prepare_operational_start(
                        rows,
                        portfolio,
                        preflight_snapshots=preflight_snapshots,
                    )
                )
            if self.portfolio_runtime.reserve_allocation_due():
                allocation_rows = await self.allocation_market.build()
                reserve_event = self.portfolio_runtime.allocate_reserve(
                    allocation_rows
                )
                if reserve_event is not None:
                    events.append(reserve_event)

            if not account_subscription_advanced:
                subscription_events, subscription_advanced = await (
                    self.portfolio_runtime.advance_account_subscription(
                        portfolio,
                        # Confirmed harvest must use the account-level
                        # subscription lane before new external capital.
                        allow_new_capital=False,
                    )
                )
                events.extend(subscription_events)
                account_subscription_advanced = subscription_advanced
                if subscription_advanced:
                    preflight_snapshots = None

            if not account_subscription_advanced:
                incremental_events, incremental_advanced = await (
                    self.portfolio_runtime.advance_incremental_capital(
                        portfolio,
                        allocation_market=self.allocation_market,
                        allow_new_capital=True,
                    )
                )
                events.extend(incremental_events)
                account_subscription_advanced = (
                    account_subscription_advanced
                    or incremental_advanced
                )
            self.portfolio_runtime.ensure_lanes()
            rows = await self._ensure_lane_market_rows(rows)
            if self.portfolio_runtime.preparation_required():
                events.extend(
                    await self._prepare_operational_start(
                        rows,
                        portfolio,
                        preflight_snapshots=preflight_snapshots,
                    )
                )
                portfolio = await self._build_portfolio_snapshot()
            self._validate_execution_portfolio(portfolio)
        elif not (
            self.engine.profit_bucket.initial_collateralization_completed
        ):
            if (
                self.engine.state.long_quantity > 1e-12
                or self.engine.state.short_quantity > 1e-12
            ):
                events.append(
                    self.engine.complete_initial_collateralization(
                        mode="local_existing_exposure_adopted"
                    )
                )
            else:
                amount = max(
                    0.0,
                    float(self.total_capital or 0),
                    float(
                        self.portfolio_runtime.coordinator
                        .ledger.total_capital
                    ),
                )
                minimum = float(
                    getattr(CoinMarginedRWUSDStrategyConfig, "MIN_SWEEP", 1.0)
                )
                if amount + 1e-12 < minimum:
                    self.engine.block_initial_collateralization(
                        "initial_rwusd_source_balance_below_minimum"
                    )
                    self.engine.persist()
                    await self._emit_events(events)
                    return
                events.append(
                    self.engine.complete_shadow_initial_collateralization(
                        amount=amount,
                        mode="local_shadow_direct_collateral",
                    )
                )
            self.portfolio_runtime.apply_capital_events(events)
            events.extend(
                self.portfolio_runtime.finalize_initial_capital_accounting()
            )
            events.extend(
                self.portfolio_runtime.inherit_initial_collateralization()
            )

        row_by_symbol = {
            str(row.get("symbol") or "").strip().upper(): row
            for row in rows
        }
        elapsed_hours = self._elapsed_hours()
        effective_now = datetime.now(timezone.utc)
        for lane in self.portfolio_runtime.active_lanes():
            row = row_by_symbol.get(lane.symbol)
            if row is None:
                lane.engine.state.last_error = "market_row_unavailable"
                lane.engine.persist()
                continue
            lane_rows = [row]
            reference_price = max(
                0.0,
                float(row.get("close", 0) or 0),
            )
            if lane.executor is not None:
                lane_rows = await lane.executor.enrich_market_rows(
                    lane_rows,
                    portfolio,
                )
                leverage_status, leverage_events = await (
                    lane.executor.refresh_symbol_leverage(
                        lane.symbol,
                        allow_upgrade=True,
                    )
                )
                events.extend(
                    self.portfolio_runtime.tag_events(
                        lane,
                        leverage_events,
                    )
                )
                await lane.executor.prepare_target_quantity(
                    symbol=lane.symbol,
                    reference_price=reference_price,
                    portfolio=portfolio,
                    maximum_notional=float(
                        leverage_status.get("max_notional_value", 0) or 0
                    ),
                    allow_target_expansion=(
                        lane.executor.capital_target_expansion_ready
                    ),
                    capital_budget=float(
                        self.portfolio_runtime.coordinator.symbol_capital(
                            lane.symbol
                        )
                    ),
                )
                events.extend(
                    self.portfolio_runtime.tag_events(
                        lane,
                        await lane.executor.reconcile_hedge_vector(
                            reference_price=reference_price,
                        ),
                    )
                )
            result = lane.engine.step(
                rows=lane_rows,
                portfolio=portfolio,
                now=effective_now,
                elapsed_hours=elapsed_hours,
            )
            events.extend(
                self.portfolio_runtime.tag_events(
                    lane,
                    result.events,
                )
            )
            if lane.executor is None:
                continue
            events.extend(
                self.portfolio_runtime.tag_events(
                    lane,
                    await lane.executor.advance_hard_risk_reduction(),
                )
            )
            events.extend(
                self.portfolio_runtime.tag_events(
                    lane,
                    await lane.executor.execute_pending_orders(
                        reference_price=lane.engine.state.last_mark_price,
                    ),
                )
            )
            if not self.portfolio_runtime.live_rwusd_execution:
                events.extend(
                    self.portfolio_runtime.tag_events(
                        lane,
                        await lane.executor.execute_live_subscription(
                            portfolio
                        ),
                    )
                )

        if (
            self.portfolio_runtime.live_rwusd_execution
            and not account_subscription_advanced
            and self.portfolio_runtime.pending_external_capital_subscription()
            <= 0
        ):
            events.extend(
                await self.portfolio_runtime.advance_harvest_subscription(
                    portfolio
                )
            )

        release_events = await self._release_profit_sweeps()
        events.extend(release_events)
        events.extend(
            await self._continue_released_harvest_subscription(
                portfolio,
                release_events,
            )
        )
        self.symbol = self.portfolio_runtime.primary_symbol
        self._last_error = None
        for lane in self.portfolio_runtime.lanes():
            lane.engine.state.last_error = None
            lane.engine.persist()
        await self._emit_events(events)

    async def _prepare_operational_start(
        self,
        rows: List[MarketRow],
        portfolio: PortfolioSnapshot,
        *,
        preflight_snapshots: Optional[
            RWUSDPreflightSnapshots
        ] = None,
    ) -> List[dict]:
        if self.executor is None:
            return []
        self.portfolio_runtime.pending_subscription_lane()
        self._validate_execution_portfolio(
            portfolio,
            allow_unfunded_initial=True,
        )
        events = await self.portfolio_runtime.prepare_lanes(
            rows=rows,
            portfolio=portfolio,
            preflight_snapshots=preflight_snapshots,
        )
        self._operational_prepared = not (
            self.portfolio_runtime.preparation_required()
        )
        self.symbol = self.portfolio_runtime.primary_symbol
        return events

    def _elapsed_hours(self) -> float:
        now = time.monotonic()
        previous = self._last_cycle_monotonic
        self._last_cycle_monotonic = now
        if previous <= 0:
            return 0.0
        return max(0.0, now - previous) / 3600.0

    async def _build_runtime_snapshot(
        self,
    ) -> Tuple[List[MarketRow], PortfolioSnapshot]:
        rows = await self._build_market_rows()
        if not rows:
            raise RuntimeError(
                "all RWUSD candidate market rows are unavailable"
            )
        portfolio = await self._build_portfolio_snapshot()
        if (
            self.executor is not None
            and not self.portfolio_runtime.primary_symbol
        ):
            rows = await self.executor.enrich_market_rows(rows, portfolio)
        return rows, portfolio

    async def _build_market_rows(self) -> List[MarketRow]:
        return await self.market_runtime.build_rows(
            self._runtime_market_symbols()
        )

    def _runtime_market_symbols(self) -> List[str]:
        symbols = self.portfolio_runtime.market_symbols()
        return symbols or list(CoinMarginedRWUSDStrategyConfig.CANDIDATE_SYMBOLS)

    async def _ensure_lane_market_rows(
        self,
        rows: List[MarketRow],
    ) -> List[MarketRow]:
        existing = {
            str(row.get("symbol") or "").strip().upper()
            for row in rows
        }
        missing = [
            symbol
            for symbol in self.portfolio_runtime.market_symbols()
            if symbol not in existing
        ]
        if not missing:
            return rows
        return rows + await self.market_runtime.build_rows(missing)

    async def _build_portfolio_snapshot(self) -> PortfolioSnapshot:
        fallback_equity = max(
            self.total_capital,
            float(self.portfolio_runtime.coordinator.ledger.total_capital),
            self.engine.initial_capital,
            1.0,
        )
        try:
            account_info, positions = await asyncio.gather(
                self.client.get_account_info(),
                self.client.get_portfolio_positions(),
            )
            equity = float((account_info or {}).get("equity", 0) or 0)
            available = float((account_info or {}).get("available", 0) or 0)
            available_known = parse_bool(
                (account_info or {}).get(
                    "available_balance_known",
                    "available" in (account_info or {}),
                )
            )
            withdrawable = float(
                (account_info or {}).get("withdrawable", available) or 0
            )
            raw = (account_info or {}).get("raw") or {}
            total_margin_balance = float(
                (raw or {}).get("totalMarginBalance", equity) or equity
            )
            total_maint_margin = float(
                (raw or {}).get("totalMaintMargin", 0) or 0
            )
            direct_unimmr = float(
                (account_info or {}).get("margin_health")
                or (account_info or {}).get("uni_mmr")
                or (raw or {}).get("uniMMR")
                or 0
            )
            account_status = str(
                (account_info or {}).get("account_status")
                or ("NORMAL" if self.is_testnet else "UNKNOWN")
            ).upper()
            estimated_liquidation_risk = parse_bool(
                (account_info or {}).get(
                    "estimated_liquidation_risk",
                    False,
                )
            )
            liquidation_risk_reason = (
                str(
                    (account_info or {}).get(
                        "liquidation_risk_reason"
                    )
                    or ""
                )
                or None
            )
            if account_status in {
                "MARGIN_CALL",
                "LIQUIDATION",
                "LIQUIDATING",
                "FORCED_LIQUIDATION",
            }:
                estimated_liquidation_risk = True
                liquidation_risk_reason = (
                    liquidation_risk_reason
                    or f"accountStatus={account_status}"
                )
            if self.executor is not None:
                uni_mmr = direct_unimmr
            else:
                uni_mmr = (
                    direct_unimmr
                    if direct_unimmr > 0
                    else (
                        total_margin_balance / total_maint_margin
                        if total_maint_margin > 0
                        else 999999.0
                    )
                )
            by_symbol: Dict[str, float] = {}
            total_abs = 0.0
            for position in positions or []:
                symbol = str(position.get("symbol") or "")
                size = abs(float(position.get("size", 0) or 0))
                entry = float(position.get("entry_price", 0) or 0)
                notional = abs(
                    float(position.get("notional", 0) or 0)
                ) or size * entry
                signed = notional if str(position.get("side")) == "long" else -notional
                total_abs += abs(notional)
                by_symbol[symbol] = by_symbol.get(symbol, 0.0) + signed

            if self.executor is None:
                lanes = self.portfolio_runtime.lanes() or []
                engines = (
                    [lane.engine for lane in lanes]
                    if lanes
                    else [self.engine]
                )
                for engine in engines:
                    mark = max(0.0, engine.state.last_mark_price)
                    simulated_long = engine.state.long_quantity * mark
                    simulated_short = engine.state.short_quantity * mark
                    total_abs += simulated_long + simulated_short
                    simulated_net = simulated_long - simulated_short
                    if engine.state.selected_symbol:
                        symbol = engine.state.selected_symbol
                        by_symbol[symbol] = (
                            by_symbol.get(symbol, 0.0) + simulated_net
                        )

            return PortfolioSnapshot(
                account_equity=(
                    equity
                    if self.executor is not None or equity > 0
                    else fallback_equity
                ),
                available_balance=(
                    available
                    if self.executor is not None or available >= 0
                    else fallback_equity
                ),
                available_balance_known=available_known,
                uni_mmr=uni_mmr,
                withdrawable_balance=(
                    withdrawable
                    if self.executor is not None
                    else available
                ),
                withdrawable_official=bool(
                    (account_info or {}).get(
                        "withdrawable_official",
                        False,
                    )
                ),
                total_abs_notional=total_abs,
                total_net_notional=sum(by_symbol.values()),
                single_symbol_net_notional=max(
                    (abs(value) for value in by_symbol.values()),
                    default=0.0,
                ),
                source=(
                    str((account_info or {}).get("source") or "")
                    if self.executor is not None
                    else "binance_read_only"
                )
                or (
                    "binance_futures_demo"
                    if self.is_testnet
                    else "binance_portfolio_margin_live"
                ),
                account_type=str(
                    (account_info or {}).get("account_type")
                    or (
                        "futures_demo"
                        if self.is_testnet
                        else "unknown"
                    )
                ),
                account_status=account_status,
                margin_health_source=str(
                    (account_info or {}).get("margin_health_source")
                    or "futures_total_margin_balance_over_maintenance_margin"
                ),
                official_uni_mmr=bool(
                    (account_info or {}).get("official_uni_mmr", False)
                ),
                account_maint_margin=max(
                    0.0,
                    float(
                        (account_info or {}).get("account_maint_margin")
                        or (raw or {}).get("accountMaintMargin")
                        or (raw or {}).get("totalMaintMargin")
                        or 0
                    ),
                ),
                estimated_liquidation_risk=(
                    estimated_liquidation_risk
                ),
                liquidation_risk_reason=liquidation_risk_reason,
            )
        except Exception as exc:
            if self.executor is not None:
                raise RuntimeError(
                    f"Binance {self.execution_mode} account snapshot "
                    f"unavailable: {exc}"
                ) from exc
            self._last_error = f"account snapshot fallback: {exc}"
            return PortfolioSnapshot(
                account_equity=fallback_equity,
                available_balance=fallback_equity,
                available_balance_known=False,
                uni_mmr=999999.0,
                withdrawable_balance=fallback_equity,
                withdrawable_official=False,
                source="runtime_capital_fallback",
                account_type="fallback",
                account_status="UNKNOWN",
                margin_health_source="runtime_capital_fallback",
                official_uni_mmr=False,
            )

    def _validate_execution_portfolio(
        self,
        portfolio: PortfolioSnapshot,
        *,
        allow_unfunded_initial: bool = False,
    ) -> None:
        if self.executor is None:
            return
        has_pending_initial_subscription = bool(
            self.engine.profit_bucket.pending_subscription_stage
            and (
                self.engine.profit_bucket.pending_subscription_kind
                in {None, "initial"}
            )
            and not (
                self.engine.profit_bucket.initial_collateralization_completed
            )
        )
        initial_funding_not_started = bool(
            allow_unfunded_initial
            and not (
                self.engine.profit_bucket.initial_collateralization_completed
            )
            and self.engine.state.long_quantity <= 1e-12
            and self.engine.state.short_quantity <= 1e-12
        )
        if (
            portfolio.account_equity <= 0
            and not has_pending_initial_subscription
            and not initial_funding_not_started
        ):
            raise RuntimeError("Binance RWUSD account equity is zero")
        if not self.is_testnet and not portfolio.official_uni_mmr:
            raise RuntimeError(
                "Binance RWUSD mainnet requires official Portfolio Margin uniMMR"
            )
        if (
            not self.is_testnet
            and str(portfolio.account_status or "").upper() != "NORMAL"
        ):
            raise RuntimeError(
                "Binance RWUSD mainnet requires accountStatus=NORMAL, got "
                f"{portfolio.account_status or 'MISSING'}"
            )

    async def _emit_events(self, events: List[dict]) -> None:
        for event in events:
            payload = dict(event)
            event_symbol = str(
                payload.get("symbol")
                or self.engine.state.selected_symbol
                or self.symbol
                or ""
            ).strip().upper()
            payload.update(
                {
                    "account_name": self.account_name,
                    "strategy_type": self.strategy_type,
                    "symbol": event_symbol,
                    "mode": self.execution_mode,
                    "execution_enabled": self.executor is not None,
                    "mainnet_execution_enabled": (
                        self.executor is not None and not self.is_testnet
                    ),
                    "rwusd_transfers_simulated": self.is_testnet,
                    "rwusd_shadow": self.is_testnet,
                    "rwusd_testnet_supported": False,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )
            if (
                self.executor is not None
                and payload.get("type") != "simulated_transfer"
            ):
                payload["simulated"] = False
                payload["testnet"] = self.is_testnet
            if payload.get("type") in {
                "simulated_fill",
                "simulated_harvest",
                "simulated_transfer",
                "exchange_fill",
                "rwusd_subscription",
                "rwusd_transfer",
            }:
                if self.on_trade:
                    await self.on_trade(self.account_name, payload)
            elif self.on_signal:
                await self.on_signal(self.account_name, payload)

    async def _emit_signal(self, action: str, message: str) -> None:
        if not self.on_signal:
            return
        await self.on_signal(
            self.account_name,
            {
                "type": "rwusd_status",
                "action": action,
                "message": message,
                "account_name": self.account_name,
                "strategy_type": self.strategy_type,
                "symbol": self.engine.state.selected_symbol or self.symbol,
                "mode": self.execution_mode,
                "execution_enabled": self.executor is not None,
                "mainnet_execution_enabled": (
                    self.executor is not None and not self.is_testnet
                ),
                "rwusd_transfers_simulated": self.is_testnet,
                "rwusd_shadow": self.is_testnet,
                "rwusd_testnet_supported": False,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )

    async def _emit_error(self, message: str) -> None:
        print(f"[RWUSD][{self.account_name}][error] {message}")
        if self.on_error:
            await self.on_error(self.account_name, message)

    def get_status(self, current_price: float = 0.0) -> dict:
        payload = self.engine.get_status()
        account_profit_bucket = (
            self.portfolio_runtime.account_profit_bucket()
        )
        payload["profit_bucket"] = account_profit_bucket
        selected_symbol = self.engine.state.selected_symbol or self.symbol
        leverage_control = (
            self.executor.leverage_status(selected_symbol)
            if self.executor is not None and selected_symbol
            else {
                "symbol": selected_symbol,
                "target_leverage": self.target_leverage,
                "exchange_leverage": self.target_leverage,
                "sizing_leverage": self.target_leverage,
                "status": "local",
            }
        )
        payload.update(
            {
                "account_name": self.account_name,
                "strategy_type": self.strategy_type,
                "is_running": bool(self.is_running),
                "exit_worker_running": bool(
                    self._exit_task is not None
                    and not self._exit_task.done()
                ),
                "symbol": self.engine.state.selected_symbol or self.symbol,
                "price": (
                    self.engine.state.last_mark_price
                    or float(current_price or 0)
                ),
                "total_capital": float(
                    self.portfolio_runtime.coordinator.ledger.total_capital
                    or self.total_capital
                ),
                "position_margin_percent": self.position_margin_percent,
                "per_side_margin_percent": (
                    self.position_margin_percent / 2.0
                ),
                "leverage": int(
                    leverage_control.get(
                        "sizing_leverage",
                        self.target_leverage,
                    )
                ),
                "target_leverage": self.target_leverage,
                "leverage_control": leverage_control,
                "yield_projection": build_rwusd_yield_projection(
                    profit_bucket=account_profit_bucket,
                    leverage_control=leverage_control,
                    margin_fraction=self.position_margin_percent,
                    target_leverage=self.target_leverage,
                    mainnet_execution=(
                        self.executor is not None and not self.is_testnet
                    ),
                    fallback_capital=self.total_capital,
                    shadow_apr=CoinMarginedRWUSDStrategyConfig.RWUSD_APR,
                ),
                "is_testnet": self.is_testnet,
                "testnet_orders_enabled": (
                    self.executor is not None and self.is_testnet
                ),
                "mainnet_orders_enabled": (
                    self.executor is not None and not self.is_testnet
                ),
                "market_data": {
                    "poll_interval_sec": float(
                        CoinMarginedRWUSDStrategyConfig.POLL_INTERVAL_SEC
                    ),
                    "selector_input_refresh_sec": float(
                        CoinMarginedRWUSDStrategyConfig.SELECTOR_INPUT_REFRESH_SEC
                    ),
                    "selector_input_max_stale_sec": float(
                        CoinMarginedRWUSDStrategyConfig.SELECTOR_INPUT_MAX_STALE_SEC
                    ),
                    "selector_input_cache_age_sec": (
                        self.market_runtime.cache_ages()
                    ),
                },
                "last_error": self._last_error or payload.get("last_error"),
                "portfolio_runtime": self.portfolio_runtime.status(),
            }
        )
        payload["exit"] = self.portfolio_runtime.exit_status()
        return payload

    async def begin_customer_exit(self) -> dict:
        async with self._current_control_lock():
            exit_status = self.portfolio_runtime.exit_status()
            if exit_status.get("reopen_locked"):
                if self.portfolio_runtime.exit_worker_required():
                    self._start_exit_worker()
                return {
                    "success": True,
                    "message": "RWUSD customer exit is already locked",
                    "exit": exit_status,
                }
            self.is_running = False
            task = self._task
            self._task = None
            if task is not None:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            events = self.portfolio_runtime.request_customer_exit(
                int(time.time() * 1000)
            )
            await self._emit_events(events)
            if self.executor is None:
                for lane in self.portfolio_runtime.lanes():
                    result = lane.engine.manual_flatten(
                        lane.engine.state.last_mark_price
                    )
                    if not result.get("success"):
                        return result
                    lane.engine.exit_state.mark_flat(
                        int(time.time() * 1000)
                    )
                    lane.engine.state.last_action = (
                        "customer_exit_complete"
                    )
                    lane.engine.persist()
            else:
                self._start_exit_worker()
            return {
                "success": True,
                "message": (
                    "RWUSD staged liquidation started; reopening is locked"
                ),
                "exit": self.portfolio_runtime.exit_status(),
            }

    async def request_customer_redemption(self) -> dict:
        async with self._current_control_lock():
            if self.executor is None or self.is_testnet:
                return {
                    "success": False,
                    "message": (
                        "RWUSD redemption is available only for mainnet "
                        "Portfolio Margin accounts"
                    ),
                }
            if self.engine.exit_state.stage in {
                RWUSDExitStage.REDEEMING,
                RWUSDExitStage.REDEEM_PENDING,
            }:
                self._start_exit_worker()
                return {
                    "success": True,
                    "message": "RWUSD redemption is already in progress",
                    "exit": self.portfolio_runtime.exit_status(),
                }
            event = self.portfolio_runtime.request_customer_redemption(
                int(time.time() * 1000)
            )
            await self._emit_events([event])
            self._start_exit_worker()
            return {
                "success": True,
                "message": "RWUSD redemption started",
                "exit": self.portfolio_runtime.exit_status(),
            }

    async def resume_pending_exit(self) -> bool:
        async with self._current_control_lock():
            if not self.portfolio_runtime.exit_worker_required():
                return False
            self.is_running = False
            self._start_exit_worker()
            return True

    def _start_exit_worker(self) -> None:
        if (
            self._exit_task is not None
            and not self._exit_task.done()
        ):
            return
        self._exit_task = asyncio.create_task(self._exit_loop())

    async def manual_close_all(self, current_price: float = 0.0) -> dict:
        async with self._current_operation_lock():
            if self.executor is not None:
                events: List[dict] = []
                results = []
                for lane in self.portfolio_runtime.active_lanes():
                    if lane.executor is None:
                        continue
                    result = await lane.executor.manual_flatten(
                        current_price
                        or lane.engine.state.last_mark_price
                    )
                    lane_events = self.portfolio_runtime.tag_events(
                        lane,
                        list(result.get("events") or []),
                    )
                    results.append({**result, "events": lane_events})
                    events.extend(lane_events)
                await self._emit_events(events)
                success = all(
                    result.get("success") for result in results
                )
                if success:
                    await self._emit_signal(
                        "manual_exchange_flatten",
                        "RWUSD exchange portfolio flattened",
                    )
                return {
                    "success": success,
                    "message": (
                        "RWUSD exchange portfolio flattened"
                        if success
                        else "RWUSD exchange portfolio flatten is pending"
                    ),
                    "simulated": False,
                    "events": events,
                    "results": results,
                }
            results = [
                lane.engine.manual_flatten(
                    current_price or lane.engine.state.last_mark_price
                )
                for lane in self.portfolio_runtime.active_lanes()
            ]
            success = all(
                result.get("success") for result in results
            )
            if success:
                await self._emit_signal(
                    "manual_simulated_flatten",
                    "RWUSD dry-run portfolio flattened",
                )
            return {
                "success": success,
                "message": (
                    "RWUSD dry-run portfolio flattened"
                    if success
                    else "RWUSD dry-run portfolio flatten failed"
                ),
                "simulated": True,
                "results": results,
            }
