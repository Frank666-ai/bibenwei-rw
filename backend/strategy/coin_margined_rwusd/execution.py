"""Binance exchange execution for the RWUSD state machine."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import math
import time
from dataclasses import dataclass, replace
from typing import Callable, Dict, List, Optional, Set, Tuple

from backend.strategy.coin_margined_rwusd.engine import (
    HARD_RISK_ALLOWED_PURPOSES,
    HEDGE_REPAIR_PAIR_PURPOSE,
    RWUSDEngine,
)
from backend.strategy.coin_margined_rwusd.exit_execution import RWUSDExitExecution
from backend.strategy.coin_margined_rwusd.leverage import (
    BinanceUMLeverageManager,
    LeverageSnapshot,
)
from backend.strategy.coin_margined_rwusd.margin import BinanceUMMarginModel
from backend.strategy.coin_margined_rwusd.models import (
    PortfolioSnapshot,
    StrategyPhase,
    parse_bool,
)
from backend.strategy.coin_margined_rwusd.preflight import (
    RWUSDAccountPreflightSnapshot,
    RWUSDMainnetPreflightSnapshot,
    RWUSDOperationalPreflight,
    load_rwusd_preflight_snapshots,
)
from backend.strategy.coin_margined_rwusd.runtime import RWUSDOwnershipSnapshot


EMERGENCY_MARKET_PURPOSES = {
    "manual_flatten",
    "customer_exit_residual",
    "hard_risk_reduction",
}
CUSTOMER_EXIT_LIMIT_PURPOSES = {"customer_exit"}
CUSTOMER_EXIT_PURPOSES = (
    CUSTOMER_EXIT_LIMIT_PURPOSES | {"customer_exit_residual"}
)
PAIR_BUILD_PURPOSES = {
    "open_hedge",
    "top_up_hedge",
}
SYNCHRONIZED_PAIR_PURPOSES = PAIR_BUILD_PURPOSES | {
    HEDGE_REPAIR_PAIR_PURPOSE,
}
VECTOR_REPAIR_PURPOSES = {"hedge_repair"}
HARVEST_ORDER_PURPOSES = {"take_profit", "restore"}
RWUSD_MARGIN_LOCKED_BLOCK_REASON = "rwusd_source_margin_locked"
LEGACY_AUTOMATIC_REDUCTION_PURPOSES = {
    "risk_reduction",
    "target_reduction",
    "hedge_repair_reduce",
}
MANAGED_VECTOR_PURPOSES = (
    SYNCHRONIZED_PAIR_PURPOSES | VECTOR_REPAIR_PURPOSES
)
SUPPORTED_RWUSD_SOURCE_ASSETS = frozenset({"USDC", "USDT"})
USD_COMMISSION_ASSETS = frozenset(
    {"USDC", "USDT", "FDUSD", "TUSD", "USDP", "BUSD", "U"}
)
SOURCE_TRANSFER_WAIT_STAGES = frozenset(
    {
        "TRANSFER_PENDING",
        "TRANSFER_UNCERTAIN",
        "FUNDING_TRANSFER_PENDING",
        "FUNDING_TRANSFER_UNCERTAIN",
    }
)
SOURCE_COLLECTION_WAIT_STAGES = frozenset(
    {
        "SOURCE_COLLECTION_PENDING",
        "SOURCE_COLLECTION_UNCERTAIN",
    }
)
CONVERSION_WAIT_STAGES = frozenset(
    {
        "CONVERT_ACCEPT_PENDING",
        "CONVERT_ACCEPT_UNCERTAIN",
        "CONVERT_PROCESSING",
    }
)
SUBSCRIPTION_WAIT_STAGES = frozenset(
    {"SUBSCRIBE_PENDING", "SUBSCRIBE_UNCERTAIN"}
)
COLLATERAL_CONFIRMATION_STAGES = frozenset(
    {
        "COLLATERAL_TRANSFER_PENDING",
        "COLLATERAL_TRANSFER_UNCERTAIN",
        "COLLATERAL_TRANSFERRED",
    }
)
EXISTING_RWUSD_CONFIRMATION_STAGES = frozenset(
    {
        "EXISTING_RWUSD_COLLATERAL_TRANSFER_PENDING",
        "EXISTING_RWUSD_COLLATERAL_TRANSFER_UNCERTAIN",
        "EXISTING_RWUSD_COLLATERAL_TRANSFERRED",
    }
)
LIVE_SUBSCRIPTION_WAIT_STAGES = (
    SOURCE_COLLECTION_WAIT_STAGES
    | SOURCE_TRANSFER_WAIT_STAGES
    | CONVERSION_WAIT_STAGES
    | SUBSCRIPTION_WAIT_STAGES
    | COLLATERAL_CONFIRMATION_STAGES
    | EXISTING_RWUSD_CONFIRMATION_STAGES
)


@dataclass(frozen=True)
class _SubscriptionSourceChoice:
    asset: Optional[str]
    amount: float
    reason: str
    balances: Dict[str, float]
    eligible_assets: Tuple[str, ...]
    minimum: float
    source_asset: Optional[str] = None
    source_amount: float = 0.0
    source_collection_required: bool = False
    source_wallet: str = ""
    source_transfer_amount: float = 0.0


class BinanceRWUSDExecutor:
    """Run one order lifecycle across isolated Testnet and Mainnet clients."""

    def __init__(
        self,
        *,
        account_name: str,
        client: object,
        engine: RWUSDEngine,
        config: object,
        total_margin_fraction: float = 0.80,
        leverage: int = 50,
        ownership_provider: Optional[
            Callable[[], RWUSDOwnershipSnapshot]
        ] = None,
        capital_budget: Optional[float] = None,
    ) -> None:
        self.account_name = account_name
        self.client = client
        self.engine = engine
        self.config = config
        self.is_testnet = bool(getattr(client, "is_testnet", False))
        self.live_rwusd = not self.is_testnet
        self.total_margin_fraction = float(total_margin_fraction)
        self.target_leverage = int(leverage)
        self._ownership_provider = ownership_provider
        self._capital_budget_enabled = capital_budget is not None
        self._capital_budget = max(
            0.0,
            float(capital_budget or 0),
        )
        if not 0 < self.total_margin_fraction <= 0.80:
            raise ValueError(
                "RWUSD total margin fraction must be within (0, 0.80]"
            )
        if not 1 <= self.target_leverage <= 50:
            raise ValueError("RWUSD leverage must be within [1, 50]")
        self.leverage = (
            self.target_leverage
            if self.is_testnet
            else min(self.target_leverage, 20)
        )
        self._account_tag = hashlib.sha256(
            str(account_name or "rwusd").encode("utf-8")
        ).hexdigest()[:8]
        self._max_notional_by_symbol: Dict[str, float] = {}
        self._margin_model = BinanceUMMarginModel(
            client=self.client,
            leverage=self.leverage,
            refresh_sec=float(
                getattr(
                    self.config,
                    "RISK_PROFILE_REFRESH_SEC",
                    15 * 60.0,
                )
            ),
            minimum_capacity_ratio=float(
                getattr(
                    self.config,
                    "MIN_SYMBOL_CAPACITY_RATIO",
                    0.90,
                )
            ),
            soft_unimmr=float(
                getattr(self.config, "SOFT_UNIMMR", 4.5)
            ),
            projected_unimmr_buffer=float(
                getattr(
                    self.config,
                    "PROJECTED_UNIMMR_BUFFER",
                    0.10,
                )
            ),
        )
        self._leverage_manager = BinanceUMLeverageManager(
            client=self.client,
            target_leverage=self.target_leverage,
            initial_leverage=self.leverage,
            refresh_sec=float(
                getattr(
                    self.config,
                    "LEVERAGE_STATUS_REFRESH_SEC",
                    15 * 60.0,
                )
            ),
            retry_sec=float(
                getattr(
                    self.config,
                    "LEVERAGE_UPGRADE_RETRY_SEC",
                    60 * 60.0,
                )
            ),
        )
        self._source_assets = self._configured_source_assets()
        self._last_capital_scan_monotonic: Optional[float] = None
        self._last_rwusd_account_refresh_monotonic: Optional[float] = None
        self._last_rwusd_rate_refresh_monotonic: Optional[float] = None
        self._capital_target_expansion_ready = not self.live_rwusd
        self._conversion_limits_by_pair: Dict[
            Tuple[str, str],
            Tuple[float, float, float, float],
        ] = {}
        self._exit_execution = RWUSDExitExecution(
            owner=self,
            client=self.client,
            engine=self.engine,
            config=self.config,
        )

    @property
    def capital_target_expansion_ready(self) -> bool:
        return (
            self._capital_budget_enabled
            or self._capital_target_expansion_ready
        )

    def reset_for_new_cycle(self) -> None:
        self._max_notional_by_symbol.clear()
        self._margin_model.invalidate()
        self._leverage_manager.invalidate()
        self._last_capital_scan_monotonic = None
        self._last_rwusd_account_refresh_monotonic = None
        self._last_rwusd_rate_refresh_monotonic = None
        self._capital_target_expansion_ready = not self.live_rwusd
        self._conversion_limits_by_pair.clear()

    def set_ownership_provider(
        self,
        provider: Callable[[], RWUSDOwnershipSnapshot],
    ) -> None:
        self._ownership_provider = provider

    def set_capital_budget(self, amount: float) -> None:
        value = max(0.0, float(amount or 0))
        self._capital_budget_enabled = True
        if math.isclose(
            self._capital_budget,
            value,
            rel_tol=0.0,
            abs_tol=1e-8,
        ):
            return
        self._capital_budget = value

    def leverage_status(self, symbol: str) -> dict:
        return self._leverage_manager.snapshot(
            symbol,
            fallback_leverage=self.leverage,
        ).to_dict()

    def _apply_leverage_snapshot(
        self,
        snapshot: LeverageSnapshot,
    ) -> None:
        if snapshot.sizing_leverage != self.leverage:
            self.leverage = snapshot.sizing_leverage
            self.engine.set_leverage(self.leverage)
            self._margin_model.set_leverage(self.leverage)
            self._max_notional_by_symbol.clear()
        if snapshot.max_notional_value > 0:
            self._max_notional_by_symbol[snapshot.symbol] = (
                snapshot.max_notional_value
            )
            self._margin_model.update_max_notional(
                snapshot.symbol,
                snapshot.max_notional_value,
            )

    async def refresh_symbol_leverage(
        self,
        symbol: str,
        *,
        allow_upgrade: bool,
        force: bool = False,
    ) -> Tuple[dict, List[dict]]:
        snapshot, event = await self._leverage_manager.sync(
            symbol,
            allow_upgrade=allow_upgrade,
            force=force,
        )
        self._apply_leverage_snapshot(snapshot)
        return snapshot.to_dict(), [event] if event else []

    async def refresh_live_rwusd_observation(
        self,
        *,
        force: bool = False,
    ) -> None:
        if not self.live_rwusd:
            return
        now = time.monotonic()
        account_interval = max(
            60.0,
            float(
                getattr(
                    self.config,
                    "RWUSD_ACCOUNT_REFRESH_SEC",
                    5 * 60.0,
                )
            ),
        )
        rate_interval = max(
            account_interval,
            float(
                getattr(
                    self.config,
                    "RWUSD_RATE_REFRESH_SEC",
                    60 * 60.0,
                )
            ),
        )
        account_due = bool(
            force
            or self._last_rwusd_account_refresh_monotonic is None
            or now - self._last_rwusd_account_refresh_monotonic
            >= account_interval
        )
        rate_due = bool(
            force
            or self._last_rwusd_rate_refresh_monotonic is None
            or now - self._last_rwusd_rate_refresh_monotonic
            >= rate_interval
        )
        requests = []
        request_kinds: List[str] = []
        if account_due:
            self._last_rwusd_account_refresh_monotonic = now
            requests.append(self.client.get_rwusd_account())
            request_kinds.append("account")
            requests.append(self.client.get_rwusd_collateral_balance())
            request_kinds.append("collateral")
            requests.append(
                self.client.get_rwusd_transferable_balance()
            )
            request_kinds.append("transferable")
        if rate_due:
            self._last_rwusd_rate_refresh_monotonic = now
            requests.append(
                self.client.get_rwusd_rate_history(current=1, size=1)
            )
            request_kinds.append("rate")
        if not requests:
            return
        results = await asyncio.gather(
            *requests,
            return_exceptions=True,
        )
        for kind, result in zip(request_kinds, results):
            if isinstance(result, Exception):
                if kind == "transferable":
                    self.engine.update_exchange_rwusd_transferable_snapshot(
                        None
                    )
                continue
            try:
                if kind == "account":
                    self.engine.update_exchange_rwusd_snapshot(result)
                elif kind == "collateral":
                    self.engine.update_exchange_rwusd_collateral_snapshot(
                        result
                    )
                elif kind == "transferable":
                    self.engine.update_exchange_rwusd_transferable_snapshot(
                        result
                    )
                else:
                    self.engine.update_exchange_rwusd_rate_snapshot(result)
            except (TypeError, ValueError):
                if kind == "transferable":
                    self.engine.update_exchange_rwusd_transferable_snapshot(
                        None
                    )
                continue

    async def inspect_read_only(
        self,
        *,
        symbol: str,
        portfolio: PortfolioSnapshot,
        account_preflight: Optional[
            RWUSDAccountPreflightSnapshot
        ] = None,
        mainnet_preflight: Optional[
            RWUSDMainnetPreflightSnapshot
        ] = None,
    ) -> List[dict]:
        target_symbol = str(symbol or "").strip().upper()
        self._validate_operational_portfolio(
            portfolio,
            allow_unfunded_initial=True,
        )
        if account_preflight is None:
            account_snapshot, loaded_mainnet = (
                await load_rwusd_preflight_snapshots(
                    self.client,
                    include_mainnet=(
                        self.live_rwusd and mainnet_preflight is None
                    ),
                )
            )
        else:
            account_snapshot = account_preflight
            loaded_mainnet = None
        if mainnet_preflight is None:
            mainnet_preflight = loaded_mainnet
        self._validate_runtime_isolation_snapshot(
            account_snapshot,
            expected_symbol=target_symbol,
        )
        commission = await self.client.get_um_commission_rate(target_symbol)
        raw_commission = commission.get("raw") or {}
        if "makerCommissionRate" not in raw_commission:
            raise RuntimeError("RWUSD maker commission rate is unavailable")
        maker_bps = (
            float(commission.get("maker_commission_rate", 0) or 0)
            * 10000.0
        )
        self.engine.set_execution_cost_bps(maker_bps)
        collateral: Dict[str, object] = {}
        quota: Dict[str, object] = {}
        transferable: Dict[str, object] = {}
        if self.live_rwusd:
            snapshot = (
                mainnet_preflight
                or await RWUSDMainnetPreflightSnapshot.load(self.client)
            )
            snapshot.validate_read_only()
            quota = snapshot.quota
            collateral = snapshot.collateral
            transferable = snapshot.transferable
            self.engine.update_exchange_rwusd_snapshot(
                snapshot.rwusd_account
            )
            self.engine.update_exchange_rwusd_collateral_snapshot(collateral)
            self.engine.update_exchange_rwusd_transferable_snapshot(
                transferable
            )
        leverage_status, _ = await self.refresh_symbol_leverage(
            target_symbol,
            allow_upgrade=False,
            force=True,
        )
        return [
            {
                "type": "exchange_preflight",
                "action": "read_only",
                "symbol": target_symbol,
                "hedge_mode": account_snapshot.hedge_mode,
                "position_count": sum(
                    1
                    for position in account_snapshot.positions
                    if abs(float(position.get("size", 0) or 0)) > 1e-12
                ),
                "open_order_count": len(account_snapshot.open_orders),
                "portfolio_margin": self.live_rwusd,
                "futures_demo": self.is_testnet,
                "account_type": portfolio.account_type,
                "account_status": portfolio.account_status,
                "official_uni_mmr": portfolio.official_uni_mmr,
                "rwusd_subscription_enabled": (
                    quota.get("subscribeEnable") is not False
                    if self.live_rwusd
                    else True
                ),
                "rwusd_collateral_balance": (
                    self._collateral_confirmation_balance(collateral)
                    if self.live_rwusd
                    else 0.0
                ),
                "rwusd_transferable_balance": (
                    float(transferable.get("amount", 0) or 0)
                    if self.live_rwusd
                    else 0.0
                ),
                "api_key_permissions": (
                    snapshot.permission_status()
                    if self.live_rwusd
                    else {}
                ),
                "api_trading_locked": (
                    snapshot.trading_locked()
                    if self.live_rwusd
                    else False
                ),
                "leverage_control": leverage_status,
                "exchange_writes_performed": False,
                "simulated": False,
                "testnet": self.is_testnet,
            }
        ]

    async def prepare(
        self,
        *,
        symbol: str,
        reference_price: float,
        portfolio: PortfolioSnapshot,
        reference_prices: Optional[Dict[str, float]] = None,
    ) -> List[dict]:
        account_preflight, mainnet_preflight = (
            await load_rwusd_preflight_snapshots(
                self.client,
                include_mainnet=self.live_rwusd,
            )
        )
        self.validate_preparation(
            symbol=symbol,
            portfolio=portfolio,
            account_preflight=account_preflight,
        )
        if mainnet_preflight is not None:
            mainnet_preflight.validate_security()
        account_preflight = await self.ensure_hedge_mode(
            account_preflight
        )
        operational = await self.build_operational_preflight(
            mainnet_preflight=mainnet_preflight,
        )
        return await self.prepare_symbol(
            symbol=symbol,
            reference_price=reference_price,
            portfolio=portfolio,
            reference_prices=reference_prices,
            account_preflight=account_preflight,
            operational_preflight=operational,
        )

    async def build_operational_preflight(
        self,
        *,
        mainnet_preflight: Optional[RWUSDMainnetPreflightSnapshot],
    ) -> RWUSDOperationalPreflight:
        if not self.live_rwusd:
            return RWUSDOperationalPreflight({}, {}, (), (), ())
        if mainnet_preflight is None:
            raise RuntimeError("RWUSD mainnet preflight snapshot is unavailable")
        return await self._apply_mainnet_operational_preflight(
            mainnet_preflight
        )

    def validate_preparation(
        self,
        *,
        symbol: str,
        portfolio: PortfolioSnapshot,
        account_preflight: RWUSDAccountPreflightSnapshot,
    ) -> None:
        self._validate_operational_portfolio(
            portfolio,
            allow_unfunded_initial=True,
        )
        target_symbol = str(
            self.engine.state.selected_symbol or symbol or ""
        ).strip().upper()
        self._validate_runtime_isolation_snapshot(
            account_preflight,
            expected_symbol=target_symbol,
        )

    async def prepare_symbol(
        self,
        *,
        symbol: str,
        reference_price: float,
        portfolio: PortfolioSnapshot,
        reference_prices: Optional[Dict[str, float]],
        account_preflight: RWUSDAccountPreflightSnapshot,
        operational_preflight: RWUSDOperationalPreflight,
    ) -> List[dict]:
        target_symbol = str(
            self.engine.state.selected_symbol or symbol or ""
        ).strip().upper()
        events = list(operational_preflight.events)
        commission: Dict[str, object] = {}
        if target_symbol:
            commission = await self.client.get_um_commission_rate(
                target_symbol
            )
            raw_commission = commission.get("raw") or {}
            if "makerCommissionRate" not in raw_commission:
                raise RuntimeError(
                    "RWUSD maker commission rate is unavailable"
                )
            maker_bps = (
                float(commission.get("maker_commission_rate", 0) or 0)
                * 10000.0
            )
            self.engine.set_execution_cost_bps(maker_bps)
        if not account_preflight.hedge_mode:
            raise RuntimeError("RWUSD shared preflight did not enable Hedge Mode")
        self.engine.ensure_exchange_trade_sync_started(
            int(time.time() * 1000)
        )
        events.extend(await self.reconcile_active_orders())
        effective_symbol = str(
            self.engine.state.selected_symbol or target_symbol
        ).strip().upper()
        maximum_notional = 0.0
        if effective_symbol:
            leverage_status, leverage_events = (
                await self.refresh_symbol_leverage(
                    effective_symbol,
                    allow_upgrade=True,
                )
            )
            events.extend(leverage_events)
            maximum_notional = max(
                0.0,
                float(
                    leverage_status.get("max_notional_value", 0) or 0
                ),
            )
            if maximum_notional <= 0:
                maximum_notional = (
                    await self._ensure_symbol_leverage_limit(
                        effective_symbol
                    )
                )
            prices = reference_prices or {}
            effective_price = max(
                0.0,
                float(
                    prices.get(effective_symbol)
                    or reference_price
                    or self.engine.state.last_mark_price
                    or 0
                ),
            )
            can_prepare_target = (
                self.engine.profit_bucket.initial_collateralization_completed
                or self.engine.state.long_quantity > 1e-12
                or self.engine.state.short_quantity > 1e-12
            )
            if can_prepare_target:
                await self.prepare_target_quantity(
                    symbol=effective_symbol,
                    reference_price=effective_price,
                    portfolio=portfolio,
                    maximum_notional=maximum_notional,
                    allow_target_expansion=(
                        self.capital_target_expansion_ready
                    ),
                )
        events.append(
            {
                "type": "exchange_preflight",
                "action": "ready",
                "symbol": effective_symbol,
                "hedge_mode": True,
                "portfolio_margin": self.live_rwusd,
                "futures_demo": self.is_testnet,
                "official_uni_mmr": self.live_rwusd,
                "maker_only": True,
                "total_margin_fraction": self.total_margin_fraction,
                "per_side_margin_fraction": (
                    self.total_margin_fraction / 2.0
                ),
                "leverage": self.leverage,
                "target_leverage": self.target_leverage,
                "leverage_control": (
                    self.leverage_status(effective_symbol)
                    if effective_symbol
                    else {}
                ),
                "max_notional_per_symbol": maximum_notional,
                "rwusd_subscription": (
                    "mainnet_real" if self.live_rwusd else "shadow"
                ),
                "rwusd_source_assets": list(self._source_assets),
                "rwusd_quota_source_assets": (
                    list(operational_preflight.quota_source_assets)
                    if self.live_rwusd
                    else list(self._source_assets)
                ),
                "rwusd_subscription_enabled": (
                    (
                        operational_preflight.quota.get(
                            "subscribeEnable"
                        )
                        is not False
                        if operational_preflight.quota
                        else None
                    )
                    if self.live_rwusd
                    else True
                ),
                "rwusd_preflight_errors": list(
                    operational_preflight.errors
                ),
                "rwusd_collateral_balance": (
                    self._collateral_confirmation_balance(
                        operational_preflight.collateral
                    )
                    if self.live_rwusd
                    else 0.0
                ),
                "simulated": False,
                "testnet": self.is_testnet,
            }
        )
        return events

    async def _apply_mainnet_operational_preflight(
        self,
        snapshot: RWUSDMainnetPreflightSnapshot,
    ) -> RWUSDOperationalPreflight:
        errors = list(snapshot.errors)
        error_kinds = {
            error.split(":", 1)[0]
            for error in errors
        }
        if "account" not in error_kinds:
            self.engine.update_exchange_rwusd_snapshot(
                snapshot.rwusd_account
            )
        if "collateral" not in error_kinds:
            self.engine.update_exchange_rwusd_collateral_snapshot(
                snapshot.collateral
            )
        if "transferable" in error_kinds:
            self.engine.update_exchange_rwusd_transferable_snapshot(None)
        else:
            self.engine.update_exchange_rwusd_transferable_snapshot(
                snapshot.transferable
            )
        events: List[dict] = []
        try:
            events.extend(await self.reconcile_live_subscription())
        except Exception as exc:
            errors.append(f"pending_reconciliation:{exc}")
        managed_recovery = bool(
            self.engine.exchange_ownership_established()
            and (
                self.engine.state.long_quantity > 1e-12
                or self.engine.state.short_quantity > 1e-12
                or any(
                    order.external_client_order_id
                    for order in self.engine.lifecycle.active_orders
                )
            )
        )
        if errors and not managed_recovery:
            raise RuntimeError(
                "RWUSD mainnet preflight failed: " + "; ".join(errors)
            )
        quota_source_assets = tuple(
            asset
            for asset in self._source_assets
            if asset in self._quota_assets(snapshot.quota)
        )
        return RWUSDOperationalPreflight(
            quota=snapshot.quota,
            collateral=snapshot.collateral,
            quota_source_assets=quota_source_assets,
            errors=tuple(errors),
            events=tuple(events),
        )

    async def validate_runtime_isolation(
        self,
        *,
        expected_symbol: Optional[str] = None,
        account_preflight: Optional[
            RWUSDAccountPreflightSnapshot
        ] = None,
    ) -> RWUSDAccountPreflightSnapshot:
        snapshot = (
            account_preflight
            or await RWUSDAccountPreflightSnapshot.load(
                self.client,
                include_hedge_mode=False,
            )
        )
        self._validate_runtime_isolation_snapshot(
            snapshot,
            expected_symbol=expected_symbol,
        )
        return snapshot

    def _validate_runtime_isolation_snapshot(
        self,
        snapshot: RWUSDAccountPreflightSnapshot,
        *,
        expected_symbol: Optional[str] = None,
    ) -> None:
        active_positions = [
            position
            for position in snapshot.positions
            if abs(float(position.get("size", 0) or 0)) > 1e-12
        ]
        active_orders = list(snapshot.open_orders)
        ownership = (
            self._ownership_provider()
            if self._ownership_provider is not None
            else RWUSDOwnershipSnapshot(
                owns_account=self.engine.exchange_ownership_established(),
                managed_symbols=frozenset(
                    {
                        str(
                            expected_symbol
                            or self.engine.state.selected_symbol
                            or ""
                        ).strip().upper()
                    }
                    - {""}
                ),
                ignored_symbols=frozenset(),
                client_order_symbols={
                    str(order.external_client_order_id or "").strip(): str(
                        expected_symbol
                        or self.engine.state.selected_symbol
                        or ""
                    ).strip().upper()
                    for order in self.engine.lifecycle.active_orders
                    if str(order.external_client_order_id or "").strip()
                },
            )
        )
        owns_account = ownership.owns_account
        ignored_symbols = set(ownership.ignored_symbols)
        relevant_positions = [
            position
            for position in active_positions
            if str(position.get("symbol") or "").strip().upper()
            not in ignored_symbols
        ]
        relevant_orders = [
            order
            for order in active_orders
            if str(order.get("symbol") or "").strip().upper()
            not in ignored_symbols
        ]
        if not owns_account and (relevant_positions or relevant_orders):
            raise RuntimeError(
                "RWUSD startup blocked: exchange positions or open orders "
                "exist without matching local RWUSD ownership state"
            )
        target_symbol = str(
            expected_symbol
            or self.engine.state.selected_symbol
            or ""
        ).strip().upper()
        if not owns_account:
            return
        if (active_positions or active_orders) and not target_symbol:
            raise RuntimeError(
                "RWUSD runtime isolation cannot identify the managed symbol"
            )
        allowed_symbols = set(ownership.managed_symbols)
        if target_symbol:
            allowed_symbols.add(target_symbol)
        foreign_position_symbols = sorted(
            {
                str(position.get("symbol") or "").strip().upper()
                for position in active_positions
                if str(position.get("symbol") or "").strip().upper()
                not in allowed_symbols | ignored_symbols
            }
        )
        if foreign_position_symbols:
            raise RuntimeError(
                "RWUSD runtime isolation found non-managed positions: "
                + ",".join(foreign_position_symbols)
            )
        foreign_orders = []
        for order in active_orders:
            client_id = str(
                order.get("clientOrderId")
                or order.get("origClientOrderId")
                or ""
            ).strip()
            order_symbol = str(order.get("symbol") or "").strip().upper()
            if order_symbol in ignored_symbols:
                continue
            if (
                ownership.client_order_symbols.get(client_id)
                != order_symbol
            ):
                foreign_orders.append(
                    f"{order_symbol or '?'}:{client_id or '?'}"
                )
        if foreign_orders:
            raise RuntimeError(
                "RWUSD runtime isolation found untracked open orders: "
                + ",".join(foreign_orders)
            )

    async def ensure_hedge_mode(
        self,
        account_preflight: RWUSDAccountPreflightSnapshot,
    ) -> RWUSDAccountPreflightSnapshot:
        if account_preflight.hedge_mode is None:
            hedge_mode = await self.client.get_um_position_mode()
            if not isinstance(hedge_mode, bool):
                raise RuntimeError("RWUSD position mode response is invalid")
            account_preflight = replace(
                account_preflight,
                hedge_mode=hedge_mode,
            )
        if account_preflight.hedge_mode:
            return account_preflight
        active_positions = [
            position
            for position in account_preflight.positions
            if abs(float(position.get("size", 0) or 0)) > 1e-12
        ]
        if active_positions or account_preflight.open_orders:
            raise RuntimeError(
                "RWUSD requires Hedge Mode, but positions or orders "
                "must be cleared before mode change"
            )
        await self.client.set_um_position_mode(True)
        if not await self.client.get_um_position_mode():
            raise RuntimeError("RWUSD failed to enable Hedge Mode")
        return replace(account_preflight, hedge_mode=True)

    def _validate_operational_portfolio(
        self,
        portfolio: PortfolioSnapshot,
        *,
        allow_unfunded_initial: bool = False,
    ) -> None:
        if self.live_rwusd:
            if not portfolio.official_uni_mmr:
                raise RuntimeError(
                    "RWUSD mainnet requires official Portfolio Margin uniMMR"
                )
            status = str(portfolio.account_status or "").strip().upper()
            if status != "NORMAL":
                raise RuntimeError(
                    "RWUSD mainnet requires accountStatus=NORMAL, got "
                    f"{status or 'MISSING'}"
                )
        pending_initial = bool(
            self.engine.profit_bucket.pending_subscription_stage
            and self.engine.profit_bucket.pending_subscription_kind
            in {None, "initial"}
            and not self.engine.profit_bucket.initial_collateralization_completed
        )
        initial_funding_not_started = bool(
            allow_unfunded_initial
            and self.live_rwusd
            and not (
                self.engine.profit_bucket.initial_collateralization_completed
            )
            and self.engine.state.long_quantity <= 1e-12
            and self.engine.state.short_quantity <= 1e-12
        )
        if (
            portfolio.account_equity <= 0
            and not pending_initial
            and not initial_funding_not_started
        ):
            raise RuntimeError("Binance RWUSD account equity is zero")

    async def _refresh_live_transfer_portfolio(
        self,
        portfolio: PortfolioSnapshot,
    ) -> PortfolioSnapshot:
        account = await self.client.get_account_info()
        data = account if isinstance(account, dict) else {}
        fresh = replace(
            portfolio,
            account_equity=float(data.get("equity", 0) or 0),
            available_balance=float(data.get("available", 0) or 0),
            available_balance_known=parse_bool(
                data.get(
                    "available_balance_known",
                    "available" in data,
                )
            ),
            uni_mmr=float(
                data.get("margin_health")
                or data.get("uni_mmr")
                or 0
            ),
            withdrawable_balance=float(
                data.get("withdrawable", 0) or 0
            ),
            withdrawable_official=bool(
                data.get("withdrawable_official", False)
            ),
            source=str(data.get("source") or portfolio.source),
            account_type=str(
                data.get("account_type") or portfolio.account_type
            ),
            account_status=str(
                data.get("account_status") or portfolio.account_status
            ).upper(),
            margin_health_source=str(
                data.get("margin_health_source")
                or portfolio.margin_health_source
            ),
            official_uni_mmr=bool(data.get("official_uni_mmr", False)),
            account_maint_margin=max(
                0.0,
                float(data.get("account_maint_margin", 0) or 0),
            ),
            estimated_liquidation_risk=parse_bool(
                data.get("estimated_liquidation_risk", False)
            ),
            liquidation_risk_reason=(
                str(data.get("liquidation_risk_reason") or "") or None
            ),
        )
        self._validate_operational_portfolio(fresh)
        return fresh

    async def enrich_market_rows(
        self,
        rows: List[dict],
        portfolio: PortfolioSnapshot,
    ) -> List[dict]:
        capital_base = (
            self._capital_budget
            if self._capital_budget_enabled
            else max(0.0, float(portfolio.account_equity or 0))
        )
        gross_budget = (
            capital_base * self.total_margin_fraction * self.leverage
        )
        selected_symbol = str(
            self.engine.state.selected_symbol or ""
        ).strip().upper()
        selected_price = next(
            (
                max(0.0, float(row.get("close", 0) or 0))
                for row in rows
                if str(row.get("symbol") or "").strip().upper()
                == selected_symbol
            ),
            max(0.0, self.engine.state.last_mark_price),
        )
        current_managed_notional = (
            self.engine.state.long_quantity
            + self.engine.state.short_quantity
        ) * selected_price
        sizing_portfolio = portfolio
        if self._capital_budget_enabled:
            sizing_portfolio = replace(
                portfolio,
                total_abs_notional=current_managed_notional,
            )
        return await self._margin_model.enrich_market_rows(
            rows=rows,
            portfolio=sizing_portfolio,
            gross_budget=gross_budget,
            current_managed_notional=current_managed_notional,
        )

    async def _ensure_symbol_leverage_limit(self, symbol: str) -> float:
        target = str(symbol or "").strip().upper()
        if not target:
            raise RuntimeError("RWUSD leverage setup requires a symbol")
        leverage_status, _ = await self.refresh_symbol_leverage(
            target,
            allow_upgrade=False,
        )
        cached = max(
            0.0,
            float(
                self._max_notional_by_symbol.get(target)
                or leverage_status.get("max_notional_value")
                or 0
            ),
        )
        if cached > 0:
            return cached
        profile = await self._margin_model.get_profile(target)
        maximum = max(0.0, float(profile.max_notional_value or 0))
        if maximum <= 0:
            raise RuntimeError(
                "RWUSD leverage profile is missing maxNotionalValue"
            )
        self._max_notional_by_symbol[target] = maximum
        return maximum

    async def prepare_target_quantity(
        self,
        *,
        symbol: str,
        reference_price: float,
        portfolio: PortfolioSnapshot,
        maximum_notional: Optional[float] = None,
        allow_target_expansion: bool = True,
        capital_budget: Optional[float] = None,
    ) -> float:
        price = max(0.0, float(reference_price or 0))
        if price <= 0:
            raise RuntimeError(
                "RWUSD exchange target sizing requires a valid price"
            )
        equity = max(0.0, float(portfolio.account_equity or 0))
        lane_capital = max(
            0.0,
            float(
                self._capital_budget
                if capital_budget is None
                else capital_budget
            ),
        )
        lane_budget_enabled = (
            self._capital_budget_enabled
            if capital_budget is None
            else True
        )
        capital_base = lane_capital if lane_budget_enabled else equity
        if capital_base <= 0:
            raise RuntimeError(
                "RWUSD exchange target sizing requires positive lane capital"
            )
        current_managed_notional = (
            self.engine.state.long_quantity
            + self.engine.state.short_quantity
        ) * price
        gross_notional_budget = (
            capital_base * self.total_margin_fraction * self.leverage
        )
        if lane_budget_enabled:
            target_notional = gross_notional_budget / 2.0
        else:
            other_abs_notional = max(
                0.0,
                portfolio.total_abs_notional - current_managed_notional,
            )
            target_notional = max(
                0.0,
                gross_notional_budget - other_abs_notional,
            ) / 2.0
        limit = max(0.0, float(maximum_notional or 0))
        if limit <= 0:
            limit = await self._ensure_symbol_leverage_limit(symbol)
        if limit <= 0:
            raise RuntimeError(
                "RWUSD exchange target sizing requires maxNotionalValue"
            )
        profile = await self._margin_model.get_profile(symbol)
        limit = min(limit, profile.max_notional_value)
        target_notional = min(target_notional, limit / 2.0)
        raw_quantity = target_notional / price
        if not allow_target_expansion:
            approved_quantity = max(
                0.0,
                self.engine.state.target_quantity,
                self.engine.effective_hedge_quantity(),
            )
            raw_quantity = min(raw_quantity, approved_quantity)
            target_notional = raw_quantity * price
        risk_cap = self.engine.risk_target_quantity_cap()
        if risk_cap is not None:
            raw_quantity = min(raw_quantity, risk_cap)
            target_notional = raw_quantity * price
        if target_notional <= 0 or raw_quantity <= 0:
            self.engine.set_external_target_sizing(
                quantity=0.0,
                target_notional=0.0,
                opening_tranche_quantity=0.0,
                opening_tranche_notional=0.0,
                opening_tranche_block_reason="target_unavailable",
                minimum_order_quantity=0.0,
                order_step_size=0.0,
            )
            return 0.0
        minimum_order = (
            await self.client.get_um_minimum_limit_order_quantity(
                symbol,
                price,
            )
        )
        minimum_quantity = max(
            0.0,
            float((minimum_order or {}).get("quantity", 0) or 0),
        )
        minimum_notional = max(
            0.0,
            float((minimum_order or {}).get("notional", 0) or 0),
        )
        if (
            (minimum_order or {}).get("success")
            and (
                minimum_quantity > raw_quantity + 1e-12
                or minimum_notional > raw_quantity * price + 1e-8
            )
        ):
            self.engine.set_external_target_sizing(
                quantity=raw_quantity,
                target_notional=target_notional,
                opening_tranche_quantity=0.0,
                opening_tranche_notional=0.0,
                opening_tranche_block_reason="exchange_minimum",
                minimum_order_quantity=minimum_quantity,
                restore_minimum_quantity=(
                    minimum_quantity
                    + max(
                        0.0,
                        float((minimum_order or {}).get("step_size", 0) or 0),
                    )
                ),
                order_step_size=max(
                    0.0,
                    float((minimum_order or {}).get("step_size", 0) or 0),
                ),
            )
            return 0.0
        normalized = await self.client.normalize_um_limit_order_quantity(
            symbol,
            raw_quantity,
            price,
        )
        if not normalized.get("success"):
            raise RuntimeError(
                "RWUSD exchange target sizing failed: "
                f"{normalized.get('error') or 'unknown'}"
            )
        quantity = max(0.0, float(normalized.get("quantity", 0) or 0))
        target_notional = quantity * price
        planned_tranche_notional = target_notional * float(
            getattr(self.config, "OPENING_TRANCHE_FRACTION", 0.05)
        )
        opening_block_reason: Optional[str] = None
        if not (minimum_order or {}).get("success"):
            opening_block_reason = "exchange_rules"
        elif (
            minimum_quantity > quantity + 1e-12
            or minimum_notional > target_notional + 1e-8
        ):
            opening_block_reason = "exchange_minimum"
        requested_tranche_notional = min(
            target_notional,
            max(planned_tranche_notional, minimum_notional),
        )
        tranche_notional = 0.0
        if opening_block_reason is None:
            tranche_notional = self._margin_model.safe_pair_tranche_notional(
                profile=profile,
                portfolio=portfolio,
                current_long_notional=(
                    max(0.0, self.engine.state.long_quantity) * price
                ),
                current_short_notional=(
                    max(0.0, self.engine.state.short_quantity) * price
                ),
                requested_notional_per_side=requested_tranche_notional,
                execution_cost_bps=self.engine.execution_cost_bps(),
            )
            if tranche_notional + 1e-8 < minimum_notional:
                tranche_notional = 0.0
                opening_block_reason = "projected_margin"
        tranche_normalized = (
            await self.client.normalize_um_limit_order_quantity(
                symbol,
                tranche_notional / price,
                price,
            )
            if tranche_notional > 0
            else {"success": False}
        )
        if (
            opening_block_reason is None
            and not tranche_normalized.get("success")
        ):
            opening_block_reason = "exchange_rules"
        tranche_quantity = (
            max(
                0.0,
                float(tranche_normalized.get("quantity", 0) or 0),
            )
            if (
                opening_block_reason is None
                and tranche_normalized.get("success")
            )
            else 0.0
        )
        self.engine.set_external_target_sizing(
            quantity=quantity,
            target_notional=target_notional,
            opening_tranche_quantity=min(quantity, tranche_quantity),
            opening_tranche_notional=(
                min(quantity, tranche_quantity) * price
            ),
            opening_tranche_block_reason=opening_block_reason,
            minimum_order_quantity=minimum_quantity,
            restore_minimum_quantity=(
                minimum_quantity
                + max(
                    0.0,
                    float((minimum_order or {}).get("step_size", 0) or 0),
                )
            ),
            order_step_size=max(
                0.0,
                float((minimum_order or {}).get("step_size", 0) or 0),
            ),
        )
        return quantity

    async def reconcile_user_trades(self) -> List[dict]:
        symbol = str(
            self.engine.state.selected_symbol or ""
        ).strip().upper()
        if not symbol:
            return []
        self.engine.ensure_exchange_trade_sync_started(
            int(time.time() * 1000)
        )
        events: List[dict] = []
        for _ in range(10):
            cursor = self.engine.exchange_trade_cursor(symbol)
            trades = await self.client.get_um_user_trades(
                symbol,
                from_id=(cursor + 1 if cursor >= 0 else None),
                start_time_ms=(
                    None
                    if cursor >= 0
                    else self.engine.profit_bucket.exchange_trade_sync_started_at_ms
                ),
                limit=1000,
            )
            enriched = []
            for trade in await self._value_trade_commissions(trades or []):
                item = dict(trade)
                order_id = str(item.get("order_id") or "")
                local = (
                    self.engine.lifecycle.get_order_by_external_id(order_id)
                )
                item["purpose"] = (
                    local.purpose if local is not None else "external_manual"
                )
                enriched.append(item)
            events.extend(self.engine.apply_external_user_trades(enriched))
            if len(trades or []) < 1000:
                break
        return events

    async def enforce_runtime_account_guard(
        self,
        *,
        reason: object,
        reference_price: float,
    ) -> List[dict]:
        events = self.engine.force_runtime_risk_freeze(
            reason=reason,
        )
        events.extend(
            await self._exit_execution.advance_hard_risk_reduction()
        )
        await self.reconcile_positions()
        return events

    async def reconcile_active_orders(self) -> List[dict]:
        events: List[dict] = []
        for order in list(self.engine.lifecycle.active_orders):
            if not order.external_client_order_id:
                continue
            try:
                response = await self.client.get_um_order(
                    symbol=self.engine.state.selected_symbol,
                    order_id=order.external_order_id,
                    client_order_id=order.external_client_order_id,
                )
                events.extend(self._apply_order_response(order.order_id, response))
            except Exception as exc:
                if self._is_order_not_found(exc):
                    events.extend(
                        self.engine.record_external_order_not_found(
                            order.order_id,
                            exc,
                        )
                    )
                else:
                    events.extend(
                        self.engine.record_external_order_error(
                            order.order_id,
                            exc,
                            definitive_rejection=False,
                        )
                    )
        await self.reconcile_positions()
        events.extend(await self.reconcile_user_trades())
        events.extend(
            await self.cancel_risk_blocked_orders(
                reference_price=self.engine.state.last_mark_price,
            )
        )
        events.extend(await self._cancel_excess_restore_coverage())
        events.extend(await self._cancel_stale_maker_orders())
        return events

    async def _cancel_excess_restore_coverage(self) -> List[dict]:
        snapshot = self.engine.restore_overcoverage()
        if snapshot is None:
            return []
        order_ids = set(snapshot["order_ids"])
        orders = [
            order
            for order in self.engine.lifecycle.active_orders
            if order.order_id in order_ids
        ]
        events = [
            {
                "type": "exchange_restore_coverage_cancel_requested",
                "action": "restore_coverage_realign",
                **snapshot,
                "simulated": False,
                "testnet": self.is_testnet,
            }
        ]
        events.extend(await self._cancel_orders(orders))
        await self.reconcile_positions()
        return events

    async def _cancel_stale_maker_orders(self) -> List[dict]:
        events: List[dict] = []
        timeout_cycles = max(
            1,
            int(getattr(self.config, "MAKER_ORDER_TIMEOUT_CYCLES", 3)),
        )
        active = list(self.engine.lifecycle.active_orders)
        pair_active = [
            order
            for order in active
            if order.purpose in SYNCHRONIZED_PAIR_PURPOSES
        ]
        repair_active = [
            order for order in active if order.purpose in VECTOR_REPAIR_PURPOSES
        ]
        difference = (
            self.engine.state.long_quantity
            - self.engine.state.short_quantity
        )
        catchup_order = (
            repair_active[0]
            if self._vector_catchup_is_exact(
                repair_active,
                difference,
            )
            else None
        )
        catchup_cycles = max(
            1,
            int(
                getattr(
                    self.config,
                    "OPENING_CATCHUP_REPRICE_CYCLES",
                    1,
                )
            ),
        )
        due = [
            order
            for order in active
            if order.maker_only
            and order.purpose not in CUSTOMER_EXIT_PURPOSES
            and order.external_order_id
            and order.external_submitted_cycle is not None
            and self.engine.lifecycle.cycle
            - order.external_submitted_cycle
            + 1
            >= (
                catchup_cycles
                if order is catchup_order
                else timeout_cycles
            )
        ]
        if not due:
            return events
        symbol = str(self.engine.state.selected_symbol or "").strip().upper()
        book = await self.client.get_book_ticker(symbol)
        pair_due: List[object] = []
        to_cancel: List[object] = []
        for order in due:
            price_mode = (
                "opening_catchup"
                if order is catchup_order
                else (
                    "opening_pair"
                    if order.purpose in SYNCHRONIZED_PAIR_PURPOSES
                    else "touch"
                )
            )
            if not await self._maker_order_is_behind(
                order,
                book,
                symbol=symbol,
                price_mode=price_mode,
            ):
                continue
            if order.purpose in SYNCHRONIZED_PAIR_PURPOSES:
                pair_due.append(order)
            else:
                to_cancel.append(order)
        if pair_due:
            to_cancel.extend(pair_active)
        seen: Set[str] = set()
        for order in to_cancel:
            if order.order_id in seen or not order.external_order_id:
                continue
            seen.add(order.order_id)
            try:
                response = await self.client.cancel_um_order(
                    symbol=symbol,
                    order_id=order.external_order_id,
                )
                events.extend(self._apply_order_response(order.order_id, response))
            except Exception as exc:
                events.extend(
                    self.engine.record_external_order_error(
                        order.order_id,
                        exc,
                        definitive_rejection=False,
                    )
                )
        return events

    async def reconcile_hedge_vector(
        self,
        *,
        reference_price: float,
    ) -> List[dict]:
        if self.engine.state.phase not in {
            StrategyPhase.OPENING_HEDGE,
            StrategyPhase.HEDGED,
            StrategyPhase.REPAIRING_HEDGE,
        }:
            return []
        difference = (
            self.engine.state.long_quantity
            - self.engine.state.short_quantity
        )
        active = [
            order
            for order in self.engine.lifecycle.active_orders
            if order.purpose in MANAGED_VECTOR_PURPOSES
        ]
        if self.engine.opening_imbalance_is_tolerated(difference):
            if self._vector_projection_is_balanced(active):
                return []
            return await self._cancel_orders(active)
        if any(order.purpose in PAIR_BUILD_PURPOSES for order in active):
            events = await self._cancel_orders(active)
            await self.reconcile_positions()
            return events
        if not active or self._vector_projection_is_balanced(active):
            return []
        events = await self._cancel_orders(active)
        await self.reconcile_positions()
        return events

    async def execute_pending_orders(
        self,
        *,
        reference_price: float,
    ) -> List[dict]:
        events: List[dict] = []
        symbol = str(self.engine.state.selected_symbol or "").strip().upper()
        if self.engine.state.phase == StrategyPhase.PAUSED:
            self._cancel_unsubmitted_orders()
            return events
        if not symbol:
            return events
        events.extend(
            await self.cancel_risk_blocked_orders(
                reference_price=reference_price,
            )
        )
        for _ in range(4):
            made_progress = False
            pending = [
                order
                for order in list(self.engine.lifecycle.active_orders)
                if not order.external_client_order_id
            ]
            build_pair = self._balanced_build_pair(pending)
            if build_pair:
                made_progress = True
                events.extend(
                    await self._execute_opening_pair(
                        build_pair,
                        symbol=symbol,
                        reference_price=reference_price,
                    )
                )
                pending = [
                    order
                    for order in list(self.engine.lifecycle.active_orders)
                    if not order.external_client_order_id
                ]
            emergency_orders = [
                order
                for order in pending
                if order.purpose in EMERGENCY_MARKET_PURPOSES
            ]
            if len(emergency_orders) > 1:
                made_progress = True
                events.extend(
                    await self._execute_emergency_orders(
                        emergency_orders,
                        symbol=symbol,
                        reference_price=reference_price,
                    )
                )
                emergency_ids = {
                    order.order_id for order in emergency_orders
                }
                pending = [
                    order
                    for order in pending
                    if order.order_id not in emergency_ids
                ]
            exit_orders = [
                order
                for order in pending
                if order.purpose in CUSTOMER_EXIT_LIMIT_PURPOSES
            ]
            if len(exit_orders) > 1:
                made_progress = True
                events.extend(
                    await self._execute_limit_close_orders(
                        exit_orders,
                        symbol=symbol,
                    )
                )
                exit_ids = {order.order_id for order in exit_orders}
                pending = [
                    order
                    for order in pending
                    if order.order_id not in exit_ids
                ]
            for order in pending:
                if self.engine.state.phase == StrategyPhase.PAUSED:
                    self._cancel_unsubmitted_orders()
                    return events
                if not self._order_allowed_by_current_risk(order.purpose):
                    self.engine.apply_external_order_update(
                        order_id=order.order_id,
                        external_order_id=None,
                        external_status="CANCELED",
                        cumulative_filled_quantity=(
                            order.cumulative_filled_quantity
                        ),
                        average_fill_price=order.price,
                    )
                    continue
                made_progress = True
                events.extend(
                    await self._execute_order(
                        order,
                        symbol=symbol,
                        reference_price=reference_price,
                    )
                )
                if (
                    order.purpose in PAIR_BUILD_PURPOSES
                    and order.status == "REJECTED"
                ):
                    events.extend(
                        await self._handle_opening_submission_failure(
                            reference_price=reference_price,
                            reason=order.external_error,
                        )
                    )
                elif (
                    order.purpose == "hedge_repair"
                    and order.status == "REJECTED"
                ):
                    self.engine.defer_repair_retry(
                        order.external_error or "repair_order_rejected"
                    )
                elif (
                    order.purpose == HEDGE_REPAIR_PAIR_PURPOSE
                    and order.status == "REJECTED"
                ):
                    self.engine.defer_repair_retry(
                        order.external_error or "repair_pair_order_rejected"
                    )
            restore_events = self.engine.plan_pending_harvest_orders(
                reference_price
            )
            if restore_events:
                events.extend(restore_events)
                made_progress = True
            if not made_progress:
                break
        return events

    async def _execute_emergency_orders(
        self,
        orders: List[object],
        *,
        symbol: str,
        reference_price: float,
    ) -> List[dict]:
        prepared = []
        events: List[dict] = []
        for order in orders:
            if not self._has_valid_hedge_mode_side(order):
                events.extend(
                    self.engine.record_external_order_error(
                        order.order_id,
                        "invalid_hedge_mode_order_shape",
                        definitive_rejection=True,
                    )
                )
                continue
            client_order_id = self._client_order_id(order.order_id)
            self.engine.assign_external_order(
                order.order_id,
                client_order_id,
            )
            prepared.append((order, client_order_id))
        responses = await asyncio.gather(
            *[
                self.client.submit_um_market_order(
                    symbol=symbol,
                    position_side=order.position_side.value,
                    side=order.side,
                    quantity=(
                        order.quantity - order.cumulative_filled_quantity
                    ),
                    reference_price=max(
                        0.0,
                        float(reference_price or order.price or 0),
                    ),
                    client_order_id=client_order_id,
                )
                for order, client_order_id in prepared
            ],
            return_exceptions=True,
        )
        for (order, client_order_id), response in zip(prepared, responses):
            events.extend(
                await self._process_submission_result(
                    order=order,
                    symbol=symbol,
                    client_order_id=client_order_id,
                    response=response,
                )
            )
        await self.reconcile_positions()
        return events

    async def _execute_opening_pair(
        self,
        orders: List[object],
        *,
        symbol: str,
        reference_price: float,
    ) -> List[dict]:
        events: List[dict] = []
        book = await self.client.get_book_ticker(symbol)
        prepared = []
        for order in orders:
            client_order_id = self._client_order_id(order.order_id)
            if not self._has_valid_hedge_mode_side(order):
                events.extend(
                    self.engine.record_external_order_error(
                        order.order_id,
                        "invalid_hedge_mode_order_shape",
                        definitive_rejection=True,
                    )
                )
                continue
            self.engine.assign_external_order(
                order.order_id,
                client_order_id,
            )
            prepared.append((order, client_order_id))
        responses = await asyncio.gather(
            *[
                self.client.submit_um_post_only_order(
                    symbol=symbol,
                    position_side=order.position_side.value,
                    side=order.side,
                    quantity=(
                        order.quantity
                        - order.cumulative_filled_quantity
                    ),
                    best_bid=float((book or {}).get("bid_price", 0) or 0),
                    best_ask=float((book or {}).get("ask_price", 0) or 0),
                    client_order_id=client_order_id,
                    price_mode="opening_pair",
                )
                for order, client_order_id in prepared
            ],
            return_exceptions=True,
        )
        for (order, client_order_id), response in zip(prepared, responses):
            events.extend(
                await self._process_submission_result(
                    order=order,
                    symbol=symbol,
                    client_order_id=client_order_id,
                    response=response,
                )
            )
        if any(order.status == "REJECTED" for order in orders):
            if any(
                order.purpose in PAIR_BUILD_PURPOSES for order in orders
            ):
                events.extend(
                    await self._handle_opening_submission_failure(
                        reference_price=reference_price,
                        reason="opening_pair_submission_failed",
                    )
                )
            else:
                events.extend(
                    await self._handle_repair_pair_submission_failure(
                        reason="repair_pair_submission_failed",
                    )
                )
        else:
            accepted = all(
                order.external_order_id
                and order.status in {"NEW", "PARTIALLY_FILLED", "FILLED"}
                for order in orders
            )
            if (
                accepted
                and orders
                and orders[0].purpose == HEDGE_REPAIR_PAIR_PURPOSE
            ):
                self.engine.clear_hedge_build_deferred()
            await self.reconcile_positions()
        return events

    async def _execute_limit_close_orders(
        self,
        orders: List[object],
        *,
        symbol: str,
    ) -> List[dict]:
        book = await self.client.get_book_ticker(symbol)
        best_bid = float((book or {}).get("bid_price", 0) or 0)
        best_ask = float((book or {}).get("ask_price", 0) or 0)
        prepared = []
        events: List[dict] = []
        for order in orders:
            if not self._has_valid_hedge_mode_side(order):
                events.extend(
                    self.engine.record_external_order_error(
                        order.order_id,
                        "invalid_hedge_mode_order_shape",
                        definitive_rejection=True,
                    )
                )
                continue
            client_order_id = self._client_order_id(order.order_id)
            self.engine.assign_external_order(
                order.order_id,
                client_order_id,
            )
            prepared.append((order, client_order_id))
        responses = await asyncio.gather(
            *[
                self.client.submit_um_limit_close_order(
                    symbol=symbol,
                    position_side=order.position_side.value,
                    side=order.side,
                    quantity=(
                        order.quantity - order.cumulative_filled_quantity
                    ),
                    best_bid=best_bid,
                    best_ask=best_ask,
                    client_order_id=client_order_id,
                )
                for order, client_order_id in prepared
            ],
            return_exceptions=True,
        )
        for (order, client_order_id), response in zip(prepared, responses):
            events.extend(
                await self._process_submission_result(
                    order=order,
                    symbol=symbol,
                    client_order_id=client_order_id,
                    response=response,
                )
            )
        await self.reconcile_positions()
        return events

    async def _execute_order(
        self,
        order: object,
        *,
        symbol: str,
        reference_price: float,
    ) -> List[dict]:
        events = await self._resize_harvest_order_to_exchange_minimum(
            order=order,
            symbol=symbol,
            reference_price=reference_price,
        )
        if order.status not in {"NEW", "PARTIALLY_FILLED"}:
            return events
        client_order_id = self._client_order_id(order.order_id)
        if not self._has_valid_hedge_mode_side(order):
            events.extend(
                self.engine.record_external_order_error(
                    order.order_id,
                    "invalid_hedge_mode_order_shape",
                    definitive_rejection=True,
                )
            )
            return events
        self.engine.assign_external_order(order.order_id, client_order_id)
        try:
            if order.purpose in EMERGENCY_MARKET_PURPOSES:
                response = await self.client.submit_um_market_order(
                    symbol=symbol,
                    position_side=order.position_side.value,
                    side=order.side,
                    quantity=(
                        order.quantity - order.cumulative_filled_quantity
                    ),
                    reference_price=max(
                        0.0,
                        float(reference_price or order.price or 0),
                    ),
                    client_order_id=client_order_id,
                )
            elif order.purpose in CUSTOMER_EXIT_LIMIT_PURPOSES:
                book = await self.client.get_book_ticker(symbol)
                response = await self.client.submit_um_limit_close_order(
                    symbol=symbol,
                    position_side=order.position_side.value,
                    side=order.side,
                    quantity=(
                        order.quantity - order.cumulative_filled_quantity
                    ),
                    best_bid=float(
                        (book or {}).get("bid_price", 0) or 0
                    ),
                    best_ask=float(
                        (book or {}).get("ask_price", 0) or 0
                    ),
                    client_order_id=client_order_id,
                )
            else:
                book = await self.client.get_book_ticker(symbol)
                response = await self.client.submit_um_post_only_order(
                    symbol=symbol,
                    position_side=order.position_side.value,
                    side=order.side,
                    quantity=(
                        order.quantity - order.cumulative_filled_quantity
                    ),
                    best_bid=float((book or {}).get("bid_price", 0) or 0),
                    best_ask=float((book or {}).get("ask_price", 0) or 0),
                    client_order_id=client_order_id,
                    price_mode=(
                        "opening_catchup"
                        if order.purpose == "hedge_repair"
                        else "touch"
                    ),
                )
        except Exception as exc:
            response = exc
        events.extend(
            await self._process_submission_result(
                order=order,
                symbol=symbol,
                client_order_id=client_order_id,
                response=response,
            )
        )
        return events

    async def _resize_harvest_order_to_exchange_minimum(
        self,
        *,
        order: object,
        symbol: str,
        reference_price: float,
    ) -> List[dict]:
        if str(order.purpose or "").strip().lower() not in (
            HARVEST_ORDER_PURPOSES
        ):
            return []
        remaining = max(
            0.0,
            float(order.quantity or 0)
            - float(order.cumulative_filled_quantity or 0),
        )
        if remaining <= 1e-12:
            return []
        normalized = await self.client.normalize_um_limit_order_quantity(
            symbol,
            remaining,
            reference_price,
        )
        is_restore = str(order.purpose or "").strip().lower() == "restore"
        if normalized.get("success") and not is_restore:
            return []
        if (
            not normalized.get("success")
            and not self._is_minimum_order_error(normalized.get("error"))
        ):
            return []
        minimum = await self.client.get_um_minimum_limit_order_quantity(
            symbol,
            reference_price,
        )
        if not minimum.get("success"):
            return self.engine.record_harvest_minimum_deferred(
                order_id=order.order_id,
                error=(
                    "exchange_minimum_unavailable:"
                    f"{minimum.get('error') or 'unknown'}"
                ),
                minimum_quantity=remaining,
                reference_price=reference_price,
                minimum_known=False,
            )
        minimum_quantity = max(
            0.0,
            float(minimum.get("quantity", 0) or 0),
        )
        step_size = max(0.0, float(minimum.get("step_size", 0) or 0))
        required_quantity = (
            minimum_quantity + step_size
            if is_restore and step_size > 0
            else minimum_quantity
        )
        normalized_quantity = max(
            0.0,
            float(normalized.get("quantity", 0) or 0),
        )
        if (
            required_quantity <= remaining + 1e-12
            and (
                not is_restore
                or normalized_quantity + 1e-12 >= required_quantity
            )
        ):
            return []
        if not is_restore:
            position_quantity = (
                self.engine.state.long_quantity
                if order.position_side.value == "LONG"
                else self.engine.state.short_quantity
            )
            available = max(
                0.0,
                float(position_quantity or 0)
                - float(order.cumulative_filled_quantity or 0),
            )
            if required_quantity > available + 1e-12:
                return self.engine.record_harvest_minimum_deferred(
                    order_id=order.order_id,
                    error="order_below_exchange_minimum",
                    minimum_quantity=required_quantity,
                    minimum_notional=float(
                        minimum.get("minimum_notional", 0) or 0
                    ),
                    reference_price=reference_price,
                    minimum_known=True,
                )
        else:
            return self.engine.record_harvest_minimum_deferred(
                order_id=order.order_id,
                error="order_below_exchange_minimum",
                minimum_quantity=required_quantity,
                minimum_notional=float(
                    minimum.get("minimum_notional", 0) or 0
                ),
                reference_price=reference_price,
                minimum_known=True,
            )
        self.engine.set_external_order_quantity(
            order.order_id,
            float(order.cumulative_filled_quantity or 0) + required_quantity,
        )
        return [
            {
                "type": "exchange_harvest_quantity_adjusted",
                "action": order.purpose,
                "order_id": order.order_id,
                "requested_quantity": remaining,
                "exchange_minimum_quantity": required_quantity,
                "exchange_step_size": step_size,
                "minimum_notional": float(
                    minimum.get("minimum_notional", 0) or 0
                ),
                "simulated": False,
            }
        ]

    async def _process_submission_result(
        self,
        *,
        order: object,
        symbol: str,
        client_order_id: str,
        response: object,
    ) -> List[dict]:
        if isinstance(response, Exception):
            if self._is_maker_requote_error(response):
                return self.engine.record_external_maker_requote(
                    order_id=order.order_id,
                    error=response,
                )
            return await self._resolve_submission_exception(
                order_id=order.order_id,
                symbol=symbol,
                client_order_id=client_order_id,
                error=response,
            )
        payload = response if isinstance(response, dict) else {}
        if not payload.get("success"):
            if (
                order.purpose in HARVEST_ORDER_PURPOSES
                and self._is_minimum_order_error(payload.get("error"))
            ):
                try:
                    minimum = (
                        await self.client.get_um_minimum_limit_order_quantity(
                            symbol,
                            order.price,
                        )
                    )
                except Exception as exc:
                    minimum = {
                        "success": False,
                        "error": f"minimum_query_failed:{exc}",
                    }
                return self.engine.record_harvest_minimum_deferred(
                    order_id=order.order_id,
                    error=payload.get("error"),
                    minimum_quantity=float(
                        (minimum or {}).get("quantity", 0) or 0
                    ),
                    minimum_notional=float(
                        (minimum or {}).get("minimum_notional", 0) or 0
                    ),
                    reference_price=order.price,
                    minimum_known=bool(
                        (minimum or {}).get("success")
                    ),
                )
            if (
                order.purpose
                in SYNCHRONIZED_PAIR_PURPOSES | VECTOR_REPAIR_PURPOSES
                and self._is_minimum_order_error(payload.get("error"))
            ):
                try:
                    minimum = (
                        await self.client.get_um_minimum_limit_order_quantity(
                            symbol,
                            order.price,
                        )
                    )
                except Exception as exc:
                    minimum = {
                        "success": False,
                        "error": f"minimum_query_failed:{exc}",
                    }
                return self.engine.record_external_order_dust(
                    order_id=order.order_id,
                    error=payload.get("error"),
                    minimum_quantity=float(
                        (minimum or {}).get("quantity", 0) or 0
                    ),
                    minimum_notional=float(
                        (minimum or {}).get("minimum_notional", 0) or 0
                    ),
                    reference_price=order.price,
                    minimum_known=bool(
                        (minimum or {}).get("success")
                    ),
                )
            if self._is_maker_requote_error(payload.get("error")):
                return self.engine.record_external_maker_requote(
                    order_id=order.order_id,
                    error=payload.get("error"),
                )
            return self.engine.record_external_order_error(
                order.order_id,
                payload.get("error") or "Binance order rejected",
                definitive_rejection=True,
            )
        submitted_price = float(payload.get("submitted_price", 0) or 0)
        normalized_quantity = float(payload.get("quantity", 0) or 0)
        if normalized_quantity > 0:
            self.engine.set_external_order_quantity(
                order.order_id,
                normalized_quantity,
            )
        if submitted_price > 0:
            self.engine.set_external_order_price(
                order.order_id,
                submitted_price,
            )
        if order.purpose in HARVEST_ORDER_PURPOSES:
            self.engine.lifecycle.clear_deferred_minimum(
                purpose=order.purpose,
                position_side=order.position_side,
            )
        if (
            order.purpose
            in SYNCHRONIZED_PAIR_PURPOSES | VECTOR_REPAIR_PURPOSES
        ):
            self.engine.lifecycle.clear_deferred_minimum(
                purpose=order.purpose,
                position_side=order.position_side,
            )
        if order.purpose in {"open_hedge", "hedge_repair"}:
            self.engine.clear_repair_deferred()
        events = [
            {
                "type": "exchange_order_submitted",
                "action": order.purpose,
                "order_id": order.order_id,
                "external_order_id": payload.get("order_id"),
                "client_order_id": client_order_id,
                "order_type": (
                    str(order.order_type or "LIMIT").upper()
                ),
                "time_in_force": (
                    str(order.time_in_force or "").upper() or None
                ),
                "maker_only": bool(order.maker_only),
                "submitted_price": submitted_price,
                "portfolio_margin": self.live_rwusd,
                "futures_demo": self.is_testnet,
                "simulated": False,
                "testnet": self.is_testnet,
            }
        ]
        events.extend(self._apply_order_response(order.order_id, payload))
        return events

    async def _handle_opening_submission_failure(
        self,
        *,
        reference_price: float,
        reason: object,
    ) -> List[dict]:
        active = [
            order
            for order in self.engine.lifecycle.active_orders
            if order.purpose in PAIR_BUILD_PURPOSES
        ]
        events = await self._cancel_orders(active)
        await self.reconcile_positions()
        if any(
            order.purpose in PAIR_BUILD_PURPOSES
            for order in self.engine.lifecycle.active_orders
        ):
            return events
        if (
            abs(
                self.engine.state.long_quantity
                - self.engine.state.short_quantity
            )
            > 1e-10
        ):
            self.engine.state.phase = StrategyPhase.REPAIRING_HEDGE
            self.engine.state.last_action = "hedge_gap_after_pair_failure"
        events.append(self.engine.record_opening_pair_failure(reason))
        return events

    async def _handle_repair_pair_submission_failure(
        self,
        *,
        reason: object,
    ) -> List[dict]:
        active = [
            order
            for order in self.engine.lifecycle.active_orders
            if order.purpose == HEDGE_REPAIR_PAIR_PURPOSE
        ]
        events = await self._cancel_orders(active)
        await self.reconcile_positions()
        self.engine.defer_repair_retry(reason)
        return events

    async def _cancel_orders(self, orders: List[object]) -> List[dict]:
        events: List[dict] = []
        symbol = str(self.engine.state.selected_symbol or "").strip().upper()
        for order in list(orders):
            if not order.external_client_order_id:
                self.engine.apply_external_order_update(
                    order_id=order.order_id,
                    external_order_id=None,
                    external_status="CANCELED",
                    cumulative_filled_quantity=(
                        order.cumulative_filled_quantity
                    ),
                    average_fill_price=order.price,
                )
                continue
            if not order.external_order_id:
                continue
            try:
                response = await self.client.cancel_um_order(
                    symbol=symbol,
                    order_id=order.external_order_id,
                )
                events.extend(
                    self._apply_order_response(
                        order.order_id,
                        response,
                        cancellation_requested=True,
                    )
                )
            except Exception as exc:
                events.extend(
                    self.engine.record_external_order_error(
                        order.order_id,
                        exc,
                        definitive_rejection=False,
                    )
                )
        return events

    def _vector_projection_is_balanced(
        self,
        active_orders: List[object],
    ) -> bool:
        projected_long = self.engine.state.long_quantity
        projected_short = self.engine.state.short_quantity
        for order in active_orders:
            remaining = max(
                0.0,
                order.quantity - order.cumulative_filled_quantity,
            )
            if order.position_side.value == "LONG":
                projected_long += (
                    -remaining if order.reduce_only else remaining
                )
            elif order.position_side.value == "SHORT":
                projected_short += (
                    -remaining if order.reduce_only else remaining
                )
        return self.engine.opening_imbalance_is_tolerated(
            projected_long - projected_short
        )

    def _vector_catchup_is_exact(
        self,
        active_orders: List[object],
        difference: float,
    ) -> bool:
        if (
            len(active_orders) != 1
            or self.engine.opening_imbalance_is_tolerated(difference)
        ):
            return False
        order = active_orders[0]
        missing_side = "SHORT" if difference > 0 else "LONG"
        remaining = max(
            0.0,
            float(order.quantity)
            - float(order.cumulative_filled_quantity),
        )
        return (
            not bool(order.reduce_only)
            and str(order.position_side.value) == missing_side
            and math.isclose(
                remaining,
                abs(difference),
                rel_tol=1e-9,
                abs_tol=1e-10,
            )
        )

    def _balanced_build_pair(
        self,
        pending: List[object],
    ) -> List[object]:
        opening = [
            order
            for order in pending
            if order.purpose in SYNCHRONIZED_PAIR_PURPOSES
            and not order.reduce_only
        ]
        if len(opening) != 2:
            return []
        sides = {order.position_side.value for order in opening}
        return (
            opening
            if (
                sides == {"LONG", "SHORT"}
                and len({order.purpose for order in opening}) == 1
                and self._vector_projection_is_balanced(opening)
            )
            else []
        )

    async def reconcile_positions(self) -> None:
        positions = await self.client.get_um_positions()
        target = str(
            self.engine.state.selected_symbol or ""
        ).strip().upper()
        relevant = [
            position
            for position in positions or []
            if str(position.get("symbol") or "").strip().upper() == target
            and abs(float(position.get("size", 0) or 0)) > 1e-12
        ]
        long_position = next(
            (item for item in relevant if str(item.get("side")) == "long"),
            None,
        )
        short_position = next(
            (item for item in relevant if str(item.get("side")) == "short"),
            None,
        )
        self.engine.reconcile_external_positions(
            symbol=target or None,
            long_quantity=float((long_position or {}).get("size", 0) or 0),
            long_entry_price=float(
                (long_position or {}).get("entry_price", 0) or 0
            ),
            short_quantity=float((short_position or {}).get("size", 0) or 0),
            short_entry_price=float(
                (short_position or {}).get("entry_price", 0) or 0
            ),
        )

    async def execute_live_subscription(
        self,
        portfolio: PortfolioSnapshot,
        *,
        kind: str = "harvest",
        requested_amount: Optional[float] = None,
    ) -> List[dict]:
        if not self.live_rwusd:
            return self.engine.sweep_shadow_rwusd(portfolio)
        events = await self.reconcile_live_subscription()
        stage = self.engine.profit_bucket.pending_subscription_stage
        if stage in LIVE_SUBSCRIPTION_WAIT_STAGES:
            return events
        if stage == "SOURCE_COLLECTED":
            events.extend(await self._advance_live_subscription_source())
            return events
        if stage in {"TRANSFERRED", "CONVERTED"}:
            events.extend(await self._finish_live_subscription())
            return events
        if stage == "SUBSCRIBED":
            events.extend(
                await self._settle_live_subscription_collateral()
            )
            return events
        requested = self._floor_asset_amount(
            (
                requested_amount
                if requested_amount is not None
                else self.engine.live_subscription_amount(portfolio)
            )
        )
        if requested <= 0:
            return events
        normalized_kind = str(kind or "").strip().lower()
        if normalized_kind == "harvest":
            try:
                portfolio = await self._refresh_live_transfer_portfolio(
                    portfolio
                )
            except Exception as exc:
                self.engine.defer_harvest_transfer(
                    f"transfer_retry_waiting_for_account_read:{exc}"
                )
                return events
            requested, safety_reason = (
                self._safe_harvest_subscription_amount(
                    portfolio=portfolio,
                    requested_amount=requested,
                )
            )
            if requested <= 0:
                self.engine.block_live_subscription(safety_reason)
                return events
        try:
            quota = await self.client.get_rwusd_quota()
            withdrawable = (
                portfolio.withdrawable_balance
                if portfolio.withdrawable_official
                else None
            )
            choice = await self._select_subscription_source(
                quota=quota,
                requested_amount=requested,
                withdrawable=withdrawable,
                include_margin=normalized_kind != "capital",
                include_spot=normalized_kind in {"initial", "capital"},
                include_funding=normalized_kind in {"initial", "capital"},
                preferred_source_asset=(
                    str(
                        getattr(
                            self.config,
                            "RWUSD_HARVEST_SOURCE_ASSET",
                            "",
                        )
                        or ""
                    ).strip().upper()
                    if normalized_kind == "harvest"
                    else None
                ),
                preferred_target_asset=(
                    str(
                        getattr(
                            self.config,
                            "RWUSD_HARVEST_SUBSCRIPTION_ASSET",
                            "",
                        )
                        or ""
                    ).strip().upper()
                    if normalized_kind == "harvest"
                    else None
                ),
            )
        except Exception as exc:
            if normalized_kind == "harvest":
                self.engine.defer_harvest_transfer(
                    f"transfer_retry_waiting_for_source_read:{exc}"
                )
            else:
                self.engine.block_live_subscription(
                    f"rwusd_source_preflight_unavailable:{exc}"
                )
            return events
        if not choice.asset or choice.amount <= 0:
            self.engine.block_live_subscription(choice.reason)
            events.append(
                {
                    "type": "rwusd_subscription_preflight",
                    "action": "blocked",
                    "reason": choice.reason,
                    "requested_amount": requested,
                    "minimum": choice.minimum,
                    "balances": dict(choice.balances),
                    "eligible_assets": list(choice.eligible_assets),
                    "simulated": False,
                    "testnet": self.is_testnet,
                }
            )
            return events
        events.extend(
            await self._execute_selected_live_subscription(
                choice=choice,
                kind=kind,
            )
        )
        return events

    async def _execute_selected_live_subscription(
        self,
        *,
        choice: _SubscriptionSourceChoice,
        kind: str,
        capital_intent_callback: Optional[Callable[..., object]] = None,
    ) -> List[dict]:
        asset = choice.asset
        amount = choice.amount
        if not asset or amount <= 0:
            return []
        source_asset = str(choice.source_asset or asset).strip().upper()
        source_amount = self._floor_asset_amount(
            choice.source_amount or amount
        )
        if source_amount <= 0:
            return []
        events: List[dict] = []
        capital_reference = None
        if (
            str(kind or "").strip().lower() == "capital"
            and capital_intent_callback is not None
        ):
            intent = capital_intent_callback(
                amount=source_amount,
                source_asset=source_asset,
                capital_contribution_before=(
                    self.engine.profit_bucket.capital_contribution_total
                ),
            )
            if inspect.isawaitable(intent):
                intent = await intent
            if intent:
                events.append(dict(intent))
                capital_reference = str(
                    intent.get("external_reference") or ""
                ).strip() or None
        started_at_ms = int(time.time() * 1000)
        self.engine.begin_live_subscription(
            amount=amount,
            asset=asset,
            started_at_ms=started_at_ms,
            kind=kind,
            harvest_amount=(source_amount if kind == "harvest" else 0.0),
            source_asset=source_asset,
            source_amount=source_amount,
            source_transfer_amount=choice.source_transfer_amount,
            source_transfer_type=choice.source_wallet,
            source_collection_required=choice.source_collection_required,
            capital_reference=capital_reference,
        )
        events.extend(await self._advance_live_subscription_source())
        return events

    async def _advance_live_subscription_source(self) -> List[dict]:
        events: List[dict] = []
        stage = str(
            self.engine.profit_bucket.pending_subscription_stage or ""
        ).upper()
        if stage in SOURCE_COLLECTION_WAIT_STAGES:
            events.extend(await self._collect_pending_source())
            stage = str(
                self.engine.profit_bucket.pending_subscription_stage or ""
            ).upper()
            if stage in SOURCE_COLLECTION_WAIT_STAGES:
                return events
        if stage in {
            "SOURCE_COLLECTED",
            "TRANSFER_PENDING",
            "FUNDING_TRANSFER_PENDING",
        }:
            events.extend(await self._transfer_pending_source())
            stage = str(
                self.engine.profit_bucket.pending_subscription_stage or ""
            ).upper()
            if stage in SOURCE_TRANSFER_WAIT_STAGES:
                return events
        if stage == "TRANSFERRED":
            events.extend(await self._finish_live_subscription())
        return events

    async def _collect_pending_source(self) -> List[dict]:
        bucket = self.engine.profit_bucket
        stage = str(bucket.pending_subscription_stage or "").upper()
        if stage not in SOURCE_COLLECTION_WAIT_STAGES:
            return []
        asset = str(bucket.pending_source_asset or "").strip().upper()
        transfer_amount = self._floor_asset_amount(
            bucket.pending_source_transfer_amount
        )
        if asset not in SUPPORTED_RWUSD_SOURCE_ASSETS or transfer_amount <= 0:
            self.engine.reset_pending_live_subscription(
                "pending_source_collection_invalid"
            )
            return []
        try:
            balance = await self.client.get_margin_asset_balance(asset)
        except Exception as exc:
            self.engine.mark_live_subscription_stage(
                "SOURCE_COLLECTION_UNCERTAIN",
                block_reason=f"source_collection_balance_unavailable:{exc}",
            )
            return []
        transferable = self._floor_asset_amount(
            float((balance or {}).get("transferable_balance", 0) or 0)
        )
        if (
            stage == "SOURCE_COLLECTION_UNCERTAIN"
            and transferable + 1e-8 >= transfer_amount
        ):
            self.engine.mark_live_subscription_stage(
                "SOURCE_COLLECTED",
                block_reason="transfer_pending",
            )
            return []
        wallet_balance = self._floor_asset_amount(
            float((balance or {}).get("wallet_balance", 0) or 0)
        )
        if wallet_balance + 1e-8 < transfer_amount:
            self.engine.reset_pending_live_subscription(
                "source_collection_balance_insufficient"
            )
            return []
        try:
            await self.client.collect_pm_asset(asset=asset)
        except Exception as exc:
            if self._write_outcome_uncertain(exc):
                self.engine.mark_live_subscription_stage(
                    "SOURCE_COLLECTION_UNCERTAIN",
                    block_reason=f"source_collection_uncertain:{exc}",
                )
            else:
                self.engine.reset_pending_live_subscription(
                    f"source_collection_rejected:{exc}"
                )
            return []
        try:
            balance = await self.client.get_margin_asset_balance(asset)
        except Exception as exc:
            self.engine.mark_live_subscription_stage(
                "SOURCE_COLLECTION_UNCERTAIN",
                block_reason=f"source_collection_confirmation_unavailable:{exc}",
            )
            return []
        transferable = self._floor_asset_amount(
            float((balance or {}).get("transferable_balance", 0) or 0)
        )
        if transferable + 1e-8 < transfer_amount:
            self.engine.reset_pending_live_subscription(
                self._margin_locked_reason((asset,))
            )
            return []
        self.engine.mark_live_subscription_stage(
            "SOURCE_COLLECTED",
            block_reason="transfer_pending",
        )
        return [
            {
                "type": "rwusd_transfer",
                "action": "portfolio_margin_asset_collection",
                "asset": asset,
                "amount": transfer_amount,
                "simulated": False,
                "testnet": False,
            }
        ]

    async def _transfer_pending_source(self) -> List[dict]:
        bucket = self.engine.profit_bucket
        stage = str(bucket.pending_subscription_stage or "").upper()
        if stage not in {
            "SOURCE_COLLECTED",
            "TRANSFER_PENDING",
            "FUNDING_TRANSFER_PENDING",
        }:
            return []
        source_asset = str(
            bucket.pending_source_asset
            or bucket.pending_subscription_asset
            or ""
        ).strip().upper()
        transfer_amount = self._floor_asset_amount(
            bucket.pending_source_transfer_amount
        )
        if (
            source_asset not in SUPPORTED_RWUSD_SOURCE_ASSETS
            or transfer_amount <= 0
        ):
            self.engine.reset_pending_live_subscription(
                "pending_source_transfer_invalid"
            )
            return []
        funding_transfer = stage.startswith("FUNDING_")
        pending_stage = (
            "FUNDING_TRANSFER_PENDING"
            if funding_transfer
            else "TRANSFER_PENDING"
        )
        is_harvest_transfer = (
            not funding_transfer
            and str(bucket.pending_subscription_kind or "").strip().lower()
            == "harvest"
        )
        if (
            is_harvest_transfer
            and self.engine.has_active_harvest_orders()
        ):
            self.engine.mark_live_subscription_stage(
                pending_stage,
                block_reason="harvest_waiting_for_position_action",
            )
            return [
                {
                    "type": "rwusd_transfer_retry",
                    "action": "waiting_for_position_action",
                    "asset": source_asset,
                    "requested_amount": transfer_amount,
                    "reason": "harvest_waiting_for_position_action",
                    "simulated": False,
                    "testnet": self.is_testnet,
                }
            ]
        if stage != pending_stage:
            self.engine.mark_live_subscription_stage(
                pending_stage,
                block_reason="transfer_pending",
            )
        if is_harvest_transfer:
            if self.engine.harvest_transfer_retry_active():
                return [
                    {
                        "type": "rwusd_transfer_retry",
                        "action": "backoff",
                        "asset": source_asset,
                        "requested_amount": transfer_amount,
                        "retry_after_ms": (
                            bucket.transfer_retry_after_ms
                        ),
                        "reason": (
                            bucket.transfer_retry_reason
                            or "transfer_retry_after_rejection"
                        ),
                        "simulated": False,
                        "testnet": self.is_testnet,
                    }
                ]
            try:
                balance = await self.client.get_margin_asset_balance(
                    source_asset
                )
            except Exception as exc:
                reason = f"transfer_retry_waiting_for_capacity_read:{exc}"
                self.engine.reset_pending_live_subscription(reason)
                self.engine.defer_harvest_transfer(reason)
                return []
            available = self._floor_asset_amount(
                float((balance or {}).get("transferable_balance", 0) or 0)
            )
            if available <= 0:
                reason = "transfer_retry_waiting_for_portfolio_margin_capacity"
                self.engine.reset_pending_live_subscription(reason)
                self.engine.defer_harvest_transfer(reason)
                return [
                    {
                        "type": "rwusd_transfer_retry",
                        "action": "waiting_for_transfer_capacity",
                        "asset": source_asset,
                        "requested_amount": transfer_amount,
                        "available_amount": available,
                        "reason": reason,
                        "simulated": False,
                        "testnet": self.is_testnet,
                    }
                ]
            if available + 1e-8 < transfer_amount:
                headroom = max(
                    available
                    * float(
                        getattr(
                            self.config,
                            "LIVE_HARVEST_TRANSFER_HEADROOM_FRACTION",
                            0.001,
                        )
                    ),
                    float(
                        getattr(
                            self.config,
                            "LIVE_HARVEST_TRANSFER_HEADROOM_MIN_USD",
                            0.01,
                        )
                    ),
                )
                transfer_amount = self._floor_asset_amount(
                    available - headroom
                )
                if transfer_amount < float(self.config.MIN_SWEEP):
                    reason = (
                        "transfer_retry_waiting_for_portfolio_margin_capacity"
                    )
                    self.engine.reset_pending_live_subscription(reason)
                    self.engine.defer_harvest_transfer(reason)
                    return []
                bucket.pending_source_transfer_amount = transfer_amount
                bucket.pending_source_amount = min(
                    float(bucket.pending_source_amount or 0),
                    transfer_amount,
                )
                bucket.pending_subscription_amount = min(
                    float(bucket.pending_subscription_amount or 0),
                    transfer_amount,
                )
                bucket.pending_harvest_amount = min(
                    float(bucket.pending_harvest_amount or 0),
                    transfer_amount,
                )
                self.engine.persist()
        try:
            if funding_transfer:
                transfer = await self.client.transfer_funding_to_spot(
                    asset=source_asset,
                    amount=transfer_amount,
                )
                transfer_action = "funding_to_spot"
            else:
                transfer = await self.client.transfer_pm_to_spot(
                    asset=source_asset,
                    amount=transfer_amount,
                )
                transfer_action = "portfolio_margin_to_spot"
        except Exception as exc:
            is_transfer_capacity_rejection = (
                is_harvest_transfer
                and (
                    "-3020" in str(exc)
                    or "-21003" in str(exc)
                    or "exceeds max amount" in str(exc).lower()
                )
            )
            if is_transfer_capacity_rejection:
                reason = f"transfer_retry_after_rejection:{exc}"
                self.engine.reset_pending_live_subscription(reason)
                self.engine.defer_harvest_transfer(reason)
            elif self._write_outcome_uncertain(exc):
                self.engine.mark_live_subscription_stage(
                    (
                        "FUNDING_TRANSFER_UNCERTAIN"
                        if funding_transfer
                        else "TRANSFER_UNCERTAIN"
                    ),
                    block_reason=(
                        f"{'funding' if funding_transfer else 'portfolio_margin'}"
                        f"_transfer_uncertain:{exc}"
                    ),
                )
            else:
                self.engine.reset_pending_live_subscription(
                    f"transfer_rejected:{exc}"
                )
            if not funding_transfer:
                return [
                    {
                        "type": "rwusd_transfer_retry",
                        "action": "retry_scheduled",
                        "asset": source_asset,
                        "requested_amount": transfer_amount,
                        "reason": str(exc),
                        "simulated": False,
                        "testnet": self.is_testnet,
                    }
                ]
            return []
        self.engine.mark_live_subscription_stage(
            "TRANSFERRED",
            transfer_id=str(
                (transfer or {}).get("tranId")
                or (transfer or {}).get("clientTranId")
                or ""
            ),
            block_reason="rwusd_subscribe_pending",
        )
        self.engine.clear_harvest_transfer_retry()
        self.engine.persist()
        return [
            {
                "type": "rwusd_transfer",
                "action": transfer_action,
                "asset": source_asset,
                "amount": transfer_amount,
                "simulated": False,
                "testnet": False,
            }
        ]

    async def _finish_live_subscription(self) -> List[dict]:
        bucket = self.engine.profit_bucket
        stage = str(bucket.pending_subscription_stage or "").upper()
        if stage not in {"TRANSFERRED", "CONVERTED"}:
            return []
        amount = self._floor_asset_amount(
            bucket.pending_subscription_amount
        )
        asset = str(bucket.pending_subscription_asset or "").upper()
        source_asset = str(
            bucket.pending_source_asset or asset
        ).strip().upper()
        if amount <= 0 or asset not in SUPPORTED_RWUSD_SOURCE_ASSETS:
            self.engine.reset_pending_live_subscription(
                f"pending_subscription_invalid:{asset or 'missing_asset'}"
            )
            return []
        events: List[dict] = []
        is_harvest = (
            str(bucket.pending_subscription_kind or "").strip().lower()
            == "harvest"
        )
        # PM-to-Spot confirmation is the cash boundary for hedge recovery.
        # Subscription-history lag must not delay the restore order.
        if stage == "TRANSFERRED" and self._profit_restore_release_ready():
            return events
        if is_harvest:
            history_events, history_matched, history_available = (
                await self._reconcile_subscription_history()
            )
            events.extend(history_events)
            if history_matched:
                if bucket.pending_subscription_stage == "SUBSCRIBED":
                    events.extend(
                        await self._settle_live_subscription_collateral()
                    )
                return events
            if not history_available:
                return events
        if source_asset != asset and stage != "CONVERTED":
            conversion_started = bool(
                bucket.pending_conversion_order_id
                or bucket.pending_conversion_quote_id
            )
            events.extend(
                await (
                    self._reconcile_pending_conversion()
                    if conversion_started
                    else self._execute_pending_conversion()
                )
            )
            if (
                str(bucket.pending_subscription_stage or "").upper()
                != "CONVERTED"
            ):
                return events
            amount = self._floor_asset_amount(
                bucket.pending_subscription_amount
            )
        spot, quota, collateral = await asyncio.gather(
            self.client.get_spot_asset_balance(asset),
            self.client.get_rwusd_quota(),
            self.client.get_rwusd_collateral_balance(),
        )
        if float((spot or {}).get("free", 0) or 0) + 1e-8 < amount:
            self.engine.mark_live_subscription_stage(
                str(bucket.pending_subscription_stage or "TRANSFERRED"),
                block_reason="spot_balance_waiting_transfer",
            )
            return events
        eligible, reason = self._eligible_subscription_amount(
            quota=quota,
            asset=asset,
            requested_amount=amount,
        )
        if eligible + 1e-8 < amount:
            self.engine.mark_live_subscription_stage(
                str(bucket.pending_subscription_stage or "TRANSFERRED"),
                block_reason=reason,
            )
            return events
        self.engine.mark_live_subscription_stage(
            "SUBSCRIBE_PENDING",
            collateral_balance_before=(
                self._collateral_confirmation_balance(collateral)
            ),
            block_reason="subscribe_pending",
        )
        self.engine.update_exchange_rwusd_collateral_snapshot(collateral)
        try:
            transferable = (
                await self.client.get_rwusd_transferable_balance()
            )
        except Exception:
            transferable = None
        self.engine.update_exchange_rwusd_transferable_snapshot(transferable)
        try:
            response = await self.client.subscribe_rwusd(
                asset=asset,
                amount=amount,
            )
        except Exception as exc:
            if self._write_outcome_uncertain(exc):
                self.engine.mark_live_subscription_stage(
                    "SUBSCRIBE_UNCERTAIN",
                    block_reason=f"subscribe_uncertain:{exc}",
                )
            else:
                self.engine.mark_live_subscription_stage(
                    (
                        "CONVERTED"
                        if source_asset != asset
                        else "TRANSFERRED"
                    ),
                    block_reason=f"subscribe_rejected:{exc}",
                )
            return events
        received = float((response or {}).get("rwusdAmount", 0) or 0)
        if received <= 0:
            self.engine.mark_live_subscription_stage(
                "SUBSCRIBE_UNCERTAIN",
                block_reason="subscription_response_missing_rwusd_amount",
            )
            return events
        self.engine.mark_live_subscription_stage(
            "SUBSCRIBED",
            rwusd_amount=received,
            block_reason="rwusd_collateral_transfer_pending",
        )
        event = self._subscription_event(
            asset=asset,
            amount=amount,
            rwusd_amount=received,
            kind=str(bucket.pending_subscription_kind or "harvest"),
        )
        events.append(event)
        events.extend(await self._settle_live_subscription_collateral())
        return events

    async def _execute_pending_conversion(self) -> List[dict]:
        bucket = self.engine.profit_bucket
        source_asset = str(
            bucket.pending_source_asset or ""
        ).strip().upper()
        target_asset = str(
            bucket.pending_subscription_asset or ""
        ).strip().upper()
        source_amount = self._floor_asset_amount(
            bucket.pending_source_amount
        )
        if (
            source_asset not in SUPPORTED_RWUSD_SOURCE_ASSETS
            or target_asset not in SUPPORTED_RWUSD_SOURCE_ASSETS
            or source_asset == target_asset
            or source_amount <= 0
        ):
            self.engine.reset_pending_live_subscription(
                "pending_stablecoin_conversion_invalid"
            )
            return []
        spot = await self.client.get_spot_asset_balance(source_asset)
        if float((spot or {}).get("free", 0) or 0) + 1e-8 < source_amount:
            self.engine.mark_live_subscription_stage(
                "TRANSFERRED",
                block_reason="conversion_source_balance_waiting_transfer",
            )
            return []
        try:
            quote = await self.client.get_convert_quote(
                from_asset=source_asset,
                to_asset=target_asset,
                from_amount=source_amount,
            )
        except Exception as exc:
            self.engine.mark_live_subscription_stage(
                "TRANSFERRED",
                block_reason=f"conversion_quote_unavailable:{exc}",
            )
            return []
        quote_id = str((quote or {}).get("quoteId") or "").strip()
        target_amount = self._floor_asset_amount(
            float((quote or {}).get("toAmount", 0) or 0)
        )
        expires_at_ms = max(
            0,
            int((quote or {}).get("validTimestamp", 0) or 0),
        )
        if not quote_id or target_amount <= 0:
            self.engine.mark_live_subscription_stage(
                "TRANSFERRED",
                block_reason="conversion_quote_invalid",
            )
            return []
        try:
            _, _, target_minimum, target_maximum = (
                await self._conversion_limits(source_asset, target_asset)
            )
            quota = await self.client.get_rwusd_quota()
        except Exception as exc:
            self.engine.mark_live_subscription_stage(
                "TRANSFERRED",
                block_reason=f"conversion_preflight_unavailable:{exc}",
            )
            return []
        if (
            target_amount + 1e-12 < target_minimum
            or target_amount > target_maximum + 1e-12
        ):
            self.engine.mark_live_subscription_stage(
                "TRANSFERRED",
                block_reason="conversion_quote_outside_pair_limits",
            )
            return []
        subscription_amount, reason = self._eligible_subscription_amount(
            quota=quota,
            asset=target_asset,
            requested_amount=target_amount,
        )
        if subscription_amount <= 0:
            self.engine.mark_live_subscription_stage(
                "TRANSFERRED",
                block_reason=reason,
            )
            return []
        if subscription_amount + 1e-8 < target_amount:
            self.engine.mark_live_subscription_stage(
                "TRANSFERRED",
                block_reason="conversion_output_exceeds_rwusd_quota",
            )
            return []
        self.engine.mark_live_conversion_stage(
            "CONVERT_ACCEPT_PENDING",
            quote_id=quote_id,
            subscription_amount=target_amount,
            quote_expires_at_ms=expires_at_ms,
            block_reason="stablecoin_conversion_pending",
        )
        try:
            response = await self.client.accept_convert_quote(
                quote_id=quote_id
            )
        except Exception as exc:
            if self._write_outcome_uncertain(exc):
                self.engine.mark_live_conversion_stage(
                    "CONVERT_ACCEPT_UNCERTAIN",
                    block_reason=f"stablecoin_conversion_uncertain:{exc}",
                )
            else:
                self.engine.reset_live_conversion(
                    f"stablecoin_conversion_rejected:{exc}"
                )
            return []
        return self._apply_conversion_status(response)

    def _apply_conversion_status(self, payload: object) -> List[dict]:
        data = payload if isinstance(payload, dict) else {}
        status = str(
            data.get("orderStatus") or data.get("status") or ""
        ).strip().upper()
        order_id = str(data.get("orderId") or "").strip()
        if status == "SUCCESS":
            bucket = self.engine.profit_bucket
            source_asset = str(
                data.get("fromAsset")
                or bucket.pending_source_asset
                or ""
            ).strip().upper()
            target_asset = str(
                data.get("toAsset")
                or bucket.pending_subscription_asset
                or ""
            ).strip().upper()
            if (
                source_asset
                != str(bucket.pending_source_asset or "").strip().upper()
                or target_asset
                != str(
                    bucket.pending_subscription_asset or ""
                ).strip().upper()
            ):
                self.engine.reset_pending_live_subscription(
                    "stablecoin_conversion_asset_mismatch"
                )
                return []
            target_amount = self._floor_asset_amount(
                float(
                    data.get("toAmount")
                    or bucket.pending_subscription_amount
                    or 0
                )
            )
            if target_amount <= 0:
                self.engine.mark_live_conversion_stage(
                    "CONVERT_PROCESSING",
                    order_id=order_id,
                    block_reason="stablecoin_conversion_amount_pending",
                )
                return []
            self.engine.mark_live_conversion_stage(
                "CONVERTED",
                order_id=order_id,
                subscription_amount=target_amount,
                block_reason="rwusd_subscribe_pending",
            )
            return [
                {
                    "type": "rwusd_transfer",
                    "action": "stablecoin_convert",
                    "source_asset": source_asset,
                    "source_amount": self._floor_asset_amount(
                        bucket.pending_source_amount
                    ),
                    "asset": target_asset,
                    "amount": target_amount,
                    "quote_id": bucket.pending_conversion_quote_id,
                    "order_id": order_id,
                    "simulated": False,
                    "testnet": False,
                }
            ]
        if status in {
            "FAIL",
            "FAILED",
            "REJECTED",
            "EXPIRED",
            "CANCELED",
            "CANCELLED",
        }:
            self.engine.reset_live_conversion(
                f"stablecoin_conversion_{status.lower()}"
            )
            return []
        self.engine.mark_live_conversion_stage(
            "CONVERT_PROCESSING",
            order_id=order_id,
            block_reason="stablecoin_conversion_confirmation_pending",
        )
        return []

    async def _reconcile_pending_conversion(self) -> List[dict]:
        bucket = self.engine.profit_bucket
        order_id = str(
            bucket.pending_conversion_order_id or ""
        ).strip()
        quote_id = str(
            bucket.pending_conversion_quote_id or ""
        ).strip()
        if not order_id and not quote_id:
            self.engine.reset_live_conversion(
                "stablecoin_conversion_identifier_missing"
            )
            return []
        try:
            response = await self.client.get_convert_order_status(
                order_id=order_id or None,
                quote_id=None if order_id else quote_id,
            )
        except Exception as exc:
            expired = (
                bucket.pending_conversion_quote_expires_at_ms > 0
                and int(time.time() * 1000)
                > bucket.pending_conversion_quote_expires_at_ms + 2000
            )
            message = str(exc).lower()
            explicitly_missing = any(
                marker in message
                for marker in (
                    "not found",
                    "does not exist",
                    "quote expired",
                    "invalid quote",
                )
            )
            if expired and explicitly_missing:
                self.engine.reset_live_conversion(
                    "stablecoin_conversion_quote_expired"
                )
            else:
                self.engine.mark_live_conversion_stage(
                    str(
                        bucket.pending_subscription_stage
                        or "CONVERT_ACCEPT_UNCERTAIN"
                    ),
                    block_reason=(
                        f"stablecoin_conversion_status_unavailable:{exc}"
                    ),
                )
            return []
        return self._apply_conversion_status(response)

    async def _settle_live_subscription_collateral(self) -> List[dict]:
        bucket = self.engine.profit_bucket
        if bucket.pending_subscription_stage != "SUBSCRIBED":
            return []
        expected = self._floor_asset_amount(bucket.pending_rwusd_amount)
        if expected <= 0:
            self.engine.mark_live_subscription_stage(
                "SUBSCRIBE_UNCERTAIN",
                block_reason="pending_rwusd_amount_missing",
            )
            return []
        collateral = await self.client.get_rwusd_collateral_balance()
        current = self._collateral_confirmation_balance(collateral)
        baseline = max(
            0.0,
            bucket.pending_collateral_balance_before,
        )
        self.engine.update_exchange_rwusd_collateral_snapshot(collateral)
        if current + 1e-8 >= baseline + expected:
            return await self._complete_live_subscription_collateral(
                collateral=collateral,
                collateralization_mode="automatic",
            )
        credited = max(0.0, current - baseline)
        remaining = self._floor_asset_amount(expected - credited)
        try:
            transferable_payload = (
                await self.client.get_rwusd_transferable_balance()
            )
        except Exception as exc:
            self.engine.update_exchange_rwusd_transferable_snapshot(None)
            self.engine.mark_live_subscription_stage(
                "SUBSCRIBED",
                block_reason=f"rwusd_transferable_balance_unavailable:{exc}",
            )
            return []
        self.engine.update_exchange_rwusd_transferable_snapshot(
            transferable_payload
        )
        transferable = self._transferable_rwusd_balance(
            transferable_payload
        )
        if transferable + 1e-8 < remaining:
            self.engine.mark_live_subscription_stage(
                "SUBSCRIBED",
                block_reason="rwusd_collateral_settlement_pending",
            )
            return []
        self.engine.mark_live_subscription_stage(
            "COLLATERAL_TRANSFER_PENDING",
            block_reason="rwusd_collateral_transfer_pending",
        )
        try:
            response = await self.client.transfer_rwusd_to_portfolio_margin(
                amount=remaining
            )
        except Exception as exc:
            if self._write_outcome_uncertain(exc):
                self.engine.mark_live_subscription_stage(
                    "COLLATERAL_TRANSFER_UNCERTAIN",
                    block_reason=f"collateral_transfer_uncertain:{exc}",
                )
            else:
                self.engine.mark_live_subscription_stage(
                    "SUBSCRIBED",
                    block_reason=f"collateral_transfer_rejected:{exc}",
                )
            return []
        self.engine.mark_live_subscription_stage(
            "COLLATERAL_TRANSFERRED",
            collateral_transfer_id=str(
                (response or {}).get("tranId")
                or (response or {}).get("clientTranId")
                or ""
            ),
            block_reason="rwusd_collateral_confirmation_pending",
        )
        return await self._confirm_live_collateral_transfer()

    async def _complete_live_subscription_collateral(
        self,
        *,
        collateral: object,
        collateralization_mode: str,
    ) -> List[dict]:
        expected = self._floor_asset_amount(
            self.engine.profit_bucket.pending_rwusd_amount
        )
        event = self.engine.confirm_live_subscription(
            rwusd_amount=expected,
            collateralization_mode=collateralization_mode,
        )
        current = self._collateral_confirmation_balance(collateral)
        event["collateral_balance"] = current
        self.engine.update_exchange_rwusd_collateral_snapshot(collateral)
        snapshot_event = await self._refresh_rwusd_account_snapshot_best_effort()
        try:
            transferable = (
                await self.client.get_rwusd_transferable_balance()
            )
        except Exception:
            transferable = None
        self.engine.update_exchange_rwusd_transferable_snapshot(transferable)
        events = [event]
        if snapshot_event is not None:
            events.append(snapshot_event)
        return events

    async def _confirm_live_collateral_transfer(self) -> List[dict]:
        bucket = self.engine.profit_bucket
        stage = str(bucket.pending_subscription_stage or "").upper()
        if stage not in COLLATERAL_CONFIRMATION_STAGES:
            return []
        expected = self._floor_asset_amount(bucket.pending_rwusd_amount)
        baseline = max(0.0, bucket.pending_collateral_balance_before)
        collateral = await self.client.get_rwusd_collateral_balance()
        current = self._collateral_confirmation_balance(collateral)
        if expected <= 0 or current + 1e-8 < baseline + expected:
            next_stage = (
                "COLLATERAL_TRANSFER_UNCERTAIN"
                if stage == "COLLATERAL_TRANSFER_PENDING"
                else stage
            )
            self.engine.mark_live_subscription_stage(
                next_stage,
                block_reason="rwusd_collateral_confirmation_pending",
            )
            return []
        return await self._complete_live_subscription_collateral(
            collateral=collateral,
            collateralization_mode="explicit_transfer",
        )

    async def reconcile_live_subscription(self) -> List[dict]:
        if not self.live_rwusd:
            return []
        bucket = self.engine.profit_bucket
        stage = str(bucket.pending_subscription_stage or "").upper()
        if not stage:
            return []
        if stage in EXISTING_RWUSD_CONFIRMATION_STAGES:
            return await self._confirm_existing_rwusd_collateral_transfer()
        pending_asset = str(
            bucket.pending_subscription_asset or ""
        ).strip().upper()
        if pending_asset not in SUPPORTED_RWUSD_SOURCE_ASSETS:
            self.engine.reset_pending_live_subscription(
                f"pending_subscription_invalid:{pending_asset or 'missing_asset'}"
            )
            return []
        now_ms = int(time.time() * 1000)
        start_ms = max(
            0,
            int(bucket.pending_subscription_started_at_ms or 0) - 60000,
        )
        events: List[dict] = []
        if stage in SOURCE_COLLECTION_WAIT_STAGES:
            events.extend(await self._collect_pending_source())
            stage = str(
                bucket.pending_subscription_stage or ""
            ).upper()
            if stage in SOURCE_COLLECTION_WAIT_STAGES:
                return events
            if stage == "SOURCE_COLLECTED":
                events.extend(await self._transfer_pending_source())
                stage = str(
                    bucket.pending_subscription_stage or ""
                ).upper()
        if stage in SOURCE_TRANSFER_WAIT_STAGES:
            funding_transfer = stage.startswith("FUNDING_")
            history_loader = (
                self.client.get_funding_to_spot_transfer_history
                if funding_transfer
                else self.client.get_pm_to_spot_transfer_history
            )
            history = await history_loader(
                start_time_ms=start_ms,
                end_time_ms=now_ms,
            )
            matched = self._matching_transfer_row(
                history,
                transfer_type=(
                    "FUNDING_MAIN"
                    if funding_transfer
                    else (
                        "PORTFOLIO_MARGIN_MAIN",
                        "MARGIN_MAIN",
                    )
                ),
            )
            if matched is None:
                self.engine.mark_live_subscription_stage(
                    (
                        "FUNDING_TRANSFER_UNCERTAIN"
                        if funding_transfer
                        else "TRANSFER_UNCERTAIN"
                    ),
                    block_reason=(
                        "funding_transfer_history_pending"
                        if funding_transfer
                        else "transfer_history_pending"
                    ),
                )
                return []
            self.engine.mark_live_subscription_stage(
                "TRANSFERRED",
                transfer_id=str(
                    matched.get("tranId")
                    or matched.get("clientTranId")
                    or ""
                ),
                block_reason="rwusd_subscribe_pending",
            )
            stage = "TRANSFERRED"
        if stage in CONVERSION_WAIT_STAGES:
            events.extend(await self._reconcile_pending_conversion())
            stage = str(
                bucket.pending_subscription_stage or ""
            ).upper()
            if stage in CONVERSION_WAIT_STAGES:
                return events
        if stage in SUBSCRIPTION_WAIT_STAGES:
            history_events, _, _ = await self._reconcile_subscription_history(
                required=True,
            )
            events.extend(history_events)
            return events
        if stage in COLLATERAL_CONFIRMATION_STAGES:
            events.extend(await self._confirm_live_collateral_transfer())
        return events

    async def _reconcile_subscription_history(
        self,
        *,
        required: bool = False,
    ) -> tuple[List[dict], bool, bool]:
        bucket = self.engine.profit_bucket
        now_ms = int(time.time() * 1000)
        start_ms = max(
            0,
            int(bucket.pending_subscription_started_at_ms or 0) - 60000,
        )
        try:
            history = await self.client.get_rwusd_subscription_history(
                start_time_ms=start_ms,
                end_time_ms=now_ms,
                asset=str(bucket.pending_subscription_asset or "").upper(),
            )
        except Exception as exc:
            self.engine.mark_live_subscription_stage(
                (
                    "SUBSCRIBE_UNCERTAIN"
                    if required
                    else str(
                        bucket.pending_subscription_stage or "TRANSFERRED"
                    ).upper()
                ),
                block_reason=f"subscription_history_unavailable:{exc}",
            )
            return [], False, False
        matched = self._matching_subscription_row(
            history,
            earliest_time_ms=max(
                0,
                int(bucket.pending_subscription_started_at_ms or 0) - 5000,
            ),
        )
        if matched is None:
            if required:
                self.engine.mark_live_subscription_stage(
                    "SUBSCRIBE_UNCERTAIN",
                    block_reason="subscription_history_pending",
                )
            return [], False, True
        received = float(
            matched.get("rwusdAmount")
            or matched.get("receiveAmount")
            or 0
        )
        if received <= 0:
            self.engine.mark_live_subscription_stage(
                "SUBSCRIBE_UNCERTAIN",
                block_reason="subscription_history_missing_rwusd_amount",
            )
            return [], False, False
        asset = str(bucket.pending_subscription_asset or "").upper()
        amount = bucket.pending_subscription_amount
        kind = str(bucket.pending_subscription_kind or "harvest")
        self.engine.mark_live_subscription_stage(
            "SUBSCRIBED",
            rwusd_amount=received,
            block_reason="rwusd_collateral_transfer_pending",
        )
        events = [
            self._subscription_event(
                asset=asset,
                amount=amount,
                rwusd_amount=received,
                kind=kind,
            )
        ]
        return events, True, True

    async def _refresh_rwusd_account_snapshot_best_effort(
        self,
    ) -> Optional[dict]:
        try:
            self.engine.update_exchange_rwusd_snapshot(
                await self.client.get_rwusd_account()
            )
        except Exception as exc:
            return {
                "type": "rwusd_status",
                "action": "rwusd_account_snapshot_unavailable",
                "reason": str(exc),
                "observation_only": True,
                "state_persisted": True,
                "simulated": False,
            }
        return None

    def _profit_restore_release_ready(self) -> bool:
        return bool(
            self.engine.state.phase == StrategyPhase.PROFIT_SWEEPING
            and str(
                self.engine.profit_bucket.pending_subscription_kind or ""
            ).strip().lower()
            == "harvest"
            and self.engine.state.pending_rebalance_side is not None
            and self.engine.state.pending_restore_quantity > 1e-10
        )

    def _remaining_initial_collateralization(
        self,
        *,
        requested_amount: float,
        collateral_amount: float = 0.0,
    ) -> float:
        bucket = self.engine.profit_bucket
        existing_rwusd = max(
            0.0,
            bucket.initial_collateralization_existing_rwusd_amount,
        )
        subscribed_rwusd = max(
            0.0,
            bucket.initial_collateralization_rwusd - existing_rwusd,
        )
        existing_collateral = max(
            0.0,
            float(collateral_amount or 0)
            - subscribed_rwusd
            - existing_rwusd,
        )
        satisfied = (
            max(0.0, bucket.initial_collateralization_source_amount)
            + existing_rwusd
            + existing_collateral
        )
        return self._floor_asset_amount(
            max(0.0, float(requested_amount or 0) - satisfied)
        )

    async def ensure_initial_collateralization(
        self,
        portfolio: PortfolioSnapshot,
        *,
        requested_amount: float,
    ) -> tuple[bool, List[dict]]:
        bucket = self.engine.profit_bucket
        if bucket.initial_collateralization_completed:
            return True, []

        events: List[dict] = []
        requested_total = self._floor_asset_amount(requested_amount)
        minimum = float(getattr(self.config, "MIN_SWEEP", 1.0))
        if requested_total + 1e-12 < minimum:
            self.engine.block_initial_collateralization(
                "initial_rwusd_requested_capital_below_minimum"
            )
            return False, events
        requested = self._remaining_initial_collateralization(
            requested_amount=requested_total,
        )
        if (
            self.live_rwusd
            and str(bucket.pending_subscription_stage or "").upper()
            in EXISTING_RWUSD_CONFIRMATION_STAGES
        ):
            events.extend(
                await self._confirm_existing_rwusd_collateral_transfer()
            )
            if bucket.pending_subscription_stage:
                return False, events
        elif self.live_rwusd and bucket.pending_subscription_stage:
            events.extend(
                await self.execute_live_subscription(
                    portfolio,
                    kind=str(
                        bucket.pending_subscription_kind or "initial"
                    ),
                    requested_amount=0.0,
                )
            )
            if bucket.pending_subscription_stage:
                return False, events

        requested = self._remaining_initial_collateralization(
            requested_amount=requested_total,
        )
        if self.live_rwusd and requested + 1e-12 < minimum:
            events.append(
                self.engine.complete_initial_collateralization(
                    mode="mainnet_rwusd_confirmed_resume",
                    rwusd_amount=bucket.initial_collateralization_rwusd,
                )
            )
            return True, events

        has_exposure = (
            self.engine.state.long_quantity > 1e-12
            or self.engine.state.short_quantity > 1e-12
        )
        if has_exposure:
            mode = (
                "testnet_existing_exposure_adopted"
                if self.is_testnet
                else "mainnet_existing_exposure_adopted"
            )
            events.append(
                self.engine.complete_initial_collateralization(mode=mode)
            )
            return True, events

        if self.engine.lifecycle.active_orders:
            events.extend(await self._cancel_managed_active_orders())
            self.engine.block_initial_collateralization(
                "initial_rwusd_waiting_managed_order_cancellation"
            )
            return False, events

        if not self.live_rwusd:
            asset = self._source_assets[0]
            has_legacy_state = (
                self.engine.state.cycle_id > 0
                or bucket.rwusd_principal > 0
                or bucket.exchange_trade_count > 0
            )
            if has_legacy_state:
                events.append(
                    self.engine.complete_initial_collateralization(
                        mode="testnet_existing_state_adopted"
                    )
                )
                return True, events
            balance = await self.client.get_margin_asset_balance(asset)
            available = self._floor_asset_amount(
                float((balance or {}).get("wallet_balance", 0) or 0)
            )
            if available + 1e-12 < requested:
                self.engine.block_initial_collateralization(
                    "initial_rwusd_source_balance_below_requested_capital"
                )
                return False, events
            events.append(
                self.engine.complete_shadow_initial_collateralization(
                    amount=requested,
                    mode="testnet_shadow_direct_collateral",
                    source_asset=asset,
                )
            )
            return True, events

        try:
            (
                collateral,
                quota,
                rwusd_account,
                transferable_rwusd,
            ) = await asyncio.gather(
                self.client.get_rwusd_collateral_balance(),
                self.client.get_rwusd_quota(),
                self.client.get_rwusd_account(),
                self.client.get_rwusd_transferable_balance(),
            )
        except Exception as exc:
            self.engine.update_exchange_rwusd_transferable_snapshot(None)
            self.engine.block_initial_collateralization(
                f"rwusd_initial_preflight_unavailable:{exc}"
            )
            return False, events
        self.engine.update_exchange_rwusd_collateral_snapshot(collateral)
        self.engine.update_exchange_rwusd_snapshot(rwusd_account)
        self.engine.update_exchange_rwusd_transferable_snapshot(
            transferable_rwusd
        )
        collateral_amount = self._collateral_confirmation_balance(
            collateral
        )
        requested = self._remaining_initial_collateralization(
            requested_amount=requested_total,
            collateral_amount=collateral_amount,
        )
        if requested + 1e-12 < minimum:
            events.append(
                self.engine.complete_initial_collateralization(
                    mode="mainnet_rwusd_direct_collateral",
                    rwusd_amount=collateral_amount,
                )
            )
            return True, events
        transferable_rwusd_amount = self._floor_asset_amount(
            self._transferable_rwusd_balance(transferable_rwusd)
        )
        if transferable_rwusd_amount + 1e-12 >= minimum:
            transfer_amount = min(
                requested,
                transferable_rwusd_amount,
            )
            events.extend(
                await self._transfer_existing_rwusd_to_collateral(
                    amount=transfer_amount,
                    collateral_balance_before=collateral_amount,
                )
            )
            if bucket.pending_subscription_stage:
                return False, events
            settled_collateral = max(
                collateral_amount,
                max(
                    (
                        float(event.get("collateral_balance", 0) or 0)
                        for event in events
                    ),
                    default=0.0,
                ),
            )
            requested = self._remaining_initial_collateralization(
                requested_amount=requested_total,
                collateral_amount=settled_collateral,
            )
            if requested + 1e-12 < minimum:
                events.append(
                    self.engine.complete_initial_collateralization(
                        mode="mainnet_rwusd_explicit_transfer",
                        rwusd_amount=settled_collateral,
                    )
                )
                return True, events
        withdrawable = (
            portfolio.withdrawable_balance
            if portfolio.withdrawable_official
            else None
        )
        try:
            choice = await self._select_subscription_source(
                quota=quota,
                requested_amount=requested,
                withdrawable=withdrawable,
                include_spot=True,
                include_funding=True,
            )
        except Exception as exc:
            self.engine.block_initial_collateralization(
                f"rwusd_initial_source_balance_unavailable:{exc}"
            )
            return False, events
        if not choice.asset or choice.amount <= 0:
            self.engine.block_initial_collateralization(
                choice.reason
                or "initial_rwusd_source_and_collateral_unavailable"
            )
            return False, events

        events.extend(
            await self._execute_selected_live_subscription(
                choice=choice,
                kind="initial",
            )
        )
        if bucket.pending_subscription_stage:
            return False, events

        try:
            collateral, quota = await asyncio.gather(
                self.client.get_rwusd_collateral_balance(),
                self.client.get_rwusd_quota(),
            )
        except Exception as exc:
            self.engine.block_initial_collateralization(
                f"rwusd_initial_confirmation_unavailable:{exc}"
            )
            return False, events
        self.engine.update_exchange_rwusd_collateral_snapshot(collateral)
        collateral_amount = self._collateral_confirmation_balance(
            collateral
        )
        requested = self._remaining_initial_collateralization(
            requested_amount=requested_total,
            collateral_amount=collateral_amount,
        )
        if requested + 1e-12 < minimum:
            events.append(
                self.engine.complete_initial_collateralization(
                    mode="mainnet_rwusd_direct_collateral",
                    rwusd_amount=collateral_amount,
                )
            )
            return True, events
        try:
            remaining_choice = await self._select_subscription_source(
                quota=quota,
                requested_amount=requested,
                withdrawable=withdrawable,
                include_spot=True,
                include_funding=True,
            )
        except Exception as exc:
            self.engine.block_initial_collateralization(
                f"rwusd_initial_remainder_unavailable:{exc}"
            )
            return False, events
        self.engine.block_initial_collateralization(
            remaining_choice.reason
            or "initial_rwusd_subscription_remainder_pending"
        )
        return False, events

    async def _transfer_existing_rwusd_to_collateral(
        self,
        *,
        amount: float,
        collateral_balance_before: float,
        kind: str = "initial_existing_rwusd",
        capital_intent_callback: Optional[Callable[..., object]] = None,
    ) -> List[dict]:
        intent_events: List[dict] = []
        capital_reference = None
        contribution_amount = amount
        if kind == "capital_existing_rwusd":
            collateralized_interest = max(
                0.0,
                self.engine.profit_bucket.exchange_rwusd_collateral_amount
                - self.engine.profit_bucket.rwusd_principal,
            )
            uncollateralized_interest = max(
                0.0,
                self.engine.profit_bucket.exchange_rwusd_total_profit
                - collateralized_interest,
            )
            contribution_amount = max(
                0.0,
                amount - min(amount, uncollateralized_interest),
            )
        if (
            kind == "capital_existing_rwusd"
            and capital_intent_callback is not None
            and contribution_amount > 0
        ):
            intent = capital_intent_callback(
                amount=contribution_amount,
                source_asset="RWUSD",
                capital_contribution_before=(
                    self.engine.profit_bucket.capital_contribution_total
                ),
            )
            if inspect.isawaitable(intent):
                intent = await intent
            if intent:
                intent_events.append(dict(intent))
                capital_reference = str(
                    intent.get("external_reference") or ""
                ).strip() or None
        self.engine.begin_existing_rwusd_collateral_transfer(
            rwusd_amount=amount,
            collateral_balance_before=collateral_balance_before,
            started_at_ms=int(time.time() * 1000),
            kind=kind,
            capital_reference=capital_reference,
        )
        try:
            response = await self.client.transfer_rwusd_to_portfolio_margin(
                amount=amount
            )
        except Exception as exc:
            if self._write_outcome_uncertain(exc):
                self.engine.mark_live_subscription_stage(
                    "EXISTING_RWUSD_COLLATERAL_TRANSFER_UNCERTAIN",
                    block_reason=(
                        f"existing_rwusd_collateral_transfer_uncertain:{exc}"
                    ),
                )
            else:
                self.engine.reset_pending_live_subscription(
                    f"existing_rwusd_collateral_transfer_rejected:{exc}"
                )
            return intent_events
        self.engine.mark_live_subscription_stage(
            "EXISTING_RWUSD_COLLATERAL_TRANSFERRED",
            collateral_transfer_id=str(
                (response or {}).get("tranId")
                or (response or {}).get("clientTranId")
                or ""
            ),
            block_reason="existing_rwusd_collateral_confirmation_pending",
        )
        return intent_events + await (
            self._confirm_existing_rwusd_collateral_transfer()
        )

    async def process_incremental_collateralization(
        self,
        portfolio: PortfolioSnapshot,
        *,
        capital_intent_callback: Optional[Callable[..., object]] = None,
        margin_reserves: Optional[Dict[str, float]] = None,
    ) -> List[dict]:
        bucket = self.engine.profit_bucket
        self._capital_target_expansion_ready = not self.live_rwusd
        if not self.live_rwusd or not bucket.initial_collateralization_completed:
            return []

        pending_kind = str(
            bucket.pending_subscription_kind or "capital"
        ).strip().lower()
        if bucket.pending_subscription_stage:
            if pending_kind not in {
                "capital",
                "capital_existing_rwusd",
            }:
                return []
            events = await self.execute_live_subscription(
                portfolio,
                kind="capital",
                requested_amount=0.0,
            )
            if any(event.get("collateralized") for event in events):
                self._last_capital_scan_monotonic = None
            return events

        if self.engine.state.phase in {
            StrategyPhase.TAKING_PROFIT,
            StrategyPhase.PROFIT_SWEEPING,
            StrategyPhase.REBALANCING,
        }:
            return []

        now = time.monotonic()
        interval = max(
            5.0,
            float(
                getattr(
                    self.config,
                    "CAPITAL_SCAN_INTERVAL_SEC",
                    30.0,
                )
            ),
        )
        if (
            self._last_capital_scan_monotonic is not None
            and now - self._last_capital_scan_monotonic < interval
        ):
            return []
        self._last_capital_scan_monotonic = now

        try:
            quota, transferable_rwusd = await asyncio.gather(
                self.client.get_rwusd_quota(),
                self.client.get_rwusd_transferable_balance(),
            )
            transferable_rwusd_amount = self._floor_asset_amount(
                self._transferable_rwusd_balance(transferable_rwusd)
            )
            self.engine.update_exchange_rwusd_transferable_snapshot(
                transferable_rwusd
            )
            minimum = float(getattr(self.config, "MIN_SWEEP", 1.0))
            if transferable_rwusd_amount + 1e-12 >= minimum:
                collateral = await self.client.get_rwusd_collateral_balance()
                self.engine.update_exchange_rwusd_collateral_snapshot(
                    collateral
                )
                events = await self._transfer_existing_rwusd_to_collateral(
                    amount=transferable_rwusd_amount,
                    collateral_balance_before=(
                        self._collateral_confirmation_balance(collateral)
                    ),
                    kind="capital_existing_rwusd",
                    capital_intent_callback=capital_intent_callback,
                )
                if any(event.get("collateralized") for event in events):
                    self._last_capital_scan_monotonic = None
                return events

            choice = await self._select_subscription_source(
                quota=quota,
                requested_amount=0.0,
                withdrawable=None,
                include_margin=True,
                include_spot=True,
                include_funding=True,
                margin_reserves=(
                    margin_reserves
                    if margin_reserves is not None
                    else self._capital_margin_reserves()
                ),
            )
        except Exception as exc:
            return [
                {
                    "type": "rwusd_capital_scan",
                    "action": "scan_unavailable",
                    "reason": str(exc),
                    "simulated": False,
                }
            ]

        if not choice.asset or choice.amount <= 0:
            self._capital_target_expansion_ready = (
                self._source_choice_is_exhausted(choice)
                and choice.reason != "rwusd_source_not_withdrawable"
                and not choice.reason.startswith(
                    RWUSD_MARGIN_LOCKED_BLOCK_REASON
                )
            )
            return []
        events = await self._execute_selected_live_subscription(
            choice=choice,
            kind="capital",
            capital_intent_callback=capital_intent_callback,
        )
        if any(event.get("collateralized") for event in events):
            self._last_capital_scan_monotonic = None
        return events

    def _capital_margin_reserves(self) -> Dict[str, float]:
        reserved = max(
            self.engine.profit_bucket.harvest_buffer,
            self.engine.profit_bucket.realized_pnl_available_for_deposit,
        ) + max(0.0, self.engine.profit_bucket.unsettled_harvest_pnl)
        if reserved <= 0:
            return {}
        symbol = str(
            self.engine.state.selected_symbol or ""
        ).strip().upper()
        asset = next(
            (
                candidate
                for candidate in self._source_assets
                if symbol.endswith(candidate)
            ),
            self._source_assets[0],
        )
        return {asset: reserved}

    async def _confirm_existing_rwusd_collateral_transfer(
        self,
    ) -> List[dict]:
        bucket = self.engine.profit_bucket
        expected = self._floor_asset_amount(bucket.pending_rwusd_amount)
        baseline = max(0.0, bucket.pending_collateral_balance_before)
        collateral = await self.client.get_rwusd_collateral_balance()
        current = self._collateral_confirmation_balance(collateral)
        if expected <= 0 or current + 1e-8 < baseline + expected:
            stage = str(bucket.pending_subscription_stage or "").upper()
            if stage == "EXISTING_RWUSD_COLLATERAL_TRANSFER_PENDING":
                stage = "EXISTING_RWUSD_COLLATERAL_TRANSFER_UNCERTAIN"
            self.engine.mark_live_subscription_stage(
                stage,
                block_reason="existing_rwusd_collateral_confirmation_pending",
            )
            return []
        event = self.engine.confirm_existing_rwusd_collateral_transfer()
        event["collateral_balance"] = current
        self.engine.update_exchange_rwusd_collateral_snapshot(collateral)
        snapshot_event = await self._refresh_rwusd_account_snapshot_best_effort()
        try:
            transferable = (
                await self.client.get_rwusd_transferable_balance()
            )
        except Exception:
            transferable = None
        self.engine.update_exchange_rwusd_transferable_snapshot(transferable)
        events = [event]
        if snapshot_event is not None:
            events.append(snapshot_event)
        return events

    async def manual_flatten(self, reference_price: float) -> dict:
        await self.validate_runtime_isolation()
        events = await self._cancel_managed_active_orders()
        await self.reconcile_positions()
        events.extend(self.engine.plan_external_manual_flatten(reference_price))
        events.extend(
            await self.execute_pending_orders(reference_price=reference_price)
        )
        await self.reconcile_positions()
        completed = self.engine.finalize_external_manual_flatten()
        environment = "Testnet" if self.is_testnet else "Portfolio Margin mainnet"
        return {
            "success": completed,
            "message": (
                f"RWUSD Binance {environment} positions flattened"
                if completed
                else f"RWUSD Binance {environment} flatten is awaiting confirmation"
            ),
            "testnet": self.is_testnet,
            "simulated": False,
            "events": events,
        }

    async def advance_customer_exit(
        self,
        *,
        finalize_flat: bool = True,
    ) -> List[dict]:
        return await self._exit_execution.advance_customer_exit(
            finalize_flat=finalize_flat
        )

    async def advance_hard_risk_reduction(self) -> List[dict]:
        return await self._exit_execution.advance_hard_risk_reduction()

    def _order_allowed_by_current_risk(self, purpose: object) -> bool:
        normalized = str(purpose or "").strip()
        if normalized in LEGACY_AUTOMATIC_REDUCTION_PURPOSES:
            return False
        if normalized in (
            EMERGENCY_MARKET_PURPOSES
            | CUSTOMER_EXIT_LIMIT_PURPOSES
        ):
            return True
        decision = self.engine.last_risk_decision
        if decision.hard_risk:
            return normalized in HARD_RISK_ALLOWED_PURPOSES
        if decision.block_expansion and normalized in PAIR_BUILD_PURPOSES:
            return False
        if (
            normalized == HEDGE_REPAIR_PAIR_PURPOSE
            and decision.block_expansion
        ):
            return decision.allows_hedge_repair_pair
        if decision.block_harvest and normalized == "take_profit":
            return False
        return True

    async def cancel_risk_blocked_orders(
        self,
        *,
        reference_price: float,
    ) -> List[dict]:
        blocked = [
            order
            for order in self.engine.lifecycle.active_orders
            if not self._order_allowed_by_current_risk(order.purpose)
        ]
        if not blocked:
            decision = self.engine.last_risk_decision
            if decision.block_expansion or decision.block_harvest:
                return self.engine.plan_pending_risk_recovery(
                    reference_price
                )
            return []
        legacy_only = all(
            order.purpose in LEGACY_AUTOMATIC_REDUCTION_PURPOSES
            for order in blocked
        )
        events: List[dict] = [
            {
                "type": "exchange_order_cancel_requested",
                "action": (
                    "legacy_automatic_reduction_cancel"
                    if legacy_only
                    else "risk_blocked_order_cancel"
                ),
                "reason": self.engine.last_risk_decision.reason,
                "order_ids": [order.order_id for order in blocked],
                "automatic_reduction": False,
                "simulated": False,
                "testnet": self.is_testnet,
            }
        ]
        events.extend(await self._cancel_orders(blocked))
        await self.reconcile_positions()
        events.extend(
            self.engine.plan_pending_risk_recovery(
                reference_price
            )
        )
        return events

    async def _cancel_managed_active_orders(
        self,
        *,
        allowed_purposes: Optional[Set[str]] = None,
    ) -> List[dict]:
        events = await self.reconcile_active_orders()
        for order in list(self.engine.lifecycle.active_orders):
            if allowed_purposes and order.purpose in allowed_purposes:
                continue
            if not order.external_client_order_id:
                self.engine.apply_external_order_update(
                    order_id=order.order_id,
                    external_order_id=None,
                    external_status="CANCELED",
                    cumulative_filled_quantity=order.cumulative_filled_quantity,
                    average_fill_price=order.price,
                )
                continue
            if not order.external_order_id:
                raise RuntimeError(
                    "RWUSD flatten blocked: an order is still "
                    "awaiting Binance confirmation"
                )
            response = await self.client.cancel_um_order(
                symbol=self.engine.state.selected_symbol,
                order_id=order.external_order_id,
            )
            events.extend(
                self._apply_order_response(
                    order.order_id,
                    response,
                    cancellation_requested=True,
                )
            )
        return events

    def _cancel_unsubmitted_orders(self) -> None:
        for order in list(self.engine.lifecycle.active_orders):
            if order.external_client_order_id:
                continue
            self.engine.apply_external_order_update(
                order_id=order.order_id,
                external_order_id=None,
                external_status="CANCELED",
                cumulative_filled_quantity=order.cumulative_filled_quantity,
                average_fill_price=order.price,
            )

    async def _resolve_submission_exception(
        self,
        *,
        order_id: str,
        symbol: str,
        client_order_id: str,
        error: Exception,
    ) -> List[dict]:
        try:
            response = await self.client.get_um_order(
                symbol=symbol,
                client_order_id=client_order_id,
            )
            return self._apply_order_response(order_id, response)
        except Exception as query_error:
            combined = f"submit={error}; query={query_error}"
            if self._is_order_not_found(query_error):
                return self.engine.record_external_order_not_found(
                    order_id,
                    combined,
                )
            return self.engine.record_external_order_error(
                order_id,
                combined,
                definitive_rejection=False,
            )

    def _apply_order_response(
        self,
        order_id: str,
        response: dict,
        *,
        cancellation_requested: bool = False,
    ) -> List[dict]:
        status = str(
            response.get("status")
            or (response.get("raw") or {}).get("status")
            or "NEW"
        ).upper()
        executed_quantity = float(
            response.get("executed_quantity")
            if response.get("executed_quantity") is not None
            else response.get("executedQty", 0)
            or 0
        )
        order = self.engine.lifecycle.get_order(order_id)
        if (
            order is not None
            and order.maker_only
            and status in {"REJECTED", "EXPIRED", "CANCELED"}
            and executed_quantity <= 1e-12
        ):
            if cancellation_requested and status == "CANCELED":
                return self.engine.apply_external_order_update(
                    order_id=order_id,
                    external_order_id=(
                        response.get("order_id")
                        or (response.get("raw") or {}).get("orderId")
                    ),
                    external_status=status,
                    cumulative_filled_quantity=executed_quantity,
                    average_fill_price=(
                        response.get("average_price")
                        if response.get("average_price") is not None
                        else response.get("avgPrice", 0)
                    ),
                )
            return self.engine.record_external_maker_requote(
                order_id=order_id,
                external_order_id=(
                    response.get("order_id")
                    or (response.get("raw") or {}).get("orderId")
                ),
                error=f"Binance returned {status} with zero maker fill",
            )
        if (
            status in {"REJECTED", "EXPIRED", "CANCELED"}
            and executed_quantity <= 1e-12
        ) or (status == "FILLED" and executed_quantity <= 1e-12):
            return self.engine.record_external_order_error(
                order_id,
                f"Binance returned {status} with zero fill",
                definitive_rejection=True,
            )
        return self.engine.apply_external_order_update(
            order_id=order_id,
            external_order_id=(
                response.get("order_id")
                or (response.get("raw") or {}).get("orderId")
            ),
            external_status=status,
            cumulative_filled_quantity=executed_quantity,
            average_fill_price=(
                response.get("average_price")
                if response.get("average_price") is not None
                else response.get("avgPrice", 0)
            ),
        )

    def _matching_transfer_row(
        self,
        payload: object,
        *,
        transfer_type: str | tuple[str, ...] = "PORTFOLIO_MARGIN_MAIN",
    ) -> Optional[dict]:
        bucket = self.engine.profit_bucket
        expected_types = (
            {str(transfer_type or "").upper()}
            if isinstance(transfer_type, str)
            else {str(item or "").upper() for item in transfer_type}
        )
        for row in list((payload or {}).get("rows") or []):
            if str(row.get("type") or "").upper() not in expected_types:
                continue
            if str(row.get("asset") or "").upper() != str(
                bucket.pending_source_asset
                or bucket.pending_subscription_asset
                or ""
            ).upper():
                continue
            if not math.isclose(
                float(row.get("amount", 0) or 0),
                (
                    bucket.pending_source_transfer_amount
                    or bucket.pending_source_amount
                    or bucket.pending_subscription_amount
                ),
                rel_tol=0.0,
                abs_tol=1e-8,
            ):
                continue
            if str(row.get("status") or "").upper() != "CONFIRMED":
                continue
            if not self._row_is_in_pending_window(row, "timestamp"):
                continue
            return row
        return None

    def _matching_subscription_row(
        self,
        payload: object,
        *,
        earliest_time_ms: Optional[int] = None,
    ) -> Optional[dict]:
        bucket = self.engine.profit_bucket
        for row in list((payload or {}).get("rows") or []):
            asset = str(row.get("asset") or "").upper()
            if asset != str(
                bucket.pending_subscription_asset or ""
            ).upper():
                continue
            if not math.isclose(
                float(row.get("amount", 0) or 0),
                bucket.pending_subscription_amount,
                rel_tol=0.0,
                abs_tol=1e-8,
            ):
                continue
            if str(row.get("status") or "").upper() != "SUCCESS":
                continue
            if str(row.get("receiveAsset") or "").upper() != "RWUSD":
                continue
            if (
                earliest_time_ms is not None
                and int(row.get("time", 0) or 0) < earliest_time_ms
            ):
                continue
            if not self._row_is_in_pending_window(row, "time"):
                continue
            return row
        return None

    @staticmethod
    def _subscription_event(
        *,
        asset: str,
        amount: float,
        rwusd_amount: float,
        kind: str = "harvest",
    ) -> dict:
        return {
            "type": "rwusd_subscription",
            "action": "rwusd_subscribe",
            "asset": str(asset or "").upper(),
            "amount": max(0.0, float(amount or 0)),
            "rwusd_amount": max(0.0, float(rwusd_amount or 0)),
            "subscription_kind": str(kind or "harvest").strip().lower(),
            "collateralized": False,
            "simulated": False,
        }

    def _configured_source_assets(self) -> Tuple[str, ...]:
        config_name = (
            "RWUSD_TESTNET_SOURCE_ASSETS"
            if self.is_testnet
            else "RWUSD_SOURCE_ASSETS"
        )
        fallback = ("USDC",) if self.is_testnet else ("USDC", "USDT")
        configured = getattr(self.config, config_name, fallback)
        if isinstance(configured, str):
            configured = (configured,)
        assets: List[str] = []
        for item in configured or ():
            asset = str(item or "").strip().upper()
            if not asset or asset in assets:
                continue
            if asset not in SUPPORTED_RWUSD_SOURCE_ASSETS:
                raise ValueError(
                    f"unsupported RWUSD source asset {asset!r}"
                )
            assets.append(asset)
        if not assets:
            raise ValueError("RWUSD source assets cannot be empty")
        return tuple(assets)

    async def _select_subscription_source(
        self,
        *,
        quota: object,
        requested_amount: float,
        withdrawable: Optional[float],
        include_margin: bool = True,
        include_spot: bool = False,
        include_funding: bool = False,
        margin_reserves: Optional[Dict[str, float]] = None,
        preferred_source_asset: Optional[str] = None,
        preferred_target_asset: Optional[str] = None,
    ) -> _SubscriptionSourceChoice:
        requested = self._floor_asset_amount(requested_amount)
        transferable = (
            self._floor_asset_amount(withdrawable)
            if withdrawable is not None
            else None
        )
        minimum = self._quota_minimum(quota)
        if minimum <= 0:
            minimum = float(getattr(self.config, "MIN_SWEEP", 1.0))
        margin_rows = (
            await asyncio.gather(
                *(
                    self.client.get_margin_asset_balance(asset)
                    for asset in self._source_assets
                )
            )
            if include_margin
            else ({},) * len(self._source_assets)
        )
        spot_rows = (
            await self.client.get_spot_asset_balances(
                list(self._source_assets)
            )
            if include_spot
            else {}
        )
        funding_rows = (
            await self.client.get_funding_asset_balances(
                list(self._source_assets)
            )
            if include_funding
            else {}
        )
        reserves = {
            str(asset or "").strip().upper(): self._floor_asset_amount(
                amount
            )
            for asset, amount in dict(margin_reserves or {}).items()
        }
        margin_balances = {
            asset: self._floor_asset_amount(
                max(
                    0.0,
                    float((row or {}).get("wallet_balance", 0) or 0)
                    - reserves.get(asset, 0.0),
                )
            )
            for asset, row in zip(self._source_assets, margin_rows)
        }
        margin_transferable_balances = {
            asset: self._floor_asset_amount(
                float((row or {}).get("transferable_balance", 0) or 0)
            )
            for asset, row in zip(self._source_assets, margin_rows)
        }
        margin_futures_balances = {
            asset: self._floor_asset_amount(
                float((row or {}).get("um_wallet_balance", 0) or 0)
                + float((row or {}).get("cm_wallet_balance", 0) or 0)
            )
            for asset, row in zip(self._source_assets, margin_rows)
        }
        spot_balances = {
            asset: self._floor_asset_amount(
                float((spot_rows.get(asset) or {}).get("free", 0) or 0)
            )
            for asset in self._source_assets
        }
        funding_balances = {
            asset: self._floor_asset_amount(
                float((funding_rows.get(asset) or {}).get("free", 0) or 0)
            )
            for asset in self._source_assets
        }
        usable_margin_balances = {
            asset: self._floor_asset_amount(
                min(
                    (
                        balance
                        if margin_futures_balances.get(asset, 0.0) > 0
                        else min(
                            balance,
                            margin_transferable_balances.get(asset, 0.0),
                        )
                    ),
                    transferable,
                )
                if transferable is not None
                else (
                    balance
                    if margin_futures_balances.get(asset, 0.0) > 0
                    else min(
                        balance,
                        margin_transferable_balances.get(asset, 0.0),
                    )
                )
            )
            for asset, balance in margin_balances.items()
        }
        balances = {
            asset: self._floor_asset_amount(
                spot_balances.get(asset, 0.0)
                + funding_balances.get(asset, 0.0)
                + usable_margin_balances.get(asset, 0.0)
            )
            for asset in self._source_assets
        }
        if include_margin and margin_balances:
            observed_asset = max(
                margin_balances,
                key=lambda asset: margin_balances.get(asset, 0.0),
            )
            bucket = self.engine.profit_bucket
            bucket.subscription_source_asset = observed_asset
            bucket.subscription_source_wallet_balance = (
                margin_balances.get(observed_asset, 0.0)
            )
            bucket.subscription_source_withdrawable_amount = (
                usable_margin_balances.get(observed_asset, 0.0)
            )
            bucket.subscription_source_minimum_amount = minimum
            bucket.subscription_source_observed_at_ms = int(time.time() * 1000)
        quota_assets = self._quota_assets(quota)
        eligible_assets = tuple(
            asset for asset in self._source_assets if asset in quota_assets
        )
        preferred = str(preferred_target_asset or "").strip().upper()
        source_preference = str(
            preferred_source_asset or ""
        ).strip().upper()
        target_assets = (preferred,) if preferred else eligible_assets
        source_assets = (
            (source_preference,)
            if source_preference in self._source_assets
            else self._source_assets
        )
        empty = _SubscriptionSourceChoice(
            asset=None,
            amount=0.0,
            reason="",
            balances=balances,
            eligible_assets=eligible_assets,
            minimum=minimum,
        )
        data = quota if isinstance(quota, dict) else {}
        if data.get("subscribeEnable") is False:
            return replace(empty, reason="rwusd_subscription_disabled")
        if not eligible_assets:
            return replace(
                empty,
                reason=(
                    "rwusd_quota_asset_unavailable:"
                    + ",".join(self._source_assets)
                ),
            )
        if preferred and preferred not in eligible_assets:
            return replace(
                empty,
                reason=f"rwusd_quota_asset_unavailable:{preferred}",
            )
        if (include_spot or include_funding) and requested + 1e-12 < minimum:
            requested = max(
                max(balances.values(), default=0.0),
                (
                    max(margin_balances.values(), default=0.0)
                    if include_margin
                    else 0.0
                ),
            )
        if requested + 1e-12 < minimum:
            return replace(
                empty,
                reason="rwusd_amount_below_exchange_minimum",
            )
        if include_margin and not (include_spot or include_funding) and (
            transferable is not None
            and transferable + 1e-12 < minimum
        ):
            return replace(
                empty,
                reason=self._margin_locked_reason(self._source_assets),
            )
        last_reason = ""
        for asset in target_assets:
            if asset not in source_assets:
                continue
            spot_balance = spot_balances.get(asset, 0.0)
            transfer_wallets = (
                ("funding", funding_balances.get(asset, 0.0)),
                (
                    "portfolio_margin",
                    usable_margin_balances.get(asset, 0.0),
                ),
            )
            for source_wallet, transfer_balance in transfer_wallets:
                if transfer_balance <= 0:
                    continue
                source_requested = self._floor_asset_amount(
                    min(requested, spot_balance + transfer_balance)
                )
                amount, reason = self._eligible_subscription_amount(
                    quota=quota,
                    asset=asset,
                    requested_amount=source_requested,
                )
                if amount > 0:
                    transfer_amount = max(
                        0.0,
                        amount - min(amount, spot_balance),
                    )
                    return _SubscriptionSourceChoice(
                        asset=asset,
                        amount=amount,
                        reason=reason,
                        balances=balances,
                        eligible_assets=eligible_assets,
                        minimum=minimum,
                        source_asset=asset,
                        source_amount=amount,
                        source_collection_required=(
                            source_wallet == "portfolio_margin"
                            and transfer_amount > 0
                            and margin_futures_balances.get(asset, 0.0) > 0
                        ),
                        source_wallet=(
                            source_wallet
                            if transfer_amount > 0
                            else "spot"
                        ),
                        source_transfer_amount=transfer_amount,
                    )
                if source_requested + 1e-12 >= minimum:
                    last_reason = reason
            source_requested = self._floor_asset_amount(
                min(requested, spot_balance)
            )
            amount, reason = self._eligible_subscription_amount(
                quota=quota,
                asset=asset,
                requested_amount=source_requested,
            )
            if amount > 0:
                return _SubscriptionSourceChoice(
                    asset=asset,
                    amount=amount,
                    reason=reason,
                    balances=balances,
                    eligible_assets=eligible_assets,
                    minimum=minimum,
                    source_asset=asset,
                    source_amount=amount,
                    source_wallet="spot",
                )
            if source_requested + 1e-12 >= minimum:
                last_reason = reason
        for source_asset in source_assets:
            spot_balance = spot_balances.get(source_asset, 0.0)
            transfer_wallets = (
                ("funding", funding_balances.get(source_asset, 0.0)),
                (
                    "portfolio_margin",
                    usable_margin_balances.get(source_asset, 0.0),
                ),
            )
            for target_asset in target_assets:
                if source_asset == target_asset:
                    continue
                for source_wallet, transfer_balance in transfer_wallets:
                    if transfer_balance <= 0:
                        continue
                    source_requested = self._floor_asset_amount(
                        min(requested, spot_balance + transfer_balance)
                    )
                    amount, reason = (
                        await self._eligible_conversion_source_amount(
                            quota=quota,
                            source_asset=source_asset,
                            target_asset=target_asset,
                            requested_amount=source_requested,
                        )
                    )
                    if amount > 0:
                        transfer_amount = max(
                            0.0,
                            amount - min(amount, spot_balance),
                        )
                        return _SubscriptionSourceChoice(
                            asset=target_asset,
                            amount=amount,
                            reason=reason,
                            balances=balances,
                            eligible_assets=eligible_assets,
                            minimum=minimum,
                            source_asset=source_asset,
                            source_amount=amount,
                            source_collection_required=(
                                source_wallet == "portfolio_margin"
                                and transfer_amount > 0
                                and margin_futures_balances.get(
                                    source_asset,
                                    0.0,
                                )
                                > 0
                            ),
                            source_wallet=(
                                source_wallet
                                if transfer_amount > 0
                                else "spot"
                            ),
                            source_transfer_amount=transfer_amount,
                        )
                    if source_requested + 1e-12 >= minimum:
                        last_reason = reason
                source_requested = self._floor_asset_amount(
                    min(requested, spot_balance)
                )
                amount, reason = (
                    await self._eligible_conversion_source_amount(
                        quota=quota,
                        source_asset=source_asset,
                        target_asset=target_asset,
                        requested_amount=source_requested,
                    )
                )
                if amount > 0:
                    return _SubscriptionSourceChoice(
                        asset=target_asset,
                        amount=amount,
                        reason=reason,
                        balances=balances,
                        eligible_assets=eligible_assets,
                        minimum=minimum,
                        source_asset=source_asset,
                        source_amount=amount,
                        source_wallet="spot",
                    )
                if source_requested + 1e-12 >= minimum:
                    last_reason = reason
        unsupported_funded = [
            asset
            for asset in self._source_assets
            if asset not in eligible_assets
            and balances.get(asset, 0.0) + 1e-12 >= minimum
        ]
        locked_margin_assets = tuple(
            asset
            for asset, balance in margin_balances.items()
            if balance + 1e-12 >= minimum
            and usable_margin_balances.get(asset, 0.0) + 1e-12 < minimum
        )
        margin_funded_but_locked = include_margin and bool(
            locked_margin_assets
        )
        return replace(
            empty,
            reason=(
                self._margin_locked_reason(locked_margin_assets)
                if margin_funded_but_locked
                else last_reason
                or (
                    "rwusd_source_asset_not_eligible:"
                    + ",".join(unsupported_funded)
                    if unsupported_funded
                    else (
                        "rwusd_source_balance_below_minimum:"
                        + ",".join(eligible_assets)
                    )
                )
            ),
        )

    @staticmethod
    def _source_choice_is_exhausted(
        choice: _SubscriptionSourceChoice,
    ) -> bool:
        return all(
            balance + 1e-12 < choice.minimum
            for balance in choice.balances.values()
        )

    @staticmethod
    def _quota_assets(quota: object) -> Set[str]:
        data = quota if isinstance(quota, dict) else {}
        subscription = data.get("subscriptionQuota")
        details = subscription if isinstance(subscription, dict) else data
        return {
            str(item or "").strip().upper()
            for item in list(details.get("assets") or [])
            if str(item or "").strip()
        }

    @classmethod
    def _eligible_subscription_amount(
        cls,
        *,
        quota: object,
        asset: str,
        requested_amount: float,
    ) -> tuple[float, str]:
        data = quota if isinstance(quota, dict) else {}
        subscription = data.get("subscriptionQuota")
        details = subscription if isinstance(subscription, dict) else data
        if data.get("subscribeEnable") is False:
            return 0.0, "rwusd_subscription_disabled"
        if asset not in cls._quota_assets(data):
            return 0.0, f"rwusd_quota_asset_unavailable:{asset}"
        quota_value = float(
            details.get("available")
            or details.get("leftQuota")
            or details.get("quota")
            or 0
        )
        if quota_value <= 0:
            return 0.0, "rwusd_quota_unavailable"
        minimum = float(
            details.get("minimum")
            or details.get("minPurchaseAmount")
            or details.get("minimumSubscriptionAmount")
            or 0
        )
        eligible = cls._floor_asset_amount(
            min(max(0.0, float(requested_amount or 0)), quota_value)
        )
        if minimum > 0 and eligible + 1e-8 < minimum:
            return 0.0, "rwusd_amount_below_exchange_minimum"
        reason = (
            "rwusd_quota_capped"
            if eligible + 1e-8 < requested_amount
            else ""
        )
        return eligible, reason

    async def _eligible_conversion_source_amount(
        self,
        *,
        quota: object,
        source_asset: str,
        target_asset: str,
        requested_amount: float,
    ) -> tuple[float, str]:
        amount, reason = self._eligible_subscription_amount(
            quota=quota,
            asset=target_asset,
            requested_amount=requested_amount,
        )
        if amount <= 0:
            return 0.0, reason
        try:
            from_minimum, from_maximum, _, _ = (
                await self._conversion_limits(source_asset, target_asset)
            )
        except Exception as exc:
            return (
                0.0,
                "stablecoin_conversion_unavailable:"
                f"{source_asset}->{target_asset}:{exc}",
            )
        amount = self._floor_asset_amount(min(amount, from_maximum))
        if amount + 1e-12 < from_minimum:
            return 0.0, "stablecoin_conversion_below_minimum"
        return amount, reason

    async def _conversion_limits(
        self,
        source_asset: str,
        target_asset: str,
    ) -> Tuple[float, float, float, float]:
        pair = (
            str(source_asset or "").strip().upper(),
            str(target_asset or "").strip().upper(),
        )
        cached = self._conversion_limits_by_pair.get(pair)
        if cached is not None:
            return cached
        rows = await self.client.get_convert_exchange_info(
            from_asset=pair[0],
            to_asset=pair[1],
        )
        row = next(
            (
                item
                for item in rows or []
                if str(item.get("fromAsset") or "").upper() == pair[0]
                and str(item.get("toAsset") or "").upper() == pair[1]
            ),
            None,
        )
        if row is None:
            raise RuntimeError("conversion pair is unavailable")
        limits = (
            max(0.0, float(row.get("fromAssetMinAmount", 0) or 0)),
            max(0.0, float(row.get("fromAssetMaxAmount", 0) or 0)),
            max(0.0, float(row.get("toAssetMinAmount", 0) or 0)),
            max(0.0, float(row.get("toAssetMaxAmount", 0) or 0)),
        )
        if limits[1] <= 0 or limits[3] <= 0:
            raise RuntimeError("conversion pair limits are invalid")
        self._conversion_limits_by_pair[pair] = limits
        return limits

    @staticmethod
    def _quota_minimum(quota: object) -> float:
        data = quota if isinstance(quota, dict) else {}
        subscription = data.get("subscriptionQuota")
        details = subscription if isinstance(subscription, dict) else data
        return max(
            0.0,
            float(
                details.get("minimum")
                or details.get("minPurchaseAmount")
                or details.get("minimumSubscriptionAmount")
                or 0
            ),
        )

    @staticmethod
    def _collateral_confirmation_balance(payload: object) -> float:
        data = payload if isinstance(payload, dict) else {}
        return max(
            0.0,
            float(data.get("um_wallet_balance", 0) or 0),
        )

    @staticmethod
    def _transferable_rwusd_balance(payload: object) -> float:
        data = payload if isinstance(payload, dict) else {}
        return max(0.0, float(data.get("amount", 0) or 0))

    def _safe_harvest_subscription_amount(
        self,
        *,
        portfolio: PortfolioSnapshot,
        requested_amount: float,
    ) -> Tuple[float, str]:
        requested = self._floor_asset_amount(requested_amount)
        if requested <= 0:
            return 0.0, "below_min_sweep"
        equity = max(0.0, float(portfolio.account_equity or 0))
        uni_mmr = max(0.0, float(portfolio.uni_mmr or 0))
        floor = max(
            0.0,
            float(
                getattr(
                    self.config,
                    "LIVE_HARVEST_TRANSFER_UNIMMR_FLOOR",
                    4.5,
                )
            ),
        )
        if equity <= 0:
            return 0.0, "rwusd_transfer_equity_unavailable"
        if uni_mmr <= floor + 1e-12:
            return 0.0, "rwusd_transfer_unimmr_buffer"
        max_fraction = min(
            1.0,
            max(
                0.0,
                float(
                    getattr(
                        self.config,
                        "LIVE_HARVEST_TRANSFER_MAX_EQUITY_FRACTION",
                        0.05,
                    )
                ),
            ),
        )
        max_by_fraction = equity * max_fraction
        max_by_unimmr = equity * max(0.0, 1.0 - floor / uni_mmr)
        safe = self._floor_asset_amount(
            min(requested, max_by_fraction, max_by_unimmr)
        )
        if safe <= 0:
            return 0.0, "rwusd_transfer_unimmr_buffer"
        headroom_fraction = max(
            0.0,
            float(
                getattr(
                    self.config,
                    "LIVE_HARVEST_TRANSFER_HEADROOM_FRACTION",
                    0.001,
                )
            ),
        )
        headroom_minimum = max(
            0.0,
            float(
                getattr(
                    self.config,
                    "LIVE_HARVEST_TRANSFER_HEADROOM_MIN_USD",
                    0.01,
                )
            ),
        )
        minimum_sweep = max(
            0.0,
            float(getattr(self.config, "MIN_SWEEP", 1.0) or 0),
        )
        headroom = max(safe * headroom_fraction, headroom_minimum)
        if safe > minimum_sweep + headroom + 1e-12:
            safe = self._floor_asset_amount(max(0.0, safe - headroom))
        if safe <= 0:
            return 0.0, "rwusd_transfer_headroom"
        return (
            safe,
            "rwusd_transfer_safety_capped"
            if safe + 1e-12 < requested
            else "",
        )

    async def _value_trade_commissions(
        self,
        trades: List[dict],
    ) -> List[dict]:
        assets = sorted(
            {
                str(trade.get("commission_asset") or "").strip().upper()
                for trade in trades
                if max(0.0, float(trade.get("commission", 0) or 0)) > 0
                and str(
                    trade.get("commission_asset") or ""
                ).strip().upper()
                not in USD_COMMISSION_ASSETS
            }
        )
        rates: Dict[str, float] = {}
        if assets:
            values = await asyncio.gather(
                *[
                    self._commission_asset_usd_rate(asset)
                    for asset in assets
                ]
            )
            rates.update(zip(assets, values))
        valued: List[dict] = []
        for trade in trades:
            item = dict(trade)
            commission = max(
                0.0,
                float(item.get("commission", 0) or 0),
            )
            asset = str(
                item.get("commission_asset") or ""
            ).strip().upper()
            if commission > 0 and not asset:
                raise RuntimeError(
                    "RWUSD commission asset is missing"
                )
            rate = 1.0 if asset in USD_COMMISSION_ASSETS else rates.get(asset)
            if commission > 0 and (rate is None or rate <= 0):
                raise RuntimeError(
                    f"RWUSD commission conversion unavailable: {asset}"
                )
            item["commission_value_usd"] = commission * float(rate or 0)
            item["commission_conversion_rate"] = float(rate or 0)
            valued.append(item)
        return valued

    async def _commission_asset_usd_rate(self, asset: str) -> float:
        normalized = str(asset or "").strip().upper()
        for quote in ("USDC", "USDT"):
            try:
                ticker = await self.client.get_ticker(
                    f"{normalized}{quote}"
                )
            except Exception:
                continue
            price = max(
                0.0,
                float(
                    (ticker or {}).get("mark_price")
                    or (ticker or {}).get("last_price")
                    or 0
                ),
            )
            if price > 0:
                return price
        raise RuntimeError(
            f"RWUSD commission conversion unavailable: {normalized}"
        )

    def _row_is_in_pending_window(
        self,
        row: dict,
        timestamp_key: str,
    ) -> bool:
        timestamp = int(row.get(timestamp_key, 0) or 0)
        if timestamp <= 0:
            return False
        started = int(
            self.engine.profit_bucket.pending_subscription_started_at_ms or 0
        )
        return timestamp + 60000 >= started

    @staticmethod
    def _floor_asset_amount(amount: float) -> float:
        value = max(0.0, float(amount or 0))
        return math.floor(value * 100000000.0) / 100000000.0

    @staticmethod
    def _margin_locked_reason(assets: Tuple[str, ...]) -> str:
        labels = [str(asset or "").strip().upper() for asset in assets]
        suffix = ",".join(asset for asset in labels if asset)
        return (
            f"{RWUSD_MARGIN_LOCKED_BLOCK_REASON}:{suffix}"
            if suffix
            else RWUSD_MARGIN_LOCKED_BLOCK_REASON
        )

    def _client_order_id(self, local_order_id: str) -> str:
        order_number = str(local_order_id or "").rsplit("-", 1)[-1]
        environment = "t" if self.is_testnet else "m"
        return (
            f"cmrwusd-{environment}-{self._account_tag}-{order_number}"[:32]
        )

    async def _maker_order_is_behind(
        self,
        order: object,
        book: object,
        *,
        symbol: str,
        price_mode: str,
    ) -> bool:
        data = book if isinstance(book, dict) else {}
        best_bid = max(0.0, float(data.get("bid_price", 0) or 0))
        best_ask = max(0.0, float(data.get("ask_price", 0) or 0))
        price = max(0.0, float(getattr(order, "price", 0) or 0))
        side = str(getattr(order, "side", "") or "").upper()
        if price <= 0 or best_bid <= 0 or best_ask <= 0:
            return False
        try:
            desired = await self.client.normalize_um_post_only_price(
                symbol=symbol,
                side=side,
                best_bid=best_bid,
                best_ask=best_ask,
                price_mode=price_mode,
            )
        except Exception:
            desired = {}
        desired_price = max(
            0.0,
            float((desired or {}).get("price", 0) or 0),
        )
        if desired_price > 0:
            if side == "BUY":
                return price + 1e-12 < desired_price
            if side == "SELL":
                return price - 1e-12 > desired_price
        if side == "BUY":
            return price + 1e-12 < best_bid
        if side == "SELL":
            return price - 1e-12 > best_ask
        return False

    @staticmethod
    def _is_minimum_order_error(error: object) -> bool:
        normalized = str(error or "").strip().lower()
        return normalized in {
            "notional_too_small",
            "qty_too_small",
            "quantity_below_minimum",
        }

    @staticmethod
    def _has_valid_hedge_mode_side(order: object) -> bool:
        position_side = str(getattr(order, "position_side", "") or "")
        if hasattr(getattr(order, "position_side", None), "value"):
            position_side = str(order.position_side.value)
        side = str(getattr(order, "side", "") or "").upper()
        reduce_only = bool(getattr(order, "reduce_only", False))
        expected = {
            ("LONG", False): "BUY",
            ("SHORT", False): "SELL",
            ("LONG", True): "SELL",
            ("SHORT", True): "BUY",
        }
        return side == expected.get((position_side.upper(), reduce_only))

    @staticmethod
    def _is_order_not_found(error: object) -> bool:
        message = str(error or "").lower()
        return "-2013" in message or "order does not exist" in message

    @staticmethod
    def _is_maker_requote_error(error: object) -> bool:
        message = str(error or "").lower()
        return any(
            token in message
            for token in (
                "-5022",
                "post only",
                "post-only",
                "gtx",
                "would immediately match",
            )
        )

    @staticmethod
    def _write_outcome_uncertain(error: object) -> bool:
        message = str(error or "").lower()
        if "-1006" in message or "-1007" in message:
            return True
        return not any(
            token in message
            for token in (
                "http 400",
                "http 401",
                "http 403",
                "http 409",
                "api -",
                "rejected",
                "insufficient",
                "quota",
            )
        )
