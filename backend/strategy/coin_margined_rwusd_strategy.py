"""Independent Binance Portfolio Margin coin-margined RWUSD strategy."""

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Callable, Iterable, List, Mapping, Optional

from backend.strategy.coin_margined_rwusd.capital_bootstrap import (
    DetectedCapitalComponent,
    OfficialCapitalSnapshot,
)
from backend.strategy.coin_margined_rwusd.coordinator import (
    PORTFOLIO_STORE_SUFFIX,
    RWUSDPortfolioCoordinator,
)
from backend.strategy.coin_margined_rwusd.engine import RWUSDEngine
from backend.strategy.coin_margined_rwusd.exit import RWUSDExitStage
from backend.strategy.coin_margined_rwusd.execution import BinanceRWUSDExecutor
from backend.strategy.coin_margined_rwusd.portfolio_runtime import (
    RWUSDPortfolioRuntime,
    RWUSDSymbolLane,
)
from backend.strategy.coin_margined_rwusd.portfolio import ZERO
from backend.strategy.coin_margined_rwusd.persistence import LANE_STATE_PREFIX
from backend.strategy.coin_margined_rwusd.runtime import RWUSDExecutionOwnership
from backend.strategy.coin_margined_rwusd.base_strategy import (
    CoinMarginedRWUSDBaseStrategy,
)


COIN_MARGINED_NAMESPACE = "coin_margined_rwusd"
COLLATERAL_STATE_SUFFIX = "__coin_collateral"
COIN_ASSETS = ("BTC", "ETH")
COIN_SYMBOLS = {"BTC": "BTCUSDC", "ETH": "ETHUSDC"}


def _runtime_dir() -> Path:
    configured = str(os.environ.get("DBRSI_RUNTIME_DIR") or "").strip()
    return (
        Path(configured)
        if configured
        else Path(__file__).resolve().parents[2] / ".runtime"
    )


def _safe_name(value: object) -> str:
    safe = "".join(
        char if char.isalnum() or char in {"_", "-"} else "_"
        for char in str(value or "")
    ).strip("_")
    return safe or "coin_margined_rwusd"


class CoinMarginedStateStore:
    """Independent JSON state store; never reads the RWUSD namespace."""

    def __init__(self, account_name: str, suffix: str = "") -> None:
        root = _runtime_dir() / COIN_MARGINED_NAMESPACE
        self.path = root / f"{_safe_name(account_name)}{suffix}.json"

    def load(self) -> dict:
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    def load_strict(self) -> dict:
        if not self.path.exists():
            return {}
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise RuntimeError(
                f"coin-margined RWUSD state is unreadable: {self.path}"
            ) from exc
        if not isinstance(payload, dict):
            raise RuntimeError(
                f"coin-margined RWUSD state is not an object: {self.path}"
            )
        return payload

    def save(self, payload: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        os.replace(temporary, self.path)


def clear_coin_margined_runtime_state(account_name: object) -> bool:
    account = _safe_name(account_name)
    root = _runtime_dir() / COIN_MARGINED_NAMESPACE
    paths = [
        root / f"{account}.json",
        root / f"{account}{PORTFOLIO_STORE_SUFFIX}.json",
        root / f"{account}{COLLATERAL_STATE_SUFFIX}.json",
    ]
    paths.extend(root.glob(f"{account}{LANE_STATE_PREFIX}*.json"))
    removed = False
    for path in dict.fromkeys(paths):
        if path.exists():
            path.unlink()
            removed = True
    return removed


def rename_coin_margined_runtime_state(
    old_account_name: object,
    new_account_name: object,
) -> bool:
    old = _safe_name(old_account_name)
    new = _safe_name(new_account_name)
    if old == new:
        return False
    root = _runtime_dir() / COIN_MARGINED_NAMESPACE
    sources = []
    if root.exists():
        for suffix in (
            "",
            PORTFOLIO_STORE_SUFFIX,
            COLLATERAL_STATE_SUFFIX,
        ):
            path = root / f"{old}{suffix}.json"
            if path.exists():
                sources.append(path)
        sources.extend(
            sorted(root.glob(f"{old}{LANE_STATE_PREFIX}*.json"))
        )
    if not sources:
        return False
    moves = [
        (source, root / f"{new}{source.name[len(old):]}")
        for source in sources
    ]
    conflicts = [
        destination
        for _, destination in moves
        if destination.exists()
    ]
    if conflicts:
        raise RuntimeError(
            "coin-margined RWUSD target runtime state already exists: "
            + ",".join(path.name for path in conflicts)
        )
    moved: list[tuple[Path, Path]] = []
    try:
        for source, destination in moves:
            os.replace(source, destination)
            moved.append((source, destination))
    except Exception:
        for source, destination in reversed(moved):
            if destination.exists() and not source.exists():
                os.replace(destination, source)
        raise
    return True


@dataclass(frozen=True)
class CoinMarginedCapitalSnapshot:
    asset: str
    asset_units: Decimal
    asset_price: Decimal
    usdc_value: Decimal
    official: OfficialCapitalSnapshot

    @property
    def external_principal(self) -> Decimal:
        return self.usdc_value

    @property
    def components(self) -> tuple[DetectedCapitalComponent, ...]:
        return self.official.components

    @property
    def observed_at_ms(self) -> int:
        return self.official.observed_at_ms


@dataclass(frozen=True)
class CoinMarginedWalletSnapshot:
    pm_units: dict[str, Decimal]
    spot_units: dict[str, Decimal]
    funding_units: dict[str, Decimal]
    prices: dict[str, Decimal]


def _positive_decimal(value: object, label: str) -> Decimal:
    try:
        parsed = Decimal(str(value or 0))
    except Exception as exc:
        raise RuntimeError(f"{label} is invalid") from exc
    if not parsed.is_finite() or parsed <= 0:
        raise RuntimeError(f"{label} must be positive and finite")
    return parsed


def _wallet_amount(
    payload: object,
    *,
    field: str,
    label: str,
) -> Decimal:
    row = payload if isinstance(payload, Mapping) else {}
    try:
        value = Decimal(str(row.get(field, 0) or 0))
    except Exception as exc:
        raise RuntimeError(f"{label} balance is invalid") from exc
    if not value.is_finite() or value < 0:
        raise RuntimeError(f"{label} balance is invalid")
    return value


async def _scan_coin_wallets(client: object) -> CoinMarginedWalletSnapshot:
    margin_loader = getattr(client, "get_margin_asset_balance", None)
    spot_loader = getattr(client, "get_spot_asset_balances", None)
    funding_loader = getattr(client, "get_funding_asset_balances", None)
    ticker_loader = getattr(client, "get_ticker", None)
    if not all(
        loader is not None
        for loader in (
            margin_loader,
            spot_loader,
            funding_loader,
            ticker_loader,
        )
    ):
        raise RuntimeError(
            "coin-margined RWUSD requires PM, Spot, Funding and ticker "
            "interfaces"
        )
    margin_rows, spot_rows, funding_rows, ticker_rows = await asyncio.gather(
        asyncio.gather(*(margin_loader(asset) for asset in COIN_ASSETS)),
        spot_loader(list(COIN_ASSETS)),
        funding_loader(list(COIN_ASSETS)),
        asyncio.gather(
            *(ticker_loader(f"{asset}USDC") for asset in COIN_ASSETS)
        ),
    )
    spot_map = spot_rows if isinstance(spot_rows, Mapping) else {}
    funding_map = funding_rows if isinstance(funding_rows, Mapping) else {}
    return CoinMarginedWalletSnapshot(
        pm_units={
            asset: _wallet_amount(
                payload,
                field="wallet_balance",
                label=f"Portfolio Margin {asset}",
            )
            for asset, payload in zip(COIN_ASSETS, margin_rows)
        },
        spot_units={
            asset: _wallet_amount(
                spot_map.get(asset),
                field="free",
                label=f"Spot {asset}",
            )
            for asset in COIN_ASSETS
        },
        funding_units={
            asset: _wallet_amount(
                funding_map.get(asset),
                field="free",
                label=f"Funding {asset}",
            )
            for asset in COIN_ASSETS
        },
        prices={
            asset: _positive_decimal(
                (
                    payload if isinstance(payload, Mapping) else {}
                ).get("mark_price")
                or (
                    payload if isinstance(payload, Mapping) else {}
                ).get("last_price"),
                f"Binance {asset}USDC valuation price",
            )
            for asset, payload in zip(COIN_ASSETS, ticker_rows)
        },
    )


def _coin_wallet_assets(snapshot: CoinMarginedWalletSnapshot) -> set[str]:
    return {
        asset
        for asset in COIN_ASSETS
        if (
            snapshot.pm_units[asset] > 0
            or snapshot.spot_units[asset] > 0
            or snapshot.funding_units[asset] > 0
        )
    }


async def _confirm_coin_transfer(
    client: object,
    *,
    stage: str,
    asset: str,
    amount: Decimal,
    source_before: Decimal,
    destination_before: Decimal,
    timeout_sec: float = 30.0,
) -> bool:
    if stage == "funding_to_spot":
        source_loader = client.get_funding_asset_balance
        destination_loader = client.get_spot_asset_balance
        destination_field = "free"
    else:
        source_loader = client.get_spot_asset_balance
        destination_loader = client.get_margin_asset_balance
        destination_field = "wallet_balance"
    threshold = max(Decimal("0.00000001"), amount * Decimal("0.999999"))
    deadline = time.monotonic() + max(1.0, timeout_sec)
    while True:
        source, destination = await asyncio.gather(
            source_loader(asset),
            destination_loader(asset),
        )
        source_after = _wallet_amount(
            source,
            field="free",
            label=f"{stage} source {asset}",
        )
        destination_after = _wallet_amount(
            destination,
            field=destination_field,
            label=f"{stage} destination {asset}",
        )
        if (
            source_before - source_after >= threshold
            and destination_after - destination_before >= threshold
        ):
            return True
        if time.monotonic() >= deadline:
            return False
        await asyncio.sleep(1.0)


async def _submit_coin_transfer(
    client: object,
    *,
    store: CoinMarginedStateStore,
    stage: str,
    asset: str,
    amount: Decimal,
    source_before: Decimal,
    destination_before: Decimal,
) -> dict:
    metadata = store.load_strict()
    pending = metadata.get("pending_transfer")
    if pending:
        pending_stage = str(pending.get("stage") or "").strip()
        if pending_stage != stage or str(pending.get("asset") or "").upper() != asset:
            raise RuntimeError(
                "coin-margined collateral transfer has an incompatible "
                "pending state"
            )
        confirmed = await _confirm_coin_transfer(
            client,
            stage=stage,
            asset=asset,
            amount=Decimal(str(pending.get("amount") or 0)),
            source_before=Decimal(str(pending.get("source_before") or 0)),
            destination_before=Decimal(
                str(pending.get("destination_before") or 0)
            ),
            timeout_sec=5.0,
        )
        if not confirmed:
            raise RuntimeError(
                "coin-margined collateral transfer is pending confirmation; "
                "no duplicate transfer was submitted"
            )
        metadata.pop("pending_transfer", None)
        store.save(metadata)
        return {
            "asset": asset,
            "amount": str(amount),
            "transfer_id": str(pending.get("transfer_id") or ""),
            "reconciled": True,
        }

    pending = {
        "stage": stage,
        "asset": asset,
        "amount": str(amount),
        "source_before": str(source_before),
        "destination_before": str(destination_before),
        "created_at_ms": int(time.time() * 1000),
    }
    metadata["pending_transfer"] = pending
    store.save(metadata)
    if stage == "funding_to_spot":
        response = await client.transfer_funding_to_spot(
            asset=asset,
            amount=float(amount),
        )
    else:
        response = await client.transfer_spot_to_pm(
            asset=asset,
            amount=float(amount),
        )
    response_payload = response if isinstance(response, Mapping) else {}
    pending["transfer_id"] = str(
        response_payload.get("tranId")
        or response_payload.get("clientTranId")
        or ""
    )
    metadata["pending_transfer"] = pending
    store.save(metadata)
    if not await _confirm_coin_transfer(
        client,
        stage=stage,
        asset=asset,
        amount=amount,
        source_before=source_before,
        destination_before=destination_before,
    ):
        raise RuntimeError(
            f"coin-margined {stage} submitted but not confirmed; "
            "retry will reconcile without resubmitting"
        )
    metadata.pop("pending_transfer", None)
    store.save(metadata)
    return {
        "asset": asset,
        "amount": str(amount),
        "transfer_id": pending["transfer_id"],
        "reconciled": False,
    }


async def prepare_coin_margined_capital(
    client: object,
    *,
    account_name: str,
    collateral_store: CoinMarginedStateStore,
    expected_asset: Optional[str] = None,
) -> tuple[CoinMarginedCapitalSnapshot, list[dict]]:
    """Select one coin and move its free wallet balances into PM."""
    metadata = collateral_store.load_strict()
    persisted_asset = str(metadata.get("asset") or "").strip().upper()
    has_persisted_collateral = bool(persisted_asset)
    snapshot = await _scan_coin_wallets(client)
    expected = str(expected_asset or "").strip().upper()
    assets = _coin_wallet_assets(snapshot)
    if expected:
        if expected not in COIN_ASSETS:
            raise RuntimeError("coin-margined persisted collateral asset is invalid")
        foreign = assets - {expected}
        if foreign:
            raise RuntimeError(
                "coin-margined collateral cannot mix assets: "
                + ",".join(sorted(foreign))
            )
        asset = expected
    else:
        if len(assets) != 1:
            raise RuntimeError(
                "coin-margined RWUSD requires exactly one positive BTC or ETH "
                "asset across PM, Spot and Funding"
            )
        asset = next(iter(assets))

    def build_snapshot() -> CoinMarginedCapitalSnapshot:
        pm_units = snapshot.pm_units[asset]
        if pm_units <= 0:
            raise RuntimeError(
                "coin-margined RWUSD requires a positive PM BTC or ETH "
                "balance after wallet transfers"
            )
        value = pm_units * snapshot.prices[asset]
        component = DetectedCapitalComponent(
            external_reference=(
                f"coin-margined:{account_name}:"
                f"binance_portfolio_margin:{asset}"
            ),
            source_asset=asset,
            source_wallet="portfolio_margin",
            amount=value,
        )
        return CoinMarginedCapitalSnapshot(
            asset=asset,
            asset_units=pm_units,
            asset_price=snapshot.prices[asset],
            usdc_value=value,
            official=OfficialCapitalSnapshot(
                environment="binance_portfolio_margin_live",
                observed_at_ms=int(time.time() * 1000),
                components=(component,),
            ),
        )

    pending_capital = metadata.get("pending_capital")
    if pending_capital:
        if not has_persisted_collateral:
            raise RuntimeError(
                "coin-margined pending incremental capital has no "
                "persisted initial collateral"
            )
        pending_asset = str(
            pending_capital.get("asset") or ""
        ).strip().upper()
        if pending_asset != asset:
            raise RuntimeError(
                "coin-margined pending capital asset does not match "
                f"wallet asset: {pending_asset or 'missing'} vs {asset}"
            )
        return (
            build_snapshot(),
            [
                {
                    "type": "coin_margined_capital",
                    "action": "incremental_coin_capital_ready",
                    "asset": asset,
                    "asset_units": str(
                        pending_capital.get("asset_units") or 0
                    ),
                    "amount": str(pending_capital.get("amount") or 0),
                    "capital_reference": str(
                        pending_capital.get("capital_reference") or ""
                    ),
                    "transfer_id": str(
                        pending_capital.get("transfer_id") or ""
                    ),
                    "reconciled": True,
                },
            ],
        )

    events: list[dict] = []
    pm_units_before = snapshot.pm_units[asset]
    source_units = (
        snapshot.funding_units[asset] + snapshot.spot_units[asset]
    )
    transfer_ids: list[str] = []
    funding_amount = snapshot.funding_units[asset]
    if funding_amount > 0:
        transfer = await _submit_coin_transfer(
            client,
            store=collateral_store,
            stage="funding_to_spot",
            asset=asset,
            amount=funding_amount,
            source_before=funding_amount,
            destination_before=snapshot.spot_units[asset],
        )
        if transfer.get("transfer_id"):
            transfer_ids.append(str(transfer["transfer_id"]))
        events.append(
            {
                "type": "coin_margined_asset_transfer",
                "action": "funding_to_spot_confirmed",
                "asset": asset,
                "amount": str(funding_amount),
                "transfer_id": str(transfer.get("transfer_id") or ""),
            }
        )
        snapshot = await _scan_coin_wallets(client)

    spot_amount = snapshot.spot_units[asset]
    if spot_amount > 0:
        transfer = await _submit_coin_transfer(
            client,
            store=collateral_store,
            stage="spot_to_pm",
            asset=asset,
            amount=spot_amount,
            source_before=spot_amount,
            destination_before=snapshot.pm_units[asset],
        )
        transfer_id = str(transfer.get("transfer_id") or "")
        if transfer_id:
            transfer_ids.append(transfer_id)
        events.append(
            {
                "type": "coin_margined_asset_transfer",
                "action": "spot_to_pm_confirmed",
                "asset": asset,
                "amount": str(spot_amount),
                "usdc_value": str(spot_amount * snapshot.prices[asset]),
                "transfer_id": transfer_id,
            }
        )
        snapshot = await _scan_coin_wallets(client)

    if has_persisted_collateral and source_units > 0:
        transfer_id = transfer_ids[-1] if transfer_ids else ""
        capital_reference = (
            f"coin-margined-transfer:{account_name}:{asset}:"
            f"{pm_units_before:.8f}:{source_units:.8f}:"
            f"{transfer_id or 'balance-delta'}"
        )
        metadata["pending_capital"] = {
            "asset": asset,
            "asset_units": str(source_units),
            "amount": str(source_units * snapshot.prices[asset]),
            "capital_reference": capital_reference,
            "transfer_id": transfer_id,
            "created_at_ms": int(time.time() * 1000),
        }
        collateral_store.save(metadata)
        events.append(
            {
                "type": "coin_margined_capital",
                "action": "incremental_coin_capital_ready",
                "asset": asset,
                "asset_units": str(source_units),
                "amount": str(source_units * snapshot.prices[asset]),
                "capital_reference": capital_reference,
                "transfer_id": transfer_id,
                "reconciled": False,
            }
        )
    return (
        build_snapshot(),
        events,
    )


async def detect_coin_margined_capital(
    client: object,
    *,
    account_name: str,
    execution_environment: str,
) -> CoinMarginedCapitalSnapshot:
    """Read exactly one positive PM BTC/ETH balance and value it in USDC."""
    if execution_environment != "binance_portfolio_margin_live":
        raise RuntimeError(
            "coin-margined RWUSD requires Binance Portfolio Margin mainnet"
        )
    snapshot = await _scan_coin_wallets(client)
    candidates: list[tuple[str, Decimal, Decimal, Decimal]] = []
    for asset in COIN_ASSETS:
        units = snapshot.pm_units[asset]
        if units <= 0:
            continue
        price = snapshot.prices[asset]
        candidates.append((asset, units, price, units * price))

    if len(candidates) != 1:
        if not candidates:
            raise RuntimeError(
                "coin-margined RWUSD requires a positive PM BTC or ETH balance"
            )
        raise RuntimeError(
            "coin-margined RWUSD supports exactly one PM asset; "
            "BTC and ETH cannot both be positive"
        )

    asset, units, price, value = candidates[0]
    component = DetectedCapitalComponent(
        external_reference=(
            f"coin-margined:{account_name}:"
            f"{execution_environment}:portfolio_margin:{asset}"
        ),
        source_asset=asset,
        source_wallet="portfolio_margin",
        amount=value,
    )
    official = OfficialCapitalSnapshot(
        environment=execution_environment,
        observed_at_ms=int(time.time() * 1000),
        components=(component,),
    )
    return CoinMarginedCapitalSnapshot(
        asset=asset,
        asset_units=units,
        asset_price=price,
        usdc_value=value,
        official=official,
    )


class CoinMarginedPortfolioRuntime(RWUSDPortfolioRuntime):
    """Single-lane runtime with an independent direct-collateral bootstrap."""

    def __init__(
        self,
        *,
        account_name: str,
        client: object,
        config: object,
        primary_engine: RWUSDEngine,
        primary_executor: Optional[BinanceRWUSDExecutor],
        total_margin_fraction: float,
    ) -> None:
        super().__init__(
            account_name=account_name,
            simulation_capital=0.0,
            config=config,
            client=client,
            execution_environment="binance_portfolio_margin_live",
            execution_enabled=True,
            live_rwusd_execution=True,
            total_margin_fraction=total_margin_fraction,
            primary_engine=primary_engine,
            primary_executor=primary_executor,
        )
        self.coordinator = RWUSDPortfolioCoordinator(
            account_name=account_name,
            config=config,
            execution_environment="binance_portfolio_margin_live",
            live_rwusd_execution=True,
            state_store=CoinMarginedStateStore(
                account_name,
                PORTFOLIO_STORE_SUFFIX,
            ),
        )
        self.ownership = RWUSDExecutionOwnership()
        self._persisted_lane_symbols = self._lane_symbols()
        self._collateral_store = CoinMarginedStateStore(
            account_name,
            COLLATERAL_STATE_SUFFIX,
        )
        self._capital_snapshot: Optional[CoinMarginedCapitalSnapshot] = None
        self._last_coin_capital_scan_monotonic = 0.0

    def _lane_symbols(self) -> set[str]:
        root = _runtime_dir() / COIN_MARGINED_NAMESPACE
        prefix = f"{_safe_name(self.account_name)}{LANE_STATE_PREFIX}"
        if not root.exists():
            return set()
        return {
            path.stem[len(prefix):].upper()
            for path in root.glob(f"{prefix}*.json")
            if path.stem.startswith(prefix)
        }

    def restore(self) -> bool:
        return self.coordinator.restore()

    def initial_allocation_required(self) -> bool:
        return False

    async def prime_official_capital_snapshot(self) -> None:
        return None

    def initialize(
        self,
        *,
        allocation_rows: Iterable[Mapping[str, object]],
        fallback_symbol: str,
    ) -> None:
        del allocation_rows, fallback_symbol
        snapshot = self._capital_snapshot
        if snapshot is None:
            raise RuntimeError("coin-margined PM capital scan was not completed")
        asset = snapshot.asset
        symbol = COIN_SYMBOLS[asset]
        metadata = self._collateral_store.load_strict()
        previous_asset = str(metadata.get("asset") or "").upper()
        if previous_asset and previous_asset != asset:
            raise RuntimeError(
                "coin-margined RWUSD collateral asset changed; "
                "close the existing strategy before switching BTC/ETH"
            )
        if self._persisted_lane_symbols - {symbol}:
            raise RuntimeError(
                "coin-margined RWUSD has persisted state for more than one lane"
            )

        engine_payload = CoinMarginedStateStore(
            self.account_name
        ).load_strict()
        has_engine_state = bool(engine_payload)
        has_portfolio_state = bool(self.coordinator.ledger.capital_lots)
        if has_engine_state != has_portfolio_state:
            raise RuntimeError(
                "coin-margined RWUSD engine and portfolio state are "
                "incomplete or mismatched"
            )
        if (has_engine_state or has_portfolio_state) and not metadata:
            raise RuntimeError(
                "coin-margined RWUSD collateral metadata is missing for "
                "persisted strategy state"
            )
        persisted_symbol = str(
            engine_payload.get("state", {}).get("selected_symbol") or ""
        ).upper()
        if persisted_symbol and persisted_symbol != symbol:
            raise RuntimeError(
                "coin-margined RWUSD engine symbol does not match collateral"
            )

        self.primary_symbol = symbol
        self.primary_engine.state.selected_symbol = symbol
        self._register_primary_lane()
        self.ensure_lanes()

        bucket = self.primary_engine.profit_bucket
        if (
            not self.coordinator.ledger.capital_lots
            and (
                bucket.initial_collateralization_completed
                or metadata
            )
        ):
            raise RuntimeError(
                "coin-margined RWUSD state has collateral metadata but no "
                "matching portfolio capital ledger"
            )
        if not self.coordinator.ledger.capital_lots:
            self.coordinator.bootstrap_capital(
                snapshot=snapshot.official,
                rows=(),
                source="coin_margined_portfolio_margin",
                fixed_symbol=symbol,
            )
            self.coordinator.mark_pending_capital_collateralized(
                snapshot.usdc_value,
                source_asset=asset,
            )
        elif (
            set(self.coordinator.ledger.allocated_capital) != {symbol}
            or self.coordinator.symbol_capital(symbol) <= 0
        ):
            raise RuntimeError(
                "coin-margined RWUSD persisted capital does not match "
                f"{symbol}"
            )

        self._mark_direct_collateral(snapshot)
        if not metadata:
            self._save_initial_collateral(snapshot)
        self._refresh_lane_budget(self.primary_lane())

    async def prepare_coin_margined_capital(
        self,
        *,
        expected_asset: Optional[str] = None,
    ) -> tuple[CoinMarginedCapitalSnapshot, List[dict]]:
        snapshot, events = await prepare_coin_margined_capital(
            self.client,
            account_name=self.account_name,
            collateral_store=self._collateral_store,
            expected_asset=expected_asset,
        )
        self._capital_snapshot = snapshot
        return snapshot, events

    def register_prepared_coin_capital(
        self,
        events: Iterable[Mapping[str, object]],
    ) -> List[dict]:
        primary = self.primary_lane()
        bucket = primary.engine.profit_bucket
        metadata = self._collateral_store.load_strict()
        persisted_asset = str(metadata.get("asset") or "").strip().upper()
        registered: List[dict] = []
        for event in events:
            if event.get("action") != "incremental_coin_capital_ready":
                continue
            asset = str(event.get("asset") or "").strip().upper()
            units = Decimal(str(event.get("asset_units") or 0))
            amount = Decimal(str(event.get("amount") or 0))
            reference = str(
                event.get("capital_reference") or ""
            ).strip()
            if (
                asset != persisted_asset
                or units <= ZERO
                or amount <= ZERO
                or not reference
            ):
                raise RuntimeError(
                    "coin-margined incremental capital event is invalid"
                )
            baseline = max(
                0.0,
                float(bucket.capital_contribution_total or 0),
            )
            lot, allocation, created = (
                self.coordinator.register_external_capital(
                    amount=amount,
                    source_asset=asset,
                    external_reference=reference,
                    rows=(),
                    collateralized=True,
                    capital_contribution_before=baseline,
                    fixed_symbol=self.primary_symbol,
                )
            )
            target = float(lot.capital_contribution_before + lot.amount)
            if (
                float(bucket.capital_contribution_total or 0)
                < target - 1e-8
            ):
                primary.engine.record_direct_coin_capital(
                    amount=float(lot.amount),
                    source_asset=asset,
                    capital_contribution_before=float(
                        lot.capital_contribution_before
                    ),
                )
            pending = metadata.get("pending_capital")
            pending_matches = (
                isinstance(pending, Mapping)
                and str(pending.get("capital_reference") or "").strip()
                == reference
            )
            if created or pending_matches:
                previous_units = Decimal(
                    str(
                        metadata.get("accounted_asset_units")
                        or metadata.get("asset_units")
                        or 0
                    )
                )
                metadata["accounted_asset_units"] = str(
                    previous_units + units
                )
            if pending_matches:
                metadata.pop("pending_capital", None)
            self._collateral_store.save(metadata)
            self._refresh_lane_budget(primary)
            registered.append(
                {
                    "type": "coin_margined_capital",
                    "action": (
                        "incremental_coin_capital_registered"
                        if created
                        else "incremental_coin_capital_duplicate_ignored"
                    ),
                    "asset": asset,
                    "asset_units": str(units),
                    "amount": float(amount),
                    "capital_reference": reference,
                    "allocation": allocation.to_dict(),
                    "collateralized": True,
                    "execution_wired": True,
                }
            )
        return registered

    def _save_initial_collateral(
        self,
        snapshot: CoinMarginedCapitalSnapshot,
    ) -> None:
        self._collateral_store.save(
            {
                "version": 1,
                "strategy_type": "coin_margined_rwusd_strategy",
                "asset": snapshot.asset,
                "asset_units": str(snapshot.asset_units),
                "asset_price": str(snapshot.asset_price),
                "symbol": COIN_SYMBOLS[snapshot.asset],
                "usdc_value": str(snapshot.usdc_value),
                "accounted_asset_units": str(snapshot.asset_units),
                "observed_at_ms": snapshot.observed_at_ms,
            }
        )

    def primary_lane(self) -> RWUSDSymbolLane:
        lanes = self.lanes()
        if not lanes:
            raise RuntimeError("coin-margined RWUSD primary lane is missing")
        return lanes[0]

    def _lane_capital_basis(self, lane: RWUSDSymbolLane) -> float:
        # Initial coin collateral is NOT part of rwusd_principal. Only
        # officially collateralized harvest is credited to this principal.
        return float(self.coordinator.symbol_capital(lane.symbol)) + max(
            0.0, float(lane.engine.profit_bucket.rwusd_principal or 0)
        )

    def ensure_lanes(self) -> List[RWUSDSymbolLane]:
        if len(self._lanes) > 1:
            raise RuntimeError(
                "coin-margined RWUSD cannot operate multiple symbol lanes"
            )
        if self._lanes:
            return list(self._lanes.values())
        self._register_primary_lane()
        return list(self._lanes.values())

    def _register_primary_lane(self) -> None:
        if self.primary_symbol in self._lanes:
            return
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

    def _mark_direct_collateral(
        self,
        snapshot: CoinMarginedCapitalSnapshot,
    ) -> None:
        bucket = self.primary_engine.profit_bucket
        if bucket.initial_collateralization_completed:
            if bucket.initial_collateralization_mode != (
                "coin_margined_direct_collateral"
            ):
                raise RuntimeError(
                    "coin-margined RWUSD state has an incompatible "
                    "initial collateralization mode"
                )
            return
        else:
            bucket.initial_collateralization_completed = True
            bucket.initial_collateralization_mode = (
                "coin_margined_direct_collateral"
            )
            bucket.initial_collateralization_source_amount = float(
                snapshot.usdc_value
            )
            bucket.initial_collateralization_source_amounts = {
                snapshot.asset: float(snapshot.usdc_value)
            }
            bucket.capital_contribution_total = float(snapshot.usdc_value)
        bucket.sweep_block_reason = None
        bucket.last_rebalance_action = "coin_margined_initial_collateral_ready"
        self.primary_engine.state.last_action = (
            "coin_margined_initial_collateral_ready"
        )
        self.primary_engine.persist()

    def official_capital_bootstrap_required(self) -> bool:
        return False

    def reserve_allocation_due(self) -> bool:
        return False

    def allocate_reserve(
        self,
        allocation_rows: Iterable[Mapping[str, object]],
    ) -> Optional[dict]:
        """Never let a copied multi-asset allocator create a second lane."""
        return None

    def clear_completed_exit_for_restart(self) -> bool:
        """Explicit restart revalues the retained coin, not stablecoin cash."""
        snapshot = self._capital_snapshot
        if (
            snapshot is None
            or COIN_SYMBOLS[snapshot.asset] != self.primary_symbol
            or self.primary_engine.exit_state.stage != RWUSDExitStage.COMPLETE
        ):
            return False
        if not super().clear_completed_exit_for_restart():
            return False
        self.coordinator.bootstrap_capital(
            snapshot=snapshot.official,
            rows=(),
            source="coin_margined_portfolio_margin",
            fixed_symbol=self.primary_symbol,
        )
        self.coordinator.mark_pending_capital_collateralized(
            snapshot.usdc_value,
            source_asset=snapshot.asset,
        )
        self._mark_direct_collateral(snapshot)
        self._save_initial_collateral(snapshot)
        self._refresh_lane_budget(self.primary_lane())
        return True

    def official_capital_bootstrap_due(self) -> bool:
        return False

    async def advance_capital_sources(
        self,
        *,
        allocation_market: object,
        now_ms: int,
    ) -> tuple[List[dict], List[Mapping[str, object]]]:
        del allocation_market, now_ms
        if not self.execution_enabled:
            return [], []
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
            self._last_coin_capital_scan_monotonic > 0
            and (
                time.monotonic() - self._last_coin_capital_scan_monotonic
                < interval
            )
        ):
            return [], []
        self._last_coin_capital_scan_monotonic = time.monotonic()
        metadata = self._collateral_store.load_strict()
        expected_asset = str(metadata.get("asset") or "").strip().upper()
        _, scan_events = await self.prepare_coin_margined_capital(
            expected_asset=expected_asset or None,
        )
        events = list(scan_events)
        events.extend(self.register_prepared_coin_capital(scan_events))
        return events, []

    async def advance_incremental_capital(
        self,
        portfolio: object,
        *,
        allocation_market: object,
        allow_new_capital: bool,
    ) -> tuple[List[dict], bool]:
        """Keep the account limited to its original BTC/ETH collateral lot."""
        del portfolio, allocation_market, allow_new_capital
        return [], False

    async def advance_account_subscription(
        self,
        portfolio: object,
        *,
        allow_new_capital: bool,
    ) -> tuple[List[dict], bool]:
        """Allow harvest RWUSD subscriptions, never external capital funding."""
        pending = self.pending_subscription_lane()
        if pending is not None:
            pending_kind = str(
                pending.engine.profit_bucket.pending_subscription_kind
                or "harvest"
            ).strip().lower()
            if pending_kind in {"capital", "capital_existing_rwusd"}:
                raise RuntimeError(
                    "coin-margined RWUSD cannot process an external "
                    "capital subscription"
                )
        return await super().advance_account_subscription(
            portfolio,
            allow_new_capital=False,
        )

    def finalize_initial_capital_accounting(self) -> List[dict]:
        return []

    def account_profit_bucket(self) -> dict:
        payload = super().account_profit_bucket()
        metadata = self._collateral_store.load()
        payload.update(
            {
                "initial_collateral_asset": metadata.get("asset"),
                "initial_collateral_units": float(
                    metadata.get("asset_units", 0) or 0
                ),
                "initial_collateralization_source_amount": float(
                    metadata.get("usdc_value", 0) or 0
                ),
            }
        )
        return payload

    def status(self) -> dict:
        payload = super().status()
        payload.update(
            {
                "strategy_type": "coin_margined_rwusd_strategy",
                "state_namespace": COIN_MARGINED_NAMESPACE,
                "single_lane": True,
                "coin_margined": True,
            }
        )
        return payload


class CoinMarginedRWUSDStrategy(CoinMarginedRWUSDBaseStrategy):
    """Independent strategy entry point for a single BTC or ETH PM account."""

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
        del total_capital, symbol
        if force_dry_run or bool(getattr(binance_client, "is_testnet", False)):
            raise ValueError(
                "coin-margined RWUSD requires Binance Portfolio Margin mainnet"
            )
        super().__init__(
            account_name=account_name,
            binance_client=binance_client,
            total_capital=0.0,
            position_margin_percent=position_margin_percent,
            leverage=leverage,
            harvest_min_net_usd=harvest_min_net_usd,
            force_dry_run=False,
            symbol="BTCUSDC",
            on_signal_callback=on_signal_callback,
            on_trade_callback=on_trade_callback,
            on_error_callback=on_error_callback,
        )
        if not self.execution_enabled or self.executor is None:
            raise ValueError(
                "coin-margined RWUSD requires live Binance PM execution"
            )
        self.engine = RWUSDEngine(
            account_name=account_name,
            initial_capital=0.0,
            config=self.strategy_config,
            external_execution=True,
            execution_environment="binance_portfolio_margin_live",
            live_rwusd_execution=True,
            leverage=min(self.target_leverage, 20),
            state_store=CoinMarginedStateStore(account_name),
        )
        self.executor = BinanceRWUSDExecutor(
            account_name=account_name,
            client=self.client,
            engine=self.engine,
            config=self.strategy_config,
            total_margin_fraction=self.position_margin_percent,
            leverage=self.target_leverage,
        )
        self.portfolio_runtime = CoinMarginedPortfolioRuntime(
            account_name=account_name,
            client=self.client,
            config=self.strategy_config,
            primary_engine=self.engine,
            primary_executor=self.executor,
            total_margin_fraction=self.position_margin_percent,
        )

    async def initialize(self) -> bool:
        if str(getattr(self.client, "exchange", "") or "").lower() != "binance":
            await self._emit_error("coin-margined RWUSD only supports Binance")
            return False
        try:
            self.engine.restore()
            self.portfolio_runtime.restore()
            metadata = self.portfolio_runtime._collateral_store.load_strict()
            expected_asset = str(metadata.get("asset") or "").strip().upper()
            snapshot, wallet_events = (
                await self.portfolio_runtime.prepare_coin_margined_capital(
                    expected_asset=expected_asset or None,
                )
            )
            self.portfolio_runtime.initialize(
                allocation_rows=(),
                fallback_symbol="BTCUSDC",
            )
            events = list(wallet_events)
            events.extend(
                self.portfolio_runtime.register_prepared_coin_capital(
                    wallet_events
                )
            )
            self.symbol = self.portfolio_runtime.primary_symbol
            rows, portfolio = await self._build_runtime_snapshot()
            row_by_symbol = {
                str(row.get("symbol") or "").strip().upper(): row
                for row in rows
            }
            row = row_by_symbol.get(self.symbol)
            if row is None:
                raise RuntimeError("coin-margined collateral market row unavailable")
            self.engine.observe(
                rows=[row],
                portfolio=portfolio,
                now=datetime.now(timezone.utc),
            )
            events.extend(
                await self.portfolio_runtime.inspect_lanes_read_only(
                    portfolio=portfolio
                )
            )
            if events:
                await self._emit_events(events)
            self._last_cycle_monotonic = time.monotonic()
            self._last_error = None
            self.engine.state.last_error = None
            self.engine.persist()
            await self._emit_signal(
                "initialized",
                "币本位RWUSD Binance Portfolio Margin 主网初始化完成；"
                "已扫描单一 BTC/ETH 抵押资产，未下单",
            )
            return True
        except Exception as exc:
            self._last_error = str(exc)
            self.engine.state.last_error = self._last_error
            await self._emit_error(f"币本位RWUSD初始化失败: {exc}")
            return False
