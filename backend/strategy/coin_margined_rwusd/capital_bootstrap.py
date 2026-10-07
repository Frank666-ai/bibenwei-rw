"""Official Binance asset detection for RWUSD capital bootstrap."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable, Mapping

from backend.strategy.coin_margined_rwusd.portfolio import (
    ZERO,
    money,
    nonnegative_money,
)


@dataclass(frozen=True)
class DetectedCapitalComponent:
    external_reference: str
    source_asset: str
    source_wallet: str
    amount: Decimal

    def to_dict(self) -> dict:
        return {
            "external_reference": self.external_reference,
            "source_asset": self.source_asset,
            "source_wallet": self.source_wallet,
            "amount": str(self.amount),
        }


@dataclass(frozen=True)
class OfficialCapitalSnapshot:
    environment: str
    observed_at_ms: int
    components: tuple[DetectedCapitalComponent, ...]
    collateralization_premium: Decimal = ZERO

    @property
    def external_principal(self) -> Decimal:
        return sum(
            (component.amount for component in self.components),
            ZERO,
        )

    @property
    def collateralization_target(self) -> Decimal:
        return self.external_principal + self.collateralization_premium

    @property
    def stability_key(self) -> tuple[object, ...]:
        return (
            self.environment,
            tuple(
                sorted(
                    (
                        component.external_reference,
                        component.source_asset,
                        component.source_wallet,
                        component.amount,
                    )
                    for component in self.components
                )
            ),
        )

    def to_dict(self) -> dict:
        return {
            "environment": self.environment,
            "observed_at_ms": self.observed_at_ms,
            "external_principal": str(self.external_principal),
            "collateralization_premium": str(
                self.collateralization_premium
            ),
            "collateralization_target": str(
                self.collateralization_target
            ),
            "components": [
                component.to_dict() for component in self.components
            ],
        }


async def detect_official_capital(
    client: object,
    *,
    account_name: str,
    execution_environment: str,
    source_assets: Iterable[str],
) -> OfficialCapitalSnapshot:
    environment = str(execution_environment or "").strip()
    if environment == "binance_futures_demo":
        return await _detect_demo_capital(
            client,
            account_name=account_name,
            environment=environment,
        )
    if environment == "binance_portfolio_margin_live":
        return await _detect_mainnet_capital(
            client,
            account_name=account_name,
            environment=environment,
            source_assets=source_assets,
        )
    raise RuntimeError(
        f"RWUSD official capital detection is unavailable for {environment!r}"
    )


async def _detect_demo_capital(
    client: object,
    *,
    account_name: str,
    environment: str,
) -> OfficialCapitalSnapshot:
    observed_at_ms = int(time.time() * 1000)
    payload = await _required_call(client, "get_account_info")
    data = _mapping(payload, "Binance Futures Demo account")
    if "source_wallet_balance" not in data:
        raise RuntimeError(
            "Binance Futures Demo USDC walletBalance is unavailable"
        )
    wallet_balance = _required_amount(
        data,
        "source_wallet_balance",
        "Binance Futures Demo USDC walletBalance",
    )
    components = (
        (
            DetectedCapitalComponent(
                external_reference=_reference(
                    account_name,
                    environment,
                    "futures_wallet",
                    "USDC",
                ),
                source_asset="USDC",
                source_wallet="futures_wallet",
                amount=wallet_balance,
            ),
        )
        if wallet_balance > ZERO
        else ()
    )
    return OfficialCapitalSnapshot(
        environment=environment,
        observed_at_ms=observed_at_ms,
        components=components,
    )


async def _detect_mainnet_capital(
    client: object,
    *,
    account_name: str,
    environment: str,
    source_assets: Iterable[str],
) -> OfficialCapitalSnapshot:
    observed_at_ms = int(time.time() * 1000)
    assets = _normalized_assets(source_assets)
    margin_loader = getattr(client, "get_margin_asset_balance", None)
    spot_loader = getattr(client, "get_spot_asset_balances", None)
    funding_loader = getattr(client, "get_funding_asset_balances", None)
    rwusd_loader = getattr(client, "get_rwusd_account", None)
    if not all(
        loader is not None
        for loader in (
            margin_loader,
            spot_loader,
            funding_loader,
            rwusd_loader,
        )
    ):
        raise RuntimeError(
            "Binance mainnet official capital interfaces are unavailable"
        )
    results = await asyncio.gather(
        *(margin_loader(asset) for asset in assets),
        spot_loader(list(assets)),
        funding_loader(list(assets)),
        rwusd_loader(),
    )
    margin_rows = results[: len(assets)]
    spot_rows = _mapping(
        results[len(assets)],
        "Binance Spot asset balances",
    )
    funding_rows = _mapping(
        results[len(assets) + 1],
        "Binance Funding asset balances",
    )
    rwusd_account = _mapping(
        results[len(assets) + 2],
        "Binance RWUSD account",
    )

    components: list[DetectedCapitalComponent] = []
    for asset, margin_payload in zip(assets, margin_rows):
        margin = _mapping(
            margin_payload,
            f"Binance Portfolio Margin {asset} balance",
        )
        _append_component(
            components,
            account_name=account_name,
            environment=environment,
            asset=asset,
            wallet="portfolio_margin",
            amount=_margin_wallet_amount(margin, asset),
        )
        _append_component(
            components,
            account_name=account_name,
            environment=environment,
            asset=asset,
            wallet="spot",
            amount=_asset_value(spot_rows, asset, "free"),
        )
        _append_component(
            components,
            account_name=account_name,
            environment=environment,
            asset=asset,
            wallet="funding",
            amount=_asset_value(funding_rows, asset, "free"),
        )

    if "rwusdAmount" not in rwusd_account or "totalProfit" not in rwusd_account:
        raise RuntimeError("Binance RWUSD principal fields are unavailable")
    rwusd_amount = _required_amount(
        rwusd_account,
        "rwusdAmount",
        "Binance RWUSD amount",
    )
    embedded_profit = min(
        rwusd_amount,
        _required_amount(
            rwusd_account,
            "totalProfit",
            "Binance RWUSD total profit",
        ),
    )
    rwusd_principal = max(ZERO, rwusd_amount - embedded_profit)
    if rwusd_principal > ZERO:
        components.append(
            DetectedCapitalComponent(
                external_reference=_reference(
                    account_name,
                    environment,
                    "rwusd",
                    "RWUSD",
                ),
                source_asset="RWUSD",
                source_wallet="rwusd",
                amount=rwusd_principal,
            )
        )
    return OfficialCapitalSnapshot(
        environment=environment,
        observed_at_ms=observed_at_ms,
        components=tuple(components),
        collateralization_premium=embedded_profit,
    )


def simulation_capital_snapshot(
    *,
    account_name: str,
    amount: object,
) -> OfficialCapitalSnapshot:
    capital = money(amount)
    components = (
        (
            DetectedCapitalComponent(
                external_reference=(
                    f"simulation-initial:{account_name}:v18"
                ),
                source_asset="USDC",
                source_wallet="simulation",
                amount=capital,
            ),
        )
        if capital > ZERO
        else ()
    )
    return OfficialCapitalSnapshot(
        environment="local_dry_run",
        observed_at_ms=int(time.time() * 1000),
        components=components,
    )


async def _required_call(client: object, method: str) -> object:
    loader = getattr(client, method, None)
    if loader is None:
        raise RuntimeError(f"Binance {method} interface is unavailable")
    return await loader()


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise RuntimeError(f"{label} returned invalid data")
    return value


def _normalized_assets(values: Iterable[str]) -> tuple[str, ...]:
    assets: list[str] = []
    for value in values:
        asset = str(value or "").strip().upper()
        if asset and asset not in assets:
            assets.append(asset)
    if not assets:
        raise ValueError("RWUSD source assets cannot be empty")
    return tuple(assets)


def _asset_value(
    rows: Mapping[str, object],
    asset: str,
    field: str,
) -> Decimal:
    row = rows.get(asset)
    if not isinstance(row, Mapping):
        raise RuntimeError(f"Binance {asset} wallet balance is unavailable")
    return _required_amount(
        row,
        field,
        f"Binance {asset} wallet {field} balance",
    )


def _margin_wallet_amount(
    payload: Mapping[str, object],
    asset: str,
) -> Decimal:
    raw = payload.get("raw")
    if isinstance(raw, Mapping) and "totalWalletBalance" in raw:
        return _required_amount(
            raw,
            "totalWalletBalance",
            f"Binance Portfolio Margin {asset} total wallet balance",
        )
    return _required_amount(
        payload,
        "wallet_balance",
        f"Binance Portfolio Margin {asset} wallet balance",
    )


def _required_amount(
    payload: Mapping[str, object],
    field: str,
    label: str,
) -> Decimal:
    if field not in payload:
        raise RuntimeError(f"{label} is unavailable")
    try:
        return nonnegative_money(payload.get(field), label)
    except ValueError as exc:
        raise RuntimeError(str(exc)) from exc


def _append_component(
    target: list[DetectedCapitalComponent],
    *,
    account_name: str,
    environment: str,
    asset: str,
    wallet: str,
    amount: object,
) -> None:
    value = nonnegative_money(amount, f"Binance {wallet} {asset} balance")
    if value <= ZERO:
        return
    target.append(
        DetectedCapitalComponent(
            external_reference=_reference(
                account_name,
                environment,
                wallet,
                asset,
            ),
            source_asset=asset,
            source_wallet=wallet,
            amount=value,
        )
    )


def _reference(
    account_name: str,
    environment: str,
    wallet: str,
    asset: str,
) -> str:
    return (
        "binance-bootstrap:"
        f"{environment}:{account_name}:{wallet}:{asset}:v18"
    )
