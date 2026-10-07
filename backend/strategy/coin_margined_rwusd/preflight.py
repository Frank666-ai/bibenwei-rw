"""Shared account-level Binance preflight snapshots for RWUSD lanes."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Mapping, Optional


def _records(payload: object, label: str) -> tuple[dict, ...]:
    if not isinstance(payload, (list, tuple)):
        raise RuntimeError(f"RWUSD {label} response is invalid")
    rows = []
    for row in payload:
        if not isinstance(row, Mapping):
            raise RuntimeError(f"RWUSD {label} record is invalid")
        rows.append(dict(row))
    return tuple(rows)


def _required_bool(
    payload: Mapping[str, object],
    field: str,
    label: str,
) -> bool:
    value = payload.get(field)
    if not isinstance(value, bool):
        raise RuntimeError(f"RWUSD {label} field is invalid: {field}")
    return value


@dataclass(frozen=True)
class RWUSDAccountPreflightSnapshot:
    """One shared UM account preflight view for every symbol lane."""

    hedge_mode: Optional[bool]
    positions: tuple[dict, ...]
    open_orders: tuple[dict, ...]

    @classmethod
    async def load(
        cls,
        client: object,
        *,
        include_hedge_mode: bool,
    ) -> "RWUSDAccountPreflightSnapshot":
        requests = [
            client.get_um_positions(),
            client.get_um_open_orders(),
        ]
        if include_hedge_mode:
            requests.insert(0, client.get_um_position_mode())
        results = await asyncio.gather(
            *requests,
            return_exceptions=True,
        )
        errors = [
            str(result)
            for result in results
            if isinstance(result, Exception)
        ]
        if errors:
            raise RuntimeError(
                "RWUSD account preflight failed: " + "; ".join(errors)
            )
        offset = 1 if include_hedge_mode else 0
        hedge_mode = None
        if include_hedge_mode:
            if not isinstance(results[0], bool):
                raise RuntimeError(
                    "RWUSD position mode response is invalid"
                )
            hedge_mode = results[0]
        return cls(
            hedge_mode=hedge_mode,
            positions=_records(results[offset], "position"),
            open_orders=_records(results[offset + 1], "open order"),
        )


@dataclass(frozen=True)
class RWUSDMainnetPreflightSnapshot:
    """One shared RWUSD/SAPI safety view for every symbol lane."""

    rwusd_account: dict
    quota: dict
    collateral: dict
    transferable: dict
    permissions: dict
    trading_status: dict
    errors: tuple[str, ...]

    @classmethod
    async def load(cls, client: object) -> "RWUSDMainnetPreflightSnapshot":
        labels = (
            "account",
            "quota",
            "collateral",
            "transferable",
            "api_permissions",
            "api_trading_status",
        )
        results = await asyncio.gather(
            client.get_rwusd_account(),
            client.get_rwusd_quota(),
            client.get_rwusd_collateral_balance(),
            client.get_rwusd_transferable_balance(),
            client.get_api_key_permissions(),
            client.get_api_trading_status(),
            return_exceptions=True,
        )
        payloads: dict[str, dict] = {}
        errors = []
        for label, result in zip(labels, results):
            if isinstance(result, Exception):
                errors.append(f"{label}:{result}")
            elif isinstance(result, Mapping):
                payloads[label] = dict(result)
            else:
                errors.append(f"{label}:invalid_payload")
        return cls(
            rwusd_account=payloads.get("account", {}),
            quota=payloads.get("quota", {}),
            collateral=payloads.get("collateral", {}),
            transferable=payloads.get("transferable", {}),
            permissions=payloads.get("api_permissions", {}),
            trading_status=payloads.get("api_trading_status", {}),
            errors=tuple(errors),
        )

    def permission_status(self) -> dict[str, bool]:
        fields = {
            "enable_reading": "enableReading",
            "enable_portfolio_margin_trading": (
                "enablePortfolioMarginTrading"
            ),
            "enable_spot_and_margin_trading": (
                "enableSpotAndMarginTrading"
            ),
            "permits_universal_transfer": "permitsUniversalTransfer",
            "ip_restricted": "ipRestrict",
        }
        status = {
            name: _required_bool(
                self.permissions,
                field,
                "API key permission",
            )
            for name, field in fields.items()
        }
        status["withdrawals_disabled"] = not _required_bool(
            self.permissions,
            "enableWithdrawals",
            "API key permission",
        )
        return status

    def trading_locked(self) -> bool:
        details = self.trading_status.get("data")
        if not isinstance(details, Mapping):
            raise RuntimeError(
                "RWUSD mainnet API trading lock status is unavailable"
            )
        return _required_bool(
            details,
            "isLocked",
            "API trading status",
        )

    def validate_security(self) -> None:
        security_errors = [
            error
            for error in self.errors
            if error.startswith(("api_permissions:", "api_trading_status:"))
        ]
        if security_errors:
            raise RuntimeError(
                "RWUSD mainnet preflight failed: "
                + "; ".join(security_errors)
            )
        status = self.permission_status()
        violations = [
            name
            for name, allowed in status.items()
            if not allowed
        ]
        if violations:
            raise RuntimeError(
                "RWUSD mainnet API key safety requirements failed: "
                + ",".join(violations)
            )
        if self.trading_locked():
            raise RuntimeError("RWUSD mainnet API trading is locked")

    def validate_read_only(self) -> None:
        if self.errors:
            raise RuntimeError(
                "RWUSD read-only preflight failed: "
                + "; ".join(self.errors)
            )
        self.validate_security()


@dataclass(frozen=True)
class RWUSDOperationalPreflight:
    quota: dict[str, object]
    collateral: dict[str, object]
    quota_source_assets: tuple[str, ...]
    errors: tuple[str, ...]
    events: tuple[dict, ...]


RWUSDPreflightSnapshots = tuple[
    RWUSDAccountPreflightSnapshot,
    Optional[RWUSDMainnetPreflightSnapshot],
]


async def load_rwusd_preflight_snapshots(
    client: object,
    *,
    include_mainnet: bool,
) -> RWUSDPreflightSnapshots:
    account_request = RWUSDAccountPreflightSnapshot.load(
        client,
        include_hedge_mode=True,
    )
    if not include_mainnet:
        return await account_request, None
    account, mainnet = await asyncio.gather(
        account_request,
        RWUSDMainnetPreflightSnapshot.load(client),
    )
    return account, mainnet
