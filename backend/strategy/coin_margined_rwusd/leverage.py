"""Runtime synchronization for Binance UM leverage."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple


_EXCHANGE_OPEN_TIME_RE = re.compile(
    r"\bby\s+(\d{4}-\d{2}-\d{2} \d{2}:\d{2})\s+\(UTC\)",
    re.IGNORECASE,
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _future_utc(seconds: float) -> str:
    return datetime.fromtimestamp(
        time.time() + max(0.0, float(seconds)),
        tz=timezone.utc,
    ).isoformat()


def _exchange_retry_epoch(error: object) -> float:
    message = str(error or "")
    if "-4300" not in message:
        return 0.0
    matched = _EXCHANGE_OPEN_TIME_RE.search(message)
    if matched is None:
        return 0.0
    try:
        available_at = datetime.strptime(
            matched.group(1),
            "%Y-%m-%d %H:%M",
        ).replace(tzinfo=timezone.utc)
    except ValueError:
        return 0.0
    return available_at.timestamp() + 60.0


@dataclass
class _LeverageRecord:
    symbol: str
    target_leverage: int
    exchange_leverage: int
    max_notional_value: float
    status: str
    checked_monotonic: float
    checked_at: str
    attempted_monotonic: float = 0.0
    retry_at_epoch: float = 0.0
    attempted_at: Optional[str] = None
    next_retry_at: Optional[str] = None
    last_error: Optional[str] = None


@dataclass(frozen=True)
class LeverageSnapshot:
    symbol: str
    target_leverage: int
    exchange_leverage: int
    sizing_leverage: int
    max_notional_value: float
    status: str
    checked_at: Optional[str]
    attempted_at: Optional[str]
    next_retry_at: Optional[str]
    last_error: Optional[str]

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "target_leverage": self.target_leverage,
            "exchange_leverage": self.exchange_leverage,
            "sizing_leverage": self.sizing_leverage,
            "max_notional_value": self.max_notional_value,
            "status": self.status,
            "checked_at": self.checked_at,
            "attempted_at": self.attempted_at,
            "next_retry_at": self.next_retry_at,
            "last_error": self.last_error,
        }


class BinanceUMLeverageManager:
    """Keep configured target separate from exchange-confirmed leverage."""

    def __init__(
        self,
        *,
        client: object,
        target_leverage: int,
        initial_leverage: int,
        refresh_sec: float,
        retry_sec: float,
    ) -> None:
        self.client = client
        self.target_leverage = max(1, int(target_leverage))
        self.initial_leverage = min(
            self.target_leverage,
            max(1, int(initial_leverage)),
        )
        self.refresh_sec = max(60.0, float(refresh_sec))
        self.retry_sec = max(60.0, float(retry_sec))
        self._records: Dict[str, _LeverageRecord] = {}

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        target = str(symbol or "").strip().upper()
        if not target:
            raise RuntimeError("RWUSD leverage synchronization requires a symbol")
        return target

    @staticmethod
    def _positive_float(value: object) -> float:
        try:
            parsed = float(value or 0)
        except (TypeError, ValueError):
            return 0.0
        return max(0.0, parsed)

    @classmethod
    def _parse_symbol_config(
        cls,
        symbol: str,
        payload: object,
    ) -> Tuple[int, float]:
        candidates = list(payload) if isinstance(payload, list) else [payload]
        row = next(
            (
                item
                for item in candidates
                if isinstance(item, dict)
                and str(item.get("symbol") or "").strip().upper() == symbol
            ),
            None,
        )
        if not isinstance(row, dict):
            raise RuntimeError(
                f"RWUSD official leverage configuration is unavailable for {symbol}"
            )
        try:
            leverage = int(row.get("leverage", 0) or 0)
        except (TypeError, ValueError):
            leverage = 0
        if leverage <= 0:
            raise RuntimeError(
                f"RWUSD official leverage configuration is incomplete for {symbol}"
            )
        return leverage, cls._positive_float(row.get("maxNotionalValue"))

    @staticmethod
    def _status(exchange_leverage: int, target_leverage: int) -> str:
        if exchange_leverage >= target_leverage:
            return (
                "target_active"
                if exchange_leverage == target_leverage
                else "target_capped"
            )
        return "upgrade_pending"

    def _snapshot(
        self,
        symbol: str,
        *,
        fallback_leverage: Optional[int] = None,
    ) -> LeverageSnapshot:
        target = self._normalize_symbol(symbol)
        record = self._records.get(target)
        if record is None:
            exchange = min(
                self.target_leverage,
                max(
                    1,
                    int(
                        self.initial_leverage
                        if fallback_leverage is None
                        else fallback_leverage
                    ),
                ),
            )
            return LeverageSnapshot(
                symbol=target,
                target_leverage=self.target_leverage,
                exchange_leverage=exchange,
                sizing_leverage=min(exchange, self.target_leverage),
                max_notional_value=0.0,
                status="unverified",
                checked_at=None,
                attempted_at=None,
                next_retry_at=None,
                last_error=None,
            )
        return LeverageSnapshot(
            symbol=record.symbol,
            target_leverage=record.target_leverage,
            exchange_leverage=record.exchange_leverage,
            sizing_leverage=min(
                record.exchange_leverage,
                record.target_leverage,
            ),
            max_notional_value=record.max_notional_value,
            status=record.status,
            checked_at=record.checked_at,
            attempted_at=record.attempted_at,
            next_retry_at=record.next_retry_at,
            last_error=record.last_error,
        )

    def snapshot(
        self,
        symbol: str,
        *,
        fallback_leverage: Optional[int] = None,
    ) -> LeverageSnapshot:
        return self._snapshot(
            symbol,
            fallback_leverage=fallback_leverage,
        )

    def invalidate(self) -> None:
        self._records.clear()

    async def sync(
        self,
        symbol: str,
        *,
        allow_upgrade: bool,
        force: bool = False,
    ) -> Tuple[LeverageSnapshot, Optional[dict]]:
        target = self._normalize_symbol(symbol)
        now = time.monotonic()
        record = self._records.get(target)
        should_check = bool(
            force
            or record is None
            or now - record.checked_monotonic >= self.refresh_sec
        )
        if should_check:
            try:
                payload = await self.client.get_um_symbol_config(target)
                exchange_leverage, maximum = self._parse_symbol_config(
                    target,
                    payload,
                )
                target_active = exchange_leverage >= self.target_leverage
                record = _LeverageRecord(
                    symbol=target,
                    target_leverage=self.target_leverage,
                    exchange_leverage=exchange_leverage,
                    max_notional_value=maximum,
                    status=self._status(
                        exchange_leverage,
                        self.target_leverage,
                    ),
                    checked_monotonic=now,
                    checked_at=_utc_now(),
                    attempted_monotonic=(
                        record.attempted_monotonic if record else 0.0
                    ),
                    retry_at_epoch=(
                        0.0
                        if target_active
                        else (record.retry_at_epoch if record else 0.0)
                    ),
                    attempted_at=record.attempted_at if record else None,
                    next_retry_at=(
                        None
                        if target_active
                        else (record.next_retry_at if record else None)
                    ),
                    last_error=(
                        None
                        if target_active
                        else (record.last_error if record else None)
                    ),
                )
                self._records[target] = record
            except Exception as exc:
                if record is None:
                    raise RuntimeError(
                        f"RWUSD official leverage check failed for {target}: {exc}"
                    ) from exc
                record.checked_monotonic = now
                record.checked_at = _utc_now()
                record.status = "check_failed"
                record.last_error = str(exc)
                return self._snapshot(target), None

        if record is None:
            raise RuntimeError(
                f"RWUSD leverage state is unavailable for {target}"
            )
        attempt_due = bool(
            (
                record.attempted_monotonic <= 0
                or now - record.attempted_monotonic >= self.retry_sec
            )
            and (
                record.retry_at_epoch <= 0
                or time.time() >= record.retry_at_epoch
            )
        )
        if (
            not allow_upgrade
            or record.exchange_leverage >= self.target_leverage
            or not attempt_due
        ):
            return self._snapshot(target), None

        record.attempted_monotonic = now
        record.retry_at_epoch = 0.0
        record.attempted_at = _utc_now()
        record.next_retry_at = _future_utc(self.retry_sec)
        try:
            response = await self.client.set_um_leverage(
                target,
                self.target_leverage,
            )
        except Exception as exc:
            record.status = "upgrade_waiting"
            record.last_error = str(exc)
            exchange_retry_at = _exchange_retry_epoch(exc)
            if exchange_retry_at > time.time():
                record.retry_at_epoch = exchange_retry_at
                record.next_retry_at = datetime.fromtimestamp(
                    exchange_retry_at,
                    tz=timezone.utc,
                ).isoformat()
            snapshot = self._snapshot(target)
            return snapshot, {
                "type": "rwusd_leverage",
                "action": "upgrade_deferred",
                **snapshot.to_dict(),
                "simulated": False,
            }

        response_maximum = (
            self._positive_float((response or {}).get("maxNotionalValue"))
            if isinstance(response, dict)
            else 0.0
        )
        try:
            confirmed = await self.client.get_um_symbol_config(target)
            exchange_leverage, maximum = self._parse_symbol_config(
                target,
                confirmed,
            )
        except Exception as exc:
            record.status = "upgrade_unconfirmed"
            record.last_error = str(exc)
            if response_maximum > 0:
                record.max_notional_value = response_maximum
            snapshot = self._snapshot(target)
            return snapshot, {
                "type": "rwusd_leverage",
                "action": "upgrade_unconfirmed",
                **snapshot.to_dict(),
                "simulated": False,
            }

        record.exchange_leverage = exchange_leverage
        record.max_notional_value = maximum or response_maximum
        record.checked_monotonic = time.monotonic()
        record.checked_at = _utc_now()
        record.status = self._status(
            exchange_leverage,
            self.target_leverage,
        )
        if exchange_leverage >= self.target_leverage:
            record.last_error = None
            record.retry_at_epoch = 0.0
            record.next_retry_at = None
            action = "upgraded"
        else:
            record.last_error = (
                "Binance confirmed leverage below the configured target"
            )
            action = "upgrade_deferred"
        snapshot = self._snapshot(target)
        return snapshot, {
            "type": "rwusd_leverage",
            "action": action,
            **snapshot.to_dict(),
            "simulated": False,
        }
