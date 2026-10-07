"""Small account-level registry for isolated RWUSD symbol lanes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Mapping


@dataclass(frozen=True)
class RWUSDOwnershipSnapshot:
    owns_account: bool
    managed_symbols: frozenset[str]
    client_order_symbols: Mapping[str, str]
    ignored_symbols: frozenset[str] = frozenset()


class RWUSDExecutionOwnership:
    """Expose exact symbol and open-order ownership across one account."""

    def __init__(self) -> None:
        self._engines: Dict[str, object] = {}
        self._ignored_symbols: frozenset[str] = frozenset()

    def set_ignored_symbols(self, symbols: Iterable[str]) -> None:
        self._ignored_symbols = frozenset(
            str(symbol or "").strip().upper()
            for symbol in symbols
            if str(symbol or "").strip()
        )

    def register(self, symbol: str, engine: object) -> None:
        target = str(symbol or "").strip().upper()
        if not target:
            raise ValueError("RWUSD lane symbol is required")
        existing = self._engines.get(target)
        if existing is not None and existing is not engine:
            raise RuntimeError(f"duplicate RWUSD lane ownership: {target}")
        self._engines[target] = engine

    def unregister(self, symbol: str) -> None:
        self._engines.pop(str(symbol or "").strip().upper(), None)

    def snapshot(self) -> RWUSDOwnershipSnapshot:
        managed_symbols: set[str] = set()
        client_order_symbols: Dict[str, str] = {}
        owns_account = False
        for symbol, engine in self._engines.items():
            established = bool(engine.exchange_ownership_established())
            owns_account = owns_account or established
            if established:
                managed_symbols.add(symbol)
            for order in engine.lifecycle.active_orders:
                client_id = str(
                    order.external_client_order_id or ""
                ).strip()
                if not client_id:
                    continue
                previous = client_order_symbols.get(client_id)
                if previous is not None and previous != symbol:
                    raise RuntimeError(
                        f"duplicate RWUSD client order ownership: {client_id}"
                    )
                client_order_symbols[client_id] = symbol
                managed_symbols.add(symbol)
        return RWUSDOwnershipSnapshot(
            owns_account=owns_account,
            managed_symbols=frozenset(managed_symbols),
            ignored_symbols=self._ignored_symbols,
            client_order_symbols=client_order_symbols,
        )

    def status(self) -> dict:
        snapshot = self.snapshot()
        return {
            "owns_account": snapshot.owns_account,
            "managed_symbols": sorted(snapshot.managed_symbols),
            "ignored_symbols": sorted(snapshot.ignored_symbols),
            "tracked_open_order_count": len(
                snapshot.client_order_symbols
            ),
        }
