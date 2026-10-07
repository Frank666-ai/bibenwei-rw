"""Capacity-aware allocation of confirmed external RWUSD capital."""

from __future__ import annotations

import math
from dataclasses import dataclass, replace
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Iterable, List, Mapping, Optional, Sequence

from backend.strategy.coin_margined_rwusd.portfolio import (
    MONEY_QUANTUM,
    ZERO,
    money,
)


def _finite(value: object) -> Optional[float]:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _positive(value: object) -> float:
    parsed = _finite(value)
    return parsed if parsed is not None and parsed > 0 else 0.0


def _nonnegative(value: object) -> Optional[float]:
    parsed = _finite(value)
    return parsed if parsed is not None and parsed >= 0 else None


def _positive_integer(value: object) -> Optional[int]:
    parsed = _finite(value)
    if parsed is None or parsed <= 0 or not parsed.is_integer():
        return None
    return int(parsed)


def _pearson(left: Sequence[float], right: Sequence[float]) -> float:
    if len(left) != len(right):
        return 1.0
    size = len(left)
    if size < 24:
        return 0.0
    x = left[-size:]
    y = right[-size:]
    mean_x = sum(x) / size
    mean_y = sum(y) / size
    covariance = sum(
        (a - mean_x) * (b - mean_y)
        for a, b in zip(x, y)
    )
    variance_x = sum((value - mean_x) ** 2 for value in x)
    variance_y = sum((value - mean_y) ** 2 for value in y)
    denominator = math.sqrt(variance_x * variance_y)
    return covariance / denominator if denominator > 0 else 1.0


def _return_series(value: object) -> tuple[tuple[float, ...], bool]:
    if isinstance(value, (str, bytes, Mapping)) or value is None:
        return (), False
    try:
        raw_values = list(value)
    except TypeError:
        return (), False
    parsed: list[float] = []
    valid = True
    for raw in raw_values:
        item = _finite(raw)
        if item is None:
            valid = False
            continue
        parsed.append(item)
    return (tuple(parsed), True) if valid else ((), False)


@dataclass(frozen=True)
class AllocationCandidate:
    symbol: str
    score: float
    sizing_leverage: int
    safe_gross_capacity: float
    staged_entry_gross_capacity: float
    observed_gross_notional: float
    capital_headroom: float
    daily_return: float
    spread_bps: float
    thirty_minute_range: float
    thirty_minute_atr4: float
    reject_reason: str = ""
    returns: tuple[float, ...] = ()
    history_start_ms: int = 0
    history_end_ms: int = 0
    history_interval_ms: int = 0

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "score": self.score,
            "sizing_leverage": self.sizing_leverage,
            "safe_gross_capacity": self.safe_gross_capacity,
            "staged_entry_gross_capacity": (
                self.staged_entry_gross_capacity
            ),
            "observed_gross_notional": self.observed_gross_notional,
            "capital_headroom": self.capital_headroom,
            "daily_return": self.daily_return,
            "spread_bps": self.spread_bps,
            "thirty_minute_range": self.thirty_minute_range,
            "thirty_minute_atr4": self.thirty_minute_atr4,
            "history_start_ms": self.history_start_ms,
            "history_end_ms": self.history_end_ms,
            "history_interval_ms": self.history_interval_ms,
            "reject_reason": self.reject_reason or None,
        }


@dataclass(frozen=True)
class AllocationResult:
    requested_capital: Decimal
    deployable_capital: Decimal
    allocated_capital: Dict[str, Decimal]
    reserve_capital: Decimal
    candidates: Sequence[AllocationCandidate] = ()

    def to_dict(self) -> dict:
        return {
            "requested_capital": str(self.requested_capital),
            "deployable_capital": str(self.deployable_capital),
            "allocated_capital": {
                symbol: str(amount)
                for symbol, amount in self.allocated_capital.items()
            },
            "reserve_capital": str(self.reserve_capital),
            "candidates": [
                candidate.to_dict() for candidate in self.candidates
            ],
        }


class RWUSDCapitalAllocator:
    """Allocate only new external capital; profits never enter this path."""

    def __init__(
        self,
        *,
        margin_fraction: float,
        minimum_daily_return: float,
        maximum_spread_bps: float,
        official_capacity_haircut: float,
        liquidity_reserve_fraction: float,
        maximum_symbol_fraction: float,
        maximum_correlation_cluster_fraction: float,
        correlation_threshold: float,
        target_weights: Optional[Mapping[str, object]] = None,
        maximum_symbol_fractions: Optional[Mapping[str, object]] = None,
        core_symbols: Sequence[str] = (),
        minimum_core_fraction: float = 0.0,
        appreciation_exempt_symbols: Sequence[str] = (),
    ) -> None:
        self.margin_fraction = min(0.80, max(0.01, margin_fraction))
        self.minimum_daily_return = max(
            0.0,
            float(minimum_daily_return),
        )
        self.maximum_spread_bps = max(0.1, maximum_spread_bps)
        self.official_capacity_haircut = min(
            1.0,
            max(0.01, official_capacity_haircut),
        )
        self.liquidity_reserve_fraction = min(
            0.50,
            max(0.0, liquidity_reserve_fraction),
        )
        self.maximum_symbol_fraction = min(
            1.0,
            max(0.05, maximum_symbol_fraction),
        )
        self.target_weights = self._fraction_map(
            target_weights,
            maximum=1.0,
        )
        if sum(self.target_weights.values()) > 1.0 + 1e-12:
            raise ValueError("RWUSD allocation target weights exceed 100%")
        self.maximum_symbol_fractions = self._fraction_map(
            maximum_symbol_fractions,
            maximum=1.0,
        )
        self.core_symbols = frozenset(
            str(symbol or "").strip().upper()
            for symbol in core_symbols
            if str(symbol or "").strip()
        )
        self.appreciation_exempt_symbols = frozenset(
            str(symbol or "").strip().upper()
            for symbol in appreciation_exempt_symbols
            if str(symbol or "").strip()
        )
        self.minimum_core_fraction = min(
            1.0,
            max(0.0, float(minimum_core_fraction or 0.0)),
        )
        if self.minimum_core_fraction > 0 and not self.core_symbols:
            raise ValueError(
                "RWUSD allocation core symbols are required"
            )
        self.maximum_correlation_cluster_fraction = min(
            1.0,
            max(
                self.maximum_symbol_fraction,
                maximum_correlation_cluster_fraction,
            ),
        )
        self.correlation_threshold = min(
            1.0,
            max(0.0, correlation_threshold),
        )

    @staticmethod
    def _fraction_map(
        values: Optional[Mapping[str, object]],
        *,
        maximum: float,
    ) -> Dict[str, float]:
        normalized: Dict[str, float] = {}
        for raw_symbol, raw_value in (values or {}).items():
            symbol = str(raw_symbol or "").strip().upper()
            value = _finite(raw_value)
            if not symbol or value is None or value <= 0 or value > maximum:
                raise ValueError("RWUSD allocation fraction map is invalid")
            normalized[symbol] = value
        return normalized

    def build_candidates(
        self,
        *,
        rows: Iterable[Mapping[str, object]],
        current_allocated_capital: Mapping[str, object],
    ) -> List[AllocationCandidate]:
        current = {
            str(symbol).strip().upper(): _positive(amount)
            for symbol, amount in current_allocated_capital.items()
            if str(symbol).strip()
        }
        candidates: List[AllocationCandidate] = []
        seen: set[str] = set()
        for row in rows:
            symbol = str(row.get("symbol") or "").strip().upper()
            if symbol in seen:
                raise ValueError(f"duplicate allocation candidate: {symbol}")
            if symbol:
                seen.add(symbol)
            candidate = self._build_candidate(row, symbol, current)
            candidates.append(candidate)
        return sorted(
            candidates,
            key=lambda item: (
                bool(item.reject_reason),
                -item.score,
                item.symbol,
            ),
        )

    def _build_candidate(
        self,
        row: Mapping[str, object],
        symbol: str,
        current: Mapping[str, float],
    ) -> AllocationCandidate:
        try:
            leverage = max(0, int(row.get("sizing_leverage", 0) or 0))
        except (TypeError, ValueError):
            leverage = 0
        maximum_notional = _positive(row.get("max_notional_value"))
        observed_gross = _nonnegative(
            row.get("existing_gross_notional")
        )
        daily = _finite(row.get("daily_return"))
        spread = _nonnegative(row.get("execution_cost_bps"))
        range_30m = _nonnegative(row.get("thirty_minute_range"))
        atr4 = _finite(row.get("thirty_minute_atr4"))
        metrics = (daily, spread, range_30m, atr4)
        values = tuple(value if value is not None else 0.0 for value in metrics)
        (
            daily_value,
            spread_value,
            range_30m_value,
            atr4_value,
        ) = values
        returns, returns_valid = _return_series(row.get("return_series"))
        history_start = _positive_integer(row.get("history_start_ms"))
        history_end = _positive_integer(row.get("history_end_ms"))
        history_interval = _positive_integer(row.get("history_interval_ms"))
        reject_reason = str(row.get("reject_reason") or "").strip()
        if not symbol:
            reject_reason = reject_reason or "symbol_missing"
        elif bool(row.get("blocked")):
            reject_reason = reject_reason or "market_data_unavailable"
        elif any(value is None for value in metrics):
            reject_reason = "market_data_invalid"
        elif atr4_value <= 0:
            reject_reason = "market_data_invalid"
        elif (
            symbol not in self.appreciation_exempt_symbols
            and daily_value <= self.minimum_daily_return + 1e-12
        ):
            reject_reason = "daily_return_below_threshold"
        elif bool(row.get("opening_volatility_blocked")):
            reject_reason = str(
                row.get("opening_volatility_block_reason")
                or "opening_volatility_blocked"
            )
        elif spread_value > self.maximum_spread_bps + 1e-12:
            reject_reason = "spread_too_wide"
        elif leverage <= 0:
            reject_reason = "leverage_unconfirmed"
        elif maximum_notional <= 0:
            reject_reason = "official_capacity_unavailable"
        elif observed_gross is None:
            reject_reason = "position_exposure_unavailable"
        elif (
            not returns_valid
            or history_start is None
            or history_end is None
            or history_interval is None
            or history_end <= history_start
            or (
                history_end - history_start
                != len(returns) * history_interval
            )
        ):
            reject_reason = "correlation_history_invalid"
        elif len(returns) < 24:
            reject_reason = "correlation_history_incomplete"

        entry_capacity = _positive(
            row.get("staged_entry_gross_capacity")
        )
        if not reject_reason and entry_capacity <= 0:
            reject_reason = "entry_order_book_depth_unavailable"
        official_safe = maximum_notional * self.official_capacity_haircut
        safe_gross = (
            min(official_safe, entry_capacity)
            if entry_capacity > 0
            else official_safe
        )
        planned_gross = (
            current.get(symbol, 0.0)
            * self.margin_fraction
            * leverage
        )
        existing_gross = max(
            planned_gross,
            observed_gross if observed_gross is not None else 0.0,
        )
        headroom = (
            max(0.0, safe_gross - existing_gross)
            / self.margin_fraction
            / leverage
            if leverage > 0
            else 0.0
        )
        if not reject_reason and headroom <= 0:
            reject_reason = "safe_capacity_full"

        trend_quality = min(
            1.0,
            max(0.0, daily_value / max(self.minimum_daily_return, 0.10)),
        )
        calmness = (
            max(
                0.0,
                1.0 - min(1.0, range_30m_value / (1.5 * atr4_value)),
            )
            if atr4_value > 0
            else 0.0
        )
        execution_quality = max(
            0.0,
            1.0 - spread_value / self.maximum_spread_bps,
        )
        capacity_quality = min(
            1.0,
            headroom / max(current.get(symbol, 0.0), 1.0),
        )
        score = (
            0.30 * trend_quality
            + 0.25 * calmness
            + 0.25 * execution_quality
            + 0.20 * capacity_quality
        )
        return AllocationCandidate(
            symbol=symbol,
            score=score if not reject_reason else 0.0,
            sizing_leverage=leverage,
            safe_gross_capacity=safe_gross,
            staged_entry_gross_capacity=entry_capacity,
            observed_gross_notional=(
                observed_gross if observed_gross is not None else 0.0
            ),
            capital_headroom=headroom,
            daily_return=daily_value,
            spread_bps=spread_value,
            thirty_minute_range=range_30m_value,
            thirty_minute_atr4=atr4_value,
            reject_reason=reject_reason,
            returns=returns,
            history_start_ms=history_start or 0,
            history_end_ms=history_end or 0,
            history_interval_ms=history_interval or 0,
        )

    def allocate(
        self,
        *,
        amount: object,
        rows: Iterable[Mapping[str, object]],
        current_allocated_capital: Mapping[str, object],
        current_total_capital: Optional[object] = None,
        capital_base_override: Optional[object] = None,
        liquidity_reserve_fraction: Optional[float] = None,
    ) -> AllocationResult:
        requested = money(amount)
        if requested <= 0:
            raise ValueError("allocation amount must be positive")
        reserve_fraction = (
            self.liquidity_reserve_fraction
            if liquidity_reserve_fraction is None
            else min(0.50, max(0.0, float(liquidity_reserve_fraction)))
        )
        fixed_reserve = (
            requested * Decimal(str(reserve_fraction))
        ).quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)
        deployable = requested - fixed_reserve
        current = {
            str(symbol).strip().upper(): money(value)
            for symbol, value in current_allocated_capital.items()
            if str(symbol).strip() and money(value) > 0
        }
        candidates = self.build_candidates(
            rows=rows,
            current_allocated_capital=current,
        )
        if current:
            by_symbol = {
                candidate.symbol: candidate
                for candidate in candidates
            }
            current_history_missing = any(
                symbol not in by_symbol
                or not self._has_valid_history(by_symbol[symbol])
                for symbol in current
            )
            candidates = [
                replace(
                    candidate,
                    score=0.0,
                    reject_reason=(
                        candidate.reject_reason
                        or "portfolio_correlation_history_incomplete"
                    ),
                )
                if (
                        current_history_missing
                        or (
                            not candidate.reject_reason
                            and not self._has_valid_history(candidate)
                        )
                    )
                else candidate
                for candidate in candidates
            ]
        eligible = [
            candidate
            for candidate in candidates
            if not candidate.reject_reason
        ]
        if not eligible or deployable < MONEY_QUANTUM:
            return AllocationResult(
                requested_capital=requested,
                deployable_capital=deployable,
                allocated_capital={},
                reserve_capital=requested,
                candidates=candidates,
            )

        current_total = max(
            sum(current.values(), ZERO),
            money(current_total_capital),
        )
        base_override = money(capital_base_override)
        capital_base = (
            base_override
            if base_override > 0
            else current_total + requested
        )
        symbol_room = {
            candidate.symbol: min(
                Decimal(str(candidate.capital_headroom)),
                max(
                    ZERO,
                    (
                        capital_base
                        * Decimal(
                            str(
                                self.maximum_symbol_fractions.get(
                                    candidate.symbol,
                                    self.maximum_symbol_fraction,
                                )
                            )
                        )
                    ).quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)
                    - current.get(candidate.symbol, ZERO),
                ),
            ).quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)
            for candidate in eligible
        }
        cluster_candidates = [
            candidate
            for candidate in candidates
            if (
                not candidate.reject_reason
                or current.get(candidate.symbol, ZERO) > 0
            )
        ]
        clusters = self._correlation_clusters(cluster_candidates)
        cluster_for = {
            symbol: index
            for index, cluster in enumerate(clusters)
            for symbol in cluster
        }
        cluster_cap = (
            capital_base
            * Decimal(str(self.maximum_correlation_cluster_fraction))
        ).quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)
        cluster_room = {
            index: max(
                ZERO,
                cluster_cap
                - sum(
                    (current.get(symbol, ZERO) for symbol in cluster),
                    ZERO,
                ),
            )
            for index, cluster in enumerate(clusters)
        }
        allocation = {
            candidate.symbol: ZERO
            for candidate in eligible
        }
        remaining = self._target_fill(
            candidates=eligible,
            allocation=allocation,
            remaining=deployable,
            current=current,
            capital_base=capital_base,
            target_weights=self.target_weights,
            symbol_room=symbol_room,
            cluster_room=cluster_room,
            cluster_for=cluster_for,
            clusters=clusters,
        )
        remaining = self._weighted_fill(
            candidates=eligible,
            allocation=allocation,
            remaining=remaining,
            symbol_room=symbol_room,
            cluster_room=cluster_room,
            cluster_for=cluster_for,
            clusters=clusters,
        )
        remaining = self._enforce_core_fraction(
            candidates=eligible,
            allocation=allocation,
            remaining=remaining,
            current=current,
        )
        allocated = {
            symbol: value
            for symbol, value in allocation.items()
            if value > 0
        }
        return AllocationResult(
            requested_capital=requested,
            deployable_capital=deployable,
            allocated_capital=allocated,
            reserve_capital=(fixed_reserve + remaining).quantize(
                MONEY_QUANTUM,
                rounding=ROUND_DOWN,
            ),
            candidates=candidates,
        )

    def _enforce_core_fraction(
        self,
        *,
        candidates: Sequence[AllocationCandidate],
        allocation: Dict[str, Decimal],
        remaining: Decimal,
        current: Mapping[str, Decimal],
    ) -> Decimal:
        if self.minimum_core_fraction <= 0:
            return remaining
        current_total = sum(current.values(), ZERO)
        current_core = sum(
            (
                amount
                for symbol, amount in current.items()
                if symbol in self.core_symbols
            ),
            ZERO,
        )
        new_core = sum(
            (
                amount
                for symbol, amount in allocation.items()
                if symbol in self.core_symbols
            ),
            ZERO,
        )
        new_noncore = sum(
            (
                amount
                for symbol, amount in allocation.items()
                if symbol not in self.core_symbols
            ),
            ZERO,
        )
        if new_noncore <= 0:
            return remaining
        core_total = current_core + new_core
        maximum_new_noncore = max(
            ZERO,
            (
                core_total
                / Decimal(str(self.minimum_core_fraction))
                - current_total
                - new_core
            ).quantize(MONEY_QUANTUM, rounding=ROUND_DOWN),
        )
        excess = max(ZERO, new_noncore - maximum_new_noncore)
        if excess < MONEY_QUANTUM:
            return remaining
        scores = {
            candidate.symbol: candidate.score
            for candidate in candidates
        }
        for symbol in sorted(
            (
                symbol
                for symbol in allocation
                if symbol not in self.core_symbols
            ),
            key=lambda symbol: (
                self.target_weights.get(symbol, 0.0),
                scores.get(symbol, 0.0),
                symbol,
            ),
        ):
            reduction = min(allocation[symbol], excess)
            allocation[symbol] -= reduction
            remaining += reduction
            excess -= reduction
            if excess < MONEY_QUANTUM:
                break
        return remaining.quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)

    @staticmethod
    def _target_fill(
        *,
        candidates: Sequence[AllocationCandidate],
        allocation: Dict[str, Decimal],
        remaining: Decimal,
        current: Mapping[str, Decimal],
        capital_base: Decimal,
        target_weights: Mapping[str, float],
        symbol_room: Mapping[str, Decimal],
        cluster_room: Mapping[int, Decimal],
        cluster_for: Mapping[str, int],
        clusters: Sequence[set[str]],
    ) -> Decimal:
        deficits = {
            candidate.symbol: min(
                symbol_room[candidate.symbol],
                max(
                    ZERO,
                    (
                        capital_base
                        * Decimal(
                            str(target_weights.get(candidate.symbol, 0.0))
                        )
                    ).quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)
                    - current.get(candidate.symbol, ZERO),
                ),
            )
            for candidate in candidates
        }
        for _ in range(len(candidates) + 1):
            open_symbols = [
                symbol
                for symbol, deficit in deficits.items()
                if deficit - allocation[symbol] >= MONEY_QUANTUM
            ]
            if not open_symbols or remaining < MONEY_QUANTUM:
                break
            total_deficit = sum(
                (deficits[symbol] - allocation[symbol] for symbol in open_symbols),
                ZERO,
            )
            if total_deficit < MONEY_QUANTUM:
                break
            cycle_budget = remaining
            progressed = ZERO
            for symbol in open_symbols:
                cluster_index = cluster_for[symbol]
                cluster_used = sum(
                    (
                        allocation.get(member, ZERO)
                        for member in clusters[cluster_index]
                    ),
                    ZERO,
                )
                room = min(
                    deficits[symbol] - allocation[symbol],
                    cluster_room[cluster_index] - cluster_used,
                )
                if room < MONEY_QUANTUM:
                    continue
                share = (
                    cycle_budget
                    * (deficits[symbol] - allocation[symbol])
                    / total_deficit
                ).quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)
                addition = min(room, remaining, max(MONEY_QUANTUM, share))
                allocation[symbol] += addition
                remaining -= addition
                progressed += addition
                if remaining < MONEY_QUANTUM:
                    break
            if progressed < MONEY_QUANTUM:
                break
        return remaining.quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)

    @staticmethod
    def _weighted_fill(
        *,
        candidates: Sequence[AllocationCandidate],
        allocation: Dict[str, Decimal],
        remaining: Decimal,
        symbol_room: Mapping[str, Decimal],
        cluster_room: Mapping[int, Decimal],
        cluster_for: Mapping[str, int],
        clusters: Sequence[set[str]],
    ) -> Decimal:
        for _ in range(len(candidates) * 2 + 2):
            open_candidates = []
            for candidate in candidates:
                symbol = candidate.symbol
                cluster_index = cluster_for[symbol]
                used_by_cluster = sum(
                    (
                        allocation.get(member, ZERO)
                        for member in clusters[cluster_index]
                    ),
                    ZERO,
                )
                room = min(
                    symbol_room[symbol] - allocation[symbol],
                    cluster_room[cluster_index] - used_by_cluster,
                )
                if room >= MONEY_QUANTUM:
                    open_candidates.append(candidate)
            if not open_candidates or remaining < MONEY_QUANTUM:
                break
            weight_total = sum(
                max(candidate.score, 0.01)
                for candidate in open_candidates
            )
            cycle_budget = remaining
            progressed = ZERO
            for candidate in open_candidates:
                symbol = candidate.symbol
                cluster_index = cluster_for[symbol]
                used_by_cluster = sum(
                    (
                        allocation.get(member, ZERO)
                        for member in clusters[cluster_index]
                    ),
                    ZERO,
                )
                room = min(
                    symbol_room[symbol] - allocation[symbol],
                    cluster_room[cluster_index] - used_by_cluster,
                )
                if room < MONEY_QUANTUM:
                    continue
                weight = max(candidate.score, 0.01) / weight_total
                share = (
                    cycle_budget * Decimal(str(weight))
                ).quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)
                addition = min(room, remaining, max(MONEY_QUANTUM, share))
                if addition < MONEY_QUANTUM:
                    continue
                allocation[candidate.symbol] += addition
                remaining -= addition
                progressed += addition
                if remaining < MONEY_QUANTUM:
                    break
            if progressed < MONEY_QUANTUM:
                break
        return remaining.quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)

    def _correlation_clusters(
        self,
        candidates: Sequence[AllocationCandidate],
    ) -> List[set[str]]:
        parents = {
            candidate.symbol: candidate.symbol
            for candidate in candidates
        }

        def find(symbol: str) -> str:
            while parents[symbol] != symbol:
                parents[symbol] = parents[parents[symbol]]
                symbol = parents[symbol]
            return symbol

        def union(left: str, right: str) -> None:
            left_root = find(left)
            right_root = find(right)
            if left_root != right_root:
                parents[max(left_root, right_root)] = min(
                    left_root,
                    right_root,
                )

        for index, left in enumerate(candidates):
            for right in candidates[index + 1 :]:
                if (
                    not self._histories_aligned(left, right)
                    or _pearson(left.returns, right.returns)
                    >= self.correlation_threshold
                ):
                    union(left.symbol, right.symbol)
        clusters: Dict[str, set[str]] = {}
        for symbol in parents:
            clusters.setdefault(find(symbol), set()).add(symbol)
        return [clusters[key] for key in sorted(clusters)]

    @staticmethod
    def _has_valid_history(candidate: AllocationCandidate) -> bool:
        return (
            len(candidate.returns) >= 24
            and candidate.history_start_ms > 0
            and candidate.history_end_ms > candidate.history_start_ms
            and candidate.history_interval_ms > 0
            and (
                candidate.history_end_ms - candidate.history_start_ms
                == len(candidate.returns) * candidate.history_interval_ms
            )
        )

    @classmethod
    def _histories_aligned(
        cls,
        left: AllocationCandidate,
        right: AllocationCandidate,
    ) -> bool:
        return (
            cls._has_valid_history(left)
            and cls._has_valid_history(right)
            and len(left.returns) == len(right.returns)
            and left.history_start_ms == right.history_start_ms
            and left.history_end_ms == right.history_end_ms
            and left.history_interval_ms == right.history_interval_ms
        )
