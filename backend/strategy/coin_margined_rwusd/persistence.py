"""Restart-safe persistence for the independent coin-margined RWUSD strategy."""

from __future__ import annotations

import json
import os
from pathlib import Path


RUNTIME_NAMESPACE = "coin_margined_rwusd"
PORTFOLIO_STATE_SUFFIX = "__coin_margined_rwusd_portfolio"
LANE_STATE_PREFIX = "__coin_margined_rwusd_lane_"


def _runtime_dir() -> Path:
    configured = str(os.environ.get("DBRSI_RUNTIME_DIR") or "").strip()
    if configured:
        return Path(configured)
    return Path(__file__).resolve().parents[3] / ".runtime"


def _safe_account_part(account_name: object) -> str:
    safe = "".join(
        char if char.isalnum() or char in {"_", "-"} else "_"
        for char in str(account_name or "")
    ).strip("_")
    return safe or "rwusd"


def rwusd_runtime_state_path(account_name: object) -> Path:
    return (
        _runtime_dir()
        / RUNTIME_NAMESPACE
        / f"{_safe_account_part(account_name)}.json"
    )


def rwusd_runtime_state_paths(account_name: object) -> list[Path]:
    root = _runtime_dir() / RUNTIME_NAMESPACE
    account_part = _safe_account_part(account_name)
    paths = [
        root / f"{account_part}.json",
        root / f"{account_part}{PORTFOLIO_STATE_SUFFIX}.json",
    ]
    if root.exists():
        paths.extend(
            sorted(root.glob(f"{account_part}{LANE_STATE_PREFIX}*.json"))
        )
    return list(dict.fromkeys(paths))


def rwusd_runtime_lane_symbols(account_name: object) -> list[str]:
    root = _runtime_dir() / RUNTIME_NAMESPACE
    account_part = _safe_account_part(account_name)
    lane_prefix = f"{account_part}{LANE_STATE_PREFIX}"
    if not root.exists():
        return []
    return sorted(
        {
            path.stem[len(lane_prefix):].strip().upper()
            for path in root.glob(f"{lane_prefix}*.json")
            if path.stem.startswith(lane_prefix)
            and path.stem[len(lane_prefix):].strip()
        }
    )


class RWUSDStateStore:
    def __init__(self, account_name: str) -> None:
        self.path = rwusd_runtime_state_path(account_name)

    def load(self) -> dict:
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}

    def load_strict(self) -> dict:
        if not self.path.exists():
            return {}
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise RuntimeError(
                f"RWUSD state is unreadable: {self.path}"
            ) from exc
        if not isinstance(payload, dict):
            raise RuntimeError(f"RWUSD state is not an object: {self.path}")
        return payload

    def save(self, payload: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        os.replace(temporary, self.path)


def clear_rwusd_runtime_state(account_name: object) -> bool:
    removed = False
    for path in rwusd_runtime_state_paths(account_name):
        if path.exists():
            path.unlink()
            removed = True
    return removed


def rename_rwusd_runtime_state(old_account_name: object, new_account_name: object) -> bool:
    old_part = _safe_account_part(old_account_name)
    new_part = _safe_account_part(new_account_name)
    if old_part == new_part:
        return False
    sources = [
        path
        for path in rwusd_runtime_state_paths(old_account_name)
        if path.exists()
    ]
    if not sources:
        return False
    root = _runtime_dir() / RUNTIME_NAMESPACE
    moves = [
        (
            source,
            root / f"{new_part}{source.stem[len(old_part):]}.json",
        )
        for source in sources
    ]
    conflicts = [
        destination
        for source, destination in moves
        if destination.exists() and destination != source
    ]
    if conflicts:
        raise RuntimeError(
            "RWUSD target runtime state already exists: "
            + ",".join(path.name for path in conflicts)
        )
    root.mkdir(parents=True, exist_ok=True)
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
