"""Pure helpers for spread-capture edge validation and fill reconciliation."""

from __future__ import annotations

from typing import Callable, Dict, Optional, Tuple

FillMap = Dict[str, Tuple[float, float]]


def compute_spread_edge(
    yes_bid: float,
    no_bid: float,
    *,
    is_locked: Callable[[float], bool],
) -> Optional[float]:
    """Return edge from real best bids only; None when quotes are unusable."""
    if yes_bid <= 0 or no_bid <= 0:
        return None
    if is_locked(yes_bid) or is_locked(no_bid):
        return None
    return round(1.0 - (yes_bid + no_bid), 4)


def dual_limit_edge(yes_price: float, no_price: float) -> float:
    return round(1.0 - (yes_price + no_price), 4)


def edge_meets_threshold(edge: float, threshold: float) -> bool:
    return edge > threshold


def is_order_fully_filled(requested: float, filled: float, min_delta: float) -> bool:
    return filled >= requested - min_delta


def dual_fill_sizes(fills: FillMap) -> Tuple[float, float]:
    yes_sz = fills.get("YES", (0.0, 0.0))[0]
    no_sz = fills.get("NO", (0.0, 0.0))[0]
    return yes_sz, no_sz


def dual_one_legged(fills: FillMap, min_delta: float) -> Optional[str]:
    """Return the filled side when only one leg has size, else None."""
    yes_sz, no_sz = dual_fill_sizes(fills)
    yes_ok = yes_sz > min_delta
    no_ok = no_sz > min_delta
    if yes_ok and not no_ok:
        return "YES"
    if no_ok and not yes_ok:
        return "NO"
    return None


def dual_both_filled(fills: FillMap, min_delta: float) -> bool:
    yes_sz, no_sz = dual_fill_sizes(fills)
    return yes_sz > min_delta and no_sz > min_delta


def spread_entry_window_ok(
    seconds_left: int,
    *,
    entry_seconds_left: int,
    min_entry_seconds_left: int,
) -> bool:
    """Dual capture allowed when within the configured entry window."""
    return min_entry_seconds_left < seconds_left <= entry_seconds_left


def spread_rebalance_window_ok(
    seconds_left: int,
    *,
    min_rebalance_seconds_left: int,
) -> bool:
    return seconds_left > min_rebalance_seconds_left
