"""Concurrent GTC bid placement, fill monitoring, and one-leg reconciliation."""

from __future__ import annotations

import asyncio
import random
import time
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from strategies.base import SpreadDecision
from utils.spread_risk import dual_both_filled, dual_one_legged, is_order_fully_filled

if TYPE_CHECKING:
    from bot import MarketWorker


async def _simulate_dry_leg(
    worker: "MarketWorker",
    side: str,
    price: float,
    size: float,
) -> Tuple[str, float, float, float, bool]:
    """Return (side, size, price, delay_sec, completed_in_time)."""
    cfg = worker.worker_config
    delay_ms = random.randint(cfg.dry_run_fill_delay_min_ms, cfg.dry_run_fill_delay_max_ms)
    await asyncio.sleep(delay_ms / 1000.0)
    completed_in_time = delay_ms <= cfg.trade_cooldown_ms
    return side, size, price, delay_ms / 1000.0, completed_in_time


async def _monitor_dry_spread_fills(
    worker: "MarketWorker",
    decision: SpreadDecision,
    legs: List[Tuple[str, float]],
) -> Dict[str, Tuple[float, float]]:
    from bot import MIN_FILL_DELTA

    cfg = worker.worker_config
    order_size = float(decision.size)
    timeout_sec = cfg.spread_fill_timeout_ms / 1000.0
    poll_sec = cfg.spread_fill_poll_ms / 1000.0
    cooldown_sec = cfg.trade_cooldown_ms / 1000.0

    leg_tasks = {
        side: asyncio.create_task(_simulate_dry_leg(worker, side, price, order_size))
        for side, price in legs
    }
    fills: Dict[str, Tuple[float, float]] = {}
    started = time.monotonic()
    one_leg_handled = False

    while leg_tasks and (time.monotonic() - started) < timeout_sec:
        for side, task in list(leg_tasks.items()):
            if not task.done():
                continue
            try:
                s, size, price, _delay, in_time = task.result()
            except asyncio.CancelledError:
                del leg_tasks[side]
                continue
            if in_time and size > MIN_FILL_DELTA:
                fills[s] = (size, price)
            del leg_tasks[side]

        elapsed = time.monotonic() - started
        if (
            decision.mode == "dual"
            and not one_leg_handled
            and elapsed >= cooldown_sec
        ):
            lone = dual_one_legged(fills, MIN_FILL_DELTA)
            if lone:
                for side, task in list(leg_tasks.items()):
                    task.cancel()
                    print(f"  🧪 [DRY CANCEL] {side} cancelled — other leg filled first")
                leg_tasks.clear()
                one_leg_handled = True

        if not leg_tasks:
            break
        await asyncio.sleep(poll_sec)

    for side, task in leg_tasks.items():
        task.cancel()
        print(f"  🧪 [DRY CANCEL] {side} timed out after {timeout_sec:.1f}s")

    return await _reconcile_dual_fills(worker, decision, fills)


async def _monitor_live_spread_fills(
    worker: "MarketWorker",
    decision: SpreadDecision,
    legs: List[Tuple[str, float]],
    placed: List[Tuple[Optional[str], float]],
) -> Dict[str, Tuple[float, float]]:
    from bot import MIN_FILL_DELTA

    cfg = worker.worker_config
    order_size = float(decision.size)
    timeout_sec = cfg.spread_fill_timeout_ms / 1000.0
    poll_sec = cfg.spread_fill_poll_ms / 1000.0
    cooldown_sec = cfg.trade_cooldown_ms / 1000.0

    fills: Dict[str, Tuple[float, float]] = {}
    pending: Dict[str, Tuple[str, float, float]] = {}

    for (side, limit_price), (order_id, immediate_fill) in zip(legs, placed):
        if not order_id:
            continue
        if immediate_fill:
            fills[side] = (order_size, limit_price)
            worker._untrack_spread_order(order_id)
        else:
            pending[side] = (order_id, limit_price, order_size)

    started = time.monotonic()
    one_leg_handled = False

    while pending and (time.monotonic() - started) < timeout_sec:
        for side in list(pending.keys()):
            order_id, limit_price, requested = pending[side]
            fill_size, fill_price = await worker.poll_order_fill(
                order_id, requested, limit_price,
            )
            if fill_size > MIN_FILL_DELTA:
                existing = fills.get(side, (0.0, 0.0))
                if existing[0] > MIN_FILL_DELTA:
                    total = existing[0] + fill_size
                    avg_px = (existing[0] * existing[1] + fill_size * fill_price) / total
                    fills[side] = (total, avg_px)
                else:
                    fills[side] = (fill_size, fill_price)
            if is_order_fully_filled(requested, fill_size, MIN_FILL_DELTA):
                worker._untrack_spread_order(order_id)
                del pending[side]

        elapsed = time.monotonic() - started
        if (
            decision.mode == "dual"
            and not one_leg_handled
            and elapsed >= cooldown_sec
        ):
            lone = dual_one_legged(fills, MIN_FILL_DELTA)
            if lone:
                for other in list(pending.keys()):
                    oid, lp, req = pending[other]
                    extra_sz, extra_px = await worker.cancel_spread_order_confirmed(
                        oid, req, lp,
                    )
                    if extra_sz > MIN_FILL_DELTA:
                        existing = fills.get(other, (0.0, 0.0))
                        if existing[0] > MIN_FILL_DELTA:
                            total = existing[0] + extra_sz
                            avg_px = (existing[0] * existing[1] + extra_sz * extra_px) / total
                            fills[other] = (total, avg_px)
                        else:
                            fills[other] = (extra_sz, extra_px)
                    del pending[other]
                one_leg_handled = True

        if not pending:
            break
        await asyncio.sleep(poll_sec)

    for side, (order_id, limit_price, requested) in list(pending.items()):
        fill_size, fill_price = await worker.cancel_spread_order_confirmed(
            order_id, requested, limit_price,
        )
        if fill_size > MIN_FILL_DELTA:
            existing = fills.get(side, (0.0, 0.0))
            if existing[0] > MIN_FILL_DELTA:
                total = existing[0] + fill_size
                avg_px = (existing[0] * existing[1] + fill_size * fill_price) / total
                fills[side] = (total, avg_px)
            else:
                fills[side] = (fill_size, fill_price)

    return await _reconcile_dual_fills(worker, decision, fills)


async def _reconcile_dual_fills(
    worker: "MarketWorker",
    decision: SpreadDecision,
    fills: Dict[str, Tuple[float, float]],
) -> Dict[str, Tuple[float, float]]:
    from bot import MIN_FILL_DELTA

    if decision.mode != "dual":
        return fills

    lone = dual_one_legged(fills, MIN_FILL_DELTA)
    if lone:
        size, price = fills[lone]
        print(
            f"⚠️ [SPREAD] One-leg detected on {lone} ({size:.2f}@{round(price*100)}c) — unwinding"
        )
        if await worker.unwind_spread_leg(lone, size, price):
            fills.pop(lone, None)
        return fills

    if dual_both_filled(fills, MIN_FILL_DELTA):
        yes_sz, no_sz = fills["YES"][0], fills["NO"][0]
        if abs(yes_sz - no_sz) > MIN_FILL_DELTA:
            print(
                f"ℹ️ [SPREAD] Asymmetric dual fill YES={yes_sz:.2f} NO={no_sz:.2f} "
                f"— rebalance will follow"
            )
    return fills


def _record_spread_fills(
    worker: "MarketWorker",
    fills: Dict[str, Tuple[float, float]],
) -> None:
    from bot import MIN_FILL_DELTA

    for side, (size, price) in fills.items():
        if size > MIN_FILL_DELTA and price > 0:
            worker.spread_inventory.record_buy(side, size, price)
            print(f"  ✅ [FILL] {side} {size:.2f}@{round(price*100)}c")


async def execute_spread_decision(worker: "MarketWorker", decision: SpreadDecision) -> None:
    from bot import MIN_FILL_DELTA, SpreadState

    cfg = worker.worker_config
    mode_label = decision.mode.upper()

    legs = worker.resolve_spread_execution_legs(decision)
    if not legs:
        print(
            f"❌ [SPREAD ABORT] {worker.asset_type.upper()} {worker.window_slug} | "
            f"no executable legs (missing/locked bids)"
        )
        return

    if not worker.validate_spread_execution_edge(decision, legs):
        return

    yes_c = round(next((p for s, p in legs if s == "YES"), 0.0) * 100)
    no_c = round(next((p for s, p in legs if s == "NO"), 0.0) * 100)

    for side, _price in legs:
        if not worker.validate_spread_order_size(side, decision.size):
            print(
                f"❌ [SPREAD ABORT] {worker.asset_type.upper()} {worker.window_slug} | "
                f"{side} size={decision.size} failed pre-submit sanity check"
            )
            return

    worker.spread_state = SpreadState.PENDING
    try:
        if worker.is_dry_run():
            print(
                f"\n🧪 [DRY SPREAD] {worker.asset_type.upper()} {worker.window_slug} | "
                f"mode={mode_label} edge={decision.edge:.4f} size={decision.size} | "
                f"YES@{yes_c}c NO@{no_c}c | monitor "
                f"{cfg.spread_fill_timeout_ms}ms max"
            )
            start = time.monotonic()
            fills = await _monitor_dry_spread_fills(worker, decision, legs)
            _record_spread_fills(worker, fills)
            if fills:
                worker.log_spread_capture_trades(mode=decision.mode, fills=fills)
            worker._log_spread_capture(decision, fills=fills or None, dry_run=True)
            print(f"  🧪 [DRY SPREAD] cycle done in {time.monotonic() - start:.2f}s")
            return

        print(
            f"\n📊 [SPREAD] {worker.asset_type.upper()} {worker.window_slug} | "
            f"{mode_label} edge={decision.edge:.4f} | "
            + " ".join(f"{s}@{round(p*100)}c" for s, p in legs)
        )

        order_size = float(decision.size)
        placed = await asyncio.gather(
            *[
                worker.place_spread_gtc(side, price, order_size)
                for side, price in legs
            ]
        )

        fills = await _monitor_live_spread_fills(worker, decision, legs, placed)
        _record_spread_fills(worker, fills)

        if fills:
            worker.log_spread_capture_trades(mode=decision.mode, fills=fills)
        worker._log_spread_capture(decision, fills=fills or None)
    finally:
        worker.spread_state = SpreadState.IDLE
