"""Spread capture — buy UP+DOWN when combined bids sum below $1 by threshold."""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional

from strategies.base import SpreadDecision
from strategies.spread_execution import execute_spread_decision

if TYPE_CHECKING:
    from bot import MarketWorker


class SpreadCaptureStrategy:
    async def evaluate(self, worker: "MarketWorker") -> Optional[SpreadDecision]:
        from bot import SpreadState, is_locked_price

        if worker.spread_state == SpreadState.PENDING:
            return None

        cfg = worker.worker_config
        up_bid = worker.effective_bid("YES")
        down_bid = worker.effective_bid("NO")
        if up_bid <= 0 or down_bid <= 0:
            return None
        if is_locked_price(up_bid) or is_locked_price(down_bid):
            return None

        combined = up_bid + down_bid
        edge = round(1.0 - combined, 4)
        if edge <= cfg.spread_threshold:
            return None

        under = worker.spread_inventory.underweight_side()
        legs: List[str] = ["YES", "NO"] if under is None else [under]
        size = worker.spread_order_size(legs)
        if size is None:
            return None

        if under is None:
            return SpreadDecision(
                yes_price=round(up_bid, 2),
                no_price=round(down_bid, 2),
                size=size,
                edge=edge,
                mode="dual",
            )

        return worker._spread_rebalance_decision(
            underweight=under,
            up_bid=up_bid,
            down_bid=down_bid,
            edge=edge,
            size=size,
        )

    async def execute(self, worker: "MarketWorker", decision: SpreadDecision) -> None:
        await execute_spread_decision(worker, decision)
