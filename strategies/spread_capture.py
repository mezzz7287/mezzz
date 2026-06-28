"""Spread capture — buy UP+DOWN when combined bids sum below $1 by threshold."""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional

from strategies.base import SpreadDecision
from strategies.spread_execution import execute_spread_decision
from utils.spread_risk import edge_meets_threshold

if TYPE_CHECKING:
    from bot import MarketWorker


class SpreadCaptureStrategy:
    async def evaluate(self, worker: "MarketWorker") -> Optional[SpreadDecision]:
        from bot import SpreadState

        if worker.spread_state == SpreadState.PENDING:
            return None

        cfg = worker.worker_config
        edge = worker.current_spread_edge()
        if edge is None or not edge_meets_threshold(edge, cfg.spread_threshold):
            return None

        up_bid = worker.spread_bid("YES")
        down_bid = worker.spread_bid("NO")

        under = worker.spread_inventory.underweight_side(cfg.spread_imbalance_epsilon)
        if under is None:
            if not worker.can_spread_dual():
                return None
        elif not worker.can_spread_rebalance():
            return None

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
            up_bid=worker.effective_bid("YES"),
            down_bid=worker.effective_bid("NO"),
            edge=edge,
            size=size,
        )

    async def execute(self, worker: "MarketWorker", decision: SpreadDecision) -> None:
        await execute_spread_decision(worker, decision)
