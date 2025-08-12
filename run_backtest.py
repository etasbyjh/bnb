# run_backtest.py
# Backtest runner: reuses PriceBus, BotManager, PaperTradingBot from bot_manager.py
# and feeds prices via CSVPriceBot.

from __future__ import annotations

import asyncio, argparse
from bot_manager import setup_logging, BotManager, BotConfig, PriceBus, PaperTradingBot
from csv_price_bot import CSVPriceBot

async def main() -> None:
    ap = argparse.ArgumentParser(description="Backtest PaperTradingBot using CSV prices")
    ap.add_argument("--csv", required=True, help="Path to CSV file (timestamp_ms,price per line)")
    ap.add_argument("--symbol", default="BTCUSD", help="Symbol key to publish (e.g., BTCUSD or BTCUSDT)")
    ap.add_argument("--speed", type=float, default=10.0, help="Replay speed factor (0 = as fast as possible)")
    ap.add_argument("--header", action="store_true", help="CSV has a header row")
    ap.add_argument("--ts-col", type=int, default=0, help="Timestamp column index (ms epoch)")
    ap.add_argument("--price-col", type=int, default=1, help="Price column index")
    ap.add_argument("--report-interval", type=int, default=60, help="Seconds between reports")
    ap.add_argument("--order-ttl", type=int, default=28800, help="Order TTL seconds (e.g., 8h = 28800)")
    ap.add_argument("--buy-gap", type=float, default=0.2, help="Buy gap percent below spot")
    ap.add_argument("--sell-gap", type=float, default=0.2, help="Sell gap percent above last buy")
    ap.add_argument("--min-profit", type=float, default=None, help="Min profit percent floor (default = sell-gap)")
    ap.add_argument("--base-start", type=float, default=0.0, help="Starting base balance")
    ap.add_argument("--quote-start", type=float, default=200.0, help="Starting quote balance")
    ap.add_argument("--tick", type=str, default="0.01", help="Price tick size")
    ap.add_argument("--step", type=str, default="0.000001", help="Qty step size")
    ap.add_argument("--min-qty", type=str, default="0.00001", help="Min trade qty")
    ap.add_argument("--min-notional", type=str, default="10.0", help="Min notional")
    args = ap.parse_args()

    setup_logging()
    bus = PriceBus()
    mgr = BotManager()

    # CSV price bot (feeds the bus)
    csv_cfg = BotConfig(
        name="csv-feed",
        tick_seconds=0.1,
        params={
            "bus": bus,
            "symbol": args.symbol,
            "csv_path": args.csv,
            "has_header": args.header,
            "ts_col": args.ts_col,
            "price_col": args.price_col,
            "replay_speed": args.speed,
            "stop_on_end": True,
        },
    )
    mgr.add("csv-feed", lambda cfg: CSVPriceBot(cfg), csv_cfg)

    # Paper bot (consumes prices from the bus)
    min_profit = args.min_profit if args.min_profit is not None else args.sell_gap
    paper_cfg = BotConfig(
        name="paper",
        tick_seconds=1.0,
        params={
            "bus": bus,
            "symbol": args.symbol,
            "filters": {
                "tick": args.tick,
                "step": args.step,
                "min_qty": args.min_qty,
                "min_notional": args.min_notional,
            },
            "gaps": {"buy_gap_pct": args.buy_gap, "sell_gap_pct": args.sell_gap},
            "min_profit_pct": min_profit,
            "balances": {"base": str(args.base_start), "quote": str(args.quote_start)},
            "fee_rate": 0.0,
            "order_ttl_seconds": args.order_ttl,
            "report_interval_sec": args.report_interval,
        },
    )
    mgr.add("paper", lambda cfg: PaperTradingBot(cfg), paper_cfg)

    await mgr.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
