# csv_price_bot.py
# A tiny price feeder that replays historical prices from CSV into PriceBus.

from __future__ import annotations

import asyncio, csv, os, signal, logging
from decimal import Decimal
from typing import Optional, List, Tuple
from bot_manager import BaseBot, BotConfig, PriceBus

class CSVPriceBot(BaseBot):
    """
    Plays back historical prices from a CSV and updates PriceBus.
    params (cfg.params):
      - bus: PriceBus
      - symbol: "BTCUSD" / "BTCUSDT"
      - csv_path: path to CSV file
      - has_header: bool (default False)
      - ts_col: int (default 0)  epoch ms
      - price_col: int (default 1)
      - replay_speed: float  (1.0 = real-time; 10.0 = 10x faster; 0 = as fast as possible)
      - stop_on_end: bool (default True) -> send SIGINT to stop the manager when replay finishes
    """
    def __init__(self, cfg: BotConfig):
        super().__init__(cfg)
        p = cfg.params
        self.bus: PriceBus = p["bus"]
        self.symbol: str = p["symbol"].upper()
        self.csv_path: str = p["csv_path"]
        self.has_header: bool = bool(p.get("has_header", False))
        self.ts_col: int = int(p.get("ts_col", 0))
        self.price_col: int = int(p.get("price_col", 1))
        self.replay_speed: float = float(p.get("replay_speed", 10.0))  # 0 = as fast as possible
        self.stop_on_end: bool = bool(p.get("stop_on_end", True))
        self._task: Optional[asyncio.Task] = None
        self._stopping = False
        self.log = logging.getLogger(f"bot.{cfg.name}")

    async def _run_csv(self) -> None:
        rows: List[Tuple[int, Decimal]] = []
        with open(self.csv_path, "r", newline="") as f:
            r = csv.reader(f)
            if self.has_header:
                next(r, None)
            for row in r:
                if not row:
                    continue
                try:
                    ts = int(row[self.ts_col])
                    px = Decimal(row[self.price_col])
                    rows.append((ts, px))
                except Exception as e:
                    self.log.debug("skip row %s (%s)", row, e)
        if not rows:
            self.log.warning("no rows in %s", self.csv_path)
            return

        rows.sort(key=lambda x: x[0])
        last_ts = rows[0][0]
        self.log.info("CSV replay: %d rows from %s (speed=%.2fx)", len(rows), self.csv_path, self.replay_speed)

        for ts, px in rows:
            if self._stopping: break
            if self.replay_speed > 0:
                delta_ms = max(0, ts - last_ts)
                sleep_s = delta_ms / 1000.0 / self.replay_speed
                if sleep_s > 0:
                    try:
                        await asyncio.sleep(sleep_s)
                    except asyncio.CancelledError:
                        break
            await self.bus.set_price(self.symbol, px)
            last_ts = ts

        self.log.info("CSV replay done.")
        if self.stop_on_end:
            try:
                os.kill(os.getpid(), signal.SIGINT)
            except Exception:
                pass

    async def on_start(self) -> None:
        self._stopping = False
        self._task = asyncio.create_task(self._run_csv())

    async def on_stop(self) -> None:
        self._stopping = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def on_tick(self) -> None:
        pass
