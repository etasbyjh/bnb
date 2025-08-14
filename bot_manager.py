# bot_manager.py
# Python 3.10+
# Paper-trading bots with HTTP admin, WS price feed, centralized FundManager,
# fee-aware order sizing, tranche buys, optional momentum seed, and compact
# global reporting (with price trails) in EST.

from __future__ import annotations

import asyncio
import json
import logging
import random
import signal
import argparse
import os
import time
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, Optional, Callable, List, Tuple
from datetime import datetime
from zoneinfo import ZoneInfo
from collections import deque

from aiohttp import web
import websockets
from websockets.exceptions import ConnectionClosed

# ---------- timezone helpers (EST/EDT) ----------
NY = ZoneInfo("America/New_York")

def now_est() -> datetime:
    return datetime.now(tz=NY)

def ts_est() -> str:
    return now_est().isoformat(timespec="seconds")

def format_epoch_est(epoch: float | None) -> str:
    if not epoch:
        return "n/a"
    return datetime.fromtimestamp(epoch, tz=NY).isoformat(timespec="seconds")

def format_time_hms_est(epoch: float | None) -> str:
    if not epoch:
        return "n/a"
    return datetime.fromtimestamp(epoch, tz=NY).strftime("%H:%M:%S")

def format_duration_dhms(seconds: float) -> str:
    s = int(max(0, seconds))
    days, rem = divmod(s, 86400)
    hrs, rem = divmod(rem, 3600)
    mins, secs = divmod(rem, 60)
    parts = []
    if days: parts.append(f"{days}d")
    parts.append(f"{hrs}h")
    parts.append(f"{mins}m")
    parts.append(f"{secs}s")
    return " ".join(parts)

# ---------------------------- logging ----------------------------
def setup_logging(level: int = logging.INFO) -> None:
    fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    datefmt = "%H:%M:%S"
    logging.basicConfig(level=level, format=fmt, datefmt=datefmt)

# ---------------------------- config -----------------------------
@dataclass
class BotConfig:
    name: str
    tick_seconds: float = 1.0
    max_backoff: float = 30.0
    jitter: float = 0.2
    params: Dict[str, Any] = field(default_factory=dict)

def d(x) -> Decimal:
    return Decimal(str(x))

# ----------------------- centralized fund manager ----------------
class FundManager:
    """Global pool of assets (e.g., {'USD': 20000, 'ETH': 0})."""
    def __init__(self):
        self._bal: Dict[str, Decimal] = {}
        self._lock = asyncio.Lock()

    def seed(self, asset: str, amt: Decimal) -> None:
        self._bal[asset] = self._bal.get(asset, d("0")) + amt

    async def deposit(self, asset: str, amt: Decimal) -> None:
        async with self._lock:
            self._bal[asset] = self._bal.get(asset, d("0")) + amt

    async def allocate(self, want: Dict[str, Decimal]) -> Dict[str, Decimal]:
        async with self._lock:
            out: Dict[str, Decimal] = {}
            for asset, amt in want.items():
                cur = self._bal.get(asset, d("0"))
                take = min(cur, amt)
                self._bal[asset] = cur - take
                out[asset] = take
            return out

    async def balances(self) -> Dict[str, Decimal]:
        async with self._lock:
            return dict(self._bal)

# --------------------------- report hub --------------------------
class ReportHub:
    def __init__(self):
        self._snapshots: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def put(self, bot_name: str, snap: Dict[str, Any]) -> None:
        async with self._lock:
            self._snapshots[bot_name] = snap

    async def clear(self, bot_name: str) -> None:
        async with self._lock:
            self._snapshots.pop(bot_name, None)

    async def all(self) -> Dict[str, Dict[str, Any]]:
        async with self._lock:
            return dict(self._snapshots)

# ---------------------------- base bot ---------------------------
class BaseBot:
    def __init__(self, cfg: BotConfig):
        self.cfg = cfg
        self.log = logging.getLogger(f"bot.{cfg.name}")
        self._stop = asyncio.Event()
        self._started_ts: Optional[float] = None
        life = cfg.params.get("bot_lifetime_seconds", 0) or 0
        self.lifetime_sec: Optional[int] = int(life) if life else None

    async def on_start(self) -> None: ...
    async def on_stop(self) -> None: ...
    async def on_tick(self) -> None: raise NotImplementedError

    async def start(self) -> None:
        self._started_ts = time.time()
        self.log.info("starting (EST %s)", ts_est())
        await self.on_start()

    async def stop(self) -> None:
        self.log.info("stopping (EST %s)", ts_est())
        self._stop.set()
        await self.on_stop()

    def active_duration(self) -> float:
        if self._started_ts is None:
            return 0.0
        return max(0.0, time.time() - self._started_ts)

    async def run(self) -> None:
        await self.start()
        try:
            while not self._stop.is_set():
                if self.lifetime_sec and self.active_duration() >= self.lifetime_sec:
                    self.log.info("lifetime reached (%ss), stopping", self.lifetime_sec)
                    break
                await self.on_tick()
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=self.cfg.tick_seconds)
                except asyncio.TimeoutError:
                    pass
        finally:
            await self.stop()

# ----------------------- shared price bus ------------------------
class PriceBus:
    """Per-symbol last price store + short rolling history for reports."""
    def __init__(self):
        self._last: Dict[str, Decimal] = {}
        self._cv = asyncio.Condition()
        self._hist: Dict[str, deque] = {}
        self._hist_len = int(os.getenv("REPORT_TICK_HISTORY", "5"))

    async def set_last(self, symbol: str, last: Decimal) -> None:
        symbol = symbol.upper()
        async with self._cv:
            self._last[symbol] = last
            dq = self._hist.get(symbol)
            if dq is None:
                dq = deque(maxlen=self._hist_len)
                self._hist[symbol] = dq
            dq.append(last)
            self._cv.notify_all()

    async def wait_for_price(self, symbol: str, timeout: Optional[float] = None) -> Decimal:
        symbol = symbol.upper()
        async with self._cv:
            if timeout is None:
                while symbol not in self._last:
                    await self._cv.wait()
            else:
                deadline = time.time() + timeout
                while symbol not in self._last:
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        raise TimeoutError(f"No price yet for {symbol}")
                    await asyncio.wait_for(self._cv.wait(), timeout=remaining)
            return self._last[symbol]

    def get_last(self, symbol: str) -> Optional[Decimal]:
        return self._last.get(symbol.upper())

    def recent_prices(self, symbol: str, k: Optional[int] = None) -> List[Decimal]:
        dq = self._hist.get(symbol.upper())
        if not dq:
            return []
        if k is None or k >= len(dq):
            return list(dq)
        return list(dq)[-k:]

# ----------------------- WebSocket price bot ---------------------
class WSPriceBot(BaseBot):
    """Subscribes to Binance(.com or .US) trade stream and updates PriceBus."""
    def __init__(self, cfg: BotConfig):
        super().__init__(cfg)
        self.bus: PriceBus = cfg.params["bus"]
        self.use_us: bool = bool(cfg.params.get("use_us", True))
        self.symbols: List[str] = [s.upper() for s in cfg.params.get("symbols", [])]
        if not self.symbols:
            raise ValueError("WSPriceBot requires 'symbols'")
        self._task: Optional[asyncio.Task] = None
        self._stopping = False

    def _ws_url(self) -> str:
        host = "stream.binance.us:9443" if self.use_us else "stream.binance.com:9443"
        streams = "/".join([f"{s.lower()}@trade" for s in self.symbols])
        return f"wss://{host}/stream?streams={streams}"

    async def _run_ws(self) -> None:
        backoff = 1.0
        while not self._stopping:
            url = self._ws_url()
            try:
                self.log.info("connecting %s (EST %s)", url, ts_est())
                async with websockets.connect(url, ping_interval=15, ping_timeout=20) as ws:
                    self.log.info("connected (EST %s)", ts_est())
                    backoff = 1.0
                    async for raw in ws:
                        if self._stopping:
                            break
                        try:
                            msg = json.loads(raw)
                            data = msg.get("data", msg)
                            s = (data.get("s") or msg.get("stream", "").split("@")[0]).upper()
                            p = data.get("p")
                            if p is not None:
                                await self.bus.set_last(s, Decimal(p))
                        except Exception as parse_err:
                            self.log.debug("parse error: %s", parse_err)
            except (ConnectionClosed, OSError) as net_err:
                self.log.warning("ws closed: %s", net_err)
            if self._stopping:
                break
            jitter = 1 + random.uniform(-self.cfg.jitter, self.cfg.jitter)
            delay = min(backoff * jitter, self.cfg.max_backoff)
            self.log.info("reconnecting in %.1fs (EST %s)", delay, ts_est())
            await asyncio.sleep(delay)
            backoff = min(backoff * 2, self.cfg.max_backoff)

    async def on_start(self) -> None:
        self._stopping = False
        self._task = asyncio.create_task(self._run_ws())

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

# ----------------------- Paper trading pieces --------------------
@dataclass
class PaperOrder:
    orderId: int
    side: str      # "BUY" / "SELL"
    price: Decimal
    qty: Decimal
    status: str = "NEW"      # NEW, FILLED, CANCELED
    executedQty: Decimal = d("0")
    tag: str = ""            # "momentum" or ""
    last_ts: float = 0.0     # epoch seconds (last action)

@dataclass
class PaperBroker:
    base_bal: Decimal
    quote_bal: Decimal
    fee_rate: Decimal
    tick: Decimal
    step: Decimal
    min_qty: Decimal
    min_notional: Decimal
    next_id: int = 1
    orders: Dict[int, PaperOrder] = field(default_factory=dict)

    def _round_down(self, val: Decimal, quantum: Decimal) -> Decimal:
        if quantum == 0: return val
        q = (val / quantum).to_integral_value(rounding=ROUND_DOWN)
        return q * quantum

    def _touch(self, o: PaperOrder) -> None:
        o.last_ts = time.time()

    def place_limit(self, side: str, price: Decimal, qty: Decimal, tag: str = "") -> PaperOrder:
        price = self._round_down(price, self.tick)
        qty   = self._round_down(qty, self.step)
        if qty < self.min_qty or (price * qty) < self.min_notional:
            raise ValueError("Order below exchange minimums")
        oid = self.next_id; self.next_id += 1
        o = PaperOrder(oid, side, price, qty, tag=tag, last_ts=time.time())
        self.orders[oid] = o
        return o

    def cancel_side(self, side: str) -> int:
        n = 0
        for o in self.orders.values():
            if o.status == "NEW" and o.side == side:
                o.status = "CANCELED"; self._touch(o); n += 1
        return n

    def cancel_all(self) -> int:
        n = 0
        for o in self.orders.values():
            if o.status == "NEW":
                o.status = "CANCELED"; self._touch(o); n += 1
        return n

    def open_by_side(self, side: str) -> List[PaperOrder]:
        return [o for o in self.orders.values() if o.status == "NEW" and o.side == side]

    def open_qty(self, side: str) -> Decimal:
        return sum(o.qty for o in self.orders.values() if o.status == "NEW" and o.side == side) or d("0")

    def mark_price(self, last: Decimal, log: logging.Logger) -> List[PaperOrder]:
        """Auto-fill on last: BUY if last <= limit; SELL if last >= limit."""
        filled: List[PaperOrder] = []
        for o in list(self.orders.values()):
            if o.status != "NEW":
                continue
            can_fill = (last <= o.price) if o.side == "BUY" else (last >= o.price)
            if not can_fill:
                log.debug("NO FILL: %s #%s limit=%s last=%s qty=%s", o.side, o.orderId, o.price, last, o.qty)
                continue

            if o.side == "BUY":
                notional = o.price * o.qty
                fee = notional * self.fee_rate
                if self.quote_bal >= (notional + fee):
                    before_q, before_b = self.quote_bal, self.base_bal
                    self.quote_bal -= (notional + fee)
                    self.base_bal  += o.qty
                    o.status = "FILLED"; o.executedQty = o.qty; self._touch(o)
                    filled.append(o)
                    log.info("FILL: BUY #%s @ %s qty=%s last=%s notional=%s fee=%s "
                             "bal USD %s->%s BASE %s->%s %s (EST %s)",
                             o.orderId, o.price, o.qty, last, notional, fee,
                             before_q, self.quote_bal, before_b, self.base_bal,
                             f'[{o.tag}]' if o.tag else '', ts_est())
                else:
                    log.debug("NO FILL: BUY #%s insufficient quote %s < %s+%s",
                              o.orderId, self.quote_bal, notional, fee)
            else:
                if self.base_bal >= o.qty:
                    gross = o.price * o.qty
                    fee = gross * self.fee_rate
                    before_q, before_b = self.quote_bal, self.base_bal
                    self.base_bal  -= o.qty
                    self.quote_bal += (gross - fee)
                    o.status = "FILLED"; o.executedQty = o.qty; self._touch(o)
                    filled.append(o)
                    log.info("FILL: SELL #%s @ %s qty=%s last=%s gross=%s fee=%s "
                             "bal USD %s->%s BASE %s->%s (EST %s)",
                             o.orderId, o.price, o.qty, last, gross, fee,
                             before_q, self.quote_bal, before_b, self.base_bal,
                             ts_est())
                else:
                    log.debug("NO FILL: SELL #%s insufficient base %s < %s",
                              o.orderId, self.base_bal, o.qty)
        return filled

    def sweep_balances(self) -> Tuple[Decimal, Decimal]:
        b, q = self.base_bal, self.quote_bal
        self.base_bal = d("0")
        self.quote_bal = d("0")
        return b, q

# ----------------------- Paper trading bot ----------------------
class PaperTradingBot(BaseBot):
    """
    - Keep exactly ONE working BUY (repriced on events). SELLs are placed from available base; NOT canceled on BUY fill.
    - order_expiry_seconds = TTL for orders (0 = never auto-cancel).
    - BUY tranche budgeting via buy_tranche_quote or buy_tranche_pct (fee-aware sizing).
    - Minimal momentum seed (one-time) if price breaks out with no prior BUY fills.
    - On stop: cancel all, return funds to pool.
    """
    def __init__(self, cfg: BotConfig):
        super().__init__(cfg)
        p = cfg.params
        self.bus: PriceBus = p["bus"]
        self.fund_mgr: FundManager = p["fund_mgr"]
        self.hub: ReportHub = p["hub"]
        self.symbol: str = p["symbol"].upper()
        flt = p["filters"]
        self.broker = PaperBroker(
            base_bal=d(p["balances"]["base"]),
            quote_bal=d(p["balances"]["quote"]),
            fee_rate=d(p.get("fee_rate", 0.0)),
            tick=d(flt["tick"]), step=d(flt["step"]),
            min_qty=d(flt["min_qty"]), min_notional=d(flt["min_notional"]),
        )
        self.buy_gap = Decimal(str(p["gaps"]["buy_gap_pct"])) / Decimal("100")
        self.sell_gap = Decimal(str(p["gaps"]["sell_gap_pct"])) / Decimal("100")
        self.min_profit = Decimal(str(p.get("min_profit_pct", p["gaps"]["sell_gap_pct"]))) / Decimal("100")
        self.order_ttl = int(p.get("order_expiry_seconds", 0))

        # BUY tranche sizing
        self.buy_tranche_quote: Decimal = d(p.get("buy_tranche_quote", "0"))
        self.buy_tranche_pct: Decimal = Decimal(str(p.get("buy_tranche_pct", 0.25)))
        if self.buy_tranche_pct <= 0 or self.buy_tranche_pct > 1:
            self.buy_tranche_pct = Decimal("0.25")

        # Momentum (optional, one-shot)
        self.momentum_enabled: bool = bool(p.get("momentum_enabled", True))
        self.momentum_breakout_pct: Decimal = Decimal(str(p.get("momentum_breakout_pct", 1.0)))
        self.momentum_seed_quote: Decimal = d(p.get("momentum_seed_quote", 50))
        self.momentum_max_seeds: int = int(p.get("momentum_max_seeds", 1))
        self._momentum_ref_price: Optional[Decimal] = None
        self._momentum_seeds_used: int = 0
        self._any_buy_ever_filled: bool = False

        # last-fill memory (for compact report)
        self._last_buy_fill_px: Optional[Decimal] = None
        self._last_buy_fill_ts: Optional[float] = None
        self._last_buy_fill_m: bool = False
        self._last_sell_fill_px: Optional[Decimal] = None
        self._last_sell_fill_ts: Optional[float] = None

        self.account_quote = self._quote_for_symbol(self.symbol)
        self._allocated_quote = d(p["balances"]["quote"])
        self._allocated_base  = d(p["balances"]["base"])
        self._allocation_done = False

        self.last_buy_price: Optional[Decimal] = None
        self._placed_at: Dict[int, float] = {}
        self._handled_fills: set[int] = set()

        # Reporting / PnL
        self._initial_equity: Optional[Decimal] = None
        self._last_price: Optional[Decimal] = None

        # Retrigger counters
        self.stage: int = 0
        self.retriggers: Dict[str, int] = {"buy_fill": 0, "sell_fill": 0, "ttl": 0, "ensure": 0}

    async def on_start(self) -> None:
        if not self._allocation_done:
            want = {self.account_quote: self._allocated_quote, self._base_for_symbol(self.symbol): self._allocated_base}
            got = await self.fund_mgr.allocate(want)
            self.broker.quote_bal = got.get(self.account_quote, d("0"))
            self.broker.base_bal  = got.get(self._base_for_symbol(self.symbol), d("0"))
            self._allocation_done = True

    async def on_stop(self) -> None:
        try:
            await self.hub.clear(self.cfg.name)
        except Exception:
            pass
        n = self.broker.cancel_all()
        self.log.info("canceled %s open orders (EST %s)", n, ts_est())
        base_bal, quote_bal = self.broker.sweep_balances()
        base_sym = self._base_for_symbol(self.symbol)
        if base_bal > 0:
            await self.fund_mgr.deposit(base_sym, base_bal)
        if quote_bal > 0:
            await self.fund_mgr.deposit(self.account_quote, quote_bal)

    # ----------- helpers -----------
    def _base_for_symbol(self, sym: str) -> str:
        if sym.endswith("USDT"): return sym[:-4]
        if sym.endswith("USD"):  return sym[:-3]
        return sym[:-3]

    def _quote_for_symbol(self, sym: str) -> str:
        if sym.endswith("USDT"): return "USDT"
        if sym.endswith("USD"):  return "USD"
        return sym[-3:]

    def _dp(self, q: Decimal) -> int:
        e = -q.as_tuple().exponent
        return max(0, int(e))

    def _fmt(self, x: Decimal | None, places: int) -> str:
        if x is None:
            return "n/a"
        q = Decimal(1).scaleb(-places)
        return str(x.quantize(q))

    def _track_placement(self, o: PaperOrder) -> None:
        self._placed_at[o.orderId] = time.time()

    def _can_place_buy(self) -> bool:
        return self.broker.quote_bal >= self.broker.min_notional

    def _fee_aware_qty(self, price: Decimal, quote_budget: Decimal) -> Decimal:
        """Max qty such that price*qty*(1+fee) <= quote_budget, rounded to step."""
        f = self.broker.fee_rate
        if price <= 0:
            return d("0")
        spendable = quote_budget / (Decimal("1") + f)
        qty = self.broker._round_down(spendable / price, self.broker.step)
        while qty > 0 and (price * qty) * (Decimal("1") + f) > quote_budget:
            qty = self.broker._round_down(qty - self.broker.step, self.broker.step)
        return max(qty, d("0"))

    def _ensure_single_buy(self, spot: Decimal, reason: str) -> None:
        canceled = self.broker.cancel_side("BUY")
        if canceled:
            self.log.info("CANCEL %s BUY(s) before reprice [reason=%s] (EST %s)", canceled, reason, ts_est())
        if not self._can_place_buy():
            self.log.info("Skip BUY ensure (%s): insufficient quote", reason)
            return

        target_buy = spot * (Decimal("1") - self.buy_gap)

        # tranche budget: absolute cap or percentage of free quote
        if self.buy_tranche_quote > 0:
            budget_cap = self.buy_tranche_quote
        else:
            budget_cap = self.broker.quote_bal * self.buy_tranche_pct
        budget = min(self.broker.quote_bal, budget_cap)

        # must at least meet exchange min notional incl. fee
        min_needed = self.broker.min_notional * (Decimal("1") + self.broker.fee_rate)
        if budget < min_needed:
            self.log.info("Skip BUY ensure (%s): insufficient quote (budget=%s < need=%s)",
                          reason, budget, min_needed)
            return

        qty = self._fee_aware_qty(target_buy, budget)
        try:
            o = self.broker.place_limit("BUY", target_buy, qty)
        except ValueError as e:
            self.log.info("Skip BUY ensure (%s): qty=%s price=%s -> %s", reason, qty, target_buy, e)
            return
        self._track_placement(o)
        self.stage += 1
        self.retriggers[reason] = self.retriggers.get(reason, 0) + 1
        self.log.info("PLACE: BUY #%s @%s qty=%s (budget=%s) [reason=%s stage=%s] (EST %s)",
                      o.orderId, o.price, o.qty, budget, reason, self.stage, ts_est())

    def _price_trail_text(self, p_dp: int) -> Optional[str]:
        recent = self.bus.recent_prices(self.symbol, int(os.getenv("REPORT_TICK_HISTORY", "5")))
        if not recent:
            return None
        trail_str = " → ".join(self._fmt(p, p_dp) for p in recent)
        if len(recent) >= 2:
            start, end = recent[0], recent[-1]
            pct = (end - start) / start * Decimal("100") if start else Decimal("0")
            return f"{trail_str}  (Δ {self._fmt(pct, 4)}%)"
        return trail_str

    async def _publish_snapshot(self, price: Optional[Decimal]) -> None:
        """Compact snapshot for global report."""
        p_dp  = self._dp(self.broker.tick)
        q_dp  = self._dp(self.broker.step)

        if price is None:
            price_str = None; pnl_str = None; pnl_pct_str = None
        else:
            price_str = self._fmt(price, p_dp)
            equity = self.broker.base_bal * price + self.broker.quote_bal
            if self._initial_equity is None:
                self._initial_equity = equity
            pnl = equity - self._initial_equity
            pnl_pct = (pnl / self._initial_equity * Decimal("100")) if self._initial_equity > 0 else Decimal("0")
            pnl_str = self._fmt(pnl, 2)
            pnl_pct_str = self._fmt(pnl_pct, 4) + "%"

        # open orders summary (counts, sums, momentum count)
        open_buys = [o for o in self.broker.orders.values() if o.status=="NEW" and o.side=="BUY"]
        open_sells= [o for o in self.broker.orders.values() if o.status=="NEW" and o.side=="SELL"]
        buy_count = len(open_buys)
        sell_count= len(open_sells)
        buy_qty_sum = sum((o.qty for o in open_buys), d("0"))
        sell_qty_sum= sum((o.qty for o in open_sells), d("0"))
        buy_m_count = sum(1 for o in open_buys if o.tag=="momentum")

        # cap list of open orders for display
        max_open = int(os.getenv("REPORT_MAX_OPEN", "3"))
        def to_line(o: PaperOrder) -> str:
            tag = "[M]" if o.tag=="momentum" else ""
            side = f"{o.side}{tag}"
            return f"{side} @ {self._fmt(o.price, p_dp)}  qty={self._fmt(o.qty, q_dp)}  placed {format_time_hms_est(o.last_ts)}"
        open_display = [to_line(o) for o in sorted(open_buys+open_sells, key=lambda x: x.last_ts, reverse=True)[:max_open]]

        snap = {
            "name": self.cfg.name,
            "symbol": self.symbol,
            "price": price_str,
            "price_trail": self._price_trail_text(p_dp),
            "stage": self.stage,
            "quote_sym": self._quote_for_symbol(self.symbol),
            "base_sym": self._base_for_symbol(self.symbol),
            "quote_bal": self._fmt(self.broker.quote_bal, 2),
            "base_bal": self._fmt(self.broker.base_bal, q_dp),
            "pnl": pnl_str if pnl_str is not None else "n/a",
            "pnl_pct": pnl_pct_str if pnl_pct_str is not None else "n/a",
            "active": format_duration_dhms(self.active_duration()),
            "open_summary": {
                "buy_count": buy_count,
                "sell_count": sell_count,
                "buy_qty_sum": self._fmt(buy_qty_sum, q_dp),
                "sell_qty_sum": self._fmt(sell_qty_sum, q_dp),
                "buy_momentum_count": buy_m_count,
            },
            "last_fills": {
                "buy": (self._fmt(self._last_buy_fill_px, p_dp) if self._last_buy_fill_px is not None else None,
                        format_time_hms_est(self._last_buy_fill_ts),
                        self._last_buy_fill_m),
                "sell": (self._fmt(self._last_sell_fill_px, p_dp) if self._last_sell_fill_px is not None else None,
                         format_time_hms_est(self._last_sell_fill_ts)),
            },
            "open_display": open_display,
        }
        await self.hub.put(self.cfg.name, snap)

    def _maybe_momentum_seed(self, spot: Decimal) -> None:
        if not self.momentum_enabled: return
        if self._any_buy_ever_filled: return
        if self._momentum_seeds_used >= self.momentum_max_seeds: return
        if self._momentum_ref_price is None:
            self._momentum_ref_price = spot
            return
        threshold = self._momentum_ref_price * (Decimal("1") + self.momentum_breakout_pct / Decimal("100"))
        if spot < threshold:
            return

        budget = min(self.momentum_seed_quote, self.broker.quote_bal)
        qty = self._fee_aware_qty(spot, budget)
        if qty < self.broker.min_qty or (spot * qty) < self.broker.min_notional:
            self.log.info("Momentum: seed too small (qty=%s, notional=%s); skip", qty, spot*qty)
            self._momentum_seeds_used += 1
            return

        try:
            o = self.broker.place_limit("BUY", spot, qty, tag="momentum")
            self._track_placement(o)
            self.stage += 1
            self.retriggers["ensure"] = self.retriggers.get("ensure", 0) + 1
            self._momentum_seeds_used += 1
            self.log.info("Momentum: PLACE BUY #%s @ %s qty=%s (budget≈%s) (EST %s)",
                          o.orderId, spot, qty, spot*qty, ts_est())
        except ValueError as e:
            self.log.info("Momentum: cannot place seed BUY -> %s", e)
            self._momentum_seeds_used += 1

    # --------------- main tick ---------------
    async def on_tick(self) -> None:
        price = self.bus.get_last(self.symbol)
        if price is None:
            try:
                price = await self.bus.wait_for_price(self.symbol, timeout=5)
            except TimeoutError:
                await self._publish_snapshot(None)
                return

        self._last_price = price
        equity = self.broker.base_bal * price + self.broker.quote_bal
        if self._initial_equity is None:
            self._initial_equity = equity

        # Momentum seed check (one-shot)
        self._maybe_momentum_seed(price)

        # Mark fills
        filled_now = self.broker.mark_price(price, self.log)

        for o in filled_now:
            if o.side == "BUY":
                self.last_buy_price = o.price
                self._any_buy_ever_filled = True
                self._last_buy_fill_px = o.price
                self._last_buy_fill_ts = o.last_ts
                self._last_buy_fill_m  = (o.tag == "momentum")
            else:
                self._last_sell_fill_px = o.price
                self._last_sell_fill_ts = o.last_ts

        # SELL placement: allow multiple; use available base
        available_base = self.broker.base_bal - self.broker.open_qty("SELL")
        if available_base >= self.broker.min_qty and self.last_buy_price is not None:
            floor = self.last_buy_price * (Decimal("1") + self.min_profit)
            target_sell = max(self.last_buy_price * (Decimal("1") + self.sell_gap), floor)
            qty = self.broker._round_down(available_base, self.broker.step)
            try:
                o = self.broker.place_limit("SELL", target_sell, qty)
                self._track_placement(o)
                self.log.info("PLACE: SELL #%s @%s qty=%s (available_base used) (EST %s)",
                              o.orderId, target_sell, qty, ts_est())
            except ValueError as e:
                self.log.info("Skip SELL (available_base=%s): %s", available_base, e)

        # TTL cancellation
        if self.order_ttl > 0:
            now = time.time()
            for o in [*self.broker.open_by_side("BUY"), *self.broker.open_by_side("SELL")]:
                placed = self._placed_at.get(o.orderId)
                if placed and (now - placed) >= self.order_ttl:
                    o.status = "CANCELED"
                    self.broker._touch(o)
                    self._placed_at.pop(o.orderId, None)
                    self.log.info("CANCEL: %s #%s (TTL %ss) (EST %s)",
                                  o.side, o.orderId, self.order_ttl, ts_est())

        # Ensure/reprice single BUY
        if not self.broker.open_by_side("BUY"):
            self._ensure_single_buy(price, reason="ensure")

        await self._publish_snapshot(self._last_price)

# -------------------------- supervisor + manager -----------------
class BotSupervisor:
    def __init__(self, factory: Callable[[BotConfig], BaseBot], cfg: BotConfig):
        self.factory = factory
        self.cfg = cfg
        self.log = logging.getLogger(f"sup.{cfg.name}")
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self.factory(self.cfg).run())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

class BotManager:
    def __init__(self):
        self.log = logging.getLogger("manager")
        self._supers: Dict[str, BotSupervisor] = {}
        self._meta: Dict[str, Tuple[Callable[[BotConfig], BaseBot], BotConfig]] = {}
        self._started = False
        self._lock = asyncio.Lock()

    def add(self, name: str, factory: Callable[[BotConfig], BaseBot], cfg: BotConfig) -> None:
        if name in self._supers:
            raise ValueError(f"bot '{name}' exists")
        self._supers[name] = BotSupervisor(factory, cfg)
        self._meta[name] = (factory, cfg)

    async def add_and_start(self, name: str, factory: Callable[[BotConfig], BaseBot], cfg: BotConfig) -> None:
        async with self._lock:
            if name in self._supers:
                raise ValueError(f"bot '{name}' exists")
            sup = BotSupervisor(factory, cfg)
            self._supers[name] = sup
            self._meta[name] = (factory, cfg)
            if self._started:
                await sup.start()

    async def remove_and_stop(self, name: str) -> bool:
        async with self._lock:
            sup = self._supers.pop(name, None)
        if not sup:
            return False
        await sup.stop()
        return True

    def get_meta(self, name: str) -> Optional[Tuple[Callable[[BotConfig], BaseBot], BotConfig]]:
        return self._meta.get(name)

    def list_names(self) -> List[str]:
        return sorted(self._supers.keys())

    async def start(self) -> None:
        self._started = True
        for sup in self._supers.values():
            await sup.start()

    async def stop(self) -> None:
        await asyncio.gather(*(sup.stop() for sup in self._supers.values()))

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        stop_ev = asyncio.Event()

        def _signal(*_: Any) -> None:
            self.log.warning("signal received; shutting down…")
            stop_ev.set()

        for s in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(s, _signal)
            except NotImplementedError:
                pass

        await self.start()
        await stop_ev.wait()
        await self.stop()

# --------------------------- admin HTTP --------------------------
class AdminHTTP:
    """
    Endpoints:
      GET  /bots
      POST /bots
      DELETE /bots/{name}
      GET  /funds
    """
    def __init__(self, mgr: BotManager, bus: PriceBus, funds: FundManager, hub: ReportHub):
        self.mgr = mgr
        self.bus = bus
        self.funds = funds
        self.hub = hub
        self.log = logging.getLogger("admin")
        self.token = os.getenv("ADMIN_TOKEN")  # optional
        self.filters_default = {"tick":"0.01","step":"0.000001","min_qty":"0.00001","min_notional":"10.0"}
        self.report_every = int(os.getenv("REPORT_EVERY", "60"))
        self.report_mode = os.getenv("REPORT_MODE", "compact").lower()
        self.report_max_open = int(os.getenv("REPORT_MAX_OPEN", "3"))

        self.app = web.Application(middlewares=[self._auth_mw])
        self.app.add_routes([
            web.get("/bots", self.get_bots),
            web.post("/bots", self.post_bot),
            web.delete("/bots/{name}", self.del_bot),
            web.get("/funds", self.get_funds),
        ])
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None
        self._report_task: Optional[asyncio.Task] = None

    @web.middleware
    async def _auth_mw(self, request, handler):
        if self.token and request.headers.get("x-admin-token") != self.token:
            return web.json_response({"error":"unauthorized"}, status=401)
        return await handler(request)

    async def get_bots(self, request):
        return web.json_response({"bots": self.mgr.list_names()})

    async def get_funds(self, request):
        bal = await self.funds.balances()
        return web.json_response({k: str(v) for k, v in bal.items()})

    async def post_bot(self, request):
        payload = await request.json()
        sym = str(payload.get("symbol","")).upper()
        if not sym:
            return web.json_response({"error":"symbol required"}, status=400)
        venue = str(payload.get("venue","us")).lower()
        use_us = (venue == "us")

        buy_gap = Decimal(str(payload.get("buy_gap_pct", 0.2)))
        sell_gap = Decimal(str(payload.get("sell_gap_pct", 0.2)))
        min_profit = Decimal(str(payload.get("min_profit_pct", payload.get("sell_gap_pct", 0.2))))
        order_expiry_seconds = int(payload.get("order_expiry_seconds", payload.get("order_ttl_seconds", 0)))
        fee = Decimal(str(payload.get("fee_rate", 0.0001)))
        starting_base_qty = Decimal(str(payload.get("starting_base_qty", payload.get("base", 0))))
        starting_quote_qty = Decimal(str(payload.get("starting_quote_qty", payload.get("quote", 2000))))
        bot_lifetime_seconds = int(payload.get("bot_lifetime_seconds", payload.get("expiry_sec", 0)))
        respawn_delay_seconds = int(payload.get("respawn_delay_seconds", payload.get("respawn_after_sec", 0)))

        # momentum
        momentum_enabled = bool(payload.get("momentum_enabled", True))
        momentum_breakout_pct = Decimal(str(payload.get("momentum_breakout_pct", 1.0)))
        momentum_seed_quote = Decimal(str(payload.get("momentum_seed_quote", 50)))
        momentum_max_seeds = int(payload.get("momentum_max_seeds", 1))

        # BUY tranche
        buy_tranche_quote = Decimal(str(payload.get("buy_tranche_quote", 0)))
        buy_tranche_pct   = Decimal(str(payload.get("buy_tranche_pct", 0.25)))

        filters = payload.get("filters", self.filters_default)

        # ensure WS feeder
        ws_name = f"ws-{sym.lower()}"
        if ws_name not in self.mgr._supers:
            ws_cfg = BotConfig(
                name=ws_name, tick_seconds=0.1,
                params={"bus": self.bus, "use_us": use_us, "symbols": [sym]},
            )
            await self.mgr.add_and_start(ws_name, lambda c: WSPriceBot(c), ws_cfg)

        paper_name = f"paper-{sym.lower()}"
        if paper_name in self.mgr._supers:
            return web.json_response({"error":f"bot {paper_name} exists"}, status=409)

        paper_cfg = BotConfig(
            name=paper_name, tick_seconds=1.0,
            params={
                "bus": self.bus,
                "fund_mgr": self.funds,
                "hub": self.hub,
                "symbol": sym,
                "filters": filters,
                "gaps": {"buy_gap_pct": float(buy_gap), "sell_gap_pct": float(sell_gap)},
                "min_profit_pct": float(min_profit),
                "balances": {"base": str(starting_base_qty), "quote": str(starting_quote_qty)},
                "fee_rate": float(fee),
                "order_expiry_seconds": order_expiry_seconds,
                "bot_lifetime_seconds": bot_lifetime_seconds,
                # momentum
                "momentum_enabled": momentum_enabled,
                "momentum_breakout_pct": float(momentum_breakout_pct),
                "momentum_seed_quote": str(momentum_seed_quote),
                "momentum_max_seeds": momentum_max_seeds,
                # buy tranche
                "buy_tranche_quote": str(buy_tranche_quote),
                "buy_tranche_pct": float(buy_tranche_pct),
            },
        )
        await self.mgr.add_and_start(paper_name, lambda c: PaperTradingBot(c), paper_cfg)
        self.log.info("added %s (+ %s if new) (EST %s)", paper_name, ws_name, ts_est())

        if respawn_delay_seconds:
            self.mgr._meta[paper_name][1].params["respawn_delay_seconds"] = respawn_delay_seconds

        return web.json_response({"ok": True, "added": [paper_name], "ws": ws_name})

    async def del_bot(self, request):
        name = request.match_info["name"]
        meta = self.mgr.get_meta(name)
        ok = await self.mgr.remove_and_stop(name)
        if not ok:
            return web.json_response({"removed": False, "name": name}, status=404)
        qs = request.rel_url.query
        respawn_after = int(qs.get("respawn_after", "0")) or 0
        if respawn_after and meta:
            factory, old_cfg = meta
            new_cfg = BotConfig(
                name=old_cfg.name, tick_seconds=old_cfg.tick_seconds,
                max_backoff=old_cfg.max_backoff, jitter=old_cfg.jitter,
                params=dict(old_cfg.params),
            )
            async def _respawn():
                await asyncio.sleep(respawn_after)
                await self.mgr.add_and_start(new_cfg.name, factory, new_cfg)
                self.log.info("respawned %s after %ss (EST %s)", new_cfg.name, respawn_after, ts_est())
            asyncio.create_task(_respawn())
        return web.json_response({"removed": True, "name": name, "respawn_scheduled": bool(respawn_after)})

    def _render_compact(self, pools: Dict[str, Decimal], snaps: Dict[str, Dict[str, Any]]) -> str:
        lines = [f"=== Global Report @ {ts_est()} ==="]
        if pools:
            pool_line = " | ".join(f"{a} {pools[a]}" for a in sorted(pools.keys()))
            lines.append(f"Available Fund: {pool_line}")
        else:
            lines.append("Available Fund: (none)")
        for name, s in sorted(snaps.items()):
            sym = s.get("symbol","")
            last = s.get("price","n/a")
            stage = s.get("stage",0)
            active = s.get("active","0h 0m 0s")
            pnl = s.get("pnl","n/a")
            pnl_pct = s.get("pnl_pct","n/a")
            qsym = s.get("quote_sym",""); bsym = s.get("base_sym","")
            qbal = s.get("quote_bal","0"); bbal = s.get("base_bal","0")
            lines.append(f"\n{name} | Stage {stage} | Active {active} | Last={last}")
            trail = s.get("price_trail")
            if trail:
                lines.append(f"Price(≤{os.getenv('REPORT_TICK_HISTORY','5')}): {trail}")
            lines.append(f"Funds: {qsym} {qbal} | {bsym} {bbal}")
            lines.append(f"PnL: {pnl} ( {pnl_pct} )")
            osum = s.get("open_summary",{})
            lines.append(f"Open: BUY {osum.get('buy_count',0)} (qty {osum.get('buy_qty_sum','0')}; M:{osum.get('buy_momentum_count',0)}), "
                         f"SELL {osum.get('sell_count',0)} (qty {osum.get('sell_qty_sum','0')})")
            bf = s.get("last_fills",{}).get("buy",(None,None,False))
            sf = s.get("last_fills",{}).get("sell",(None,None))
            buy_tag = "[M]" if (bf[2] if len(bf)>2 else False) else ""
            lines.append(f"Last fills: BUY{buy_tag} @ {bf[0] or 'n/a'} ({bf[1]}), SELL @ {sf[0] or 'n/a'} ({sf[1]})")
            ods = s.get("open_display",[])
            if ods:
                lines.append("Open orders:")
                for od in ods:
                    lines.append(f"  {od}")
        return "\n".join(lines)

    async def _global_reporter(self):
        log = logging.getLogger("report")
        bar = "=" * 80
        while True:
            try:
                await asyncio.sleep(self.report_every)
                pools = await self.funds.balances()
                snaps = await self.hub.all()
                if self.report_mode == "compact":
                    text = self._render_compact(pools, snaps)
                else:
                    text = json.dumps({"pools": {k:str(v) for k,v in pools.items()},
                                       "snaps": snaps}, indent=2)
                log.info("\n%s\n%s\n%s\n", bar, text, bar)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.getLogger("report").warning("reporter error: %s", e)

    async def start(self, host="127.0.0.1", port=8080):
        self._runner = web.AppRunner(self.app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, host=host, port=port)
        await self._site.start()
        self._report_task = asyncio.create_task(self._global_reporter())
        self.log.info("admin API listening on http://%s:%s (EST %s)", host, port, ts_est())

    async def stop(self):
        if self._report_task:
            self._report_task.cancel()
            try:
                await self._report_task
            except asyncio.CancelledError:
                pass
        if self._site:
            await self._site.stop()
        if self._runner:
            await self._runner.cleanup()

# --------------------------- CLI & bootstrap ---------------------
def parse_args():
    ap = argparse.ArgumentParser(description="Paper bots with admin API + fund manager + compact global report (price trails, last-price fills, fee-aware sizing, tranche buys)")
    ap.add_argument("--admin-host", default=os.getenv("ADMIN_HOST","127.0.0.1"))
    ap.add_argument("--admin-port", type=int, default=int(os.getenv("ADMIN_PORT","8080")))
    ap.add_argument("--venue", choices=["us","com"], default="us", help="Binance venue")
    ap.add_argument("--paper", action="append", default=[], help="Start these symbols at boot (repeatable)")
    ap.add_argument("--seed-quote", default=os.getenv("SEED_QUOTE","USD"))
    ap.add_argument("--seed-amount", type=float, default=float(os.getenv("SEED_AMOUNT","20000")))
    return ap.parse_args()

async def main() -> None:
    setup_logging()
    args = parse_args()

    bus = PriceBus()
    funds = FundManager()
    hub = ReportHub()
    funds.seed(args.seed_quote, d(args.seed_amount))

    mgr = BotManager()

    # Optional boot bots
    for sym in [s.upper() for s in args.paper]:
        ws = BotConfig(
            name=f"ws-{sym.lower()}",
            tick_seconds=0.1,
            params={"bus": bus, "use_us": (args.venue=='us'), "symbols":[sym]},
        )
        paper = BotConfig(
            name=f"paper-{sym.lower()}",
            tick_seconds=1.0,
            params={
                "bus": bus,
                "fund_mgr": funds,
                "hub": hub,
                "symbol": sym,
                "filters": {"tick":"0.01","step":"0.000001","min_qty":"0.00001","min_notional":"10.0"},
                "gaps": {"buy_gap_pct": 0.2, "sell_gap_pct": 0.2},
                "min_profit_pct": 0.1,
                "balances": {"base":"0", "quote":"2000"},
                "fee_rate": 0.0001,
                "order_expiry_seconds": 0,
                "bot_lifetime_seconds": 0,
                "momentum_enabled": True,
                "momentum_breakout_pct": 1.0,
                "momentum_seed_quote": "50",
                "momentum_max_seeds": 1,
                "buy_tranche_quote": "0",
                "buy_tranche_pct": 0.25,
            },
        )
        await mgr.add_and_start(ws.name, lambda c: WSPriceBot(c), ws)
        await mgr.add_and_start(paper.name, lambda c: PaperTradingBot(c), paper)

    admin = AdminHTTP(mgr, bus, funds, hub)
    await admin.start(host=args.admin_host, port=args.admin_port)

    try:
        await mgr.run()
    finally:
        await admin.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
