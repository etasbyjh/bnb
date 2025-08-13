# bot_manager.py
# Python 3.10+
# - WebSocket price feed(s)
# - Paper-trading bot(s)
# - Central FundManager shared by bots
# - Global one-minute report (configurable via REPORT_EVERY)
# - Admin HTTP API to add/remove/respawn bots at runtime
# - Clear params; graceful cancel (close orders + return funds)
# - Optional lifetime per bot + optional auto-respawn

from __future__ import annotations

import asyncio
import json
import logging
import random
import signal
import argparse
import os
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, Optional, Callable, List, Tuple
from datetime import datetime, timezone

from aiohttp import web
import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed


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


# ----------------------- centralized fund manager ----------------
def d(x) -> Decimal:
    return Decimal(str(x))


class FundManager:
    """
    Tracks global funds across all bots.
    Balances: {asset -> Decimal}, e.g., {"USD": 20000, "ETH": 0}
    """
    def __init__(self):
        self._bal: Dict[str, Decimal] = {}
        self._lock = asyncio.Lock()

    def seed(self, asset: str, amt: Decimal) -> None:
        self._bal[asset] = self._bal.get(asset, d("0")) + amt

    async def deposit(self, asset: str, amt: Decimal) -> None:
        async with self._lock:
            self._bal[asset] = self._bal.get(asset, d("0")) + amt

    async def withdraw(self, asset: str, amt: Decimal) -> bool:
        async with self._lock:
            cur = self._bal.get(asset, d("0"))
            if cur >= amt:
                self._bal[asset] = cur - amt
                return True
            return False

    async def allocate(self, want: Dict[str, Decimal]) -> Dict[str, Decimal]:
        """
        Try to allocate the requested amounts from the pool.
        Returns actually allocated amounts (may be less than requested).
        """
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
    """
    Bots push lightweight snapshots here; a global reporter prints a consolidated view.
    """
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
        # Optional lifetime in seconds (0/None = no expiry)
        life = cfg.params.get("bot_lifetime_sec", 0) or 0
        self.lifetime_sec: Optional[int] = int(life) if life else None

    async def on_start(self) -> None: ...
    async def on_stop(self) -> None: ...
    async def on_tick(self) -> None: raise NotImplementedError

    async def start(self) -> None:
        self._started_ts = asyncio.get_running_loop().time()
        self.log.info("starting")
        await self.on_start()

    async def stop(self) -> None:
        self.log.info("stopping")
        self._stop.set()
        await self.on_stop()

    def active_duration(self) -> float:
        if self._started_ts is None:
            return 0.0
        return max(0.0, asyncio.get_running_loop().time() - self._started_ts)

    async def run(self) -> None:
        await self.start()
        try:
            while not self._stop.is_set():
                # expiry check
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
    """In-memory store for latest prices + a notifier."""
    def __init__(self):
        self._prices: Dict[str, Decimal] = {}
        self._cv = asyncio.Condition()

    async def set_price(self, symbol: str, price: Decimal) -> None:
        symbol = symbol.upper()
        async with self._cv:
            self._prices[symbol] = price
            self._cv.notify_all()

    async def wait_for_price(self, symbol: str, timeout: Optional[float] = None) -> Decimal:
        symbol = symbol.upper()
        async with self._cv:
            if timeout is None:
                while symbol not in self._prices:
                    await self._cv.wait()
            else:
                loop = asyncio.get_running_loop()
                deadline = loop.time() + timeout
                while symbol not in self._prices:
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        raise TimeoutError(f"No price yet for {symbol}")
                    await asyncio.wait_for(self._cv.wait(), timeout=remaining)
            return self._prices[symbol]

    def get_last(self, symbol: str) -> Optional[Decimal]:
        return self._prices.get(symbol.upper())


# ----------------------- WebSocket price bot ---------------------
class WSPriceBot(BaseBot):
    """
    Subscribes to Binance(.com or .US) public trade streams and updates PriceBus.
    params:
      - use_us: bool
      - symbols: list[str]  (e.g., ["BTCUSD"] for US, ["BTCUSDT"] for .com)
      - bus: PriceBus
    """
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
                self.log.info("connecting %s", url)
                async with websockets.connect(url, ping_interval=15, ping_timeout=20) as ws:
                    self.log.info("connected")
                    backoff = 1.0
                    async for raw in ws:
                        if self._stopping:
                            break
                        try:
                            msg = json.loads(raw)
                            data = msg.get("data", msg)
                            if data.get("e") == "trade" and "p" in data:
                                symbol = (data.get("s") or msg.get("stream", "").split("@")[0]).upper()
                                price = Decimal(data["p"])
                                await self.bus.set_price(symbol, price)
                        except Exception as parse_err:
                            self.log.debug("parse error: %s", parse_err)
            except (ConnectionClosed, OSError) as net_err:
                self.log.warning("ws closed: %s", net_err)
            if self._stopping:
                break
            jitter = 1 + random.uniform(-self.cfg.jitter, self.cfg.jitter)
            delay = min(backoff * jitter, self.cfg.max_backoff)
            self.log.info("reconnecting in %.1fs", delay)
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
        if quantum == 0:
            return val
        q = (val / quantum).to_integral_value(rounding=ROUND_DOWN)
        return q * quantum

    def place_limit(self, side: str, price: Decimal, qty: Decimal) -> PaperOrder:
        price = self._round_down(price, self.tick)
        qty   = self._round_down(qty, self.step)
        if qty < self.min_qty or (price * qty) < self.min_notional:
            raise ValueError("Order below exchange minimums")
        oid = self.next_id; self.next_id += 1
        o = PaperOrder(oid, side, price, qty)
        self.orders[oid] = o
        return o

    def cancel_side(self, side: str) -> int:
        n = 0
        for o in self.orders.values():
            if o.status == "NEW" and o.side == side:
                o.status = "CANCELED"
                n += 1
        return n

    def cancel_all(self) -> int:
        n = 0
        for o in self.orders.values():
            if o.status == "NEW":
                o.status = "CANCELED"
                n += 1
        return n

    def open_by_side(self, side: str) -> List[PaperOrder]:
        return [o for o in self.orders.values() if o.status == "NEW" and o.side == side]

    def mark_price(self, last: Decimal) -> List[PaperOrder]:
        """Naive fill rule: if trade price crosses limit, fill fully. Returns list of fills."""
        filled: List[PaperOrder] = []
        for o in list(self.orders.values()):
            if o.status != "NEW":
                continue
            if o.side == "BUY" and last <= o.price:
                cost = o.price * o.qty
                fee  = cost * self.fee_rate
                if self.quote_bal >= (cost + fee):
                    self.quote_bal -= (cost + fee)
                    self.base_bal  += o.qty
                    o.status = "FILLED"; o.executedQty = o.qty
                    filled.append(o)
            elif o.side == "SELL" and last >= o.price:
                if self.base_bal >= o.qty:
                    gross = o.price * o.qty
                    fee   = gross * self.fee_rate
                    self.base_bal  -= o.qty
                    self.quote_bal += (gross - fee)
                    o.status = "FILLED"; o.executedQty = o.qty
                    filled.append(o)
        return filled

    def sweep_balances(self) -> Tuple[Decimal, Decimal]:
        b, q = self.base_bal, self.quote_bal
        self.base_bal = d("0")
        self.quote_bal = d("0")
        return b, q


# ----------------------- Paper trading bot ----------------------
class PaperTradingBot(BaseBot):
    """
    Paper bot that:
      - Keeps one BUY at spot*(1 - buy_gap) if funds allow
      - Keeps one SELL at last_buy*(1 + sell_gap), floor = last_buy*(1 + min_profit)
      - TTL cancels & reprices after N seconds
      - Publishes snapshots to ReportHub (global reporter prints them)
      - On stop: cancel all, return funds to FundManager
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
        self.order_ttl = int(p.get("order_ttl_sec", 0))  # 0 = no TTL

        self.account_quote = self._quote_for_symbol(self.symbol)
        self._allocated_quote = d(p["balances"]["quote"])
        self._allocated_base  = d(p["balances"]["base"])
        self._allocation_done = False

        self.last_buy_price: Optional[Decimal] = None
        self.last_sell_price: Optional[Decimal] = None
        self._placed_at: Dict[int, float] = {}
        self._handled_fills: set[int] = set()

        # Reporting / PnL
        self._initial_equity: Optional[Decimal] = None
        self._last_price: Optional[Decimal] = None

    async def on_start(self) -> None:
        # allocate funds from the central pool (best-effort)
        if not self._allocation_done:
            want = {self.account_quote: self._allocated_quote, self._base_for_symbol(self.symbol): self._allocated_base}
            got = await self.fund_mgr.allocate(want)
            self.broker.quote_bal = got.get(self.account_quote, d("0"))
            self.broker.base_bal  = got.get(self._base_for_symbol(self.symbol), d("0"))
            self._allocation_done = True

    async def on_stop(self) -> None:
        # publish final snapshot removal
        try:
            await self.hub.clear(self.cfg.name)
        except Exception:
            pass
        # cancel all and return funds
        n = self.broker.cancel_all()
        self.log.info("canceled %s open orders", n)
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

    def _split_symbol(self, sym: str) -> Tuple[str, str]:
        return self._base_for_symbol(sym), self._quote_for_symbol(sym)

    def _dp(self, q: Decimal) -> int:
        e = -q.as_tuple().exponent
        return max(0, int(e))

    def _fmt(self, x: Decimal, places: int) -> str:
        q = Decimal(1).scaleb(-places)
        return str(x.quantize(q))

    def _track_placement(self, o: PaperOrder, side: str) -> None:
        self._placed_at[o.orderId] = asyncio.get_running_loop().time()

    def _can_place_buy(self) -> bool:
        return self.broker.quote_bal >= self.broker.min_notional

    def _reprice_buy_from_spot(self, spot: Decimal, reason: str) -> None:
        _ = self.broker.cancel_side("BUY")

        if not self._can_place_buy():
            self.log.info("Skip BUY reprice (%s): insufficient quote", reason)
            return

        target_buy = spot * (Decimal("1") - self.buy_gap)
        budget = self.broker.quote_bal
        qty = self.broker._round_down(budget / target_buy, self.broker.step)

        try:
            o = self.broker.place_limit("BUY", target_buy, qty)
        except ValueError as e:
            self.log.info("Skip BUY reprice (%s): rounded qty=%s price=%s -> %s", reason, qty, target_buy, e)
            return

        self._track_placement(o, "BUY")
        # stage = number of BUY (re)placements
        reason_map = {"buy_fill": "buy_fill", "sell_fill": "sell_fill", "ttl": "ttl", "ensure": "ensure"}
        self.stage = getattr(self, "stage", 0) + 1
        self.retriggers = getattr(self, "retriggers", {"buy_fill": 0, "sell_fill": 0, "ttl": 0, "ensure": 0})
        self.retriggers[reason_map.get(reason, "ensure")] = self.retriggers.get(reason, 0) + 1
        self.log.info("BUY (re)placed #%s @%s qty=%s [reason=%s stage=%s]",
                      o.orderId, o.price, o.qty, reason, self.stage)

    async def _publish_snapshot(self, price: Optional[Decimal]) -> None:
        p_dp  = self._dp(self.broker.tick)
        q_dp  = self._dp(self.broker.step)

        if price is None:
            price_str = None
            pnl_str = None
            pnl_pct_str = None
        else:
            price_str = self._fmt(price, p_dp)
            equity = self.broker.base_bal * price + self.broker.quote_bal
            if self._initial_equity is None:
                self._initial_equity = equity
            pnl = equity - self._initial_equity
            pnl_pct = (pnl / self._initial_equity * Decimal("100")) if self._initial_equity > 0 else Decimal("0")
            pnl_str = self._fmt(pnl, 2)
            pnl_pct_str = self._fmt(pnl_pct, 4) + "%"

        order_lines = []
        for oid in sorted(self.broker.orders.keys()):
            o = self.broker.orders[oid]
            state = {"NEW":"placed","FILLED":"filled","CANCELED":"canceled"}.get(o.status, o.status.lower())
            px_str = self._fmt(o.price, p_dp)
            qty_str = self._fmt(o.qty, q_dp)
            order_lines.append(f"{o.side}: @ {px_str}  qty={qty_str}  state={state}")

        mins, secs = divmod(int(self.active_duration()), 60)
        snap = {
            "symbol": self.symbol,
            "price": price_str,
            "stage": getattr(self, "stage", 0),
            "quote_sym": self._quote_for_symbol(self.symbol),
            "quote_bal": self._fmt(self.broker.quote_bal, 2),
            "pnl": pnl_str if pnl_str is not None else "n/a",
            "pnl_pct": pnl_pct_str if pnl_pct_str is not None else "n/a",
            "active": f"{mins}m {secs}s",
            "orders": order_lines,
        }
        await self.hub.put(self.cfg.name, snap)

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

        filled_now = self.broker.mark_price(price)

        buy_filled = any(o.side == "BUY"  and o.orderId not in self._handled_fills for o in filled_now)
        sell_filled= any(o.side == "SELL" and o.orderId not in self._handled_fills for o in filled_now)

        for o in filled_now:
            if o.orderId in self._handled_fills:
                continue
            self._handled_fills.add(o.orderId)
            if o.side == "BUY":
                self.last_buy_price = o.price
            else:
                self.last_sell_price = None

        # SELL placement if we have base and no SELL open
        have_base = self.broker.base_bal >= self.broker.min_qty
        have_open_sell = bool(self.broker.open_by_side("SELL"))
        if have_base and not have_open_sell and self.last_buy_price is not None:
            floor = self.last_buy_price * (Decimal("1") + self.min_profit)
            target_sell = max(self.last_buy_price * (Decimal("1") + self.sell_gap), floor)
            qty = self.broker._round_down(self.broker.base_bal, self.broker.step)
            try:
                o = self.broker.place_limit("SELL", target_sell, qty)
                self._track_placement(o, "SELL")
                self.last_sell_price = o.price
                self.log.info("SELL placed #%s @%s qty=%s", o.orderId, target_sell, qty)
            except ValueError as e:
                self.log.info("Skip SELL: %s", e)

        # TTL cancellation & flag
        canceled_buy_ttl = False
        if self.order_ttl > 0:
            now = asyncio.get_running_loop().time()
            for o in [*self.broker.open_by_side("BUY"), *self.broker.open_by_side("SELL")]:
                placed = self._placed_at.get(o.orderId)
                if placed and (now - placed) >= self.order_ttl:
                    o.status = "CANCELED"
                    self._placed_at.pop(o.orderId, None)
                    if o.side == "BUY":
                        canceled_buy_ttl = True
                    self.log.info("%s #%s canceled (TTL %ss)", o.side, o.orderId, self.order_ttl)

        # reprice reason
        reprice_reason = None
        if buy_filled:
            reprice_reason = "buy_fill"
        elif sell_filled:
            reprice_reason = "sell_fill"
        elif canceled_buy_ttl:
            reprice_reason = "ttl"
        elif not self.broker.open_by_side("BUY"):
            reprice_reason = "ensure"

        if reprice_reason:
            self._reprice_buy_from_spot(price, reason=reprice_reason)

        await self._publish_snapshot(self._last_price)


# -------------------------- supervisor + manager -----------------
class BotSupervisor:
    """
    Wraps a bot, adds start/stop.
    """
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
    """
    Holds supervisors + metadata so Admin can respawn with same params later.
    """
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
    Admin API:
      GET  /bots                      -> list bots
      POST /bots                      -> add
      DELETE /bots/{name}             -> stop and return funds; optional ?respawn_after=<sec>
      GET  /funds                     -> current fund pool balances

    POST /bots parameters (clear names):
      symbol: "ETHUSD" | "BTCUSDT" ...
      venue: "us" | "com"
      buy_gap_pct: float (e.g., 0.2)
      sell_gap_pct: float
      min_profit_pct: float
      order_ttl_sec: int (time-to-live for unfilled orders; 0 = never auto-cancel)
      fee_rate: float (e.g., 0.0001)
      init_base: decimal string/number (requested base asset amount, e.g., "0")
      init_quote: decimal string/number (requested quote amount, e.g., 2000)
      bot_lifetime_sec: int (0 = no expiry)
      respawn_wait_sec: int (optional; auto-respawn with same params after cancel/expiry)
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
        self._watch_task: Optional[asyncio.Task] = None

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

        # renamed (with backwards-compat fallbacks)
        buy_gap = Decimal(str(payload.get("buy_gap_pct", 0.2)))
        sell_gap = Decimal(str(payload.get("sell_gap_pct", 0.2)))
        min_profit = Decimal(str(payload.get("min_profit_pct", payload.get("sell_gap_pct", 0.2))))
        order_ttl_sec = int(payload.get("order_ttl_sec", payload.get("order_ttl_seconds", 0)))
        fee = Decimal(str(payload.get("fee_rate", 0.0001)))
        init_base = Decimal(str(payload.get("init_base", payload.get("base", 0))))
        init_quote = Decimal(str(payload.get("init_quote", payload.get("quote", 2000))))
        bot_lifetime_sec = int(payload.get("bot_lifetime_sec", payload.get("expiry_sec", 0)))
        respawn_wait_sec = int(payload.get("respawn_wait_sec", payload.get("respawn_after_sec", 0)))

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
                "balances": {"base": str(init_base), "quote": str(init_quote)},
                "fee_rate": float(fee),
                "order_ttl_sec": order_ttl_sec,
                "bot_lifetime_sec": bot_lifetime_sec,
            },
        )
        await self.mgr.add_and_start(paper_name, lambda c: PaperTradingBot(c), paper_cfg)
        self.log.info("added %s (+ %s if new)", paper_name, ws_name)

        # remember respawn preference in meta (stored in cfg.params)
        if respawn_wait_sec:
            self.mgr._meta[paper_name][1].params["respawn_wait_sec"] = respawn_wait_sec

        return web.json_response({"ok": True, "added": [paper_name], "ws": ws_name})

    async def del_bot(self, request):
        name = request.match_info["name"]
        qs = request.rel_url.query
        respawn_after = int(qs.get("respawn_after", "0")) or 0

        meta = self.mgr.get_meta(name)
        ok = await self.mgr.remove_and_stop(name)

        if not ok:
            return web.json_response({"removed": False, "name": name}, status=404)

        # explicit manual respawn timer
        if respawn_after and meta:
            factory, old_cfg = meta
            new_cfg = BotConfig(
                name=old_cfg.name,
                tick_seconds=old_cfg.tick_seconds,
                max_backoff=old_cfg.max_backoff,
                jitter=old_cfg.jitter,
                params=dict(old_cfg.params),
            )
            async def _respawn():
                await asyncio.sleep(respawn_after)
                await self.mgr.add_and_start(new_cfg.name, factory, new_cfg)
                self.log.info("respawned %s after %ss", new_cfg.name, respawn_after)
            asyncio.create_task(_respawn())

        return web.json_response({"removed": True, "name": name, "respawn_scheduled": bool(respawn_after)})

    async def _global_reporter(self):
        log = logging.getLogger("report")
        while True:
            try:
                await asyncio.sleep(self.report_every)
                pools = await self.funds.balances()
                snaps = await self.hub.all()

                ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
                lines = [f"=== Global Report @ {ts} ===", "Available Fund:"]
                if pools:
                    for a, amt in sorted(pools.items()):
                        lines.append(f"  {a}: {amt}")
                else:
                    lines.append("  (none)")

                # Compute global PnL summary across bots if present (sum quoted in quote currency per bot)
                for name, s in sorted(snaps.items()):
                    lines.append(f"{name}: Stage {s.get('stage',0)}, "
                                 f"Current Fund {s.get('quote_bal','0')} {s.get('quote_sym','')}, "
                                 f"PnL: {s.get('pnl','$?')}, {s.get('pnl_pct','%?')}, "
                                 f"Active {s.get('active','0m 0s')}")
                    if s.get('symbol') and s.get('price'):
                        lines.append(f"  {s['symbol']}: Current Price: {s['price']}")
                    orders = s.get('orders', [])
                    if orders:
                        lines.extend([f"  {ol}" for ol in orders])
                    else:
                        lines.append("  orders: (none)")

                log.info("\n" + "\n".join(lines))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.getLogger("report").warning("reporter error: %s", e)

    async def _watch_expired_and_respawn(self):
        """
        Every 5s, look for paper-* bots that are no longer running but have
        respawn_wait_sec set in their saved params; respawn them once.
        """
        while True:
            try:
                await asyncio.sleep(5)
                running = set(self.mgr.list_names())
                for name, (factory, cfg) in list(self.mgr._meta.items()):
                    if not name.startswith("paper-"):
                        continue
                    if name in running:
                        continue
                    ra = int(cfg.params.get("respawn_wait_sec", 0)) or 0
                    if ra > 0:
                        # consume the flag to avoid repeated respawns
                        cfg.params["respawn_wait_sec"] = 0
                        async def _do():
                            await asyncio.sleep(ra)
                            await self.mgr.add_and_start(cfg.name, factory, cfg)
                            self.log.info("auto-respawned %s after %ss", cfg.name, ra)
                        asyncio.create_task(_do())
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.getLogger("respawn").warning("watch error: %s", e)

    async def start(self, host="127.0.0.1", port=8080):
        self._runner = web.AppRunner(self.app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, host=host, port=port)
        await self._site.start()
        self._report_task = asyncio.create_task(self._global_reporter())
        self._watch_task = asyncio.create_task(self._watch_expired_and_respawn())
        self.log.info("admin API listening on http://%s:%s", host=host, port=port)

    async def stop(self):
        for t in ("_report_task", "_watch_task"):
            task = getattr(self, t, None)
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        if self._site:
            await self._site.stop()
        if self._runner:
            await self._runner.cleanup()


# --------------------------- CLI & bootstrap ---------------------
def parse_args():
    ap = argparse.ArgumentParser(description="Paper bots with admin API + fund manager + global report")
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
    # seed initial pool (so new bots can allocate from it)
    funds.seed(args.seed_quote, d(args.seed_amount))

    mgr = BotManager()

    # Optional: start initial symbols (each gets its own WS + paper bot)
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
                "order_ttl_sec": 28800,
                "bot_lifetime_sec": 0,
            },
        )
        await mgr.add_and_start(ws.name, lambda c: WSPriceBot(c), ws)
        await mgr.add_and_start(paper.name, lambda c: PaperTradingBot(c), paper)

    # Start admin API
    admin = AdminHTTP(mgr, bus, funds, hub)
    await admin.start(host=args.admin_host, port=args.admin_port)

    try:
        await mgr.run()  # until signal
    finally:
        await admin.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
