#!/usr/bin/env python3
# bot_manager.py (updated Δ→last to use last price as denominator)
# See previous description; two-branch engine (CORE + MOMO), gap-only orders, repricing, compact report.

import os
import asyncio
import json
import time
import math
import signal
import uuid
import logging
from collections import deque, defaultdict
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any, Tuple

import aiohttp
from aiohttp import web
import websockets
import contextlib

# --------------------------- Config & Utilities ---------------------------

TZ = os.environ.get("TZ", "America/New_York")
REPORT_MODE = os.environ.get("REPORT_MODE", "compact")
REPORT_EVERY = int(os.environ.get("REPORT_EVERY", "60"))
REPORT_MAX_OPEN = int(os.environ.get("REPORT_MAX_OPEN", "3"))
REPORT_TICK_HISTORY = int(os.environ.get("REPORT_TICK_HISTORY", "5"))

ADMIN_HOST = os.environ.get("ADMIN_HOST", "0.0.0.0")
ADMIN_PORT = int(os.environ.get("ADMIN_PORT", "8080"))

SEED_QUOTE = os.environ.get("SEED_QUOTE", "USD")
SEED_AMOUNT = float(os.environ.get("SEED_AMOUNT", "20000"))

PRICE_HISTORY_MINUTES = 10  # keep 10-minute global history for start→end line

def fmt_money(v: float) -> str:
    try:
        return f"${v:,.2f}"
    except Exception:
        return f"${v}"

def fmt_qty(v: float, places: int = 6) -> str:
    s = f"{v:.{places}f}"
    s = s.rstrip('0').rstrip('.')
    return s if s else "0"

def fmt_delta(last: float, limit: float) -> str:
    # kept for other uses; not used for order Δ lines
    d = last - limit
    pct = (d / limit) * 100.0 if limit else 0.0
    sign = "+" if d >= 0 else ""
    return f"{sign}{d:.2f} ({sign}{pct:.2f}%)"

def fmt_gap_from_last(last: float, limit: float) -> str:
    # Display the price gap relative to the current last price (matches gap params)
    d = last - limit  # positive if last > limit
    pct = (d / last) * 100.0 if last else 0.0
    sign = "+" if d >= 0 else ""
    return f"{sign}{d:.2f} ({sign}{pct:.2f}%)"

def now_est_str() -> str:
    import datetime, pytz
    return datetime.datetime.now(pytz.timezone(TZ)).strftime("%Y-%m-%d %H:%M:%S %Z")

def clamp_price_below_last(target: float, last: float, tick: float) -> float:
    return min(target, last - tick)

# --------------------------- Fund Manager ---------------------------

class FundManager:
    def __init__(self, quote_symbol: str, seed_amount: float):
        self.quote_symbol = quote_symbol
        self.pool_quote = seed_amount
        self.balances: Dict[str, Dict[str, float]] = {}

    def allocate(self, bot_name: str, quote: float, base: float = 0.0):
        if quote > self.pool_quote:
            raise ValueError(f"Insufficient pool quote to allocate: need {quote}, have {self.pool_quote}")
        self.pool_quote -= quote
        self.balances[bot_name] = {"quote": quote, "base": base}

    def sweep_back(self, bot_name: str, quote: float, base: float):
        self.pool_quote += quote
        self.balances.pop(bot_name, None)

    def get_bot_balances(self, bot_name: str) -> Tuple[float, float]:
        bal = self.balances.get(bot_name, {"quote": 0.0, "base": 0.0})
        return bal["quote"], bal["base"]

    def set_bot_balances(self, bot_name: str, quote: float, base: float):
        if bot_name not in self.balances:
            self.balances[bot_name] = {"quote": 0.0, "base": 0.0}
        self.balances[bot_name]["quote"] = quote
        self.balances[bot_name]["base"] = base

    def funds_summary(self) -> Dict[str, Any]:
        return {"pool_quote": self.pool_quote, "quote_symbol": self.quote_symbol, "bots": self.balances}

# --------------------------- Price Feed ---------------------------

class PriceFeed:
    def __init__(self):
        self.last_price: Dict[str, float] = {}
        self.listeners: Dict[str, List] = defaultdict(list)
        self.ws_tasks: Dict[Tuple[str, str], asyncio.Task] = {}
        self._stop = False

    def get_last(self, symbol: str) -> Optional[float]:
        return self.last_price.get(symbol)

    def subscribe(self, venue: str, symbol: str, on_price):
        key = (venue, symbol.upper())
        self.listeners[symbol.upper()].append(on_price)
        if key not in self.ws_tasks:
            self.ws_tasks[key] = asyncio.create_task(self._run_ws(venue, symbol.upper()))

    async def _run_ws(self, venue: str, symbol: str):
        base_url = "wss://stream.binance.us:9443/ws" if venue.lower() == "us" else "wss://stream.binance.com:9443/ws"
        url = f"{base_url}/{symbol.lower()}@trade"
        backoff = 1.0
        while not self._stop:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    backoff = 1.0
                    while not self._stop:
                        data = json.loads(await ws.recv())
                        price = None
                        if isinstance(data, dict):
                            if "p" in data:
                                try: price = float(data["p"])
                                except: price = None
                            elif "data" in data and isinstance(data["data"], dict) and "p" in data["data"]:
                                try: price = float(data["data"]["p"])
                                except: price = None
                        if price and price > 0:
                            self.last_price[symbol.upper()] = price
                            for cb in list(self.listeners.get(symbol.upper(), [])):
                                try: cb(price)
                                except Exception as e: logging.exception("Listener error: %s", e)
            except Exception as e:
                logging.warning("WS connection error for %s on %s: %s", symbol, venue, e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def stop(self):
        self._stop = True
        for task in self.ws_tasks.values(): task.cancel()
        await asyncio.gather(*self.ws_tasks.values(), return_exceptions=True)

# --------------------------- Trading Structures ---------------------------

from dataclasses import dataclass, field

@dataclass
class Order:
    id: str
    side: str
    price: float
    qty: float
    branch: str
    is_momentum: bool = False
    spent_quote: Optional[float] = None
    created_ts: float = field(default_factory=lambda: time.time())

@dataclass
class BranchState:
    name: str
    open_buy: Optional[Order] = None
    sell_orders: List[Order] = field(default_factory=list)
    base_avail: float = 0.0
    last_buy_fill_ts: Optional[float] = None
    last_sell_fill_ts: Optional[float] = None
    stage_count: int = 0
    active: bool = False
    reprices_count: int = 0
    avg_cost: float = 0.0

class Bot:
    def __init__(self, manager, name: str, symbol: str, venue: str, params: Dict[str, Any], price_feed: PriceFeed):
        self.manager = manager
        self.name = name
        self.symbol = symbol.upper()
        self.venue = venue
        self.params = params
        self.price_feed = price_feed

        q, b = manager.fund.get_bot_balances(self.name)
        self.quote = q
        self.base = b

        self.core = BranchState(name="CORE")
        self.momo = BranchState(name="MOMO")
        self.momo_ref: Optional[float] = None

        self.tick_prices: deque = deque(maxlen=REPORT_TICK_HISTORY)
        self.minute_prices: deque = deque(maxlen=10)

        self.nav_baseline: Optional[float] = None

        self._stop = False
        self._task: Optional[asyncio.Task] = None

        self.price_feed.subscribe(self.venue, self.symbol, self._on_price)

    def start(self):
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    async def stop(self):
        self._stop = True
        if self._task:
            self._task.cancel()
            with contextlib.suppress(Exception): await self._task
        last = self.price_feed.get_last(self.symbol) or 0.0
        sweep_quote = self.quote
        total_base = self.core.base_avail + self.momo.base_avail
        if last > 0 and total_base > 0:
            sweep_quote += total_base * last
        self.core = BranchState(name="CORE")
        self.momo = BranchState(name="MOMO")
        self.manager.fund.sweep_back(self.name, sweep_quote, 0.0)

    def _on_price(self, price: float):
        self.tick_prices.append(price)

    async def _run(self):
        min_tick = self.params.get("price_tick", 0.01)
        qty_tick = self.params.get("qty_tick", 1e-6)
        buy_gap_pct = float(self.params.get("buy_gap_pct", 0.2))
        sell_gap_pct = float(self.params.get("sell_gap_pct", 0.4))
        min_profit_pct = float(self.params.get("min_profit_pct", 0.2))
        order_expiry_seconds = int(self.params.get("order_expiry_seconds", 0))
        fee_rate = float(self.params.get("fee_rate", 0.0))

        buy_tranche_quote = float(self.params.get("buy_tranche_quote", 0.0))
        buy_tranche_pct = float(self.params.get("buy_tranche_pct", 0.5))

        momentum_enabled = bool(self.params.get("momentum_enabled", True))
        momentum_breakout_pct = float(self.params.get("momentum_breakout_pct", 0.5))

        bot_lifetime_seconds = int(self.params.get("bot_lifetime_seconds", 0))
        start_ts = time.time()

        while not self._stop:
            last = self.price_feed.get_last(self.symbol)
            if last and last > 0:
                if self.momo_ref is None: self.momo_ref = last
                if self.nav_baseline is None:
                    self.nav_baseline = self.quote + (self.core.base_avail + self.momo.base_avail) * last
                break
            await asyncio.sleep(0.1)

        while not self._stop:
            last = self.price_feed.get_last(self.symbol)
            now = time.time()
            if not last or last <= 0:
                await asyncio.sleep(0.1)
                continue

            if momentum_enabled and self.momo.open_buy is None:
                self.momo_ref = max(self.momo_ref or last, last)

            if self.core.open_buy is None:
                self._maybe_place_core_buy(last, buy_gap_pct, fee_rate, buy_tranche_quote, buy_tranche_pct, min_tick, qty_tick)
            else:
                self._reprice_buy(self.core, last, buy_gap_pct, min_tick, reset_ttl=False)
            self._maybe_place_core_sells(last, sell_gap_pct, min_profit_pct, fee_rate, min_tick, qty_tick)

            if momentum_enabled:
                if self.momo.open_buy is None:
                    if self._breakout_triggered(last, self.momo_ref, momentum_breakout_pct):
                        self._maybe_place_momo_buy(last, buy_gap_pct, fee_rate, buy_tranche_quote, buy_tranche_pct, min_tick, qty_tick)
                else:
                    self._reprice_buy(self.momo, last, buy_gap_pct, min_tick, reset_ttl=False)
                self._maybe_place_momo_sells(last, sell_gap_pct, min_profit_pct, fee_rate, min_tick, qty_tick)

            self._simulate_fills(self.core, last, fee_rate, min_tick, qty_tick, order_expiry_seconds)
            self._simulate_fills(self.momo, last, fee_rate, min_tick, qty_tick, order_expiry_seconds)

            self._maybe_close_momo_branch(last)

            if bot_lifetime_seconds > 0 and (now - start_ts) >= bot_lifetime_seconds:
                await self.stop()
                return

            await asyncio.sleep(0.05)

    def _free_quote(self) -> float:
        return max(0.0, self.quote)

    def _budget_for_buy(self, buy_tranche_quote: float, buy_tranche_pct: float) -> float:
        if buy_tranche_quote > 0:
            return min(buy_tranche_quote, self._free_quote())
        return min(self._free_quote(), max(0.0, self._free_quote() * buy_tranche_pct))

    def _qty_for_budget(self, price: float, budget: float, fee_rate: float, qty_tick: float) -> float:
        if price <= 0 or budget <= 0: return 0.0
        qty = budget / (price * (1.0 + fee_rate))
        steps = math.floor(qty / qty_tick)
        return max(0.0, steps * qty_tick)

    def _place_buy(self, branch: 'BranchState', last: float, buy_gap_pct: float, fee_rate: float,
                   buy_tranche_quote: float, buy_tranche_pct: float, min_tick: float, qty_tick: float,
                   is_momentum: bool) -> Optional['Order']:
        budget = self._budget_for_buy(buy_tranche_quote, buy_tranche_pct)
        if budget <= 0: return None
        limit = clamp_price_below_last(last * (1.0 - buy_gap_pct / 100.0), last, min_tick)
        qty = self._qty_for_budget(limit, budget, fee_rate, qty_tick)
        if qty <= 0: return None
        spent = limit * qty * (1.0 + fee_rate)
        if spent > self._free_quote() + 1e-9: return None
        order = Order(id=str(uuid.uuid4()), side="BUY", price=limit, qty=qty,
                      branch=branch.name, is_momentum=is_momentum, spent_quote=round(spent, 2))
        branch.open_buy = order
        branch.active = True
        return order

    def _maybe_place_core_buy(self, last, buy_gap_pct, fee_rate, buy_tranche_quote, buy_tranche_pct, min_tick, qty_tick):
        self._place_buy(self.core, last, buy_gap_pct, fee_rate, buy_tranche_quote, buy_tranche_pct, min_tick, qty_tick, is_momentum=False)

    def _maybe_place_momo_buy(self, last, buy_gap_pct, fee_rate, buy_tranche_quote, buy_tranche_pct, min_tick, qty_tick):
        self._place_buy(self.momo, last, buy_gap_pct, fee_rate, buy_tranche_quote, buy_tranche_pct, min_tick, qty_tick, is_momentum=True)

    def _reprice_buy(self, branch: 'BranchState', last: float, buy_gap_pct: float, min_tick: float, reset_ttl: bool = False):
        if branch.open_buy is None: return
        target = clamp_price_below_last(last * (1.0 - buy_gap_pct / 100.0), last, min_tick)
        if abs(branch.open_buy.price - target) >= min_tick:
            branch.open_buy.price = round(target, 8)
            branch.reprices_count += 1
            if reset_ttl: branch.open_buy.created_ts = time.time()

    def _place_sell_from_inventory(self, branch: 'BranchState', last: float, sell_gap_pct: float,
                                   min_profit_pct: float, fee_rate: float, min_tick: float, qty_tick: float) -> Optional['Order']:
        qty = math.floor(branch.base_avail / qty_tick) * qty_tick
        if qty <= 0: return None
        gap_target = last * (1.0 + sell_gap_pct / 100.0)
        profit_target = branch.avg_cost * (1.0 + min_profit_pct / 100.0) if branch.avg_cost > 0 else gap_target
        limit = max(gap_target, profit_target)
        limit = max(min_tick, round(limit, 8))
        order = Order(id=str(uuid.uuid4()), side="SELL", price=limit, qty=qty, branch=branch.name)
        branch.sell_orders.append(order)
        branch.active = True
        return order

    def _maybe_place_core_sells(self, last, sell_gap_pct, min_profit_pct, fee_rate, min_tick, qty_tick):
        if self.core.base_avail > 0 and len(self.core.sell_orders) < 3:
            self._place_sell_from_inventory(self.core, last, sell_gap_pct, min_profit_pct, fee_rate, min_tick, qty_tick)

    def _maybe_place_momo_sells(self, last, sell_gap_pct, min_profit_pct, fee_rate, min_tick, qty_tick):
        if self.momo.base_avail > 0 and len(self.momo.sell_orders) < 3:
            self._place_sell_from_inventory(self.momo, last, sell_gap_pct, min_profit_pct, fee_rate, min_tick, qty_tick)

    def _breakout_triggered(self, last: float, ref: Optional[float], breakout_pct: float) -> bool:
        if ref is None or ref <= 0 or breakout_pct <= 0: return False
        return last >= ref * (1.0 + breakout_pct / 100.0)

    def _simulate_fills(self, branch: 'BranchState', last: float, fee_rate: float, min_tick: float, qty_tick: float, ttl: int):
        now = time.time()
        if branch.open_buy is not None:
            o = branch.open_buy
            expired = (ttl > 0 and now - o.created_ts >= ttl)
            if expired:
                branch.open_buy = None
            else:
                if last <= o.price + 1e-12:
                    spent = o.price * o.qty * (1.0 + fee_rate)
                    self.quote -= spent
                    branch.base_avail += o.qty
                    if branch.avg_cost <= 0:
                        branch.avg_cost = o.price
                    else:
                        total_qty = branch.base_avail
                        if total_qty > 0:
                            branch.avg_cost = ((branch.avg_cost * (total_qty - o.qty)) + (o.price * o.qty)) / total_qty
                    branch.last_buy_fill_ts = now
                    branch.open_buy = None

        filled_ids = []
        for s in branch.sell_orders:
            expired = (ttl > 0 and now - s.created_ts >= ttl)
            if expired:
                filled_ids.append(s.id)
                continue
            if last >= s.price - 1e-12:
                qty = s.qty
                gross = s.price * qty
                net = gross * (1.0 - fee_rate)
                self.quote += net
                branch.base_avail = max(0.0, branch.base_avail - qty)
                branch.last_sell_fill_ts = now
                filled_ids.append(s.id)
        if filled_ids:
            branch.sell_orders = [s for s in branch.sell_orders if s.id not in filled_ids]

    def _maybe_close_momo_branch(self, last: float, dust_threshold: float = 1e-12):
        has_open_buy = self.momo.open_buy is not None
        has_sell = len(self.momo.sell_orders) > 0
        has_inv = self.momo.base_avail > dust_threshold
        if not has_open_buy and not has_sell and not has_inv:
            if self.momo.active: self.momo.stage_count += 1
            self.momo.active = False
            self.momo_ref = last

    # ------------------- Reporting -------------------

    def snapshot(self) -> Dict[str, Any]:
        last = self.price_feed.get_last(self.symbol) or 0.0
        if len(self.minute_prices) == 0 or (time.time() - (self.minute_prices[-1][0] if self.minute_prices else 0)) >= 60 - 1e-6:
            self.minute_prices.append((time.time(), float(last)))

        nav = self.quote + (self.core.base_avail + self.momo.base_avail) * (last if last > 0 else 0.0)
        pnl_abs = (nav - self.nav_baseline) if self.nav_baseline is not None else 0.0
        pnl_pct = (pnl_abs / self.nav_baseline * 100.0) if (self.nav_baseline and self.nav_baseline != 0) else 0.0

        momo_buy_count = 1 if (self.momo.open_buy is not None) else 0
        open_orders_total = (1 if self.core.open_buy else 0) + len(self.core.sell_orders) + \
                            (1 if self.momo.open_buy else 0) + len(self.momo.sell_orders)

        recent_orders: List[str] = []
        def order_line(o: Order) -> str:
            last_p = last if last > 0 else o.price
            tag = " [M]" if (o.is_momentum and o.side == "BUY") else ""
            tag = " [M]" if (o.is_momentum and o.side == "BUY") else ""
            spent_txt = f", spent≈{fmt_money(o.spent_quote)}" if (o.side == "BUY" and o.spent_quote) else ""
            return f"{o.side}{tag} @ {fmt_money(o.price)} qty={fmt_qty(o.qty)}{spent_txt} Δ→last={fmt_gap_from_last(last_p, o.price)}"
        candidates: List[Tuple[float, Order]] = []
        if self.core.open_buy: candidates.append((self.core.open_buy.created_ts, self.core.open_buy))
        if self.momo.open_buy: candidates.append((self.momo.open_buy.created_ts, self.momo.open_buy))
        for s in self.core.sell_orders: candidates.append((s.created_ts, s))
        for s in self.momo.sell_orders: candidates.append((s.created_ts, s))
        candidates.sort(key=lambda x: x[0], reverse=True)
        for _, o in candidates[:REPORT_MAX_OPEN]:
            recent_orders.append(order_line(o))
        # tick trail
        tick_trail = list(self.tick_prices)
        tick_entries = []
        for i, p in enumerate(tick_trail):
            if i == 0:
                tick_entries.append(f"{fmt_qty(p, 2)}")
            else:
                prev = tick_trail[i - 1]
                dpct = ((p - prev) / prev * 100.0) if prev else 0.0
                sign = "+" if dpct >= 0 else ""
                tick_entries.append(f"{fmt_qty(p, 2)} ({sign}{dpct:.2f}%)")

        # 10-minute start→end (print only start and end)
        start_end = "n/a"
        if len(self.minute_prices) >= 2:
            p0 = self.minute_prices[0][1]
            p1 = self.minute_prices[-1][1]
            d = p1 - p0
            pct = (d / p0 * 100.0) if p0 else 0.0
            sign = "+" if d >= 0 else ""
            start_end = f"start={fmt_money(p0)} → end={fmt_money(p1)}  Δ={sign}{d:.2f} ({sign}{pct:.2f}%)"

        return {
            "symbol": self.symbol,
            "last": last,
            "quote": self.quote,
            "core_base": self.core.base_avail,
            "momo_base": self.momo.base_avail,
            "pnl_abs": pnl_abs,
            "pnl_pct": pnl_pct,
            "core_stage": self.core.stage_count,
            "momo_stage": self.momo.stage_count,
            "open_orders_total": open_orders_total,
            "momo_open_buy_count": momo_buy_count,
            "last_buy_fill_ts": max([t for t in [self.core.last_buy_fill_ts, self.momo.last_buy_fill_ts] if t] + [0.0]) if (self.core.last_buy_fill_ts or self.momo.last_buy_fill_ts) else None,
            "last_sell_fill_ts": max([t for t in [self.core.last_sell_fill_ts, self.momo.last_sell_fill_ts] if t] + [0.0]) if (self.core.last_sell_fill_ts or self.momo.last_sell_fill_ts) else None,
            "recent_orders": recent_orders,
            "tick_entries": tick_entries,
            "ten_minute": start_end,
        }

    def render_compact(self) -> str:
        ss = self.snapshot()
        lines = []
        lines.append("=" * 60)
        lines.append(f"{now_est_str()}  |  {ss['symbol']}")
        lines.append("")
        lines.append(f"Price(10m): {ss['ten_minute']}")
        lines.append("")
        lines.append(f"Stage: Core={ss['core_stage']}  Momo={ss['momo_stage']}   |   Uptime: (since-start)")
        lines.append(f"Funds: {fmt_money(ss['quote'])}  /  Core:{fmt_qty(ss['core_base'])} {self.symbol}  Momo:{fmt_qty(ss['momo_base'])} {self.symbol}")
        lines.append(f"PnL: {fmt_money(ss['pnl_abs'])}  ({ss['pnl_pct']:.2f}%)")
        lines.append("")
        core_lines = []
        if self.core.open_buy:
            o = self.core.open_buy
            tag = " [M]" if (o.is_momentum and o.side == "BUY") else ""
            core_lines.append(f"  BUY{tag} @ {fmt_money(o.price)} qty={fmt_qty(o.qty)}{', spent≈'+fmt_money(o.spent_quote) if o.spent_quote else ''} Δ→last={fmt_gap_from_last(ss['last'], o.price)}")
        for s in self.core.sell_orders:
            core_lines.append(f"  SELL @ {fmt_money(s.price)} qty={fmt_qty(s.qty)} Δ→last={fmt_gap_from_last(ss['last'], s.price)}")

        momo_lines = []
        if self.momo.open_buy:
            o = self.momo.open_buy
            tag = " [M]" if (o.is_momentum and o.side == "BUY") else ""
            momo_lines.append(f"  BUY{tag} @ {fmt_money(o.price)} qty={fmt_qty(o.qty)}{', spent≈'+fmt_money(o.spent_quote) if o.spent_quote else ''} Δ→last={fmt_gap_from_last(ss['last'], o.price)}")
        for s in self.momo.sell_orders:
            momo_lines.append(f"  SELL [M] @ {fmt_money(s.price)} qty={fmt_qty(s.qty)} Δ→last={fmt_gap_from_last(ss['last'], s.price)}")

        total_open = len(core_lines) + len(momo_lines)
        lines.append(f"Open Orders ({total_open} total, {1 if self.momo.open_buy else 0} momentum BUY):")
        if core_lines:
            lines.append("  Core:")
            lines.extend(core_lines)
        if momo_lines:
            lines.append("  Momentum:")
            lines.extend(momo_lines)

        lb = max([t for t in [self.core.last_buy_fill_ts, self.momo.last_buy_fill_ts] if t] + [0.0]) if (self.core.last_buy_fill_ts or self.momo.last_buy_fill_ts) else 0.0
        ls = max([t for t in [self.core.last_sell_fill_ts, self.momo.last_sell_fill_ts] if t] + [0.0]) if (self.core.last_sell_fill_ts or self.momo.last_sell_fill_ts) else 0.0
        if lb or ls:
            lines.append("")
            lines.append("Last fills:")
            if lb: lines.append(f"  BUY  {time.strftime('%H:%M:%S', time.localtime(lb))}")
            if ls: lines.append(f"  SELL {time.strftime('%H:%M:%S', time.localtime(ls))}")

        if ss["tick_entries"]:
            lines.append("")
            lines.append("------------------------------------------------------------")
            lines.append("Price trail (last ticks, Δ%):")
            lines.append("  " + " | ".join(ss["tick_entries"]))

        return "\n".join(lines)

# --------------------------- Manager / API ---------------------------

class Manager:
    def __init__(self):
        self.fund = FundManager(SEED_QUOTE, SEED_AMOUNT)
        self.price_feed = PriceFeed()
        self.bots: Dict[str, Bot] = {}
        self._report_task: Optional[asyncio.Task] = None

    async def start(self):
        if REPORT_MODE == "compact":
            self._report_task = asyncio.create_task(self._report_loop())

    async def stop(self):
        if self._report_task:
            self._report_task.cancel()
            with contextlib.suppress(Exception): await self._report_task
        for b in list(self.bots.values()):
            with contextlib.suppress(Exception): await b.stop()
        await self.price_feed.stop()

    async def _report_loop(self):
        while True:
            await asyncio.sleep(REPORT_EVERY)
            print(self.render_report())

    def render_report(self) -> str:
        parts = []
        parts.append("=" * 60)
        parts.append(f"{now_est_str()}  |  POOL SUMMARY")
        funds = self.fund.funds_summary()
        allocated = sum(v["quote"] for v in funds["bots"].values())
        parts.append(f"Pool Funds: {fmt_money(funds['pool_quote'])}   (seed {fmt_money(SEED_AMOUNT)} → allocated to bots: {fmt_money(allocated)})")
        parts.append(f"Active Bots: {len(self.bots)}  ({', '.join(self.bots.keys())})")
        for name, bot in self.bots.items():
            parts.append("")
            parts.append(bot.render_compact())
        return "\n".join(parts)

    async def handle_get_funds(self, request: web.Request):
        return web.json_response(self.fund.funds_summary())

    async def handle_get_bots(self, request: web.Request):
        out = []
        for name, bot in self.bots.items():
            ss = bot.snapshot()
            out.append({
                "name": name,
                "symbol": bot.symbol,
                "venue": bot.venue,
                "quote": bot.quote,
                "core_base": bot.core.base_avail,
                "momo_base": bot.momo.base_avail,
                "pnl_abs": ss["pnl_abs"],
                "pnl_pct": ss["pnl_pct"],
                "core_stage": ss["core_stage"],
                "momo_stage": ss["momo_stage"],
            })
        return web.json_response(out)

    async def handle_post_bots(self, request: web.Request):
        data = await request.json()
        symbol = data.get("symbol", "").upper()
        venue = data.get("venue", "us")
        if not symbol:
            return web.json_response({"error": "symbol required"}, status=400)

        params = {
            "buy_gap_pct": float(data.get("buy_gap_pct", 0.4)),
            "sell_gap_pct": float(data.get("sell_gap_pct", 0.5)),
            "min_profit_pct": float(data.get("min_profit_pct", 0.25)),
            "order_expiry_seconds": int(data.get("order_expiry_seconds", 0)),
            "fee_rate": float(data.get("fee_rate", 0.001)),
            "starting_base_qty": float(data.get("starting_base_qty", 0.0)),
            "starting_quote_qty": float(data.get("starting_quote_qty", 0.0)),
            "buy_tranche_quote": float(data.get("buy_tranche_quote", 0.0)),
            "buy_tranche_pct": float(data.get("buy_tranche_pct", 0.5)),
            "momentum_enabled": bool(data.get("momentum_enabled", True)),
            "momentum_breakout_pct": float(data.get("momentum_breakout_pct", 0.5)),
            "bot_lifetime_seconds": int(data.get("bot_lifetime_seconds", 0)),
            "price_tick": float(data.get("price_tick", 0.01)),
            "qty_tick": float(data.get("qty_tick", 1e-6)),
        }

        bot_name = f"{symbol}-{str(uuid.uuid4())[:8]}"
        try:
            self.fund.allocate(bot_name, params["starting_quote_qty"], params["starting_base_qty"])
        except Exception as e:
            return web.json_response({"error": str(e)}, status=400)

        bot = Bot(self, bot_name, symbol, venue, params, self.price_feed)
        self.bots[bot_name] = bot
        bot.start()
        return web.json_response({"name": bot_name, "symbol": symbol, "venue": venue, "params": params})

    async def handle_delete_bot(self, request: web.Request):
        name = request.match_info.get("name")
        if name not in self.bots:
            return web.json_response({"error": "not found"}, status=404)

        respawn_after = request.rel_url.query.get("respawn_after")
        params = self.bots[name].params if respawn_after else None
        symbol = self.bots[name].symbol if respawn_after else None
        venue  = self.bots[name].venue if respawn_after else None

        await self.bots[name].stop()
        del self.bots[name]

        if respawn_after:
            try: delay = int(respawn_after)
            except: delay = 0
            asyncio.create_task(self._respawn_later(symbol, venue, params, delay))
            return web.json_response({"status": "deleted", "respawn_scheduled_in_seconds": delay})
        else:
            return web.json_response({"status": "deleted"})

    async def _respawn_later(self, symbol: str, venue: str, params: Dict[str, Any], delay: int):
        await asyncio.sleep(max(0, delay))
        bot_name = f"{symbol}-{str(uuid.uuid4())[:8]}"
        try:
            self.fund.allocate(bot_name, params["starting_quote_qty"], params["starting_base_qty"])
        except Exception as e:
            logging.error("Respawn allocation failed: %s", e)
            return
        bot = Bot(self, bot_name, symbol, venue, params, self.price_feed)
        self.bots[bot_name] = bot
        bot.start()

# --------------------------- Server ---------------------------

async def init_app(manager: Manager) -> web.Application:
    app = web.Application()
    app["manager"] = manager
    app.add_routes([
        web.get("/funds", lambda req: manager.handle_get_funds(req)),
        web.get("/bots", lambda req: manager.handle_get_bots(req)),
        web.post("/bots", lambda req: manager.handle_post_bots(req)),
        web.delete("/bots/{name}", lambda req: manager.handle_delete_bot(req)),
    ])
    return app

async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    manager = Manager()
    await manager.start()
    app = await init_app(manager)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, ADMIN_HOST, ADMIN_PORT)
    await site.start()
    logging.info("Admin API listening on http://%s:%s", ADMIN_HOST, ADMIN_PORT)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _signal():
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _signal)

    await stop_event.wait()
    logging.info("Shutting down...")
    await manager.stop()
    await runner.cleanup()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass