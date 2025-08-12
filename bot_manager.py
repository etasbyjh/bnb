# bot_manager.py
# Python 3.10+
# WS price feed + paper-trading bot under an async manager.
# Report every minute:
#   - stage + retrigger breakdown
#   - price, USD balance, BTC balance, PnL
#   - one line per order: side, target price, amount, state (placed/filled/canceled)

from __future__ import annotations

import asyncio
import json
import logging
import random
import signal
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, Optional, Callable, List, Tuple

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

# ---------------------------- base bot ---------------------------
class BaseBot:
    def __init__(self, cfg: BotConfig):
        self.cfg = cfg
        self.log = logging.getLogger(f"bot.{cfg.name}")
        self._stop = asyncio.Event()

    async def on_start(self) -> None: ...
    async def on_stop(self) -> None: ...
    async def on_tick(self) -> None: raise NotImplementedError

    async def start(self) -> None:
        self.log.info("starting")
        await self.on_start()

    async def stop(self) -> None:
        self.log.info("stopping")
        self._stop.set()
        await self.on_stop()

    async def run(self) -> None:
        await self.start()
        try:
            while not self._stop.is_set():
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
                                # Optional heartbeat to verify feed:
                                # self.log.info("tick %s %s", symbol, price)
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
def d(x) -> Decimal: return Decimal(str(x))

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

# ----------------------- Paper trading bot ----------------------
class PaperTradingBot(BaseBot):
    """
    Paper bot that:
      - Keeps one BUY at spot*(1 - buy_gap) if funds allow
      - Keeps one SELL at last_buy*(1 + sell_gap), floor = last_buy*(1 + min_profit)
      - TTL cancels & reprices after N seconds
      - Event rules:
          * BUY filled  -> keep SELL (if any), cancel & reprice BUY from current price
          * SELL filled -> cancel & reprice BUY from current price
      - REPORT every report_interval_sec: stage + retriggers, price, balances, PnL, per-order lines.
    """
    def __init__(self, cfg: BotConfig):
        super().__init__(cfg)
        p = cfg.params
        self.bus: PriceBus = p["bus"]
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
        self.order_ttl = int(p.get("order_ttl_seconds", 0))  # 0 = no TTL
        self.report_every = int(p.get("report_interval_sec", 60))

        self.last_buy_price: Optional[Decimal] = None
        self.last_sell_price: Optional[Decimal] = None
        self._placed_at: Dict[int, float] = {}
        self._handled_fills: set[int] = set()

        # Reporting / PnL
        try:
            self._report_t0 = asyncio.get_running_loop().time()
        except RuntimeError:
            self._report_t0 = 0.0
        self._initial_equity: Optional[Decimal] = None
        self._last_price: Optional[Decimal] = None

        # Retrigger tracking
        self.stage: int = 0
        self.retriggers: Dict[str, int] = {"buy_fill": 0, "sell_fill": 0, "ttl": 0, "ensure": 0}

        # derive base/quote symbols for display
        self.base_sym, self.quote_sym = self._split_symbol(self.symbol)

    # ----------- helpers -----------
    def _split_symbol(self, sym: str) -> Tuple[str, str]:
        if sym.endswith("USDT"): return sym[:-4], "USDT"
        if sym.endswith("USD"):  return sym[:-3], "USD"
        return sym[:-3], sym[-3:]

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
        """Cancel any open BUY and place a fresh BUY at spot * (1 - buy_gap).
           Only increments stage/counters if a new BUY is actually placed."""
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
            # e.g., below min_notional/min_qty after rounding
            self.log.info("Skip BUY reprice (%s): %s", reason, e)
            return
    
        # success → track + bump stage & reason count
        self._track_placement(o, "BUY")
        self.stage += 1
        self.retriggers[reason] = self.retriggers.get(reason, 0) + 1
        self.log.info(
            "BUY (re)placed #%s @%s qty=%s [reason=%s stage=%s]",
            o.orderId, o.price, o.qty, reason, self.stage
        )


    def _maybe_init_equity(self, price: Decimal) -> None:
        if self._initial_equity is None:
            self._initial_equity = self.broker.base_bal * price + self.broker.quote_bal

    def _report_if_due(self, price: Optional[Decimal]) -> None:
        loop = asyncio.get_running_loop()
        now = loop.time()
        if self._report_t0 == 0.0:
            self._report_t0 = now

        if now - self._report_t0 >= self.report_every:
            p_dp  = self._dp(self.broker.tick)
            q_dp  = self._dp(self.broker.step)

            if price is None:
                price_str = "n/a"
                equity = self.broker.quote_bal
                pnl_str = "n/a"
                pnl_pct_str = "n/a"
            else:
                price_str = self._fmt(price, p_dp)
                equity = self.broker.base_bal * price + self.broker.quote_bal
                if self._initial_equity is None:
                    self._initial_equity = equity
                pnl = equity - self._initial_equity
                pnl_pct = (pnl / self._initial_equity * Decimal("100")) if self._initial_equity > 0 else Decimal("0")
                pnl_str = self._fmt(pnl, 2)
                pnl_pct_str = self._fmt(pnl_pct, 4) + "%"

            # stage / retrigger summary
            r = self.retriggers
            stage_str = f"stage={self.stage} (buy_fill={r.get('buy_fill',0)}, sell_fill={r.get('sell_fill',0)}, ttl={r.get('ttl',0)}, ensure={r.get('ensure',0)})"

            lines = []
            lines.append(
                f"REPORT | {stage_str} | price={price_str} | "
                f"{self.quote_sym}={self._fmt(self.broker.quote_bal, 2)} | "
                f"{self.base_sym}={self._fmt(self.broker.base_bal, q_dp)} | "
                f"PnL={pnl_str} ({pnl_pct_str})"
            )
            lines.append("orders:")
            for oid in sorted(self.broker.orders.keys()):
                o = self.broker.orders[oid]
                state = {"NEW":"placed","FILLED":"filled","CANCELED":"canceled"}.get(o.status, o.status.lower())
                px_str = self._fmt(o.price, p_dp)
                qty_str = self._fmt(o.qty, q_dp)
                lines.append(f"  {o.side:<4} @ {px_str}  qty={qty_str}  state={state}")

            self.log.info("\n" + "\n".join(lines))
            self._report_t0 = now

    # --------------- main tick ---------------
    async def on_tick(self) -> None:
        price = self.bus.get_last(self.symbol)
        if price is None:
            try:
                price = await self.bus.wait_for_price(self.symbol, timeout=5)
            except TimeoutError:
                # still report on schedule even without price
                self._report_if_due(None)
                return

        self._last_price = price
        self._maybe_init_equity(price)

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

        # Event-driven BUY reprices (counted as retriggers)
        if buy_filled:
            self._reprice_buy_from_spot(price, reason="buy_fill")
        if sell_filled:
            self._reprice_buy_from_spot(price, reason="sell_fill")

        # TTL: cancel any open orders older than N seconds (if BUY canceled, reprice immediately with reason=ttl)
        if self.order_ttl > 0:
            now = asyncio.get_running_loop().time()
            canceled_buy = 0
            for o in [*self.broker.open_by_side("BUY"), *self.broker.open_by_side("SELL")]:
                placed = self._placed_at.get(o.orderId)
                if placed and (now - placed) >= self.order_ttl:
                    o.status = "CANCELED"
                    self._placed_at.pop(o.orderId, None)
                    if o.side == "BUY":
                        canceled_buy += 1
                    self.log.info("%s #%s canceled (TTL %ss)", o.side, o.orderId, self.order_ttl)
            if canceled_buy > 0:
                self._reprice_buy_from_spot(price, reason="ttl")

        # Ensure at least one BUY exists (counted under 'ensure' if we had to place one)
        if not self.broker.open_by_side("BUY"):
            self._reprice_buy_from_spot(price, reason="ensure")

        # Minute report (works even if price briefly missing)
        self._report_if_due(self._last_price)

# -------------------------- supervisor ---------------------------
class BotSupervisor:
    def __init__(self, factory: Callable[[BotConfig], BaseBot], cfg: BotConfig):
        self.factory = factory
        self.cfg = cfg
        self.log = logging.getLogger(f"sup.{cfg.name}")
        self._task: Optional[asyncio.Task] = None
        self._stopping = False

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run_forever())

    async def stop(self) -> None:
        self._stopping = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run_forever(self) -> None:
        backoff = 1.0
        while not self._stopping:
            bot = self.factory(self.cfg)
            try:
                await bot.run()
                return
            except asyncio.CancelledError:
                return
            except Exception as e:
                self.log.exception("bot crashed: %s", e)
                jitter = 1 + random.uniform(-self.cfg.jitter, self.cfg.jitter)
                delay = min(backoff * jitter, self.cfg.max_backoff)
                self.log.warning("restarting in %.1fs", delay)
                try:
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    return
                backoff = min(backoff * 2, self.cfg.max_backoff)

# --------------------------- manager -----------------------------
class BotManager:
    def __init__(self):
        self.log = logging.getLogger("manager")
        self._supers: Dict[str, BotSupervisor] = {}

    def add(self, name: str, factory: Callable[[BotConfig], BaseBot], cfg: BotConfig) -> None:
        if name in self._supers:
            raise ValueError(f"bot '{name}' exists")
        self._supers[name] = BotSupervisor(factory, cfg)

    async def start(self) -> None:
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

# --------------------------- bootstrap ---------------------------
async def main() -> None:
    setup_logging()
    bus = PriceBus()

    # WS feed (Binance.US). For Binance.com: use_us=False and symbols=["BTCUSDT"].
    ws_cfg = BotConfig(
        name="ws-feed",
        tick_seconds=0.1,
        params={"bus": bus, "use_us": True, "symbols": ["BTCUSD"]},
    )

    # Paper trader config (e.g., 0.2% gaps, 8h TTL, 1-min reporting)
    paper_cfg = BotConfig(
        name="paper-btc",
        tick_seconds=1.0,
        params={
            "bus": bus,
            "symbol": "BTCUSD",
            "filters": {"tick": "0.01", "step": "0.000001", "min_qty": "0.00001", "min_notional": "10.0"},
            "gaps": {"buy_gap_pct": 0.2, "sell_gap_pct": 0.2},
            "min_profit_pct": 0.1,
            "balances": {"base": "0", "quote": "2000"},
            "fee_rate": 0.0001,
            "order_ttl_seconds": 28800,   # 8 hours
            "report_interval_sec": 60,    # 1 minute
        },
    )

    mgr = BotManager()
    mgr.add("ws-feed", lambda cfg: WSPriceBot(cfg), ws_cfg)
    mgr.add("paper-btc", lambda cfg: PaperTradingBot(cfg), paper_cfg)

    await mgr.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
