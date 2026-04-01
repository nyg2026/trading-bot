"""
===============================================================================
  Autonomous Trading Bot -- 7-Indicator Confluence Strategy
  Capital.com CFDs | UK100 (10x leverage) + Brent Crude (5x leverage)
  -----------------------------------------------------------------------------
  * Scans every SCAN_INTERVAL seconds during London session (08:00-16:30 BST)
  * 7 indicators: EMA9/21, RSI, MACD, Bollinger Bands, ATR, Stochastic, ADX
  * Requires MIN_SIGNAL_SCORE/7 confluence before entering
  * ATR-based SL (1x ATR) and TP (2x ATR) for adaptive risk management
  * PAPER_TRADE=true by default -- validates strategy before live execution
  * Profit protection: never risks more than original starting capital
  * EOD close: closes all positions before overnight CFD funding fees
===============================================================================
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import threading
import uuid
import xml.etree.ElementTree as ET
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal

import httpx
from fastapi import Depends, FastAPI, HTTPException, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# ==============================================================================
#  CONFIG  --  all from Railway environment variables
# ==============================================================================

# Paper trading -- set to "false" only when strategy is validated and you're ready
PAPER_TRADE       = os.getenv("PAPER_TRADE", "true").lower() == "true"

# Capital.com credentials
CAPITAL_API_KEY   = os.getenv("CAPITAL_API_KEY", "")
CAPITAL_EMAIL     = os.getenv("CAPITAL_EMAIL", "")
CAPITAL_PASSWORD  = os.getenv("CAPITAL_PASSWORD", "")
CAPITAL_DEMO      = os.getenv("CAPITAL_DEMO", "true").lower() == "true"

# Instruments -- 1 position max per instrument
ALLOWED_SYMBOLS   = set(s.strip().upper() for s in
                        os.getenv("ALLOWED_SYMBOLS", "UK100,OIL_BRENT").split(",") if s.strip())
MAX_OPEN_TRADES   = int(os.getenv("MAX_OPEN_TRADES", "2"))   # 1 per instrument

# Per-instrument leverage (Capital.com defaults: UK100=10x, Brent=5x)
_LEVERAGE_DEFAULTS = {"UK100": 10.0, "OIL_BRENT": 5.0}
LEVERAGE_UK100    = float(os.getenv("LEVERAGE_UK100", "10"))
LEVERAGE_OIL_BRENT = float(os.getenv("LEVERAGE_OIL_BRENT", "5"))
LEVERAGE_MAP      = {**_LEVERAGE_DEFAULTS,
                     "UK100": LEVERAGE_UK100, "OIL_BRENT": LEVERAGE_OIL_BRENT}

# Risk management
RISK_PCT          = float(os.getenv("RISK_PCT", "2.0"))      # % of risk capital per trade
ATR_SL_MULT       = float(os.getenv("ATR_SL_MULT", "1.0"))  # SL = ATR_SL_MULT x ATR
ATR_TP_MULT       = float(os.getenv("ATR_TP_MULT", "0.25"))  # TP = ATR_TP_MULT x ATR
DEFAULT_QTY       = float(os.getenv("DEFAULT_QTY", "0.1"))  # fallback minimum lot size

# Profit protection: never risk profits, only original capital
PROTECT_PROFITS   = os.getenv("PROTECT_PROFITS", "true").lower() == "true"

# EOD close: close all positions before 22:00 UTC Capital.com rollover
CLOSE_EOD         = os.getenv("CLOSE_EOD", "true").lower() == "true"
EOD_CLOSE_HOUR    = int(os.getenv("EOD_CLOSE_HOUR", "21"))
EOD_CLOSE_MINUTE  = int(os.getenv("EOD_CLOSE_MINUTE", "45"))

# London session: 07:00-15:30 UTC (08:00-16:30 BST)
MARKET_OPEN_HOUR   = int(os.getenv("MARKET_OPEN_HOUR", "7"))
MARKET_OPEN_MINUTE = int(os.getenv("MARKET_OPEN_MINUTE", "0"))
MARKET_CLOSE_HOUR  = int(os.getenv("MARKET_CLOSE_HOUR", "15"))
MARKET_CLOSE_MINUTE = int(os.getenv("MARKET_CLOSE_MINUTE", "30"))

# Signal engine settings
MIN_SIGNAL_SCORE  = int(os.getenv("MIN_SIGNAL_SCORE", "5"))  # out of 7
SCAN_INTERVAL     = int(os.getenv("SCAN_INTERVAL", "300"))   # seconds (5 min = 1 candle)
CANDLE_RES        = os.getenv("CANDLE_RES", "MINUTE_15")
CANDLE_COUNT      = int(os.getenv("CANDLE_COUNT", "120"))    # 120 x 15-min = 30 hours history

# Optional webhook secret
WEBHOOK_SECRET    = os.getenv("WEBHOOK_SECRET", "")

# ==============================================================================
#  LOGGING
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("bot")

# ==============================================================================
#  TRADE MANAGER  --  tracks live open positions
# ==============================================================================
@dataclass
class Trade:
    order_id:  str
    symbol:    str
    direction: str    # "BUY" or "SELL"
    entry:     float
    sl:        float | None
    tp:        float | None
    qty:       float = 0.0
    opened:    datetime = field(default_factory=lambda: datetime.now(timezone.utc))

class TradeManager:
    def __init__(self, max_trades: int = 2):
        self._trades: dict[str, Trade] = {}
        self._lock = threading.Lock()
        self.max_trades = max_trades

    def add(self, order_id, symbol, direction, entry, sl=None, tp=None, qty=0.0):
        with self._lock:
            self._trades[order_id] = Trade(order_id, symbol, direction, entry, sl, tp, qty)

    def remove_by_symbol(self, symbol):
        with self._lock:
            keys = [k for k, v in self._trades.items() if v.symbol == symbol]
            for k in keys:
                del self._trades[k]

    def get_by_symbol(self, symbol) -> Trade | None:
        with self._lock:
            return next((v for v in self._trades.values() if v.symbol == symbol), None)

    def count(self):
        with self._lock:
            return len(self._trades)

    def all(self):
        with self._lock:
            return list(self._trades.values())

trade_mgr = TradeManager(max_trades=MAX_OPEN_TRADES)

# ==============================================================================
#  PAPER TRADING  --  simulated execution with realistic P&L
# ==============================================================================
@dataclass
class PaperTrade:
    id:          str
    symbol:      str
    direction:   str    # "BUY" or "SELL"
    entry:       float
    sl:          float
    tp:          float
    qty:         float
    atr:         float
    score:       int
    opened:      str    # ISO timestamp
    closed:      str | None = None
    close_price: float | None = None
    close_reason: str | None = None  # "TP" | "SL" | "EOD" | "REVERSED"
    pnl:         float | None = None   # gross P&L in instrument currency

# In-memory paper trade log (resets on restart -- acceptable for paper demo)
_paper_trades: list[PaperTrade] = []
_paper_open: dict[str, PaperTrade] = {}   # keyed by symbol, at most 1 per instrument
_paper_pnl_total: float = 0.0
_paper_lock = threading.Lock()

_starting_balance: float = 0.0


def _paper_open_trade(symbol: str, direction: str, entry: float,
                      sl: float, tp: float, qty: float, atr: float, score: int) -> PaperTrade:
    pt = PaperTrade(
        id=str(uuid.uuid4())[:8],
        symbol=symbol, direction=direction, entry=entry,
        sl=sl, tp=tp, qty=qty, atr=atr, score=score,
        opened=datetime.now(timezone.utc).isoformat(),
    )
    with _paper_lock:
        _paper_trades.append(pt)
        _paper_open[symbol] = pt
    log.info("[PAPER] PAPER %s %s | qty=%.2f | entry=%.4f | SL=%.4f | TP=%.4f | score=%d/7",
             direction, symbol, qty, entry, sl, tp, score)
    return pt


def _paper_close_trade(symbol: str, close_price: float, reason: str) -> PaperTrade | None:
    global _paper_pnl_total
    with _paper_lock:
        pt = _paper_open.pop(symbol, None)
        if not pt:
            return None
        pt.closed = datetime.now(timezone.utc).isoformat()
        pt.close_price = round(close_price, 4)
        pt.close_reason = reason
        # P&L = qty x price_delta (in instrument points/dollars)
        if pt.direction == "BUY":
            pt.pnl = round(pt.qty * (close_price - pt.entry), 4)
        else:
            pt.pnl = round(pt.qty * (pt.entry - close_price), 4)
        _paper_pnl_total += pt.pnl
    log.info("[PAPER] PAPER CLOSED %s %s | reason=%s | entry=%.4f -> exit=%.4f | P&L=%.4f | cumPnL=%.4f",
             pt.direction, symbol, reason, pt.entry, close_price, pt.pnl, _paper_pnl_total)
    return pt


def _check_paper_exits(symbol: str, candle_high: float, candle_low: float):
    """Check if current candle's range would have hit SL or TP for open paper positions."""
    with _paper_lock:
        pt = _paper_open.get(symbol)
    if not pt:
        return
    if pt.direction == "BUY":
        if candle_low <= pt.sl:
            _paper_close_trade(symbol, pt.sl, "SL")
        elif candle_high >= pt.tp:
            _paper_close_trade(symbol, pt.tp, "TP")
    else:  # SELL
        if candle_high >= pt.sl:
            _paper_close_trade(symbol, pt.sl, "SL")
        elif candle_low <= pt.tp:
            _paper_close_trade(symbol, pt.tp, "TP")

# ==============================================================================
#  INDICATOR ENGINE  --  pure Python, no external dependencies
# ==============================================================================

def _ema(prices: list[float], period: int) -> list[float]:
    """Exponential Moving Average (standard multiplier k = 2/(n+1))."""
    if len(prices) < period:
        return prices[:]
    k = 2.0 / (period + 1)
    result = [sum(prices[:period]) / period]
    for p in prices[period:]:
        result.append(p * k + result[-1] * (1 - k))
    pad = len(prices) - len(result)
    return [result[0]] * pad + result

def _sma(prices: list[float], period: int) -> list[float]:
    result = []
    for i in range(len(prices)):
        if i < period - 1:
            result.append(prices[i])
        else:
            result.append(sum(prices[i - period + 1:i + 1]) / period)
    return result

def _rsi(prices: list[float], period: int = 14) -> list[float]:
    """RSI -- Wilder smoothing."""
    if len(prices) < period + 1:
        return [50.0] * len(prices)
    deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    gains  = [max(d, 0.0) for d in deltas]
    losses = [abs(min(d, 0.0)) for d in deltas]
    avg_g  = sum(gains[:period]) / period
    avg_l  = sum(losses[:period]) / period
    rsi_vals = [50.0] * (period + 1)
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        rs = avg_g / avg_l if avg_l else 100.0
        rsi_vals.append(100.0 - 100.0 / (1 + rs))
    return rsi_vals

def _macd(prices: list[float], fast=12, slow=26, signal=9):
    """Returns (macd_line, signal_line, histogram)."""
    ema_f = _ema(prices, fast)
    ema_s = _ema(prices, slow)
    macd  = [f - s for f, s in zip(ema_f, ema_s)]
    sig   = _ema(macd, signal)
    hist  = [m - s for m, s in zip(macd, sig)]
    return macd, sig, hist

def _bollinger(prices: list[float], period=20, mult=2.0):
    """Returns list of (lower, mid, upper)."""
    result = []
    for i in range(len(prices)):
        if i < period - 1:
            result.append((prices[i], prices[i], prices[i]))
        else:
            w   = prices[i - period + 1:i + 1]
            mid = sum(w) / period
            std = (sum((p - mid) ** 2 for p in w) / period) ** 0.5
            result.append((mid - mult * std, mid, mid + mult * std))
    return result

def _atr(highs: list[float], lows: list[float], closes: list[float], period=14) -> list[float]:
    """Average True Range -- Wilder smoothing."""
    trs = [highs[0] - lows[0]]
    for i in range(1, len(highs)):
        trs.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i]  - closes[i - 1]),
        ))
    atr_vals = [sum(trs[:period]) / period]
    for i in range(period, len(trs)):
        atr_vals.append((atr_vals[-1] * (period - 1) + trs[i]) / period)
    pad = len(highs) - len(atr_vals)
    return [atr_vals[0]] * pad + atr_vals

def _stochastic(highs: list[float], lows: list[float], closes: list[float],
                k_period=14, d_period=3):
    """Stochastic Oscillator. Returns (K, D)."""
    k_vals: list[float] = []
    for i in range(len(closes)):
        start = max(0, i - k_period + 1)
        hh = max(highs[start:i + 1])
        ll = min(lows[start:i + 1])
        k_vals.append(100.0 * (closes[i] - ll) / (hh - ll) if hh != ll else 50.0)
    d_vals = _sma(k_vals, d_period)
    return k_vals, d_vals

def _adx(highs: list[float], lows: list[float], closes: list[float], period=14):
    """ADX + +DI / -DI."""
    n = len(closes)
    plus_dm, minus_dm = [0.0], [0.0]
    for i in range(1, n):
        up   = highs[i]  - highs[i - 1]
        down = lows[i - 1] - lows[i]
        plus_dm.append(up   if up > down and up > 0   else 0.0)
        minus_dm.append(down if down > up and down > 0 else 0.0)
    atr_v   = _atr(highs, lows, closes, period)
    sm_p    = _ema(plus_dm, period)
    sm_m    = _ema(minus_dm, period)
    plus_di = [100.0 * p / a if a else 0.0 for p, a in zip(sm_p, atr_v)]
    minus_di= [100.0 * m / a if a else 0.0 for m, a in zip(sm_m, atr_v)]
    dx_vals = []
    for p, m in zip(plus_di, minus_di):
        s = p + m
        dx_vals.append(100.0 * abs(p - m) / s if s else 0.0)
    adx_v = _ema(dx_vals, period)
    return adx_v, plus_di, minus_di


def compute_signal(
    symbol: str,
    closes: list[float],
    highs:  list[float],
    lows:   list[float],
) -> tuple[str | None, int, dict]:
    """
    Score all 7 indicators and return (direction, score, details).
    direction = 'BUY', 'SELL', or None.
    score = 0-7.  Entry only when score >= MIN_SIGNAL_SCORE.
    """
    if len(closes) < 50:
        return None, 0, {"reason": "insufficient_data", "candles": len(closes)}

    price = closes[-1]

    ema9  = _ema(closes, 9)
    ema21 = _ema(closes, 21)
    rsi_v = _rsi(closes, 14)
    _, _, macd_hist = _macd(closes, 12, 26, 9)
    bbs   = _bollinger(closes, 20, 2.0)
    atr_v = _atr(highs, lows, closes, 14)
    k_v, d_v = _stochastic(highs, lows, closes, 14, 3)
    adx_v, pdi_v, mdi_v = _adx(highs, lows, closes, 14)

    e9  = ema9[-1];  e9p  = ema9[-2]  if len(ema9)  > 1 else e9
    e21 = ema21[-1]
    rsi = rsi_v[-1]
    mh  = macd_hist[-1]; mhp = macd_hist[-2] if len(macd_hist) > 1 else mh
    bb_lo, bb_mid, bb_hi = bbs[-1]
    atr = atr_v[-1]
    k   = k_v[-1]; kp = k_v[-2] if len(k_v) > 1 else k
    d   = d_v[-1]; dp = d_v[-2] if len(d_v) > 1 else d
    adx = adx_v[-1]
    pdi = pdi_v[-1]
    mdi = mdi_v[-1]

    atr_pct = (atr / price * 100) if price else 0

    # Skip dead markets (extremely low volatility)
    if atr_pct < 0.03:
        return None, 0, {"reason": "low_volatility", "atr_pct": round(atr_pct, 4)}

    buy_score  = 0
    sell_score = 0
    signals: dict[str, str] = {}

    # 1. EMA 9/21 Cross -- trend direction
    if e9 > e21:
        buy_score  += 1; signals["ema_cross"] = "BUY"
    elif e9 < e21:
        sell_score += 1; signals["ema_cross"] = "SELL"
    else:
        signals["ema_cross"] = "NEUTRAL"

    # 2. RSI 14 -- momentum confirmation
    if 38 < rsi < 63:
        buy_score  += 1; signals["rsi"] = f"BUY ({rsi:.1f})"
    if 37 < rsi < 62:
        sell_score += 1; signals["rsi"] = f"SELL ({rsi:.1f})"
    if rsi <= 30:
        buy_score  += 1; signals["rsi"] = f"OVERSOLD-BUY ({rsi:.1f})"
    elif rsi >= 70:
        sell_score += 1; signals["rsi"] = f"OVERBOUGHT-SELL ({rsi:.1f})"

    # 3. MACD Histogram -- momentum acceleration
    if mh > 0 and mh >= mhp:
        buy_score  += 1; signals["macd"] = f"BUY (hist {mh:.4f}^)"
    elif mh < 0 and mh <= mhp:
        sell_score += 1; signals["macd"] = f"SELL (hist {mh:.4f}v)"
    else:
        signals["macd"] = f"NEUTRAL (hist {mh:.4f})"

    # 4. Bollinger Bands -- trend confirmation + squeeze
    if price > bb_mid:
        buy_score  += 1; signals["bbands"] = f"BUY (above mid {bb_mid:.2f})"
    elif price < bb_mid:
        sell_score += 1; signals["bbands"] = f"SELL (below mid {bb_mid:.2f})"
    if price <= bb_lo * 1.002:
        buy_score  += 1; signals["bbands"] = f"BB-SQUEEZE-BUY (at lower {bb_lo:.2f})"
    elif price >= bb_hi * 0.998:
        sell_score += 1; signals["bbands"] = f"BB-SQUEEZE-SELL (at upper {bb_hi:.2f})"

    # 5. ATR -- confirms sufficient volatility for a tradeable move
    atr_avg = sum(atr_v[-10:]) / min(10, len(atr_v))
    if atr >= atr_avg:
        buy_score  += 1; sell_score += 1
        signals["atr"] = f"ACTIVE ({atr_pct:.2f}%)"
    else:
        signals["atr"] = f"QUIET ({atr_pct:.2f}%)"

    # 6. Stochastic (14,3,3) -- entry timing
    if k > d and k < 80 and kp <= dp:
        buy_score  += 1; signals["stoch"] = f"BUY-CROSS (K={k:.1f} D={d:.1f})"
    elif k < d and k > 20 and kp >= dp:
        sell_score += 1; signals["stoch"] = f"SELL-CROSS (K={k:.1f} D={d:.1f})"
    elif k > d and k < 80:
        buy_score  += 1; signals["stoch"] = f"BUY (K={k:.1f} D={d:.1f})"
    elif k < d and k > 20:
        sell_score += 1; signals["stoch"] = f"SELL (K={k:.1f} D={d:.1f})"
    else:
        signals["stoch"] = f"NEUTRAL (K={k:.1f} D={d:.1f})"

    # 7. ADX + Directional Index -- trend strength and direction
    if adx > 20 and pdi > mdi:
        buy_score  += 1; signals["adx"] = f"BUY (ADX={adx:.1f} +DI={pdi:.1f} -DI={mdi:.1f})"
    elif adx > 20 and mdi > pdi:
        sell_score += 1; signals["adx"] = f"SELL (ADX={adx:.1f} +DI={pdi:.1f} -DI={mdi:.1f})"
    else:
        signals["adx"] = f"NEUTRAL (ADX={adx:.1f})"

    buy_score  = min(buy_score, 7)
    sell_score = min(sell_score, 7)

    details = {
        "price":      round(price, 4),
        "ema9":       round(e9, 4),
        "ema21":      round(e21, 4),
        "rsi":        round(rsi, 2),
        "macd_hist":  round(mh, 6),
        "bb_lower":   round(bb_lo, 4),
        "bb_mid":     round(bb_mid, 4),
        "bb_upper":   round(bb_hi, 4),
        "atr":        round(atr, 4),
        "atr_pct":    round(atr_pct, 3),
        "stoch_k":    round(k, 2),
        "stoch_d":    round(d, 2),
        "adx":        round(adx, 2),
        "plus_di":    round(pdi, 2),
        "minus_di":   round(mdi, 2),
        "buy_score":  buy_score,
        "sell_score": sell_score,
        "signals":    signals,
    }

    if buy_score >= MIN_SIGNAL_SCORE and buy_score > sell_score:
        return "BUY", buy_score, details
    if sell_score >= MIN_SIGNAL_SCORE and sell_score > buy_score:
        return "SELL", sell_score, details
    return None, max(buy_score, sell_score), details


def compute_position_size(price: float, atr: float, risk_amount: float,
                          min_qty: float = 0.1) -> float:
    """
    ATR-based position sizing: qty = risk_amount / (atr x ATR_SL_MULT).
    Ensures actual risk = qty x SL_distance = risk_amount.
    """
    sl_distance = atr * ATR_SL_MULT
    if sl_distance > 0:
        qty = risk_amount / sl_distance
    else:
        qty = risk_amount / price
    return max(round(qty, 2), min_qty)


# ==============================================================================
#  CAPITAL.COM  --  auth, market data, order execution
# ==============================================================================
CAPITAL_BASE = (
    "https://demo-api-capital.backend-capital.com"
    if CAPITAL_DEMO else
    "https://api-capital.backend-capital.com"
)
_cap_session: dict = {}


async def _capital_auth() -> tuple[str, str]:
    """Returns (CST, X-SECURITY-TOKEN). Auto-refreshes every 8 minutes."""
    if _cap_session.get("ts", 0) + 480 > time.time():
        return _cap_session["cst"], _cap_session["token"]
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.post(
            f"{CAPITAL_BASE}/api/v1/session",
            json={"identifier": CAPITAL_EMAIL, "password": CAPITAL_PASSWORD,
                  "encryptedPassword": False},
            headers={"X-CAP-API-KEY": CAPITAL_API_KEY, "Content-Type": "application/json"},
        )
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Capital.com auth failed [{r.status_code}]: {r.text[:200]}")
    _cap_session.update({"cst": r.headers["CST"],
                         "token": r.headers["X-SECURITY-TOKEN"],
                         "ts": time.time()})
    log.info("[AUTH] Capital.com session refreshed")
    return _cap_session["cst"], _cap_session["token"]


def _cap_headers(cst, token):
    return {"CST": cst, "X-SECURITY-TOKEN": token, "Content-Type": "application/json"}


async def _capital_get_balance() -> float:
    cst, token = await _capital_auth()
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get(f"{CAPITAL_BASE}/api/v1/accounts", headers=_cap_headers(cst, token))
    accounts = r.json().get("accounts", [])
    for acc in sorted(accounts, key=lambda a: not a.get("preferred", False)):
        bal = acc.get("balance", {}).get("available", 0)
        if bal > 0:
            return float(bal)
    return 0.0


async def _get_risk_capital() -> float:
    """Profit protection: never risk more than original starting capital."""
    current = await _capital_get_balance()
    if PROTECT_PROFITS and _starting_balance > 0:
        risk_cap = min(current, _starting_balance)
        if current > _starting_balance:
            log.info("[PROFIT] Profit protection active: balance=%.2f  starting=%.2f  risk_cap=%.2f",
                     current, _starting_balance, risk_cap)
        return risk_cap
    return current


async def fetch_candles(epic: str) -> tuple[list, list, list, list]:
    """Fetch OHLCV candles. Returns (opens, highs, lows, closes)."""
    cst, token = await _capital_auth()
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.get(
            f"{CAPITAL_BASE}/api/v1/prices/{epic.upper()}",
            params={"resolution": CANDLE_RES, "max": CANDLE_COUNT},
            headers=_cap_headers(cst, token),
        )
    prices = r.json().get("prices", [])
    if not prices:
        return [], [], [], []
    opens  = [(p["openPrice"]["bid"]  + p["openPrice"]["ask"])  / 2 for p in prices]
    highs  = [max(p["highPrice"]["bid"],  p["highPrice"]["ask"])    for p in prices]
    lows   = [min(p["lowPrice"]["bid"],   p["lowPrice"]["ask"])     for p in prices]
    closes = [(p["closePrice"]["bid"] + p["closePrice"]["ask"]) / 2 for p in prices]
    return opens, highs, lows, closes


async def fetch_candles_timestamped(epic: str) -> list[dict]:
    """Fetch candles with snapshotTime -- used by dashboard chart."""
    cst, token = await _capital_auth()
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.get(
            f"{CAPITAL_BASE}/api/v1/prices/{epic.upper()}",
            params={"resolution": CANDLE_RES, "max": CANDLE_COUNT},
            headers=_cap_headers(cst, token),
        )
    prices = r.json().get("prices", [])
    return [{
        "time":  p.get("snapshotTime", ""),
        "open":  round((p["openPrice"]["bid"]  + p["openPrice"]["ask"])  / 2, 2),
        "high":  round(max(p["highPrice"]["bid"],  p["highPrice"]["ask"]), 2),
        "low":   round(min(p["lowPrice"]["bid"],   p["lowPrice"]["ask"]), 2),
        "close": round((p["closePrice"]["bid"] + p["closePrice"]["ask"]) / 2, 2),
    } for p in prices]


async def capital_open(symbol: str, direction: str, qty: float,
                       sl: float | None, tp: float | None) -> dict:
    """Open a live market position on Capital.com."""
    cst, token = await _capital_auth()
    body: dict = {
        "epic": symbol, "direction": direction.upper(), "size": qty,
        "guaranteedStop": False, "trailingStop": False,
    }
    if sl is not None: body["stopLevel"]   = round(sl, 4)
    if tp is not None: body["profitLevel"] = round(tp, 4)
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.post(f"{CAPITAL_BASE}/api/v1/positions", json=body,
                         headers=_cap_headers(cst, token))
    data = r.json()
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Capital.com error [{r.status_code}]: {data}")
    if data.get("status") in ("REJECTED", "DELETED"):
        raise RuntimeError(f"Deal rejected: {data.get('reason')} | {data}")
    log.info("[OK] LIVE %s %s | deal=%s | qty=%.2f | SL=%.4f | TP=%.4f",
             direction, symbol, data.get("dealId") or data.get("dealReference"), qty,
             sl or 0, tp or 0)
    return data


async def capital_close(symbol: str) -> dict:
    """Close the open live position for a given epic."""
    cst, token = await _capital_auth()
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get(f"{CAPITAL_BASE}/api/v1/positions", headers=_cap_headers(cst, token))
    positions = r.json().get("positions", [])
    deal_id = next(
        (p["position"]["dealId"] for p in positions if p["market"]["epic"] == symbol), None
    )
    if not deal_id:
        log.warning("No open position for %s to close", symbol)
        return {"status": "not_found"}
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.delete(f"{CAPITAL_BASE}/api/v1/positions/{deal_id}",
                           headers=_cap_headers(cst, token))
    log.info("[CLOSE] LIVE closed %s | deal=%s", symbol, deal_id)
    return r.json()


# ==============================================================================
#  AUTONOMOUS TRADING LOOP
# ==============================================================================
_last_signals: dict[str, dict] = {}


def _is_london_session() -> bool:
    """True during London session: 07:00-15:30 UTC (08:00-16:30 BST)."""
    now = datetime.now(timezone.utc)
    open_mins  = MARKET_OPEN_HOUR  * 60 + MARKET_OPEN_MINUTE
    close_mins = MARKET_CLOSE_HOUR * 60 + MARKET_CLOSE_MINUTE
    current    = now.hour * 60 + now.minute
    return open_mins <= current < close_mins


async def evaluate_symbol(epic: str):
    """Fetch candles, score 7 indicators, and enter/exit based on confluence."""
    try:
        _, highs, lows, closes = await fetch_candles(epic)
        if len(closes) < 50:
            log.warning("[WARN]  %s: only %d candles -- need 50+", epic, len(closes))
            return

        direction, score, details = compute_signal(epic, closes, highs, lows)
        _last_signals[epic] = {**details, "timestamp": datetime.now(timezone.utc).isoformat(),
                               "paper_mode": PAPER_TRADE}

        log.info("[SCAN] %s | BUY=%d/7  SELL=%d/7 | RSI=%.1f  ADX=%.1f  K=%.1f  MACD=%.5f",
                 epic, details.get("buy_score", 0), details.get("sell_score", 0),
                 details.get("rsi", 0), details.get("adx", 0),
                 details.get("stoch_k", 0), details.get("macd_hist", 0))

        atr   = details.get("atr", 0)
        price = closes[-1]

        # -- Paper exit check -------------------------------------------------
        if PAPER_TRADE:
            _check_paper_exits(epic, highs[-1], lows[-1])

        # -- Live exit check --------------------------------------------------
        live_trade = trade_mgr.get_by_symbol(epic)
        if live_trade and not PAPER_TRADE:
            opp_score = details.get(
                "sell_score" if live_trade.direction == "BUY" else "buy_score", 0
            )
            if direction and direction != live_trade.direction and opp_score >= max(MIN_SIGNAL_SCORE - 1, 4):
                log.info("[FLIP] %s: reversing %s->%s (opp_score=%d)", epic, live_trade.direction, direction, opp_score)
                await capital_close(epic)
                trade_mgr.remove_by_symbol(epic)
                live_trade = None

        # -- Entry check -------------------------------------------------------
        if direction is None:
            return

        paper_open = _paper_open.get(epic) if PAPER_TRADE else None
        already_in = (paper_open is not None) if PAPER_TRADE else (live_trade is not None)

        if already_in:
            return   # already holding this instrument

        total_open = (len(_paper_open) if PAPER_TRADE else trade_mgr.count())
        if total_open >= MAX_OPEN_TRADES:
            log.info("[BLOCK] %s: max positions reached (%d)", epic, MAX_OPEN_TRADES)
            return

        # ATR-based SL and TP levels
        if direction == "BUY":
            sl_level = round(price - atr * ATR_SL_MULT, 4)
            tp_level = round(price + atr * ATR_TP_MULT, 4)
        else:
            sl_level = round(price + atr * ATR_SL_MULT, 4)
            tp_level = round(price - atr * ATR_TP_MULT, 4)

        # ATR-based position sizing: risk RISK_PCT% of account
        if PAPER_TRADE:
            risk_cap = _starting_balance if _starting_balance > 0 else 1000.0
        else:
            risk_cap = await _get_risk_capital()

        leverage  = LEVERAGE_MAP.get(epic, 5.0)
        risk_gbp  = risk_cap * RISK_PCT / 100.0
        qty       = compute_position_size(price, atr, risk_gbp)

        log.info("[SIGNAL] SIGNAL: %s %s | score=%d/7 | price=%.4f | SL=%.4f | TP=%.4f | qty=%.2f | %s",
                 direction, epic, score, price, sl_level, tp_level, qty,
                 "PAPER" if PAPER_TRADE else "LIVE")

        if PAPER_TRADE:
            _paper_open_trade(epic, direction, price, sl_level, tp_level, qty, atr, score)
        else:
            order = await capital_open(epic, direction, qty, sl_level, tp_level)
            oid   = order.get("dealId") or order.get("dealReference", "unknown")
            trade_mgr.add(oid, epic, direction, price, sl_level, tp_level, qty)

    except Exception as e:
        log.error("[ERR] evaluate_symbol(%s) error: %s", epic, e)


async def scan_loop():
    """Main autonomous scan -- runs every SCAN_INTERVAL seconds during London session."""
    log.info("[BOT] Scan loop started | interval=%ds | symbols=%s | min_score=%d/7 | mode=%s",
             SCAN_INTERVAL, sorted(ALLOWED_SYMBOLS), MIN_SIGNAL_SCORE,
             "PAPER" if PAPER_TRADE else "LIVE")
    await asyncio.sleep(10)  # Brief startup delay
    while True:
        if _is_london_session():
            log.info("[TIMER]  Scanning %d symbols (London session)...", len(ALLOWED_SYMBOLS))
            for epic in sorted(ALLOWED_SYMBOLS):
                await evaluate_symbol(epic)
                await asyncio.sleep(2)
        else:
            now = datetime.now(timezone.utc)
            log.debug("[SLEEP] Outside London session (%02d:%02d UTC)", now.hour, now.minute)
        await asyncio.sleep(SCAN_INTERVAL)


async def eod_close_loop():
    """Close all positions before Capital.com's 22:00 UTC funding rollover."""
    if not CLOSE_EOD:
        return
    log.info("[EOD] EOD-close loop started | triggers at %02d:%02d UTC", EOD_CLOSE_HOUR, EOD_CLOSE_MINUTE)
    _eod_done: set[str] = set()
    while True:
        now = datetime.now(timezone.utc)
        today = now.strftime("%Y-%m-%d")
        if (now.hour == EOD_CLOSE_HOUR and now.minute >= EOD_CLOSE_MINUTE
                and today not in _eod_done):
            _eod_done.add(today)
            if len(_eod_done) > 2:
                _eod_done.pop()

            if PAPER_TRADE:
                # Close any open paper positions at current approximate price
                for sym in list(_paper_open.keys()):
                    try:
                        _, _, _, closes = await fetch_candles(sym)
                        close_px = closes[-1] if closes else _paper_open[sym].entry
                        _paper_close_trade(sym, close_px, "EOD")
                    except Exception as e:
                        log.error("Paper EOD close error for %s: %s", sym, e)
            else:
                for t in trade_mgr.all():
                    try:
                        await capital_close(t.symbol)
                        trade_mgr.remove_by_symbol(t.symbol)
                        log.info("[CLOSE] EOD closed %s", t.symbol)
                    except Exception as e:
                        log.error("EOD close error for %s: %s", t.symbol, e)
        await asyncio.sleep(60)


# ==============================================================================
#  FASTAPI APP
# ==============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global _starting_balance
    if CAPITAL_API_KEY:
        try:
            _starting_balance = await _capital_get_balance()
            log.info("[GBP] Starting balance: GBP%.2f (profit protection=%s)", _starting_balance, PROTECT_PROFITS)
        except Exception as e:
            log.warning("Could not fetch starting balance: %s -- using default GBP1000", e)
            _starting_balance = 1000.0

    log.info(
        "[START] Bot started | exchange=%s | mode=%s | scan=%ds | %s | "
        "risk=%.1f%% | sl_mult=%.1fx | tp_mult=%.1fx | "
        "london=07:%02d-15:%02d UTC | symbols=%s",
        "CAPITAL", "PAPER" if PAPER_TRADE else "LIVE",
        SCAN_INTERVAL, CANDLE_RES,
        RISK_PCT, ATR_SL_MULT, ATR_TP_MULT,
        MARKET_OPEN_MINUTE, MARKET_CLOSE_MINUTE,
        sorted(ALLOWED_SYMBOLS),
    )
    asyncio.create_task(scan_loop())
    asyncio.create_task(eod_close_loop())
    yield
    log.info("[STOP] Bot shutting down")


app = FastAPI(title="Trading Bot -- 7-Indicator v3", version="3.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET", "POST", "DELETE"],
                  allow_headers=["*"])


# -- Webhook model -------------------------------------------------------------
class AlertPayload(BaseModel):
    action:   Literal["buy", "sell", "close"]
    symbol:   str
    price:    float = Field(..., gt=0)
    sl:       float | None = None
    tp:       float | None = None
    qty:      float | None = None

def verify_secret(x_webhook_secret: str | None = Header(default=None)):
    if WEBHOOK_SECRET and x_webhook_secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Invalid webhook secret")


# ==============================================================================
#  ROUTES
# ==============================================================================

@app.get("/health")
async def health():
    _default_leverage = int(max(LEVERAGE_MAP.values()))
    return {
        "status":           "ok",
        "mode":             "PAPER" if PAPER_TRADE else "LIVE",
        "environment":      "DEMO" if CAPITAL_DEMO else "LIVE",
        "london_session":   _is_london_session(),
        "scan_interval_s":  SCAN_INTERVAL,
        "candle_res":       CANDLE_RES,
        "min_signal_score": MIN_SIGNAL_SCORE,
        "risk_pct":         RISK_PCT,
        "atr_sl_mult":      ATR_SL_MULT,
        "atr_tp_mult":      ATR_TP_MULT,
        "leverage_map":     LEVERAGE_MAP,
        "protect_profits":  PROTECT_PROFITS,
        "starting_balance": round(_starting_balance, 2),
        "symbols":          sorted(ALLOWED_SYMBOLS),
        "open_live_trades": trade_mgr.count(),
        "open_paper_trades": len(_paper_open),
        # Dashboard-compatible aliases
        "exchange":         "Capital.com",
        "tp_pct":           ATR_TP_MULT,
        "sl_pct":           ATR_SL_MULT,
        "leverage":         int(os.getenv("LEVERAGE", str(_default_leverage))),
        "max_trades":       MAX_OPEN_TRADES,
        "allowed_symbols":  sorted(ALLOWED_SYMBOLS),
        "open_trades":      trade_mgr.count(),
    }


@app.get("/signals")
async def get_signals():
    """Live indicator scores for all watched symbols (on-demand scan)."""
    results = {}
    for epic in sorted(ALLOWED_SYMBOLS):
        try:
            _, highs, lows, closes = await fetch_candles(epic)
            direction, score, details = compute_signal(epic, closes, highs, lows)
            results[epic] = {
                "direction":    direction,
                "score":        score,
                "min_required": MIN_SIGNAL_SCORE,
                "would_trade":  direction is not None and score >= MIN_SIGNAL_SCORE,
                "paper_mode":   PAPER_TRADE,
                **details,
            }
        except Exception as e:
            results[epic] = {"error": str(e)}
    return {"signals": results, "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/paper_trades")
async def get_paper_trades():
    """Full paper trade log with per-trade P&L."""
    with _paper_lock:
        all_trades = [vars(t).copy() for t in _paper_trades]
        open_now   = {k: vars(v).copy() for k, v in _paper_open.items()}
    return {
        "mode":       "PAPER",
        "total_pnl":  round(_paper_pnl_total, 4),
        "open":       open_now,
        "history":    all_trades,
        "timestamp":  datetime.now(timezone.utc).isoformat(),
    }


@app.get("/paper_summary")
async def get_paper_summary():
    """Paper trading performance summary: win rate, expectancy, monthly projection."""
    with _paper_lock:
        closed = [t for t in _paper_trades if t.pnl is not None]

    if not closed:
        return {"message": "No closed paper trades yet -- strategy is scanning"}

    wins   = [t for t in closed if t.pnl > 0]
    losses = [t for t in closed if t.pnl <= 0]
    win_rate = len(wins) / len(closed) * 100 if closed else 0

    avg_win  = sum(t.pnl for t in wins)  / len(wins)  if wins   else 0
    avg_loss = sum(t.pnl for t in losses)/ len(losses) if losses else 0
    expectancy = (win_rate / 100 * avg_win) + ((1 - win_rate / 100) * avg_loss)

    # Rough monthly projection based on current trade frequency
    if len(closed) > 0:
        first = datetime.fromisoformat(closed[0].opened.replace("Z", "+00:00"))
        days_running = max((datetime.now(timezone.utc) - first).days, 1)
        trades_per_month = (len(closed) / days_running) * 30
    else:
        trades_per_month = 20  # estimate

    monthly_projection = expectancy * trades_per_month

    return {
        "total_closed":        len(closed),
        "wins":                len(wins),
        "losses":              len(losses),
        "win_rate_pct":        round(win_rate, 1),
        "avg_win":             round(avg_win, 4),
        "avg_loss":            round(avg_loss, 4),
        "expectancy_per_trade": round(expectancy, 4),
        "trades_per_month_est": round(trades_per_month, 1),
        "monthly_pnl_projection": round(monthly_projection, 2),
        "total_pnl":           round(_paper_pnl_total, 4),
        "open_positions":      len(_paper_open),
        "note": "P&L denominated in instrument units (GBP for UK100, USD for OIL_BRENT)",
    }


@app.get("/candles/{epic}")
async def candles_endpoint(epic: str):
    """Return OHLCV candle data with timestamps for the dashboard candlestick chart."""
    try:
        candles = await fetch_candles_timestamped(epic.upper())
        return {"epic": epic.upper(), "resolution": CANDLE_RES, "candles": candles}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/news")
async def news_endpoint(q: str = "FTSE 100", limit: int = 6):
    """Fetch Google News RSS server-side -- avoids CORS issues in the dashboard."""
    try:
        url = f"https://news.google.com/rss/search?q={q}&hl=en-GB&gl=GB&ceid=GB:en"
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as c:
            r = await c.get(url, headers={"User-Agent": "Mozilla/5.0 (compatible; TradingBot/3.0)"})
        root = ET.fromstring(r.text)
        items = []
        for item in root.findall("./channel/item")[:limit]:
            def _t(tag):
                el = item.find(tag)
                return el.text or "" if el is not None else ""
            items.append({
                "title":   _t("title"),
                "link":    _t("guid") or _t("link"),
                "source":  _t("source"),
                "pubDate": _t("pubDate"),
            })
        return {"items": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/account")
async def get_account():
    cst, token = await _capital_auth()
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get(f"{CAPITAL_BASE}/api/v1/accounts", headers=_cap_headers(cst, token))
    data = r.json()
    current = next(
        (acc.get("balance", {}).get("available", 0)
         for acc in data.get("accounts", []) if acc.get("preferred")), 0)
    data["profit_protection"] = {
        "enabled":          PROTECT_PROFITS,
        "starting_balance": round(_starting_balance, 2),
        "current_balance":  round(float(current), 2),
        "protected_profit": round(max(0, float(current) - _starting_balance), 2),
    }
    return data


@app.get("/positions")
async def get_positions():
    cst, token = await _capital_auth()
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get(f"{CAPITAL_BASE}/api/v1/positions", headers=_cap_headers(cst, token))
    return r.json()


@app.get("/price/{epic}")
async def get_price(epic: str):
    cst, token = await _capital_auth()
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.get(f"{CAPITAL_BASE}/api/v1/markets/{epic.upper()}",
                        headers=_cap_headers(cst, token))
    return r.json()


@app.get("/markets")
async def search_markets(q: str = "brent"):
    cst, token = await _capital_auth()
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.get(f"{CAPITAL_BASE}/api/v1/markets",
                        params={"searchTerm": q, "limit": 10},
                        headers=_cap_headers(cst, token))
    return r.json()


@app.post("/webhook", dependencies=[Depends(verify_secret)])
async def webhook(payload: AlertPayload):
    """Optional manual override -- also validated against 7-indicator engine."""
    sym = payload.symbol.upper()
    if sym not in ALLOWED_SYMBOLS:
        return {"status": "rejected", "reason": "symbol_not_allowed"}

    log.info("[WEBHOOK] Webhook | action=%s | symbol=%s | price=%.4f", payload.action, sym, payload.price)

    if payload.action in ("buy", "sell"):
        direction = "BUY" if payload.action == "buy" else "SELL"
        # Validate against live indicators before acting
        try:
            _, highs, lows, closes = await fetch_candles(sym)
            sig_dir, score, details = compute_signal(sym, closes, highs, lows)
            atr = details.get("atr", 0)
            if sig_dir != direction:
                return {"status": "rejected", "reason": "indicator_disagreement",
                        "webhook": direction, "indicators": sig_dir, "score": score}
        except Exception as e:
            log.warning("Indicator check failed for webhook (%s) -- proceeding", e)
            atr = 0; closes = [payload.price]

        price = closes[-1]
        qty = payload.qty or compute_position_size(price, atr, _starting_balance * RISK_PCT / 100)

        if PAPER_TRADE:
            sl = payload.sl or (price - atr * ATR_SL_MULT if direction == "BUY" else price + atr * ATR_SL_MULT)
            tp = payload.tp or (price + atr * ATR_TP_MULT if direction == "BUY" else price - atr * ATR_TP_MULT)
            _paper_open_trade(sym, direction, price, round(sl, 4), round(tp, 4), qty, atr, score if 'score' in dir() else 0)
        else:
            sl = payload.sl; tp = payload.tp
            order = await capital_open(sym, direction, qty, sl, tp)
            oid = order.get("dealId") or order.get("dealReference", "unknown")
            trade_mgr.add(oid, sym, direction, price, sl, tp, qty)

        return {"status": "order_placed", "direction": direction, "mode": "PAPER" if PAPER_TRADE else "LIVE"}

    elif payload.action == "close":
        if PAPER_TRADE:
            pt = _paper_close_trade(sym, payload.price, "MANUAL")
            return {"status": "paper_closed", "pnl": pt.pnl if pt else None}
        else:
            result = await capital_close(sym)
            trade_mgr.remove_by_symbol(sym)
            return {"status": "closed", "result": result}

    return {"status": "ignored"}


@app.delete("/positions/{epic}")
async def force_close(epic: str):
    """Manually force-close a position."""
    sym = epic.upper()
    if PAPER_TRADE:
        try:
            _, _, _, closes = await fetch_candles(sym)
            px = closes[-1] if closes else 0
        except Exception:
            px = _paper_open.get(sym, None)
            px = px.entry if px else 0
        pt = _paper_close_trade(sym, px, "MANUAL")
        return {"status": "paper_closed", "pnl": pt.pnl if pt else None}
    result = await capital_close(sym)
    trade_mgr.remove_by_symbol(sym)
    return {"status": "closed", "result": result}


@app.exception_handler(Exception)
async def global_error(request: Request, exc: Exception):
    log.exception("[CRASH] Unhandled error: %s", exc)
    return JSONResponse(status_code=500, content={"status": "error", "detail": str(exc)})
