"""
Autonomous Trading Bot â 7-Indicator Confluence Strategy
Capital.com CFDs | UK100 + Brent Crude
â¢ Scans every SCAN_INTERVAL seconds (default 5 min)
â¢ Requires 5/7 indicator confluence before entering
â¢ Profit protection: never risks profit, only original capital
â¢ EOD close: closes all positions before overnight CFD fees
â¢ TradingView webhooks still accepted as optional manual override
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
import threading
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal

import httpx
from fastapi import Depends, FastAPI, HTTPException, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
#  CONFIG  â  all from Railway environment variables
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
EXCHANGE          = os.getenv("EXCHANGE", "CAPITAL").upper()
MAX_OPEN_TRADES   = int(os.getenv("MAX_OPEN_TRADES", "3"))
DEFAULT_QTY       = float(os.getenv("DEFAULT_QTY", "0.1"))
RISK_PCT          = float(os.getenv("RISK_PCT", "2"))        # % of trading capital per trade
LEVERAGE          = float(os.getenv("LEVERAGE", "5"))        # broker leverage
TP_PCT            = float(os.getenv("TP_PCT", "0.3"))        # take-profit % from entry
SL_PCT            = float(os.getenv("SL_PCT", "0.15"))       # stop-loss %  from entry
WEBHOOK_SECRET    = os.getenv("WEBHOOK_SECRET", "")
ALLOWED_SYMBOLS   = set(s.strip().upper() for s in os.getenv("ALLOWED_SYMBOLS", "UK100,OIL_BRENT").split(",") if s.strip())

# Autonomous scan settings
SCAN_INTERVAL     = int(os.getenv("SCAN_INTERVAL", "300"))      # seconds between scans (300 = 5 min)
CANDLE_RES        = os.getenv("CANDLE_RES", "MINUTE_15")        # candle resolution
CANDLE_COUNT      = int(os.getenv("CANDLE_COUNT", "120"))        # candles to fetch per scan
MIN_SIGNAL_SCORE  = int(os.getenv("MIN_SIGNAL_SCORE", "5"))     # out of 7 (higher = fewer but better trades)

# Profit & risk management
PROTECT_PROFITS   = os.getenv("PROTECT_PROFITS", "true").lower() == "true"  # never risk profits
CLOSE_EOD         = os.getenv("CLOSE_EOD", "true").lower() == "true"        # close before overnight fees
EOD_CLOSE_HOUR    = int(os.getenv("EOD_CLOSE_HOUR", "21"))       # UTC hour to close (21:45 = before Capital.com 22:00 rollover)
EOD_CLOSE_MINUTE  = int(os.getenv("EOD_CLOSE_MINUTE", "45"))

# Market hours (UTC) â only scan during active trading hours
MARKET_OPEN_HOUR  = int(os.getenv("MARKET_OPEN_HOUR", "7"))     # 07:00 UTC = 08:00 BST
MARKET_CLOSE_HOUR = int(os.getenv("MARKET_CLOSE_HOUR", "21"))   # 21:00 UTC = 22:00 BST

# Bybit (legacy â not used in default setup)
BYBIT_API_KEY     = os.getenv("BYBIT_API_KEY", "")
BYBIT_API_SECRET  = os.getenv("BYBIT_API_SECRET", "")
BYBIT_TESTNET     = os.getenv("BYBIT_TESTNET", "true").lower() == "true"

# Capital.com
CAPITAL_API_KEY   = os.getenv("CAPITAL_API_KEY", "")
CAPITAL_EMAIL     = os.getenv("CAPITAL_EMAIL", "")
CAPITAL_PASSWORD  = os.getenv("CAPITAL_PASSWORD", "")
CAPITAL_DEMO      = os.getenv("CAPITAL_DEMO", "true").lower() == "true"

# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
#  LOGGING
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("bot")

# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
#  TRADE MANAGER
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
@dataclass
class Trade:
    order_id:  str
    symbol:    str
    direction: str         # "BUY" or "SELL"
    entry:     float
    sl:        float | None
    tp:        float | None
    opened:    datetime = field(default_factory=lambda: datetime.now(timezone.utc))

class TradeManager:
    def __init__(self, max_trades: int = 3):
        self._trades: dict[str, Trade] = {}
        self._lock = threading.Lock()
        self.max_trades = max_trades

    def add(self, order_id, symbol, direction, entry, sl=None, tp=None):
        with self._lock:
            self._trades[order_id] = Trade(order_id, symbol, direction, entry, sl, tp)

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

# Starting balance for profit protection â set at startup
_starting_balance: float = 0.0

# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
#  INDICATOR ENGINE  â  pure Python, no external deps needed
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

def _ema(prices: list[float], period: int) -> list[float]:
    """Exponential Moving Average."""
    if len(prices) < period:
        return prices[:]
    k = 2.0 / (period + 1)
    result = [sum(prices[:period]) / period]
    for p in prices[period:]:
        result.append(p * k + result[-1] * (1 - k))
    # Pad front so length matches input
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
    """RSI â Wilder smoothed."""
    if len(prices) < period + 1:
        return [50.0] * len(prices)
    deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    gains  = [max(d, 0.0) for d in deltas]
    losses = [abs(min(d, 0.0)) for d in deltas]
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    rsi_vals = [50.0] * (period + 1)  # pad
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        rs = avg_g / avg_l if avg_l else 100.0
        rsi_vals.append(100.0 - 100.0 / (1 + rs))
    return rsi_vals

def _macd(prices: list[float], fast=12, slow=26, signal=9):
    """Returns (macd_line, signal_line, histogram) each same length as prices."""
    ema_f = _ema(prices, fast)
    ema_s = _ema(prices, slow)
    macd  = [f - s for f, s in zip(ema_f, ema_s)]
    sig   = _ema(macd, signal)
    hist  = [m - s for m, s in zip(macd, sig)]
    return macd, sig, hist

def _bollinger(prices: list[float], period=20, mult=2.0):
    """Returns list of (lower, mid, upper) same length as prices."""
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
    """Average True Range â Wilder smoothed."""
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
    """Stochastic Oscillator. Returns (K, D) each same length as closes."""
    k_vals: list[float] = []
    for i in range(len(closes)):
        start = max(0, i - k_period + 1)
        hh = max(highs[start:i + 1])
        ll = min(lows[start:i + 1])
        k_vals.append(100.0 * (closes[i] - ll) / (hh - ll) if hh != ll else 50.0)
    d_vals = _sma(k_vals, d_period)
    return k_vals, d_vals

def _adx(highs: list[float], lows: list[float], closes: list[float], period=14):
    """ADX + +DI / -DI. Returns (adx, plus_di, minus_di) same length as closes."""
    n = len(closes)
    plus_dm  = [0.0]
    minus_dm = [0.0]
    for i in range(1, n):
        up   = highs[i]  - highs[i - 1]
        down = lows[i - 1] - lows[i]
        plus_dm.append(up   if up > down and up > 0   else 0.0)
        minus_dm.append(down if down > up and down > 0 else 0.0)

    atr_v  = _atr(highs, lows, closes, period)
    sm_p   = _ema(plus_dm,  period)
    sm_m   = _ema(minus_dm, period)

    plus_di  = [100.0 * p / a if a else 0.0 for p, a in zip(sm_p,  atr_v)]
    minus_di = [100.0 * m / a if a else 0.0 for m, a in zip(sm_m, atr_v)]

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
    direction is 'BUY', 'SELL', or None.
    score is 0-7 â we enter only when score >= MIN_SIGNAL_SCORE.
    """
    if len(closes) < 50:
        return None, 0, {"reason": "insufficient_data", "candles": len(closes)}

    price = closes[-1]

    # ââ Compute all indicators ââââââââââââââââââââââââââââââââââââââââââââââ
    ema9  = _ema(closes, 9)
    ema21 = _ema(closes, 21)
    rsi_v = _rsi(closes, 14)
    _, _, macd_hist = _macd(closes, 12, 26, 9)
    bbs   = _bollinger(closes, 20, 2.0)
    atr_v = _atr(highs, lows, closes, 14)
    k_v, d_v = _stochastic(highs, lows, closes, 14, 3)
    adx_v, pdi_v, mdi_v = _adx(highs, lows, closes, 14)

    # ââ Latest values ââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    e9  = ema9[-1];  e9p  = ema9[-2]  if len(ema9)  > 1 else e9
    e21 = ema21[-1]; e21p = ema21[-2] if len(ema21) > 1 else e21
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

    # Skip if volatility is too low (dead market)
    if atr_pct < 0.04:
        return None, 0, {"reason": "low_volatility", "atr_pct": round(atr_pct, 4)}

    # ââ Score each indicator for BUY and SELL ââââââââââââââââââââââââââââââââ
    buy_score  = 0
    sell_score = 0
    signals: dict[str, str] = {}

    # 1. EMA 9/21 Cross â trend direction
    if e9 > e21:
        buy_score  += 1; signals["ema_cross"] = "BUY"
    elif e9 < e21:
        sell_score += 1; signals["ema_cross"] = "SELL"
    else:
        signals["ema_cross"] = "NEUTRAL"

    # 2. RSI 14 â momentum window (not overbought/oversold at entry)
    if 38 < rsi < 63:
        buy_score  += 1; signals["rsi"] = f"BUY ({rsi:.1f})"
    elif 37 < rsi < 62:
        sell_score += 1; signals["rsi"] = f"SELL ({rsi:.1f})"
    else:
        signals["rsi"] = f"NEUTRAL ({rsi:.1f})"
    # Override: extreme RSI adds to opposite score  (mean reversion flavour)
    if rsi <= 30:
        buy_score  += 1; signals["rsi"] = f"OVERSOLD-BUY ({rsi:.1f})"
    elif rsi >= 70:
        sell_score += 1; signals["rsi"] = f"OVERBOUGHT-SELL ({rsi:.1f})"

    # 3. MACD Histogram â momentum direction & acceleration
    if mh > 0 and mh >= mhp:
        buy_score  += 1; signals["macd"] = f"BUY (hist {mh:.4f}â)"
    elif mh < 0 and mh <= mhp:
        sell_score += 1; signals["macd"] = f"SELL (hist {mh:.4f}â)"
    else:
        signals["macd"] = f"NEUTRAL (hist {mh:.4f})"

    # 4. Bollinger Bands â trend confirmation & mean reversion
    if price > bb_mid:
        buy_score  += 1; signals["bbands"] = f"BUY (above mid {bb_mid:.1f})"
    elif price < bb_mid:
        sell_score += 1; signals["bbands"] = f"SELL (below mid {bb_mid:.1f})"
    # Squeeze play: near lower band = buy, near upper = sell
    if price <= bb_lo * 1.002:
        buy_score  += 1; signals["bbands"] = f"BB-SQUEEZE-BUY (at lower {bb_lo:.1f})"
    elif price >= bb_hi * 0.998:
        sell_score += 1; signals["bbands"] = f"BB-SQUEEZE-SELL (at upper {bb_hi:.1f})"

    # 5. ATR â confirms meaningful volatility (already past the floor check)
    #    Higher ATR relative to recent average = trending move
    atr_avg = sum(atr_v[-10:]) / min(10, len(atr_v))
    if atr >= atr_avg:
        buy_score  += 1; sell_score += 1  # Both benefit equally from good vol
        signals["atr"] = f"ACTIVE ({atr_pct:.2f}%)"
    else:
        signals["atr"] = f"QUIET ({atr_pct:.2f}%)"

    # 6. Stochastic (14,3,3) â entry timing
    if k > d and k < 80 and kp <= dp:   # Bullish cross below overbought
        buy_score  += 1; signals["stoch"] = f"BUY-CROSS (K={k:.1f} D={d:.1f})"
    elif k < d and k > 20 and kp >= dp: # Bearish cross above oversold
        sell_score += 1; signals["stoch"] = f"SELL-CROSS (K={k:.1f} D={d:.1f})"
    elif k > d and k < 80:
        buy_score  += 1; signals["stoch"] = f"BUY (K={k:.1f} D={d:.1f})"
    elif k < d and k > 20:
        sell_score += 1; signals["stoch"] = f"SELL (K={k:.1f} D={d:.1f})"
    else:
        signals["stoch"] = f"NEUTRAL (K={k:.1f} D={d:.1f})"

    # 7. ADX + Directional Index â trend strength & direction
    if adx > 20 and pdi > mdi:
        buy_score  += 1; signals["adx"] = f"BUY (ADX={adx:.1f} +DI={pdi:.1f} -DI={mdi:.1f})"
    elif adx > 20 and mdi > pdi:
        sell_score += 1; signals["adx"] = f"SELL (ADX={adx:.1f} +DI={pdi:.1f} -DI={mdi:.1f})"
    else:
        signals["adx"] = f"NEUTRAL (ADX={adx:.1f})"

    # ââ Cap scores at 7 âââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    buy_score  = min(buy_score, 7)
    sell_score = min(sell_score, 7)

    details = {
        "price":      round(price, 4),
        "ema9":       round(e9,  4),
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


# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
#  CAPITAL.COM  â  auth, market data, order execution
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
CAPITAL_BASE = (
    "https://demo-api-capital.backend-capital.com"
    if CAPITAL_DEMO else
    "https://api-capital.backend-capital.com"
)
_cap_session: dict = {}


async def _capital_auth() -> tuple[str, str]:
    """Returns (CST, X-SECURITY-TOKEN). Auto-refreshes every 8 min."""
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
    log.info("ð Capital.com session refreshed")
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
    """
    Profit Protection: never risk more than the original starting capital.
    Profits accumulate in the account but are never put at risk.
    """
    current = await _capital_get_balance()
    if PROTECT_PROFITS and _starting_balance > 0:
        risk_cap = min(current, _starting_balance)
        if current > _starting_balance:
            profit = current - _starting_balance
            log.info("ð° Profit protection: balance=%.2f  starting=%.2f  protected_profit=%.2f  risk_capital=%.2f",
                     current, _starting_balance, profit, risk_cap)
        return risk_cap
    return current


async def fetch_candles(epic: str) -> tuple[list, list, list, list]:
    """Fetch OHLCV candles from Capital.com. Returns (opens, highs, lows, closes)."""
    cst, token = await _capital_auth()
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.get(
            f"{CAPITAL_BASE}/api/v1/prices/{epic.upper()}",
            params={"resolution": CANDLE_RES, "max": CANDLE_COUNT},
            headers=_cap_headers(cst, token),
        )
    data = r.json()
    prices = data.get("prices", [])
    if not prices:
        log.warning("No candles returned for %s: %s", epic, data)
        return [], [], [], []

    opens  = [(p["openPrice"]["bid"]  + p["openPrice"]["ask"])  / 2 for p in prices]
    highs  = [max(p["highPrice"]["bid"],  p["highPrice"]["ask"])    for p in prices]
    lows   = [min(p["lowPrice"]["bid"],   p["lowPrice"]["ask"])     for p in prices]
    closes = [(p["closePrice"]["bid"] + p["closePrice"]["ask"]) / 2 for p in prices]
    return opens, highs, lows, closes


async def capital_open(symbol: str, qty_override: float | None, price: float,
                       direction: str = "BUY", sl=None, tp=None) -> dict:
    cst, token = await _capital_auth()
    is_long = direction.upper() == "BUY"

    # ââ Sizing ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    if qty_override and qty_override > 0:
        final_qty = qty_override
    else:
        risk_cap  = await _get_risk_capital()
        trade_val = risk_cap * RISK_PCT / 100.0 * LEVERAGE
        raw_qty   = trade_val / price if price else DEFAULT_QTY
        # Minimum size enforcement (Capital.com: UK100 min = 0.1, Brent min = 0.1)
        final_qty = max(round(raw_qty, 2), 0.1)
        log.info("ð Sizing: risk_cap=%.2f Ã %.1f%% Ã %.0fx leverage â val=Â£%.2f Ã· price=%.2f â qty=%.2f",
                 risk_cap, RISK_PCT, LEVERAGE, trade_val, price, final_qty)

    # ââ TP / SL âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    if is_long:
        auto_tp = round(price * (1 + TP_PCT / 100), 4) if TP_PCT > 0 else None
        auto_sl = round(price * (1 - SL_PCT / 100), 4) if SL_PCT > 0 else None
    else:
        auto_tp = round(price * (1 - TP_PCT / 100), 4) if TP_PCT > 0 else None
        auto_sl = round(price * (1 + SL_PCT / 100), 4) if SL_PCT > 0 else None
    final_tp = tp or auto_tp
    final_sl = sl or auto_sl

    log.info("ð¯ TP=%.4f  ð SL=%.4f", final_tp or 0, final_sl or 0)

    body: dict = {
        "epic": symbol,
        "direction": direction.upper(),
        "size": final_qty,
        "guaranteedStop": False,
        "trailingStop": False,
    }
    if final_sl: body["stopLevel"]   = final_sl
    if final_tp: body["profitLevel"] = final_tp

    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.post(f"{CAPITAL_BASE}/api/v1/positions", json=body,
                         headers=_cap_headers(cst, token))
    data = r.json()
    log.info("ð Capital.com %s [%d]: %s", direction, r.status_code, data)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Capital.com error [{r.status_code}]: {data}")
    if data.get("status") in ("REJECTED", "DELETED"):
        raise RuntimeError(f"Deal rejected: {data.get('reason')} | {data}")
    log.info("â %s opened | deal=%s | qty=%.2f | price=%.4f",
             direction, data.get("dealId") or data.get("dealReference"), final_qty, price)
    return data


async def capital_close(symbol: str) -> dict:
    cst, token = await _capital_auth()
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get(f"{CAPITAL_BASE}/api/v1/positions", headers=_cap_headers(cst, token))
    positions = r.json().get("positions", [])
    deal_id = next(
        (p["position"]["dealId"] for p in positions if p["market"]["epic"] == symbol), None
    )
    if not deal_id:
        log.warning("No open position found for %s to close", symbol)
        return {"status": "not_found"}
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.delete(f"{CAPITAL_BASE}/api/v1/positions/{deal_id}",
                           headers=_cap_headers(cst, token))
    log.info("ð Closed %s | deal=%s", symbol, deal_id)
    return r.json()


# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
#  BYBIT  (legacy â kept for compatibility)
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
BYBIT_BASE = "https://api-testnet.bybit.com" if BYBIT_TESTNET else "https://api.bybit.com"

def _bybit_sign(body: dict, ts: str) -> str:
    payload = ts + BYBIT_API_KEY + "5000" + json.dumps(body, separators=(",", ":"))
    return hmac.new(BYBIT_API_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()

def _bybit_headers(body: dict) -> dict:
    ts = str(int(time.time() * 1000))
    return {
        "X-BAPI-API-KEY": BYBIT_API_KEY, "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-SIGN": _bybit_sign(body, ts), "X-BAPI-RECV-WINDOW": "5000",
        "Content-Type": "application/json",
    }

async def bybit_open(symbol, qty, direction="BUY", sl=None, tp=None):
    side = "Buy" if direction.upper() == "BUY" else "Sell"
    body = {"category": "linear", "symbol": symbol, "side": side,
            "orderType": "Market", "qty": str(qty), "timeInForce": "IOC",
            "reduceOnly": False, "closeOnTrigger": False, "positionIdx": 0}
    if sl: body["stopLoss"] = str(round(sl, 6))
    if tp: body["takeProfit"] = str(round(tp, 6))
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.post(f"{BYBIT_BASE}/v5/order/create", json=body, headers=_bybit_headers(body))
    data = r.json()
    if data.get("retCode") != 0:
        raise RuntimeError(f"Bybit {data.get('retCode')}: {data.get('retMsg')}")
    return data["result"]

async def bybit_close(symbol, position_direction="BUY"):
    side = "Sell" if position_direction.upper() == "BUY" else "Buy"
    body = {"category": "linear", "symbol": symbol, "side": side,
            "orderType": "Market", "qty": "0", "timeInForce": "IOC",
            "reduceOnly": True, "closeOnTrigger": True, "positionIdx": 0}
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.post(f"{BYBIT_BASE}/v5/order/create", json=body, headers=_bybit_headers(body))
    data = r.json()
    if data.get("retCode") != 0:
        raise RuntimeError(f"Bybit close {data.get('retMsg')}")
    return data["result"]


# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
#  AUTONOMOUS TRADING LOOP
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

# Cache last signals for the /signals endpoint
_last_signals: dict[str, dict] = {}


async def evaluate_symbol(epic: str):
    """Fetch candles, score indicators, and open/close if conditions are met."""
    try:
        _, highs, lows, closes = await fetch_candles(epic)
        if len(closes) < 50:
            log.warning("â ï¸  %s: only %d candles â need 50+", epic, len(closes))
            return

        direction, score, details = compute_signal(epic, closes, highs, lows)
        _last_signals[epic] = {**details, "timestamp": datetime.now(timezone.utc).isoformat()}

        log.info(
            "ð %s | BUY=%d/7  SELL=%d/7 | RSI=%.1f  ADX=%.1f  K=%.1f  MACD=%.5f",
            epic, details.get("buy_score", 0), details.get("sell_score", 0),
            details.get("rsi", 0), details.get("adx", 0),
            details.get("stoch_k", 0), details.get("macd_hist", 0),
        )

        existing = trade_mgr.get_by_symbol(epic)

        # ââ EXIT check ââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
        if existing:
            # Close if a strong opposite signal appears (score â¥ MIN_SIGNAL_SCORE - 1)
            opp_score = details.get("sell_score" if existing.direction == "BUY" else "buy_score", 0)
            if direction and direction != existing.direction and opp_score >= max(MIN_SIGNAL_SCORE - 1, 4):
                log.info("ð %s: reversing %sâ%s (opp_score=%d)", epic, existing.direction, direction, opp_score)
                if EXCHANGE == "CAPITAL":
                    await capital_close(epic)
                else:
                    await bybit_close(epic, existing.direction)
                trade_mgr.remove_by_symbol(epic)
                existing = None

        # ââ ENTRY check âââââââââââââââââââââââââââââââââââââââââââââââââââââââ
        if not existing and direction and trade_mgr.count() < MAX_OPEN_TRADES:
            price = closes[-1]
            log.info("ð¯ SIGNAL: %s %s | score=%d/7 | price=%.4f", direction, epic, score, price)
            if EXCHANGE == "CAPITAL":
                order = await capital_open(epic, None, price, direction)
                oid = order.get("dealId") or order.get("dealReference", "unknown")
            else:
                qty = DEFAULT_QTY
                order = await bybit_open(epic, qty, direction)
                oid = order.get("orderId", "unknown")
            trade_mgr.add(oid, epic, direction, price)

    except Exception as e:
        log.error("â evaluate_symbol(%s) error: %s", epic, e)


async def scan_loop():
    """Main autonomous scan â runs every SCAN_INTERVAL seconds."""
    log.info("ð¤ Autonomous scan started | interval=%ds | candles=%sÃ%s | min_score=%d/7",
             SCAN_INTERVAL, CANDLE_COUNT, CANDLE_RES, MIN_SIGNAL_SCORE)
    await asyncio.sleep(10)  # Brief startup delay
    while True:
        now = datetime.now(timezone.utc)
        in_hours = MARKET_OPEN_HOUR <= now.hour < MARKET_CLOSE_HOUR
        if in_hours:
            log.info("â±ï¸  Scanning %d symbolsâ¦", len(ALLOWED_SYMBOLS))
            for epic in list(ALLOWED_SYMBOLS):
                await evaluate_symbol(epic)
                await asyncio.sleep(2)   # Small gap between symbols
        else:
            log.debug("ð¤ Outside market hours (%02d:%02d UTC)", now.hour, now.minute)
        await asyncio.sleep(SCAN_INTERVAL)


async def eod_close_loop():
    """Close all positions before Capital.com's 22:00 UTC overnight funding rollover."""
    if not CLOSE_EOD:
        return
    log.info("ð EOD-close loop started | will close at %02d:%02d UTC", EOD_CLOSE_HOUR, EOD_CLOSE_MINUTE)
    _eod_done_today: set[str] = set()   # prevent double-close same day
    while True:
        now = datetime.now(timezone.utc)
        today_key = now.strftime("%Y-%m-%d")
        if (now.hour == EOD_CLOSE_HOUR and now.minute >= EOD_CLOSE_MINUTE
                and today_key not in _eod_done_today):
            _eod_done_today.add(today_key)
            # Keep only last 2 dates to avoid unbounded growth
            if len(_eod_done_today) > 2:
                _eod_done_today.pop()
            if trade_mgr.count() > 0:
                log.info("ð EOD close triggered (%02d:%02d UTC) â %d open positions",
                         now.hour, now.minute, trade_mgr.count())
                for t in trade_mgr.all():
                    try:
                        await capital_close(t.symbol)
                        trade_mgr.remove_by_symbol(t.symbol)
                        log.info("ð EOD closed %s", t.symbol)
                    except Exception as e:
                        log.error("EOD close error for %s: %s", t.symbol, e)
            else:
                log.info("ð EOD check: no open positions")
        await asyncio.sleep(60)


# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
#  FASTAPI APP
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
@asynccontextmanager
async def lifespan(app: FastAPI):
    global _starting_balance
    # Record starting balance for profit protection
    if EXCHANGE == "CAPITAL" and CAPITAL_API_KEY:
        try:
            _starting_balance = await _capital_get_balance()
            log.info("ð· Starting balance recorded: Â£%.2f (profit protection active=%s)",
                     _starting_balance, PROTECT_PROFITS)
        except Exception as e:
            log.warning("Could not fetch starting balance: %s", e)

    log.info(
        "ð Bot started | exchange=%s | mode=%s | scan=%ds | candles=%sÃ%s | "
        "min_score=%d/7 | risk=%.1f%% | leverage=%.0fx | tp=%.2f%% | sl=%.2f%% | "
        "protect_profits=%s | close_eod=%s | symbols=%s",
        EXCHANGE,
        "DEMO" if (CAPITAL_DEMO if EXCHANGE == "CAPITAL" else BYBIT_TESTNET) else "LIVE",
        SCAN_INTERVAL, CANDLE_COUNT, CANDLE_RES, MIN_SIGNAL_SCORE,
        RISK_PCT, LEVERAGE, TP_PCT, SL_PCT,
        PROTECT_PROFITS, CLOSE_EOD,
        sorted(ALLOWED_SYMBOLS),
    )

    # Start background tasks
    asyncio.create_task(scan_loop())
    asyncio.create_task(eod_close_loop())
    yield
    log.info("ð Bot shutting down")


app = FastAPI(title="Trading Bot â 7-Indicator", version="2.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET", "POST", "DELETE"],
                  allow_headers=["*"])

# ââ Models ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
class AlertPayload(BaseModel):
    action:   Literal["buy", "sell", "close"]
    symbol:   str
    exchange: str   = "CAPITAL"
    price:    float = Field(..., gt=0)
    sl:       float | None = None
    tp:       float | None = None
    qty:      float | None = None
    # Legacy fields (ignored but accepted)
    rsi:      float | None = None
    bb_lower: float | None = None
    bb_mid:   float | None = None

# ââ Security ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
def verify_secret(x_webhook_secret: str | None = Header(default=None)):
    if WEBHOOK_SECRET and x_webhook_secret != WEBHOOK_SECRET:
        log.warning("â ï¸ Rejected webhook â bad secret")
        raise HTTPException(status_code=403, detail="Invalid webhook secret")

# ââ Routes ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
@app.get("/health")
async def health():
    return {
        "status":          "ok",
        "exchange":        EXCHANGE,
        "mode":            "DEMO" if (CAPITAL_DEMO if EXCHANGE == "CAPITAL" else BYBIT_TESTNET) else "LIVE",
        "open_trades":     trade_mgr.count(),
        "max_trades":      MAX_OPEN_TRADES,
        "risk_pct":        RISK_PCT,
        "leverage":        LEVERAGE,
        "tp_pct":          TP_PCT,
        "sl_pct":          SL_PCT,
        "min_signal_score": MIN_SIGNAL_SCORE,
        "protect_profits": PROTECT_PROFITS,
        "close_eod":       CLOSE_EOD,
        "scan_interval_s": SCAN_INTERVAL,
        "candle_res":      CANDLE_RES,
        "allowed_symbols": sorted(ALLOWED_SYMBOLS),
        "starting_balance": round(_starting_balance, 2),
    }


@app.get("/signals")
async def get_signals():
    """Live indicator scores for all watched symbols â run on-demand scan."""
    if EXCHANGE != "CAPITAL":
        return {"error": "only available when EXCHANGE=CAPITAL"}
    results = {}
    for epic in sorted(ALLOWED_SYMBOLS):
        try:
            _, highs, lows, closes = await fetch_candles(epic)
            direction, score, details = compute_signal(epic, closes, highs, lows)
            results[epic] = {
                "direction":   direction,
                "score":       score,
                "min_required": MIN_SIGNAL_SCORE,
                "would_trade": direction is not None and score >= MIN_SIGNAL_SCORE,
                **details,
            }
        except Exception as e:
            results[epic] = {"error": str(e)}
    return {"signals": results, "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/account")
async def get_account():
    if EXCHANGE != "CAPITAL": return {"error": "only available when EXCHANGE=CAPITAL"}
    cst, token = await _capital_auth()
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get(f"{CAPITAL_BASE}/api/v1/accounts", headers=_cap_headers(cst, token))
    data = r.json()
    # Append profit protection info
    current_bal = next(
        (acc.get("balance", {}).get("available", 0)
         for acc in data.get("accounts", []) if acc.get("preferred")), 0)
    data["profit_protection"] = {
        "enabled":          PROTECT_PROFITS,
        "starting_balance": round(_starting_balance, 2),
        "current_balance":  round(float(current_bal), 2),
        "protected_profit": round(max(0, float(current_bal) - _starting_balance), 2),
        "risk_capital":     round(min(float(current_bal), _starting_balance) if PROTECT_PROFITS else float(current_bal), 2),
    }
    return data


@app.get("/positions")
async def get_positions():
    if EXCHANGE != "CAPITAL": return {"error": "only available when EXCHANGE=CAPITAL"}
    cst, token = await _capital_auth()
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get(f"{CAPITAL_BASE}/api/v1/positions", headers=_cap_headers(cst, token))
    return r.json()


@app.get("/price/{epic}")
async def get_market_price(epic: str):
    if EXCHANGE != "CAPITAL": return {"error": "only available when EXCHANGE=CAPITAL"}
    cst, token = await _capital_auth()
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.get(f"{CAPITAL_BASE}/api/v1/markets/{epic.upper()}",
                        headers=_cap_headers(cst, token))
    return r.json()



@app.get("/candles/{epic}")
async def candles_endpoint(epic: str):
    """Return OHLCV candle data for the dashboard chart."""
    try:
        opens, highs, lows, closes = await fetch_candles(epic.upper())
        return {"epic": epic.upper(), "resolution": CANDLE_RES,
                "opens": opens, "highs": highs, "lows": lows, "closes": closes}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/news")
async def news_endpoint(q: str = "FTSE 100", limit: int = 6):
    """Fetch Google News RSS server-side and return parsed articles for the dashboard."""
    import xml.etree.ElementTree as ET
    try:
        url = f"https://news.google.com/rss/search?q={q}&hl=en-GB&gl=GB&ceid=GB:en"
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as c:
            r = await c.get(url, headers={"User-Agent": "Mozilla/5.0 (compatible; TradingBot/2.0)"})
        root = ET.fromstring(r.text)
        items = []
        for item in root.findall('./channel/item')[:limit]:
            def _t(tag):
                el = item.find(tag)
                return el.text or '' if el is not None else ''
            items.append({
                "title":   _t('title'),
                "link":    _t('guid') or _t('link'),
                "source":  _t('source'),
                "pubDate": _t('pubDate'),
            })
        return {"items": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/markets")
async def search_markets(q: str = "brent"):
    if EXCHANGE != "CAPITAL": return {"error": "only available when EXCHANGE=CAPITAL"}
    cst, token = await _capital_auth()
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.get(f"{CAPITAL_BASE}/api/v1/markets",
                        params={"searchTerm": q, "limit": 10},
                        headers=_cap_headers(cst, token))
    return r.json()


@app.post("/webhook", dependencies=[Depends(verify_secret)])
async def webhook(payload: AlertPayload):
    """
    Optional manual override â TradingView or any external signal.
    Still runs through the 7-indicator filter for safety (unless score check is bypassed).
    """
    log.info("ð© Webhook | action=%s | symbol=%s | price=%.4f",
             payload.action, payload.symbol, payload.price)
    sym = payload.symbol.upper()
    if sym not in ALLOWED_SYMBOLS:
        return {"status": "rejected", "reason": "symbol_not_allowed", "allowed": sorted(ALLOWED_SYMBOLS)}

    if payload.action in ("buy", "sell"):
        direction = "BUY" if payload.action == "buy" else "SELL"
        if trade_mgr.count() >= MAX_OPEN_TRADES:
            return {"status": "skipped", "reason": "max_trades_reached", "open": trade_mgr.count()}

        # Run indicator check before accepting webhook signal
        try:
            _, highs, lows, closes = await fetch_candles(sym)
            sig_dir, score, details = compute_signal(sym, closes, highs, lows)
            if sig_dir != direction:
                log.warning("â ï¸ Webhook %s %s REJECTED by indicators (indicators say %s, score=%d)",
                            direction, sym, sig_dir, score)
                return {"status": "rejected", "reason": "indicator_disagreement",
                        "webhook_direction": direction, "indicator_direction": sig_dir,
                        "score": score, "details": details}
            log.info("â Webhook confirmed by indicators (score=%d/7)", score)
        except Exception as e:
            log.warning("Indicator check failed for webhook (%s) â proceeding anyway", e)

        if EXCHANGE == "CAPITAL":
            order = await capital_open(sym, payload.qty, payload.price, direction, payload.sl, payload.tp)
            oid = order.get("dealId") or order.get("dealReference", "unknown")
        else:
            qty = payload.qty or DEFAULT_QTY
            order = await bybit_open(sym, qty, direction, payload.sl, payload.tp)
            oid = order.get("orderId", "unknown")

        trade_mgr.add(oid, sym, direction, payload.price, payload.sl, payload.tp)
        return {"status": "order_placed", "direction": direction, "order_id": oid,
                "open_trades": trade_mgr.count()}

    elif payload.action == "close":
        if EXCHANGE == "CAPITAL":
            result = await capital_close(sym)
        else:
            t = trade_mgr.get_by_symbol(sym)
            result = await bybit_close(sym, t.direction if t else "BUY")
        trade_mgr.remove_by_symbol(sym)
        return {"status": "position_closed", "open_trades": trade_mgr.count()}

    return {"status": "ignored"}


@app.delete("/positions/{epic}")
async def force_close(epic: str):
    """Manually force-close a position by epic code."""
    if EXCHANGE != "CAPITAL": return {"error": "only available when EXCHANGE=CAPITAL"}
    result = await capital_close(epic.upper())
    trade_mgr.remove_by_symbol(epic.upper())
    return {"status": "closed", "result": result}


@app.exception_handler(Exception)
async def global_error(request: Request, exc: Exception):
    log.exception("ð¥ Unhandled error: %s", exc)
    return JSONResponse(status_code=500, content={"status": "error", "detail": str(exc)})
