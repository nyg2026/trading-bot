"""
===============================================================================
  Walk-Forward Optimiser for UK100 / Brent Crude Trading Bot
  ---------------------------------------------------------------------------
  Fetches the last 30 days of 15-min OHLCV candles from Capital.com,
  grid-searches over ATR_SL_MULT x ATR_TP_MULT x MIN_SIGNAL_SCORE,
  scores each combination by Profit Factor + Win Rate, and returns
  the best parameter set so the live bot can self-update.

  Indicators replicated exactly as the live bot:
    EMA(9/21) cross  .  RSI(14)  .  MACD(12,26,9)  .  Bollinger(20,2)
    ATR(14)  .  Stochastic(14,3)  .  ADX(14, Wilder)

  Usage (standalone test):
    python backtest.py

  Usage (from FastAPI):
    from backtest import run_optimisation
    result = await run_optimisation(capital_base, cst, token)
===============================================================================
"""

from __future__ import annotations

import itertools
import json
import logging
import math
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

log = logging.getLogger("backtest")

# -- Parameter search space ---------------------------------------------------
PARAM_GRID = {
    "ATR_SL_MULT":       [0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.5],
    "ATR_TP_MULT":       [1.0,  1.5, 2.0,  2.5, 3.0,  3.5],
    "MIN_SIGNAL_SCORE":  [4,    5,   6,    7],
}

# Minimum trades a combo must generate to be considered valid
MIN_TRADES = 12

# Path where optimal params are persisted between restarts
PARAMS_FILE = "/tmp/optimal_params.json"


def _ema(prices: list, period: int) -> list:
    """Exponential moving average using the standard multiplier k = 2/(period+1)."""
    k = 2.0 / (period + 1)
    out = [prices[0]]
    for p in prices[1:]:
        out.append(p * k + out[-1] * (1.0 - k))
    return out


def _wilders(values: list, period: int) -> list:
    """Wilder's smoothing -- used by RSI, ATR, ADX."""
    out = [None] * (period - 1)
    seed = sum(values[:period]) / period
    out.append(seed)
    for v in values[period:]:
        out.append(out[-1] * (1 - 1.0 / period) + v * (1.0 / period))
    return out


def _rsi(closes: list, period: int = 14) -> list:
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains  = [max(d, 0.0) for d in deltas]
    losses = [max(-d, 0.0) for d in deltas]
    sg = _wilders(gains,  period)
    sl = _wilders(losses, period)
    out = [None]
    for g, l in zip(sg, sl):
        if g is None:
            out.append(None)
        elif l == 0:
            out.append(100.0)
        else:
            out.append(100.0 - 100.0 / (1.0 + g / l))
    return out


def _macd(closes: list) -> tuple:
    """Returns (macd_line, signal_line, histogram)."""
    e12 = _ema(closes, 12)
    e26 = _ema(closes, 26)
    macd_line = [a - b for a, b in zip(e12, e26)]
    signal    = _ema(macd_line, 9)
    hist      = [m - s for m, s in zip(macd_line, signal)]
    return macd_line, signal, hist


def _bollinger(closes: list, period: int = 20, k: float = 2.0):
    """Returns (upper, mid, lower) lists."""
    upper, mid, lower = [], [], []
    for i in range(len(closes)):
        if i < period - 1:
            upper.append(None); mid.append(None); lower.append(None)
        else:
            window = closes[i - period + 1: i + 1]
            m = sum(window) / period
            std = math.sqrt(sum((x - m) ** 2 for x in window) / period)
            mid.append(m)
            upper.append(m + k * std)
            lower.append(m - k * std)
    return upper, mid, lower


def _atr(highs: list, lows: list, closes: list, period: int = 14) -> list:
    """Wilder's ATR."""
    trs = [None]
    for i in range(1, len(closes)):
        tr = max(highs[i] - lows[i],
                 abs(highs[i] - closes[i - 1]),
                 abs(lows[i]  - closes[i - 1]))
        trs.append(tr)
    real_trs = [t for t in trs if t is not None]
    out = [None] * period
    seed = sum(real_trs[:period]) / period
    out.append(seed)
    for t in real_trs[period:]:
        out.append(out[-1] * (1 - 1.0 / period) + t * (1.0 / period))
    return out


def _stochastic(highs: list, lows: list, closes: list,
                k_period: int = 14, d_period: int = 3):
    """Returns (%K, %D) lists."""
    raw_k = []
    for i in range(len(closes)):
        if i < k_period - 1:
            raw_k.append(None)
        else:
            h = max(highs[i - k_period + 1: i + 1])
            l = min(lows[i - k_period + 1:  i + 1])
            raw_k.append(100.0 * (closes[i] - l) / (h - l) if h != l else 50.0)

    raw_d = []
    none_prefix = raw_k.count(None)
    for i in range(len(raw_k)):
        if raw_k[i] is None or i < none_prefix + d_period - 1:
            raw_d.append(None)
        else:
            window = [raw_k[j] for j in range(i - d_period + 1, i + 1) if raw_k[j] is not None]
            raw_d.append(sum(window) / len(window) if len(window) == d_period else None)

    return raw_k, raw_d


def _adx(highs: list, lows: list, closes: list, period: int = 14):
    """Wilder's ADX. Returns (adx, plus_di, minus_di) lists."""
    plus_dm  = []
    minus_dm = []
    tr_vals  = []

    for i in range(1, len(closes)):
        up   = highs[i]  - highs[i - 1]
        down = lows[i - 1] - lows[i]
        plus_dm.append(max(up, 0.0)   if up > down  else 0.0)
        minus_dm.append(max(down, 0.0) if down > up  else 0.0)
        tr_vals.append(max(highs[i] - lows[i],
                           abs(highs[i] - closes[i - 1]),
                           abs(lows[i]  - closes[i - 1])))

    def _ws(vals):
        if len(vals) < period:
            return [None] * len(vals)
        out = [None] * (period - 1)
        s = sum(vals[:period])
        out.append(s)
        for v in vals[period:]:
            out.append(out[-1] - out[-1] / period + v)
        return out

    str_ = _ws(tr_vals)
    spdm = _ws(plus_dm)
    smdm = _ws(minus_dm)

    adx_vals = [None]
    plus_di  = [None]
    minus_di = [None]
    dx_raw   = []

    for t, p, m in zip(str_, spdm, smdm):
        if t is None or t == 0:
            adx_vals.append(None); plus_di.append(None); minus_di.append(None)
            continue
        pdi = 100.0 * p / t
        mdi = 100.0 * m / t
        plus_di.append(pdi)
        minus_di.append(mdi)
        denom = pdi + mdi
        dx_raw.append(100.0 * abs(pdi - mdi) / denom if denom else 0.0)
        if len(dx_raw) < period:
            adx_vals.append(None)
        elif len(dx_raw) == period:
            adx_vals.append(sum(dx_raw) / period)
        else:
            adx_vals.append(adx_vals[-1] * (1 - 1.0 / period) + dx_raw[-1] * (1.0 / period))

    return adx_vals, plus_di, minus_di


def compute_signals_series(candles: list) -> list:
    """
    For each candle (from index ~40 onwards), compute buy_score, sell_score,
    and ATR -- exactly matching the live bot's 7-indicator logic.
    """
    closes = [c["close"] for c in candles]
    highs  = [c["high"]  for c in candles]
    lows   = [c["low"]   for c in candles]

    ema9   = _ema(closes, 9)
    ema21  = _ema(closes, 21)
    rsi14  = _rsi(closes, 14)
    _, _, macd_hist = _macd(closes)
    bb_up, bb_mid, bb_lo = _bollinger(closes, 20, 2.0)
    atr14  = _atr(highs, lows, closes, 14)
    stk, std = _stochastic(highs, lows, closes, 14, 3)
    adx14, pdi, mdi = _adx(highs, lows, closes, 14)

    result = []
    for i in range(40, len(candles)):
        atr = atr14[i] if i < len(atr14) else None
        if not atr or atr == 0:
            continue

        c = closes[i]
        buy, sell = 0, 0

        if ema9[i] > ema21[i]:  buy  += 1
        else:                    sell += 1

        r = rsi14[i] if i < len(rsi14) else None
        if r is not None:
            if r < 40:   buy  += 1
            elif r > 60: sell += 1

        h = macd_hist[i] if i < len(macd_hist) else None
        if h is not None:
            if h > 0:   buy  += 1
            elif h < 0: sell += 1

        mid = bb_mid[i] if i < len(bb_mid) else None
        if mid is not None:
            if c > mid: buy  += 1
            else:       sell += 1

        atr_pct = atr / c * 100
        if atr_pct >= 0.1:
            buy += 1

        k = stk[i] if i < len(stk) else None
        if k is not None:
            if k < 20:   buy  += 1
            elif k > 80: sell += 1

        a  = adx14[i] if i < len(adx14) else None
        pd = pdi[i]   if i < len(pdi)   else None
        md = mdi[i]   if i < len(mdi)   else None
        if a is not None and pd is not None and md is not None and a > 20:
            if pd > md: buy  += 1
            else:       sell += 1

        result.append({
            "idx":        i,
            "time":       candles[i].get("time", ""),
            "close":      c,
            "buy_score":  buy,
            "sell_score": sell,
            "atr":        atr,
        })

    return result


def backtest(signals: list, sl_mult: float, tp_mult: float, min_score: int) -> dict:
    """Simulate the bot's trading logic on a pre-computed signal series."""
    trades = []
    in_trade  = False
    direction = ""
    entry = sl = tp = 0.0

    for s in signals:
        if in_trade:
            if direction == "BUY":
                if s["close"] <= sl:
                    trades.append(sl - entry); in_trade = False
                elif s["close"] >= tp:
                    trades.append(tp - entry); in_trade = False
            else:
                if s["close"] >= sl:
                    trades.append(entry - sl); in_trade = False
                elif s["close"] <= tp:
                    trades.append(entry - tp); in_trade = False

        if not in_trade:
            bs, ss = s["buy_score"], s["sell_score"]
            if bs >= min_score and bs > ss:
                direction = "BUY";  entry = s["close"]
                sl = entry - sl_mult * s["atr"]
                tp = entry + tp_mult * s["atr"]
                in_trade = True
            elif ss >= min_score and ss > bs:
                direction = "SELL"; entry = s["close"]
                sl = entry + sl_mult * s["atr"]
                tp = entry - tp_mult * s["atr"]
                in_trade = True

    if len(trades) < 2:
        return {"trades": len(trades), "profit_factor": 0.0,
                "win_rate": 0.0, "expectancy": 0.0, "total_pnl_pts": 0.0}

    wins   = [t for t in trades if t > 0]
    losses = [t for t in trades if t <= 0]
    gross_profit = sum(wins)
    gross_loss   = abs(sum(losses))
    pf  = gross_profit / gross_loss if gross_loss else (99.0 if gross_profit else 0.0)
    wr  = len(wins) / len(trades) * 100
    avg_w = gross_profit / len(wins)   if wins   else 0.0
    avg_l = gross_loss   / len(losses) if losses else 0.0
    exp   = (wr / 100) * avg_w - (1 - wr / 100) * avg_l

    return {
        "trades":         len(trades),
        "wins":           len(wins),
        "losses":         len(losses),
        "win_rate":       round(wr, 1),
        "profit_factor":  round(pf, 3),
        "expectancy_pts": round(exp, 4),
        "total_pnl_pts":  round(sum(trades), 4),
    }


def grid_search(sig_uk100: list, sig_oil: list) -> list:
    """
    Test every parameter combination across both instruments.
    Scoring = 0.6 x combined_profit_factor + 0.4 x win_rate / 100
    Returns top-10 results sorted best-first.
    """
    combos = list(itertools.product(
        PARAM_GRID["ATR_SL_MULT"],
        PARAM_GRID["ATR_TP_MULT"],
        PARAM_GRID["MIN_SIGNAL_SCORE"],
    ))

    results = []
    for sl, tp, ms in combos:
        ms = int(ms)
        uk  = backtest(sig_uk100, sl, tp, ms)
        oil = backtest(sig_oil,   sl, tp, ms)
        total = uk["trades"] + oil["trades"]
        if total < MIN_TRADES:
            continue

        if total:
            cpf = (uk["profit_factor"]  * uk["trades"] +
                   oil["profit_factor"] * oil["trades"]) / total
        else:
            cpf = 0.0

        cwr = (uk["win_rate"] * uk["trades"] +
               oil["win_rate"] * oil["trades"]) / total if total else 0.0

        score = 0.6 * cpf + 0.4 * cwr / 100.0

        results.append({
            "ATR_SL_MULT":      sl,
            "ATR_TP_MULT":      tp,
            "MIN_SIGNAL_SCORE": ms,
            "score":            round(score,  4),
            "profit_factor":    round(cpf,    3),
            "win_rate":         round(cwr,    1),
            "total_trades":     total,
            "uk100":            uk,
            "oil_brent":        oil,
        })

    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:10]


async def fetch_candles(capital_base: str, cst: str, token: str,
                        epic: str, days: int = 30) -> list:
    """Fetch `days` days of 15-min OHLCV candles from Capital.com."""
    from_dt = (datetime.now(timezone.utc) - timedelta(days=days)).strftime(
        "%Y-%m-%dT%H:%M:%S")
    headers = {"X-SECURITY-TOKEN": token, "CST": cst}

    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.get(
            f"{capital_base}/api/v1/prices/{epic}",
            params={"resolution": "MINUTE_15", "max": 5000, "from": from_dt},
            headers=headers,
        )

    raw = r.json()
    prices = raw.get("prices", [])
    candles = []
    for p in prices:
        try:
            candles.append({
                "time":  p.get("snapshotTimeUTC", p.get("snapshotTime", "")),
                "open":  float(p["openPrice"]["bid"]),
                "high":  float(p["highPrice"]["bid"]),
                "low":   float(p["lowPrice"]["bid"]),
                "close": float(p["closePrice"]["bid"]),
            })
        except (KeyError, TypeError, ValueError):
            continue

    log.info("[BACKTEST] %s: fetched %d candles (%.1f days)",
             epic, len(candles), len(candles) / (8.5 * 4))
    return candles


async def run_optimisation(capital_base: str, cst: str, token: str,
                           days: int = 30) -> dict:
    """
    Full walk-forward optimisation run.
    Fetches data, computes signals, grid-searches, saves best params.
    """
    import time as _time
    t0 = _time.monotonic()

    log.info("[BACKTEST] Starting %d-day walk-forward optimisation ...", days)

    candles_uk  = await fetch_candles(capital_base, cst, token, "UK100",     days)
    candles_oil = await fetch_candles(capital_base, cst, token, "OIL_BRENT", days)

    if len(candles_uk) < 100 or len(candles_oil) < 100:
        return {"error": "Not enough candle data",
                "candles_uk100": len(candles_uk),
                "candles_oil":   len(candles_oil)}

    sig_uk  = compute_signals_series(candles_uk)
    sig_oil = compute_signals_series(candles_oil)
    log.info("[BACKTEST] Signal series: UK100=%d  OIL=%d", len(sig_uk), len(sig_oil))

    top10 = grid_search(sig_uk, sig_oil)
    if not top10:
        return {"error": "No valid parameter combinations found (too few trades)"}

    best = top10[0]
    log.info(
        "[BACKTEST] Best params: SL=%.2f  TP=%.2f  MIN_SCORE=%d  PF=%.3f  WR=%.1f%%  trades=%d",
        best["ATR_SL_MULT"], best["ATR_TP_MULT"], best["MIN_SIGNAL_SCORE"],
        best["profit_factor"], best["win_rate"], best["total_trades"],
    )

    optimal = {
        "ATR_SL_MULT":      best["ATR_SL_MULT"],
        "ATR_TP_MULT":      best["ATR_TP_MULT"],
        "MIN_SIGNAL_SCORE": best["MIN_SIGNAL_SCORE"],
        "last_optimised":   datetime.now(timezone.utc).isoformat(),
        "profit_factor":    best["profit_factor"],
        "win_rate":         best["win_rate"],
        "total_trades":     best["total_trades"],
    }
    try:
        with open(PARAMS_FILE, "w") as f:
            json.dump(optimal, f, indent=2)
        log.info("[BACKTEST] Saved optimal params to %s", PARAMS_FILE)
    except OSError as e:
        log.warning("[BACKTEST] Could not save params file: %s", e)

    elapsed = round(_time.monotonic() - t0, 1)
    return {
        "best":          optimal,
        "top10":         top10,
        "candles_uk100": len(candles_uk),
        "candles_oil":   len(candles_oil),
        "elapsed_secs":  elapsed,
    }


def load_optimal_params() -> dict:
    """
    Load persisted optimal params from disk.
    Returns empty dict if file doesn't exist (bot falls back to env vars).
    """
    try:
        with open(PARAMS_FILE) as f:
            data = json.load(f)
        log.info("[BACKTEST] Loaded optimal params: SL=%.2f TP=%.2f SCORE=%d",
                 data.get("ATR_SL_MULT", 0), data.get("ATR_TP_MULT", 0),
                 data.get("MIN_SIGNAL_SCORE", 0))
        return data
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


if __name__ == "__main__":
    import asyncio, os as _os

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    base  = _os.getenv("CAPITAL_BASE", "https://api-capital.backend-capital.com")
    email = _os.getenv("CAPITAL_EMAIL")
    pwd   = _os.getenv("CAPITAL_PASSWORD")
    key   = _os.getenv("CAPITAL_API_KEY")

    if not all([email, pwd, key]):
        print("Set CAPITAL_BASE / CAPITAL_EMAIL / CAPITAL_PASSWORD / CAPITAL_API_KEY")
        raise SystemExit(1)

    async def _main():
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post(
                f"{base}/api/v1/session",
                json={"identifier": email, "password": pwd},
                headers={"X-CAP-API-KEY": key},
            )
        cst   = r.headers.get("CST", "")
        token = r.headers.get("X-SECURITY-TOKEN", "")
        result = await run_optimisation(base, cst, token, days=30)
        print(json.dumps(result, indent=2))

    asyncio.run(_main())
