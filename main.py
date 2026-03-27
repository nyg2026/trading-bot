"""
Mean Reversion Trading Bot â Single-file version (easy deploy)
Receives TradingView JSON alerts â executes orders on Bybit or Capital.com
"""

from __future__ import annotations

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
#  CONFIG  â  all values come from environment variables (set in Railway)
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
EXCHANGE         = os.getenv("EXCHANGE", "CAPITAL").upper()  # BYBIT or CAPITAL
MAX_OPEN_TRADES  = int(os.getenv("MAX_OPEN_TRADES", "3"))
DEFAULT_QTY      = float(os.getenv("DEFAULT_QTY", "0.01"))   # fallback if RISK_PCT=0
RISK_PCT         = float(os.getenv("RISK_PCT", "10"))         # % of balance per trade (0 = disabled)
LEVERAGE         = float(os.getenv("LEVERAGE", "1"))          # broker leverage multiplier (e.g. 10 = 10x)
TP_PCT           = float(os.getenv("TP_PCT", "3"))            # take-profit % above entry (0 = disabled)
SL_PCT           = float(os.getenv("SL_PCT", "1.5"))          # stop-loss % below entry  (0 = disabled)
WEBHOOK_SECRET   = os.getenv("WEBHOOK_SECRET", "")

# Bybit
BYBIT_API_KEY    = os.getenv("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BYBIT_TESTNET    = os.getenv("BYBIT_TESTNET", "true").lower() == "true"

# Capital.com
CAPITAL_API_KEY  = os.getenv("CAPITAL_API_KEY", "")
CAPITAL_EMAIL    = os.getenv("CAPITAL_EMAIL", "")
CAPITAL_PASSWORD = os.getenv("CAPITAL_PASSWORD", "")
CAPITAL_DEMO     = os.getenv("CAPITAL_DEMO", "true").lower() == "true"

# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
#  LOGGING
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("bot")

# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
#  TRADE MANAGER  â  in-memory tracker (max trades safety guard)
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
@dataclass
class Trade:
    order_id: str
    symbol:   str
    entry:    float
    sl:       float | None
    tp:       float | None
    opened:   datetime = field(default_factory=lambda: datetime.now(timezone.utc))

class TradeManager:
    def __init__(self, max_trades: int = 3):
        self._trades: dict[str, Trade] = {}
        self._lock    = threading.Lock()
        self.max_trades = max_trades

    def add(self, order_id, symbol, entry, sl=None, tp=None):
        with self._lock:
            self._trades[order_id] = Trade(order_id, symbol, entry, sl, tp)

    def remove_by_symbol(self, symbol):
        with self._lock:
            keys = [k for k, v in self._trades.items() if v.symbol == symbol]
            for k in keys:
                del self._trades[k]

    def count(self):
        with self._lock:
            return len(self._trades)

    def all(self):
        with self._lock:
            return list(self._trades.values())

trade_mgr = TradeManager(max_trades=MAX_OPEN_TRADES)

# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
#  BYBIT V5  â  order execution
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
BYBIT_BASE = "https://api-testnet.bybit.com" if BYBIT_TESTNET else "https://api.bybit.com"

def _bybit_sign(body: dict, ts: str) -> str:
    payload = ts + BYBIT_API_KEY + "5000" + json.dumps(body, separators=(",", ":"))
    return hmac.new(BYBIT_API_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()

def _bybit_headers(body: dict) -> dict:
    ts = str(int(time.time() * 1000))
    return {
        "X-BAPI-API-KEY":     BYBIT_API_KEY,
        "X-BAPI-TIMESTAMP":   ts,
        "X-BAPI-SIGN":        _bybit_sign(body, ts),
        "X-BAPI-RECV-WINDOW": "5000",
        "Content-Type":       "application/json",
    }

async def bybit_buy(symbol: str, qty: float, sl=None, tp=None) -> dict:
    body: dict = {
        "category": "linear", "symbol": symbol,
        "side": "Buy", "orderType": "Market",
        "qty": str(qty), "timeInForce": "IOC",
        "reduceOnly": False, "closeOnTrigger": False, "positionIdx": 0,
    }
    if sl: body["stopLoss"] = str(round(sl, 6)); body["slTriggerBy"] = "LastPrice"
    if tp: body["takeProfit"] = str(round(tp, 6)); body["tpTriggerBy"] = "LastPrice"

    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.post(f"{BYBIT_BASE}/v5/order/create", json=body, headers=_bybit_headers(body))
    data = r.json()
    if data.get("retCode") != 0:
        raise RuntimeError(f"Bybit error {data.get('retCode')}: {data.get('retMsg')}")
    log.info("â Bybit order placed | orderId=%s", data["result"].get("orderId"))
    return data["result"]

async def bybit_close(symbol: str) -> dict:
    body: dict = {
        "category": "linear", "symbol": symbol,
        "side": "Sell", "orderType": "Market",
        "qty": "0", "timeInForce": "IOC",
        "reduceOnly": True, "closeOnTrigger": True, "positionIdx": 0,
    }
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.post(f"{BYBIT_BASE}/v5/order/create", json=body, headers=_bybit_headers(body))
    data = r.json()
    if data.get("retCode") != 0:
        raise RuntimeError(f"Bybit close error: {data.get('retMsg')}")
    return data["result"]

# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
#  CAPITAL.COM  â  order execution
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
CAPITAL_BASE  = "https://demo-api-capital.backend-capital.com" if CAPITAL_DEMO else "https://api-capital.backend-capital.com"
_cap_session: dict = {}

async def _capital_auth() -> tuple[str, str]:
    """Returns (CST, X-SECURITY-TOKEN). Auto-refreshes every 8 minutes."""
    if _cap_session.get("ts", 0) + 480 > time.time():
        return _cap_session["cst"], _cap_session["token"]

    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.post(
            f"{CAPITAL_BASE}/api/v1/session",
            json={"identifier": CAPITAL_EMAIL, "password": CAPITAL_PASSWORD, "encryptedPassword": False},
            headers={"X-CAP-API-KEY": CAPITAL_API_KEY, "Content-Type": "application/json"},
        )
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Capital.com auth failed [{r.status_code}]: {r.text[:200]}")

    _cap_session.update({"cst": r.headers["CST"], "token": r.headers["X-SECURITY-TOKEN"], "ts": time.time()})
    log.info("ð Capital.com session refreshed")
    return _cap_session["cst"], _cap_session["token"]

def _cap_headers(cst, token):
    return {"CST": cst, "X-SECURITY-TOKEN": token, "Content-Type": "application/json"}

async def _capital_get_balance() -> float:
    """Fetch available balance from the preferred Capital.com account."""
    cst, token = await _capital_auth()
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get(f"{CAPITAL_BASE}/api/v1/accounts", headers=_cap_headers(cst, token))
    accounts = r.json().get("accounts", [])
    # Use preferred account; fall back to first account
    for acc in sorted(accounts, key=lambda a: not a.get("preferred", False)):
        bal = acc.get("balance", {}).get("available", 0)
        if bal > 0:
            log.info("ð· Balance: %.2f %s (account: %s)", bal, acc.get("currency", "?"), acc.get("accountType", "?"))
            return float(bal)
    return 0.0

def _calc_qty(price: float, override_qty: float | None) -> float:
    """Return trade size: payload qty > RISK_PCT sizing > DEFAULT_QTY fallback."""
    if override_qty and override_qty > 0:
        return override_qty
    if RISK_PCT > 0:
        # qty is calculated at trade time using live balance â placeholder here;
        # actual call happens in capital_buy where balance is fetched
        return 0.0   # signal to fetch balance
    return DEFAULT_QTY

async def capital_buy(symbol: str, qty: float | None, price: float, sl=None, tp=None) -> dict:
    cst, token = await _capital_auth()

    # ââ Position sizing âââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    if qty and qty > 0:
        # Explicit qty from TradingView alert â use as-is
        final_qty = qty
        log.info("ð Sizing: manual qty=%.6f", final_qty)
    elif RISK_PCT > 0:
        # Auto-size: allocate RISK_PCT% of available balance, scaled by leverage
        balance = await _capital_get_balance()
        trade_value = balance * RISK_PCT / 100.0 * LEVERAGE
        final_qty = round(trade_value / price, 4)
        log.info("ð Sizing: balance=%.2f Ã %.1f%% Ã %.0fx leverage = Â£%.2f / price=%.4f â qty=%.6f",
                 balance, RISK_PCT, LEVERAGE, trade_value, price, final_qty)
    else:
        final_qty = DEFAULT_QTY
        log.info("ð Sizing: default qty=%.6f", final_qty)

    # ââ TP / SL safety net ââââââââââââââââââââââââââââââââââââââââââââââââââââ
    # Use levels from TradingView alert if provided; fall back to env-var %
    final_tp = tp if tp else (round(price * (1 + TP_PCT / 100), 6) if TP_PCT > 0 else None)
    final_sl = sl if sl else (round(price * (1 - SL_PCT / 100), 6) if SL_PCT > 0 else None)
    if final_tp:
        log.info("ð¯ TP: %.4f (%s)", final_tp, "from alert" if tp else f"auto {TP_PCT}%")
    if final_sl:
        log.info("ð SL: %.4f (%s)", final_sl, "from alert" if sl else f"auto {SL_PCT}%")

    body: dict = {"epic": symbol, "direction": "BUY", "size": final_qty,
                  "guaranteedStop": False, "trailingStop": False}
    if final_sl: body["stopLevel"]   = final_sl
    if final_tp: body["profitLevel"] = final_tp

    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.post(f"{CAPITAL_BASE}/api/v1/positions", json=body, headers=_cap_headers(cst, token))
    data = r.json()
    log.info("ð Capital.com position response [%d]: %s", r.status_code, data)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Capital.com error [{r.status_code}]: {data}")
    # Check for deal-level rejection (Capital.com returns 200 even for rejected deals)
    deal_status = data.get("status", "")
    if deal_status in ("REJECTED", "DELETED"):
        reason = data.get("reason", "unknown")
        raise RuntimeError(f"Capital.com deal rejected: status={deal_status}, reason={reason}, full={data}")
    log.info("â Capital.com position opened | dealId=%s | status=%s",
             data.get("dealId") or data.get("dealReference"), deal_status)
    return data

async def capital_close(symbol: str) -> dict:
    cst, token = await _capital_auth()
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get(f"{CAPITAL_BASE}/api/v1/positions", headers=_cap_headers(cst, token))
    positions = r.json().get("positions", [])
    deal_id = next((p["position"]["dealId"] for p in positions if p["market"]["epic"] == symbol), None)
    if not deal_id:
        log.warning("No open position found for %s", symbol)
        return {"status": "not_found"}
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.delete(f"{CAPITAL_BASE}/api/v1/positions/{deal_id}", headers=_cap_headers(cst, token))
    log.info("ð Capital.com position closed | dealId=%s", deal_id)
    return r.json()

# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
#  FASTAPI APP
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("ð Trading Bot started | exchange=%s | max_trades=%d | risk_pct=%.1f%% | leverage=%.0fx | tp=%.1f%% | sl=%.1f%% | demo/testnet=%s",
             EXCHANGE, MAX_OPEN_TRADES, RISK_PCT, LEVERAGE, TP_PCT, SL_PCT,
             CAPITAL_DEMO if EXCHANGE == "CAPITAL" else BYBIT_TESTNET)
    yield
    log.info("ð Trading Bot shutting down")

app = FastAPI(title="MR Trading Bot", version="1.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET", "POST"], allow_headers=["*"])

# ââ Models ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
class AlertPayload(BaseModel):
    action:   Literal["buy", "sell", "close"]
    symbol:   str
    exchange: str   = "CAPITAL"
    price:    float = Field(..., gt=0)
    sl:       float | None = None
    tp:       float | None = None
    rsi:      float | None = None
    bb_lower: float | None = None
    bb_mid:   float | None = None
    qty:      float | None = None

# ââ Security ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
def verify_secret(x_webhook_secret: str | None = Header(default=None)):
    if WEBHOOK_SECRET and x_webhook_secret != WEBHOOK_SECRET:
        log.warning("â ï¸ Rejected webhook â bad secret")
        raise HTTPException(status_code=403, detail="Invalid webhook secret")

# ââ Routes ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
@app.get("/health")
async def health():
    return {
        "status":      "ok",
        "exchange":    EXCHANGE,
        "mode":        "demo/testnet" if (CAPITAL_DEMO if EXCHANGE == "CAPITAL" else BYBIT_TESTNET) else "LIVE",
        "open_trades": trade_mgr.count(),
        "max_trades":  MAX_OPEN_TRADES,
        "risk_pct":    RISK_PCT,
        "leverage":    LEVERAGE,
        "tp_pct":      TP_PCT,
        "sl_pct":      SL_PCT,
    }

@app.post("/webhook", dependencies=[Depends(verify_secret)])
async def webhook(payload: AlertPayload):
    log.info("ð© Alert | action=%s | symbol=%s | price=%.4f | rsi=%s",
             payload.action, payload.symbol, payload.price, payload.rsi)

    if payload.action == "buy":
        if trade_mgr.count() >= MAX_OPEN_TRADES:
            log.warning("â Skipped â max trades (%d) reached", MAX_OPEN_TRADES)
            return {"status": "skipped", "reason": "max_trades_reached", "open": trade_mgr.count()}

        if EXCHANGE == "BYBIT":
            qty = payload.qty or DEFAULT_QTY
            order = await bybit_buy(payload.symbol, qty, payload.sl, payload.tp)
            oid = order.get("orderId", "unknown")
        else:
            # Capital.com: qty=None triggers RISK_PCT auto-sizing inside capital_buy
            order = await capital_buy(payload.symbol, payload.qty, payload.price, payload.sl, payload.tp)
            oid = order.get("dealId") or order.get("dealReference", "unknown")

        trade_mgr.add(oid, payload.symbol, payload.price, payload.sl, payload.tp)
        return {"status": "order_placed", "order_id": oid, "open_trades": trade_mgr.count()}

    elif payload.action in ("close", "sell"):
        if EXCHANGE == "BYBIT":
            result = await bybit_close(payload.symbol)
        else:
            result = await capital_close(payload.symbol)
        trade_mgr.remove_by_symbol(payload.symbol)
        return {"status": "position_closed", "open_trades": trade_mgr.count()}

    return {"status": "ignored"}

@app.exception_handler(Exception)
async def global_error(request: Request, exc: Exception):
    log.exception("ð¥ Unhandled error: %s", exc)
    return JSONResponse(status_code=500, content={"status": "error", "detail": str(exc)})
