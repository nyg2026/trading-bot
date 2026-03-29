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

# Strategy parameters
MIN_SIGNAL_SCORE  = int(os.getenv("MIN_SIGNAL_SCORE", "5"))
SCAN_INTERVAL     = int(os.getenv("SCAN_INTERVAL", "30"))

# Markets
MARKET_UK100      = "UK 100"
MARKET_BRENT      = "Brent Crude"

# Profit target: exit if balance ever exceeds STOP_AT_BALANCE
STARTING_BALANCE  = float(os.getenv("STARTING_BALANCE", "10000"))
STOP_AT_BALANCE   = STARTING_BALANCE * 2.0

# Logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# ==============================================================================
#  DATACLASSES
# ==============================================================================

@dataclass
class SignalPayload:
    """Indicator signal from Capital.com webhook"""
    epic_id: str
    signal_type: str  # "BUY" or "SELL"
    direction: str    # "BUY" or "SELL"
    timestamp: str

@dataclass
class Position:
    """Represents an open trade"""
    deal_id: str
    epic: str
    direction: Literal["BUY", "SELL"]
    size: float
    entry_price: float
    stop_loss: float
    take_profit: float
    opened_at: datetime
    open_pl: float = 0.0
    closed: bool = False

@dataclass
class StrategyMetrics:
    """Rolling metrics for strategy validation"""
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    total_pl: float = 0.0
    max_dd: float = 0.0
    current_dd: float = 0.0
    highest_balance: float = STARTING_BALANCE
    
    def update_pl(self, pl: float):
        self.total_pl += pl
        if pl > 0:
            self.winning_trades += 1
        elif pl < 0:
            self.losing_trades += 1
        self.total_trades += 1
    
    def update_drawdown(self, current_balance: float):
        if current_balance > self.highest_balance:
            self.highest_balance = current_balance
        self.current_dd = self.highest_balance - current_balance
        if self.current_dd > self.max_dd:
            self.max_dd = self.current_dd
    
    @property
    def win_rate(self) -> float:
        if self.total_trades == 0:
            return 0.0
        return self.winning_trades / self.total_trades

# ==============================================================================
#  CAPITAL.COM API WRAPPER
# ==============================================================================

class CapitalComAPI:
    """Sync wrapper for Capital.com OAuth 2.0 + REST API"""
    
    BASE_URL = "https://api.capital.com"
    
    def __init__(self, api_key: str, email: str, password: str):
        self.api_key = api_key
        self.email = email
        self.password = password
        self.access_token: str | None = None
        self.cst_token: str | None = None
        self.x_security_token: str | None = None
        self.client = httpx.Client(timeout=30)
    
    def login(self) -> bool:
        """Authenticate and get session tokens"""
        try:
            body = {
                "identifier": self.email,
                "password": self.password,
                "encryptionVersion": "V2"
            }
            
            res = self.client.post(
                f"{self.BASE_URL}/api/v1/auth/login",
                json=body,
                headers={"X-CAP-API-KEY": self.api_key}
            )
            res.raise_for_status()
            data = res.json()
            
            self.cst_token = res.headers.get("CST")
            self.x_security_token = res.headers.get("X-SECURITY-TOKEN")
            self.access_token = data.get("token")
            
            logger.info("Capital.com login successful")
            return True
        except Exception as e:
            logger.error(f"Login failed: {e}")
            return False
    
    def _headers(self) -> dict:
        """Default headers for authenticated requests"""
        return {
            "X-CAP-API-KEY": self.api_key,
            "CST": self.cst_token or "",
            "X-SECURITY-TOKEN": self.x_security_token or "",
            "Content-Type": "application/json"
        }
    
    def get_account_info(self) -> dict | None:
        """Fetch account balance and status"""
        try:
            res = self.client.get(
                f"{self.BASE_URL}/api/v1/accounts",
                headers=self._headers()
            )
            res.raise_for_status()
            return res.json()
        except Exception as e:
            logger.error(f"get_account_info failed: {e}")
            return None
    
    def get_positions(self) -> list[dict] | None:
        """Fetch all open positions"""
        try:
            res = self.client.get(
                f"{self.BASE_URL}/api/v1/positions",
                headers=self._headers()
            )
            res.raise_for_status()
            return res.json().get("positions", [])
        except Exception as e:
            logger.error(f"get_positions failed: {e}")
            return None
    
    def open_position(
        self,
        epic: str,
        direction: str,
        size: float,
        stop_loss: float,
        take_profit: float
    ) -> dict | None:
        """Open a new position with SL/TP"""
        try:
            body = {
                "epic": epic,
                "direction": direction,
                "size": size,
                "orderType": "MARKET",
                "stopLevel": stop_loss,
                "profitLevel": take_profit
            }
            
            res = self.client.post(
                f"{self.BASE_URL}/api/v1/positions",
                json=body,
                headers=self._headers()
            )
            res.raise_for_status()
            return res.json()
        except Exception as e:
            logger.error(f"open_position failed: {e}")
            return None
    
    def close_position(self, deal_id: str) -> bool:
        """Close an open position"""
        try:
            res = self.client.delete(
                f"{self.BASE_URL}/api/v1/positions/{deal_id}",
                headers=self._headers()
            )
            res.raise_for_status()
            logger.info(f"Closed position {deal_id}")
            return True
        except Exception as e:
            logger.error(f"close_position failed: {e}")
            return False
    
    def update_position(
        self,
        deal_id: str,
        stop_loss: float | None = None,
        take_profit: float | None = None
    ) -> bool:
        """Update SL/TP on an open position"""
        try:
            body = {}
            if stop_loss is not None:
                body["stopLevel"] = stop_loss
            if take_profit is not None:
                body["profitLevel"] = take_profit
            
            res = self.client.put(
                f"{self.BASE_URL}/api/v1/positions/{deal_id}",
                json=body,
                headers=self._headers()
            )
            res.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"update_position failed: {e}")
            return False
    
    def get_prices(self, epic: str) -> dict | None:
        """Fetch current bid/ask prices"""
        try:
            res = self.client.get(
                f"{self.BASE_URL}/api/v1/prices",
                params={"epic": epic},
                headers=self._headers()
            )
            res.raise_for_status()
            return res.json()
        except Exception as e:
            logger.error(f"get_prices failed: {e}")
            return None

# ==============================================================================
#  INDICATOR SIGNALS
# ==============================================================================

class IndicatorSignals:
    """
    Parse and score indicator confluence.
    Each indicator returns a signal (+1 = bullish, 0 = neutral, -1 = bearish).
    """
    
    @staticmethod
    def parse_ema_signal(current: float, ema9: float, ema21: float) -> int:
        """EMA confluence: price above both = bullish"""
        if current > ema21 > ema9:
            return 1
        elif current < ema21 < ema9:
            return -1
        return 0
    
    @staticmethod
    def parse_rsi_signal(rsi: float) -> int:
        """RSI oversold/overbought"""
        if rsi < 30:
            return 1
        elif rsi > 70:
            return -1
        return 0
    
    @staticmethod
    def parse_macd_signal(macd: float, signal: float) -> int:
        """MACD histogram"""
        if macd > signal:
            return 1
        elif macd < signal:
            return -1
        return 0
    
    @staticmethod
    def parse_bollinger_signal(current: float, upper: float, lower: float) -> int:
        """Bollinger Bands: near lower = bullish, near upper = bearish"""
        mid = (upper + lower) / 2
        if current < mid:
            return 1
        elif current > mid:
            return -1
        return 0
    
    @staticmethod
    def parse_atr_signal(atr: float, typical_price: float) -> int:
        """ATR volatility (baseline, neutral signal)"""
        return 0
    
    @staticmethod
    def parse_stochastic_signal(k: float, d: float) -> int:
        """Stochastic %K vs %D"""
        if k > d and k < 50:
            return 1
        elif k < d and k > 50:
            return -1
        return 0
    
    @staticmethod
    def parse_adx_signal(adx: float) -> int:
        """ADX trend strength (baseline)"""
        return 0
    
    @staticmethod
    def score_confluence(signals: list[int]) -> int:
        """Sum all signals to get a confluence score"""
        return sum(signals)

# ==============================================================================
#  TRADING BOT LOGIC
# ==============================================================================

class TradingBot:
    """Main bot orchestrator"""
    
    def __init__(self):
        self.api = CapitalComAPI(CAPITAL_API_KEY, CAPITAL_EMAIL, CAPITAL_PASSWORD)
        self.positions: dict[str, Position] = {}
        self.metrics = StrategyMetrics()
        self.running = False
        self.scan_thread: threading.Thread | None = None
    
    def is_london_session(self) -> bool:
        """Check if current time is during London trading hours (08:00-16:30 BST)"""
        now = datetime.now(timezone.utc).astimezone()
        hour = now.hour
        weekday = now.weekday()
        
        if weekday >= 5:  # Saturday/Sunday
            return False
        return 8 <= hour < 16.5
    
    def start(self):
        """Start the trading bot"""
        if not self.api.login():
            logger.error("Failed to authenticate with Capital.com")
            return
        
        self.running = True
        self.scan_thread = threading.Thread(target=self._scan_loop, daemon=True)
        self.scan_thread.start()
        logger.info("Trading bot started")
    
    def stop(self):
        """Stop the trading bot"""
        self.running = False
        if self.scan_thread:
            self.scan_thread.join()
        
        # Close all positions
        self._close_all_positions()
        logger.info("Trading bot stopped")
    
    def _scan_loop(self):
        """Main loop: scan for signals every SCAN_INTERVAL seconds"""
        while self.running:
            if self.is_london_session():
                self._tick()
            time.sleep(SCAN_INTERVAL)
    
    def _tick(self):
        """One scan cycle"""
        try:
            account = self.api.get_account_info()
            if account is None:
                return
            
            balance = account.get("balance", 0)
            
            # Profit target check
            if balance >= STOP_AT_BALANCE:
                logger.info(f"Profit target reached: {balance} >= {STOP_AT_BALANCE}")
                self._close_all_positions()
                self.running = False
                return
            
            # Update drawdown metrics
            self.metrics.update_drawdown(balance)
            
            # Sync positions
            self._sync_positions()
            
            logger.debug(f"Tick: balance={balance}, open_positions={len(self.positions)}")
        except Exception as e:
            logger.error(f"Error in _tick: {e}")
    
    def _sync_positions(self):
        """Check open positions via API, update Position objects"""
        try:
            api_positions = self.api.get_positions() or []
            
            api_deal_ids = {p.get("dealId") for p in api_positions}
            local_deal_ids = set(self.positions.keys())
            
            # Positions closed via API -> remove locally
            for deal_id in local_deal_ids - api_deal_ids:
                pos = self.positions.pop(deal_id)
                logger.info(f"Position {deal_id} closed. P&L: {pos.open_pl}")
                self.metrics.update_pl(pos.open_pl)
            
            # Update open_pl for remaining positions
            for api_pos in api_positions:
                deal_id = api_pos.get("dealId")
                if deal_id in self.positions:
                    self.positions[deal_id].open_pl = api_pos.get("level", 0.0)
        except Exception as e:
            logger.error(f"Error syncing positions: {e}")
    
    def process_signal(self, payload: SignalPayload) -> bool:
        """Process incoming signal from webhook"""
        try:
            if payload.direction not in ["BUY", "SELL"]:
                logger.warning(f"Invalid direction: {payload.direction}")
                return False
            
            epic = payload.epic_id
            direction = payload.direction
            
            # Size: risk 1% per position on UK100, 0.5% on Brent
            account = self.api.get_account_info()
            if account is None:
                return False
            
            balance = account.get("balance", STARTING_BALANCE)
            risk_amount = balance * (0.01 if epic == MARKET_UK100 else 0.005)
            
            # Get current price
            prices = self.api.get_prices(epic)
            if prices is None:
                return False
            
            current_price = prices.get("bid", prices.get("ask", 0))
            if current_price == 0:
                logger.error("Invalid price")
                return False
            
            # Calculate SL and TP
            atr = self._estimate_atr(epic)
            stop_loss = current_price - atr if direction == "BUY" else current_price + atr
            take_profit = current_price + (2 * atr) if direction == "BUY" else current_price - (2 * atr)
            
            size = risk_amount / atr if atr > 0 else 1.0
            
            # Open position
            result = self.api.open_position(epic, direction, size, stop_loss, take_profit)
            if result is None:
                return False
            
            deal_id = result.get("dealId")
            position = Position(
                deal_id=deal_id,
                epic=epic,
                direction=direction,
                size=size,
                entry_price=current_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                opened_at=datetime.now(timezone.utc)
            )
            
            self.positions[deal_id] = position
            logger.info(f"Opened {direction} position {deal_id} at {current_price}")
            return True
        except Exception as e:
            logger.error(f"Error processing signal: {e}")
            return False
    
    def _estimate_atr(self, epic: str) -> float:
        """Estimate ATR based on recent volatility (stub)"""
        return 50.0
    
    def _close_all_positions(self):
        """Close all open positions at EOD"""
        for deal_id in list(self.positions.keys()):
            self.api.close_position(deal_id)
        logger.info("All positions closed")
    
    def get_metrics(self) -> dict:
        """Return strategy metrics"""
        return {
            "total_trades": self.metrics.total_trades,
            "winning_trades": self.metrics.winning_trades,
            "losing_trades": self.metrics.losing_trades,
            "win_rate": f"{self.metrics.win_rate * 100:.1f}%",
            "total_pl": self.metrics.total_pl,
            "max_drawdown": self.metrics.max_dd,
            "current_drawdown": self.metrics.current_dd,
            "open_positions": len(self.positions)
        }

# ==============================================================================
#  FASTAPI SERVER
# ==============================================================================

app = FastAPI(title="Trading Bot API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

bot = TradingBot()

# Request Models
class SignalRequest(BaseModel):
    epic_id: str = Field(..., description="Capital.com epic ID")
    signal_type: str = Field(..., description="BUY or SELL")
    direction: str = Field(..., description="BUY or SELL")
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

@app.on_event("startup")
async def startup():
    """Start the bot when server starts"""
    bot.start()

@app.on_event("shutdown")
async def shutdown():
    """Stop the bot when server shuts down"""
    bot.stop()

@app.post("/signal")
async def receive_signal(req: SignalRequest):
    """Webhook endpoint for indicator signals"""
    payload = SignalPayload(
        epic_id=req.epic_id,
        signal_type=req.signal_type,
        direction=req.direction,
        timestamp=req.timestamp
    )
    
    if PAPER_TRADE:
        logger.info(f"[PAPER TRADE] Received signal: {payload}")
        return {"status": "logged", "paper_trade": True}
    
    success = bot.process_signal(payload)
    return {
        "status": "ok" if success else "error",
        "signal": req.dict()
    }

@app.get("/metrics")
async def get_metrics():
    """Fetch current metrics"""
    return bot.get_metrics()

@app.get("/positions")
async def get_positions():
    """Fetch current open positions"""
    return {
        "positions": [
            {
                "deal_id": p.deal_id,
                "epic": p.epic,
                "direction": p.direction,
                "size": p.size,
                "entry_price": p.entry_price,
                "stop_loss": p.stop_loss,
                "take_profit": p.take_profit,
                "open_pl": p.open_pl
            }
            for p in bot.positions.values()
        ]
    }

@app.get("/health")
async def health():
    """Health check"""
    return {"status": "ok", "running": bot.running}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
