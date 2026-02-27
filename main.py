"""
HIGH LEVERAGE TOP20 SCANNER — Grok 4.1 Thinking (Cross Margin)
Enhanced with technical indicators, multi-timeframe analysis, order book data
Risk-based position sizing: size = risk_budget / SL_distance
Scans top 20 coins by volume, strictly one position at a time
Binance Demo Trading — runs 24/7 on Render.com
"""

import os
import sys
import json
import asyncio
import math
import time
import re
import traceback
from datetime import datetime, UTC
from openai import AsyncOpenAI
import ccxt
from dotenv import load_dotenv

print("=== HIGH LEVERAGE TOP20 SCANNER STARTING ===", flush=True)
print("Grok 4.1 Thinking + Risk-Based Sizing + Cross Margin", flush=True)

load_dotenv()

# === SETTINGS ===
XAI_API_KEY = os.getenv("XAI_API_KEY")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")
INTERVAL_MINUTES = int(os.getenv("INTERVAL_MINUTES", "15"))
MAX_RISK_PERCENT = float(os.getenv("MAX_RISK_PERCENT", "2.5"))
MAX_MARGIN_PERCENT = float(os.getenv("MAX_MARGIN_PERCENT", "50"))  # Max % of balance used as margin

# === CHECK KEYS ===
missing = []
if not XAI_API_KEY:
    missing.append("XAI_API_KEY")
if not BINANCE_API_KEY:
    missing.append("BINANCE_API_KEY")
if not BINANCE_API_SECRET:
    missing.append("BINANCE_API_SECRET")
if missing:
    print(f"❌ FATAL: Missing: {', '.join(missing)}", flush=True)
    sys.exit(1)

client = AsyncOpenAI(api_key=XAI_API_KEY, base_url="https://api.x.ai/v1")

# PUBLIC exchange — live Binance for market data
public_exchange = ccxt.binance({
    'enableRateLimit': True,
    'options': {'defaultType': 'future'},
})
print("✅ Connected to Binance LIVE for market data", flush=True)

# PRIVATE exchange — demo Binance for trading
trading_exchange = ccxt.binance({
    'apiKey': BINANCE_API_KEY,
    'secret': BINANCE_API_SECRET,
    'enableRateLimit': True,
    'options': {
        'defaultType': 'future',
        'warnOnFetchOpenOrdersWithoutSymbol': False,
    },
})
trading_exchange.enable_demo_trading(True)
print("✅ Connected to Binance DEMO for trading (cross margin)", flush=True)

# Runtime blacklist — symbols where SL/TP placement fails on ALL endpoints
_blacklisted_symbols = set()


# ============================================================
#  MARKET CACHE
# ============================================================

_markets_cache = None
_markets_cache_time = 0
MARKETS_CACHE_TTL = 3600


def get_cached_markets():
    global _markets_cache, _markets_cache_time
    now = time.time()
    if _markets_cache is None or (now - _markets_cache_time) > MARKETS_CACHE_TTL:
        attempt = 0
        while True:
            attempt += 1
            try:
                _markets_cache = public_exchange.load_markets()
                trading_exchange.load_markets()
                _markets_cache_time = time.time()
                print(f"   🔄 Markets refreshed ({len(_markets_cache)} loaded)", flush=True)
                return _markets_cache
            except (ccxt.DDoSProtection, ccxt.ExchangeNotAvailable, ccxt.RequestTimeout) as e:
                # Parse ban duration if available
                ban_msg = str(e)
                wait = min(300, 60 * attempt)  # Cap at 5 min

                # Check for "banned until" timestamp in error message
                if 'banned until' in ban_msg:
                    try:
                        ban_ts = int(''.join(c for c in ban_msg.split('banned until')[1].split('.')[0].strip() if c.isdigit()))
                        ban_remaining = max(0, (ban_ts / 1000) - time.time())
                        if ban_remaining > 0:
                            wait = min(ban_remaining + 30, 600)  # Wait until ban lifts + 30s buffer, max 10 min
                    except Exception:
                        pass

                print(f"   ⚠️ Binance rate limit/ban (attempt {attempt}): {ban_msg[:100]}", flush=True)
                print(f"   ⏳ Waiting {wait:.0f}s before retry...", flush=True)
                time.sleep(wait)

                # If we have a stale cache, use it rather than blocking forever
                if _markets_cache is not None and attempt >= 3:
                    print(f"   ⚠️ Using stale market cache", flush=True)
                    return _markets_cache
            except Exception as e:
                print(f"   ❌ Markets load error: {e}", flush=True)
                if _markets_cache is not None:
                    return _markets_cache
                time.sleep(30)
                if attempt >= 10:
                    print(f"   ❌ Failed to load markets after {attempt} attempts", flush=True)
                    return _markets_cache  # Will be None on first run
    return _markets_cache


# ============================================================
#  SYMBOL NORMALIZATION
# ============================================================

def normalize_symbol(symbol):
    if not symbol:
        return symbol
    if '/' in symbol and ':' in symbol:
        return symbol
    clean = symbol.replace(':USDT', '').replace(':usdt', '').replace('/', '')
    markets = public_exchange.markets or {}
    if not markets:
        # Markets not loaded yet — try basic normalization
        if clean.endswith('USDT'):
            base = clean[:-4]
            return f"{base}/USDT:USDT"
        return symbol
    if clean in markets:
        return clean
    if clean.endswith('USDT'):
        base = clean[:-4]
        unified = f"{base}/USDT:USDT"
        if unified in markets:
            return unified
    for mkt_symbol, mkt in markets.items():
        if mkt.get('id') == clean:
            return mkt_symbol
    return symbol


def symbol_to_binance_raw(symbol):
    return symbol.replace('/', '').replace(':USDT', '').replace(':usdt', '')


# ============================================================
#  ORDER PRECISION
# ============================================================

def get_min_notional(symbol):
    try:
        market = trading_exchange.market(symbol)
        cost_min = market.get('limits', {}).get('cost', {}).get('min')
        if cost_min is not None:
            return float(cost_min)
    except Exception:
        pass
    return 5.0


def round_amount(amount, symbol):
    try:
        return trading_exchange.amount_to_precision(symbol, amount)
    except Exception:
        return round(amount, 3)


def round_price(price, symbol):
    try:
        return float(trading_exchange.price_to_precision(symbol, price))
    except Exception:
        return price


# ============================================================
#  ROBUST JSON PARSING
# ============================================================

def parse_grok_json(text):
    if not text:
        return None
    text = text.strip()
    if "```json" in text:
        match = re.search(r'```json\s*(.*?)\s*```', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass
    if "```" in text:
        match = re.search(r'```\s*(.*?)\s*```', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass
    match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


# ============================================================
#  RETRY HELPER
# ============================================================

async def retry_async(func, retries=2, delay=5, label=""):
    last_error = None
    for attempt in range(retries):
        try:
            return await func()
        except (ccxt.NetworkError, ccxt.ExchangeNotAvailable, ccxt.RequestTimeout,
                ccxt.DDoSProtection, ConnectionError, TimeoutError) as e:
            last_error = e
            if attempt < retries - 1:
                print(f"   🔁 Retry {attempt + 1}/{retries - 1} for {label}: {e}", flush=True)
                await asyncio.sleep(delay)
            else:
                raise
        except Exception:
            raise
    raise last_error


def retry_sync(func, retries=2, delay=5, label=""):
    last_error = None
    for attempt in range(retries):
        try:
            return func()
        except (ccxt.NetworkError, ccxt.ExchangeNotAvailable, ccxt.RequestTimeout,
                ccxt.DDoSProtection, ConnectionError, TimeoutError) as e:
            last_error = e
            if attempt < retries - 1:
                print(f"   🔁 Retry {attempt + 1}/{retries - 1} for {label}: {e}", flush=True)
                time.sleep(delay)
            else:
                raise
        except Exception:
            raise
    raise last_error


# ============================================================
#  TECHNICAL INDICATORS
# ============================================================

def calc_rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50.0
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [d if d > 0 else 0 for d in deltas]
    losses = [-d if d < 0 else 0 for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calc_ema(values, period):
    if len(values) < period:
        return values[-1] if values else 0
    multiplier = 2 / (period + 1)
    ema = sum(values[:period]) / period
    for val in values[period:]:
        ema = (val - ema) * multiplier + ema
    return ema


def calc_atr(highs, lows, closes, period=14):
    if len(closes) < period + 1:
        return 0
    trs = []
    for i in range(1, len(closes)):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        trs.append(tr)
    if len(trs) < period:
        return sum(trs) / len(trs) if trs else 0
    return sum(trs[-period:]) / period


def calc_vwap(highs, lows, closes, volumes):
    if not closes or not volumes:
        return closes[-1] if closes else 0
    typical_prices = [(h + l + c) / 3 for h, l, c in zip(highs, lows, closes)]
    cum_tp_vol = sum(tp * v for tp, v in zip(typical_prices, volumes))
    cum_vol = sum(volumes)
    return cum_tp_vol / cum_vol if cum_vol > 0 else closes[-1]


def calc_bollinger(closes, period=20, std_dev=2):
    if len(closes) < period:
        return closes[-1], closes[-1], closes[-1]
    sma = sum(closes[-period:]) / period
    variance = sum((c - sma) ** 2 for c in closes[-period:]) / period
    std = math.sqrt(variance)
    return sma - std_dev * std, sma, sma + std_dev * std


def analyze_candles(candles):
    if not candles or len(candles) < 20:
        return None
    opens = [c[1] for c in candles]
    highs = [c[2] for c in candles]
    lows = [c[3] for c in candles]
    closes = [c[4] for c in candles]
    volumes = [c[5] for c in candles]
    current = closes[-1]
    rsi = calc_rsi(closes)
    ema9 = calc_ema(closes, 9)
    ema21 = calc_ema(closes, 21)
    ema50 = calc_ema(closes, 50) if len(closes) >= 50 else ema21
    atr = calc_atr(highs, lows, closes)
    atr_pct = (atr / current * 100) if current > 0 else 0
    vwap = calc_vwap(highs, lows, closes, volumes)
    bb_lower, bb_mid, bb_upper = calc_bollinger(closes)
    avg_vol_20 = sum(volumes[-20:]) / 20
    vol_ratio = volumes[-1] / avg_vol_20 if avg_vol_20 > 0 else 1
    recent_high = max(highs[-20:])
    recent_low = min(lows[-20:])
    momentum_5 = (closes[-1] - closes[-6]) / closes[-6] * 100 if len(closes) >= 6 else 0
    streak = 0
    for i in range(len(closes) - 1, 0, -1):
        if closes[i] > opens[i]:
            if streak >= 0:
                streak += 1
            else:
                break
        elif closes[i] < opens[i]:
            if streak <= 0:
                streak -= 1
            else:
                break
        else:
            break
    return {
        'rsi': rsi, 'ema9': ema9, 'ema21': ema21, 'ema50': ema50,
        'ema_trend': 'bullish' if ema9 > ema21 > ema50 else ('bearish' if ema9 < ema21 < ema50 else 'mixed'),
        'atr': atr, 'atr_pct': atr_pct,
        'vwap': vwap, 'price_vs_vwap': 'above' if current > vwap else 'below',
        'bb_lower': bb_lower, 'bb_upper': bb_upper,
        'bb_position': 'near_upper' if current > bb_upper * 0.98 else ('near_lower' if current < bb_lower * 1.02 else 'mid'),
        'vol_ratio': vol_ratio, 'recent_high': recent_high, 'recent_low': recent_low,
        'momentum_5': momentum_5, 'candle_streak': streak,
    }


def analyze_order_book(order_book):
    if not order_book:
        return None
    bids = order_book.get('bids', [])
    asks = order_book.get('asks', [])
    if not bids or not asks:
        return None
    bid_depth = sum(b[1] * b[0] for b in bids[:20])
    ask_depth = sum(a[1] * a[0] for a in asks[:20])
    total_depth = bid_depth + ask_depth
    imbalance = (bid_depth - ask_depth) / total_depth if total_depth > 0 else 0
    spread = asks[0][0] - bids[0][0]
    spread_pct = (spread / bids[0][0] * 100) if bids[0][0] > 0 else 0
    biggest_bid = max(bids[:20], key=lambda x: x[1] * x[0]) if bids else [0, 0]
    biggest_ask = max(asks[:20], key=lambda x: x[1] * x[0]) if asks else [0, 0]
    return {
        'bid_depth_usdt': bid_depth, 'ask_depth_usdt': ask_depth,
        'imbalance': imbalance,
        'imbalance_label': 'buy_heavy' if imbalance > 0.15 else ('sell_heavy' if imbalance < -0.15 else 'balanced'),
        'spread_pct': spread_pct, 'bid_wall': biggest_bid[0], 'ask_wall': biggest_ask[0],
    }


# ============================================================
#  PNL TRACKING
# ============================================================

trade_log = []
session_start_balance = None


def fetch_trade_history():
    try:
        return trading_exchange.fapiprivate_get_income({'incomeType': 'REALIZED_PNL', 'limit': 100})
    except Exception as e:
        print(f"   ⚠️ Could not fetch income history: {e}", flush=True)
        return []


def log_trade_open(symbol, action, size_usdt, leverage, entry_price, sl, tp, confidence, reason, margin, risk_amount):
    trade = {
        'symbol': symbol, 'action': action, 'size_usdt': size_usdt,
        'leverage': leverage, 'entry_price': entry_price, 'sl': sl, 'tp': tp,
        'confidence': confidence, 'reason': reason, 'margin': margin, 'risk_amount': risk_amount,
        'opened_at': datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC'),
        'closed_at': None, 'exit_price': None, 'pnl': None, 'pnl_pct': None, 'result': None,
    }
    trade_log.append(trade)


def log_trade_close(exit_price, pnl):
    if not trade_log:
        return
    trade = trade_log[-1]
    if trade['closed_at'] is not None:
        return
    trade['closed_at'] = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')
    trade['exit_price'] = exit_price
    trade['pnl'] = pnl
    if trade['size_usdt'] > 0:
        trade['pnl_pct'] = (pnl / trade['size_usdt']) * 100
    trade['result'] = 'WIN' if pnl >= 0 else 'LOSS'


def print_pnl_dashboard():
    if not trade_log:
        print("   📊 No completed trades yet this session", flush=True)
        return
    completed = [t for t in trade_log if t['closed_at'] is not None]
    active = [t for t in trade_log if t['closed_at'] is None]
    if not completed and not active:
        return
    total_pnl = sum(t['pnl'] for t in completed if t['pnl'] is not None)
    wins = sum(1 for t in completed if t['result'] == 'WIN')
    losses = sum(1 for t in completed if t['result'] == 'LOSS')
    win_rate = (wins / len(completed) * 100) if completed else 0
    best = max(completed, key=lambda t: t['pnl'] or 0) if completed else None
    worst = min(completed, key=lambda t: t['pnl'] or 0) if completed else None
    print(f"\n   {'='*50}", flush=True)
    print(f"   📊 PNL DASHBOARD (Session)", flush=True)
    print(f"   {'='*50}", flush=True)
    print(f"   Total trades:  {len(completed)} closed, {len(active)} open", flush=True)
    print(f"   Total PnL:     ${total_pnl:+,.2f}", flush=True)
    print(f"   Win rate:      {wins}W / {losses}L ({win_rate:.0f}%)", flush=True)
    if best and best['pnl'] is not None:
        print(f"   Best trade:    {best['symbol']} ${best['pnl']:+,.2f} ({best['pnl_pct']:+.1f}%)", flush=True)
    if worst and worst['pnl'] is not None:
        print(f"   Worst trade:   {worst['symbol']} ${worst['pnl']:+,.2f} ({worst['pnl_pct']:+.1f}%)", flush=True)
    print(f"   {'-'*50}", flush=True)
    for t in completed[-10:]:
        icon = '✅' if t['result'] == 'WIN' else '❌'
        print(f"   {icon} {t['symbol']} {t['action'].upper()} {t['leverage']}x | "
              f"${t['entry_price']:,.2f} → ${t['exit_price']:,.2f} | "
              f"PnL: ${t['pnl']:+,.2f} ({t['pnl_pct']:+.1f}%) | "
              f"{t['opened_at'][:16]}", flush=True)
    if active:
        print(f"   {'-'*50}", flush=True)
        for t in active:
            print(f"   🔄 OPEN: {t['symbol']} {t['action'].upper()} {t['leverage']}x | "
                  f"Entry ${t['entry_price']:,.2f} | SL ${t['sl']:,.2f} / TP ${t['tp']:,.2f} | "
                  f"Notional ${t['size_usdt']:,.0f} | Margin ${t.get('margin', 0):,.0f}", flush=True)
    print(f"   {'='*50}", flush=True)


def print_income_summary():
    incomes = fetch_trade_history()
    if not incomes:
        return
    total = sum(float(i.get('income', 0)) for i in incomes)
    count = len(incomes)
    print(f"\n   📈 BINANCE INCOME HISTORY (last {count} realized PnL entries)", flush=True)
    print(f"   💵 Total realized PnL on account: ${total:+,.2f}", flush=True)
    for i in incomes[-5:]:
        pnl = float(i.get('income', 0))
        symbol = i.get('symbol', '???')
        ts = datetime.fromtimestamp(int(i.get('time', 0)) / 1000, tz=UTC).strftime('%m-%d %H:%M')
        icon = '✅' if pnl >= 0 else '❌'
        print(f"   {icon} {symbol}: ${pnl:+,.2f} @ {ts}", flush=True)


def get_recent_performance_summary():
    completed = [t for t in trade_log if t['closed_at'] is not None]
    if not completed:
        incomes = fetch_trade_history()
        if not incomes:
            return "No recent trade history available."
        total = sum(float(i.get('income', 0)) for i in incomes)
        wins = sum(1 for i in incomes if float(i.get('income', 0)) > 0)
        losses = len(incomes) - wins
        last_3 = incomes[-3:]
        last_str = ", ".join([f"{i.get('symbol', '?')} ${float(i.get('income', 0)):+,.2f}" for i in last_3])
        return f"Account history: {len(incomes)} trades, ${total:+,.2f} total PnL, {wins}W/{losses}L. Last 3: {last_str}"
    total_pnl = sum(t['pnl'] for t in completed if t['pnl'] is not None)
    wins = sum(1 for t in completed if t['result'] == 'WIN')
    losses = sum(1 for t in completed if t['result'] == 'LOSS')
    last_3 = completed[-3:]
    last_str = ", ".join([f"{t['symbol']} {t['action']} ${t['pnl']:+,.2f}" for t in last_3 if t['pnl'] is not None])
    return f"Session: {len(completed)} trades, ${total_pnl:+,.2f} PnL, {wins}W/{losses}L. Last 3: {last_str}"


# ============================================================
#  POSITION TRACKING
# ============================================================

def get_open_position():
    try:
        positions = retry_sync(
            lambda: trading_exchange.fetch_positions(),
            retries=3, delay=10,
            label="fetch_positions"
        )
        for pos in positions:
            contracts = float(pos.get('contracts', 0) or 0)
            if contracts != 0:
                # Debug: see what leverage fields exist
                raw_lev = pos.get('leverage')
                info_lev = pos.get('info', {}).get('leverage')
                print(f"   🔍 DEBUG leverage: pos.leverage={raw_lev} ({type(raw_lev).__name__}), info.leverage={info_lev} ({type(info_lev).__name__})", flush=True)

                lev = pos.get('leverage')
                # Fallback: read from raw Binance response
                if lev is None or lev == 0:
                    lev = pos.get('info', {}).get('leverage')
                try:
                    lev = int(float(lev)) if lev is not None else 1
                except (ValueError, TypeError):
                    lev = 1
                if lev <= 0:
                    lev = 1

                # Use ccxt's side field directly — contracts is always positive
                side = pos.get('side', '').lower()
                if side not in ('long', 'short'):
                    # Fallback: check positionAmt from raw info
                    raw_amt = float(pos.get('info', {}).get('positionAmt', 0) or 0)
                    side = 'long' if raw_amt > 0 else 'short' if raw_amt < 0 else 'long'

                return {
                    'symbol': pos['symbol'],
                    'side': side,
                    'contracts': abs(contracts),
                    'entry_price': float(pos.get('entryPrice', 0) or 0),
                    'unrealized_pnl': float(pos.get('unrealizedPnl', 0) or 0),
                    'leverage': lev,
                    'notional': abs(float(pos.get('notional', 0) or 0)),
                }
        return None
    except Exception as e:
        print(f"   ⚠️ Error fetching positions: {e}", flush=True)
        return None



# Track placed order IDs (kept for handle_position_close cleanup)
_active_sl_tp = {
    'symbol': None,
    'sl_id': None,
    'tp_id': None,
    'placed_at': 0,
    'sl_price': None,
    'tp_price': None,
    'position_side': None,  # 'long' or 'short'
}


def reset_sl_tp_tracking():
    """Reset all SL/TP tracking to defaults."""
    _active_sl_tp['symbol'] = None
    _active_sl_tp['sl_id'] = None
    _active_sl_tp['tp_id'] = None
    _active_sl_tp['placed_at'] = 0
    _active_sl_tp['sl_price'] = None
    _active_sl_tp['tp_price'] = None
    _active_sl_tp['position_side'] = None




def force_close_position(symbol, reason=""):
    """Market close a position. Does NOT log PnL — caller should use handle_position_close after."""
    for attempt in range(5):
        pos = get_open_position()
        if not pos or pos['symbol'] != symbol:
            reset_sl_tp_tracking()
            return True  # Already closed
        close_side = 'sell' if pos['side'] == 'long' else 'buy'
        try:
            trading_exchange.create_market_order(symbol, close_side, pos['contracts'])
            print(f"   🔴 CLOSED {pos['side'].upper()} {symbol} — {reason}", flush=True)
            cancel_all_open_orders(symbol)
            reset_sl_tp_tracking()
            return True
        except Exception as e:
            print(f"   💀 Force-close attempt {attempt + 1}/5 failed: {e}", flush=True)
            if attempt < 4:
                time.sleep(3)
    print(f"   💀 CRITICAL: Could not close {symbol} after 5 attempts!", flush=True)
    return False


def _close_stale_trade_log(symbol=None):
    """Close any open entries in trade_log that no longer have a Binance position.
    If symbol is given, only close that symbol. Otherwise close all stale entries
    EXCEPT the currently open position."""
    if not trade_log:
        return
    # Find the actual open position on Binance
    current_pos = get_open_position()
    current_symbol = current_pos['symbol'] if current_pos else None

    for t in trade_log:
        if t['closed_at'] is None:
            target = symbol or t['symbol']
            if t['symbol'] == target and t['symbol'] != current_symbol:
                t['closed_at'] = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')
                t['exit_price'] = t.get('entry_price', 0)
                t['pnl'] = t.get('pnl', 0) or 0
                t['pnl_pct'] = 0
                t['result'] = 'UNKNOWN'
                print(f"   🧹 Closed stale trade_log entry for {t['symbol']}", flush=True)


def cancel_all_open_orders(symbol):
    """Cancel ALL orders including conditional (STOP_MARKET, TAKE_PROFIT_MARKET).
    Uses both ccxt and raw Binance endpoint since fetch_open_orders can't see conditionals on demo."""
    cancelled = 0

    # Method 1: ccxt (catches regular orders)
    try:
        open_orders = trading_exchange.fetch_open_orders(symbol)
        for order in open_orders:
            try:
                trading_exchange.cancel_order(order['id'], symbol)
                cancelled += 1
            except Exception:
                pass
    except Exception:
        pass

    # Method 2: raw Binance endpoint (catches conditional orders ccxt misses)
    try:
        raw_sym = symbol_to_binance_raw(symbol)
        raw_orders = trading_exchange.fapiprivate_get_openorders({'symbol': raw_sym})
        for ro in raw_orders:
            try:
                trading_exchange.fapiprivate_delete_order({
                    'symbol': raw_sym,
                    'orderId': ro['orderId'],
                })
                cancelled += 1
            except Exception:
                pass
    except Exception:
        pass

    # Method 3: nuclear option — only if methods 1+2 found nothing but we know orders should exist
    if cancelled == 0 and _active_sl_tp.get('symbol') == symbol and _active_sl_tp.get('sl_id'):
        try:
            raw_sym = symbol_to_binance_raw(symbol)
            trading_exchange.fapiprivate_delete_allopenorders({'symbol': raw_sym})
            print(f"   🧹 Force-cancelled all orders on {symbol}", flush=True)
        except Exception:
            pass
    elif cancelled > 0:
        print(f"   🧹 Cancelled {cancelled} orders on {symbol}", flush=True)


def cleanup_all_orders():
    """Startup cleanup: cancel orphaned orders not belonging to current position."""
    try:
        position = get_open_position()
        pos_symbol = position['symbol'] if position else None

        # Get ALL open orders across all symbols via raw endpoint
        try:
            raw_orders = trading_exchange.fapiprivate_get_openorders()
            if raw_orders:
                orphan_count = 0
                for ro in raw_orders:
                    ro_symbol = ro.get('symbol', '')
                    # If no position, cancel everything. If position exists, keep its orders.
                    if pos_symbol and ro_symbol == symbol_to_binance_raw(pos_symbol):
                        continue
                    try:
                        trading_exchange.fapiprivate_delete_order({
                            'symbol': ro_symbol,
                            'orderId': ro['orderId'],
                        })
                        orphan_count += 1
                    except Exception:
                        pass
                if orphan_count:
                    print(f"   🧹 Cleaned up {orphan_count} orphan orders", flush=True)
        except Exception:
            pass

        if position:
            print(f"   📍 Existing position on {pos_symbol}", flush=True)
    except Exception as e:
        print(f"   ⚠️ Cleanup error: {e}", flush=True)


# ============================================================
#  SL/TP VALIDATION
# ============================================================

def validate_sl_tp(action, entry_price, sl, tp):
    warnings = []
    if action == "long":
        if sl >= entry_price:
            sl = round(entry_price * 0.97, 8)
            warnings.append(f"SL was above entry — corrected to ${sl:,.4f} (-3%)")
        if tp <= entry_price:
            tp = round(entry_price * 1.045, 8)
            warnings.append(f"TP was below entry — corrected to ${tp:,.4f} (+4.5%)")
    elif action == "short":
        if sl <= entry_price:
            sl = round(entry_price * 1.03, 8)
            warnings.append(f"SL was below entry — corrected to ${sl:,.4f} (+3%)")
        if tp >= entry_price:
            tp = round(entry_price * 0.955, 8)
            warnings.append(f"TP was above entry — corrected to ${tp:,.4f} (-4.5%)")

    sl_dist_pct = abs(entry_price - sl) / entry_price * 100
    if sl_dist_pct < 0.3:
        if action == "long":
            sl = round(entry_price * 0.99, 8)
        else:
            sl = round(entry_price * 1.01, 8)
        warnings.append(f"SL was too tight ({sl_dist_pct:.2f}%) — widened to 1%")
    elif sl_dist_pct > 10:
        if action == "long":
            sl = round(entry_price * 0.95, 8)
        else:
            sl = round(entry_price * 1.05, 8)
        warnings.append(f"SL was too wide ({sl_dist_pct:.1f}%) — capped at 5%")

    if warnings:
        for w in warnings:
            print(f"   ⚠️ SL/TP FIX: {w}", flush=True)
    return sl, tp


# ============================================================
#  RISK-BASED POSITION SIZING
# ============================================================

def calculate_position_size(balance, entry_price, sl_price, leverage):
    """
    Risk-based sizing:
    - risk_amount = balance × MAX_RISK_PERCENT (the max $ you lose if SL hits)
    - sl_distance = |entry - SL| / entry (as a fraction)
    - position_notional = risk_amount / sl_distance
    - margin_required = position_notional / leverage
    - Cap margin at MAX_MARGIN_PERCENT of balance
    
    Example with balance=$6816, risk=1%, SL=3%, leverage=20x:
    - risk_amount = $68.16
    - position_notional = $68.16 / 0.03 = $2,272
    - margin = $2,272 / 20 = $113.60
    - If margin > 50% of balance, scale down
    """
    risk_amount = balance * MAX_RISK_PERCENT / 100
    sl_distance = abs(entry_price - sl_price) / entry_price

    if sl_distance < 0.001:
        sl_distance = 0.01  # Safety floor: 1%

    # Position size from risk budget
    position_notional = risk_amount / sl_distance

    # Margin required
    margin_required = position_notional / leverage
    max_margin = balance * MAX_MARGIN_PERCENT / 100

    # Cap by margin
    if margin_required > max_margin:
        margin_required = max_margin
        position_notional = margin_required * leverage
        actual_risk = position_notional * sl_distance
        print(f"   ⚠️ Margin capped at {MAX_MARGIN_PERCENT}% (${max_margin:,.0f}) — risk reduced to ${actual_risk:,.2f}", flush=True)

    return position_notional, margin_required, risk_amount


# ============================================================
#  MARKET SCANNING
# ============================================================

async def get_top_candidates(n=20):
    get_cached_markets()
    tickers = None
    for attempt in range(3):
        try:
            tickers = public_exchange.fetch_tickers()
            break
        except (ccxt.DDoSProtection, ccxt.ExchangeNotAvailable) as e:
            wait = 60 * (attempt + 1)
            print(f"   ⚠️ Rate limited fetching tickers (attempt {attempt + 1}/3) — waiting {wait}s", flush=True)
            await asyncio.sleep(wait)
        except Exception:
            raise
    if not tickers:
        print(f"   ❌ Could not fetch tickers — skipping scan", flush=True)
        return []
    # TradFi symbols that require separate Binance agreement — skip these
    TRADFI_BASES = {'XAG', 'XAU', 'EUR', 'GBP', 'JPY', 'AUD', 'CHF', 'XPT', 'XPD',
                    'USO', 'SPX', 'NDX', 'DJI', 'VIX', 'NAS', 'RUS'}

    # Innovation zone tokens that don't support stop orders or leverage on demo
    INNOVATION_BASES = {'ESP', 'ENSO'}
    SKIP_BASES = TRADFI_BASES | INNOVATION_BASES

    markets = public_exchange.markets or {}
    candidates = []
    for symbol, ticker in tickers.items():
        market = markets.get(symbol, {})
        if ('USDT' in symbol
                and market.get('swap', False)
                and market.get('active', True)):
            # Skip TradFi and innovation zone tokens
            base = market.get('base', '')
            if base in SKIP_BASES:
                continue
            # Skip symbols that previously failed SL/TP placement
            if symbol in _blacklisted_symbols:
                continue
            vol = ticker.get('quoteVolume') or 0
            candidates.append({
                'symbol': symbol,
                'price': ticker.get('last', 0),
                'change24h': ticker.get('percentage', 0),
                'volume': vol,
            })
    candidates.sort(key=lambda x: x['volume'], reverse=True)
    return candidates[:n]


async def deep_enrich(candidates, top_n=10):
    """
    Tiered enrichment to stay within rate limits:
    - Top 5: Full (funding + 15m + 1h + 4h + OI + L/S + order book) ~7 calls each
    - 6-10: Partial (funding + 15m + 1h) ~3 calls each
    Total: ~50 calls with 0.3s gaps = ~15 seconds
    """
    enriched = []

    # === MARKET CONTEXT: BTC + ETH ===
    market_context = {}
    try:
        btc_candles = public_exchange.fetch_ohlcv('BTC/USDT:USDT', '1h', limit=24)
        if btc_candles and len(btc_candles) >= 2:
            btc_now = btc_candles[-1][4]
            btc_24h_ago = btc_candles[0][1]
            btc_4h_ago = btc_candles[-4][1] if len(btc_candles) >= 4 else btc_now
            market_context['btc_price'] = btc_now
            market_context['btc_24h_pct'] = (btc_now - btc_24h_ago) / btc_24h_ago * 100
            market_context['btc_4h_pct'] = (btc_now - btc_4h_ago) / btc_4h_ago * 100
            btc_ta = analyze_candles(btc_candles)
            market_context['btc_rsi'] = btc_ta['rsi'] if btc_ta else 50
            market_context['btc_ema_trend'] = btc_ta['ema_trend'] if btc_ta else 'mixed'
        await asyncio.sleep(0.3)

        eth_candles = public_exchange.fetch_ohlcv('ETH/USDT:USDT', '1h', limit=24)
        if eth_candles and len(eth_candles) >= 2:
            eth_now = eth_candles[-1][4]
            eth_24h_ago = eth_candles[0][1]
            market_context['eth_price'] = eth_now
            market_context['eth_24h_pct'] = (eth_now - eth_24h_ago) / eth_24h_ago * 100
        await asyncio.sleep(0.3)
    except Exception as e:
        print(f"   ⚠️ Market context fetch error: {e}", flush=True)

    for i, c in enumerate(candidates[:top_n]):
        symbol = c['symbol']
        is_top5 = i < 5

        try:
            # Funding rate (all coins)
            funding_data = public_exchange.fetch_funding_rate(symbol)
            c['funding'] = funding_data.get('fundingRate', 0.0) if funding_data else 0.0
            await asyncio.sleep(0.3)

            # 15-minute candles (all coins)
            candles_15m = public_exchange.fetch_ohlcv(symbol, '15m', limit=50)
            c['ta_15m'] = analyze_candles(candles_15m)
            await asyncio.sleep(0.3)

            # 1-hour candles (all coins)
            candles_1h = public_exchange.fetch_ohlcv(symbol, '1h', limit=50)
            c['ta_1h'] = analyze_candles(candles_1h)
            await asyncio.sleep(0.3)

            # --- TOP 5 ONLY: deeper analysis ---
            if is_top5:
                # 4h candles
                candles_4h = public_exchange.fetch_ohlcv(symbol, '4h', limit=30)
                c['ta_4h'] = analyze_candles(candles_4h)
                await asyncio.sleep(0.3)

                # Open Interest
                try:
                    raw_symbol = symbol_to_binance_raw(symbol)
                    oi_data = public_exchange.fapipublic_get_openinterest({'symbol': raw_symbol})
                    c['open_interest'] = float(oi_data.get('openInterest', 0))
                    c['oi_notional'] = c['open_interest'] * c['price']
                except Exception:
                    c['open_interest'] = 0
                    c['oi_notional'] = 0
                await asyncio.sleep(0.3)

                # Long/Short Ratio
                try:
                    ls_data = public_exchange.fapipublic_get_toplongshortaccountratio({
                        'symbol': raw_symbol,
                        'period': '1h',
                        'limit': 5,
                    })
                    if ls_data and len(ls_data) > 0:
                        latest_ls = ls_data[-1]
                        c['long_short_ratio'] = float(latest_ls.get('longShortRatio', 1.0))
                        c['long_pct'] = float(latest_ls.get('longAccount', 0.5)) * 100
                        c['short_pct'] = float(latest_ls.get('shortAccount', 0.5)) * 100
                        if len(ls_data) >= 5:
                            old_ls = float(ls_data[0].get('longShortRatio', 1.0))
                            new_ls = float(ls_data[-1].get('longShortRatio', 1.0))
                            c['ls_trend'] = 'longs_increasing' if new_ls > old_ls * 1.05 else (
                                'shorts_increasing' if new_ls < old_ls * 0.95 else 'stable')
                        else:
                            c['ls_trend'] = 'stable'
                    else:
                        c['long_short_ratio'] = 1.0
                        c['long_pct'] = 50
                        c['short_pct'] = 50
                        c['ls_trend'] = 'stable'
                except Exception:
                    c['long_short_ratio'] = 1.0
                    c['long_pct'] = 50
                    c['short_pct'] = 50
                    c['ls_trend'] = 'stable'
                await asyncio.sleep(0.3)

                # Order book
                ob = public_exchange.fetch_order_book(symbol, limit=20)
                c['order_book'] = analyze_order_book(ob)
                await asyncio.sleep(0.3)
            else:
                # Partial enrichment for coins 6-10
                c['ta_4h'] = None
                c['open_interest'] = 0
                c['oi_notional'] = 0
                c['long_short_ratio'] = 1.0
                c['long_pct'] = 50
                c['short_pct'] = 50
                c['ls_trend'] = 'n/a'
                c['order_book'] = None

        except (ccxt.DDoSProtection, ccxt.ExchangeNotAvailable) as e:
            print(f"   ⚠️ Rate limited during enrichment — pausing 30s: {e}", flush=True)
            await asyncio.sleep(30)
            c['funding'] = c.get('funding', 0.0)
            c['ta_15m'] = c.get('ta_15m')
            c['ta_1h'] = c.get('ta_1h')
            c['ta_4h'] = None
            c['open_interest'] = 0
            c['oi_notional'] = 0
            c['long_short_ratio'] = 1.0
            c['long_pct'] = 50
            c['short_pct'] = 50
            c['ls_trend'] = 'n/a'
            c['order_book'] = None
        except Exception as e:
            print(f"   ⚠️ Enrich error on {symbol}: {e}", flush=True)
            c['funding'] = c.get('funding', 0.0)
            c['ta_15m'] = c.get('ta_15m')
            c['ta_1h'] = c.get('ta_1h')
            c['ta_4h'] = None
            c['open_interest'] = 0
            c['oi_notional'] = 0
            c['long_short_ratio'] = 1.0
            c['long_pct'] = 50
            c['short_pct'] = 50
            c['ls_trend'] = 'n/a'
            c['order_book'] = None
        enriched.append(c)

    for c in enriched:
        c['market_context'] = market_context

    return enriched


# ============================================================
#  GROK AI DECISION
# ============================================================

async def grok_decision(candidates, balance):
    valid_symbols = [c['symbol'] for c in candidates]

    # Market context (from first candidate, shared across all)
    mc = candidates[0].get('market_context', {}) if candidates else {}
    market_str = ""
    if mc:
        market_str = f"""BTC: ${mc.get('btc_price', 0):,.0f} | 4h: {mc.get('btc_4h_pct', 0):+.2f}% | 24h: {mc.get('btc_24h_pct', 0):+.2f}% | RSI: {mc.get('btc_rsi', 50):.0f} | Trend: {mc.get('btc_ema_trend', 'mixed')}
ETH: ${mc.get('eth_price', 0):,.0f} | 24h: {mc.get('eth_24h_pct', 0):+.2f}%
Market regime: {'RISK-ON (BTC bullish)' if mc.get('btc_ema_trend') == 'bullish' else 'RISK-OFF (BTC bearish)' if mc.get('btc_ema_trend') == 'bearish' else 'CHOPPY (BTC mixed)'}"""

    lines = []
    for c in candidates:
        ta15 = c.get('ta_15m')
        ta1h = c.get('ta_1h')
        ta4h = c.get('ta_4h')
        ob = c.get('order_book')
        line = f"\n--- {c['symbol']} ---\n"
        line += f"Price: ${c['price']:,.2f} | 24h: {c['change24h']:+.2f}% | Vol: ${c['volume'] / 1e9:.1f}B | Funding: {c.get('funding', 0) * 100:.4f}%\n"

        # Positioning data
        line += f"OI: ${c.get('oi_notional', 0) / 1e6:.1f}M | L/S Ratio: {c.get('long_short_ratio', 1.0):.2f} ({c.get('long_pct', 50):.0f}%L/{c.get('short_pct', 50):.0f}%S) | L/S Trend: {c.get('ls_trend', 'stable')}\n"

        if ta15:
            line += f"15m: RSI {ta15['rsi']:.1f} | EMA9 ${ta15['ema9']:,.2f} EMA21 ${ta15['ema21']:,.2f} ({ta15['ema_trend']}) | "
            line += f"ATR {ta15['atr_pct']:.2f}% | Vol×{ta15['vol_ratio']:.1f} | "
            line += f"BB {ta15['bb_position']} | VWAP {ta15['price_vs_vwap']} | "
            line += f"Mom5: {ta15['momentum_5']:+.2f}% | Streak: {ta15['candle_streak']:+d} | "
            line += f"S/R: ${ta15['recent_low']:,.2f}-${ta15['recent_high']:,.2f}\n"
        if ta1h:
            line += f"1h:  RSI {ta1h['rsi']:.1f} | EMA9 ${ta1h['ema9']:,.2f} EMA21 ${ta1h['ema21']:,.2f} ({ta1h['ema_trend']}) | "
            line += f"ATR {ta1h['atr_pct']:.2f}% | Vol×{ta1h['vol_ratio']:.1f} | "
            line += f"Mom5: {ta1h['momentum_5']:+.2f}% | Streak: {ta1h['candle_streak']:+d}\n"
        if ta4h:
            line += f"4h:  RSI {ta4h['rsi']:.1f} | EMA9 ${ta4h['ema9']:,.2f} EMA21 ${ta4h['ema21']:,.2f} ({ta4h['ema_trend']}) | "
            line += f"ATR {ta4h['atr_pct']:.2f}% | Vol×{ta4h['vol_ratio']:.1f} | "
            line += f"Mom5: {ta4h['momentum_5']:+.2f}% | Streak: {ta4h['candle_streak']:+d}\n"
        if ob:
            line += f"Book: {ob['imbalance_label']} (imb {ob['imbalance']:+.2f}) | "
            line += f"Spread {ob['spread_pct']:.4f}% | "
            line += f"Bid wall ${ob['bid_wall']:,.2f} | Ask wall ${ob['ask_wall']:,.2f}\n"
        lines.append(line)

    data_str = "".join(lines)
    perf_summary = get_recent_performance_summary()
    risk_budget = balance * MAX_RISK_PERCENT / 100

    prompt = f"""You are **AlphaEdge**, an elite quantitative futures trader.
You find asymmetric setups with favorable risk/reward. You prefer to be in a trade when there's an edge rather than sitting on the sidelines.

═══ MACRO CONTEXT ═══
{market_str if market_str else "Market data unavailable."}

═══ ACCOUNT ═══
Time: {datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")}
Balance: {balance:.2f} USDT | Risk budget: ${risk_budget:.2f} ({MAX_RISK_PERCENT}%) | Max margin: {MAX_MARGIN_PERCENT}%
Position sizing is automatic — tighter SL = bigger position, wider SL = smaller. Same risk.

═══ RECENT PERFORMANCE ═══
{perf_summary}

═══ TOP {len(candidates)} CANDIDATES ═══
{data_str}

═══ FIRST PRINCIPLES ANALYSIS ═══
Before picking a trade, think through each layer:

**1. MACRO REGIME**
- Check BTC trend. In strong BTC downtrends, prefer shorts or reduce leverage on longs.
- BTC choppy/mixed = trade individual coin setups on their own merit with moderate leverage.
- Don't let a neutral BTC prevent you from taking good altcoin setups.

**2. POSITIONING & CROWDING**
- Long/Short ratio: If >65% are long, contrarian shorts have edge. If <35% are long, look for squeeze longs.
- L/S trend: Rapidly increasing longs = potential FOMO, adds weight to short thesis but isn't a veto.
- Open Interest: Rising OI + rising price = genuine trend. Rising OI + falling price = shorts building.
- Use positioning as an edge multiplier, not a trade filter.

**3. FUNDING RATE AS SENTIMENT**
- Very positive funding (>0.05%) = crowded long, adds edge to shorts but doesn't block longs.
- Very negative funding (<-0.05%) = crowded short, squeeze longs have extra edge.
- Near zero = neutral, trade on technicals alone.
- Funding is a confidence booster, not a gate.

**4. MULTI-TIMEFRAME TREND ALIGNMENT**
- Best trades: 4h + 1h + 15m ALL agree on direction.
- Good trades: 2 of 3 timeframes agree — this is enough to enter with moderate leverage.
- 15m pullback against 1h/4h trend = potential entry opportunity, not a disqualifier.
- VWAP position is a tiebreaker, not a dealbreaker.

**5. VOLATILITY & MOMENTUM**
- ATR tells you expected movement range. Low ATR = use tighter SL, not a reason to skip.
- Bollinger squeeze (price near mid, bands tight) = breakout imminent. Good setup.
- Volume ratio >1.5 adds conviction. <0.8 means lighter position, not no position.
- Candle streak: +5 or more = consider counter-trend trades or tighter SL, but don't skip entirely.

**6. ORDER FLOW (top 5 coins)**
- Buy-heavy imbalance + bullish trend = strong confirmation for longs.
- Sell-heavy imbalance + rising price = hidden distribution, be cautious.
- Large bid/ask walls act as magnets — price tends to test them.

**7. STOP LOSS PLACEMENT (critical)**
- SL should be at a level where your thesis is INVALIDATED, not just an arbitrary %.
- Below recent support (for longs) or above recent resistance (for shorts).
- Below/above Bollinger lower/upper band.
- Must give enough room for normal volatility (at least 1x ATR from entry).

**8. TAKE PROFIT PLACEMENT**
- At the next meaningful resistance (for longs) or support (for shorts).
- Near the opposite Bollinger band.
- Near recent high/low as measured by 4h or 1h timeframe.
- Minimum 2:1 R:R ratio — this is NON-NEGOTIABLE. The system will auto-correct if you violate this.

**9. WHEN TO HOLD**
- Hold if ALL timeframes conflict with no clear direction.
- Hold if a coin just pumped 20%+ — usually too late for a clean entry.
- Otherwise, look for the best available setup. There's usually something tradeable in the top 20.

═══ RULES ═══
- CRITICAL: "symbol" must be EXACTLY one of the symbols listed above.
- For LONG: stop_loss BELOW entry, take_profit ABOVE entry.
- For SHORT: stop_loss ABOVE entry, take_profit BELOW entry.
- SL at a technical invalidation level (0.5-5% from entry).
- TP at minimum 2:1 R:R (the system enforces this — don't even try 1:1).
- Leverage 15-20x when 3/3 timeframes align with confirming positioning/funding.
- Leverage 7-12x when 2/3 timeframes align — this is your bread and butter.
- Leverage 3-5x for weaker setups with one strong signal.
- Confidence ≥ 0.60 to trade. Below 0.60 = hold.
- You should be trading more often than not. Find the best setup available.

═══ RESPOND WITH ONLY VALID JSON ═══
{{
  "symbol": "EXACT symbol from the list above",
  "action": "long" | "short" | "hold",
  "leverage": integer 1-20,
  "stop_loss": number (technical invalidation level),
  "take_profit": number (next major level, ≥2x SL distance),
  "confidence": 0.00-1.00,
  "reason": "2-3 sentences: what's the edge? Cite the strongest 2-3 signals that support this trade."
}}"""

    try:
        response = await asyncio.wait_for(
            client.chat.completions.create(
                model="grok-4-1-fast-reasoning",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
                max_tokens=1200
            ),
            timeout=120
        )
    except asyncio.TimeoutError:
        print("   ⏰ Grok API timed out after 120s — holding", flush=True)
        return {"action": "hold", "confidence": 0, "reason": "API timeout"}
    except Exception as e:
        print(f"   ⚠️ Grok API error: {e}", flush=True)
        return {"action": "hold", "confidence": 0, "reason": f"API error: {e}"}

    try:
        text = response.choices[0].message.content.strip()
        decision = parse_grok_json(text)
        if decision is None:
            print(f"   ⚠️ Could not parse Grok JSON — holding", flush=True)
            print(f"   [RAW]: {text[:200]}", flush=True)
            return {"action": "hold", "confidence": 0, "reason": "JSON parse failure"}
        # If Grok says hold, skip symbol validation entirely
        if decision.get('action', '').lower() == 'hold':
            return decision
        raw_symbol = decision.get('symbol', '')
        normalized = normalize_symbol(raw_symbol)
        if normalized not in valid_symbols:
            raw_clean = symbol_to_binance_raw(raw_symbol)
            found = False
            for vs in valid_symbols:
                if symbol_to_binance_raw(vs) == raw_clean:
                    normalized = vs
                    found = True
                    break
            if not found:
                print(f"   ⚠️ Grok returned unknown symbol '{raw_symbol}' — holding", flush=True)
                return {"action": "hold", "confidence": 0, "reason": f"Unknown symbol: {raw_symbol}"}
        decision['symbol'] = normalized
        return decision
    except Exception as e:
        print(f"   ⚠️ Decision parsing error: {e}", flush=True)
        return {"action": "hold", "confidence": 0}



# ============================================================
#  GROK POSITION MANAGEMENT
# ============================================================

async def grok_evaluate_position(position, candidates, balance):
    """Ask Grok whether to keep or close an existing position.
    Returns {'action': 'keep'|'close', 'reason': '...'}"""
    symbol = position['symbol']
    side = position['side']
    entry = position['entry_price']
    pnl = position['unrealized_pnl']
    pnl_pct = (pnl / position['notional'] * 100) if position['notional'] else 0
    leverage = position['leverage']

    # Find this coin's enriched data
    coin_data = None
    for c in candidates:
        if c['symbol'] == symbol:
            coin_data = c
            break

    # Build market data string for the position's coin
    coin_str = ""
    if coin_data:
        ta15 = coin_data.get('ta_15m')
        ta1h = coin_data.get('ta_1h')
        ta4h = coin_data.get('ta_4h')
        ob = coin_data.get('order_book')
        coin_str = f"Price: ${coin_data['price']:,.2f} | 24h: {coin_data['change24h']:+.2f}% | Funding: {coin_data.get('funding', 0) * 100:.4f}%\n"
        coin_str += f"OI: ${coin_data.get('oi_notional', 0) / 1e6:.1f}M | L/S: {coin_data.get('long_short_ratio', 1.0):.2f}\n"
        if ta15:
            coin_str += f"15m: RSI {ta15['rsi']:.1f} | EMA9 ${ta15['ema9']:,.2f} EMA21 ${ta15['ema21']:,.2f} ({ta15['ema_trend']}) | Mom: {ta15['momentum_5']:+.2f}% | Streak: {ta15['candle_streak']:+d}\n"
        if ta1h:
            coin_str += f"1h:  RSI {ta1h['rsi']:.1f} | EMA9 ${ta1h['ema9']:,.2f} EMA21 ${ta1h['ema21']:,.2f} ({ta1h['ema_trend']}) | Mom: {ta1h['momentum_5']:+.2f}% | Streak: {ta1h['candle_streak']:+d}\n"
        if ta4h:
            coin_str += f"4h:  RSI {ta4h['rsi']:.1f} | EMA9 ${ta4h['ema9']:,.2f} EMA21 ${ta4h['ema21']:,.2f} ({ta4h['ema_trend']}) | Mom: {ta4h['momentum_5']:+.2f}% | Streak: {ta4h['candle_streak']:+d}\n"
        if ob:
            coin_str += f"Book: {ob['imbalance_label']} (imb {ob['imbalance']:+.2f}) | Spread {ob['spread_pct']:.4f}%\n"

    # Market context
    mc = candidates[0].get('market_context', {}) if candidates else {}
    market_str = ""
    if mc:
        market_str = f"BTC: ${mc.get('btc_price', 0):,.0f} | 4h: {mc.get('btc_4h_pct', 0):+.2f}% | 24h: {mc.get('btc_24h_pct', 0):+.2f}% | RSI: {mc.get('btc_rsi', 50):.0f} | Trend: {mc.get('btc_ema_trend', 'mixed')}"

    prompt = f"""You are **AlphaEdge**, an elite quantitative futures trader managing an open position.

═══ MACRO ═══
{market_str if market_str else "Unavailable"}

═══ OPEN POSITION ═══
{side.upper()} {symbol} | Entry: ${entry:,.2f} | {leverage}x Cross
Unrealized PnL: ${pnl:+,.2f} ({pnl_pct:+.2f}%)
Notional: ${position['notional']:,.0f} | Contracts: {position['contracts']}

═══ CURRENT MARKET DATA FOR {symbol} ═══
{coin_str if coin_str else "Data unavailable — close if uncertain."}

═══ FIRST PRINCIPLES POSITION MANAGEMENT ═══
Think through each layer before deciding:

**1. IS THE MACRO STILL SUPPORTIVE?**
- If you're long and BTC is now in a clear downtrend (not just a dip), that weakens your thesis.
- BTC choppy/sideways is fine for altcoin positions — don't close just because BTC isn't pumping.
- A macro shift against your direction is a strong close signal, but a neutral macro is not.

**2. HAS THE POSITIONING/CROWDING STORY CHANGED?**
- If you entered a squeeze play (e.g. short squeeze on negative funding), is the funding still negative? Has it normalized?
- If L/S ratio has flipped against your thesis, that's worth noting but not an automatic close.
- Positioning that was your edge disappearing = weaker conviction to hold.

**3. ARE THE TRENDS STILL ALIGNED?**
- Check if the multi-timeframe alignment that got you into this trade still holds.
- One timeframe flipping is normal noise. Two or more flipping = thesis weakening.
- 15m noise against your position is fine if 1h and 4h still support you.

**4. MOMENTUM & PRICE ACTION**
- Is momentum still in your favor or has it clearly shifted?
- Candle streaks extending in your direction = let it run.
- Momentum flipping on 1h+ timeframes = consider closing.
- Price breaking below key EMAs (for longs) or above (for shorts) is meaningful.

**5. PROFIT & LOSS REALITY**
- Nice profit (>3-5% leveraged)? Consider taking it — don't be greedy, but don't panic-close small gains either.
- Moderate loss (-3% to -8% leveraged)? Only close if the thesis is also broken. Losses alone aren't a reason to close if the setup is still valid.
- Large loss (>-10% leveraged)? Close unless you have very strong conviction the thesis is intact.
- Small loss or small gain? Focus on whether the thesis holds, not the P&L.

**6. OVERALL JUDGMENT**
- The default should be to KEEP if nothing has fundamentally changed.
- Don't close on minor pullbacks or noise — these are leveraged positions, some volatility is expected.
- CLOSE when your original reason for entering is no longer valid, or when a clear better opportunity exists.
- You're re-evaluated every {INTERVAL_MINUTES} minutes, so there's no urgency to close on weak signals.

═══ RESPOND WITH ONLY VALID JSON ═══
{{
  "action": "keep" | "close",
  "reason": "1-2 sentences: what changed or didn't change since entry?"
}}"""

    try:
        response = await asyncio.wait_for(
            client.chat.completions.create(
                model="grok-4-1-fast-reasoning",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.5,
                max_tokens=300
            ),
            timeout=60
        )
        text = response.choices[0].message.content.strip()
        result = parse_grok_json(text)
        if result and result.get('action') in ('keep', 'close'):
            return result
        print(f"   ⚠️ Grok position eval parse failed — defaulting to keep", flush=True)
        print(f"   [RAW]: {text[:200]}", flush=True)
        return {"action": "keep", "reason": "Parse failure — holding"}
    except asyncio.TimeoutError:
        print("   ⏰ Grok position eval timed out — keeping position", flush=True)
        return {"action": "keep", "reason": "API timeout"}
    except Exception as e:
        print(f"   ⚠️ Grok position eval error: {e} — keeping position", flush=True)
        return {"action": "keep", "reason": f"API error: {e}"}


# ============================================================
#  TRADE EXECUTION
# ============================================================

async def execute_trade(decision, balance):
    action = decision.get("action", "hold")
    try:
        confidence = float(decision.get("confidence", 0))
    except (ValueError, TypeError):
        confidence = 0
    symbol = decision.get("symbol")
    try:
        sl = float(decision.get("stop_loss", 0)) or None
    except (ValueError, TypeError):
        sl = None
    try:
        tp = float(decision.get("take_profit", 0)) or None
    except (ValueError, TypeError):
        tp = None

    if action == "hold" or confidence < 0.60:
        print(f"   💤 HOLD — confidence {confidence:.2f} | {decision.get('reason', 'n/a')}", flush=True)
        return False

    if not symbol or not sl or not tp:
        print(f"   ⚠️ Missing symbol/SL/TP in Grok response — skipping", flush=True)
        return False

    # Safety: don't open if we already have a position
    existing = get_open_position()
    if existing:
        print(f"   ⚠️ Already have {existing['side'].upper()} {existing['symbol']} — skipping new trade", flush=True)
        return False

    try:
        get_cached_markets()
        cancel_all_open_orders(symbol)
        await asyncio.sleep(1)

        try:
            trading_exchange.set_margin_mode('cross', symbol)
        except Exception:
            pass
        await asyncio.sleep(1)  # Brief pause before leverage to avoid -1000

        # Set leverage — auto-reduce if rejected
        try:
            leverage = min(int(decision.get("leverage", 10)), 20)
        except (ValueError, TypeError):
            leverage = 10
        # Deduplicate: put requested leverage first, then fallbacks
        lev_attempts = list(dict.fromkeys([leverage, 15, 10, 7, 5, 3, 2, 1]))
        lev_set = False
        for lev in lev_attempts:
            for lev_retry in range(2):
                try:
                    trading_exchange.set_leverage(lev, symbol)
                    leverage = lev
                    lev_set = True
                    break
                except Exception as e:
                    err_str = str(e)
                    if '-4028' in err_str and lev > 1:
                        print(f"   ⚠️ {lev}x not supported for {symbol}, trying lower...", flush=True)
                        break  # Try next lower leverage
                    elif '-4028' in err_str and lev == 1:
                        _blacklisted_symbols.add(symbol)
                        print(f"   🚫 No valid leverage for {symbol} — blacklisted, skipping", flush=True)
                        return False
                    elif '-1000' in err_str and lev_retry < 1:
                        await asyncio.sleep(3)
                        continue
                    elif '-1000' in err_str:
                        # Binance remembers last leverage per symbol — proceed with whatever is set
                        print(f"   ⚠️ Can't set leverage (-1000) — proceeding with existing leverage", flush=True)
                        lev_set = True
                        break
                    else:
                        raise
            if lev_set:
                break
        if not lev_set:
            _blacklisted_symbols.add(symbol)
            print(f"   🚫 Could not set any leverage for {symbol} — blacklisted, skipping", flush=True)
            return False
        print(f"   ⚙️ Set leverage: {leverage}x", flush=True)

        # Get current price (with retry)
        current_price = None
        for tick_attempt in range(3):
            try:
                ticker = public_exchange.fetch_ticker(symbol)
                current_price = ticker['last']
                break
            except Exception as tick_err:
                if tick_attempt < 2:
                    print(f"   🔁 Ticker fetch failed — retrying in 3s: {tick_err}", flush=True)
                    await asyncio.sleep(3)
                else:
                    raise
        if not current_price or current_price <= 0:
            print(f"   ❌ Invalid price for {symbol} — skipping", flush=True)
            return False

        # Validate SL/TP
        sl, tp = validate_sl_tp(action, current_price, sl, tp)
        sl = round_price(sl, symbol)
        tp = round_price(tp, symbol)

        # Hard R:R check — reject if TP isn't at least 1.8x the SL distance
        if action == "long":
            sl_dist = abs(current_price - sl)
            tp_dist = abs(tp - current_price)
        else:
            sl_dist = abs(sl - current_price)
            tp_dist = abs(current_price - tp)

        if sl_dist > 0:
            actual_rr = tp_dist / sl_dist
        else:
            actual_rr = 0

        if actual_rr < 1.8:
            original_rr = actual_rr
            # Auto-fix: keep SL, push TP to 2:1
            if action == "long":
                tp = round_price(current_price + (sl_dist * 2.0), symbol)
            else:
                tp = round_price(current_price - (sl_dist * 2.0), symbol)
            actual_rr = 2.0
            print(f"   ⚠️ R:R was {original_rr:.1f}:1 — TP auto-adjusted to ${tp:,.2f} (2:1)", flush=True)

        # === RISK-BASED POSITION SIZING ===
        position_notional, margin_required, risk_amount = calculate_position_size(
            balance, current_price, sl, leverage
        )

        # Check minimum notional
        min_notional = get_min_notional(symbol)
        if position_notional < min_notional:
            print(f"   ⚠️ Notional ${position_notional:.2f} below minimum ${min_notional:.2f} — adjusting", flush=True)
            position_notional = min_notional * 1.1
            margin_required = position_notional / leverage

        # Calculate amount in coins
        raw_amount = position_notional / current_price
        amount = float(round_amount(raw_amount, symbol))

        if amount <= 0:
            print(f"   ⚠️ Calculated amount is 0 after rounding — skipping", flush=True)
            return False

        # Recalculate actual notional after rounding
        actual_notional = amount * current_price
        actual_margin = actual_notional / leverage
        sl_dist_pct = abs(current_price - sl) / current_price * 100
        tp_dist_pct = abs(tp - current_price) / current_price * 100
        actual_risk = actual_notional * abs(current_price - sl) / current_price

        # === Check runtime blacklist (symbols that failed SL/TP on real trades) ===
        if symbol in _blacklisted_symbols:
            print(f"   🚫 {symbol} is blacklisted — skipping", flush=True)
            return False

        # Place entry order (with retry for transient -1000 errors)
        entry_side = "buy" if action == "long" else "sell"
        for attempt in range(3):
            try:
                trading_exchange.create_market_order(symbol, entry_side, amount)
                break
            except Exception as entry_err:
                if '-1000' in str(entry_err) and attempt < 2:
                    print(f"   🔁 Transient error on entry order — retrying in 5s ({attempt + 1}/3)", flush=True)
                    await asyncio.sleep(5)
                else:
                    raise

        # Log the trade immediately
        log_trade_open(symbol, action, actual_notional, leverage, current_price, sl, tp,
                       confidence, decision.get('reason', ''), actual_margin, actual_risk)

        # No SL/TP orders — Grok manages position each cycle
        # R:R ratio (for logging only)
        if action == "long":
            risk = abs(current_price - sl)
            reward = abs(tp - current_price)
        else:
            risk = abs(sl - current_price)
            reward = abs(current_price - tp)
        rr = reward / risk if risk > 0 else 0

        print(f"   🔥 OPENED {action.upper()} {symbol} @ ${current_price:,.2f} | {leverage}x Cross", flush=True)
        print(f"   📐 Notional: ${actual_notional:,.0f} | Margin: ${actual_margin:,.0f} | Risk: ${actual_risk:,.2f} ({MAX_RISK_PERCENT}%)", flush=True)
        print(f"   📊 Grok targets: SL ${sl:,.2f} (-{sl_dist_pct:.1f}%) | TP ${tp:,.2f} (+{tp_dist_pct:.1f}%) | R:R {rr:.1f}:1", flush=True)
        print(f"   🤖 Grok will re-evaluate every {INTERVAL_MINUTES} min — no exchange SL/TP orders", flush=True)
        print(f"   💡 {decision.get('reason', 'n/a')}", flush=True)

        return True

    except Exception as e:
        err_str = str(e)
        if '-4411' in err_str or 'TradFi' in err_str:
            print(f"   ⚠️ {symbol} requires TradFi agreement — skipping", flush=True)
        elif '-1000' in err_str:
            print(f"   ⚠️ Binance transient error on {symbol} — will retry next cycle", flush=True)
        else:
            print(f"   ❌ Execution error on {symbol}: {e}", flush=True)
        return False


# ============================================================
#  SCAN + TRADE
# ============================================================
#  SCAN + TRADE / MANAGE POSITION
# ============================================================

async def manage_position(position):
    """Ask Grok whether to keep or close the current position. Returns True if closed."""
    try:
        candidates = await get_top_candidates(20)
        if not candidates:
            print(f"   ⚠️ No market data — keeping position", flush=True)
            return False

        # Ensure the position's coin is in candidates
        pos_in_candidates = any(c['symbol'] == position['symbol'] for c in candidates)
        if not pos_in_candidates:
            # Fetch just this coin's data
            try:
                ticker = public_exchange.fetch_ticker(position['symbol'])
                candidates.insert(0, {
                    'symbol': position['symbol'],
                    'price': ticker['last'],
                    'change24h': ticker.get('percentage', 0) or 0,
                    'volume': ticker.get('quoteVolume', 0) or 0,
                })
            except Exception:
                pass

        # Enrich (position coin + top others for context)
        print(f"   🔬 Enriching market data for position evaluation...", flush=True)
        enriched = await deep_enrich(candidates, 10)

        balance = None
        for bal_attempt in range(3):
            try:
                balance_data = trading_exchange.fetch_balance()
                balance = float(balance_data['total'].get('USDT', 0))
                break
            except Exception:
                if bal_attempt < 2:
                    await asyncio.sleep(5)
        if balance:
            print(f"   💰 Balance: ${balance:,.2f} USDT", flush=True)
            if session_start_balance and session_start_balance > 0:
                session_pnl = balance - session_start_balance
                print(f"   📊 Session PnL: ${session_pnl:+,.2f} ({session_pnl / session_start_balance * 100:+.2f}%)", flush=True)

        # Ask Grok
        eval_result = await grok_evaluate_position(position, enriched, balance or 0)
        action = eval_result.get('action', 'keep')
        reason = eval_result.get('reason', 'n/a')

        if action == 'close':
            print(f"   🤖 Grok says CLOSE: {reason}", flush=True)
            force_close_position(position['symbol'], f"Grok: {reason}")
            return True
        else:
            print(f"   🤖 Grok says KEEP: {reason}", flush=True)
            return False

    except (ccxt.DDoSProtection, ccxt.ExchangeNotAvailable) as e:
        print(f"   🚫 Rate limited during position eval — keeping: {str(e)[:80]}", flush=True)
        return False
    except Exception as e:
        print(f"   ❌ Position eval error — keeping: {e}", flush=True)
        return False


async def scan_and_trade():
    try:
        candidates = await get_top_candidates(20)
        print(f"   📈 Found {len(candidates)} futures", flush=True)

        if not candidates:
            print(f"   ⚠️ No candidates available — skipping this cycle", flush=True)
            return False

        print(f"   🔬 Enriching top 10 with technicals + order book...", flush=True)
        enriched = await deep_enrich(candidates, 10)

        balance = None
        for bal_attempt in range(3):
            try:
                balance_data = trading_exchange.fetch_balance()
                balance = float(balance_data['total'].get('USDT', 0))
                break
            except Exception as bal_err:
                if bal_attempt < 2:
                    print(f"   🔁 Balance fetch failed — retrying in 5s: {bal_err}", flush=True)
                    await asyncio.sleep(5)
                else:
                    print(f"   ❌ Could not fetch balance after 3 attempts — skipping cycle", flush=True)
                    return False
        if balance is None or balance <= 0:
            print(f"   ❌ Invalid balance (${balance}) — skipping cycle", flush=True)
            return False
        print(f"   💰 Balance: ${balance:,.2f} USDT", flush=True)

        if session_start_balance and session_start_balance > 0:
            session_pnl = balance - session_start_balance
            print(f"   📊 Session PnL: ${session_pnl:+,.2f} ({session_pnl / session_start_balance * 100:+.2f}%)", flush=True)

        decision = await grok_decision(enriched, balance)
        print(f"   🤖 Grok picks: {decision.get('symbol', 'none')} "
              f"{decision.get('action', 'hold')} "
              f"(confidence: {decision.get('confidence', 0):.2f})", flush=True)

        opened = await execute_trade(decision, balance)
        return opened

    except (ccxt.DDoSProtection, ccxt.ExchangeNotAvailable, ccxt.RequestTimeout, ccxt.NetworkError) as e:
        print(f"   🚫 Exchange error during scan — skipping this cycle: {str(e)[:80]}", flush=True)
        return False
    except ccxt.ExchangeError as e:
        print(f"   ⚠️ Binance error during scan — skipping this cycle: {str(e)[:80]}", flush=True)
        return False
    except Exception as e:
        print(f"   ❌ Unexpected scan error: {e}", flush=True)
        traceback.print_exc()
        return False


# ============================================================
#  CLOSE DETECTION
# ============================================================

def handle_position_close(symbol):
    # Wait for Binance to settle the trade
    time.sleep(3)
    try:
        raw_sym = symbol_to_binance_raw(symbol)
        incomes = trading_exchange.fapiprivate_get_income({
            'symbol': raw_sym,
            'incomeType': 'REALIZED_PNL', 'limit': 5,
        })
        if incomes:
            # Sum recent PnL entries (may be split across partial fills)
            realized_pnl = sum(float(i.get('income', 0)) for i in incomes[-3:])
            try:
                trades = trading_exchange.fetch_my_trades(symbol, limit=1)
                exit_price = float(trades[-1]['price']) if trades else 0
            except Exception:
                exit_price = 0
            log_trade_close(exit_price, realized_pnl)
            icon = '✅' if realized_pnl >= 0 else '❌'
            print(f"   {icon} Realized PnL: ${realized_pnl:+,.2f}", flush=True)
        else:
            print(f"   ⚠️ No realized PnL data from Binance", flush=True)
            log_trade_close(0, 0)
    except Exception as e:
        print(f"   ⚠️ Could not fetch realized PnL: {e}", flush=True)
        log_trade_close(0, 0)

    # Cancel any remaining orders
    cancel_all_open_orders(symbol)
    reset_sl_tp_tracking()


# ============================================================
#  MAIN LOOP
# ============================================================

last_position_symbol = None
had_position_last_cycle = False

async def main_loop():
    global last_position_symbol, had_position_last_cycle, session_start_balance

    print("🚀 High Leverage Top20 Scanner is now RUNNING on DEMO (Cross Margin)", flush=True)
    print(f"📊 Scanning every {INTERVAL_MINUTES} minutes | Risk per trade: {MAX_RISK_PERCENT}% | Max margin: {MAX_MARGIN_PERCENT}%", flush=True)
    print(f"📐 Risk-based sizing: position = risk_budget / SL_distance", flush=True)
    print(f"📌 Strict single position — new signals ignored while position is open", flush=True)
    print(f"🔬 Enhanced: RSI, EMA, ATR, Bollinger, VWAP, order book, multi-timeframe", flush=True)
    print("=" * 60, flush=True)

    # Balance fetch with retry
    for _ in range(10):
        try:
            bal = trading_exchange.fetch_balance()
            session_start_balance = float(bal['total'].get('USDT', 0))
            print(f"💰 Session starting balance: ${session_start_balance:,.2f}", flush=True)
            print(f"💵 Risk budget per trade: ${session_start_balance * MAX_RISK_PERCENT / 100:,.2f} ({MAX_RISK_PERCENT}%)", flush=True)
            print(f"🏦 Max margin per trade: ${session_start_balance * MAX_MARGIN_PERCENT / 100:,.2f} ({MAX_MARGIN_PERCENT}%)", flush=True)
            break
        except (ccxt.DDoSProtection, ccxt.ExchangeNotAvailable) as e:
            print(f"   ⚠️ Rate limited fetching balance — waiting 60s: {str(e)[:80]}", flush=True)
            await asyncio.sleep(60)
        except Exception:
            session_start_balance = 0
            break

    get_cached_markets()

    # Cleanup with retry
    for _ in range(3):
        try:
            cleanup_all_orders()
            break
        except (ccxt.DDoSProtection, ccxt.ExchangeNotAvailable):
            print(f"   ⚠️ Rate limited during cleanup — waiting 60s", flush=True)
            await asyncio.sleep(60)
        except Exception as e:
            print(f"   ⚠️ Cleanup error: {e}", flush=True)
            break

    # Check for existing position on startup
    position = get_open_position()
    if position:
        print(f"   📍 Found existing position: {position['side'].upper()} {position['symbol']}", flush=True)
        print(f"   🤖 Grok will evaluate this position on first cycle", flush=True)
        # Cancel any stale orders from previous runs
        try:
            raw_sym = symbol_to_binance_raw(position['symbol'])
            trading_exchange.fapiprivate_delete_allopenorders({'symbol': raw_sym})
            print(f"   🧹 Cancelled stale orders on {position['symbol']}", flush=True)
        except Exception:
            pass
        last_position_symbol = position['symbol']
        had_position_last_cycle = True

    try:
        print_income_summary()
    except Exception:
        pass

    # Clean up any ghost trade_log entries from previous runs
    _close_stale_trade_log()

    cycle_count = 0

    while True:
        try:
            cycle_count += 1
            now = datetime.now(UTC).strftime('%H:%M:%S')
            position = get_open_position()

            if position:
                last_position_symbol = position['symbol']
                had_position_last_cycle = True
                pnl = position['unrealized_pnl']
                pnl_pct = (pnl / position['notional'] * 100) if position['notional'] else 0
                print(f"\n[{now}] 📍 OPEN: {position['side'].upper()} {position['symbol']} | "
                      f"Entry ${position['entry_price']:,.2f} | {position['leverage']}x | "
                      f"Notional ${position['notional']:,.0f} | "
                      f"Contracts: {position['contracts']} | "
                      f"PnL: ${pnl:+,.2f} ({pnl_pct:+.2f}%)", flush=True)

                # Ask Grok: keep or close?
                closed = await manage_position(position)
                if closed:
                    had_position_last_cycle = False
                    last_position_symbol = None
                    try:
                        handle_position_close(position['symbol'])
                    except Exception as hpc_err:
                        print(f"   ⚠️ Error in close handler: {hpc_err}", flush=True)
                    print_pnl_dashboard()

                    # Immediate rescan after closing
                    print(f"\n[{now}] 🔍 Immediate rescan after Grok-close...", flush=True)
                    opened = await scan_and_trade()
                    if opened:
                        had_position_last_cycle = True
                        pos = get_open_position()
                        if pos:
                            last_position_symbol = pos['symbol']

            else:
                if had_position_last_cycle and last_position_symbol:
                    # Position was closed externally (liquidation, manual, TP hit)
                    await asyncio.sleep(2)
                    recheck = get_open_position()
                    if recheck:
                        print(f"\n[{now}] ⚠️ Position re-detected after transient glitch — {recheck['side'].upper()} {recheck['symbol']}", flush=True)
                        last_position_symbol = recheck['symbol']
                        had_position_last_cycle = True
                    else:
                        print(f"\n[{now}] 🔔 Position CLOSED on {last_position_symbol}!", flush=True)
                        closed_sym = last_position_symbol
                        last_position_symbol = None
                        had_position_last_cycle = False
                        try:
                            handle_position_close(closed_sym)
                        except Exception as hpc_err:
                            print(f"   ⚠️ Error in close handler: {hpc_err}", flush=True)
                        print_pnl_dashboard()

                        # Immediate rescan
                        print(f"\n[{now}] 🔍 Immediate rescan after close...", flush=True)
                        opened = await scan_and_trade()
                        if opened:
                            had_position_last_cycle = True
                            pos = get_open_position()
                            if pos:
                                last_position_symbol = pos['symbol']

                else:
                    _close_stale_trade_log()
                    print(f"\n[{now}] 🔍 No open position — scanning top 20 coins...", flush=True)
                    opened = await scan_and_trade()
                    if opened:
                        had_position_last_cycle = True
                        pos = get_open_position()
                        if pos:
                            last_position_symbol = pos['symbol']

            if cycle_count % 10 == 0:
                print_pnl_dashboard()
                print_income_summary()

        except (ccxt.DDoSProtection, ccxt.ExchangeNotAvailable) as e:
            ban_msg = str(e)
            wait = 120
            if 'banned until' in ban_msg:
                try:
                    ban_ts = int(''.join(c for c in ban_msg.split('banned until')[1].split('.')[0].strip() if c.isdigit()))
                    ban_remaining = max(0, (ban_ts / 1000) - time.time())
                    if ban_remaining > 0:
                        wait = min(ban_remaining + 30, 600)
                except Exception:
                    pass
            print(f"   🚫 Binance rate limit/ban: {ban_msg[:100]}", flush=True)
            print(f"   ⏳ Sleeping {wait:.0f}s...", flush=True)
            await asyncio.sleep(wait)
            continue

        except Exception as e:
            print(f"   ❌ Loop error: {e}", flush=True)
            traceback.print_exc()

        # Simple sleep — Grok re-evaluates next cycle
        await asyncio.sleep(INTERVAL_MINUTES * 60)


# Crash-proof wrapper — restart on unexpected death
def run_forever():
    while True:
        try:
            print("Starting main loop...", flush=True)
            asyncio.run(main_loop())
        except KeyboardInterrupt:
            print("Shutting down...", flush=True)
            break
        except Exception as e:
            print(f"💀 FATAL: Main loop died — {e}", flush=True)
            traceback.print_exc()
            print(f"🔄 Restarting in 60s...", flush=True)
            time.sleep(60)


run_forever()
