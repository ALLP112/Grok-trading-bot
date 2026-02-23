"""
HIGH LEVERAGE TOP100 SCANNER — Grok 4.1 Thinking (Cross Margin)
Enhanced with technical indicators, multi-timeframe analysis, order book data
Risk-based position sizing: size = risk_budget / SL_distance
Scans top 100 coins, strictly one position at a time
Binance Demo Trading — runs 24/7 on Render.com
"""

import os
import sys
import json
import asyncio
import math
import time
import re
from datetime import datetime, UTC
from openai import AsyncOpenAI
import ccxt
from dotenv import load_dotenv

print("=== HIGH LEVERAGE TOP100 SCANNER STARTING ===", flush=True)
print("Grok 4.1 Thinking + Risk-Based Sizing + Cross Margin", flush=True)

load_dotenv()

# === SETTINGS ===
XAI_API_KEY = os.getenv("XAI_API_KEY")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")
INTERVAL_MINUTES = int(os.getenv("INTERVAL_MINUTES", "30"))
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
        _markets_cache = public_exchange.load_markets()
        trading_exchange.load_markets()
        _markets_cache_time = now
        print(f"   🔄 Markets refreshed ({len(_markets_cache)} loaded)", flush=True)
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
                ConnectionError, TimeoutError) as e:
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
                ConnectionError, TimeoutError) as e:
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
        return 100.0
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
            label="fetch_positions"
        )
        for pos in positions:
            contracts = float(pos.get('contracts', 0) or 0)
            if contracts != 0:
                lev = pos.get('leverage')
                try:
                    lev = int(float(lev)) if lev is not None else 1
                except (ValueError, TypeError):
                    lev = 1
                return {
                    'symbol': pos['symbol'],
                    'side': 'long' if contracts > 0 else 'short',
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


def has_sl_tp_orders(symbol):
    try:
        orders = trading_exchange.fetch_open_orders(symbol)
        has_sl = any(o.get('type', '').upper() in ('STOP_MARKET', 'STOP') for o in orders)
        has_tp = any(o.get('type', '').upper() in ('TAKE_PROFIT_MARKET', 'TAKE_PROFIT') for o in orders)
        return has_sl, has_tp, orders
    except Exception:
        return False, False, []


def place_emergency_sl_tp(position):
    symbol = position['symbol']
    side = position['side']
    entry = position['entry_price']
    if entry <= 0:
        print(f"   ⚠️ Cannot place emergency SL/TP — entry price unknown", flush=True)
        return

    # Wait for Binance to fully register the position
    time.sleep(3)

    # Re-verify position still exists
    current_pos = get_open_position()
    if not current_pos or current_pos['symbol'] != symbol:
        print(f"   ⚠️ Position no longer exists — skipping emergency SL/TP", flush=True)
        return

    contracts = current_pos['contracts']

    if side == 'long':
        sl = round_price(entry * 0.97, symbol)
        tp = round_price(entry * 1.045, symbol)
        order_side = 'sell'
    else:
        sl = round_price(entry * 1.03, symbol)
        tp = round_price(entry * 0.955, symbol)
        order_side = 'buy'

    # Try closePosition first, fall back to amount-based
    for use_close_pos in [True, False]:
        try:
            params_sl = {'stopPrice': sl}
            params_tp = {'stopPrice': tp}
            amount = contracts

            if use_close_pos:
                params_sl['closePosition'] = True
                params_tp['closePosition'] = True

            trading_exchange.create_order(
                symbol, 'STOP_MARKET', order_side, amount,
                None, params_sl
            )
            trading_exchange.create_order(
                symbol, 'TAKE_PROFIT_MARKET', order_side, amount,
                None, params_tp
            )
            print(f"   🚨 Emergency SL/TP placed on {symbol}: SL ${sl:,.2f} / TP ${tp:,.2f}"
                  f"{'' if use_close_pos else ' (amount-based)'}", flush=True)
            return
        except Exception as e:
            if use_close_pos and ('-4509' in str(e) or 'GTE' in str(e)):
                print(f"   ⚠️ closePosition rejected — retrying with amount-based orders...", flush=True)
                continue
            else:
                print(f"   ❌ Failed to place emergency SL/TP: {e}", flush=True)
                return


def cancel_all_open_orders(symbol):
    try:
        open_orders = trading_exchange.fetch_open_orders(symbol)
        for order in open_orders:
            trading_exchange.cancel_order(order['id'], symbol)
        if open_orders:
            print(f"   🧹 Cancelled {len(open_orders)} open orders on {symbol}", flush=True)
    except Exception as e:
        print(f"   ⚠️ Error cancelling orders on {symbol}: {e}", flush=True)


def cleanup_all_orders():
    try:
        all_orders = trading_exchange.fetch_open_orders()
        if all_orders:
            position = get_open_position()
            pos_symbol = position['symbol'] if position else None
            orphan_orders = [o for o in all_orders if o['symbol'] != pos_symbol]
            if orphan_orders:
                print(f"   🧹 Found {len(orphan_orders)} orphan orders — cleaning up...", flush=True)
                for order in orphan_orders:
                    try:
                        trading_exchange.cancel_order(order['id'], order['symbol'])
                    except Exception:
                        pass
                print(f"   🧹 Cleanup complete", flush=True)
            if position:
                print(f"   📍 Existing position found on {pos_symbol} — keeping its orders", flush=True)
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

async def get_top_candidates(n=100):
    get_cached_markets()
    async def _fetch():
        return public_exchange.fetch_tickers()
    tickers = await retry_async(_fetch, label="fetch_tickers")
    markets = public_exchange.markets or {}
    candidates = []
    for symbol, ticker in tickers.items():
        market = markets.get(symbol, {})
        if ('USDT' in symbol
                and market.get('swap', False)
                and market.get('active', True)):
            vol = ticker.get('quoteVolume') or 0
            candidates.append({
                'symbol': symbol,
                'price': ticker.get('last', 0),
                'change24h': ticker.get('percentage', 0),
                'volume': vol,
            })
    candidates.sort(key=lambda x: x['volume'], reverse=True)
    return candidates[:n]


async def deep_enrich(candidates, top_n=15):
    enriched = []
    for i, c in enumerate(candidates[:top_n]):
        symbol = c['symbol']
        try:
            funding = public_exchange.fetch_funding_rate(symbol).get('fundingRate', 0.0)
            c['funding'] = funding
            await asyncio.sleep(0.15)
            candles_15m = public_exchange.fetch_ohlcv(symbol, '15m', limit=50)
            c['ta_15m'] = analyze_candles(candles_15m)
            await asyncio.sleep(0.15)
            candles_1h = public_exchange.fetch_ohlcv(symbol, '1h', limit=50)
            c['ta_1h'] = analyze_candles(candles_1h)
            await asyncio.sleep(0.15)
            if i < 5:
                ob = public_exchange.fetch_order_book(symbol, limit=20)
                c['order_book'] = analyze_order_book(ob)
                await asyncio.sleep(0.15)
            else:
                c['order_book'] = None
        except Exception as e:
            print(f"   ⚠️ Enrich error on {symbol}: {e}", flush=True)
            c['funding'] = c.get('funding', 0.0)
            c['ta_15m'] = c.get('ta_15m')
            c['ta_1h'] = c.get('ta_1h')
            c['order_book'] = None
        enriched.append(c)
    return enriched


# ============================================================
#  GROK AI DECISION
# ============================================================

async def grok_decision(candidates, balance):
    valid_symbols = [c['symbol'] for c in candidates]
    lines = []
    for c in candidates:
        ta15 = c.get('ta_15m')
        ta1h = c.get('ta_1h')
        ob = c.get('order_book')
        line = f"\n--- {c['symbol']} ---\n"
        line += f"Price: ${c['price']:,.2f} | 24h: {c['change24h']:+.2f}% | Vol: ${c['volume'] / 1e9:.1f}B | Funding: {c.get('funding', 0) * 100:.4f}%\n"
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
        if ob:
            line += f"Book: {ob['imbalance_label']} (imb {ob['imbalance']:+.2f}) | "
            line += f"Spread {ob['spread_pct']:.4f}% | "
            line += f"Bid wall ${ob['bid_wall']:,.2f} | Ask wall ${ob['ask_wall']:,.2f}\n"
        lines.append(line)

    data_str = "".join(lines)
    perf_summary = get_recent_performance_summary()
    risk_budget = balance * MAX_RISK_PERCENT / 100

    prompt = f"""You are **AlphaEdge**, an elite quantitative futures trader using first-principles analysis.
You combine technical analysis, order flow, and multi-timeframe confluence to find high-leverage edges.

═══ MARKET CONTEXT ═══
Current time: {datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")}
Available balance: {balance:.2f} USDT
Risk per trade: {MAX_RISK_PERCENT}% = ${risk_budget:.2f} (max loss if SL hits)
Position sizing: HANDLED AUTOMATICALLY by the system using risk-based math.
  → You only need to pick the trade, SL, TP, and leverage. The system calculates position size.
  → Tighter SL = larger position. Wider SL = smaller position. Same risk either way.
Max margin per trade: {MAX_MARGIN_PERCENT}% of balance (${balance * MAX_MARGIN_PERCENT / 100:.0f})
Scan interval: every {INTERVAL_MINUTES} minutes

═══ YOUR RECENT PERFORMANCE ═══
{perf_summary}

═══ TOP {len(candidates)} CANDIDATES (enriched with 15m + 1h technicals + order book) ═══
{data_str}

═══ ANALYSIS FRAMEWORK ═══
For each candidate, evaluate:
1. TREND ALIGNMENT: Do 15m and 1h EMAs agree? Is price above/below VWAP?
2. MOMENTUM: RSI divergence? Candle streak strength? 5-candle momentum direction?
3. VOLATILITY: Is ATR high enough for the leverage you're using? Bollinger position?
4. VOLUME CONFIRMATION: Is volume ratio > 1.5 (above average)? Rising or fading?
5. ORDER FLOW: Is the book imbalanced in your trade direction? Any walls to watch?
6. SUPPORT/RESISTANCE: Is entry near S/R? Is there room to TP before next resistance?
7. RISK/REWARD: SL at technical level. TP ≥2x the SL distance for good R:R.

═══ RULES ═══
- Pick the SINGLE BEST setup with multi-timeframe confluence, or HOLD if nothing aligns.
- CRITICAL: The "symbol" in your JSON must be EXACTLY one of the symbols listed above.
- You MUST set stop_loss and take_profit at TECHNICAL levels (support/resistance, BBands, recent high/low).
- For LONG: stop_loss MUST be BELOW entry price, take_profit MUST be ABOVE entry price.
- For SHORT: stop_loss MUST be ABOVE entry price, take_profit MUST be BELOW entry price.
- SL should be between 0.5% and 5% from entry at a meaningful technical level.
- TP must give at least 2:1 reward:risk ratio.
- Leverage 10-20x when both timeframes align and volume confirms. 5-10x when mixed.
- Keep leverage ≤15x for altcoins outside top 10 by volume.
- Only trade if confidence >= 0.74.
- You do NOT need to set size_usdt — the system auto-calculates based on risk budget and SL distance.

═══ RESPOND WITH ONLY VALID JSON ═══
{{
  "symbol": "EXACT symbol from the list above",
  "action": "long" | "short" | "hold",
  "leverage": integer 1-20,
  "stop_loss": number (BELOW entry for long, ABOVE entry for short — at a technical level),
  "take_profit": number (ABOVE entry for long, BELOW entry for short — ≥2x SL distance),
  "confidence": 0.00-1.00,
  "reason": "2-3 sentences citing specific indicators that create confluence"
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
        raw_symbol = decision.get('symbol', '')
        normalized = normalize_symbol(raw_symbol)
        if normalized not in valid_symbols:
            raw_clean = symbol_to_binance_raw(raw_symbol)
            for vs in valid_symbols:
                if symbol_to_binance_raw(vs) == raw_clean:
                    normalized = vs
                    break
        decision['symbol'] = normalized
        return decision
    except Exception as e:
        print(f"   ⚠️ Decision parsing error: {e}", flush=True)
        return {"action": "hold", "confidence": 0}


# ============================================================
#  TRADE EXECUTION
# ============================================================

async def execute_trade(decision, balance):
    action = decision.get("action", "hold")
    confidence = decision.get("confidence", 0)
    symbol = decision.get("symbol")
    sl = decision.get("stop_loss")
    tp = decision.get("take_profit")

    if action == "hold" or confidence < 0.74:
        print(f"   ⏸️  HOLD — confidence {confidence:.2f} | {decision.get('reason', 'n/a')}", flush=True)
        return False

    if not symbol or not sl or not tp:
        print(f"   ⚠️ Missing symbol/SL/TP in Grok response — skipping", flush=True)
        return False

    try:
        get_cached_markets()
        cancel_all_open_orders(symbol)
        await asyncio.sleep(1)

        try:
            trading_exchange.set_margin_mode('cross', symbol)
        except Exception:
            pass

        # Set leverage — auto-reduce if rejected
        leverage = min(decision.get("leverage", 10), 20)
        for lev in [leverage, 15, 10, 7, 5, 3]:
            try:
                trading_exchange.set_leverage(lev, symbol)
                leverage = lev
                break
            except Exception as e:
                if '-4028' in str(e) and lev > 3:
                    print(f"   ⚠️ {lev}x not supported for {symbol}, trying lower...", flush=True)
                    continue
                else:
                    raise
        print(f"   ⚙️ Set leverage: {leverage}x", flush=True)

        # Get current price
        ticker = public_exchange.fetch_ticker(symbol)
        current_price = ticker['last']

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

        # Place entry order
        entry_side = "buy" if action == "long" else "sell"
        trading_exchange.create_market_order(symbol, entry_side, amount)

        # Place stop loss
        sl_side = "sell" if action == "long" else "buy"
        trading_exchange.create_order(
            symbol, 'STOP_MARKET', sl_side, amount,
            None, {'stopPrice': sl, 'closePosition': True}
        )

        # Place take profit
        tp_side = "sell" if action == "long" else "buy"
        trading_exchange.create_order(
            symbol, 'TAKE_PROFIT_MARKET', tp_side, amount,
            None, {'stopPrice': tp, 'closePosition': True}
        )

        # Log the trade
        log_trade_open(symbol, action, actual_notional, leverage, current_price, sl, tp,
                       confidence, decision.get('reason', ''), actual_margin, actual_risk)

        # R:R ratio
        if action == "long":
            risk = abs(current_price - sl)
            reward = abs(tp - current_price)
        else:
            risk = abs(sl - current_price)
            reward = abs(current_price - tp)
        rr = reward / risk if risk > 0 else 0

        print(f"   🔥 OPENED {action.upper()} {symbol} @ ${current_price:,.2f} | {leverage}x Cross", flush=True)
        print(f"   📐 Notional: ${actual_notional:,.0f} | Margin: ${actual_margin:,.0f} | Risk: ${actual_risk:,.2f} ({MAX_RISK_PERCENT}%)", flush=True)
        print(f"   🛑 SL: ${sl:,.2f} (-{sl_dist_pct:.1f}%) | 🎯 TP: ${tp:,.2f} (+{tp_dist_pct:.1f}%) | R:R {rr:.1f}:1", flush=True)
        print(f"   💡 {decision.get('reason', 'n/a')}", flush=True)
        return True

    except Exception as e:
        print(f"   ❌ Execution error on {symbol}: {e}", flush=True)
        return False


# ============================================================
#  SCAN + TRADE
# ============================================================

async def scan_and_trade():
    candidates = await get_top_candidates(100)
    print(f"   📈 Found {len(candidates)} futures", flush=True)

    print(f"   🔬 Enriching top 15 with technicals + order book...", flush=True)
    enriched = await deep_enrich(candidates, 15)

    balance_data = trading_exchange.fetch_balance()
    balance = float(balance_data['total'].get('USDT', 0))
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


# ============================================================
#  CLOSE DETECTION
# ============================================================

def handle_position_close(symbol):
    try:
        raw_sym = symbol_to_binance_raw(symbol)
        incomes = trading_exchange.fapiprivate_get_income({
            'symbol': raw_sym,
            'incomeType': 'REALIZED_PNL', 'limit': 1,
        })
        if incomes:
            realized_pnl = float(incomes[-1].get('income', 0))
            try:
                trades = trading_exchange.fetch_my_trades(symbol, limit=1)
                exit_price = float(trades[-1]['price']) if trades else 0
            except Exception:
                exit_price = 0
            log_trade_close(exit_price, realized_pnl)
            icon = '✅' if realized_pnl >= 0 else '❌'
            print(f"   {icon} Realized PnL: ${realized_pnl:+,.2f}", flush=True)
    except Exception as e:
        print(f"   ⚠️ Could not fetch realized PnL: {e}", flush=True)

    cancel_all_open_orders(symbol)


# ============================================================
#  MAIN LOOP
# ============================================================

last_position_symbol = None
had_position_last_cycle = False

async def main_loop():
    global last_position_symbol, had_position_last_cycle, session_start_balance

    print("🚀 High Leverage Top100 Scanner is now RUNNING on DEMO (Cross Margin)", flush=True)
    print(f"📊 Scanning every {INTERVAL_MINUTES} minutes | Risk per trade: {MAX_RISK_PERCENT}% | Max margin: {MAX_MARGIN_PERCENT}%", flush=True)
    print(f"📐 Risk-based sizing: position = risk_budget / SL_distance", flush=True)
    print(f"📌 Strict single position — new signals ignored while position is open", flush=True)
    print(f"🔬 Enhanced: RSI, EMA, ATR, Bollinger, VWAP, order book, multi-timeframe", flush=True)
    print("=" * 60, flush=True)

    try:
        bal = trading_exchange.fetch_balance()
        session_start_balance = float(bal['total'].get('USDT', 0))
        print(f"💰 Session starting balance: ${session_start_balance:,.2f}", flush=True)
        print(f"💵 Risk budget per trade: ${session_start_balance * MAX_RISK_PERCENT / 100:,.2f} ({MAX_RISK_PERCENT}%)", flush=True)
        print(f"🏦 Max margin per trade: ${session_start_balance * MAX_MARGIN_PERCENT / 100:,.2f} ({MAX_MARGIN_PERCENT}%)", flush=True)
    except Exception:
        session_start_balance = 0

    get_cached_markets()
    cleanup_all_orders()

    # Check for orphaned position on startup
    position = get_open_position()
    if position:
        has_sl, has_tp, _ = has_sl_tp_orders(position['symbol'])
        if not has_sl or not has_tp:
            print(f"   🚨 Orphaned position detected: {position['side'].upper()} {position['symbol']} "
                  f"(SL: {'✅' if has_sl else '❌'}, TP: {'✅' if has_tp else '❌'})", flush=True)
            cancel_all_open_orders(position['symbol'])
            place_emergency_sl_tp(position)
        else:
            print(f"   📍 Existing position with SL/TP intact: {position['side'].upper()} {position['symbol']}", flush=True)
        last_position_symbol = position['symbol']
        had_position_last_cycle = True

    print_income_summary()
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
                      f"PnL: ${pnl:+,.2f} ({pnl_pct:+.2f}%)", flush=True)

                # Verify SL/TP orders still exist — re-place if missing
                has_sl, has_tp, _ = has_sl_tp_orders(position['symbol'])
                if not has_sl or not has_tp:
                    print(f"   🚨 Missing orders (SL: {'✅' if has_sl else '❌'}, TP: {'✅' if has_tp else '❌'}) — re-placing...", flush=True)
                    cancel_all_open_orders(position['symbol'])
                    place_emergency_sl_tp(position)
                else:
                    print(f"   ⏳ Waiting for SL/TP to trigger — not scanning for new trades", flush=True)

            else:
                if had_position_last_cycle and last_position_symbol:
                    print(f"\n[{now}] 🔔 Position CLOSED on {last_position_symbol}!", flush=True)
                    handle_position_close(last_position_symbol)
                    last_position_symbol = None
                    had_position_last_cycle = False
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
                    print(f"\n[{now}] 🔍 No open position — scanning top 100 coins...", flush=True)
                    opened = await scan_and_trade()
                    if opened:
                        had_position_last_cycle = True
                        pos = get_open_position()
                        if pos:
                            last_position_symbol = pos['symbol']

            if cycle_count % 10 == 0:
                print_pnl_dashboard()
                print_income_summary()

        except Exception as e:
            print(f"Loop error: {e}", flush=True)

        await asyncio.sleep(INTERVAL_MINUTES * 60)


print("Starting main loop...", flush=True)
asyncio.run(main_loop())
