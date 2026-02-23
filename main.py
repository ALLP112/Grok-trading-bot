"""
HIGH LEVERAGE TOP100 SCANNER — Grok 4.1 Thinking (Cross Margin)
Enhanced with technical indicators, multi-timeframe analysis, order book data
Scans top 100 coins, strictly one position at a time
Binance Demo Trading — runs 24/7 on Render.com
"""

import os
import sys
import json
import asyncio
import math
from datetime import datetime, UTC
from openai import AsyncOpenAI
import ccxt
from dotenv import load_dotenv

print("=== HIGH LEVERAGE TOP100 SCANNER STARTING ===", flush=True)
print("Grok 4.1 Thinking + Enhanced Technical Analysis + Cross Margin", flush=True)

load_dotenv()

# === SETTINGS ===
XAI_API_KEY = os.getenv("XAI_API_KEY")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")
INTERVAL_MINUTES = int(os.getenv("INTERVAL_MINUTES", "30"))
MAX_RISK_PERCENT = float(os.getenv("MAX_RISK_PERCENT", "2.5"))

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
    'options': {'defaultType': 'future'},
})
trading_exchange.enable_demo_trading(True)
print("✅ Connected to Binance DEMO for trading (cross margin)", flush=True)


# ============================================================
#  TECHNICAL INDICATORS
# ============================================================

def calc_rsi(closes, period=14):
    """Calculate RSI from a list of closing prices."""
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
    """Calculate EMA from a list of values."""
    if len(values) < period:
        return values[-1] if values else 0
    multiplier = 2 / (period + 1)
    ema = sum(values[:period]) / period
    for val in values[period:]:
        ema = (val - ema) * multiplier + ema
    return ema


def calc_atr(highs, lows, closes, period=14):
    """Calculate Average True Range."""
    if len(closes) < period + 1:
        return 0
    trs = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1])
        )
        trs.append(tr)
    if len(trs) < period:
        return sum(trs) / len(trs) if trs else 0
    return sum(trs[-period:]) / period


def calc_vwap(highs, lows, closes, volumes):
    """Calculate approximate VWAP."""
    if not closes or not volumes:
        return closes[-1] if closes else 0
    typical_prices = [(h + l + c) / 3 for h, l, c in zip(highs, lows, closes)]
    cum_tp_vol = sum(tp * v for tp, v in zip(typical_prices, volumes))
    cum_vol = sum(volumes)
    return cum_tp_vol / cum_vol if cum_vol > 0 else closes[-1]


def calc_bollinger(closes, period=20, std_dev=2):
    """Calculate Bollinger Bands."""
    if len(closes) < period:
        return closes[-1], closes[-1], closes[-1]
    sma = sum(closes[-period:]) / period
    variance = sum((c - sma) ** 2 for c in closes[-period:]) / period
    std = math.sqrt(variance)
    return sma - std_dev * std, sma, sma + std_dev * std


def analyze_candles(candles):
    """Extract full technical analysis from OHLCV candles."""
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

    # Volume analysis
    avg_vol_20 = sum(volumes[-20:]) / 20 if len(volumes) >= 20 else sum(volumes) / len(volumes)
    current_vol = volumes[-1]
    vol_ratio = current_vol / avg_vol_20 if avg_vol_20 > 0 else 1

    # Recent high/low as support/resistance
    recent_high = max(highs[-20:])
    recent_low = min(lows[-20:])

    # Price momentum (% change over last 5 candles)
    if len(closes) >= 6:
        momentum_5 = (closes[-1] - closes[-6]) / closes[-6] * 100
    else:
        momentum_5 = 0

    # Consecutive green/red candles
    streak = 0
    for i in range(len(closes) - 1, 0, -1):
        if closes[i] > opens[i]:  # Green candle
            if streak >= 0:
                streak += 1
            else:
                break
        elif closes[i] < opens[i]:  # Red candle
            if streak <= 0:
                streak -= 1
            else:
                break
        else:
            break

    return {
        'rsi': rsi,
        'ema9': ema9,
        'ema21': ema21,
        'ema50': ema50,
        'ema_trend': 'bullish' if ema9 > ema21 > ema50 else ('bearish' if ema9 < ema21 < ema50 else 'mixed'),
        'atr': atr,
        'atr_pct': atr_pct,
        'vwap': vwap,
        'price_vs_vwap': 'above' if current > vwap else 'below',
        'bb_lower': bb_lower,
        'bb_upper': bb_upper,
        'bb_position': 'near_upper' if current > bb_upper * 0.98 else ('near_lower' if current < bb_lower * 1.02 else 'mid'),
        'vol_ratio': vol_ratio,
        'recent_high': recent_high,
        'recent_low': recent_low,
        'momentum_5': momentum_5,
        'candle_streak': streak,
    }


def analyze_order_book(order_book):
    """Extract order book imbalance and key levels."""
    if not order_book:
        return None

    bids = order_book.get('bids', [])
    asks = order_book.get('asks', [])

    if not bids or not asks:
        return None

    # Top 20 levels each side
    bid_depth = sum(b[1] * b[0] for b in bids[:20])
    ask_depth = sum(a[1] * a[0] for a in asks[:20])
    total_depth = bid_depth + ask_depth

    imbalance = (bid_depth - ask_depth) / total_depth if total_depth > 0 else 0

    spread = asks[0][0] - bids[0][0]
    spread_pct = (spread / bids[0][0] * 100) if bids[0][0] > 0 else 0

    # Find largest bid/ask walls
    biggest_bid = max(bids[:20], key=lambda x: x[1] * x[0]) if bids else [0, 0]
    biggest_ask = max(asks[:20], key=lambda x: x[1] * x[0]) if asks else [0, 0]

    return {
        'bid_depth_usdt': bid_depth,
        'ask_depth_usdt': ask_depth,
        'imbalance': imbalance,
        'imbalance_label': 'buy_heavy' if imbalance > 0.15 else ('sell_heavy' if imbalance < -0.15 else 'balanced'),
        'spread_pct': spread_pct,
        'bid_wall': biggest_bid[0],
        'ask_wall': biggest_ask[0],
    }


# ============================================================
#  PNL TRACKING
# ============================================================

trade_log = []
session_start_balance = None


def fetch_trade_history():
    try:
        incomes = trading_exchange.fapiprivate_get_income({
            'incomeType': 'REALIZED_PNL',
            'limit': 100,
        })
        return incomes
    except Exception as e:
        print(f"   ⚠️ Could not fetch income history: {e}", flush=True)
        return []


def log_trade_open(symbol, action, size_usdt, leverage, entry_price, sl, tp, confidence, reason):
    trade = {
        'symbol': symbol, 'action': action, 'size_usdt': size_usdt,
        'leverage': leverage, 'entry_price': entry_price, 'sl': sl, 'tp': tp,
        'confidence': confidence, 'reason': reason,
        'opened_at': datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC'),
        'closed_at': None, 'exit_price': None, 'pnl': None, 'pnl_pct': None, 'result': None,
    }
    trade_log.append(trade)
    return trade


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
                  f"Entry ${t['entry_price']:,.2f} | SL ${t['sl']:,.2f} / TP ${t['tp']:,.2f}", flush=True)
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
    """Build a performance string to feed back to Grok."""
    completed = [t for t in trade_log if t['closed_at'] is not None]
    if not completed:
        # Try Binance history
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
        positions = trading_exchange.fetch_positions()
        for pos in positions:
            contracts = float(pos.get('contracts', 0))
            if contracts != 0:
                return {
                    'symbol': pos['symbol'],
                    'side': 'long' if contracts > 0 else 'short',
                    'contracts': abs(contracts),
                    'entry_price': float(pos.get('entryPrice', 0)),
                    'unrealized_pnl': float(pos.get('unrealizedPnl', 0)),
                    'leverage': int(pos.get('leverage', 1)),
                    'notional': abs(float(pos.get('notional', 0))),
                }
        return None
    except Exception as e:
        print(f"   ⚠️ Error fetching positions: {e}", flush=True)
        return None


def cancel_open_orders(symbol):
    try:
        open_orders = trading_exchange.fetch_open_orders(symbol)
        for order in open_orders:
            trading_exchange.cancel_order(order['id'], symbol)
        if open_orders:
            print(f"   🧹 Cancelled {len(open_orders)} leftover orders on {symbol}", flush=True)
    except Exception as e:
        print(f"   ⚠️ Error cancelling orders: {e}", flush=True)


# ============================================================
#  MARKET SCANNING WITH TECHNICAL ENRICHMENT
# ============================================================

async def get_top_candidates(n=100):
    markets = public_exchange.load_markets()
    tickers = public_exchange.fetch_tickers()
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
    """
    Enrich the top N candidates with:
    - Funding rate
    - 15m candles → RSI, EMA, ATR, volume ratio, Bollinger, momentum
    - 1h candles → higher timeframe trend
    - Order book snapshot (top 5 only)
    """
    enriched = []
    for i, c in enumerate(candidates[:top_n]):
        symbol = c['symbol']
        try:
            # Funding rate
            funding = public_exchange.fetch_funding_rate(symbol).get('fundingRate', 0.0)
            c['funding'] = funding

            # 15-minute candles (last 50)
            candles_15m = public_exchange.fetch_ohlcv(symbol, '15m', limit=50)
            ta_15m = analyze_candles(candles_15m)
            c['ta_15m'] = ta_15m

            # 1-hour candles (last 50)
            candles_1h = public_exchange.fetch_ohlcv(symbol, '1h', limit=50)
            ta_1h = analyze_candles(candles_1h)
            c['ta_1h'] = ta_1h

            # Order book for top 5 candidates only (API intensive)
            if i < 5:
                ob = public_exchange.fetch_order_book(symbol, limit=20)
                c['order_book'] = analyze_order_book(ob)
            else:
                c['order_book'] = None

        except Exception as e:
            c['funding'] = 0.0
            c['ta_15m'] = None
            c['ta_1h'] = None
            c['order_book'] = None

        enriched.append(c)

    return enriched


# ============================================================
#  GROK AI DECISION — ENHANCED PROMPT
# ============================================================

async def grok_decision(candidates, balance):
    # Build rich data string for each candidate
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

    prompt = f"""You are **AlphaEdge**, an elite quantitative futures trader using first-principles analysis.
You combine technical analysis, order flow, and multi-timeframe confluence to find high-leverage edges.

═══ MARKET CONTEXT ═══
Current time: {datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")}
Available balance: {balance:.2f} USDT
Max risk per trade: {MAX_RISK_PERCENT}% of balance ({balance * MAX_RISK_PERCENT / 100:.2f} USDT)
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
7. RISK/REWARD: SL should be at a technical level (below support/above resistance). TP should be ≥2x the SL distance.

═══ RULES ═══
- Pick the SINGLE BEST setup with multi-timeframe confluence, or HOLD if nothing aligns.
- You MUST set stop_loss and take_profit at TECHNICAL levels (support/resistance, BBands, recent high/low).
- SL must be between 0.5% and 5% from entry. TP must give at least 2:1 reward:risk.
- Use cross margin. Leverage 10-20x ONLY when 15m + 1h trends align and volume confirms.
- Lower leverage (5-10x) when signals are mixed.
- Only trade if confidence >= 0.74.
- Learn from recent performance: if losing, be more selective. If winning, maintain discipline.

═══ RESPOND WITH ONLY VALID JSON ═══
{{
  "symbol": "e.g. SOL/USDT:USDT",
  "action": "long" | "short" | "hold",
  "leverage": integer 1-20,
  "size_usdt": number,
  "stop_loss": number (price level at technical support/resistance),
  "take_profit": number (price level, ≥2x distance of SL from entry),
  "confidence": 0.00-1.00,
  "reason": "2-3 sentences citing specific indicators that create confluence"
}}"""

    response = await client.chat.completions.create(
        model="grok-4-1-fast-reasoning",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7,
        max_tokens=1200
    )

    try:
        text = response.choices[0].message.content.strip()
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        return json.loads(text)
    except Exception:
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
        return

    if not symbol or not sl or not tp:
        print(f"   ⚠️ Missing symbol/SL/TP in Grok response — skipping", flush=True)
        return

    try:
        trading_exchange.load_markets()

        try:
            trading_exchange.set_margin_mode('cross', symbol)
        except Exception:
            pass

        leverage = min(decision.get("leverage", 10), 20)
        trading_exchange.set_leverage(leverage, symbol)

        max_size = balance * MAX_RISK_PERCENT / 100
        size_usdt = min(decision.get("size_usdt", max_size), max_size)

        ticker = public_exchange.fetch_ticker(symbol)
        current_price = ticker['last']
        amount = size_usdt / current_price

        entry_side = "buy" if action == "long" else "sell"
        trading_exchange.create_market_order(symbol, entry_side, amount)

        sl_side = "sell" if action == "long" else "buy"
        trading_exchange.create_order(
            symbol, 'STOP_MARKET', sl_side, amount,
            None, {'stopPrice': sl, 'closePosition': True}
        )

        tp_side = "sell" if action == "long" else "buy"
        trading_exchange.create_order(
            symbol, 'TAKE_PROFIT_MARKET', tp_side, amount,
            None, {'stopPrice': tp, 'closePosition': True}
        )

        log_trade_open(symbol, action, size_usdt, leverage, current_price, sl, tp, confidence,
                       decision.get('reason', ''))

        # Calculate risk/reward for logging
        if action == "long":
            risk = abs(current_price - sl)
            reward = abs(tp - current_price)
        else:
            risk = abs(sl - current_price)
            reward = abs(current_price - tp)
        rr = reward / risk if risk > 0 else 0

        print(f"   🔥 OPENED {action.upper()} {symbol} | ${size_usdt:,.0f} | {leverage}x Cross @ ${current_price:,.2f}", flush=True)
        print(f"   🛑 SL: ${sl:,.2f} | 🎯 TP: ${tp:,.2f} | R:R {rr:.1f}:1", flush=True)
        print(f"   💡 {decision.get('reason', 'n/a')}", flush=True)

    except Exception as e:
        print(f"   ❌ Execution error on {symbol}: {e}", flush=True)


# ============================================================
#  MAIN LOOP
# ============================================================

last_position_symbol = None
had_position_last_cycle = False

async def main_loop():
    global last_position_symbol, had_position_last_cycle, session_start_balance

    print("🚀 High Leverage Top100 Scanner is now RUNNING on DEMO (Cross Margin)", flush=True)
    print(f"📊 Scanning every {INTERVAL_MINUTES} minutes | Max risk: {MAX_RISK_PERCENT}%", flush=True)
    print(f"📌 Strict single position — new signals ignored while position is open", flush=True)
    print(f"🔬 Enhanced: RSI, EMA, ATR, Bollinger, VWAP, order book, multi-timeframe", flush=True)
    print("=" * 60, flush=True)

    try:
        bal = trading_exchange.fetch_balance()
        session_start_balance = float(bal['total'].get('USDT', 0))
        print(f"💰 Session starting balance: ${session_start_balance:,.2f}", flush=True)
    except Exception:
        session_start_balance = 0

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
                      f"PnL: ${pnl:+,.2f} ({pnl_pct:+.2f}%)", flush=True)
                print(f"   ⏳ Waiting for SL/TP to trigger — not scanning for new trades", flush=True)

            else:
                if had_position_last_cycle and last_position_symbol:
                    print(f"\n[{now}] 🔔 Position CLOSED on {last_position_symbol}!", flush=True)
                    try:
                        incomes = trading_exchange.fapiprivate_get_income({
                            'symbol': last_position_symbol.replace('/', ''),
                            'incomeType': 'REALIZED_PNL',
                            'limit': 1,
                        })
                        if incomes:
                            realized_pnl = float(incomes[-1].get('income', 0))
                            try:
                                trades = trading_exchange.fetch_my_trades(last_position_symbol, limit=1)
                                exit_price = float(trades[-1]['price']) if trades else 0
                            except Exception:
                                exit_price = 0
                            log_trade_close(exit_price, realized_pnl)
                            icon = '✅' if realized_pnl >= 0 else '❌'
                            print(f"   {icon} Realized PnL: ${realized_pnl:+,.2f}", flush=True)
                    except Exception as e:
                        print(f"   ⚠️ Could not fetch realized PnL: {e}", flush=True)

                    cancel_open_orders(last_position_symbol)
                    last_position_symbol = None
                    had_position_last_cycle = False
                    print_pnl_dashboard()

                else:
                    print(f"\n[{now}] 🔍 No open position — scanning top 100 coins...", flush=True)

                    candidates = await get_top_candidates(100)
                    print(f"   📈 Found {len(candidates)} futures", flush=True)

                    print(f"   🔬 Enriching top 15 with technicals + order book...", flush=True)
                    enriched = await deep_enrich(candidates, 15)

                    balance_data = trading_exchange.fetch_balance()
                    balance = float(balance_data['total'].get('USDT', 0))
                    print(f"   💰 Balance: ${balance:,.2f} USDT", flush=True)

                    if session_start_balance > 0:
                        session_pnl = balance - session_start_balance
                        print(f"   📊 Session PnL: ${session_pnl:+,.2f} ({session_pnl / session_start_balance * 100:+.2f}%)", flush=True)

                    decision = await grok_decision(enriched, balance)
                    print(f"   🤖 Grok picks: {decision.get('symbol', 'none')} "
                          f"{decision.get('action', 'hold')} "
                          f"(confidence: {decision.get('confidence', 0):.2f})", flush=True)

                    await execute_trade(decision, balance)

            if cycle_count % 10 == 0:
                print_pnl_dashboard()
                print_income_summary()

        except Exception as e:
            print(f"Loop error: {e}", flush=True)

        await asyncio.sleep(INTERVAL_MINUTES * 60)


print("Starting main loop...", flush=True)
asyncio.run(main_loop())
