"""
HIGH LEVERAGE TOP100 SCANNER — Grok 4.1 Thinking (Cross Margin)
DEBUG VERSION — extra logging to diagnose market scanning
Binance Demo Trading — runs 24/7 on Render.com
"""

import os
import sys
import json
import asyncio
from datetime import datetime, UTC
from openai import AsyncOpenAI
import ccxt
from dotenv import load_dotenv

print("=== HIGH LEVERAGE TOP100 SCANNER STARTING (DEBUG) ===", flush=True)
print("Grok 4.1 Thinking + Cross Margin + Single Position mode", flush=True)

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
print("✅ Connected to Binance DEMO for trading", flush=True)


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
#  MARKET SCANNING — DEBUG VERSION
# ============================================================

async def get_top_candidates(n=100):
    """Fetch top N futures coins by 24h volume — with debug logging."""
    
    print("   [DEBUG] Loading markets...", flush=True)
    markets = public_exchange.load_markets()
    print(f"   [DEBUG] Total markets loaded: {len(markets)}", flush=True)
    
    # Show 3 sample market keys and their properties
    sample_count = 0
    for symbol, market in markets.items():
        if sample_count < 3:
            print(f"   [DEBUG] Sample market: {symbol} -> type={market.get('type')}, swap={market.get('swap')}, "
                  f"contractType={market.get('contractType')}, active={market.get('active')}, "
                  f"quote={market.get('quote')}, settle={market.get('settle')}", flush=True)
            sample_count += 1
    
    print("   [DEBUG] Fetching tickers...", flush=True)
    tickers = public_exchange.fetch_tickers()
    print(f"   [DEBUG] Total tickers fetched: {len(tickers)}", flush=True)
    
    # Show 3 sample ticker keys
    sample_count = 0
    for symbol in tickers:
        if sample_count < 3:
            print(f"   [DEBUG] Sample ticker key: {symbol}", flush=True)
            sample_count += 1
    
    # Try to find matches with relaxed filters step by step
    usdt_tickers = [s for s in tickers if 'USDT' in s]
    print(f"   [DEBUG] Tickers containing 'USDT': {len(usdt_tickers)}", flush=True)
    
    in_markets = [s for s in usdt_tickers if s in markets]
    print(f"   [DEBUG] Also in markets dict: {len(in_markets)}", flush=True)
    
    swap_matches = [s for s in in_markets if markets[s].get('swap')]
    print(f"   [DEBUG] With swap=True: {len(swap_matches)}", flush=True)
    
    perp_matches = [s for s in swap_matches if markets[s].get('contractType') == 'PERPETUAL']
    print(f"   [DEBUG] With contractType=PERPETUAL: {len(perp_matches)}", flush=True)
    
    active_matches = [s for s in perp_matches if markets[s].get('active')]
    print(f"   [DEBUG] With active=True: {len(active_matches)}", flush=True)
    
    # If the strict filter fails, show what contractTypes exist
    if len(perp_matches) == 0 and len(swap_matches) > 0:
        contract_types = set(markets[s].get('contractType') for s in swap_matches)
        print(f"   [DEBUG] Available contractTypes: {contract_types}", flush=True)
    
    # If swap filter fails, show what types exist
    if len(swap_matches) == 0 and len(in_markets) > 0:
        types = set(markets[s].get('type') for s in in_markets[:20])
        swaps = set(markets[s].get('swap') for s in in_markets[:20])
        print(f"   [DEBUG] Market types: {types}, swap values: {swaps}", flush=True)
    
    # If nothing matches markets, check key format mismatch
    if len(in_markets) == 0 and len(usdt_tickers) > 0:
        print(f"   [DEBUG] KEY MISMATCH — ticker keys don't match market keys", flush=True)
        print(f"   [DEBUG] First 3 ticker keys: {list(tickers.keys())[:3]}", flush=True)
        print(f"   [DEBUG] First 3 market keys: {list(markets.keys())[:3]}", flush=True)
    
    # Build candidate list using the working filter
    candidates = []
    for symbol, ticker in tickers.items():
        market = markets.get(symbol, {})
        if (('USDT' in symbol)
                and market.get('swap', False)
                and market.get('active', False)):
            # Accept any perpetual-like contract
            ct = market.get('contractType', '')
            if ct in ('PERPETUAL', 'perpetual', None, ''):
                vol = ticker.get('quoteVolume') or 0
                candidates.append({
                    'symbol': symbol,
                    'price': ticker.get('last', 0),
                    'change24h': ticker.get('percentage', 0),
                    'volume': vol,
                })

    print(f"   [DEBUG] Final candidates (relaxed filter): {len(candidates)}", flush=True)
    candidates.sort(key=lambda x: x['volume'], reverse=True)
    return candidates[:n]


async def enrich_top_picks(candidates, top_n=25):
    enriched = []
    for c in candidates[:top_n]:
        try:
            funding = public_exchange.fetch_funding_rate(c['symbol']).get('fundingRate', 0.0)
        except Exception:
            funding = 0.0
        c['funding'] = funding
        enriched.append(c)
    return enriched


# ============================================================
#  GROK AI DECISION
# ============================================================

async def grok_decision(candidates, balance):
    data_str = "\n".join([
        f"{c['symbol']}: ${c['price']:,.2f}, 24h {c['change24h']:+.2f}%, "
        f"Funding {c.get('funding', 0) * 100:.4f}%, Vol ${c['volume'] / 1e9:.1f}B"
        for c in candidates
    ])

    prompt = f"""You are **AlphaEdge**, an aggressive first-principles futures trader.
You use CROSS margin and high leverage (10-20x) when conviction is high.

Current time: {datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")}
Available balance: {balance:.2f} USDT
Scan interval: every {INTERVAL_MINUTES} minutes
Max risk per trade: {MAX_RISK_PERCENT}% of balance

Top 25 futures coins by volume (from a top-100 scan):
{data_str}

RULES:
- Pick the SINGLE BEST setup, or hold if nothing is strong enough.
- You MUST set a stop_loss and take_profit — the position will close ONLY when one of these is hit.
- Use cross margin. Leverage 10-20x when confident.
- size_usdt must not exceed {MAX_RISK_PERCENT}% of balance ({balance * MAX_RISK_PERCENT / 100:.2f} USDT).
- Only trade if confidence >= 0.74.

Return ONLY valid JSON:
{{
  "symbol": "e.g. SOLUSDT",
  "action": "long" | "short" | "hold",
  "leverage": integer 1-20,
  "size_usdt": number,
  "stop_loss": number (required — price level),
  "take_profit": number (required — price level),
  "confidence": 0.00-1.00,
  "reason": "1-2 sentence explanation"
}}"""

    response = await client.chat.completions.create(
        model="grok-4-1-fast-reasoning",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7,
        max_tokens=1000
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

        print(f"   🔥 OPENED {action.upper()} {symbol} | ${size_usdt:,.0f} | {leverage}x Cross @ ${current_price:,.2f}", flush=True)
        print(f"   🛑 SL: ${sl:,.2f} | 🎯 TP: ${tp:,.2f}", flush=True)
        print(f"   💡 {decision.get('reason', 'n/a')}", flush=True)

    except Exception as e:
        print(f"   ❌ Execution error on {symbol}: {e}", flush=True)


# ============================================================
#  MAIN LOOP
# ============================================================

last_position_symbol = None

async def main_loop():
    global last_position_symbol

    print("🚀 High Leverage Top100 Scanner is now RUNNING on DEMO (Cross Margin)", flush=True)
    print(f"📊 Scanning every {INTERVAL_MINUTES} minutes | Max risk: {MAX_RISK_PERCENT}%", flush=True)
    print(f"📌 Strict single position — new signals ignored while position is open", flush=True)
    print("=" * 60, flush=True)

    while True:
        try:
            now = datetime.now(UTC).strftime('%H:%M:%S')
            position = get_open_position()

            if position:
                last_position_symbol = position['symbol']
                pnl = position['unrealized_pnl']
                pnl_pct = (pnl / position['notional'] * 100) if position['notional'] else 0
                print(f"\n[{now}] 📍 OPEN: {position['side'].upper()} {position['symbol']} | "
                      f"Entry ${position['entry_price']:,.2f} | {position['leverage']}x | "
                      f"PnL: ${pnl:+,.2f} ({pnl_pct:+.2f}%)", flush=True)
                print(f"   ⏳ Waiting for SL/TP to trigger — not scanning for new trades", flush=True)

            else:
                if last_position_symbol:
                    cancel_open_orders(last_position_symbol)
                    last_position_symbol = None

                print(f"\n[{now}] 🔍 No open position — scanning top 100 coins...", flush=True)

                candidates = await get_top_candidates(100)
                print(f"   📈 Found {len(candidates)} perpetual futures", flush=True)

                enriched = await enrich_top_picks(candidates, 25)

                balance_data = trading_exchange.fetch_balance()
                balance = float(balance_data['total'].get('USDT', 0))
                print(f"   💰 Balance: ${balance:,.2f} USDT", flush=True)

                decision = await grok_decision(enriched, balance)
                print(f"   🤖 Grok picks: {decision.get('symbol', 'none')} "
                      f"{decision.get('action', 'hold')} "
                      f"(confidence: {decision.get('confidence', 0):.2f})", flush=True)

                await execute_trade(decision, balance)

        except Exception as e:
            print(f"Loop error: {e}", flush=True)

        await asyncio.sleep(INTERVAL_MINUTES * 60)


print("Starting main loop...", flush=True)
asyncio.run(main_loop())
