"""
HIGH LEVERAGE TOP100 SCANNER — Grok 4.1 Thinking (Aggressive Isolated Margin)
Scans top 100 coins, only one position open at once, closes current before new
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

print("=== HIGH LEVERAGE TOP100 SCANNER STARTING ===", flush=True)
print("Grok 4.1 Thinking + Aggressive High-Leverage First-Principles mode active", flush=True)

load_dotenv()

# === SETTINGS ===
XAI_API_KEY = os.getenv("XAI_API_KEY")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")
INTERVAL_MINUTES = int(os.getenv("INTERVAL_MINUTES", "30"))
MAX_RISK_PERCENT = float(os.getenv("MAX_RISK_PERCENT", "2.5"))  # Higher risk for aggressive high-leverage style

# === CHECK KEYS ===
missing = []
if not XAI_API_KEY: missing.append("XAI_API_KEY")
if not BINANCE_API_KEY: missing.append("BINANCE_API_KEY")
if not BINANCE_API_SECRET: missing.append("BINANCE_API_SECRET")
if missing:
    print(f"❌ FATAL: Missing: {', '.join(missing)}", flush=True)
    sys.exit(1)

client = AsyncOpenAI(api_key=XAI_API_KEY, base_url="https://api.x.ai/v1")

exchange = ccxt.binance({
    'apiKey': BINANCE_API_KEY,
    'secret': BINANCE_API_SECRET,
    'enableRateLimit': True,
    'options': {'defaultType': 'future'},
})
exchange.enable_demo_trading(True)
print("✅ Connected to Binance DEMO futures", flush=True)

async def close_current_position():
    """Closes the current position (if any) before opening a new one"""
    try:
        positions = exchange.fetch_positions()
        for pos in positions:
            if float(pos['contracts']) != 0:
                symbol = pos['symbol']
                side = 'sell' if float(pos['contracts']) > 0 else 'buy'
                amount = abs(float(pos['contracts']))
                exchange.create_market_order(symbol, side, amount)
                print(f"   🔄 Closed current position: {symbol}", flush=True)
        await asyncio.sleep(2)  # Allow Binance to update margin
    except Exception as e:
        print(f"   ⚠️ Error closing current position: {e}", flush=True)

async def get_top_candidates(n=25):
    markets = exchange.load_markets()
    tickers = exchange.fetch_tickers()
    candidates = []
    for symbol, ticker in tickers.items():
        market = markets.get(symbol, {})
        if (symbol.endswith('USDT') and
            market.get('swap', False) and
            market.get('contractType') == 'PERPETUAL' and
            market.get('active', False)):
            
            try:
                funding = exchange.fetch_funding_rate(symbol).get('fundingRate', 0.0)
            except:
                funding = 0.0
            vol = ticker.get('quoteVolume') or 0
            candidates.append({
                'symbol': symbol,
                'price': ticker['last'],
                'change24h': ticker.get('percentage', 0),
                'volume': vol,
                'funding': funding
            })
    candidates.sort(key=lambda x: x['volume'], reverse=True)
    return candidates[:n]

async def grok_decision(candidates):
    data_str = "\n".join([f"{c['symbol']}: Price ${c['price']:,.2f}, 24h {c['change24h']:.2f}%, Funding {c['funding']*100:.4f}%, Vol ${c['volume']/1e9:.1f}B" for c in candidates])

    prompt = f"""You are **AlphaEdge High-Leverage**, an aggressive first-principles trader specializing in high-leverage (10-20x) isolated margin plays on Binance futures.

You actively seek strong, fast-moving imbalances that justify high leverage and significant risk when the edge is clear.

Current time: {datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")}
Timeframe: next {INTERVAL_MINUTES} minutes
Top 25 candidates by volume:
{data_str}

Perform deep first-principles analysis and pick the **single best** high-leverage long or short setup (or hold if no strong edge).

Return ONLY valid JSON:
{{
  "symbol": "e.g. SOLUSDT",
  "action": "long" | "short" | "close" | "hold",
  "leverage": integer 1-20,
  "size_usdt": number (respect {MAX_RISK_PERCENT}% risk),
  "stop_loss": number,
  "take_profit": number or null,
  "confidence": 0.00-1.00,
  "reason": "concise 1-2 sentence explanation of the high-leverage edge"
}}

Only take the trade if confidence >= 0.74. Use high leverage (12-20x) when conviction is high. Use isolated margin only."""

    response = await client.chat.completions.create(
        model="grok-4-1-fast-reasoning",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.75,
        max_tokens=1100
    )
    
    try:
        text = response.choices[0].message.content.strip()
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        return json.loads(text)
    except:
        return {"action": "hold", "confidence": 0}

async def execute(decision):
    if decision["action"] == "hold" or decision.get("confidence", 0) < 0.65:
        print(f" ⏸️ HOLD — best candidate was {decision.get('symbol', 'none')} (confidence {decision.get('confidence', 0):.2f})", flush=True)
        return

    symbol = decision.get("symbol")
    if not symbol:
        return

    try:
        # Always close current position before opening a new one (strict single position)
        await close_current_position()

        # Fetch fresh price
        ticker = exchange.fetch_ticker(symbol)
        current_price = ticker['last']

        # Set isolated margin
        exchange.set_margin_mode('isolated', symbol)

        leverage = min(decision.get("leverage", 15), 20)
        exchange.set_leverage(leverage, symbol)

        side = "buy" if decision["action"] == "long" else "sell"
        amount = decision["size_usdt"] / current_price
        exchange.create_market_order(symbol, side, amount)

        print(f" 🔥 EXECUTED {decision['action'].upper()} {symbol} | Size ${decision['size_usdt']:.0f} | Leverage {leverage}x @ ${current_price:,.2f} (Isolated)", flush=True)
        print(f" 💡 Reason: {decision.get('reason', 'n/a')}", flush=True)

    except Exception as e:
        print(f" ❌ Execution error on {symbol}: {e}", flush=True)

async def main_loop():
    print("🚀 High Leverage Top100 Scanner (Grok 4.1 Thinking + Isolated Margin) is now RUNNING on DEMO", flush=True)
    while True:
        try:
            candidates = await get_top_candidates(25)
            print(f"\n[{datetime.now(UTC).strftime('%H:%M:%S')}] Scanning top 25 for high-leverage edge...", flush=True)
            decision = await grok_decision(candidates)
            print(f" 🤖 Grok picks: {decision.get('symbol')} {decision.get('action')} (confidence: {decision.get('confidence', 0):.2f})", flush=True)
            await execute(decision)
        except Exception as e:
            print(f"Loop error: {e}", flush=True)
        await asyncio.sleep(INTERVAL_MINUTES * 60)

print("Starting main loop...", flush=True)
asyncio.run(main_loop())
