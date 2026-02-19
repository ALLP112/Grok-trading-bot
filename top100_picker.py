"""
TOP100 PICKER BOT — Grok 4.1 Thinking
Scans top 100, picks the single best long or short on Binance demo futures
"""

import os
import sys
import json
import asyncio
from datetime import datetime, UTC
from openai import AsyncOpenAI
import ccxt
from dotenv import load_dotenv

print("=== TOP100 PICKER BOT STARTING ===", flush=True)
print("Grok 4.1 Thinking + Smart Scanner mode active", flush=True)

load_dotenv()

# === SETTINGS ===
XAI_API_KEY = os.getenv("XAI_API_KEY")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")
INTERVAL_MINUTES = int(os.getenv("INTERVAL_MINUTES", "30"))
MAX_RISK_PERCENT = float(os.getenv("MAX_RISK_PERCENT", "1.0"))

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

async def get_top_candidates(n=20):
    markets = exchange.load_markets()
    tickers = exchange.fetch_tickers()
    candidates = []
    for symbol, ticker in tickers.items():
        if symbol.endswith('USDT') and 'swap' in markets.get(symbol, {}).get('type', ''):
            vol = ticker.get('quoteVolume') or 0
            candidates.append({
                'symbol': symbol,
                'price': ticker['last'],
                'change24h': ticker.get('percentage', 0),
                'volume': vol,
                'funding': exchange.fetch_funding_rate(symbol)['fundingRate'] if 'fundingRate' in ticker else 0
            })
    candidates.sort(key=lambda x: x['volume'], reverse=True)
    return candidates[:n]

async def grok_decision(candidates):
    data_str = "\n".join([f"{c['symbol']}: Price ${c['price']:,.2f}, 24h {c['change24h']:.2f}%, Funding {c['funding']*100:.4f}% " for c in candidates])

    prompt = f"""You are AlphaEdge Pro, a first-principles trader.

Current time: {datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")}
Timeframe: next {INTERVAL_MINUTES} minutes

Top 20 candidates by volume:
{data_str}

From first principles (order flow, liquidity, positioning, asymmetry, feedback loops), rank them and pick the **single best** long or short setup (or hold if no edge).

Return ONLY valid JSON:
{{
  "symbol": "e.g. BTCUSDT",
  "action": "long" | "short" | "hold",
  "leverage": 1-12,
  "size_usdt": number (respect {MAX_RISK_PERCENT}% risk),
  "stop_loss": number,
  "take_profit": number or null,
  "confidence": 0.00-1.00,
  "reason": "concise 1-2 sentence first-principles explanation"
}}

Only trade if confidence >= 0.76 and R:R >= 2.2:1."""

    response = await client.chat.completions.create(
        model="grok-4-1-fast-reasoning",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7,
        max_tokens=900
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
        print(f"   ⏸️ HOLD — best was {decision.get('symbol')} (confidence {decision.get('confidence', 0):.2f})", flush=True)
        return
    try:
        exchange.set_leverage(decision["leverage"], decision["symbol"])
        side = "buy" if decision["action"] == "long" else "sell"
        amount = decision["size_usdt"] / decision["price"]
        exchange.create_market_order(decision["symbol"], side, amount)
        print(f"   ✅ EXECUTED {decision['action'].upper()} {decision['symbol']} | Size ${decision['size_usdt']:.0f} | Leverage {decision['leverage']}x", flush=True)
    except Exception as e:
        print(f"   ❌ Execution error: {e}", flush=True)

async def main_loop():
    print("🚀 Top100 Picker Bot (Grok 4.1 Thinking) is now RUNNING on DEMO", flush=True)
    while True:
        try:
            candidates = await get_top_candidates(20)
            print(f"\n[{datetime.now(UTC).strftime('%H:%M:%S')}] Scanning top 20 candidates...", flush=True)
            decision = await grok_decision(candidates)
            print(f"   🤖 Grok picks: {decision.get('symbol')} {decision.get('action')} (confidence: {decision.get('confidence', 0):.2f})", flush=True)
            await execute(decision)
        except Exception as e:
            print(f"Loop error: {e}", flush=True)
        await asyncio.sleep(INTERVAL_MINUTES * 60)

print("Starting main loop...", flush=True)
asyncio.run(main_loop())
