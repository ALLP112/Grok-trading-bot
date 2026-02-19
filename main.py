"""
FINAL GROK TRADING BOT — 100% Grok 4.1 Thinking (first-principles + profit-optimized)
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

print("=== GROK TRADING BOT STARTING (FINAL Binance v1) ===", flush=True)
print("Grok 4.1 Thinking + First-Principles AlphaEdge Pro prompt active", flush=True)

load_dotenv()

# === SETTINGS ===
XAI_API_KEY = os.getenv("XAI_API_KEY")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")
SYMBOL = os.getenv("SYMBOL", "BTCUSDT")
INTERVAL_MINUTES = int(os.getenv("INTERVAL_MINUTES", "30"))
MAX_RISK_PERCENT = float(os.getenv("MAX_RISK_PERCENT", "1.0"))

# === CHECK REQUIRED KEYS ===
missing = []
if not XAI_API_KEY: missing.append("XAI_API_KEY")
if not BINANCE_API_KEY: missing.append("BINANCE_API_KEY")
if not BINANCE_API_SECRET: missing.append("BINANCE_API_SECRET")
if missing:
    print(f"❌ FATAL: Missing environment variables: {', '.join(missing)}", flush=True)
    print("Set them in Render Dashboard → Environment tab", flush=True)
    sys.exit(1)

client = AsyncOpenAI(api_key=XAI_API_KEY, base_url="https://api.x.ai/v1")

exchange = ccxt.binance({
    'apiKey': BINANCE_API_KEY,
    'secret': BINANCE_API_SECRET,
    'enableRateLimit': True,
    'options': {'defaultType': 'future'},
})
exchange.enable_demo_trading(True)
print("✅ Connected to Binance DEMO futures (demo-fapi.binance.com)", flush=True)

async def get_live_data():
    ticker = exchange.fetch_ticker(SYMBOL)
    funding = exchange.fetch_funding_rate(SYMBOL)
    balance = exchange.fetch_balance()['total'].get('USDT', 0)
    return {
        "price": ticker['last'],
        "funding_rate": funding['fundingRate'],
        "balance": balance
    }

async def grok_decision(data):
    prompt = f"""You are **AlphaEdge Pro**, a first-principles crypto perpetuals trader who thinks like a physicist of markets.

Price moves **only** because of imbalance between aggressive buying and selling pressure. This imbalance arises from:
- Net order flow (who is hitting bids/offers harder)
- Liquidity consumption vs provision (where stops cluster, where resting liquidity sits)
- Positioning & forced flows (funding payments, liquidations, deleveraging)
- Information asymmetry (whales, smart money, on-chain signals, narrative consensus)
- Psychological feedback loops (FOMO, capitulation, herd behaviour)
- Macro capital allocation (risk-on/risk-off, correlation flows)

Current snapshot:
- Time (UTC): {datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")}
- Asset: {SYMBOL}
- Price: ${data['price']:,.2f}
- Funding Rate: {data['funding_rate']*100:.4f}%
- Balance: ${data['balance']:,.2f} USDT
- Timeframe: next {INTERVAL_MINUTES} minutes

Reason step-by-step from first principles above. Then calculate true statistical edge after slippage and funding.

Return **ONLY** valid JSON, nothing else:

{{
  "action": "long" | "short" | "close" | "hold",
  "leverage": integer 1-12,
  "size_usdt": number (strictly respect max {MAX_RISK_PERCENT}% account risk),
  "stop_loss": number,
  "take_profit": number or null,
  "confidence": float 0.00-1.00,
  "reason": "concise 1-2 sentence synthesis of the first-principles edge",
  "key_risks": ["bullet point 1", "bullet point 2"]
}}

Strict rule: Only take long or short if confidence >= 0.76 **AND** expected risk-reward >= 2.2:1. Otherwise always "hold". Never chase or over-leverage."""

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

async def execute(decision, data):
    if decision["action"] == "hold" or decision.get("confidence", 0) < 0.65:
        print(f"   ⏸️ HOLD — confidence {decision.get('confidence', 0):.2f} | Reason: {decision.get('reason', 'n/a')}", flush=True)
        return
    try:
        exchange.set_leverage(decision["leverage"], SYMBOL)
        side = "buy" if decision["action"] == "long" else "sell"
        amount = decision["size_usdt"] / data["price"]
        exchange.create_market_order(SYMBOL, side, amount)
        print(f"   ✅ EXECUTED {decision['action'].upper()} | Size ${decision['size_usdt']:.0f} | Leverage {decision['leverage']}x", flush=True)
        print(f"   💡 Reason: {decision.get('reason', 'n/a')}", flush=True)
    except Exception as e:
        print(f"   ❌ Execution error: {e}", flush=True)

async def main_loop():
    print("🚀 Grok Binance Auto-Trader (Grok 4.1 Thinking + First-Principles AlphaEdge Pro) is now RUNNING on DEMO", flush=True)
    print(f"📊 Trading {SYMBOL} every {INTERVAL_MINUTES} minutes", flush=True)
    print(f"⚙️ Max risk per trade: {MAX_RISK_PERCENT}%", flush=True)
    print("=" * 60, flush=True)
    while True:
        try:
            data = await get_live_data()
            print(f"\n[{datetime.now(UTC).strftime('%H:%M:%S')}] {SYMBOL} — ${data['price']:,.2f} | Balance: ${data['balance']:,.2f} USDT", flush=True)
            decision = await grok_decision(data)
            print(f"   🤖 Grok says: {decision.get('action', 'unknown')} (confidence: {decision.get('confidence', 0):.2f})", flush=True)
            await execute(decision, data)
        except Exception as e:
            print(f"Loop error: {e}", flush=True)
        await asyncio.sleep(INTERVAL_MINUTES * 60)

print("Starting main loop...", flush=True)
asyncio.run(main_loop())
