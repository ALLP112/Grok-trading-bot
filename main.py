"""
FINAL GROK TRADING BOT — 100% Grok 4.20 (SuperGrok Heavy style)
Binance Testnet — runs 24/7 on Render.com
"""

import os
import json
import asyncio
from datetime import datetime
from openai import AsyncOpenAI
import ccxt
from dotenv import load_dotenv

print("=== GROK TRADING BOT STARTING (FINAL Binance v1) ===")
print("Grok 4.20 + SuperGrok Heavy style prompt active")

load_dotenv()

# === SETTINGS ===
XAI_API_KEY = os.getenv("XAI_API_KEY")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")
SYMBOL = os.getenv("SYMBOL", "BTCUSDT")
INTERVAL_MINUTES = int(os.getenv("INTERVAL_MINUTES", "30"))
MAX_RISK_PERCENT = float(os.getenv("MAX_RISK_PERCENT", "1.0"))

client = AsyncOpenAI(api_key=XAI_API_KEY, base_url="https://api.x.ai/v1")

exchange = ccxt.binance({
    'apiKey': BINANCE_API_KEY,
    'secret': BINANCE_API_SECRET,
    'enableRateLimit': True,
    'options': {'defaultType': 'future'},
})
exchange.set_sandbox_mode(True)  # Binance Testnet

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
    prompt = f"""You are running in full 16-agent SuperGrok Heavy mode.
Current time: {datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")}
Asset: {SYMBOL}
Price: {data['price']}
Funding rate: {data['funding_rate']}
Balance: {data['balance']} USDT
Timeframe: next {INTERVAL_MINUTES} minutes.

Return ONLY valid JSON:
{{
  "action": "long" or "short" or "close" or "hold",
  "leverage": number 1-10,
  "size_usdt": number (max {MAX_RISK_PERCENT}% risk),
  "stop_loss": number,
  "take_profit": number or null,
  "confidence": number 0.00-1.00,
  "reason": "short explanation"
}}

Only trade if confidence > 0.65."""

    response = await client.chat.completions.create(
        model="grok-4-1-fast-reasoning",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7,
        max_tokens=800
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
        print("No confident trade — holding")
        return
    try:
        exchange.set_leverage(decision["leverage"], SYMBOL)
        side = "buy" if decision["action"] == "long" else "sell"
        amount = decision["size_usdt"] / data["price"]
        exchange.create_market_order(SYMBOL, side, amount)
        print(f"✅ EXECUTED {decision['action'].upper()} | Size ${decision['size_usdt']:.0f}")
    except Exception as e:
        print(f"Execution error: {e}")

async def main_loop():
    print("🚀 Grok Binance Auto-Trader (Grok 4.20 Heavy style) is now RUNNING on TESTNET")
    while True:
        try:
            data = await get_live_data()
            print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] Checking {SYMBOL} — Price ${data['price']:,.2f}")
            decision = await grok_decision(data)
            await execute(decision, data)
        except Exception as e:
            print(f"Loop error: {e}")
        await asyncio.sleep(INTERVAL_MINUTES * 60)

print("Starting main loop...")
asyncio.run(main_loop())
