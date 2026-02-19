"""
GROK TRADING BOT v2 - DEBUG MODE
100% Grok 4.20 (xAI) - NOT OpenAI
"""

import os
import json
import asyncio
from datetime import datetime
from openai import AsyncOpenAI
import ccxt
from dotenv import load_dotenv

print("=== GROK TRADING BOT STARTING (DEBUG v2) ===")

load_dotenv()

# === CHECK ALL VARIABLES ===
print("Checking environment variables...")
XAI_API_KEY = os.getenv("XAI_API_KEY")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")
SYMBOL = os.getenv("SYMBOL", "BTCUSDT")
INTERVAL_MINUTES = int(os.getenv("INTERVAL_MINUTES", "30"))
MAX_RISK_PERCENT = float(os.getenv("MAX_RISK_PERCENT", "1.0"))

if not XAI_API_KEY:
    print("❌ ERROR: XAI_API_KEY is missing!")
if not BINANCE_API_KEY:
    print("❌ ERROR: BINANCE_API_KEY is missing!")
if not BINANCE_API_SECRET:
    print("❌ ERROR: BINANCE_API_SECRET is missing!")

print(f"✅ SYMBOL = {SYMBOL}")
print(f"✅ INTERVAL = {INTERVAL_MINUTES} minutes")
print(f"✅ MAX RISK = {MAX_RISK_PERCENT}%")

# Create Grok client
print("Connecting to Grok 4.20...")
try:
    client = AsyncOpenAI(api_key=XAI_API_KEY, base_url="https://api.x.ai/v1")
    print("✅ Connected to Grok 4.20 successfully")
except Exception as e:
    print(f"❌ Failed to connect to Grok: {e}")

# Create Binance connection (Testnet)
print("Connecting to Binance Testnet...")
try:
    exchange = ccxt.binance({
        'apiKey': BINANCE_API_KEY,
        'secret': BINANCE_API_SECRET,
        'enableRateLimit': True,
        'options': {'defaultType': 'future'},
    })
    exchange.set_sandbox_mode(True)
    print("✅ Connected to Binance Testnet")
except Exception as e:
    print(f"❌ Failed to connect to Binance: {e}")

async def main_loop():
    print("🚀 Grok Binance Auto-Trader (Grok 4.20) is now RUNNING on TESTNET")
    while True:
        try:
            ticker = exchange.fetch_ticker(SYMBOL)
            price = ticker['last']
            print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] Checking {SYMBOL} — Price ${price:,.2f}")
            
            # TODO: add Grok decision here later
            print("   → No trade (debug mode)")
            
        except Exception as e:
            print(f"   ❌ Error in loop: {e}")
        
        await asyncio.sleep(INTERVAL_MINUTES * 60)

print("Starting main loop...")
asyncio.run(main_loop())
