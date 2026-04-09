import os
import time
import math
import requests
import pandas as pd
import pandas_ta as ta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from supabase import create_client, Client
from dotenv import load_dotenv
from utils import supabase, get_all_tickers

load_dotenv()
INVEZGO_KEY = os.getenv("INVEZGO_API_KEY")

session = requests.Session()
session.headers.update({"Authorization": f"Bearer {INVEZGO_KEY}"})
retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

def get_graham_number(ticker):
    url = f"https://api.invezgo.com/analysis/keystat/{ticker}?type=Q&limit=1"
    try:
        res = session.get(url, timeout=10)
        if res.status_code == 200:
            data = res.json()
            if data and 'rows' in data and isinstance(data['rows'], list):
                eps = bvps = 0
                for r in data['rows']:
                    name = r.get('name', '').upper()
                    if "EPS" in name or "EARNING PER SHARE" in name:
                        vals = r.get('values', [])
                        if vals: eps = float(vals[0].get('amount', 0) or 0)
                    elif "BVPS" in name or "BOOK VALUE PER SHARE" in name:
                        vals = r.get('values', [])
                        if vals: bvps = float(vals[0].get('amount', 0) or 0)
                
                if eps > 0 and bvps > 0:
                    graham_product = 22.5 * eps * bvps
                    if graham_product > 0:
                        return math.sqrt(graham_product)
        return 0
    except Exception:
        return 0

def engineer_features():
    tickers = get_all_tickers()
    total = len(tickers)
    print(f"🧠 [FEATURE ENGINEERING] Memulai rekayasa fitur (Tech, Funda, Volume) dengan Self-Healing untuk {total} emiten...")

    for i, ticker in enumerate(tickers):
        print(f"🔄 ({i+1}/{total}) Mengkalkulasi {ticker}...", end=" ")
        
        graham_number = get_graham_number(ticker)
        time.sleep(0.2) 
        
        # PERTAHANAN JARINGAN: Mekanisme Retry untuk Penarikan Data (Select)
        res = None
        for attempt in range(3):
            try:
                res = supabase.table("daily_market_prices")\
                    .select("trade_date, high_price, low_price, adjusted_close, volume")\
                    .eq("ticker", ticker)\
                    .order("trade_date", desc=False)\
                    .limit(3000)\
                    .execute()
                break # Keluar dari loop jika berhasil
            except Exception as e:
                if attempt == 2:
                    print(f"❌ Gagal tarik harga setelah 3 percobaan: {e}")
                else:
                    time.sleep(2) # Jeda napas sebelum mencoba lagi

        # Jika res masih None (gagal ditarik), lewati emiten ini tanpa mematikan program
        if not res or not res.data or len(res.data) < 30:
            if not res: 
                continue # Pesan error sudah dicetak di atas
            print("⚠️ Dilewati (Data tidak cukup)")
            continue
            
        df = pd.DataFrame(res.data)
        
        for col in ['high_price', 'low_price', 'adjusted_close', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df.ta.rsi(close='adjusted_close', length=14, append=True)
        df.ta.macd(close='adjusted_close', fast=12, slow=26, signal=9, append=True)
        df.ta.mfi(high='high_price', low='low_price', close='adjusted_close', volume='volume', length=14, append=True)
        
        def calculate_mos(price, g_num):
            if g_num <= 0 or price <= 0: return 0
            return ((g_num - price) / g_num) * 100
            
        df['margin_of_safety'] = df['adjusted_close'].apply(lambda x: calculate_mos(x, graham_number))
        df.dropna(subset=['RSI_14', 'MACD_12_26_9', 'MFI_14'], inplace=True)
        
        if df.empty:
            print("⚠️ Dilewati (Data kosong setelah kalkulasi)")
            continue

        updates = []
        for _, row in df.iterrows():
            updates.append({
                "ticker": ticker,
                "calc_date": row['trade_date'],
                "rsi_14": float(row['RSI_14']),
                "macd": float(row['MACD_12_26_9']),
                "margin_of_safety": float(row['margin_of_safety']),
                "mfi_14": float(row['MFI_14'])
            })
            
        # PERTAHANAN JARINGAN: Chunk diperkecil (500) dan diberi Retry
        try:
            CHUNK_SIZE = 500 # Turunkan agar Supabase tidak 502 Bad Gateway
            for c in range(0, len(updates), CHUNK_SIZE):
                chunk = updates[c:c+CHUNK_SIZE]
                
                # Retry loop untuk injeksi (Upsert)
                for attempt_upsert in range(3):
                    try:
                        supabase.table("technical_features").upsert(chunk, on_conflict="ticker,calc_date").execute()
                        break
                    except Exception as e:
                        if attempt_upsert == 2: raise e
                        time.sleep(3) # Tunggu Cloudflare pulih
                        
            print(f"✅ Selesai ({len(updates)} baris)")
        except Exception as e:
            print(f"❌ Gagal Upsert Final: {e}")

    print("\n🎉 REKAYASA FITUR (3 PILAR) SELESAI.")

if __name__ == "__main__":
    engineer_features()