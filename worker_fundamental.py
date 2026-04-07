import os
import time
import requests
from datetime import datetime
from dotenv import load_dotenv
from utils import supabase, get_all_tickers
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()
INVEZGO_KEY = os.getenv("INVEZGO_API_KEY")

# PERTAHANAN SESI INVEZGO
session = requests.Session()
session.headers.update({"Authorization": f"Bearer {INVEZGO_KEY}"})
retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

def fetch_fundamentals_invezgo():
    tickers = get_all_tickers()
    total = len(tickers)
    print(f"🏦 [FUNDAMENTAL ENGINE] Menarik PER, PBV, ROA, ROE via Invezgo untuk {total} emiten...")
    
    # Menggunakan tanggal hari ini sebagai penanda periode data terbaru
    period_date = datetime.now().strftime('%Y-%m-%d')
    updates = []

    for i, ticker in enumerate(tickers):
        print(f"📊 ({i+1}/{total}) Ekstraksi Invezgo: {ticker}...", end=" ")
        
        # Endpoint Invezgo untuk Key Statistics Kuartalan terbaru
        url = f"https://api.invezgo.com/analysis/keystat/{ticker}?type=Q&limit=1"
        
        try:
            res = session.get(url, timeout=10)
            if res.status_code == 200:
                data = res.json()
                per = pbv = roa = roe = None
                
                # Membedah JSON balikan dari Invezgo
                if data and 'rows' in data and isinstance(data['rows'], list):
                    for r in data['rows']:
                        name = r.get('name', '').upper()
                        vals = r.get('values', [])
                        if not vals: continue
                        
                        val = float(vals[0].get('amount', 0) or 0)
                        
                        # Pencocokan nama rasio sesuai format Invezgo
                        if name == "PER" or "PRICE EARNING RATIO" in name: per = val
                        elif name == "PBV" or "PRICE TO BOOK VALUE" in name: pbv = val
                        elif name == "ROA" or "RETURN ON ASSET" in name: roa = val
                        elif name == "ROE" or "RETURN ON EQUITY" in name: roe = val
                
                # Abaikan emiten jika Invezgo tidak memiliki data fundamentalnya
                if all(v is None for v in [per, pbv, roa, roe]):
                    print("⚠️ Skip (Data rasio di Invezgo kosong)")
                    continue
                    
                updates.append({
                    "ticker": ticker,
                    "period_date": period_date,
                    "per": per,
                    "pbv": pbv,
                    "roa": roa,
                    "roe": roe
                })
                print("✅ Sukses")
            else:
                print(f"⚠️ API Error (Status: {res.status_code})")
                
        except Exception as e:
            print(f"❌ Error Jaringan: {e}")

        # Wajib: Jeda 0.2 detik untuk mencegah Rate Limit Invezgo (5 request/detik)
        time.sleep(0.2)

    # Injeksi ke Supabase secara massal (Batch Upsert)
    if updates:
        print(f"\n💾 Menyimpan {len(updates)} baris data fundamental ke Supabase...")
        try:
            CHUNK_SIZE = 500
            for c in range(0, len(updates), CHUNK_SIZE):
                chunk = updates[c:c+CHUNK_SIZE]
                supabase.table("financial_reports").upsert(chunk, on_conflict="ticker,period_date").execute()
            print("🎉 SELURUH DATA FUNDAMENTAL INVEZGO BERHASIL DIINJEKSI!")
        except Exception as e:
            print(f"❌ Gagal menyimpan ke database: {e}")

if __name__ == "__main__":
    fetch_fundamentals_invezgo()