import os
import time
import requests
from supabase import create_client, Client
from dotenv import load_dotenv
from utils import get_all_tickers

load_dotenv()

INVEZGO_KEY = os.getenv("INVEZGO_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not all([INVEZGO_KEY, SUPABASE_URL, SUPABASE_KEY]):
    print("❌ Error: Pastikan file .env sudah diisi lengkap!")
    exit()

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def update_fundamentals():
    tickers = get_all_tickers()
    
    # FILTER ABSOLUT: Hanya ambil saham utama (tidak mengandung tanda "-")
    clean_tickers = [t for t in tickers if "-" not in t]
    total = len(clean_tickers)
    
    print(f"💰 [FUNDAMENTAL WORKER] Memulai ekstraksi data untuk {total} emiten utama murni...")

    session = requests.Session()
    headers = {"Authorization": f"Bearer {INVEZGO_KEY}"}
    failed_stocks = []

    for i, ticker in enumerate(clean_tickers):
        print(f"📊 ({i+1}/{total}) Fundamental: {ticker}...", end=" ")
        
        url = f"https://api.invezgo.com/analysis/keystat/{ticker}?type=Q&limit=1"
        
        try:
            res = session.get(url, headers=headers, timeout=10)
            
            if res.status_code == 200:
                data = res.json()
                
                if data and 'rows' in data and isinstance(data['rows'], list):
                    rows = data['rows']
                    
                    def get_val(keywords):
                        for r in rows:
                            row_name = r.get('name', '')
                            if not row_name: continue
                            if any(k in row_name.upper() for k in keywords):
                                vals = r.get('values', [])
                                if vals and isinstance(vals, list):
                                    try:
                                        return float(vals[0].get('amount', 0))
                                    except (ValueError, TypeError):
                                        return 0
                        return 0

                    eps = get_val(["EPS", "EARNING PER SHARE"])
                    bvps = get_val(["BVPS", "BOOK VALUE PER SHARE"])
                    roe = get_val(["ROE", "RETURN ON EQUITY"])
                    der = get_val(["DER", "DEBT TO EQUITY"])
                    npm = get_val(["NPM", "NET PROFIT MARGIN"])
                    per = get_val(["PER", "PRICE EARNING RATIO"])
                    pbv = get_val(["PBV", "PRICE TO BOOK"])
                    roa = get_val(["ROA", "RETURN ON ASSETS"])
                    
                    graham_num = 0
                    if eps > 0 and bvps > 0:
                        graham_product = 22.5 * eps * bvps
                        if graham_product > 0:
                            graham_num = graham_product ** 0.5

                    update_data = {
                        "eps_ttm": eps,
                        "bvps": bvps,
                        "roe": roe,
                        "der": der,
                        "npm": npm,
                        "per": per,
                        "pbv": pbv,
                        "roa": roa,
                        "graham_number": graham_num, 
                        "raw_fundamentals": data,
                        "updated_at": "now()"
                    }
                    
                    supabase.table("stocks").update(update_data).eq("ticker", ticker).execute()
                    print(f"✅ (Graham: {graham_num:.0f})")
                else:
                    print("⚠️ Kosong/Format Tidak Valid")
                    failed_stocks.append(ticker)
            else:
                print(f"❌ HTTP {res.status_code}")
                failed_stocks.append(ticker)
                
        except requests.exceptions.Timeout:
            print("⏳ Timeout")
            failed_stocks.append(ticker)
        except Exception as e:
            print(f"❌ Error: {e}")
            failed_stocks.append(ticker)
        
        time.sleep(0.3)

    print("\n🎉 EKSTRAKSI FUNDAMENTAL SELESAI!")
    if failed_stocks:
        print(f"\n⚠️ {len(failed_stocks)} saham gagal ditarik (kosong/error):")
        for i in range(0, len(failed_stocks), 15):
            print(", ".join(failed_stocks[i:i+15]))

if __name__ == "__main__":
    update_fundamentals()