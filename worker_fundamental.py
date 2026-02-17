import time
from utils import supabase, fetch_invezgo, get_all_tickers

def update_fundamentals():
    tickers = get_all_tickers()
    total = len(tickers)
    print(f"💰 [FUNDAMENTAL WORKER] Update Laporan Keuangan {total} saham...")

    for i, ticker in enumerate(tickers):
        # 1. SKIP WARAN & RIGHTS (Produk Turunan tidak punya Fundamental)
        # Ciri-cirinya: Kode lebih dari 4 huruf dan ada tanda strip "-"
        if "-" in ticker and len(ticker) > 4:
            print(f"⏩ ({i+1}/{total}) Skip {ticker} (Produk Turunan)")
            continue

        print(f"📊 ({i+1}/{total}) Fundamental: {ticker}...", end=" ")
        
        # Ambil data Key Statistic (EPS, BVPS, ROE)
        data = fetch_invezgo(f"/analysis/keystat/{ticker}?type=Q&limit=1")
        
        # 2. LOGIKA PENGECEKAN LEBIH AMAN (Anti-Crash)
        # Kita pastikan 'rows' ada DAN isinya berupa List (bukan None)
        if data and 'rows' in data and isinstance(data['rows'], list):
            rows = data['rows']
            
            # Helper parsing yang aman
            def get_val(keywords):
                if not rows: return 0 # Guard clause jika rows kosong
                
                for r in rows:
                    # Pastikan 'name' ada sebelum di-upper()
                    row_name = r.get('name', '')
                    if not row_name: continue
                        
                    if any(k in row_name.upper() for k in keywords):
                        vals = r.get('values', [])
                        if vals and isinstance(vals, list):
                            return float(vals[0].get('amount', 0))
                return 0

            eps = get_val(["EPS", "EARNING PER SHARE"])
            bvps = get_val(["BVPS", "BOOK VALUE"])
            roe = get_val(["ROE", "RETURN ON EQUITY"])
            der = get_val(["DER", "DEBT TO EQUITY"])
            npm = get_val(["NPM", "NET PROFIT"])

            # 3. HITUNG GRAHAM NUMBER (Nilai Wajar)
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
                "graham_number": graham_num, 
                "updated_at": "now()"
            }
            
            try:
                supabase.table("stocks").update(update_data).eq("ticker", ticker).execute()
                print(f"✅ (Graham: {graham_num:.0f})")
            except Exception as e:
                print(f"❌ DB Error: {e}")

        else:
            print("⚠️ Data tidak lengkap / Kosong")
        
        # Jeda biar aman dari Rate Limit
        time.sleep(0.2)

if __name__ == "__main__":
    update_fundamentals()