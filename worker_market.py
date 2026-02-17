import time
from utils import supabase, fetch_invezgo, get_all_tickers

def update_market_data():
    tickers = get_all_tickers()
    total = len(tickers)
    print(f"📈 [MARKET WORKER] Update Harga Harian {total} saham...")

    # Ambil data fundamental yang sudah ada di DB local (Graham Number)
    # Ini penting agar kita tidak perlu hitung ulang, tinggal ambil yang sudah dihitung worker_fundamental
    try:
        db_data = supabase.table("stocks").select("ticker, graham_number").execute().data
        stock_map = {item['ticker']: item for item in db_data}
    except Exception as e:
        print(f"❌ Gagal ambil cache database: {e}")
        stock_map = {}

    for i, ticker in enumerate(tickers):
        # 1. SKIP WARAN & RIGHTS (Filter Wajib)
        if "-" in ticker and len(ticker) > 4:
            print(f"⏩ ({i+1}/{total}) Skip {ticker} (Produk Turunan)")
            continue

        print(f"💹 ({i+1}/{total}) Market: {ticker}...", end=" ")
        
        # Ambil Intraday/Quote terbaru via KeyStat (karena price realtime ada disitu juga)
        data = fetch_invezgo(f"/analysis/keystat/{ticker}?type=Q&limit=1")
        
        # 2. VALIDASI DATA (Anti-Crash)
        if data and 'rows' in data and isinstance(data['rows'], list):
            rows = data['rows']
            
             # Helper parsing yang aman dari NoneType
            def get_val(keywords):
                if not rows: return 0 
                
                for r in rows:
                    name = r.get('name', '')
                    if not name: continue
                        
                    if any(k in name.upper() for k in keywords):
                        vals = r.get('values', [])
                        if vals and isinstance(vals, list) and len(vals) > 0:
                            return float(vals[0].get('amount', 0))
                return 0
            
            # Ambil data pasar
            price = get_val(["CLOSE", "PRICE", "HARGA"])
            mcap = get_val(["MARKET CAP"])
            vol = get_val(["VOLUME"])
            per = get_val(["PER", "PRICE EARNING"]) 
            pbv = get_val(["PBV", "PRICE TO BOOK"])

            # 3. HITUNG MARGIN OF SAFETY (MOS)
            # Ambil Graham Number dari "Memori" Database kita
            saved_fund = stock_map.get(ticker, {})
            graham_num = saved_fund.get('graham_number') or 0
            
            mos = 0
            status = "Neutral"
            
            # Logika MOS: (Nilai Wajar - Harga Sekarang) / Nilai Wajar
            if graham_num > 0 and price > 0:
                mos = ((graham_num - price) / graham_num) * 100
                status = "Undervalued" if mos > 0 else "Overvalued"

            # Persiapan Data Update
            update_data = {
                "last_price": price,
                "market_cap": mcap,
                "daily_volume": vol,
                "per": per,
                "pbv": pbv,
                "margin_of_safety": mos,
                "valuation_status": status,
                "updated_at": "now()"
            }
            
            try:
                supabase.table("stocks").update(update_data).eq("ticker", ticker).execute()
                print(f"✅ Price: {price} | MOS: {mos:.1f}%")
            except Exception as e:
                print(f"❌ DB Error: {e}")
        else:
            print("⚠️ Data Kosong/Gagal")
            
        time.sleep(0.2)

if __name__ == "__main__":
    update_market_data()