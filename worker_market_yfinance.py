import time
import requests
import yfinance as yf
import pandas as pd
from utils import supabase, get_all_tickers

def update_market_yfinance():
    tickers = get_all_tickers()
    
    # FILTER ABSOLUT: Buang Waran & Right Issue
    clean_tickers = [t for t in tickers if "-" not in t]
    total = len(clean_tickers)
    
    print(f"📈 [YFINANCE WORKER] Memulai Update Harga untuk {total} emiten utama...")
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    })

    try:
        db_data = supabase.table("stocks").select("ticker, graham_number").execute().data
        stock_map = {item['ticker']: item for item in db_data}
    except Exception as e:
        print(f"❌ Gagal mengambil cache database: {e}")
        stock_map = {}

    # Turunkan ke 10 agar tidak mencolok di radar firewall Yahoo
    BATCH_SIZE = 10 
    failed_stocks = []
    
    for i in range(0, total, BATCH_SIZE):
        batch_tickers = clean_tickers[i:i+BATCH_SIZE]
        yf_symbols = [f"{t}.JK" for t in batch_tickers]
        
        print(f"🔄 Memproses Batch {i+1}-{min(i+BATCH_SIZE, total)}...", end=" ")
        
        # Algoritma Pertahanan: Retry jika kena Rate Limit
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # KUNCI PERUBAHAN: threads=False. Tarik berurutan, jangan serang server.
                data = yf.download(
                    yf_symbols, 
                    period="5d", 
                    group_by='ticker', 
                    progress=False, 
                    threads=False, 
                    auto_adjust=False,
                    session=session
                )
                
                updates = []
                for ticker in batch_tickers:
                    symbol = f"{ticker}.JK"
                    try:
                        if len(batch_tickers) > 1:
                            if symbol not in data.columns.levels[0]:
                                failed_stocks.append(ticker)
                                continue
                            stock_data = data[symbol].dropna(subset=['Close'])
                        else:
                            stock_data = data.dropna(subset=['Close'])
                            
                        if stock_data.empty or len(stock_data) < 1: 
                            failed_stocks.append(ticker)
                            continue
                            
                        last_row = stock_data.iloc[-1]
                        price = float(last_row['Close'])
                        vol = int(last_row['Volume']) if pd.notna(last_row['Volume']) else 0
                        
                        change_pct = 0
                        if len(stock_data) > 1:
                            prev_close = float(stock_data.iloc[-2]['Close'])
                            if prev_close > 0:
                                change_pct = ((price - prev_close) / prev_close) * 100
                        
                        saved_fund = stock_map.get(ticker, {})
                        graham_num = saved_fund.get('graham_number') or 0
                        
                        mos = 0
                        status = "Neutral"
                        if graham_num > 0 and price > 0:
                            mos = ((graham_num - price) / graham_num) * 100
                            if mos > 20: status = "Undervalued"
                            elif mos > 0: status = "Fair"
                            else: status = "Overvalued"
                            
                        updates.append({
                            "ticker": ticker,
                            "last_price": price,
                            "daily_volume": vol,
                            "change_pct": change_pct,
                            "margin_of_safety": mos,
                            "valuation_status": status,
                            "updated_at": "now()"
                        })
                        
                    except Exception:
                        failed_stocks.append(ticker)
                        continue

                if updates:
                    for up in updates:
                        supabase.table("stocks").update(up).eq("ticker", up["ticker"]).execute()
                    print(f"✅ Disimpan ({len(updates)} aktif).")
                else:
                    print("⚠️ Tidak ada data aktif.")
                
                # Jika berhasil mencapai baris ini, keluar dari loop retry
                break 

            except Exception as e:
                # Menangkap error 429 atau Rate Limit dari Exception Text
                if "Rate limit" in str(e) or "429" in str(e):
                    print(f"❌ Rate Limit! Mendinginkan IP selama 30 detik (Percobaan {attempt+1}/{max_retries})...")
                    time.sleep(30)
                else:
                    print(f"❌ Error YF: {e}")
                    failed_stocks.extend(batch_tickers)
                    break 
        else:
            # Jika sudah mencoba 3 kali dan masih gagal
            print("❌ Batch dilewati karena diblokir terus-menerus.")
            failed_stocks.extend(batch_tickers)

        # Jeda mutlak 4 detik antar batch agar terlihat seperti manusia
        time.sleep(4)

    print("\n🎉 PEMBARUAN HARGA SELESAI!")
    if failed_stocks:
        failed_stocks = list(set(failed_stocks))
        print(f"\n⚠️ {len(failed_stocks)} saham utama gagal ditarik (Suspensi/Error YF):")
        for i in range(0, len(failed_stocks), 15):
            print(", ".join(failed_stocks[i:i+15]))

if __name__ == "__main__":
    update_market_yfinance()