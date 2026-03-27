import time
import yfinance as yf
import pandas as pd
from datetime import datetime
from utils import supabase, get_all_tickers

def update_market_yfinance():
    tickers = get_all_tickers()
    total = len(tickers)
    print(f"📈 [DATA LAKE INGESTOR] Memulai Ekstraksi Harga OHLCV untuk {total} emiten...")

    # Turunkan batch size untuk stabilitas
    BATCH_SIZE = 10 
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    for i in range(0, total, BATCH_SIZE):
        batch_tickers = tickers[i:i+BATCH_SIZE]
        yf_symbols = [f"{t}.JK" for t in batch_tickers]
        
        print(f"🔄 Memproses Batch {i+1}-{min(i+BATCH_SIZE, total)}...", end=" ")
        
        try:
            # PERUBAHAN KRITIS: Hapus parameter session=session. 
            # Biarkan yfinance menggunakan curl_cffi internal mereka.
            data = yf.download(
                yf_symbols, 
                period="5d", 
                group_by='ticker', 
                progress=False, 
                threads=False, 
                auto_adjust=False 
            )
            
            updates = []
            for ticker in batch_tickers:
                symbol = f"{ticker}.JK"
                try:
                    # Parsing hasil multi-index YFinance
                    if len(batch_tickers) > 1:
                        if symbol not in data.columns.levels[0]: continue
                        stock_data = data[symbol].dropna(subset=['Close'])
                    else:
                        stock_data = data.dropna(subset=['Close'])
                        
                    if stock_data.empty: continue
                        
                    last_row = stock_data.iloc[-1]
                    trade_date = stock_data.index[-1].strftime('%Y-%m-%d')
                    
                    # 1. CEK SABUK PENGAMAN ADMIN
                    existing_db = supabase.table("daily_market_prices")\
                        .select("is_manually_overridden")\
                        .eq("ticker", ticker).eq("trade_date", trade_date).execute()
                    
                    if existing_db.data and existing_db.data[0].get("is_manually_overridden") == True:
                        print(f"\n   🛡️ [OVERRIDE BLOCK] {ticker} dilewati. Data dikunci.")
                        continue 
                    
                    # 2. PERSIAPKAN PAYLOAD
                    updates.append({
                        "ticker": ticker,
                        "trade_date": trade_date,
                        "open_price": float(last_row['Open']),
                        "high_price": float(last_row['High']),
                        "low_price": float(last_row['Low']),
                        "raw_close": float(last_row['Close']),
                        "adjusted_close": float(last_row['Adj Close']), 
                        "volume": int(last_row['Volume']) if pd.notna(last_row['Volume']) else 0
                    })
                    
                except Exception as e:
                    continue

            # 3. EKSEKUSI UPSERT KE DATABASE
            if updates:
                supabase.table("daily_market_prices").upsert(updates, on_conflict="ticker,trade_date").execute()
                print(f"✅ {len(updates)} baris disuntikkan ke Data Lake.")
            else:
                print("⚠️ Tidak ada pembaruan (Data kosong/Suspensi).")

        except Exception as e:
            print(f"❌ Error Eksekusi: {e}")

        time.sleep(3) # Jeda sopan santun mutlak

    print("\n🎉 AKUISISI DATA LAKE SELESAI!")

if __name__ == "__main__":
    update_market_yfinance()