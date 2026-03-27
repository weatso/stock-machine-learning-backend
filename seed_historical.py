import time
import yfinance as yf
import pandas as pd
from utils import supabase, get_all_tickers

def ingest_historical_data():
    tickers = get_all_tickers()
    total = len(tickers)
    print(f"🕰️ [HISTORICAL INGESTOR] Memulai ekstraksi data 3 TAHUN untuk {total} emiten...")

    # Turunkan batch ke 10. Jika kita menarik 3 tahun data untuk 10 saham,
    # itu berarti kita memproses sekitar 7.500 baris data per siklus loop.
    BATCH_SIZE = 10 
    
    for i in range(0, total, BATCH_SIZE):
        batch_tickers = tickers[i:i+BATCH_SIZE]
        yf_symbols = [f"{t}.JK" for t in batch_tickers]
        
        print(f"🔄 Memproses Batch {i+1}-{min(i+BATCH_SIZE, total)}...", end=" ")
        
        try:
            # PERUBAHAN KRITIS: period="3y"
            data = yf.download(
                yf_symbols, 
                period="5y", 
                group_by='ticker', 
                progress=False, 
                threads=False, 
                auto_adjust=False 
            )
            
            updates = []
            for ticker in batch_tickers:
                symbol = f"{ticker}.JK"
                try:
                    if len(batch_tickers) > 1:
                        if symbol not in data.columns.levels[0]: continue
                        stock_data = data[symbol].dropna(subset=['Close'])
                    else:
                        stock_data = data.dropna(subset=['Close'])
                        
                    if stock_data.empty: continue
                    
                    # ITERASI HISTORIS: Kita bongkar seluruh baris tanggal untuk saham ini
                    for date_idx, row in stock_data.iterrows():
                        trade_date = date_idx.strftime('%Y-%m-%d')
                        
                        updates.append({
                            "ticker": ticker,
                            "trade_date": trade_date,
                            "open_price": float(row['Open']),
                            "high_price": float(row['High']),
                            "low_price": float(row['Low']),
                            "raw_close": float(row['Close']),
                            "adjusted_close": float(row['Adj Close']), 
                            "volume": int(row['Volume']) if pd.notna(row['Volume']) else 0
                        })
                except Exception as e:
                    continue # Abaikan jika data berantakan (biasanya saham baru IPO)

            # PERTAHANAN DATABASE: Chunking
            # Supabase akan "Stream Reset" jika kita melempar 7.500 baris sekaligus.
            # Kita potong muatan menjadi potongan-potongan kecil berisi 1000 baris.
            if updates:
                CHUNK_SIZE = 1000
                for c in range(0, len(updates), CHUNK_SIZE):
                    chunk = updates[c:c+CHUNK_SIZE]
                    # Kita tidak peduli dengan override admin di sini karena ini data masa lalu
                    supabase.table("daily_market_prices").upsert(chunk, on_conflict="ticker,trade_date").execute()
                    
                print(f"✅ {len(updates)} baris historis disuntikkan.")
            else:
                print("⚠️ Tidak ada data historis yang valid.")

        except Exception as e:
            print(f"❌ Error Eksekusi: {e}")

        # JEDA MUTLAK: Yahoo Finance sangat kejam terhadap penarikan massal bertahun-tahun.
        time.sleep(4) 

    print("\n🎉 AKUISISI DATA HISTORIS 5 TAHUN SELESAI!")

if __name__ == "__main__":
    ingest_historical_data()