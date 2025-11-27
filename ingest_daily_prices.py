# File: backend/ingest_daily_prices.py

import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
import os
import yfinance as yf
import time
import sys

# --- Setup Koneksi ---
load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("FATAL ERROR: Pastikan SUPABASE_URL dan SUPABASE_KEY ada di file .env Anda")
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
print("Berhasil terhubung ke Supabase.")

def fetch_all_stocks_from_db():
    """Mengambil daftar ID dan Ticker dari tabel 'stocks'."""
    try:
        response = supabase.table('stocks').select('id, ticker').execute()
        if response.data:
            print(f"Ditemukan {len(response.data)} saham di database master.")
            return response.data
        else:
            print("Tidak ada saham di tabel 'stocks'. Jalankan 'seed_database.py' dulu.")
            return []
    except Exception as e:
        print(f"Error mengambil daftar saham: {e}")
        return []

def ingest_stock_data(stock_id, ticker):
    """Mengunduh data historis dan memasukkannya ke DB (VERSI FIX 5 - Metode BBCA)."""
    try:
        # --- PERUBAHAN UTAMA (METODE BBCA) ---
        ticker_str = f"{ticker}.JK"
        
        # 1. Unduh sebagai LIST OF ONE dan GROUP BY TICKER
        # Ini memaksa yfinance memberi kita struktur data yang konsisten
        df_multi = yf.download([ticker_str], period="max", interval="1d", auto_adjust=False, group_by='ticker')
        
        if df_multi.empty or ticker_str not in df_multi.columns:
            print(f"    -> Tidak ada data untuk {ticker}. Dilewati.")
            return

        # 2. "Ratakan" MultiIndex dengan memilih ticker spesifik
        # Ini adalah trik yang berhasil untuk BBCA
        df = df_multi[ticker_str]
        # --- AKHIR PERUBAHAN UTAMA ---

        # 3. Bersihkan data (Kode ini sekarang akan aman)
        df.reset_index(inplace=True)
        
        if 'Adj Close' in df.columns:
            df.rename(columns={
                'Date': 'date',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Adj Close': 'close',
                'Volume': 'volume'
            }, inplace=True)
            if 'Close' in df.columns:
                df = df.drop(columns=['Close'])
        else:
            df.rename(columns={
                'Date': 'date',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume'
            }, inplace=True)
            
        # 4. Tambahkan 'stock_id'
        df['stock_id'] = stock_id
        
        # 5. Pilih hanya kolom yang kita butuhkan
        kolom = ['date', 'stock_id', 'open', 'high', 'low', 'close', 'volume']
        kolom_valid = [k for k in kolom if k in df.columns]
        df_to_upload = df[kolom_valid].copy()
        
        # 6. Konversi Tipe Data (Ini tetap diperlukan)
        df_to_upload['date'] = df_to_upload['date'].apply(lambda x: x.isoformat())
        
        float_cols = ['open', 'high', 'low', 'close']
        for col in float_cols:
            if col in df_to_upload.columns:
                df_to_upload[col] = df_to_upload[col].apply(lambda x: float(x) if pd.notna(x) else None)

        int_cols = ['volume']
        for col in int_cols:
             if col in df_to_upload.columns:
                df_to_upload[col] = df_to_upload[col].apply(lambda x: int(x) if pd.notna(x) else None)
        
        # 7. Ubah ke format list of dictionaries
        data_list = df_to_upload.to_dict(orient='records')
        
        # 8. Upload ke Supabase
        supabase.table('daily_stock_prices').upsert(data_list, on_conflict='stock_id,date').execute()
        
        print(f"    -> Berhasil meng-upload {len(data_list)} rekor untuk {ticker}.")

    except Exception as e:
        print(f"    -> GAGAL TOTAL pada {ticker}: {e}")
        
def main():
    stocks_to_ingest = fetch_all_stocks_from_db()
    
    if not stocks_to_ingest:
        return
    
    total = len(stocks_to_ingest)
    for index, stock in enumerate(stocks_to_ingest):
        print(f"\n--- Memproses Saham {index + 1}/{total}: {stock['ticker']} ---")
        ingest_stock_data(stock['id'], stock['ticker'])
        time.sleep(0.5) # Jeda agar tidak di-blok yfinance

if __name__ == "__main__":
    main()