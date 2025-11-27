from fastapi import FastAPI, Query, Body, HTTPException # <-- Tambahkan Body dan HTTPException
from fastapi.middleware.cors import CORSMiddleware # Penting untuk koneksi ke Frontend
import joblib
import pandas as pd
import yfinance as yf
import pandas_ta as ta
from pydantic import BaseModel
from supabase import create_client, Client
import os
from dotenv import load_dotenv
from typing import Optional


load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Muat model yg sudah dilatih
model = joblib.load('random_forest_model.joblib')
fitur = ['rsi_14', 'ma_50', 'volume'] # HARUS SAMA DENGAN SAAT TRAINING

# 2. Inisialisasi Aplikasi FastAPI
app = FastAPI()

# 3. Konfigurasi CORS (Cross-Origin Resource Sharing)
# Ini WAJIB agar Next.js (dari domain lain) bisa memanggil API ini
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Nanti ganti dgn URL Vercel Anda
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 4. Definisikan model data untuk request (jika perlu)
# class StockFeatures(BaseModel):
#     rsi_14: float
#     ma_50: float
#     Volume: int

# 5. Endpoint untuk Cek Status
@app.get("/")
def read_root():
    return {"status": "Sistem Pendukung Keputusan Investasi Aktif!"}

# 6. Endpoint untuk mendapatkan rekomendasi
@app.get("/api/predict/{ticker}")
def predict_stock(ticker: str):
    try:
        # 1. Ambil data TERBARU (misal 100 hari terakhir untuk kalkulasi MA 50)
        # UBAH di sini: gunakan list [ticker] dan group_by
        tickers_list = [ticker.upper()] # Pastikan ticker huruf besar
        df_multi = yf.download(tickers_list, period='100d', interval='1d', group_by='ticker')
        
        # --- INI ADALAH PERBAIKANNYA ---
        # Pilih DataFrame untuk ticker spesifik Anda
        df = df_multi[tickers_list[0]]
        
        # 2. Hitung fitur yg sama
        # Ganti nama header agar konsisten (huruf kecil)
        df.columns = [col.lower() for col in df.columns]
        
        # Sekarang pandas_ta akan menemukan 'close'
        df.ta.rsi(length=14, append=True)
        df.ta.sma(length=50, append=True)
        
        # --- INI ADALAH PERBAIKANNYA ---
        # Ganti nama kolom default (RSI_14) ke nama yg kita gunakan (rsi_14)
        # agar cocok dengan data training
        df.rename(columns={'RSI_14': 'rsi_14', 'SMA_50': 'ma_50'}, inplace=True)
        
        # 3. Ambil data hari terakhir yg valid
        latest_data = df.dropna().iloc[-1]
        
        # 4. Format data untuk prediksi
        # 'fitur' diambil dari list global yg sudah kita perbaiki
        features_df = pd.DataFrame([latest_data[fitur]])
        
        # 5. Lakukan Prediksi
        prediction = model.predict(features_df)
        probability = model.predict_proba(features_df)
        
        hasil = "Potensial" if prediction[0] == 1 else "Tidak Potensial"
        skor_potensial = probability[0][1] # Probabilitas kelas '1'
        
        return {
            "ticker": ticker.upper(),
            "prediksi": hasil,
            "skor_potensial": f"{skor_potensial * 100:.2f}%",
            "fitur_terakhir": latest_data[fitur].to_dict()
        }
    except Exception as e:
        # Mengembalikan error yang jelas ke frontend
        print(f"Error pada predict_stock: {e}") # Untuk debug di server
        return {"error": f"Gagal memprediksi {ticker}: Data tidak cukup atau ticker tidak valid."}

# 7. Endpoint untuk mengambil data historis (untuk chart)
@app.get("/api/history/{ticker}")
def get_history(ticker: str):
    try:
        # Ambil data dari Supabase (lebih cepat daripada yfinance)
        response = supabase.table('daily_stock_data').select('date, close') \
                           .eq('ticker', ticker) \
                           .order('date', desc=False) \
                           .execute()
        
        return response.data
    except Exception as e:
        return {"error": str(e)}
    
# Cari fungsi ini dan tambahkan kolom baru di .select()
@app.get("/api/stock/profile/{ticker}")
def get_stock_profile(ticker: str):
    try:
        clean_ticker = ticker.upper().strip()
        
        # --- PERUBAHAN DI SINI ---
        # Tambahkan 'summary', 'website', 'logo_url' ke dalam select
        response = supabase.table('stocks').select(
            'ticker, company_name, market_cap, fundamental_per, fundamental_pbv, fundamental_ps, sectors(name), summary, website'
        ).eq('ticker', clean_ticker).execute()
        # -------------------------
        
        if response.data:
            data = response.data[0]
            if data['sectors']:
                data['sector_name'] = data['sectors']['name']
            else:
                data['sector_name'] = '-'
            del data['sectors']
            return data
        else:
            return {"error": "Saham tidak ditemukan"}
            
    except Exception as e:
        print(f"Error get_stock_profile: {e}")
        return {"error": str(e)}

# 8. Endpoint untuk DASHBOARD / SCREENER (Versi UPGRADE dengan LIMIT)
@app.get("/api/dashboard/all-stocks")
def get_all_stocks(
    search: Optional[str] = Query(None),
    cap_type: Optional[str] = Query(None),
    sector_id: Optional[int] = Query(None),
    sort_by: Optional[str] = Query('market_cap'),
    sort_order: Optional[str] = Query('desc'),
    limit: Optional[int] = Query(100) # <-- INI TAMBAHAN BARU
):
    try:
        BIG_CAP_MIN = 100_000_000_000_000
        MEDIUM_CAP_MIN = 10_000_000_000_000

        query = supabase.table('stocks').select(
            'ticker, company_name, market_cap, fundamental_per, fundamental_pbv, fundamental_ps, sectors(id, name)'
        )
        
        if search:
            search_query = f"%{search}%"
            query = query.or_(f"ticker.ilike.{search_query},company_name.ilike.{search_query}")
            
        if sector_id:
            query = query.filter('sectors.id', 'eq', sector_id)

        if cap_type == 'big':
            query = query.filter('market_cap', 'gte', BIG_CAP_MIN)
        elif cap_type == 'medium':
            query = query.filter('market_cap', 'gte', MEDIUM_CAP_MIN).filter('market_cap', 'lt', BIG_CAP_MIN)
        elif cap_type == 'small':
            query = query.filter('market_cap', 'lt', MEDIUM_CAP_MIN)

        allowed_sort_cols = ['ticker', 'company_name', 'market_cap', 'fundamental_per', 'fundamental_pbv', 'fundamental_ps']
        if sort_by not in allowed_sort_cols:
            sort_by = 'market_cap'
        
        is_descending = sort_order == 'desc'

        # --- INI PERUBAHANNYA ---
        # Ganti .limit(100) menjadi .limit(limit)
        if sort_by in ['fundamental_per', 'fundamental_pbv', 'fundamental_ps']:
             response = query.order(sort_by, desc=is_descending, nullsfirst=is_descending).limit(limit).execute()
        else:
             response = query.order(sort_by, desc=is_descending).limit(limit).execute()
        # --- AKHIR PERUBAHAN ---

        stocks_data = []
        for stock in response.data:
            if stock['sectors']:
                stock['sector_name'] = stock['sectors']['name']
            else:
                stock['sector_name'] = 'N/A'
            del stock['sectors']
            stocks_data.append(stock)
            
        return stocks_data
        
    except Exception as e:
        print(f"Error pada get_all_stocks: {e}")
        return {"error": str(e)}
    

# 9. Endpoint untuk HEATMAP SEKTOR (VERSI BARU - SUPER CEPAT)
@app.get("/api/heatmap")
def get_heatmap_data():
    try:
        # Kita hanya perlu mengambil data dari tabel ringkasan
        # Ini akan instan, tidak ada timeout lagi
        response = supabase.table('sector_performance_summary').select('*').execute()
        
        if response.data:
            return response.data
        else:
            # Ini akan muncul jika Anda belum menjalankan skrip kalkulasi
            return {"error": "Data heatmap belum dikalkulasi. Jalankan 'calculate_heatmap.py'"}
            
    except Exception as e:
        print(f"Error pada get_heatmap_data: {e}")
        return {"error": str(e)}

# 10. Endpoint BARU untuk mengambil daftar sektor (untuk filter dropdown)
@app.get("/api/sectors")
def get_sectors():
    try:
        # Ambil semua sektor dan urutkan berdasarkan nama
        response = supabase.table('sectors').select('id, name').order('name', desc=False).execute()
        return response.data
    except Exception as e:
        print(f"Error pada get_sectors: {e}")
        return {"error": str(e)}
# ... (Baris terakhir Anda, misal: uvicorn.run(...)) ...

# ... (kode endpoint lainnya) ...

# 11. Endpoint KHUSUS ADMIN untuk Update Data Saham
class StockUpdate(BaseModel):
    market_cap: Optional[int] = None
    fundamental_per: Optional[float] = None
    fundamental_pbv: Optional[float] = None
    fundamental_ps: Optional[float] = None

@app.put("/api/stock/{ticker}")
def update_stock(ticker: str, stock: StockUpdate):
    try:
        # Filter data yang tidak None (agar tidak menimpa data lain dengan null)
        # exclude_unset=True berarti hanya field yang dikirim yg diupdate
        update_data = stock.dict(exclude_unset=True)

        if not update_data:
            return {"message": "Tidak ada data yang diupdate"}

        # Lakukan Update ke Supabase
        response = supabase.table('stocks').update(update_data).eq('ticker', ticker.upper()).execute()

        if response.data:
            return {"message": f"Berhasil update {ticker}", "data": response.data[0]}
        else:
            raise HTTPException(status_code=404, detail="Ticker tidak ditemukan")

    except Exception as e:
        print(f"Error update stock: {e}")
        raise HTTPException(status_code=500, detail=str(e))