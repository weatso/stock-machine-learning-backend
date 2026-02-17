import os
import requests
from typing import Optional, List
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client
from dotenv import load_dotenv

# --- KONFIGURASI ---
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
INVEZGO_KEY = os.getenv("INVEZGO_API_KEY")

# Inisialisasi App
app = FastAPI(
    title="WEATSO Stock API",
    description="Backend API untuk Data Saham Indonesia & Valuasi Benjamin Graham",
    version="2.0.0"
)

# CORS (Agar Next.js bisa akses)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Di production nanti ganti dengan URL Vercel Anda
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Koneksi Database
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    print(f"❌ Gagal koneksi Supabase: {e}")

# --- ENDPOINTS ---

@app.get("/")
def read_root():
    return {"status": "online", "message": "WEATSO API Ready! 🚀"}

@app.get("/stocks/screener")
def get_stock_screener(
    sector: Optional[str] = None,
    status: Optional[str] = None,
    sort: str = "ticker", # Tambah parameter ini
    limit: int = 100
):
    try:
        # Select data yang dibutuhkan untuk tabel
        query = supabase.table("stocks").select(
            "ticker, company_name, sector, logo_url, "
            "last_price, change_pct, "
            "eps_ttm, bvps, "
            "graham_number, margin_of_safety, valuation_status"
        )

        if sector:
            query = query.eq("sector", sector)
        if status:
            query = query.eq("valuation_status", status)
            
        # LOGIKA SORTING
        if sort == "ticker":
            query = query.order("ticker", desc=False) # A-Z
        elif sort == "mos":
            query = query.order("margin_of_safety", desc=True) # Diskon terbesar
            
        response = query.limit(limit).execute()
        return {"data": response.data, "count": len(response.data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stocks/{ticker}")
def get_stock_detail(ticker: str):
    """
    Endpoint untuk Halaman Detail Saham.
    Mengirimkan SEMUA data (Profil, Fundamental, Valuasi).
    """
    try:
        response = supabase.table("stocks").select("*").eq("ticker", ticker.upper()).execute()
        
        if not response.data:
            raise HTTPException(status_code=404, detail="Saham tidak ditemukan")
            
        return {"data": response.data[0]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stocks/{ticker}/chart")
def get_stock_chart(ticker: str, timeframe: str = "D"):
    """
    Proxy Teknikal Chart.
    Data langsung ditembak ke Invezgo (Realtime) agar tidak membebani database kita.
    """
    try:
        # URL Invezgo untuk history chart (sesuai dokumentasi api-1.json)
        # Endpoint: /analysis/history?ticker={ticker}&interval={timeframe}
        url = f"https://api.invezgo.com/analysis/history?ticker={ticker.upper()}&interval={timeframe}"
        headers = {"Authorization": f"Bearer {INVEZGO_KEY}"}
        
        res = requests.get(url, headers=headers)
        
        if res.status_code != 200:
             raise HTTPException(status_code=res.status_code, detail="Gagal ambil chart dari Invezgo")
             
        return res.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats/sectors")
def get_sectors():
    """
    Untuk dropdown filter kategori di Frontend
    """
    try:
        # Mengambil distinct sector
        # (Supabase JS library belum support distinct mudah, kita pakai RPC atau manual query di frontend sebetulnya)
        # Tapi cara simpel: ambil semua, filter di python (agak lambat kalau data jutaan, tapi ok untuk ribuan)
        res = supabase.table("stocks").select("sector").execute()
        sectors = list(set([item['sector'] for item in res.data if item['sector']]))
        return {"sectors": sorted(sectors)}
    except Exception as e:
         raise HTTPException(status_code=500, detail=str(e))