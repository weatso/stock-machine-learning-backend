import os
import requests
from typing import Optional, List
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client
from dotenv import load_dotenv
import yfinance as yf
import pandas as pd

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
    sort_by: str = "ticker",    # Field yang mau disort
    sort_order: str = "asc",    # Arah: 'asc' atau 'desc'
    limit: int = 1000
):
    try:
        # Mapping dari Frontend ke Kolom Database
        sort_map = {
            "ticker": "ticker",
            "company_name": "company_name",
            "sector": "sector",
            "last_price": "last_price",
            "graham_number": "graham_number",
            "margin_of_safety": "margin_of_safety",
            "change_pct": "change_pct"
        }
        
        # Ambil nama kolom asli, default ke ticker jika tidak ketemu
        db_column = sort_map.get(sort_by, "ticker")
        is_desc = (sort_order.lower() == "desc")

        query = supabase.table("stocks").select("*")

        # Filter
        if sector:
            query = query.eq("sector", sector)
        if status:
            query = query.eq("valuation_status", status)
            
        # Sorting Dinamis
        # Supabase Python client menggunakan .order(column, desc=True/False)
        query = query.order(db_column, desc=is_desc)
            
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
def get_stock_chart(ticker: str, timeframe: str = "5y"): 
    """
    Mengambil data OHLCV historis dari Yahoo Finance.
    Timeframe: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max
    """
    try:
        # Tambahkan .JK
        symbol = f"{ticker.upper()}.JK"
        stock = yf.Ticker(symbol)
        
        # Ambil history "sejauh mungkin" sesuai request (max)
        # Atau default '5y' biar chart enak dilihat
        hist = stock.history(period=timeframe)
        
        # Reset index agar Date menjadi kolom
        hist.reset_index(inplace=True)
        
        # Format ke JSON array yang bersih untuk Recharts/TradingView
        chart_data = []
        for _, row in hist.iterrows():
            chart_data.append({
                "time": row['Date'].strftime('%Y-%m-%d'), # Format YYYY-MM-DD
                "open": row['Open'],
                "high": row['High'],
                "low": row['Low'],
                "close": row['Close'],
                "volume": row['Volume']
            })
            
        return chart_data
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

# --- TAMBAHKAN DI BAGIAN BAWAH MAIN.PY ---

@app.get("/stats/dashboard-widgets")
def get_dashboard_widgets():
    """
    Satu endpoint untuk semua widget dashboard biar cepat loadingnya.
    Mengembalikan: Graham Radar, Gainers, Losers, dan AI Insight.
    """
    try:
        # 1. GRAHAM RADAR (5 Saham Undervalued dengan MOS Tertinggi, Liquid Only)
        # Filter: Undervalued, Volume > 1M (biar ga saham gorengan sepi), Urutkan MOS
        radar = supabase.table("stocks")\
            .select("ticker, company_name, last_price, margin_of_safety, change_pct")\
            .eq("valuation_status", "Undervalued")\
            .gt("daily_volume", 1000000)\
            .order("margin_of_safety", desc=True)\
            .limit(5)\
            .execute().data

        # 2. TOP GAINERS (5 Saham)
        gainers = supabase.table("stocks")\
            .select("ticker, last_price, change_pct")\
            .order("change_pct", desc=True)\
            .limit(5)\
            .execute().data

        # 3. TOP LOSERS (5 Saham)
        losers = supabase.table("stocks")\
            .select("ticker, last_price, change_pct")\
            .order("change_pct", desc=False)\
            .limit(5)\
            .execute().data

        # 4. "AI" INSIGHT GENERATOR (Analisis Data Statistik)
        # Kita hitung statistik sederhana untuk membuat kalimat 'pintar'
        stats = supabase.table("stocks").select("valuation_status, change_pct").execute().data
        
        total_stocks = len(stats)
        undervalued_count = sum(1 for s in stats if s.get('valuation_status') == 'Undervalued')
        green_stocks = sum(1 for s in stats if (s.get('change_pct') or 0) > 0)
        
        # Logika Kalimat AI
        sentiment = "Netral"
        if green_stocks > (total_stocks / 2):
            sentiment = "Bullish (Optimis)"
        else:
            sentiment = "Bearish (Pesimis)"

        insight_text = (
            f"Pasar saat ini cenderung {sentiment}. "
            f"Terdeteksi {undervalued_count} saham yang berada di area 'Undervalued' menurut perhitungan Graham. "
            f"Ini adalah {(undervalued_count/total_stocks)*100:.1f}% dari total pasar. "
        )

        if undervalued_count > (total_stocks * 0.3):
             insight_text += "Saat ini adalah waktu yang menarik untuk Value Investing karena banyak saham diskon."
        else:
             insight_text += "Pasar relatif mahal, berhati-hatilah dalam memilih saham."

        return {
            "radar": radar,
            "gainers": gainers,
            "losers": losers,
            "insight": {
                "text": insight_text,
                "sentiment": sentiment,
                "undervalued_count": undervalued_count
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))