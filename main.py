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
    limit: int = 3000
):
    try:
        # Base query
        query = supabase.table("stocks").select("*")

        # Filters
        if sector:
            query = query.eq("sector", sector)
        if status:
            query = query.eq("valuation_status", status)

        # Bypass Supabase's 1000 max_rows hard limit using loop ranges
        all_data = []
        page_size = 1000
        
        for i in range(5):  # Loop maksimal 5 kali (Kapasitas 5000 saham)
            start = i * page_size
            end = start + page_size - 1
            
            # Ambil data per rentang (0-999, 1000-1999, dst)
            response = query.range(start, end).execute()
            all_data.extend(response.data)
            
            # Jika data yang didapat kurang dari 1000, berarti itu adalah halaman terakhir
            if len(response.data) < page_size:
                break
                
        # Potong sesuai parameter limit akhir (jika diminta spesifik)
        result_data = all_data[:limit]
        
        return {"data": result_data, "count": len(result_data)}
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

@app.get("/stats/heatmap")
def get_market_heatmap():
    """
    Menghasilkan data hierarkis untuk Treemap / Market Heatmap.
    Dikelompokkan berdasarkan Sektor. Ukuran = Turnover Value, Warna = Change Pct.
    """
    try:
        # Ambil saham yang hari ini aktif (volume > 0) untuk menghindari error kalkulasi
        stocks = supabase.table("stocks")\
            .select("ticker, sector, last_price, daily_volume, change_pct")\
            .gt("daily_volume", 0)\
            .execute().data

        tree = {}
        for s in stocks:
            sec = s.get('sector') or "Others"
            if sec not in tree:
                tree[sec] = []
            
            # Hitung estimasi nilai transaksi (Price * Volume) sebagai ukuran kotak
            price = s.get('last_price') or 0
            vol = s.get('daily_volume') or 0
            val = price * vol
            
            if val > 0:
                tree[sec].append({
                    "name": s['ticker'],
                    "size": val,
                    "change": s.get('change_pct') or 0
                })
        
        # Format ke array untuk Recharts Treemap
        result = []
        for sec, children in tree.items():
            # Urutkan dari size terbesar, ambil maksimal 15 saham per sektor agar UI mulus
            children = sorted(children, key=lambda x: x['size'], reverse=True)[:15]
            if children:
                result.append({
                    "name": sec,
                    "children": children
                })
                
        return result
    except Exception as e:
        print(f"Heatmap Error: {e}")
        return []

@app.get("/news")
def get_market_news(limit: int = 50, ticker: Optional[str] = None, sentiment: Optional[str] = None):
    """
    Mengambil berita pasar yang sudah dianalisis oleh AI.
    Bisa difilter berdasarkan emiten (ticker) atau sentimen.
    """
    try:
        query = supabase.table("market_news").select("*").order("published_at", desc=True)
        
        # Filter berdasarkan sentiment jika diminta
        if sentiment and sentiment.upper() != "ALL":
            query = query.eq("sentiment", sentiment.upper())
            
        # Filter JSONB array jika mencari ticker spesifik
        if ticker:
            # Menggunakan operator JSONB contains di Supabase
            query = query.contains("affected_tickers", [ticker.upper()])
            
        res = query.limit(limit).execute()
        return {"data": res.data}
    except Exception as e:
        print(f"Error fetch news: {e}")
        return {"data": []}

@app.get("/stocks/profile/{ticker}")
def get_stock_profile(ticker: str):
    clean_ticker = ticker.upper().strip()
    try:
        # 1. Tarik Data Utama & Fundamental
        stock_res = supabase.table("stocks").select("*").eq("ticker", clean_ticker).execute()
        
        if not stock_res.data or len(stock_res.data) == 0:
            raise HTTPException(status_code=404, detail="Emiten tidak ditemukan di database.")
            
        stock_data = stock_res.data[0]
        
        # 2. Tarik Berita AI (Perbaikan format JSONB Query)
        # Supabase API lebih stabil menerima string JSON untuk filter array JSONB
        json_filter = f'["{clean_ticker}"]'
        
        news_res = supabase.table("market_news")\
            .select("title, link, published_at, source, sentiment, insight")\
            .contains("affected_tickers", json_filter)\
            .order("published_at", desc=True)\
            .limit(5)\
            .execute()
            
        # Gabungkan data
        stock_data["ai_news"] = news_res.data if news_res.data else []
        
        return stock_data
        
    except HTTPException:
        raise
    except Exception as e:
        # INI ADALAH LOG YANG SEHARUSNYA ANDA BACA:
        print(f"❌ FATAL ERROR fetch profile {clean_ticker}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Terjadi kesalahan internal server: {str(e)}")

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