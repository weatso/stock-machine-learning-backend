import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# Impor koneksi yang sudah terbukti solid dari utils
from utils import supabase 

app = FastAPI(title="Weatso AI Stock Screener API", version="2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"status": "Machine Learning API Server is Running", "version": "2.0"}

@app.get("/api/stocks")
def get_all_stocks_screener():
    """
    Hyper-Optimized Screener.
    Memangkas beban komputasi jaringan dari O(N) menjadi O(1).
    Hanya menembak 2 query absolut.
    """
    try:
        # 1. Tarik seluruh tebakan AI sekaligus
        res_ai = supabase.table("ml_predictions").select("ticker, predicted_grade").execute()
        ai_dict = {row['ticker']: row['predicted_grade'] for row in res_ai.data} if res_ai.data else {}

        # 2. Tarik seluruh identitas perusahaan sekaligus
        res_emitens = supabase.table("emitens").select("ticker, company_name, logo_url, sector").execute()
        
        screener_data = []
        if res_emitens.data:
            for emiten in res_emitens.data:
                ticker = emiten['ticker']
                # Gabungkan data di dalam memori RAM, bukan dengan menembak database berulang kali
                if ticker in ai_dict:
                    screener_data.append({
                        "ticker": ticker,
                        "company_name": emiten['company_name'],
                        "sector": emiten['sector'],
                        "logo_url": emiten['logo_url'],
                        "ai_grade": ai_dict[ticker]
                    })
                    
        return {"data": screener_data}
    except Exception as e:
        print(f"❌ API FATAL ERROR: {e}") # Log paksa ke terminal
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stocks")
@app.get("/stocks/screener")
def get_all_stocks_screener():
    try:
        # Gunakan select("*") secara eksplisit untuk menarik latest_price dan mos
        res = supabase.table("screener_view").select("*").execute()
        
        # Log untuk debugging di terminal backend Anda
        if res.data:
            print(f"✅ Berhasil menarik {len(res.data)} saham dari View")
        
        return {"data": res.data if res.data else []}
    except Exception as e:
        print(f"❌ Error API: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stocks/{ticker}")
def get_stock_detail(ticker: str):
    """
    Endpoint berat. Hanya dipanggil saat 1 saham diklik.
    """
    ticker = ticker.upper()
    try:
        res_info = supabase.table("emitens").select("*").eq("ticker", ticker).execute()
        if not res_info.data:
            raise HTTPException(status_code=404, detail="Emiten tidak ditemukan")
            
        res_ai = supabase.table("ml_predictions").select("*").eq("ticker", ticker).execute()
        
        res_history = supabase.table("daily_market_prices")\
            .select("trade_date, open_price, high_price, low_price, raw_close, volume")\
            .eq("ticker", ticker).order("trade_date", desc=True).limit(100).execute()

        res_tech = supabase.table("technical_features")\
            .select("*").eq("ticker", ticker).order("calc_date", desc=True).limit(1).execute()

        return {
            "identity": res_info.data[0],
            "ai_analysis": res_ai.data[0] if res_ai.data else None,
            "latest_technical": res_tech.data[0] if res_tech.data else None,
            "historical_chart": res_history.data[::-1] if res_history.data else [] 
        }
    except HTTPException as he:
        raise he
    except Exception as e:
        print(f"❌ API FATAL ERROR: {e}")
        raise HTTPException(status_code=500, detail=str(e))

        