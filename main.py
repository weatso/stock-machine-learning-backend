from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from utils import supabase 

app = FastAPI(title="Weatso Kuantitatif API", version="2.0")

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

# HANYA BOLEH ADA SATU FUNGSI SCREENER INI
@app.get("/api/stocks")
@app.get("/stocks/screener")
def get_all_stocks_screener():
    try:
        res = supabase.table("screener_view").select("*").execute()
        return {"data": res.data if res.data else []}
    except Exception as e:
        print(f"❌ API ERROR: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stocks/{ticker}")
def get_stock_detail(ticker: str):
    ticker = ticker.upper()
    try:
        res_info = supabase.table("emitens").select("*").eq("ticker", ticker).execute()
        if not res_info.data:
            raise HTTPException(status_code=404, detail="Emiten tidak ditemukan")
            
        res_ai = supabase.table("ml_predictions").select("*").eq("ticker", ticker).execute()
        
        # TARIK DATA HARGA EOD & TEKNIKAL
        res_history = supabase.table("daily_market_prices")\
            .select("trade_date, open_price, high_price, low_price, raw_close, volume")\
            .eq("ticker", ticker).order("trade_date", desc=True).limit(100).execute()

        res_tech = supabase.table("technical_features")\
            .select("*").eq("ticker", ticker).order("calc_date", desc=True).limit(1).execute()

        # [PERBAIKAN] TARIK DATA FUNDAMENTAL TERBARU
        res_fund = supabase.table("financial_reports")\
            .select("*").eq("ticker", ticker).order("period_date", desc=True).limit(1).execute()

        return {
            "identity": res_info.data[0],
            "ai_analysis": res_ai.data[0] if res_ai.data else None,
            "latest_technical": res_tech.data[0] if res_tech.data else None,
            # Ini yang ditunggu oleh komponen ValuationHeatmap di Next.js:
            "latest_fundamental": res_fund.data[0] if res_fund.data else None, 
            "historical_chart": res_history.data[::-1] if res_history.data else [] 
        }
    except Exception as e:
        print(f"❌ API ERROR: {e}")
        raise HTTPException(status_code=500, detail=str(e))