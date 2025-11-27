import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
import os
import yfinance as yf
import time
import sys

load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_all_tickers():
    try:
        response = supabase.table('stocks').select('ticker').order('ticker').execute()
        return [row['ticker'] for row in response.data]
    except Exception as e:
        print(f"Gagal: {e}")
        return []

def update_profile(ticker):
    print(f"Updating Profile: {ticker}...", end=" ", flush=True)
    try:
        yf_ticker = yf.Ticker(f"{ticker}.JK")
        info = yf_ticker.info
        
        # Ambil data profil
        summary = info.get('longBusinessSummary')
        website = info.get('website')


        update_data = {
            'summary': summary,
            'website': website,
        }

        supabase.table('stocks').update(update_data).eq('ticker', ticker).execute()
        print("OK.")
    except Exception as e:
        print(f"ERROR: {e}")

def main():
    tickers = get_all_tickers()
    # Bisa batasi dulu untuk tes: tickers[:10]
    for ticker in tickers:
        update_profile(ticker)
        time.sleep(0.5) # Jeda sopan

if __name__ == "__main__":
    main()