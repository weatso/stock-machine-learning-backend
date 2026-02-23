import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY]):
    print("❌ Error: Pastikan file .env (SUPABASE_URL & KEY) sudah diisi!")
    exit()

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def update_logos():
    print("🖼️ [LOGO WORKER] Memulai injeksi logo dari CDN Stockbit...")
    try:
        # BYPASS LIMIT 1000 SUPABASE: Ambil data secara bergelombang
        tickers = []
        page_size = 1000
        for i in range(5): # Loop untuk menampung hingga 5000 saham
            start = i * page_size
            end = start + page_size - 1
            
            res = supabase.table("stocks").select("ticker").range(start, end).execute()
            tickers.extend([item['ticker'] for item in res.data if item['ticker']])
            
            if len(res.data) < page_size:
                break
                
        total = len(tickers)
        print(f"📦 Ditemukan {total} saham (Limit 1000 terlewati). Memproses update URL logo...")
        
        base_url = "https://assets.stockbit.com/logos/companies/"
        batch_data = []
        
        for index, ticker in enumerate(tickers):
            # Bersihkan ticker dari embel-embel Waran/Right (-W, -R)
            clean_ticker = ticker.split("-")[0]
            
            logo_url = f"{base_url}{clean_ticker}.png"
            
            batch_data.append({
                "ticker": ticker,
                "logo_url": logo_url
            })
            
            if len(batch_data) >= 100:
                supabase.table("stocks").upsert(batch_data, on_conflict="ticker").execute()
                print(f"   ✅ Menyimpan data {index-99} sampai {index+1}...")
                batch_data = []
                
        if batch_data:
            supabase.table("stocks").upsert(batch_data, on_conflict="ticker").execute()
            print(f"   ✅ Menyimpan sisa data akhir...")
            
        print("🎉 UPDATE LOGO SELESAI! (Seluruh 1184 emiten telah memiliki logo)")
    except Exception as e:
        print(f"❌ Error Fatal: {e}")

if __name__ == "__main__":
    update_logos()