import time
from utils import supabase, get_all_tickers

def update_logos():
    tickers = get_all_tickers()
    total = len(tickers)
    print(f"🎨 [LOGO WORKER] Mengisi Logo untuk {total} saham...")

    # Kita pakai CDN Stockbit yang sangat lengkap untuk saham Indo
    # Pattern: https://assets.stockbit.com/logos/companies/{TICKER}.png
    
    base_url = "https://assets.stockbit.com/logos/companies/"

    # Batch Update biar cepat (opsional), tapi loop biasa lebih aman
    for i, ticker in enumerate(tickers):
        # Bersihkan ticker (kadang ada kode aneh)
        clean_ticker = ticker.replace("-W", "").replace("-R", "") 
        
        # URL Logo
        logo_url = f"{base_url}{clean_ticker}.png"
        
        print(f"🖼️ ({i+1}/{total}) Set Logo {ticker}...", end=" ")

        try:
            # Update ke Supabase
            supabase.table("stocks").update({"logo_url": logo_url}).eq("ticker", ticker).execute()
            print("✅")
        except Exception as e:
            print(f"❌ Error: {e}")
        
        # Tidak perlu sleep lama karena kita cuma update string URL, tidak request ke API Invezgo
        # Tapi kasih jeda dikit biar database ga kaget
        # time.sleep(0.05) 

    print("\n🎉 UPDATE LOGO SELESAI! Silakan refresh Frontend.")

if __name__ == "__main__":
    update_logos()