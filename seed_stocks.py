import os
import time
import requests
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

INVEZGO_KEY = os.getenv("INVEZGO_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not all([INVEZGO_KEY, SUPABASE_URL, SUPABASE_KEY]):
    print("❌ Error: Pastikan file .env sudah diisi lengkap!")
    exit()

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def seed_master_data():
    print("🚀 MEMULAI PROSES IMPORT MASTER DATA SAHAM (Invezgo API)...")

    # Menggunakan endpoint tanpa pagination sesuai dokumen
    url_list = "https://api.invezgo.com/analysis/list/stock"
    headers = {"Authorization": f"Bearer {INVEZGO_KEY}"}
    
    print("\n📡 Mengambil daftar seluruh saham aktif dari Invezgo...")
    try:
        # Timeout 15 detik untuk memanggil daftar utama
        res = requests.get(url_list, headers=headers, timeout=15)
        if res.status_code != 200:
            print(f"❌ Gagal ambil list: {res.text}")
            return
            
        stock_list = res.json()
        total_stocks = len(stock_list)
        print(f"✅ Berhasil menarik {total_stocks} emiten.")
        
        print("💾 Menyimpan data dasar ke Supabase...")
        batch_data = []
        for item in stock_list:
            batch_data.append({
                "ticker": item.get('code'),
                "company_name": item.get('name'),
                "logo_url": item.get('logo'),
                "updated_at": "now()"
            })
            
            # Batch upsert per 100 data
            if len(batch_data) >= 100:
                supabase.table("stocks").upsert(batch_data, on_conflict="ticker").execute()
                batch_data = []
                
        if batch_data:
            supabase.table("stocks").upsert(batch_data, on_conflict="ticker").execute()

        print("✅ Data dasar tersimpan! Mulai melengkapi Data Industri/Sektor...")

        failed_details = []

        # Looping endpoint Information
        for index, stock in enumerate(stock_list):
            ticker = stock.get('code')
            if not ticker: continue
            
            print(f"   🔄 ({index+1}/{total_stocks}) Ekstraksi detail: {ticker}...", end="\r")
            
            try:
                url_detail = f"https://api.invezgo.com/analysis/information/{ticker}"
                # PERTAHANAN: Timeout 5 detik agar skrip tidak hang selamanya seperti kasus MEDC-W
                res_det = requests.get(url_detail, headers=headers, timeout=5)
                
                if res_det.status_code == 200:
                    info = res_det.json()
                    
                    update_payload = {
                        "sector": info.get('sector'),
                        "subsector": info.get('subsector'),
                        "industry": info.get('industry'),
                        "subindustry": info.get('subsindustry'),
                        "listing_date": info.get('listing_date'),
                        "updated_at": "now()"
                    }
                    supabase.table("stocks").update(update_payload).eq("ticker", ticker).execute()
                else:
                    # Gagal HTTP (misal 404/500)
                    failed_details.append(ticker)
                
                # Jeda agar tidak kena Error 429
                time.sleep(0.1) 

            except requests.exceptions.Timeout:
                # Menangkap error jika server Invezgo bengong
                failed_details.append(ticker)
            except Exception as e:
                # Error lainnya
                failed_details.append(ticker)

        print("\n\n🎉 SELESAI! Database Master Saham Anda sudah tersinkronisasi.")
        
        # LAPORAN SAHAM GAGAL
        if failed_details:
            failed_details = list(set(failed_details))
            print(f"\n⚠️ Terdapat {len(failed_details)} emiten yang gagal ditarik detail sektornya (Biasanya Waran/Right/Timeout):")
            for i in range(0, len(failed_details), 15):
                print(", ".join(failed_details[i:i+15]))

    except Exception as e:
        print(f"\n❌ Error Fatal pada eksekusi: {e}")

if __name__ == "__main__":
    seed_master_data()