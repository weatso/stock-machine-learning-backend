import os
import time
import requests
from supabase import create_client, Client
from dotenv import load_dotenv

# 1. Load Konfigurasi dari file .env
load_dotenv()

INVEZGO_KEY = os.getenv("INVEZGO_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not all([INVEZGO_KEY, SUPABASE_URL, SUPABASE_KEY]):
    print("❌ Error: Pastikan file .env sudah diisi lengkap!")
    exit()

# 2. Inisialisasi Koneksi Database
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def seed_master_data():
    print("🚀 MEMULAI PROSES IMPORT MASTER DATA SAHAM...")

    # --- TAHAP 1: AMBIL DAFTAR UTAMA ---
    print("\n📡 Mengambil daftar seluruh saham dari Invezgo...")
    url_list = "https://api.invezgo.com/analysis/list/stock"
    headers = {"Authorization": f"Bearer {INVEZGO_KEY}"}
    
    try:
        res = requests.get(url_list, headers=headers)
        if res.status_code != 200:
            print(f"❌ Gagal ambil list: {res.text}")
            return
        
        stock_list = res.json()
        total_stocks = len(stock_list)
        print(f"✅ Berhasil menarik {total_stocks} saham.")

        # --- TAHAP 2: SIMPAN DATA DASAR (BATCH) ---
        print("💾 Menyimpan data dasar ke Supabase...")
        
        batch_data = []
        for item in stock_list:
            batch_data.append({
                "ticker": item['code'],
                "company_name": item['name'],
                "logo_url": item.get('logo'),
                "updated_at": "now()"
            })
            
            # Simpan per 100 data agar hemat koneksi
            if len(batch_data) >= 100:
                supabase.table("stocks").upsert(batch_data).execute()
                batch_data = [] # Reset batch
        
        # Simpan sisa data
        if batch_data:
            supabase.table("stocks").upsert(batch_data).execute()
            
        print("✅ Data dasar tersimpan! Mulai melengkapi Sektor & Detail...")

        # --- TAHAP 3: LENGKAPI DETAIL (LOOPING) ---
        # Invezgo memisahkan data list dan detail. Kita perlu panggil satu per satu
        # endpoint /analysis/information/{code} untuk dapat Sektor.
        
        for index, stock in enumerate(stock_list):
            ticker = stock['code']
            print(f"   🔄 ({index+1}/{total_stocks}) Update detail: {ticker}...", end="\r")
            
            try:
                url_detail = f"https://api.invezgo.com/analysis/information/{ticker}"
                res_det = requests.get(url_detail, headers=headers)
                
                if res_det.status_code == 200:
                    info = res_det.json()
                    
                    # Update baris di Supabase dengan detail baru
                    update_payload = {
                        "sector": info.get('sector'),
                        "subsector": info.get('subsector'),
                        "industry": info.get('industry'),
                        "subindustry": info.get('subsindustry'),
                        "listing_date": info.get('listing_date'),
                        "updated_at": "now()"
                    }
                    
                    supabase.table("stocks").update(update_payload).eq("ticker", ticker).execute()
                
                # Istirahat sebentar agar tidak dianggap spam oleh server
                time.sleep(0.1) 

            except Exception as e:
                print(f"\n   ⚠️ Error pada {ticker}: {e}")

    except Exception as e:
        print(f"\n❌ Error Fatal: {e}")

    print("\n\n🎉 SELESAI! Database Master Saham sudah siap.")

if __name__ == "__main__":
    seed_master_data()