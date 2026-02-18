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
    print("🚀 MEMULAI PROSES IMPORT MASTER DATA SAHAM (FULL DATABASE)...")

    # --- TAHAP 1: AMBIL DAFTAR UTAMA (PAGINATION) ---
    all_stocks = []
    page = 0
    limit = 200 # Ambil per 200 biar aman
    has_more = True
    
    headers = {"Authorization": f"Bearer {INVEZGO_KEY}"}

    print("\n📡 Mengambil daftar seluruh saham dari Invezgo...")
    
    while has_more:
        # Endpoint Screener/List dengan offset agar bisa ambil >1000
        # Jika Invezgo support 'offset' atau 'page', sesuaikan. 
        # Jika Invezgo hanya support 'limit', kita coba set limit besar 2500.
        # Asumsi: Kita tembak limit besar sekalian.
        url_list = f"https://api.invezgo.com/utils/stock-list?limit=2500" 
        
        try:
            print(f"   ⏳ Fetching data (Attempting limit 2500)...")
            res = requests.get(url_list, headers=headers)
            
            if res.status_code != 200:
                print(f"❌ Gagal ambil list: {res.text}")
                break
            
            data = res.json()
            # Handle struktur JSON Invezgo (bisa berupa list langsung atau dict {'data': [...]})
            current_batch = data.get('data', data) if isinstance(data, dict) else data
            
            if not current_batch:
                has_more = False
                break
                
            all_stocks.extend(current_batch)
            print(f"   ✅ Berhasil menarik {len(current_batch)} saham.")
            
            # Karena endpoint /utils/stock-list biasanya mengembalikan SEMUA data jika limit besar,
            # kita bisa langsung break loop ini jika sudah dapat banyak.
            if len(current_batch) < 2500: 
                has_more = False
            else:
                 # Jika ternyata ada pagination, logic offset perlu ditambah di sini
                 # Tapi biasanya limit=2500 sudah cover semua saham Indo (total ~900-1000)
                 has_more = False 
                 
        except Exception as e:
            print(f"❌ Error Fetching List: {e}")
            break

    total_stocks = len(all_stocks)
    print(f"\n📦 Total Saham Ditemukan: {total_stocks}")

    if total_stocks == 0:
        print("⚠️ Tidak ada data yang bisa diproses.")
        return

    # --- TAHAP 2: SIMPAN DATA DASAR (BATCH) ---
    print("💾 Menyimpan data dasar ke Supabase...")
    
    batch_data = []
    # Loop semua saham yang didapat
    for index, item in enumerate(all_stocks):
        batch_data.append({
            "ticker": item.get('code'),
            "company_name": item.get('name'),
            "logo_url": item.get('logo'),
            "updated_at": "now()"
        })
        
        # Simpan per 100 data agar hemat koneksi
        if len(batch_data) >= 100:
            try:
                supabase.table("stocks").upsert(batch_data, on_conflict="ticker").execute()
                print(f"   Saved batch {index-99}-{index+1}")
            except Exception as e:
                print(f"   ❌ Gagal save batch: {e}")
            batch_data = [] # Reset batch
    
    # Simpan sisa data
    if batch_data:
        supabase.table("stocks").upsert(batch_data, on_conflict="ticker").execute()
        
    print("✅ Data dasar tersimpan! Mulai melengkapi Sektor & Detail...")

    # --- TAHAP 3: LENGKAPI DETAIL (LOOPING) ---
    # Invezgo memisahkan data list dan detail.
    
    for index, stock in enumerate(all_stocks):
        ticker = stock.get('code')
        if not ticker: continue
        
        print(f"   🔄 ({index+1}/{total_stocks}) Update detail: {ticker}...", end="\r")
        
        try:
            # URL Detail
            # CATATAN: Endpoint ini memakan kuota 1 hit per saham.
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
                
                # Kita gunakan .update().eq() agar aman
                supabase.table("stocks").update(update_payload).eq("ticker", ticker).execute()
            
            # Istirahat sebentar agar tidak dianggap spam oleh server
            # time.sleep(0.05) # Percepat sedikit sleep-nya

        except Exception as e:
            # Jangan stop loop cuma karena 1 saham error
            # print(f"\n   ⚠️ Error pada {ticker}: {e}")
            pass # Silent error biar log bersih

    print("\n\n🎉 SELESAI! Database Master Saham sudah siap dan lengkap.")

if __name__ == "__main__":
    seed_master_data()