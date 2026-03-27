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
    print("🚀 MEMULAI SINKRONISASI MASTER EMITEN (Invezgo API) -> TABEL 'emitens'")

    url_list = "https://api.invezgo.com/analysis/list/stock"
    headers = {"Authorization": f"Bearer {INVEZGO_KEY}"}
    
    print("\n📡 Mengambil daftar seluruh saham dari Invezgo...")
    try:
        res = requests.get(url_list, headers=headers, timeout=15)
        if res.status_code != 200:
            print(f"❌ Gagal ambil list: {res.text}")
            return
            
        stock_list = res.json()
        
        # FILTER ABSOLUT: Tolak semua instrumen turunan (Waran & Right Issue)
        clean_stock_list = [
            s for s in stock_list 
            if isinstance(s.get('code'), str) and not s.get('code').endswith('-W') and not s.get('code').endswith('-R')
        ]
        
        total_stocks = len(clean_stock_list)
        print(f"✅ Berhasil menarik {total_stocks} emiten murni.")
        
        print("💾 Menyimpan data ke tabel 'emitens' secara bertahap...")
        batch_data = []
        
        # MITIGASI STREAM RESET: Turunkan batch ke 50
        BATCH_SIZE = 50 
        
        for index, item in enumerate(clean_stock_list):
            ticker = item.get('code')
            
            batch_data.append({
                "ticker": ticker,
                "company_name": item.get('name'),
                "logo_url": item.get('logo'),
                "sector": "Unknown",
                "is_active": True
            })
            
            # Eksekusi per 50 data dengan penanganan error individual
            if len(batch_data) >= BATCH_SIZE:
                try:
                    supabase.table("emitens").upsert(batch_data, on_conflict="ticker").execute()
                    print(f"   => Tersimpan {index + 1} / {total_stocks} emiten...")
                except Exception as e:
                    print(f"   ❌ Gagal upsert batch pada index {index}: {e}")
                
                batch_data = []
                # MITIGASI STREAM RESET: Jeda napas untuk Supabase
                time.sleep(0.5) 
                
        # Simpan sisa data yang kurang dari 50
        if batch_data:
            try:
                supabase.table("emitens").upsert(batch_data, on_conflict="ticker").execute()
                print(f"   => Sisa data tersimpan.")
            except Exception as e:
                print(f"   ❌ Gagal upsert sisa data: {e}")

        print("\n✅ Data dasar tersimpan! Mulai melengkapi Sektor...")

        failed_details = []

        # 1. BANGUN SESSION YANG TAHAN BANTING (Mitigasi Firewall API)
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        session = requests.Session()
        session.headers.update({"Authorization": f"Bearer {INVEZGO_KEY}"})
        
        # Jika kena Rate Limit (429) atau Server Error (5xx), sistem akan otomatis mencoba ulang 3x
        # dengan jeda waktu yang terus meningkat (1s, 2s, 4s)
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))

        # 2. EKSTRAKSI SEKTOR (Dengan Kecepatan Rasional)
        print("\n⏳ Mengambil data sektor (Ini akan memakan waktu untuk menghindari pemblokiran)...")
        for index, stock in enumerate(clean_stock_list):
            ticker = stock.get('code')
            print(f"   🔄 ({index+1}/{total_stocks}) Update Sektor: {ticker}...", end="\r")
            
            try:
                url_detail = f"https://api.invezgo.com/analysis/information/{ticker}"
                # Gunakan session.get (bukan requests.get) dan naikkan batas toleransi timeout ke 10 detik
                res_det = session.get(url_detail, timeout=10)
                
                if res_det.status_code == 200:
                    info = res_det.json()
                    update_payload = {
                        "sector": info.get('sector') or "Others"
                    }
                    supabase.table("emitens").update(update_payload).eq("ticker", ticker).execute()
                else:
                    failed_details.append(ticker)
                
                # MITIGASI RATE LIMIT: Jeda waktu rasional. Maksimal 3-4 request per detik.
                time.sleep(0.3) 

            except requests.exceptions.Timeout:
                failed_details.append(ticker)
            except Exception as e:
                failed_details.append(ticker)

        print("\n\n🎉 SELESAI! Tabel 'emitens' siap digunakan.")
        if failed_details:
             print(f"⚠️ Masih ada {len(failed_details)} saham yang gagal (Kemungkinan data tidak ada di Invezgo). Aman untuk dilanjutkan.")

    except Exception as e:
        print(f"\n❌ Error Fatal pada eksekusi: {e}")

if __name__ == "__main__":
    seed_master_data()