import time
import json
from utils import supabase, fetch_invezgo, get_all_tickers

def update_profiles():
    # Ambil semua ticker dari database
    tickers = get_all_tickers()
    total = len(tickers)
    
    print(f"🚀 [PROFILE WORKER] Memulai update untuk {total} saham...")
    print("   Target Endpoint: /analysis/information/{code}")

    success_count = 0
    
    for i, ticker in enumerate(tickers):
        # Skip waran atau rights (biasanya berakhiran -W, -R, -P) karena tidak punya profil perusahaan
        if "-" in ticker and len(ticker) > 4:
            print(f"⏩ ({i+1}/{total}) Skip {ticker} (Produk Turunan)")
            continue

        print(f"📄 ({i+1}/{total}) Profil: {ticker}...", end=" ")

        # KOREKSI: Endpoint yang benar sesuai api-1.json adalah /analysis/information/
        data = fetch_invezgo(f"/analysis/information/{ticker}")
        
        if data:
            # Berdasarkan api-1.json, data langsung berupa object (tidak dibungkus 'data')
            # tapi untuk jaga-jaga kita handle kedua format
            d = data.get('data', data) if isinstance(data, dict) else data

            # Mapping Field sesuai dokumentasi api-1.json
            # Field tersedia: industry, sector, listing_date, website, board, ipo_price, description
            
            # Kadang description kosong, kita bisa pakai 'activity' sebagai cadangan
            deskripsi = d.get('description') or d.get('activity') or "Tidak ada deskripsi"
            
            update_data = {
                "description": deskripsi,
                "website": d.get('website'),
                "listing_board": d.get('board'), # Utama / Pengembangan / Akselerasi
                "ipo_date": d.get('listing_date'),
                "ipo_price": d.get('ipo_price'),
                
                # Kita sekalian perbaiki sektor jika di seed awal belum lengkap
                "sector": d.get('sector'), 
                "subsector": d.get('subsector'),
                
                "updated_at": "now()"
            }
            
            try:
                # Update ke Supabase
                supabase.table("stocks").update(update_data).eq("ticker", ticker).execute()
                print("✅")
                success_count += 1
            except Exception as e:
                print(f"❌ DB Error: {e}")
        
        else:
            # Jika masih gagal, biasanya karena saham baru delisting atau ticker tidak valid
            print("⚠️ Kosong/Gagal")
        
        # Jeda 0.2 detik agar aman
        time.sleep(0.2)

    print(f"\n🎉 SELESAI! Berhasil mengupdate {success_count} profil perusahaan.")

if __name__ == "__main__":
    update_profiles()