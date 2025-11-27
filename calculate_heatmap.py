# File: backend/calculate_heatmap.py

import sys
import time
from dotenv import load_dotenv
import os
from supabase import create_client, Client

# --- Setup Koneksi ---
load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("FATAL ERROR: Pastikan SUPABASE_URL dan SUPABASE_KEY ada di file .env Anda")
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
print("Berhasil terhubung ke Supabase.")

def fetch_stocks_and_sectors():
    """Mengambil semua saham & peta sektor dari DB."""
    try:
        # Ambil semua saham yang punya sector_id
        stocks_res = supabase.table('stocks').select('id, ticker, sector_id').filter('sector_id', 'not.is', 'null').execute()
        
        # Ambil semua nama sektor
        sectors_res = supabase.table('sectors').select('id, name').execute()
        
        # Buat "peta" dari ID ke Nama (cth: {1: 'Keuangan'})
        sector_map = {row['id']: row['name'] for row in sectors_res.data}
        
        print(f"Ditemukan {len(stocks_res.data)} saham dan {len(sector_map)} sektor.")
        return stocks_res.data, sector_map
    except Exception as e:
        print(f"Error mengambil data master: {e}")
        return [], {}

def get_price_changes_for_stock(stock_id):
    """Mengambil harga terbaru & sebelumnya untuk 1 saham."""
    try:
        # Ambil 22 data harga terakhir untuk 1 saham
        res = supabase.table('daily_stock_prices').select('close').filter('stock_id', 'eq', stock_id).order('date', desc=True).limit(22).execute()
        
        # Cek apakah datanya cukup (minimal 22 hari)
        if not res.data or len(res.data) < 22:
            return None # Tidak cukup data, lewati saham ini

        # Ekstrak harga
        prices = [r['close'] for r in res.data]
        p_today = prices[0]
        p_1d_ago = prices[1]
        p_1w_ago = prices[5]  # 5 hari kerja lalu
        p_1m_ago = prices[21] # 21 hari kerja lalu
        
        # Fungsi helper untuk menghitung %
        def calc_pct(new, old):
            if old is None or old == 0 or new is None:
                return None
            return ((new - old) / old) * 100

        # Hitung perubahannya
        return {
            'daily': calc_pct(p_today, p_1d_ago),
            'weekly': calc_pct(p_today, p_1w_ago),
            'monthly': calc_pct(p_today, p_1m_ago)
        }
        
    except Exception as e:
        print(f"    -> Peringatan (gagal ambil harga): {e}")
        return None

def main():
    print("Mulai menghitung performa sektor...")
    stocks, sector_map = fetch_stocks_and_sectors()
    
    if not stocks:
        print("Tidak ada saham untuk diproses. Hentikan.")
        return

    # Siapkan "keranjang" untuk menyimpan hasil
    # cth: {1: {'daily': [0.5, -0.2], 'weekly': [1.2, 0.9], ...}}
    sector_results_raw = {
        sector_id: {'daily': [], 'weekly': [], 'monthly': []} 
        for sector_id in sector_map.keys()
    }

    # --- Ini adalah "Pekerjaan Berat" ---
    print("Memproses harga untuk setiap saham...")
    for index, stock in enumerate(stocks):
        # Tampilkan progres setiap 20 saham
        if (index + 1) % 20 == 0:
            print(f"  ...memproses {index + 1}/{len(stocks)} ({stock['ticker']})")
        
        changes = get_price_changes_for_stock(stock['id'])
        
        # Jika data harga valid, masukkan ke keranjang
        if changes:
            sector_id = stock['sector_id']
            if changes['daily'] is not None:
                sector_results_raw[sector_id]['daily'].append(changes['daily'])
            if changes['weekly'] is not None:
                sector_results_raw[sector_id]['weekly'].append(changes['weekly'])
            if changes['monthly'] is not None:
                sector_results_raw[sector_id]['monthly'].append(changes['monthly'])
        
        # Jeda singkat agar tidak membebani database
        time.sleep(0.05) 
    
    # --- Kalkulasi Rata-rata ---
    print("Menghitung rata-rata performa sektor...")
    final_data_to_upload = []
    
    for sector_id, name in sector_map.items():
        daily_list = sector_results_raw[sector_id]['daily']
        weekly_list = sector_results_raw[sector_id]['weekly']
        monthly_list = sector_results_raw[sector_id]['monthly']
        
        # Hitung rata-rata, jika list kosong, hasilnya 0
        avg_daily = (sum(daily_list) / len(daily_list)) if daily_list else 0
        avg_weekly = (sum(weekly_list) / len(weekly_list)) if weekly_list else 0
        avg_monthly = (sum(monthly_list) / len(monthly_list)) if monthly_list else 0
        
        final_data_to_upload.append({
            'sector_id': sector_id,
            'sector_name': name,
            'avg_daily_change': avg_daily,
            'avg_weekly_change': avg_weekly,
            'avg_monthly_change': avg_monthly
        })

    # --- Upload Hasil ke Tabel Ringkasan ---
    print("Meng-upload hasil ke tabel 'sector_performance_summary'...")
    try:
        # 1. Hapus semua data LAMA
        supabase.table('sector_performance_summary').delete().neq('sector_id', -1).execute() 
        
        # 2. Masukkan data BARU
        supabase.table('sector_performance_summary').insert(final_data_to_upload).execute()
        
        print(f"BERHASIL! Heatmap telah diperbarui dengan {len(final_data_to_upload)} sektor.")
    except Exception as e:
        print(f"GAGAL meng-upload hasil: {e}")

if __name__ == "__main__":
    main()