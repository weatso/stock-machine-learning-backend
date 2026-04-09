import os
from supabase import create_client, Client
from dotenv import load_dotenv

# Muat environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def check_price_alerts():
    print("🔍 [ALERT WORKER] Memulai pemindaian target harga...")

    # 1. Tarik semua alert yang belum terpicu dan belum dinotifikasi
    alerts_res = supabase.table("user_watchlists")\
        .select("*")\
        .eq("is_triggered", False)\
        .execute()
    alerts = alerts_res.data

    if not alerts:
        print("✅ Tidak ada alert aktif yang perlu dipantau.")
        return

    print(f"📊 Ditemukan {len(alerts)} alert aktif. Memeriksa harga pasar terbaru...")

    triggered_count = 0

    for alert in alerts:
        ticker = alert['ticker']
        target_price = alert['alert_threshold_price']
        user_id = alert['user_id']
        alert_id = alert['id']

        # 2. Tarik harga TERAKHIR dari database market_prices kita
        price_res = supabase.table("daily_market_prices")\
            .select("adjusted_close, trade_date")\
            .eq("ticker", ticker)\
            .order("trade_date", desc=True)\
            .limit(1)\
            .execute()

        if price_res.data:
            latest_price = price_res.data[0]['adjusted_close']
            trade_date = price_res.data[0]['trade_date']
            
            print(f"[{ticker}] Target: {target_price} | Harga Saat Ini ({trade_date}): {latest_price}")

            # 3. LOGIKA TRIGGER: Take Profit (Harga Saat Ini >= Target)
            if latest_price >= target_price:
                print(f"   🚨 TRIGGERED! {ticker} telah menyentuh target {target_price}!")
                
                # UPDATE DATABASE UNTUK MEMICU SUPABASE REALTIME DI FRONTEND
                # Ini adalah jembatan kunci antara Backend Python dan Frontend Next.js
                try:
                    supabase.table("user_watchlists")\
                        .update({
                            "is_triggered": True,
                            "is_notified": False  # Siap untuk ditangkap oleh browser user
                        })\
                        .eq("id", alert_id)\
                        .execute()
                    
                    print(f"   📧 Database di-update. Notifikasi Real-time dikirim ke User ID: {user_id}...")
                    triggered_count += 1
                except Exception as e:
                    print(f"   ❌ Gagal update database untuk alert {alert_id}: {e}")
        else:
            print(f"⚠️ Data harga untuk {ticker} tidak ditemukan.")

    print(f"🏁 Pemindaian selesai. {triggered_count} notifikasi terkirim ke antarmuka pengguna.")

if __name__ == "__main__":
    check_price_alerts()