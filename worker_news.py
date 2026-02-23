import os
import time
import json
import feedparser
from datetime import datetime
from email.utils import parsedate_to_datetime
from supabase import create_client, Client
from dotenv import load_dotenv

# SDK Google GenAI Terbaru
from google import genai
from google.genai import types

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY, GEMINI_API_KEY]):
    print("❌ Error: Pastikan SUPABASE_URL, SUPABASE_KEY, dan GEMINI_API_KEY sudah diisi di .env!")
    exit()

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Inisialisasi Client Baru
client = genai.Client(api_key=GEMINI_API_KEY)

RSS_FEEDS = {
    "CNBC Market": "https://www.cnbcindonesia.com/market/rss",
    "Kontan Bursa": "https://investasi.kontan.co.id/rss/bursa"
}

def parse_date(date_str):
    try:
        dt = parsedate_to_datetime(date_str)
        return dt.isoformat()
    except:
        return datetime.now().isoformat()

def process_news():
    print("📰 [NEWS WORKER] Memulai agregasi dan analisis sentimen berita (GenAI SDK Terbaru)...")
    
    new_articles_count = 0
    system_instruction = "Anda adalah analis pasar modal Indonesia tingkat institusi. Tugas Anda adalah membaca judul dan ringkasan berita, lalu menentukan sentimen pasar (BULLISH, BEARISH, atau NEUTRAL) dan mengidentifikasi kode saham (ticker BEI 4 huruf) yang secara langsung terdampak. Berikan juga 1 kalimat insight analitis yang tajam. Output wajib JSON murni dengan skema: {\"sentiment\": \"BULLISH|BEARISH|NEUTRAL\", \"affected_tickers\": [\"BBCA\", \"GOTO\"], \"insight\": \"string\"}. Jika tidak ada saham spesifik yang dibahas, biarkan array affected_tickers kosong []."
    
    for source_name, feed_url in RSS_FEEDS.items():
        print(f"\n📡 Menarik data dari: {source_name}")
        feed = feedparser.parse(feed_url)
        
        # Ambil maksimal 15 berita terbaru per sumber
        entries = feed.entries[:15]
        
        for entry in entries:
            title = entry.get('title', '').strip()
            link = entry.get('link', '').strip()
            published = entry.get('published', '')
            summary = entry.get('summary', '').strip()
            
            if not title or not link:
                continue
                
            # Cek Duplikasi di Database
            check = supabase.table("market_news").select("id").eq("link", link).execute()
            if len(check.data) > 0:
                continue 
                
            print(f"   🧠 Menganalisis: {title[:60]}...")
            prompt = f"Judul: {title}\nRingkasan: {summary}"
            
            try:
                # Eksekusi Analisis Kognitif dengan SDK Baru
                response = client.models.generate_content(
                    model='gemini-2.5-flash',
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        temperature=0.1,
                        response_mime_type="application/json",
                        system_instruction=system_instruction
                    )
                )
                
                llm_output = response.text
                analysis = json.loads(llm_output)
                
                sentiment = analysis.get("sentiment", "NEUTRAL")
                affected_tickers = analysis.get("affected_tickers", [])
                insight = analysis.get("insight", "")
                
                # Validasi format ticker
                clean_tickers = [str(t).upper().strip() for t in affected_tickers if isinstance(t, str) and len(str(t).strip()) == 4]
                
                news_data = {
                    "title": title,
                    "link": link,
                    "published_at": parse_date(published),
                    "source": source_name,
                    "sentiment": sentiment,
                    "insight": insight,
                    "affected_tickers": clean_tickers
                }
                
                supabase.table("market_news").insert(news_data).execute()
                print(f"      ✅ [{sentiment}] Tickers: {clean_tickers}")
                new_articles_count += 1
                
            except json.JSONDecodeError:
                print("      ❌ Gagal memparsing JSON dari AI.")
            except Exception as e:
                print(f"      ❌ Error AI/DB: {e}")
                
            # Jeda Rate Limit
            time.sleep(4)

    print(f"\n🎉 PROSES SELESAI! {new_articles_count} berita baru berhasil dianalisis dan disimpan.")

if __name__ == "__main__":
    process_news()