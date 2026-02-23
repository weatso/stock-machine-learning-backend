import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from supabase import create_client, Client
from dotenv import load_dotenv

# Load Config
load_dotenv()
INVEZGO_KEY = os.getenv("INVEZGO_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY]):
    print("❌ Error: Pastikan file .env (SUPABASE_URL & KEY) sudah diisi!")
    exit()

# Inisialisasi Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def create_session():
    """Membuat session yang tahan banting (Auto Retry jika putus)"""
    session = requests.Session()
    if INVEZGO_KEY:
        session.headers.update({"Authorization": f"Bearer {INVEZGO_KEY}"})
    
    # Konfigurasi Retry: Coba 3x, jeda bertahap
    retries = Retry(
        total=3,
        backoff_factor=1, 
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

session = create_session()

def fetch_invezgo(endpoint):
    try:
        url = f"https://api.invezgo.com{endpoint}"
        res = session.get(url, timeout=15) 
        
        if res.status_code == 200:
            return res.json()
        elif res.status_code == 404:
            print(f"   ⚠️ [404] Data tidak ditemukan.")
            return None
        elif res.status_code == 429:
            print("   ⚠️ [429] Rate Limit! Terlalu cepat.")
            return None
        else:
            print(f"   ❌ Error {res.status_code}: {res.text[:50]}")
            return None
            
    except Exception as e:
        print(f"   ❌ Connection Error: {str(e)[:50]}...")
        return None

def get_all_tickers():
    """
    Mengambil SELURUH ticker dari database, mem-bypass limit 1000 Supabase.
    Digunakan oleh semua worker.
    """
    all_tickers = []
    page_size = 1000
    
    for i in range(5):  # Kapasitas hingga 5000 saham
        start = i * page_size
        end = start + page_size - 1
        
        res = supabase.table("stocks").select("ticker").order('ticker').range(start, end).execute()
        
        if res.data:
            all_tickers.extend([item['ticker'] for item in res.data if item['ticker']])
            
        if len(res.data) < page_size:
            break
            
    return all_tickers