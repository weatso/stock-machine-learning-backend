import os
from supabase import create_client, Client
from dotenv import load_dotenv

# Load Config
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY]):
    print("❌ Error: Pastikan file .env (SUPABASE_URL & KEY) sudah diisi!")
    exit()

# Inisialisasi Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_all_tickers():
    """
    Mengambil SELURUH ticker dari tabel 'emitens' yang berstatus aktif.
    """
    all_tickers = []
    page_size = 1000
    
    for i in range(5): 
        start = i * page_size
        end = start + page_size - 1
        
        res = supabase.table("emitens").select("ticker").eq("is_active", True).order('ticker').range(start, end).execute()
        
        if res.data:
            all_tickers.extend([item['ticker'] for item in res.data if item['ticker']])
            
        if len(res.data) < page_size:
            break
            
    return all_tickers