import os
import pandas as pd
from datetime import datetime
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import TimeSeriesSplit, GridSearchCV
from dotenv import load_dotenv
from utils import supabase, get_all_tickers
import warnings

# Abaikan warning dari pandas/sklearn agar terminal tetap bersih
warnings.filterwarnings('ignore')
load_dotenv()

def train_and_predict():
    tickers = get_all_tickers()
    total = len(tickers)
    print(f"🧠 [ML ENGINE ADVANCED] Memulai Hyperparameter Tuning & Prediksi untuk {total} emiten...")
    
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    # Konfigurasi Grid Search (Mencari parameter otak paling optimal)
    param_grid = {
        'n_estimators': [50, 100], # Uji dengan 50 pohon vs 100 pohon
        'max_depth': [None, 5, 10] # Uji kedalaman logika tak terbatas vs dibatasi
    }

    for i, ticker in enumerate(tickers):
        print(f"🤖 ({i+1}/{total}) Fitting Model: {ticker}...", end=" ")
        
        try:
            # 1. TARIK DATA FITUR (4 Variabel X)
            res_feat = supabase.table("technical_features")\
                .select("calc_date, rsi_14, macd, margin_of_safety, mfi_14")\
                .eq("ticker", ticker).order("calc_date", desc=False).execute()
                
            # 2. TARIK DATA HARGA (Variabel Y)
            res_price = supabase.table("daily_market_prices")\
                .select("trade_date, adjusted_close")\
                .eq("ticker", ticker).order("trade_date", desc=False).execute()

            if not res_feat.data or not res_price.data or len(res_feat.data) < 50:
                print("⚠️ Skip (Data < 50 baris)")
                continue

            df_feat = pd.DataFrame(res_feat.data).rename(columns={"calc_date": "date"})
            df_price = pd.DataFrame(res_price.data).rename(columns={"trade_date": "date"})
            
            # 3. PENGGABUNGAN DATA & LABELING T+5
            df = pd.merge(df_feat, df_price, on="date", how="inner")
            if df.empty: continue
            
            df['adjusted_close'] = pd.to_numeric(df['adjusted_close'])
            df['future_price_5d'] = df['adjusted_close'].shift(-5)
            
            def assign_grade(row):
                if pd.isna(row['future_price_5d']): return None
                ret = ((row['future_price_5d'] - row['adjusted_close']) / row['adjusted_close']) * 100
                if ret >= 3.0: return 'A'
                elif ret <= -3.0: return 'C'
                else: return 'B'
                
            df['target_grade'] = df.apply(assign_grade, axis=1)
            
            # 4. PEMISAHAN DATA
            today_data = df.iloc[-1:] # Baris hari ini yang belum punya masa depan
            train_data = df.dropna(subset=['target_grade']) 
            
            if len(train_data) < 30:
                print("⚠️ Skip (Data latih kurang)")
                continue
                
            # Fitur sekarang memasukkan pilar ketiga (mfi_14)
            features = ['rsi_14', 'macd', 'margin_of_safety', 'mfi_14']
            X_train = train_data[features]
            Y_train = train_data['target_grade']
            X_today = today_data[features]
            
            # 5. ARSITEKTUR ANTI-KEBOCORAN DATA (TimeSeriesSplit)
            tscv = TimeSeriesSplit(n_splits=3)
            
            # 6. EVOLUSI OTOMATIS (GridSearchCV)
            rf = RandomForestClassifier(random_state=42, class_weight='balanced')
            grid_search = GridSearchCV(
                estimator=rf, 
                param_grid=param_grid, 
                cv=tscv, 
                scoring='accuracy',
                n_jobs=-1 # Gunakan seluruh core CPU laptop Anda
            )
            
            # Proses komputasi berat dimulai di sini
            grid_search.fit(X_train, Y_train)
            
            # Ambil model terbaik hasil evolusi
            best_model = grid_search.best_estimator_
            
            # 7. PREDIKSI HARI INI
            prediction = best_model.predict(X_today)[0]
            
            # 8. EKSTRAKSI KEPINTARAN (Mengapa AI memilih Grade tersebut?)
            importances = best_model.feature_importances_
            feat_imp_dict = {
                "rsi_14": round(float(importances[0]), 4),
                "macd": round(float(importances[1]), 4),
                "margin_of_safety": round(float(importances[2]), 4),
                "mfi_14": round(float(importances[3]), 4)
            }
            
            # 9. SIMPAN KE DATABASE
            payload = {
                "ticker": ticker,
                "prediction_date": today_str,
                "predicted_grade": prediction,
                "feature_importance": feat_imp_dict
            }
            
            supabase.table("ml_predictions").upsert(payload, on_conflict="ticker,prediction_date").execute()
            
            # Cetak hasil dan parameter terbaik yang ditemukan mesin
            print(f"✅ Grade: {prediction} (Opt: {grid_search.best_params_['n_estimators']} trees)")

        except Exception as e:
            print(f"❌ Error: {e}")
            
    print("\n🎉 SELURUH MODEL SELESAI DILATIH & PREDIKSI HARI INI TELAH DISIMPAN!")

if __name__ == "__main__":
    train_and_predict()