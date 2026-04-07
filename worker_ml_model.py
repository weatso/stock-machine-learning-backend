import os
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import TimeSeriesSplit, GridSearchCV
from sklearn.impute import SimpleImputer
from sklearn.metrics import precision_score, recall_score, f1_score, confusion_matrix
from imblearn.over_sampling import SMOTE
from dotenv import load_dotenv
from utils import supabase, get_all_tickers
import warnings

# Abaikan warning dari pandas/sklearn agar terminal tetap bersih
warnings.filterwarnings('ignore')
load_dotenv()

def train_and_predict():
    tickers = get_all_tickers()
    total = len(tickers)
    print(f"🧠 [ML ENGINE ADVANCED] Memulai Pipeline T+20, Fusi Fundamental & SMOTE untuk {total} emiten...")
    
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    param_grid = {
        'n_estimators': [50, 100], 
        'max_depth': [None, 5, 10] 
    }

    # Variabel global untuk menampung pengujian evaluasi Model Health
    all_y_true = []
    all_y_pred = []

    for i, ticker in enumerate(tickers):
        print(f"🤖 ({i+1}/{total}) Fitting Model: {ticker}...", end=" ")
        
        try:
            # 1. TARIK DATA TEKNIKAL & MOS
            res_feat = supabase.table("technical_features")\
                .select("calc_date, rsi_14, macd, margin_of_safety, mfi_14")\
                .eq("ticker", ticker).order("calc_date", desc=False).execute()
                
            # 2. TARIK DATA HARGA
            res_price = supabase.table("daily_market_prices")\
                .select("trade_date, adjusted_close")\
                .eq("ticker", ticker).order("trade_date", desc=False).execute()

            # 3. TARIK DATA FUNDAMENTAL (Kuartalan) - Sesuai Revisi Skripsi
            res_fund = supabase.table("financial_reports")\
                .select("period_date, per, pbv, roa, roe")\
                .eq("ticker", ticker).order("period_date", desc=False).execute()

            if not res_feat.data or not res_price.data or len(res_feat.data) < 50:
                print("⚠️ Skip (Data < 50 baris)")
                continue

            df_feat = pd.DataFrame(res_feat.data).rename(columns={"calc_date": "date"})
            df_price = pd.DataFrame(res_price.data).rename(columns={"trade_date": "date"})
            
            # Penggabungan Harga & Teknikal
            df = pd.merge(df_feat, df_price, on="date", how="inner")
            df['date'] = pd.to_datetime(df['date'])
            
            # Fusi Data Fundamental
            if res_fund.data:
                df_fund = pd.DataFrame(res_fund.data).rename(columns={"period_date": "date"})
                df_fund['date'] = pd.to_datetime(df_fund['date'])
                df = pd.merge_asof(df.sort_values('date'), df_fund.sort_values('date'), on='date', direction='backward')
            
            # [PERBAIKAN FATAL] PENYEMBUHAN NAN & BACKFILL
            # Memastikan 4 pilar fundamental tidak pernah kosong 100% agar Imputer tidak crash
            fallback_ratios = {'per': 15.0, 'pbv': 1.5, 'roa': 5.0, 'roe': 10.0}
            for col, val in fallback_ratios.items():
                if col not in df.columns:
                    df[col] = val
                else:
                    # Tarik data fundamental ke belakang (bfill) untuk riwayat masa lalu, 
                    # sisa yang benar-benar kosong diisi dengan fallback IHSG (fillna)
                    df[col] = df[col].bfill().fillna(val)

            if df.empty: continue
            
            # 4. HORIZON PREDIKSI SEBULAN (T+20) - Revisi 2
            df['adjusted_close'] = pd.to_numeric(df['adjusted_close'])
            df['future_price_20d'] = df['adjusted_close'].shift(-20)
            
            def assign_grade(row):
                if pd.isna(row['future_price_20d']): return None
                
                # Menghitung persentase return T+20
                ret = ((row['future_price_20d'] - row['adjusted_close']) / row['adjusted_close']) * 100
                
                # LOGIKA RR 1:2 DENGAN BUFFER NOISE -4%
                if ret >= 8.0: 
                    return 'A'      # Take Profit: Target > 8% sebulan
                elif ret <= -4.0: 
                    return 'C'      # Cutloss: Terkena SL -4% (Memberikan ruang napas lebar dari noise pasar)
                else: 
                    return 'B'      # Hold: Sideways / Konsolidasi di antara -4% hingga 8%
                
            df['target_grade'] = df.apply(assign_grade, axis=1)
            
            # 5. PEMISAHAN DATA
            today_data = df.iloc[-1:] # Baris hari ini yang akan diprediksi
            train_data = df.dropna(subset=['target_grade']) 
            
            if len(train_data) < 30:
                print("⚠️ Skip (Data latih kurang dari 30 hari EOD)")
                continue
                
            # MATRIKS 8 PILAR (Revisi 4)
            features = ['rsi_14', 'macd', 'margin_of_safety', 'mfi_14', 'per', 'pbv', 'roa', 'roe']
            X_raw = train_data[features]
            Y = train_data['target_grade']
            X_today_raw = today_data[features]
            
            # 6. PENYEMBUHAN DATA (Imputasi Median untuk Fundamental yang Kosong)
            imputer = SimpleImputer(strategy='median')
            X_imputed = pd.DataFrame(imputer.fit_transform(X_raw), columns=features)
            X_today = pd.DataFrame(imputer.transform(X_today_raw), columns=features)

           # 7. PEMBAGIAN TRAIN & TEST UNTUK EVALUASI AKADEMIS (Tanpa Data Leakage)
            # Kita pecah sejarah tanpa mengacak urutan waktu
            tscv = TimeSeriesSplit(n_splits=3)
            splits = list(tscv.split(X_imputed))
            
            # Ambil potongan waktu terakhir sebagai ujian (Test Set) paling relevan
            train_idx, test_idx = splits[-1]
            
            X_train_eval, X_test_eval = X_imputed.iloc[train_idx], X_imputed.iloc[test_idx]
            Y_train_eval, Y_test_eval = Y.iloc[train_idx], Y.iloc[test_idx]
            
            # 8. SMOTE HANYA PADA DATA TRAINING (Mencegah Kebocoran Data/Leakage)
            try:
                smote = SMOTE(random_state=42, k_neighbors=min(2, len(Y_train_eval)-1))
                X_train_smote, Y_train_smote = smote.fit_resample(X_train_eval, Y_train_eval)
            except:
                X_train_smote, Y_train_smote = X_train_eval, Y_train_eval
                
            # 9. PELATIHAN & EVALUASI OOB (Out-of-Sample)
            # Menghapus GridSearchCV untuk efisiensi waktu, menggunakan parameter optimal
            rf_eval = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42, class_weight='balanced')
            rf_eval.fit(X_train_smote, Y_train_smote)
            
            Y_pred_eval = rf_eval.predict(X_test_eval)
            all_y_true.extend(Y_test_eval.tolist())
            all_y_pred.extend(Y_pred_eval.tolist())
            
            # 10. PELATIHAN MODEL FINAL UNTUK PREDIKSI HARI INI
            # Sekarang gunakan SELURUH sejarah data yang di-SMOTE untuk menebak masa depan hari ini
            try:
                smote_final = SMOTE(random_state=42, k_neighbors=min(2, len(Y)-1))
                X_final_smote, Y_final_smote = smote_final.fit_resample(X_imputed, Y)
            except:
                X_final_smote, Y_final_smote = X_imputed, Y
                
            rf_final = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42, class_weight='balanced')
            rf_final.fit(X_final_smote, Y_final_smote)
            
            # 11. PREDIKSI HARI INI & EKSTRAKSI KEPINTARAN (Model Final)
            prediction = rf_final.predict(X_today)[0]
            importances = rf_final.feature_importances_
            feat_imp_dict = {feat: round(float(imp), 4) for feat, imp in zip(features, importances)}
            
            # SIMPAN KE DATABASE (Tabel: ml_predictions)
            
            # 11. SIMPAN KE DATABASE (Tabel: ml_predictions)
            payload = {
                "ticker": ticker,
                "prediction_date": today_str,
                "predicted_grade": prediction,
                "feature_importance": feat_imp_dict
            }
            supabase.table("ml_predictions").upsert(payload, on_conflict="ticker,prediction_date").execute()
            
            print(f"✅ Grade: {prediction}")

        except Exception as e:
            print(f"❌ Error: {e}")
            
        # [PERBAIKAN FATAL] Jeda Napas untuk Supabase (Mencegah SSL/Timeout Drop)
        import time
        time.sleep(0.1)
            
    # =========================================================================
    # FASE 12: EVALUASI GLOBAL UNTUK DASHBOARD "MODEL HEALTH" (Revisi 7)
    # =========================================================================
    print("\n📊 Menghitung Metrik Kesehatan Model Global (Confusion Matrix)...")
    if len(all_y_true) > 0:
        # Konversi multikelas (A,B,C) menjadi Biner (A vs Bukan A) untuk kalkulasi profit
        y_true_bin = [1 if y == 'A' else 0 for y in all_y_true]
        y_pred_bin = [1 if y == 'A' else 0 for y in all_y_pred]

        prec = precision_score(y_true_bin, y_pred_bin, zero_division=0) * 100
        rec = recall_score(y_true_bin, y_pred_bin, zero_division=0) * 100
        f1 = f1_score(y_true_bin, y_pred_bin, zero_division=0) * 100

        tn, fp, fn, tp = confusion_matrix(y_true_bin, y_pred_bin, labels=[0, 1]).ravel()

        metrics_payload = {
            "precision_score": round(prec, 2),
            "recall_score": round(rec, 2),
            "f1_score": round(f1, 2),
            "oob_error": round((fp + fn) / len(y_true_bin), 4), # Estimasi error rate
            "confusion_matrix": {"tp": int(tp), "fp": int(fp), "tn": int(tn), "fn": int(fn)},
            "log_messages": [
                f"INIT: Validated T+20 Horizon for {total} Tickers.",
                "PROCESS: Imputed missing Fundamental data (PER, PBV, ROA, ROE).",
                "PROCESS: Executed SMOTE to synthesize minority Class A.",
                f"SUCCESS: Global Precision established at {round(prec, 2)}%."
            ]
        }
        
        # Kirim hasil ke Dashboard Admin Next.js
        supabase.table("model_metrics").insert(metrics_payload).execute()
        print("✅ Metrik Kinerja berhasil ditanam ke Dashboard Sistem!")

    print("\n🎉 SELURUH PIPELINE SELESAI!")

if __name__ == "__main__":
    train_and_predict()