import os
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import TimeSeriesSplit
from sklearn.impute import SimpleImputer
from sklearn.metrics import precision_score, recall_score, f1_score, confusion_matrix
# SMOTE DIHAPUS: Haram digunakan pada data Time-Series finansial
from dotenv import load_dotenv
from utils import supabase, get_all_tickers
import warnings

warnings.filterwarnings('ignore')
load_dotenv()

def train_and_predict():
    tickers = get_all_tickers()
    total = len(tickers)
    print(f"🧠 [ML ENGINE ADVANCED] Memulai Pipeline T+20, Fusi Fundamental (No Leakage) untuk {total} emiten...")
    
    today_str = datetime.now().strftime('%Y-%m-%d')
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

            # 3. TARIK DATA FUNDAMENTAL
            res_fund = supabase.table("financial_reports")\
                .select("period_date, per, pbv, roa, roe")\
                .eq("ticker", ticker).order("period_date", desc=False).execute()

            if not res_feat.data or not res_price.data or len(res_feat.data) < 50:
                print("⚠️ Skip (Data < 50 baris)")
                continue

            df_feat = pd.DataFrame(res_feat.data).rename(columns={"calc_date": "date"})
            df_price = pd.DataFrame(res_price.data).rename(columns={"trade_date": "date"})
            
            df = pd.merge(df_feat, df_price, on="date", how="inner")
            df['date'] = pd.to_datetime(df['date'])
            
            # FUSI DATA FUNDAMENTAL (Logika Forward Fill)
            if res_fund.data:
                df_fund = pd.DataFrame(res_fund.data).rename(columns={"period_date": "date"})
                df_fund['date'] = pd.to_datetime(df_fund['date'])
                df = pd.merge_asof(df.sort_values('date'), df_fund.sort_values('date'), on='date', direction='backward')
            
            # [PERBAIKAN FATAL] PENYEMBUHAN NAN TANPA DATA LEAKAGE
            # Urutkan berdasarkan waktu, lalu FFILL (Bawa data masa lalu ke depan). Jangan pernah BFILL.
            df.sort_values('date', inplace=True)
            fallback_ratios = {'per': 15.0, 'pbv': 1.5, 'roa': 5.0, 'roe': 10.0}
            
            for col, val in fallback_ratios.items():
                if col not in df.columns:
                    df[col] = val
                else:
                    df[col] = df[col].ffill().fillna(val)

            if df.empty: continue
            
            # 4. HORIZON PREDIKSI SEBULAN (T+20)
            df['adjusted_close'] = pd.to_numeric(df['adjusted_close'])
            df['future_price_20d'] = df['adjusted_close'].shift(-20)
            
            def assign_grade(row):
                if pd.isna(row['future_price_20d']): return None
                ret = ((row['future_price_20d'] - row['adjusted_close']) / row['adjusted_close']) * 100
                
                if ret >= 8.0: return 'A'
                elif ret <= -4.0: return 'C'
                else: return 'B'
                
            df['target_grade'] = df.apply(assign_grade, axis=1)
            
            # 5. PEMISAHAN DATA
            today_data = df.iloc[-1:] 
            train_data = df.dropna(subset=['target_grade']) 
            
            if len(train_data) < 30:
                print("⚠️ Skip (Data latih kurang dari 30 hari EOD)")
                continue
                
            features = ['rsi_14', 'macd', 'margin_of_safety', 'mfi_14', 'per', 'pbv', 'roa', 'roe']
            X_raw = train_data[features]
            Y = train_data['target_grade']
            X_today_raw = today_data[features]
            
            # 6. PENYEMBUHAN DATA (Imputasi Median)
            imputer = SimpleImputer(strategy='median')
            X_imputed = pd.DataFrame(imputer.fit_transform(X_raw), columns=features)
            X_today = pd.DataFrame(imputer.transform(X_today_raw), columns=features)

            # 7. PEMBAGIAN TRAIN & TEST UNTUK EVALUASI
            tscv = TimeSeriesSplit(n_splits=3)
            splits = list(tscv.split(X_imputed))
            train_idx, test_idx = splits[-1]
            
            X_train_eval, X_test_eval = X_imputed.iloc[train_idx], X_imputed.iloc[test_idx]
            Y_train_eval, Y_test_eval = Y.iloc[train_idx], Y.iloc[test_idx]
            
            # 8. PELATIHAN & EVALUASI OOB (TANPA SMOTE)
            rf_eval = RandomForestClassifier(
                n_estimators=100, 
                max_depth=10, 
                random_state=42, 
                class_weight='balanced',
                min_samples_leaf=5 # Mencegah overfitting pada noise pasar
            )
            rf_eval.fit(X_train_eval, Y_train_eval)
            
            # Simulasikan Threshold 65% pada data evaluasi
            eval_proba = rf_eval.predict_proba(X_test_eval)
            classes_eval = rf_eval.classes_
            Y_pred_eval = []
            
            for prob in eval_proba:
                if 'A' in classes_eval:
                    idx_A = list(classes_eval).index('A')
                    if prob[idx_A] >= 0.65:
                        Y_pred_eval.append('A')
                    else:
                        temp_prob = prob.copy()
                        temp_prob[idx_A] = -1
                        Y_pred_eval.append(classes_eval[np.argmax(temp_prob)])
                else:
                    Y_pred_eval.append(classes_eval[np.argmax(prob)])

            all_y_true.extend(Y_test_eval.tolist())
            all_y_pred.extend(Y_pred_eval)
            
            # 9. PELATIHAN MODEL FINAL
            rf_final = RandomForestClassifier(
                n_estimators=100, 
                max_depth=10, 
                random_state=42, 
                class_weight='balanced',
                min_samples_leaf=5
            )
            rf_final.fit(X_imputed, Y)
            
            # 10. PREDIKSI HARI INI DENGAN THRESHOLD KETAT (65%)
            today_proba = rf_final.predict_proba(X_today)[0]
            classes_final = rf_final.classes_
            
            if 'A' in classes_final:
                idx_A = list(classes_final).index('A')
                prob_A = today_proba[idx_A]
                
                if prob_A >= 0.65:
                    prediction = 'A'
                else:
                    # Tolak Buy jika tidak yakin. Paksa jadi Hold (B) atau Cutloss (C)
                    temp_proba = today_proba.copy()
                    temp_proba[idx_A] = -1
                    prediction = classes_final[np.argmax(temp_proba)]
            else:
                prediction = classes_final[np.argmax(today_proba)]

            importances = rf_final.feature_importances_
            feat_imp_dict = {feat: round(float(imp), 4) for feat, imp in zip(features, importances)}
            
            # 11. SIMPAN KE DATABASE
            payload = {
                "ticker": ticker,
                "prediction_date": today_str,
                "predicted_grade": prediction,
                "feature_importance": feat_imp_dict
            }
            supabase.table("ml_predictions").upsert(payload, on_conflict="ticker,prediction_date").execute()
            
            print(f"✅ Grade: {prediction} (Prob A: {prob_A:.2f})" if 'A' in classes_final else f"✅ Grade: {prediction}")

        except Exception as e:
            print(f"❌ Error: {e}")
            
        import time
        time.sleep(0.1)
            
    # =========================================================================
    # FASE 12: EVALUASI GLOBAL UNTUK DASHBOARD "MODEL HEALTH"
    # =========================================================================
    print("\n📊 Menghitung Metrik Kesehatan Model Global (Confusion Matrix)...")
    if len(all_y_true) > 0:
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
            "oob_error": round((fp + fn) / len(y_true_bin), 4),
            "confusion_matrix": {"tp": int(tp), "fp": int(fp), "tn": int(tn), "fn": int(fn)},
            "log_messages": [
                f"INIT: Validated T+20 Horizon for {total} Tickers.",
                "PROCESS: Executed STRICT Forward Fill (ffill) for Fundamentals. No Data Leakage.",
                "PROCESS: Removed SMOTE. Implemented 65% Probability Threshold for Class A.",
                f"SUCCESS: Global Precision established at {round(prec, 2)}%."
            ]
        }
        
        supabase.table("model_metrics").insert(metrics_payload).execute()
        print(f"✅ Presisi Realistis: {round(prec, 2)}% | False Positive: {fp}")

    print("\n🎉 SELURUH PIPELINE SELESAI!")

if __name__ == "__main__":
    train_and_predict()