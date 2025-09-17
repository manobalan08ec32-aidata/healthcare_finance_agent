pip install lightgbm optuna

# COMMAND ----------

import pandas as pd
import numpy as np
from lightgbm import LGBMRegressor
import lightgbm as lgb
from sklearn.metrics import mean_squared_error, mean_absolute_percentage_error
from datetime import timedelta, datetime
import optuna
from pyspark.sql import SparkSession
 
spark = SparkSession.builder.getOrCreate()

# Get client list
client_df = spark.sql("""
SELECT DISTINCT lob as client_id
FROM prd_optumrx_orxfdmprdsa.fdmenh.projections_enhanced_jul 
""")
client_list = [row.client_id for row in client_df.collect()]

forecast_results = []
error_log = []

for client_id in client_list:
    try:
        print(f"\n=== 🚀 Processing {client_id} ===")

        # 📋 Load data with holiday join
        query = f"""
        SELECT SBM_DT, SUM(f.adjusted_cnt) AS adjusted_total
        FROM prd_optumrx_orxfdmprdsa.fdmenh.projections_enhanced_jul f
        LEFT JOIN fdmenh.holiday_flg h ON f.SBM_DT = h.date
        WHERE f.lob = '{client_id}'
        GROUP BY SBM_DT
        """
        df = spark.sql(query).toPandas()
        if df.empty:
            continue

        df['ds'] = pd.to_datetime(df['SBM_DT'])
        df['y'] = df['adjusted_total']
        df = df[df['y'] > 0].sort_values('ds').reset_index(drop=True)
        
        print(f"📊 Data: {len(df)} days, {df['y'].min():,} to {df['y'].max():,}")

        # 📋 FULL Holiday Features (like XGBoost)
        holiday_pdf = spark.table("fdmenh.holiday_flg").toPandas()
        holiday_pdf['date'] = pd.to_datetime(holiday_pdf['date'])
        
        # Add holiday proximity features
        for offset, col in zip([1, 2, 3], ['is_day1_after_major', 'is_day2_after_major', 'is_day3_after_major']):
            holiday_pdf[col] = 0
            target_dates = holiday_pdf[holiday_pdf['is_major_holiday'] == 1]['date'] + pd.to_timedelta(offset, unit='d')
            holiday_pdf.loc[holiday_pdf['date'].isin(target_dates), col] = 1
        
        holiday_pdf = holiday_pdf[['date', 'holiday_name', 'is_major_holiday', 'is_minor_holiday',
                                   'is_weekend', 'is_day1_after_major', 'is_day2_after_major', 'is_day3_after_major']]
        
        df = df.merge(holiday_pdf, left_on='ds', right_on='date', how='left').drop(columns=['date'])
        
        # Fill missing holiday values
        holiday_cols = ['is_major_holiday', 'is_minor_holiday', 'is_weekend',
                       'is_day1_after_major', 'is_day2_after_major', 'is_day3_after_major']
        for col in holiday_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0).astype(int)

        print(f"🎄 Holiday data merged successfully")
        
        # Check Labor Day patterns in training data
        labor_day_data = df[df['holiday_name'].str.contains('Labor', na=False)]
        if len(labor_day_data) > 0:
            print(f"📊 Historical Labor Day volumes: {labor_day_data['y'].tolist()}")

        # 📋 Time Features
        df['dayofweek'] = df['ds'].dt.dayofweek
        df['month'] = df['ds'].dt.month
        df['dayofmonth'] = df['ds'].dt.day
        df['quarter'] = df['ds'].dt.quarter
        
        # 📋 Simple lag features (to avoid contamination)
        df['lag_7'] = df['y'].shift(7)
        df['lag_14'] = df['y'].shift(14)
        df['rolling_mean_7'] = df['y'].rolling(7).mean()
        df['rolling_std_7'] = df['y'].rolling(7).std()
        
        # Remove NaN rows
        df = df.dropna().copy()
        
        # 📋 LOG TRANSFORMATION (like XGBoost)
        df['y_log'] = np.log1p(df['y'])
        df = df.drop(columns=['SBM_DT'])
        
        print(f"✅ Features created: {len(df)} clean rows")

        # 📋 Features (same as XGBoost for consistency)
        features = [
            'dayofweek', 'dayofmonth', 'month', 'quarter',
            'is_weekend', 'is_major_holiday', 'is_minor_holiday',
            'is_day1_after_major', 'is_day2_after_major', 'is_day3_after_major',
            'lag_7', 'lag_14', 'rolling_mean_7', 'rolling_std_7'
        ]
        
        categorical_features = ['dayofweek', 'month', 'quarter']

        # 📋 Train/validation split
        val_days = 90
        split_date = df['ds'].max() - pd.Timedelta(days=val_days)
        
        train_df = df[df['ds'] <= split_date]
        val_df = df[df['ds'] > split_date]

        print(f"📆 Training: {train_df['ds'].min().date()} to {train_df['ds'].max().date()}")
        print(f"📆 Validation: {val_df['ds'].min().date()} to {val_df['ds'].max().date()}")

        X_train, y_train = train_df[features], train_df['y_log']
        X_val, y_val = val_df[features], val_df['y_log']
        X_train_full, y_train_full = df[features], df['y_log']
        
        # Check holiday patterns in training data
        train_major_holidays = train_df[train_df['is_major_holiday'] == 1]
        if len(train_major_holidays) > 0:
            holiday_avg = np.expm1(train_major_holidays['y_log'].mean())
            normal_avg = np.expm1(train_df[train_df['is_major_holiday'] == 0]['y_log'].mean())
            holiday_ratio = holiday_avg / normal_avg
            print(f"📊 Training holiday pattern: {holiday_ratio:.3f} ({holiday_ratio*100:.1f}% of normal)")

        # 📋 LightGBM Hyperparameter Optimization
        def objective(trial):
            params = {
                'objective': 'regression',
                'metric': 'rmse',
                'boosting_type': 'gbdt',
                'num_leaves': trial.suggest_int('num_leaves', 31, 100),
                'learning_rate': trial.suggest_float('learning_rate', 0.05, 0.2),
                'feature_fraction': trial.suggest_float('feature_fraction', 0.7, 1.0),
                'bagging_fraction': trial.suggest_float('bagging_fraction', 0.7, 1.0),
                'bagging_freq': trial.suggest_int('bagging_freq', 1, 7),
                'min_child_samples': trial.suggest_int('min_child_samples', 5, 30),
                'lambda_l1': trial.suggest_float('lambda_l1', 0, 1.0),
                'lambda_l2': trial.suggest_float('lambda_l2', 0, 1.0),
                'verbosity': -1,
                'random_state': 42
            }
            
            model = LGBMRegressor(n_estimators=300, **params)
            model.fit(
                X_train, y_train,
                eval_set=[(X_val, y_val)],
                categorical_feature=categorical_features,
                callbacks=[lgb.early_stopping(20), lgb.log_evaluation(0)]
            )
            
            preds = model.predict(X_val)
            rmse = mean_squared_error(y_val, preds, squared=False)
            return rmse

        print("🔍 Optimizing LightGBM hyperparameters...")
        study = optuna.create_study(direction='minimize')
        study.optimize(objective, n_trials=50)
        
        print(f"🏆 Best RMSE: {study.best_value:.6f}")

        # 📋 Train final model with best parameters
        final_params = {
            'objective': 'regression',
            'metric': 'rmse',
            'boosting_type': 'gbdt',
            'verbosity': -1,
            'random_state': 42,
            **study.best_params
        }
        
        model = LGBMRegressor(n_estimators=400, **final_params)
        model.fit(X_train_full, y_train_full, categorical_feature=categorical_features)

        # Log results
        log_entry = f"""
        adjusted_total, client_id: {client_id}, 
        model: lightgbm_with_holidays,
        trial_number: {study.best_trial.number}, 
        best_rmse: {study.best_value:.6f}, 
        best_params: {study.best_params}, 
        timestamp: {datetime.now().isoformat()}
        """
        log_entry = log_entry.replace("'", " ")
        spark.sql(f"INSERT INTO log_ml_data (col1) VALUES ('{log_entry}')")

        # 📋 FIXED PREDICTION with PROPER HOLIDAY DETECTION
        future_start = df['ds'].max() + timedelta(days=1)
        future_dates = pd.date_range(start=future_start, periods=31)
        
        # Create a copy for predictions (same as XGBoost approach)
        latest_df = df.copy()
        
        for future_day in future_dates:
            row = {}
            row['ds'] = future_day
            
            # Basic time features
            row['dayofweek'] = future_day.dayofweek
            row['dayofmonth'] = future_day.day
            row['month'] = future_day.month
            row['quarter'] = (future_day.month - 1) // 3 + 1
            
            # 🎄 CRITICAL: PROPER HOLIDAY DETECTION (like XGBoost)
            h = holiday_pdf[holiday_pdf['date'] == future_day]
            row['is_weekend'] = int(future_day.weekday() >= 5)
            row['is_major_holiday'] = int(h['is_major_holiday'].iloc[0]) if not h.empty else 0
            row['is_minor_holiday'] = int(h['is_minor_holiday'].iloc[0]) if not h.empty else 0
            row['is_day1_after_major'] = int(h['is_day1_after_major'].iloc[0]) if not h.empty else 0
            row['is_day2_after_major'] = int(h['is_day2_after_major'].iloc[0]) if not h.empty else 0
            row['is_day3_after_major'] = int(h['is_day3_after_major'].iloc[0]) if not h.empty else 0
            
            # Show what holiday was detected
            if row['is_major_holiday'] == 1 and not h.empty:
                holiday_name = h['holiday_name'].iloc[0]
                print(f"🎄 HOLIDAY DETECTED: {future_day.date()} is {holiday_name}")
            
            # Lag features (same as XGBoost approach)
            row['lag_7'] = latest_df['y'].iloc[-7] if len(latest_df) >= 7 else latest_df['y'].iloc[-1]
            row['lag_14'] = latest_df['y'].iloc[-14] if len(latest_df) >= 14 else latest_df['y'].iloc[-1]
            row['rolling_mean_7'] = latest_df['y'].iloc[-7:].mean()
            row['rolling_std_7'] = latest_df['y'].iloc[-7:].std()

            # Make prediction (with log transformation like XGBoost)
            row_df = pd.DataFrame([row])
            pred_log = model.predict(row_df[features])[0]
            pred = np.expm1(pred_log)  # Convert back from log scale
            pred = max(0, pred)  # Ensure non-negative
            row['y'] = pred
            
            # Add to latest_df for next iteration
            latest_df = pd.concat([latest_df, pd.DataFrame([row])], ignore_index=True)
            
            # Show prediction with context
            day_name = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'][future_day.dayofweek]
            holiday_flag = " 🎄 HOLIDAY" if row['is_major_holiday'] == 1 else ""
            print(f"🔮 {future_day.date()} ({day_name}): {pred:,.0f}{holiday_flag}")
            
            forecast_results.append((client_id, future_day, float(pred), 'lightgbm_holidays'))

    except Exception as e:
        print(f"❌ Error for {client_id}: {str(e)}")
        error_log.append((client_id, str(e)))

# 📋 Save results
forecast_df = pd.DataFrame(forecast_results, columns=['client_id', 'forecast_date', 'yhat', 'model_type'])
error_df = pd.DataFrame(error_log, columns=['client_id', 'error_message'])

print(f"\n🎉 LightGBM with Holiday Detection completed!")
print(f"🎯 Labor Day 2025 should now be properly predicted as a major holiday!")

# COMMAND ----------

spark_df = spark.createDataFrame(forecast_df)
spark_df.createOrReplaceTempView("forecast_df_temp")
