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
 
# 📋 Get client list
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
 
        # 📋 Step 1: Load data
        query = f"""
        SELECT SBM_DT, SUM(f.adjusted_cnt) AS adjusted_total
        FROM prd_optumrx_orxfdmprdsa.fdmenh.projections_enhanced_jul f
        LEFT JOIN fdmenh.holiday_flg h ON f.SBM_DT = h.date
        WHERE f.lob = '{client_id}'
        GROUP BY SBM_DT
        """
        df = spark.sql(query).toPandas()
        if df.empty:
            print(f"⚠ No data for {client_id}")
            continue
 
        df['ds'] = pd.to_datetime(df['SBM_DT'])
        df['y'] = df['adjusted_total']
        df = df[df['y'] > 0].sort_values('ds').reset_index(drop=True)
        
        print(f"📊 Data loaded: {len(df)} days from {df['ds'].min().date()} to {df['ds'].max().date()}")
        print(f"📈 Volume range: {df['y'].min():,} to {df['y'].max():,}")
 
        # 📋 Enhanced Holiday Features
        holiday_pdf = spark.table("fdmenh.holiday_flg").toPandas()
        holiday_pdf['date'] = pd.to_datetime(holiday_pdf['date'])
        
        # Create holiday proximity features
        for offset, col in zip([1, 2, 3], ['is_day1_after_major', 'is_day2_after_major', 'is_day3_after_major']):
            holiday_pdf[col] = 0
            target_dates = holiday_pdf[holiday_pdf['is_major_holiday'] == 1]['date'] + pd.to_timedelta(offset, unit='d')
            holiday_pdf.loc[holiday_pdf['date'].isin(target_dates), col] = 1
        
        for offset, col in zip([1, 2], ['is_day1_before_major', 'is_day2_before_major']):
            holiday_pdf[col] = 0
            target_dates = holiday_pdf[holiday_pdf['is_major_holiday'] == 1]['date'] - pd.to_timedelta(offset, unit='d')
            holiday_pdf.loc[holiday_pdf['date'].isin(target_dates), col] = 1
            
        holiday_pdf = holiday_pdf[['date', 'holiday_name', 'is_major_holiday', 'is_minor_holiday',
                                   'is_weekend', 'is_day1_after_major', 'is_day2_after_major', 'is_day3_after_major',
                                   'is_day1_before_major', 'is_day2_before_major']]
        df = df.merge(holiday_pdf, left_on='ds', right_on='date', how='left').drop(columns=['date'])
        
        # Fill missing holiday values
        holiday_cols = ['is_major_holiday', 'is_minor_holiday', 'is_weekend', 
                       'is_day1_after_major', 'is_day2_after_major', 'is_day3_after_major',
                       'is_day1_before_major', 'is_day2_before_major']
        for col in holiday_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0).astype(int)
 
        # 📋 CLEAN Time Features (No Target Leakage)
        df['dayofweek'] = df['ds'].dt.dayofweek
        df['dayofmonth'] = df['ds'].dt.day
        df['month'] = df['ds'].dt.month
        df['quarter'] = df['ds'].dt.quarter
        df['day_of_year'] = df['ds'].dt.dayofyear
        df['week_of_year'] = df['ds'].dt.isocalendar().week
        
        # Cyclical features for seasonality
        df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
        df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
        df['dow_sin'] = np.sin(2 * np.pi * df['dayofweek'] / 7)
        df['dow_cos'] = np.cos(2 * np.pi * df['dayofweek'] / 7)
        
        # Specific day/period flags
        df['is_monday'] = (df['dayofweek'] == 0).astype(int)
        df['is_friday'] = (df['dayofweek'] == 4).astype(int)
        df['is_saturday'] = (df['dayofweek'] == 5).astype(int)
        df['is_sunday'] = (df['dayofweek'] == 6).astype(int)
        df['is_month_end'] = (df['dayofmonth'] >= 28).astype(int)
        df['is_month_start'] = (df['dayofmonth'] <= 3).astype(int)
        
        # 📋 LAG Features (Historical values - NO target leakage)
        for lag in [1, 2, 3, 7, 14, 21, 30, 60, 90, 365]:
            df[f'lag_{lag}'] = df['y'].shift(lag)
        
        # 📋 ROLLING Features (Historical trends - NO current target)
        for window in [7, 14, 21, 30, 60, 90]:
            # Use only historical data (shift by 1 to avoid leakage)
            df[f'rolling_mean_{window}'] = df['y'].shift(1).rolling(window, min_periods=max(1, window//3)).mean()
            df[f'rolling_std_{window}'] = df['y'].shift(1).rolling(window, min_periods=max(1, window//3)).std()
            df[f'rolling_min_{window}'] = df['y'].shift(1).rolling(window, min_periods=max(1, window//3)).min()
            df[f'rolling_max_{window}'] = df['y'].shift(1).rolling(window, min_periods=max(1, window//3)).max()
        
        # 📋 TREND Features (Historical trends only)
        for window in [7, 14, 30]:
            df[f'trend_slope_{window}'] = df['y'].shift(1).rolling(window).apply(
                lambda x: np.polyfit(range(len(x)), x, 1)[0] if len(x) >= 3 else 0, raw=True
            )
        
        # 📋 CLEAN Ratio Features (No target leakage)
        df['lag7_to_rolling30_ratio'] = df['lag_7'] / df['rolling_mean_30']
        df['lag1_to_lag7_ratio'] = df['lag_1'] / df['lag_7']
        df['rolling7_to_rolling30_ratio'] = df['rolling_mean_7'] / df['rolling_mean_30']
        
        # 📋 Volatility Features (Historical only)
        df['volatility_7d'] = df['rolling_std_7'] / df['rolling_mean_7']
        df['volatility_30d'] = df['rolling_std_30'] / df['rolling_mean_30']
        
        # Drop rows with NaN (from lag features)
        df = df.dropna().copy()
        
        # ❌ REMOVED: df['y_log'] = np.log1p(df['y'])  # This was the problem!
        # ✅ USE RAW VALUES: Work directly with original prescription volumes
        
        df = df.drop(columns=['SBM_DT'])
        
        print(f"✅ Features created, clean dataset: {len(df)} rows")
 
        # 📋 Prediction period
        future_start = df['ds'].max() + timedelta(days=1)
        future_dates = pd.date_range(start=future_start, periods=31)
 
        # 📋 Select Features (NO target-leaking features)
        exclude_cols = ['ds', 'y', 'holiday_name', 'date']
        features = [col for col in df.columns 
                   if col not in exclude_cols 
                   and df[col].dtype in ['int64', 'float64', 'int32', 'float32']
                   and not col.startswith('ratio_to_')  # Remove target-leaking ratios
                   and not col.startswith('current_')]  # Remove any current_ features
        
        # Define categorical features for LightGBM
        categorical_features = ['dayofweek', 'month', 'quarter', 'dayofmonth', 'week_of_year']
        categorical_features = [f for f in categorical_features if f in features]
        
        print(f"🔧 Using {len(features)} clean features ({len(categorical_features)} categorical)")
 
        # 📋 Step 2: Validation split
        val_days = 90  # Back to 3 months for faster debugging
        split_date = df['ds'].max() - pd.Timedelta(days=val_days)
 
        print(f"📆 Data ends: {df['ds'].max().date()}")
        print(f"📆 Validation starts: {(split_date + timedelta(days=1)).date()}")
        print(f"📆 Training ends: {split_date.date()}")
 
        train_df = df[df['ds'] <= split_date].copy()
        val_df = df[df['ds'] > split_date].copy()
 
        print(f"✅ Training rows: {len(train_df)}, Validation rows: {len(val_df)}")
        
        # Check data quality
        train_weekend_avg = train_df[train_df['is_weekend'] == 1]['y'].mean()
        train_weekday_avg = train_df[train_df['is_weekend'] == 0]['y'].mean()
        weekend_ratio = train_weekend_avg / train_weekday_avg
        
        print(f"📊 Training data patterns:")
        print(f"   Weekday avg: {train_weekday_avg:,.0f}")
        print(f"   Weekend avg: {train_weekend_avg:,.0f}")
        print(f"   Weekend ratio: {weekend_ratio:.3f} ({weekend_ratio*100:.1f}%)")
 
        X_train = train_df[features]
        y_train = train_df['y']  # ✅ RAW VALUES, not log transformed!
        X_val = val_df[features]
        y_val = val_df['y']      # ✅ RAW VALUES, not log transformed!
 
        X_train_full = df[features]
        y_train_full = df['y']   # ✅ RAW VALUES, not log transformed!
 
        # 📋 Step 3: OPTUNA Optimization (Back to find best params)
        def objective(trial):
            params = {
                'objective': 'regression',
                'metric': 'rmse',
                'boosting_type': 'gbdt',
                'num_leaves': trial.suggest_int('num_leaves', 31, 120),
                'learning_rate': trial.suggest_float('learning_rate', 0.05, 0.3),
                'feature_fraction': trial.suggest_float('feature_fraction', 0.7, 1.0),
                'bagging_fraction': trial.suggest_float('bagging_fraction', 0.7, 1.0),
                'bagging_freq': trial.suggest_int('bagging_freq', 1, 7),
                'min_child_samples': trial.suggest_int('min_child_samples', 5, 50),
                'lambda_l1': trial.suggest_float('lambda_l1', 0, 2.0),
                'lambda_l2': trial.suggest_float('lambda_l2', 0, 2.0),
                'verbosity': -1,
                'random_state': 42
            }
 
            model = LGBMRegressor(n_estimators=300, **params)
            model.fit(
                X_train, y_train,
                eval_set=[(X_val, y_val)],
                categorical_feature=categorical_features,
                callbacks=[lgb.early_stopping(30), lgb.log_evaluation(0)]
            )
            preds = model.predict(X_val)
            
            # Use MAPE for better business interpretation
            mape = mean_absolute_percentage_error(y_val, preds)
            return mape
 
        print("🔍 Optimizing LightGBM hyperparameters...")
        study = optuna.create_study(direction='minimize')
        study.optimize(objective, n_trials=100)
        
        print(f"🏆 Best MAPE: {study.best_value:.4f} ({study.best_value*100:.2f}%)")
        print(f"🎯 Best parameters: {study.best_params}")
        
        # Validate the weekend pattern learning
        best_model = LGBMRegressor(n_estimators=300, **study.best_params)
        best_model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            categorical_feature=categorical_features,
            callbacks=[lgb.early_stopping(30), lgb.log_evaluation(0)]
        )
        
        # Test weekend vs weekday predictions
        val_preds = best_model.predict(X_val)
        val_weekend_pred = val_preds[val_df['is_weekend'] == 1].mean()
        val_weekday_pred = val_preds[val_df['is_weekend'] == 0].mean()
        pred_weekend_ratio = val_weekend_pred / val_weekday_pred
        
        print(f"📊 Model learned patterns:")
        print(f"   Predicted weekday avg: {val_weekday_pred:,.0f}")
        print(f"   Predicted weekend avg: {val_weekend_pred:,.0f}")
        print(f"   Predicted weekend ratio: {pred_weekend_ratio:.3f} ({pred_weekend_ratio*100:.1f}%)")
        
        # Log comprehensive results
        log_entry = f"""
        adjusted_total, client_id: {client_id}, 
        model: lightgbm_fixed,
        trial_number: {study.best_trial.number}, 
        best_mape: {study.best_value:.6f}, 
        best_params: {study.best_params}, 
        features_used: {len(features)},
        actual_weekend_ratio: {weekend_ratio:.3f},
        predicted_weekend_ratio: {pred_weekend_ratio:.3f},
        pattern_learned: {'YES' if abs(pred_weekend_ratio - weekend_ratio) < 0.2 else 'NO'},
        timestamp: {datetime.now().isoformat()}
        """
        log_entry = log_entry.replace("'", " ")
        spark.sql(f"INSERT INTO log_ml_data (col1) VALUES ('{log_entry}')")
 
        # 📋 Step 4: Train final model on all data
        final_params = {
            'objective': 'regression',
            'metric': 'rmse',
            'boosting_type': 'gbdt',
            'verbosity': -1,
            'random_state': 42,
            **study.best_params
        }
        
        model = LGBMRegressor(n_estimators=500, **final_params)
        model.fit(
            X_train_full, y_train_full,
            categorical_feature=categorical_features
        )
 
        # 📋 Step 5: Generate predictions with CLEAN feature engineering
        latest_df = df.copy()
        for future_day in future_dates:
            row = {}
            row['ds'] = future_day
            
            # Basic time features
            row['dayofweek'] = future_day.dayofweek
            row['dayofmonth'] = future_day.day
            row['month'] = future_day.month
            row['quarter'] = (future_day.month - 1) // 3 + 1
            row['day_of_year'] = future_day.timetuple().tm_yday
            row['week_of_year'] = future_day.isocalendar()[1]
            
            # Cyclical features
            row['month_sin'] = np.sin(2 * np.pi * row['month'] / 12)
            row['month_cos'] = np.cos(2 * np.pi * row['month'] / 12)
            row['dow_sin'] = np.sin(2 * np.pi * row['dayofweek'] / 7)
            row['dow_cos'] = np.cos(2 * np.pi * row['dayofweek'] / 7)
            
            # Specific day flags
            row['is_monday'] = int(future_day.dayofweek == 0)
            row['is_friday'] = int(future_day.dayofweek == 4)
            row['is_saturday'] = int(future_day.dayofweek == 5)
            row['is_sunday'] = int(future_day.dayofweek == 6)
            row['is_month_end'] = int(future_day.day >= 28)
            row['is_month_start'] = int(future_day.day <= 3)
 
            # Holiday features
            h = holiday_pdf[holiday_pdf['date'] == future_day]
            row['is_weekend'] = int(future_day.weekday() >= 5)
            for holiday_col in holiday_cols:
                if holiday_col in features:
                    row[holiday_col] = int(h[holiday_col].iloc[0]) if not h.empty and holiday_col in h.columns else 0
 
            # Lag features using recent actual data
            recent_data = latest_df['y'].values
            for lag in [1, 2, 3, 7, 14, 21, 30, 60, 90, 365]:
                if f'lag_{lag}' in features:
                    row[f'lag_{lag}'] = recent_data[-lag] if len(recent_data) >= lag else recent_data[-1]
            
            # Rolling features (historical only - no leakage)
            for window in [7, 14, 21, 30, 60, 90]:
                if len(recent_data) >= window + 1:
                    # Use data excluding current (shift by 1 equivalent)
                    historical_data = recent_data[-(window+1):-1]
                    if f'rolling_mean_{window}' in features:
                        row[f'rolling_mean_{window}'] = np.mean(historical_data)
                    if f'rolling_std_{window}' in features:
                        row[f'rolling_std_{window}'] = np.std(historical_data)
                    if f'rolling_min_{window}' in features:
                        row[f'rolling_min_{window}'] = np.min(historical_data)
                    if f'rolling_max_{window}' in features:
                        row[f'rolling_max_{window}'] = np.max(historical_data)
                else:
                    if f'rolling_mean_{window}' in features:
                        row[f'rolling_mean_{window}'] = np.mean(recent_data[:-1]) if len(recent_data) > 1 else recent_data[-1]
                    if f'rolling_std_{window}' in features:
                        row[f'rolling_std_{window}'] = np.std(recent_data[:-1]) if len(recent_data) > 2 else 0
                    if f'rolling_min_{window}' in features:
                        row[f'rolling_min_{window}'] = np.min(recent_data[:-1]) if len(recent_data) > 1 else recent_data[-1]
                    if f'rolling_max_{window}' in features:
                        row[f'rolling_max_{window}'] = np.max(recent_data[:-1]) if len(recent_data) > 1 else recent_data[-1]
            
            # Trend features
            for window in [7, 14, 30]:
                if f'trend_slope_{window}' in features:
                    if len(recent_data) >= window + 1:
                        historical_data = recent_data[-(window+1):-1]
                        row[f'trend_slope_{window}'] = np.polyfit(range(len(historical_data)), historical_data, 1)[0] if len(historical_data) >= 3 else 0
                    else:
                        row[f'trend_slope_{window}'] = 0
            
            # Clean ratio features (no target leakage)
            if 'lag7_to_rolling30_ratio' in features:
                row['lag7_to_rolling30_ratio'] = row.get(f'lag_7', 0) / max(row.get('rolling_mean_30', 1), 1)
            if 'lag1_to_lag7_ratio' in features:
                row['lag1_to_lag7_ratio'] = row.get('lag_1', 0) / max(row.get('lag_7', 1), 1)
            if 'rolling7_to_rolling30_ratio' in features:
                row['rolling7_to_rolling30_ratio'] = row.get('rolling_mean_7', 0) / max(row.get('rolling_mean_30', 1), 1)
            
            # Volatility features
            if 'volatility_7d' in features:
                row['volatility_7d'] = row.get('rolling_std_7', 0) / max(row.get('rolling_mean_7', 1), 1)
            if 'volatility_30d' in features:
                row['volatility_30d'] = row.get('rolling_std_30', 0) / max(row.get('rolling_mean_30', 1), 1)
            
            # Fill any missing features with 0
            for feature in features:
                if feature not in row:
                    row[feature] = 0
 
            # Make prediction (NO log transform - direct prediction!)
            row_df = pd.DataFrame([row])
            pred = model.predict(row_df[features])[0]  # ✅ Direct prediction, no expm1!
            pred = max(0, pred)  # Ensure non-negative
            row['y'] = pred
 
            latest_df = pd.concat([latest_df, pd.DataFrame([row])], ignore_index=True)
            forecast_results.append((client_id, future_day, float(pred), 'lightgbm_fixed_v2'))
            
            # Print prediction with day context
            day_name = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'][future_day.dayofweek]
            print(f"🔮 {future_day.date()} ({day_name}): {pred:,.0f}")
 
    except Exception as e:
        print(f"❌ Error for {client_id}: {str(e)}")
        error_log.append((client_id, str(e)))
        continue
 
# 📋 Save results
forecast_df = pd.DataFrame(forecast_results, columns=['client_id', 'forecast_date', 'yhat', 'model_type'])
error_df = pd.DataFrame(error_log, columns=['client_id', 'error_message'])
 
print(f"\n✅ Fixed LightGBM completed!")
print(f"🎯 Key fixes: No log transform, clean features, proper weekend learning")

# COMMAND ----------

spark_df = spark.createDataFrame(forecast_df)
spark_df.createOrReplaceTempView("forecast_df_temp")
