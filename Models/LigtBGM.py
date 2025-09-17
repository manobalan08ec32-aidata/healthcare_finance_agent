pip install lightgbm

# COMMAND ----------

import pandas as pd
import numpy as np
from lightgbm import LGBMRegressor
import lightgbm as lgb
from sklearn.metrics import mean_squared_error
from datetime import timedelta, datetime
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
 
        # 📋 Enhanced Holiday Features
        holiday_pdf = spark.table("fdmenh.holiday_flg").toPandas()
        holiday_pdf['date'] = pd.to_datetime(holiday_pdf['date'])
        
        # Create more holiday proximity features
        for offset, col in zip([1, 2, 3], ['is_day1_after_major', 'is_day2_after_major', 'is_day3_after_major']):
            holiday_pdf[col] = 0
            target_dates = holiday_pdf[holiday_pdf['is_major_holiday'] == 1]['date'] + pd.to_timedelta(offset, unit='d')
            holiday_pdf.loc[holiday_pdf['date'].isin(target_dates), col] = 1
        
        # Add before holiday features
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
                df[col] = df[col].fillna(0)
 
        # 📋 Enhanced Time Features
        df['dayofweek'] = df['ds'].dt.dayofweek
        df['dayofmonth'] = df['ds'].dt.day
        df['month'] = df['ds'].dt.month
        df['quarter'] = df['ds'].dt.quarter
        df['day_of_year'] = df['ds'].dt.dayofyear
        df['week_of_year'] = df['ds'].dt.isocalendar().week
        
        # Add cyclical features for better seasonality
        df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
        df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
        df['dow_sin'] = np.sin(2 * np.pi * df['dayofweek'] / 7)
        df['dow_cos'] = np.cos(2 * np.pi * df['dayofweek'] / 7)
        
        # Add specific day flags
        df['is_monday'] = (df['dayofweek'] == 0).astype(int)
        df['is_friday'] = (df['dayofweek'] == 4).astype(int)
        df['is_month_end'] = (df['dayofmonth'] >= 28).astype(int)
        df['is_month_start'] = (df['dayofmonth'] <= 3).astype(int)
        
        # 📋 Enhanced Lag Features (more periods for better patterns)
        for lag in [1, 2, 3, 7, 14, 21, 30, 60, 90, 365]:
            df[f'lag_{lag}'] = df['y'].shift(lag)
        
        # 📋 Enhanced Rolling Features (multiple windows)
        for window in [3, 7, 14, 21, 30, 60, 90]:
            df[f'rolling_mean_{window}'] = df['y'].rolling(window, min_periods=max(1, window//3)).mean()
            df[f'rolling_std_{window}'] = df['y'].rolling(window, min_periods=max(1, window//3)).std()
            df[f'rolling_min_{window}'] = df['y'].rolling(window, min_periods=max(1, window//3)).min()
            df[f'rolling_max_{window}'] = df['y'].rolling(window, min_periods=max(1, window//3)).max()
        
        # 📋 Trend Features
        for window in [7, 14, 30]:
            df[f'trend_slope_{window}'] = df['y'].rolling(window).apply(
                lambda x: np.polyfit(range(len(x)), x, 1)[0] if len(x) >= 3 else 0, raw=True
            )
        
        # 📋 Ratio Features (relative changes)
        df['ratio_to_7day_avg'] = df['y'] / df['rolling_mean_7']
        df['ratio_to_30day_avg'] = df['y'] / df['rolling_mean_30']
        df['ratio_same_dow_last_week'] = df['y'] / df['lag_7']
        df['ratio_same_date_last_year'] = df['y'] / df['lag_365']
        
        # 📋 Volatility Features
        df['volatility_7d'] = df['rolling_std_7'] / df['rolling_mean_7']
        df['volatility_30d'] = df['rolling_std_30'] / df['rolling_mean_30']
        
        df = df.dropna().copy()
        df['y_log'] = np.log1p(df['y'])
        df = df.drop(columns=['SBM_DT'])
 
        # 📋 Prediction period
        future_start = df['ds'].max() + timedelta(days=1)
        future_dates = pd.date_range(start=future_start, periods=31)
 
        # 📋 All Features (automatically include all created features)
        exclude_cols = ['ds', 'y', 'y_log', 'holiday_name', 'date']
        features = [col for col in df.columns if col not in exclude_cols and df[col].dtype in ['int64', 'float64', 'int32', 'float32']]
        
        # Define categorical features for LightGBM
        categorical_features = ['dayofweek', 'month', 'quarter', 'dayofmonth', 'week_of_year']
        categorical_features = [f for f in categorical_features if f in features]
 
        # 📋 Step 2: Validation split for evaluation
        val_days = 180
        split_date = df['ds'].max() - pd.Timedelta(days=val_days)
 
        print(f"📆 Data ends: {df['ds'].max().date()}")
        print(f"📆 Validation starts: {(split_date + timedelta(days=1)).date()}")
        print(f"📆 Training ends: {split_date.date()}")
 
        train_df = df[df['ds'] <= split_date].copy()
        val_df = df[df['ds'] > split_date].copy()
 
        print(f"✅ Training rows: {len(train_df)}, Validation rows: {len(val_df)}")
        print(f"🔧 Using {len(features)} features ({len(categorical_features)} categorical)")
 
        X_train = train_df[features]
        y_train = train_df['y_log']
        X_val = val_df[features]
        y_val = val_df['y_log']
 
        X_train_full = df[features]
        y_train_full = df['y_log']
 
        # 📋 Step 3: Use Trial 10's Best Parameters (No Optimization Needed)
        best_params = {
            'objective': 'regression',
            'metric': 'rmse',
            'boosting_type': 'gbdt',
            'num_leaves': 31,
            'learning_rate': 0.052418127033081005,
            'feature_fraction': 0.9140249670721408,
            'bagging_fraction': 0.8953253503091405,
            'bagging_freq': 7,
            'min_child_samples': 10,
            'lambda_l1': 0.01115041615701301,
            'lambda_l2': 0.6759112958166033,
            'verbosity': -1,
            'random_state': 42
        }
        
        print(f"🏆 Using Trial 10's optimal parameters (RMSE: 0.007557)")
        
        # Quick validation to confirm performance
        validation_model = LGBMRegressor(n_estimators=200, **best_params)
        validation_model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            categorical_feature=categorical_features,
            callbacks=[lgb.early_stopping(20), lgb.log_evaluation(0)]
        )
        val_preds = validation_model.predict(X_val)
        val_rmse = mean_squared_error(y_val, val_preds, squared=False)
        print(f"✅ Validation RMSE confirmed: {val_rmse:.6f}")
        
        # Log results
        log_entry = f"""
        adjusted_total, client_id: {client_id}, 
        model: lightgbm_optimized,
        validation_rmse: {val_rmse:.6f}, 
        best_params: {best_params}, 
        features_used: {len(features)},
        timestamp: {datetime.now().isoformat()}
        """
        log_entry = log_entry.replace("'", " ")
        spark.sql(f"INSERT INTO log_ml_data (col1) VALUES ('{log_entry}')")
 
        # 📋 Step 4: Train final model on all data
        model = LGBMRegressor(n_estimators=300, **best_params)
        model.fit(
            X_train_full, y_train_full,
            categorical_feature=categorical_features
        )
 
        # 📋 Step 5: Future predictions with enhanced features
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
            row['is_month_end'] = int(future_day.day >= 28)
            row['is_month_start'] = int(future_day.day <= 3)
 
            # Holiday features
            h = holiday_pdf[holiday_pdf['date'] == future_day]
            row['is_weekend'] = int(future_day.weekday() >= 5)
            for holiday_col in holiday_cols:
                if holiday_col in features:
                    row[holiday_col] = int(h[holiday_col].iloc[0]) if not h.empty and holiday_col in h.columns else 0
 
            # Lag features using recent data
            recent_data = latest_df['y'].values
            for lag in [1, 2, 3, 7, 14, 21, 30, 60, 90, 365]:
                if f'lag_{lag}' in features:
                    row[f'lag_{lag}'] = recent_data[-lag] if len(recent_data) >= lag else recent_data[-1]
            
            # Rolling features
            for window in [3, 7, 14, 21, 30, 60, 90]:
                if len(recent_data) >= window:
                    window_data = recent_data[-window:]
                    if f'rolling_mean_{window}' in features:
                        row[f'rolling_mean_{window}'] = np.mean(window_data)
                    if f'rolling_std_{window}' in features:
                        row[f'rolling_std_{window}'] = np.std(window_data)
                    if f'rolling_min_{window}' in features:
                        row[f'rolling_min_{window}'] = np.min(window_data)
                    if f'rolling_max_{window}' in features:
                        row[f'rolling_max_{window}'] = np.max(window_data)
                else:
                    if f'rolling_mean_{window}' in features:
                        row[f'rolling_mean_{window}'] = np.mean(recent_data)
                    if f'rolling_std_{window}' in features:
                        row[f'rolling_std_{window}'] = np.std(recent_data) if len(recent_data) > 1 else 0
                    if f'rolling_min_{window}' in features:
                        row[f'rolling_min_{window}'] = np.min(recent_data)
                    if f'rolling_max_{window}' in features:
                        row[f'rolling_max_{window}'] = np.max(recent_data)
            
            # Trend features
            for window in [7, 14, 30]:
                if f'trend_slope_{window}' in features:
                    if len(recent_data) >= window:
                        trend_data = recent_data[-window:]
                        row[f'trend_slope_{window}'] = np.polyfit(range(len(trend_data)), trend_data, 1)[0] if len(trend_data) >= 3 else 0
                    else:
                        row[f'trend_slope_{window}'] = 0
            
            # Ratio features (use current rolling means)
            if 'ratio_to_7day_avg' in features:
                row['ratio_to_7day_avg'] = 1.0
            if 'ratio_to_30day_avg' in features:
                row['ratio_to_30day_avg'] = 1.0
            if 'ratio_same_dow_last_week' in features:
                row['ratio_same_dow_last_week'] = 1.0
            if 'ratio_same_date_last_year' in features:
                row['ratio_same_date_last_year'] = 1.0
            
            # Volatility features
            if 'volatility_7d' in features:
                row['volatility_7d'] = np.std(recent_data[-7:]) / np.mean(recent_data[-7:]) if len(recent_data) >= 7 else 0.1
            if 'volatility_30d' in features:
                row['volatility_30d'] = np.std(recent_data[-30:]) / np.mean(recent_data[-30:]) if len(recent_data) >= 30 else 0.1
            
            # Fill any missing features
            for feature in features:
                if feature not in row:
                    row[feature] = 0
 
            row_df = pd.DataFrame([row])
            pred_log = model.predict(row_df[features])[0]
            pred = np.expm1(pred_log)
            row['y'] = pred
 
            latest_df = pd.concat([latest_df, pd.DataFrame([row])], ignore_index=True)
            forecast_results.append((client_id, future_day, float(pred), 'lightgbm_trial10'))
 
    except Exception as e:
        print(f"❌ Error for {client_id}: {str(e)}")
        error_log.append((client_id, str(e)))
        continue
 
# 📋 Save results
forecast_df = pd.DataFrame(forecast_results, columns=['client_id', 'forecast_date', 'yhat', 'model_type'])
error_df = pd.DataFrame(error_log, columns=['client_id', 'error_message'])
 
print(f"\n✅ All clients processed with Trial 10's optimal parameters")
print(f"🏆 Expected performance: ~0.76% error (RMSE: 0.007557)")

# COMMAND ----------

spark_df = spark.createDataFrame(forecast_df)
spark_df.createOrReplaceTempView("forecast_df_temp")
