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

# COMMAND ----------

def calculate_days_since_last_holiday(date, holiday_dates):
    """Calculate days since last holiday"""
    past_holidays = [h for h in holiday_dates if h < date]
    if past_holidays:
        return (date - max(past_holidays)).days
    return 999  # No recent holiday found

def calculate_days_until_next_holiday(date, holiday_dates):
    """Calculate days until next holiday"""  
    future_holidays = [h for h in holiday_dates if h > date]
    if future_holidays:
        return (min(future_holidays) - date).days
    return 999  # No upcoming holiday found

def count_holidays_in_window(date, holiday_dates, start_offset, end_offset):
    """Count holidays in a time window around the date"""
    start_date = date + timedelta(days=start_offset)
    end_date = date + timedelta(days=end_offset)
    return len([h for h in holiday_dates if start_date <= h <= end_date])

def create_dynamic_holiday_features(date, holiday_df):
    """Create dynamic holiday features without hardcoded factors"""
    
    # Convert to datetime if needed
    if isinstance(date, str):
        date = pd.to_datetime(date)
    
    # Get holiday lists
    major_holidays = pd.to_datetime(holiday_df[holiday_df['is_major_holiday'] == 1]['date']).tolist()
    minor_holidays = pd.to_datetime(holiday_df[holiday_df['is_minor_holiday'] == 1]['date']).tolist()
    
    # Check if current date is a holiday
    holiday_match = holiday_df[pd.to_datetime(holiday_df['date']) == date]
    is_major = int(holiday_match['is_major_holiday'].iloc[0]) if len(holiday_match) > 0 else 0
    is_minor = int(holiday_match['is_minor_holiday'].iloc[0]) if len(holiday_match) > 0 else 0
    
    # Calculate distance metrics
    days_since_major = calculate_days_since_last_holiday(date, major_holidays)
    days_until_major = calculate_days_until_next_holiday(date, major_holidays)
    days_since_minor = calculate_days_since_last_holiday(date, minor_holidays)
    days_until_minor = calculate_days_until_next_holiday(date, minor_holidays)
    
    # Calculate dynamic proximity features
    features = {
        # Current day status
        'is_major_holiday': is_major,
        'is_minor_holiday': is_minor,
        
        # Distance to major holidays (let model learn what these distances mean)
        'days_since_last_major': days_since_major,
        'days_until_next_major': days_until_major,
        
        # Distance to minor holidays  
        'days_since_last_minor': days_since_minor,
        'days_until_next_minor': days_until_minor,
        
        # 🎯 SPECIFIC REBOUND FEATURES (the missing piece!)
        'is_1day_after_major': int(days_since_major == 1),
        'is_2day_after_major': int(days_since_major == 2),
        'is_3day_after_major': int(days_since_major == 3),
        'is_1day_before_major': int(days_until_major == 1),
        'is_2day_before_major': int(days_until_major == 2),
        
        # 🎯 WEEKDAY + HOLIDAY INTERACTION FEATURES (critical for rebound)
        'is_monday_after_major': int(date.weekday() == 0 and days_since_major <= 3),
        'is_tuesday_after_major': int(date.weekday() == 1 and days_since_major <= 3),
        'is_tuesday_after_monday_major': int(
            date.weekday() == 1 and  # Tuesday
            days_since_major == 1 and  # 1 day after major
            any((date - timedelta(days=1)) == h and h.weekday() == 0 for h in major_holidays)  # Yesterday was Monday major holiday
        ),
        
        # Holiday density features
        'major_holidays_in_next_7days': count_holidays_in_window(date, major_holidays, 0, 7),
        'major_holidays_in_last_7days': count_holidays_in_window(date, major_holidays, -7, 0),
        'minor_holidays_in_next_7days': count_holidays_in_window(date, minor_holidays, 0, 7),
        'minor_holidays_in_last_7days': count_holidays_in_window(date, minor_holidays, -7, 0),
        
        # Proximity flags (let model learn patterns around holidays)
        'is_within_3days_of_major': int(days_since_major <= 3 or days_until_major <= 3),
        'is_within_7days_of_major': int(days_since_major <= 7 or days_until_major <= 7)
    }
    
    return features

# COMMAND ----------

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

        # 📋 Load prescription data
        query = f"""
        SELECT SBM_DT, SUM(f.adjusted_cnt) AS adjusted_total
        FROM prd_optumrx_orxfdmprdsa.fdmenh.projections_enhanced_jul f
        WHERE f.lob = '{client_id}'
        GROUP BY SBM_DT
        ORDER BY SBM_DT
        """
        df = spark.sql(query).toPandas()
        if df.empty:
            print(f"⚠ No data for {client_id}")
            continue

        df['ds'] = pd.to_datetime(df['SBM_DT'])
        df['y'] = df['adjusted_total']
        df = df[df['y'] > 0].sort_values('ds').reset_index(drop=True)
        
        print(f"📊 Data: {len(df)} days, {df['y'].min():,} to {df['y'].max():,}")

        # 📋 Load holiday data from your existing table
        print("🎄 Loading holiday data from fdmenh.holiday_list...")
        holiday_query = """
        SELECT date, is_major_holiday, is_minor_holiday
        FROM fdmenh.holiday_list
        ORDER BY date
        """
        holiday_df = spark.sql(holiday_query).toPandas()
        print(f"✅ Loaded {len(holiday_df)} holidays ({holiday_df['is_major_holiday'].sum()} major, {holiday_df['is_minor_holiday'].sum()} minor)")

        # 📋 Create time features
        df['dayofweek'] = df['ds'].dt.dayofweek
        df['month'] = df['ds'].dt.month
        df['dayofmonth'] = df['ds'].dt.day
        df['quarter'] = df['ds'].dt.quarter
        df['day_of_year'] = df['ds'].dt.dayofyear
        
        # Cyclical features for seasonality
        df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
        df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
        df['dow_sin'] = np.sin(2 * np.pi * df['dayofweek'] / 7)
        df['dow_cos'] = np.cos(2 * np.pi * df['dayofweek'] / 7)

        # 📋 Add DYNAMIC holiday features for each date
        print("🧠 Creating dynamic holiday features...")
        holiday_features_list = []
        for _, row in df.iterrows():
            holiday_features = create_dynamic_holiday_features(row['ds'], holiday_df)
            holiday_features_list.append(holiday_features)
        
        # Convert to DataFrame and merge
        holiday_features_df = pd.DataFrame(holiday_features_list)
        df = pd.concat([df.reset_index(drop=True), holiday_features_df.reset_index(drop=True)], axis=1)
        
        print(f"✅ Dynamic holiday features created")
        
        # Check some patterns
        major_holiday_rows = df[df['is_major_holiday'] == 1]
        if len(major_holiday_rows) > 0:
            major_avg = major_holiday_rows['y'].mean()
            normal_avg = df[df['is_major_holiday'] == 0]['y'].mean()
            print(f"📊 Major holidays avg: {major_avg:,.0f} vs normal: {normal_avg:,.0f} (ratio: {major_avg/normal_avg:.3f})")

        # 📋 Create REDUCED lag features (less dominance)
        df['lag_7'] = df['y'].shift(7)
        df['lag_30'] = df['y'].shift(30)
        # ❌ REMOVED: lag_14 (was overpowering holiday features)
        
        # REDUCED rolling features (less dominance)
        df['rolling_mean_21'] = df['y'].shift(1).rolling(21, min_periods=7).mean()
        # ❌ REMOVED: rolling_mean_7, rolling_std_7 (too many lag-based features)
        
        # Remove NaN rows
        df = df.dropna().copy()
        
        # Log transformation (like working XGBoost)
        df['y_log'] = np.log1p(df['y'])
        df = df.drop(columns=['SBM_DT'])
        
        print(f"✅ Final dataset: {len(df)} clean rows")

        # 📋 REDUCED FEATURE SET (giving holiday features more importance)
        features = [
            # Time features (keep all - important for seasonality)
            'dayofweek', 'month', 'dayofmonth', 'quarter', 'day_of_year',
            'month_sin', 'month_cos', 'dow_sin', 'dow_cos',
            
            # 🎯 HOLIDAY FEATURES (should get higher importance now)
            'is_major_holiday', 'is_minor_holiday',
            'is_1day_after_major', 'is_2day_after_major', 'is_3day_after_major',
            'is_1day_before_major', 'is_2day_before_major',
            'is_monday_after_major', 'is_tuesday_after_major', 'is_tuesday_after_monday_major',
            'days_since_last_major', 'days_until_next_major',
            'days_since_last_minor', 'days_until_next_minor',
            'major_holidays_in_next_7days', 'major_holidays_in_last_7days',
            'minor_holidays_in_next_7days', 'minor_holidays_in_last_7days',
            'is_within_3days_of_major', 'is_within_7days_of_major',
            
            # 📉 REDUCED HISTORICAL FEATURES (less dominance)
            'lag_7', 'lag_30',          # Only 2 lag features (removed lag_14)
            'rolling_mean_21'           # Only 1 rolling feature (removed rolling_mean_7, rolling_std_7)
        ]
        
        categorical_features = ['dayofweek', 'month', 'quarter']
        
        print(f"🔧 Using REDUCED feature set: {len(features)} features ({len(categorical_features)} categorical)")
        print(f"📉 Removed lag_14, rolling_mean_7, rolling_std_7 to boost holiday feature importance")

        # 📋 Train/validation split
        val_days = 90
        split_date = df['ds'].max() - pd.Timedelta(days=val_days)
        
        train_df = df[df['ds'] <= split_date]
        val_df = df[df['ds'] > split_date]

        print(f"📆 Training: {train_df['ds'].min().date()} to {train_df['ds'].max().date()} ({len(train_df)} days)")
        print(f"📆 Validation: {val_df['ds'].min().date()} to {val_df['ds'].max().date()} ({len(val_df)} days)")

        X_train, y_train = train_df[features], train_df['y_log']
        X_val, y_val = val_df[features], val_df['y_log']
        X_train_full, y_train_full = df[features], df['y_log']

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

        # 📋 Train final model
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

        # 📊 Analyze feature importance
        feature_importance = pd.DataFrame({
            'feature': features,
            'importance': model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        print(f"\n🧠 TOP 10 FEATURES MODEL LEARNED:")
        for i, (_, row) in enumerate(feature_importance.head(10).iterrows()):
            print(f"{i+1:2d}. {row['feature']:<25} {row['importance']:.4f}")

        # Log results
        log_entry = f"""
        adjusted_total, client_id: {client_id}, 
        model: lightgbm_dynamic_holidays,
        trial_number: {study.best_trial.number}, 
        best_rmse: {study.best_value:.6f}, 
        features_used: {len(features)},
        top_holiday_feature: {feature_importance[feature_importance['feature'].str.contains('holiday|days_')]['feature'].iloc[0] if len(feature_importance[feature_importance['feature'].str.contains('holiday|days_')]) > 0 else 'none'},
        timestamp: {datetime.now().isoformat()}
        """
        log_entry = log_entry.replace("'", " ")
        spark.sql(f"INSERT INTO log_ml_data (col1) VALUES ('{log_entry}')")

        # 📋 Generate predictions with STABLE historical data  
        future_start = df['ds'].max() + timedelta(days=1)
        future_dates = pd.date_range(start=future_start, periods=31)
        
        # Use ORIGINAL historical data for REDUCED lag features
        original_data = df['y'].values
        original_rolling_21 = df['rolling_mean_21'].iloc[-1]
        
        for future_day in future_dates:
            # REDUCED lag features (using original stable data)
            row = {
                'dayofweek': future_day.dayofweek,
                'month': future_day.month,
                'dayofmonth': future_day.day,
                'quarter': (future_day.month - 1) // 3 + 1,
                'day_of_year': future_day.timetuple().tm_yday,
                
                # Cyclical features
                'month_sin': np.sin(2 * np.pi * future_day.month / 12),
                'month_cos': np.cos(2 * np.pi * future_day.month / 12),
                'dow_sin': np.sin(2 * np.pi * future_day.dayofweek / 7),
                'dow_cos': np.cos(2 * np.pi * future_day.dayofweek / 7),
                
                # REDUCED stable lag features (only 2 lag features now)
                'lag_7': original_data[-7],
                'lag_30': original_data[-30],
                'rolling_mean_21': original_rolling_21
            }
            
            # 🧠 DYNAMIC holiday features (no hardcoded factors!)
            holiday_features = create_dynamic_holiday_features(future_day, holiday_df)
            row.update(holiday_features)
            
            # Show holiday detection with rebound info
            if holiday_features['is_major_holiday'] == 1:
                print(f"🎄 MAJOR HOLIDAY DETECTED: {future_day.date()}")
            elif holiday_features['is_1day_after_major'] == 1:
                print(f"📈 1 DAY AFTER MAJOR HOLIDAY (REBOUND): {future_day.date()}")
            elif holiday_features['is_2day_after_major'] == 1:
                print(f"📈 2 DAYS AFTER MAJOR HOLIDAY: {future_day.date()}")
            elif holiday_features['is_tuesday_after_monday_major'] == 1:
                print(f"🚀 TUESDAY AFTER MONDAY MAJOR HOLIDAY (SUPER REBOUND): {future_day.date()}")
            elif holiday_features['is_1day_before_major'] == 1:
                print(f"📅 1 day before major holiday: {future_day.date()}")
            elif holiday_features['days_until_next_major'] <= 3:
                print(f"📅 {holiday_features['days_until_next_major']} days until major holiday: {future_day.date()}")

            # Make prediction
            row_df = pd.DataFrame([row])
            pred_log = model.predict(row_df[features])[0]
            pred = np.expm1(pred_log)
            pred = max(0, pred)
            
            day_name = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'][future_day.dayofweek]
            holiday_flag = " 🎄 MAJOR" if holiday_features['is_major_holiday'] == 1 else ""
            holiday_flag += " 🚀 REBOUND" if holiday_features['is_1day_after_major'] == 1 else ""
            holiday_flag += " 📈 RECOVERY" if holiday_features['is_2day_after_major'] == 1 else ""
            holiday_flag += " 🔔 MINOR" if holiday_features['is_minor_holiday'] == 1 else ""
            
            print(f"🔮 {future_day.date()} ({day_name}): {pred:,.0f}{holiday_flag}")
            
            forecast_results.append((client_id, future_day, float(pred), 'lightgbm_dynamic'))

    except Exception as e:
        print(f"❌ Error for {client_id}: {str(e)}")
        error_log.append((client_id, str(e)))
        continue

# 📋 Save results
forecast_df = pd.DataFrame(forecast_results, columns=['client_id', 'forecast_date', 'yhat', 'model_type'])
error_df = pd.DataFrame(error_log, columns=['client_id', 'error_message'])

print(f"\n🎉 DYNAMIC HOLIDAY LIGHTGBM COMPLETED!")
print(f"🧠 Model learned holiday patterns from data without hardcoded factors")
print(f"🎯 Labor Day 2025 should be properly predicted based on learned patterns")

# COMMAND ----------

spark_df = spark.createDataFrame(forecast_df)
spark_df.createOrReplaceTempView("forecast_df_temp")
