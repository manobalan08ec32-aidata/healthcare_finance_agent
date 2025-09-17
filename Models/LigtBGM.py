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

        # Load data
        query = f"""
        SELECT SBM_DT, SUM(f.adjusted_cnt) AS adjusted_total
        FROM prd_optumrx_orxfdmprdsa.fdmenh.projections_enhanced_jul f
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

        # Load holidays
        try:
            holiday_pdf = spark.table("fdmenh.holiday_flg").toPandas()
            holiday_pdf['date'] = pd.to_datetime(holiday_pdf['date'])
            df = df.merge(holiday_pdf[['date', 'is_major_holiday', 'is_minor_holiday', 'is_weekend']], 
                         left_on='ds', right_on='date', how='left')
            df['is_major_holiday'] = df['is_major_holiday'].fillna(0).astype(int)
            df['is_minor_holiday'] = df['is_minor_holiday'].fillna(0).astype(int)
            df['is_weekend'] = df['is_weekend'].fillna(0).astype(int)
        except:
            df['is_major_holiday'] = 0
            df['is_minor_holiday'] = 0
            df['is_weekend'] = (df['ds'].dt.dayofweek >= 5).astype(int)

        # SIMPLE FEATURES ONLY (no complex lag features that break)
        df['dayofweek'] = df['ds'].dt.dayofweek
        df['month'] = df['ds'].dt.month
        df['dayofmonth'] = df['ds'].dt.day
        df['quarter'] = df['ds'].dt.quarter
        
        # Cyclical features
        df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
        df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
        df['dow_sin'] = np.sin(2 * np.pi * df['dayofweek'] / 7)
        df['dow_cos'] = np.cos(2 * np.pi * df['dayofweek'] / 7)
        
        # Simple lag features (only essential ones)
        df['lag_7'] = df['y'].shift(7)
        df['lag_30'] = df['y'].shift(30)
        df['rolling_mean_21'] = df['y'].shift(1).rolling(21, min_periods=7).mean()
        
        # Remove NaN rows
        df = df.dropna().copy()
        print(f"✅ Clean data: {len(df)} rows")

        # Features
        features = ['dayofweek', 'month', 'dayofmonth', 'quarter', 'month_sin', 'month_cos', 
                   'dow_sin', 'dow_cos', 'is_major_holiday', 'is_minor_holiday', 'is_weekend',
                   'lag_7', 'lag_30', 'rolling_mean_21']
        
        categorical_features = ['dayofweek', 'month', 'quarter']

        # Train/validation split
        val_days = 90
        split_date = df['ds'].max() - pd.Timedelta(days=val_days)
        train_df = df[df['ds'] <= split_date]
        val_df = df[df['ds'] > split_date]

        X_train, y_train = train_df[features], train_df['y']
        X_val, y_val = val_df[features], val_df['y']
        
        # Check patterns
        train_weekend_avg = train_df[train_df['is_weekend'] == 1]['y'].mean()
        train_weekday_avg = train_df[train_df['is_weekend'] == 0]['y'].mean()
        print(f"📊 Training: Weekday {train_weekday_avg:,.0f}, Weekend {train_weekend_avg:,.0f}")

        # Simple hyperparameter optimization
        def objective(trial):
            params = {
                'n_estimators': trial.suggest_int('n_estimators', 100, 500),
                'learning_rate': trial.suggest_float('learning_rate', 0.05, 0.2),
                'num_leaves': trial.suggest_int('num_leaves', 31, 100),
                'min_child_samples': trial.suggest_int('min_child_samples', 10, 50),
                'random_state': 42,
                'verbosity': -1
            }
            
            model = LGBMRegressor(**params)
            model.fit(X_train, y_train, 
                     eval_set=[(X_val, y_val)], 
                     categorical_feature=categorical_features,
                     callbacks=[lgb.early_stopping(20), lgb.log_evaluation(0)])
            
            preds = model.predict(X_val)
            return mean_absolute_percentage_error(y_val, preds)

        study = optuna.create_study(direction='minimize')
        study.optimize(objective, n_trials=50)
        
        print(f"🏆 Best MAPE: {study.best_value:.4f}")

        # Train final model
        final_model = LGBMRegressor(**study.best_params, random_state=42)
        final_model.fit(df[features], df['y'], categorical_feature=categorical_features)

        # FIXED PREDICTION: Use actual historical values, not predicted ones
        future_start = df['ds'].max() + timedelta(days=1)
        future_dates = pd.date_range(start=future_start, periods=31)
        
        # Get stable historical values for lag features
        historical_values_7 = df['y'].iloc[-7:].values  # Last 7 days
        historical_values_30 = df['y'].iloc[-30:].values  # Last 30 days
        historical_rolling_21 = df['rolling_mean_21'].iloc[-1]  # Latest 21-day average

        for i, future_day in enumerate(future_dates):
            row = {}
            
            # Time features
            row['dayofweek'] = future_day.dayofweek
            row['month'] = future_day.month
            row['dayofmonth'] = future_day.day
            row['quarter'] = (future_day.month - 1) // 3 + 1
            
            # Cyclical features
            row['month_sin'] = np.sin(2 * np.pi * row['month'] / 12)
            row['month_cos'] = np.cos(2 * np.pi * row['month'] / 12)
            row['dow_sin'] = np.sin(2 * np.pi * row['dayofweek'] / 7)
            row['dow_cos'] = np.cos(2 * np.pi * row['dayofweek'] / 7)
            
            # Holiday features (simple detection)
            row['is_weekend'] = int(future_day.weekday() >= 5)
            row['is_major_holiday'] = 0  # Simplified
            row['is_minor_holiday'] = 0  # Simplified
            
            # FIXED: Use actual historical values, not predictions
            row['lag_7'] = historical_values_7[(-7 + i) % 7] if i < 7 else historical_values_7[-1]
            row['lag_30'] = historical_values_30[(-30 + i) % 30] if i < 30 else historical_values_30[-1]
            row['rolling_mean_21'] = historical_rolling_21  # Use stable historical average

            # Predict
            pred = final_model.predict(pd.DataFrame([row]))[0]
            pred = max(0, pred)
            
            day_name = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'][future_day.dayofweek]
            print(f"🔮 {future_day.date()} ({day_name}): {pred:,.0f}")
            
            forecast_results.append((client_id, future_day, float(pred), 'lightgbm_simple'))

    except Exception as e:
        print(f"❌ Error for {client_id}: {str(e)}")
        error_log.append((client_id, str(e)))

# Save results
forecast_df = pd.DataFrame(forecast_results, columns=['client_id', 'forecast_date', 'yhat', 'model_type'])
print(f"\n✅ Simplified LightGBM completed - should have proper scale now!")

# COMMAND ----------

spark_df = spark.createDataFrame(forecast_df)
spark_df.createOrReplaceTempView("forecast_df_temp")
