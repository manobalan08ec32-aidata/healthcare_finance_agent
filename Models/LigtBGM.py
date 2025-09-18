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

def calculate_days_since_last_holiday(date, holiday_dates):
    past_holidays = [h for h in holiday_dates if h < date]
    if past_holidays:
        return (date - max(past_holidays)).days
    return 999

def calculate_days_until_next_holiday(date, holiday_dates):
    future_holidays = [h for h in holiday_dates if h > date]
    if future_holidays:
        return (min(future_holidays) - date).days
    return 999

def create_dynamic_holiday_features(date, holiday_df):
    if isinstance(date, str):
        date = pd.to_datetime(date)
    
    major_holidays = pd.to_datetime(holiday_df[holiday_df['is_major_holiday'] == 1]['date']).tolist()
    minor_holidays = pd.to_datetime(holiday_df[holiday_df['is_minor_holiday'] == 1]['date']).tolist()
    
    holiday_match = holiday_df[pd.to_datetime(holiday_df['date']) == date]
    is_major = int(holiday_match['is_major_holiday'].iloc[0]) if len(holiday_match) > 0 else 0
    is_minor = int(holiday_match['is_minor_holiday'].iloc[0]) if len(holiday_match) > 0 else 0
    
    days_since_major = calculate_days_since_last_holiday(date, major_holidays)
    days_until_major = calculate_days_until_next_holiday(date, major_holidays)
    days_since_minor = calculate_days_since_last_holiday(date, minor_holidays)
    days_until_minor = calculate_days_until_next_holiday(date, minor_holidays)
    
    features = {
        'is_major_holiday': is_major,
        'is_minor_holiday': is_minor,
        'days_since_last_major': days_since_major,
        'days_until_next_major': days_until_major,
        'days_since_last_minor': days_since_minor,
        'days_until_next_minor': days_until_minor,
        'is_1day_after_major': int(days_since_major == 1),
        'is_2day_after_major': int(days_since_major == 2),
        'is_3day_after_major': int(days_since_major == 3),
        'is_1day_before_major': int(days_until_major == 1),
        'is_2day_before_major': int(days_until_major == 2),
        'is_tuesday_after_monday_major': int(
            date.weekday() == 1 and 
            days_since_major == 1 and 
            any((date - timedelta(days=1)) == h and h.weekday() == 0 for h in major_holidays)
        )
    }
    return features

# Get client list
client_df = spark.sql("SELECT DISTINCT lob as client_id FROM prd_optumrx_orxfdmprdsa.fdmenh.projections_enhanced_jul")
client_list = [row.client_id for row in client_df.collect()]

forecast_results = []
error_log = []

for client_id in client_list:
    try:
        print(f"\n=== Processing {client_id} ===")

        # Load data
        query = f"""
        SELECT SBM_DT, SUM(f.adjusted_cnt) AS adjusted_total
        FROM prd_optumrx_orxfdmprdsa.fdmenh.projections_enhanced_jul f
        WHERE f.lob = '{client_id}'
        GROUP BY SBM_DT ORDER BY SBM_DT
        """
        df = spark.sql(query).toPandas()
        if df.empty:
            continue

        df['ds'] = pd.to_datetime(df['SBM_DT'])
        df['y'] = df['adjusted_total']
        df = df[df['y'] > 0].sort_values('ds').reset_index(drop=True)

        # Load holidays
        holiday_query = "SELECT date, is_major_holiday, is_minor_holiday FROM fdmenh.holiday_list ORDER BY date"
        holiday_df = spark.sql(holiday_query).toPandas()

        # Time features
        df['dayofweek'] = df['ds'].dt.dayofweek
        df['month'] = df['ds'].dt.month
        df['dayofmonth'] = df['ds'].dt.day
        df['quarter'] = df['ds'].dt.quarter
        df['day_of_year'] = df['ds'].dt.dayofyear
        df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
        df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
        df['dow_sin'] = np.sin(2 * np.pi * df['dayofweek'] / 7)
        df['dow_cos'] = np.cos(2 * np.pi * df['dayofweek'] / 7)

        # Holiday features
        holiday_features_list = []
        for _, row in df.iterrows():
            holiday_features = create_dynamic_holiday_features(row['ds'], holiday_df)
            holiday_features_list.append(holiday_features)
        
        holiday_features_df = pd.DataFrame(holiday_features_list)
        df = pd.concat([df.reset_index(drop=True), holiday_features_df.reset_index(drop=True)], axis=1)

        # INTERACTION FEATURES (boost holiday importance)
        df['major_holiday_weekday_interaction'] = df['is_major_holiday'] * df['dayofweek']
        df['rebound_tuesday_signal'] = df['is_1day_after_major'] * (df['dayofweek'] == 1).astype(int)
        df['major_holiday_month_interaction'] = df['is_major_holiday'] * df['month']
        df['pre_holiday_friday_signal'] = df['is_1day_before_major'] * (df['dayofweek'] == 4).astype(int)
        df['weekend_after_major'] = df['is_1day_after_major'] * (df['dayofweek'] >= 5).astype(int)
        df['holiday_proximity_strength'] = (df['is_1day_after_major'] * 3 + 
                                           df['is_2day_after_major'] * 2 + 
                                           df['is_1day_before_major'] * 2)

        # Lag features
        df['lag_7'] = df['y'].shift(7)
        df['lag_30'] = df['y'].shift(30)
        df['rolling_mean_21'] = df['y'].shift(1).rolling(21, min_periods=7).mean()
        
        df = df.dropna().copy()
        df['y_log'] = np.log1p(df['y'])  # Keep log transformation
        df = df.drop(columns=['SBM_DT'])

        features = [
            'dayofweek', 'month', 'dayofmonth', 'quarter', 'day_of_year',
            'month_sin', 'month_cos', 'dow_sin', 'dow_cos',
            'is_major_holiday', 'is_minor_holiday',
            'is_1day_after_major', 'is_2day_after_major', 'is_3day_after_major',
            'is_1day_before_major', 'is_2day_before_major', 'is_tuesday_after_monday_major',
            'days_since_last_major', 'days_until_next_major',
            'days_since_last_minor', 'days_until_next_minor',
            # INTERACTION FEATURES
            'major_holiday_weekday_interaction', 'rebound_tuesday_signal',
            'major_holiday_month_interaction', 'pre_holiday_friday_signal',
            'weekend_after_major', 'holiday_proximity_strength',
            'lag_7', 'lag_30', 'rolling_mean_21'
        ]
        
        categorical_features = ['dayofweek', 'month', 'quarter']

        # Train/validation split
        val_days = 90
        split_date = df['ds'].max() - pd.Timedelta(days=val_days)
        train_df = df[df['ds'] <= split_date]
        val_df = df[df['ds'] > split_date]

        X_train, y_train = train_df[features], train_df['y_log']  # Log values
        X_val, y_val = val_df[features], val_df['y_log']

        # Hyperparameter optimization
        def objective(trial):
            params = {
                'objective': 'regression',
                'metric': 'rmse',
                'boosting_type': 'gbdt',
                'num_leaves': trial.suggest_int('num_leaves', 31, 100),
                'learning_rate': trial.suggest_float('learning_rate', 0.05, 0.2),
                'feature_fraction': trial.suggest_float('feature_fraction', 0.7, 1.0),
                'bagging_fraction': trial.suggest_float('bagging_fraction', 0.7, 1.0),
                'min_child_samples': trial.suggest_int('min_child_samples', 5, 30),
                'lambda_l1': trial.suggest_float('lambda_l1', 0, 1.0),
                'lambda_l2': trial.suggest_float('lambda_l2', 0, 1.0),
                'verbosity': -1,
                'random_state': 42
            }
            
            model = LGBMRegressor(n_estimators=300, **params)
            model.fit(X_train, y_train, eval_set=[(X_val, y_val)],
                     categorical_feature=categorical_features,
                     callbacks=[lgb.early_stopping(20), lgb.log_evaluation(0)])
            
            preds = model.predict(X_val)
            return mean_squared_error(y_val, preds, squared=False)

        study = optuna.create_study(direction='minimize')
        study.optimize(objective, n_trials=50)

        # Train final model
        final_model = LGBMRegressor(n_estimators=400, **study.best_params, 
                                   verbosity=-1, random_state=42)
        final_model.fit(df[features], df['y_log'], categorical_feature=categorical_features)

        # Generate predictions
        future_start = df['ds'].max() + timedelta(days=1)
        future_dates = pd.date_range(start=future_start, periods=31)
        
        original_data = df['y'].values
        original_rolling_21 = df['rolling_mean_21'].iloc[-1]
        
        for future_day in future_dates:
            row = {
                'dayofweek': future_day.dayofweek,
                'month': future_day.month,
                'dayofmonth': future_day.day,
                'quarter': (future_day.month - 1) // 3 + 1,
                'day_of_year': future_day.timetuple().tm_yday,
                'month_sin': np.sin(2 * np.pi * future_day.month / 12),
                'month_cos': np.cos(2 * np.pi * future_day.month / 12),
                'dow_sin': np.sin(2 * np.pi * future_day.dayofweek / 7),
                'dow_cos': np.cos(2 * np.pi * future_day.dayofweek / 7),
                'lag_7': original_data[-7],
                'lag_30': original_data[-30],
                'rolling_mean_21': original_rolling_21
            }
            
            holiday_features = create_dynamic_holiday_features(future_day, holiday_df)
            row.update(holiday_features)
            
            # Calculate interaction features
            row['major_holiday_weekday_interaction'] = row['is_major_holiday'] * row['dayofweek']
            row['rebound_tuesday_signal'] = row['is_1day_after_major'] * (row['dayofweek'] == 1)
            row['major_holiday_month_interaction'] = row['is_major_holiday'] * row['month']
            row['pre_holiday_friday_signal'] = row['is_1day_before_major'] * (row['dayofweek'] == 4)
            row['weekend_after_major'] = row['is_1day_after_major'] * (row['dayofweek'] >= 5)
            row['holiday_proximity_strength'] = (row['is_1day_after_major'] * 3 + 
                                                row['is_2day_after_major'] * 2 + 
                                                row['is_1day_before_major'] * 2)
            
            # Prediction with log transformation
            pred_log = final_model.predict(pd.DataFrame([row]))[0]
            pred = np.expm1(pred_log)
            pred = max(0, pred)
            
            day_name = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'][future_day.dayofweek]
            print(f"{future_day.date()} ({day_name}): {pred:,.0f}")
            
            forecast_results.append((client_id, future_day, float(pred), 'lightgbm_interactions'))

    except Exception as e:
        print(f"Error for {client_id}: {str(e)}")
        error_log.append((client_id, str(e)))

forecast_df = pd.DataFrame(forecast_results, columns=['client_id', 'forecast_date', 'yhat', 'model_type'])
spark.createDataFrame(forecast_df).createOrReplaceTempView("forecast_df_temp")
