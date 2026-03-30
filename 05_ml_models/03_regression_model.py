# =============================================================================
# MSFT-SNOWFLAKE QUICKSTART LAB: Regression Model
# =============================================================================
# Trains two regression models to predict monthly revenue by category:
#   1. XGBRegressor (gradient boosting)
#   2. LinearRegression (baseline)
#
# Uses snowflake.ml.modeling which wraps scikit-learn/XGBoost and runs
# training directly on Snowflake's compute (Snowpark-optimized warehouse).
#
# Run in a Snowflake Notebook or locally with snowflake-ml-python installed.
# Prerequisites: Run 05_ml_models/01_feature_engineering.py first.
# =============================================================================

from snowflake.snowpark import Session
from snowflake.snowpark import functions as F

from snowflake.ml.modeling.xgboost import XGBRegressor
from snowflake.ml.modeling.linear_model import LinearRegression
from snowflake.ml.modeling.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score,
)
import math

# =============================================================================
# 1. SESSION SETUP
# =============================================================================
# For Snowflake Notebooks: session = get_active_session()

# Uncomment for local execution:
# connection_params = {
#     "account": "<org>-<account>",
#     "user": "<your_username>",
#     "password": "<your_password>",
#     "role": "DEMO_ML_ENGINEER",
#     "warehouse": "DEMO_ML_WH",
#     "database": "MSFT_SNOWFLAKE_DEMO",
#     "schema": "ML"
# }
# session = Session.builder.configs(connection_params).create()

session.use_role("DEMO_ML_ENGINEER")
session.use_warehouse("DEMO_ML_WH")
session.use_database("MSFT_SNOWFLAKE_DEMO")
session.use_schema("ML")

print("Using Snowpark-optimized warehouse for ML training.")

# =============================================================================
# 2. LOAD FEATURES
# =============================================================================

features_df = session.table("ML.REVENUE_REGRESSION_FEATURES")
print(f"Feature table rows: {features_df.count():,}")
features_df.show(5)

# Define feature and target columns
FEATURE_COLS = [
    "CATEGORY_ENCODED",
    "MONTH_NUM",
    "QUARTER",
    "YEAR",
    "ORDER_COUNT_SCALED",
    "UNIQUE_CUSTOMERS_SCALED",
    "TOTAL_UNITS_SCALED",
    "AVG_DISCOUNT_SCALED",
    "REVENUE_LAG_1_SCALED",
    "REVENUE_LAG_2_SCALED",
    "REVENUE_LAG_3_SCALED",
    "REVENUE_MA_3_SCALED",
    "ORDER_COUNT_LAG_1_SCALED",
]
TARGET_COL = "MONTHLY_REVENUE"

# =============================================================================
# 3. TRAIN/TEST SPLIT
# =============================================================================
# For time-series, split chronologically: earlier months train, later months test
# Use the last ~20% of months per category as the test set

# Find the cutoff date (80th percentile of ORDER_MONTH)
month_list = (
    features_df
    .select("ORDER_MONTH")
    .distinct()
    .sort("ORDER_MONTH")
    .collect()
)
cutoff_idx = int(len(month_list) * 0.8)
cutoff_date = month_list[cutoff_idx]["ORDER_MONTH"]
print(f"\nTime-series split cutoff: {cutoff_date}")

train_df = features_df.filter(F.col("ORDER_MONTH") < cutoff_date)
test_df = features_df.filter(F.col("ORDER_MONTH") >= cutoff_date)

print(f"Training set: {train_df.count():,} rows")
print(f"Test set:     {test_df.count():,} rows")

# Show target distribution
print("\nTarget (MONTHLY_REVENUE) statistics — Training:")
train_df.select(TARGET_COL).describe().show()

# =============================================================================
# 4. MODEL 1: XGBOOST REGRESSOR
# =============================================================================

print("\n=== Training XGBRegressor ===")

xgb_model = XGBRegressor(
    input_cols=FEATURE_COLS,
    label_cols=[TARGET_COL],
    output_cols=["XGB_PREDICTED"],
    n_estimators=200,
    max_depth=6,
    learning_rate=0.1,
    subsample=0.8,
    colsample_bytree=0.8,
    reg_alpha=0.1,
    reg_lambda=1.0,
    random_state=42,
)

xgb_model.fit(train_df)
print("XGBRegressor training complete.")

# Predict on test set
xgb_predictions = xgb_model.predict(test_df)

# Evaluate
xgb_mae = mean_absolute_error(
    df=xgb_predictions,
    y_true_col_names=[TARGET_COL],
    y_pred_col_names=["XGB_PREDICTED"],
)

xgb_mse = mean_squared_error(
    df=xgb_predictions,
    y_true_col_names=[TARGET_COL],
    y_pred_col_names=["XGB_PREDICTED"],
)
xgb_rmse = math.sqrt(xgb_mse)

xgb_r2 = r2_score(
    df=xgb_predictions,
    y_true_col_names=[TARGET_COL],
    y_pred_col_names=["XGB_PREDICTED"],
)

# Calculate MAPE
xgb_mape_df = (
    xgb_predictions
    .with_column("APE",
        F.abs(F.col(TARGET_COL) - F.col("XGB_PREDICTED")) / F.col(TARGET_COL) * 100)
    .select(F.avg("APE").alias("MAPE"))
    .collect()
)
xgb_mape = xgb_mape_df[0]["MAPE"]

print(f"\nXGBRegressor Results:")
print(f"  MAE:  ${xgb_mae:,.2f}")
print(f"  RMSE: ${xgb_rmse:,.2f}")
print(f"  R²:   {xgb_r2:.4f}")
print(f"  MAPE: {xgb_mape:.2f}%")

# =============================================================================
# 5. MODEL 2: LINEAR REGRESSION (Baseline)
# =============================================================================

print("\n=== Training LinearRegression ===")

lr_model = LinearRegression(
    input_cols=FEATURE_COLS,
    label_cols=[TARGET_COL],
    output_cols=["LR_PREDICTED"],
)

lr_model.fit(train_df)
print("LinearRegression training complete.")

# Predict on test set
lr_predictions = lr_model.predict(test_df)

# Evaluate
lr_mae = mean_absolute_error(
    df=lr_predictions,
    y_true_col_names=[TARGET_COL],
    y_pred_col_names=["LR_PREDICTED"],
)

lr_mse = mean_squared_error(
    df=lr_predictions,
    y_true_col_names=[TARGET_COL],
    y_pred_col_names=["LR_PREDICTED"],
)
lr_rmse = math.sqrt(lr_mse)

lr_r2 = r2_score(
    df=lr_predictions,
    y_true_col_names=[TARGET_COL],
    y_pred_col_names=["LR_PREDICTED"],
)

lr_mape_df = (
    lr_predictions
    .with_column("APE",
        F.abs(F.col(TARGET_COL) - F.col("LR_PREDICTED")) / F.col(TARGET_COL) * 100)
    .select(F.avg("APE").alias("MAPE"))
    .collect()
)
lr_mape = lr_mape_df[0]["MAPE"]

print(f"\nLinearRegression Results:")
print(f"  MAE:  ${lr_mae:,.2f}")
print(f"  RMSE: ${lr_rmse:,.2f}")
print(f"  R²:   {lr_r2:.4f}")
print(f"  MAPE: {lr_mape:.2f}%")

# =============================================================================
# 6. MODEL COMPARISON
# =============================================================================

print("\n=== Model Comparison ===")
print(f"{'Metric':<12} {'XGBRegressor':>16} {'LinearRegression':>18}")
print("-" * 48)
print(f"{'MAE':<12} ${xgb_mae:>14,.2f} ${lr_mae:>16,.2f}")
print(f"{'RMSE':<12} ${xgb_rmse:>14,.2f} ${lr_rmse:>16,.2f}")
print(f"{'R²':<12} {xgb_r2:>15.4f} {lr_r2:>17.4f}")
print(f"{'MAPE':<12} {xgb_mape:>14.2f}% {lr_mape:>16.2f}%")

best_model_name = "XGBRegressor" if xgb_r2 >= lr_r2 else "LinearRegression"
best_r2 = max(xgb_r2, lr_r2)
print(f"\nBest model: {best_model_name} (R²={best_r2:.4f})")

# =============================================================================
# 7. SAVE PREDICTIONS
# =============================================================================

# Save XGB predictions with actuals for analysis
xgb_output = (
    xgb_predictions
    .with_column("MODEL_NAME", F.lit("XGBRegressor"))
    .with_column("MODEL_VERSION", F.lit("v1"))
    .with_column("RESIDUAL",
        F.col(TARGET_COL) - F.col("XGB_PREDICTED"))
    .with_column("PCT_ERROR",
        F.round(
            F.abs(F.col(TARGET_COL) - F.col("XGB_PREDICTED"))
            / F.col(TARGET_COL) * 100, 2))
    .select(
        "ORDER_MONTH", "CATEGORY", "MONTHLY_REVENUE",
        "XGB_PREDICTED", "RESIDUAL", "PCT_ERROR",
        "MODEL_NAME", "MODEL_VERSION",
    )
    .with_column_renamed("XGB_PREDICTED", "PREDICTED_REVENUE")
)

xgb_output.write.save_as_table(
    "ML.REGRESSION_PREDICTIONS",
    mode="overwrite",
)

print(f"\nPredictions saved to ML.REGRESSION_PREDICTIONS")
print(f"  Rows: {xgb_output.count():,}")

# Show sample predictions vs actuals
print("\nSample Predictions vs Actuals:")
session.table("ML.REGRESSION_PREDICTIONS").sort("ORDER_MONTH", "CATEGORY").show(10)

# =============================================================================
# 8. FEATURE IMPORTANCE (XGBoost)
# =============================================================================

try:
    sklearn_model = xgb_model.to_sklearn()
    importances = sklearn_model.feature_importances_

    print("\n=== Feature Importance (XGBRegressor) ===")
    feature_imp = sorted(
        zip(FEATURE_COLS, importances),
        key=lambda x: x[1],
        reverse=True,
    )
    for feat, imp in feature_imp:
        bar = "█" * int(imp * 50)
        print(f"  {feat:<35} {imp:.4f} {bar}")
except Exception as e:
    print(f"Could not extract feature importance: {e}")

# =============================================================================
# 9. RESIDUAL ANALYSIS
# =============================================================================

print("\n=== Residual Analysis ===")
residuals = session.table("ML.REGRESSION_PREDICTIONS")
residuals.select(
    F.avg("RESIDUAL").alias("MEAN_RESIDUAL"),
    F.stddev("RESIDUAL").alias("STD_RESIDUAL"),
    F.min("RESIDUAL").alias("MIN_RESIDUAL"),
    F.max("RESIDUAL").alias("MAX_RESIDUAL"),
    F.avg("PCT_ERROR").alias("AVG_PCT_ERROR"),
).show()

# Residuals by category
print("Residuals by Category:")
residuals.group_by("CATEGORY").agg(
    F.avg("PCT_ERROR").alias("AVG_PCT_ERROR"),
    F.avg("RESIDUAL").alias("AVG_RESIDUAL"),
    F.count("*").alias("COUNT"),
).sort("CATEGORY").show()

print("\nRegression model training complete.")
print(f"Best model ({best_model_name}) ready for registry in 04_model_registry.py.")
