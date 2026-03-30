# =============================================================================
# MSFT-SNOWFLAKE QUICKSTART LAB: Model Registry
# =============================================================================
# Registers trained models in Snowflake's Model Registry for:
#   - Version management and model lineage
#   - One-click deployment as SQL UDFs for inference
#   - Model metadata and metrics tracking
#
# Registers both:
#   1. RandomForestClassifier (ticket priority)
#   2. XGBRegressor (revenue prediction)
#
# Run in a Snowflake Notebook or locally with snowflake-ml-python installed.
# Prerequisites: Run 02_classification_model.py and 03_regression_model.py.
# =============================================================================

from snowflake.snowpark import Session
from snowflake.snowpark import functions as F

from snowflake.ml.registry import Registry
from snowflake.ml.modeling.ensemble import RandomForestClassifier
from snowflake.ml.modeling.xgboost import XGBRegressor
from snowflake.ml.modeling.metrics import (
    accuracy_score,
    f1_score,
    mean_absolute_error,
    r2_score,
)

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

# =============================================================================
# 2. INITIALIZE REGISTRY
# =============================================================================

registry = Registry(session=session, database_name="MSFT_SNOWFLAKE_DEMO", schema_name="ML")

print("Model Registry initialized.")
print(f"Existing models: {len(registry.show_models())} model(s)")

# =============================================================================
# 3. RETRAIN CLASSIFICATION MODEL (for fresh object to register)
# =============================================================================
# We retrain a fresh model here so we have the model object in memory.
# In production, you'd serialize/deserialize or pass objects between scripts.

print("\n=== Retraining Classification Model for Registry ===")

class_features = session.table("ML.TICKET_CLASSIFICATION_FEATURES")

CLASS_FEATURE_COLS = [
    "CATEGORY_ENCODED",
    "SEGMENT_ENCODED",
    "STATUS_ENCODED",
    "HAS_PRODUCT",
    "TICKET_DESC_LENGTH_SCALED",
    "TICKET_SUBJECT_LENGTH_SCALED",
    "TICKET_WORD_COUNT_SCALED",
    "DAYS_AS_CUSTOMER_SCALED",
    "TICKET_HOUR_SCALED",
    "TICKET_DAY_OF_WEEK_SCALED",
    "UNIT_PRICE_SCALED",
    "CUSTOMER_ORDER_COUNT_SCALED",
    "CUSTOMER_TOTAL_SPEND_SCALED",
    "CUSTOMER_AVG_ORDER_SCALED",
]
CLASS_TARGET = "PRIORITY_ENCODED"

train_class, test_class = class_features.random_split([0.8, 0.2], seed=42)

rf_model = RandomForestClassifier(
    input_cols=CLASS_FEATURE_COLS,
    label_cols=[CLASS_TARGET],
    output_cols=["PREDICTED_PRIORITY"],
    n_estimators=100,
    max_depth=10,
    min_samples_split=5,
    random_state=42,
    n_jobs=-1,
)
rf_model.fit(train_class)

# Evaluate
rf_preds = rf_model.predict(test_class)
rf_acc = accuracy_score(df=rf_preds, y_true_col_names=[CLASS_TARGET], y_pred_col_names=["PREDICTED_PRIORITY"])
rf_f1 = f1_score(df=rf_preds, y_true_col_names=[CLASS_TARGET], y_pred_col_names=["PREDICTED_PRIORITY"], average="weighted")

print(f"  Accuracy: {rf_acc:.4f}, F1: {rf_f1:.4f}")

# =============================================================================
# 4. REGISTER CLASSIFICATION MODEL
# =============================================================================

print("\n=== Registering Classification Model ===")

class_model_version = registry.log_model(
    model=rf_model,
    model_name="TICKET_PRIORITY_CLASSIFIER",
    version_name="v1",
    comment="RandomForestClassifier for support ticket priority prediction. "
            "Trained on ticket attributes, customer history, and product info.",
    metrics={
        "accuracy": rf_acc,
        "f1_weighted": rf_f1,
        "training_rows": train_class.count(),
        "test_rows": test_class.count(),
        "n_features": len(CLASS_FEATURE_COLS),
        "algorithm": "RandomForestClassifier",
        "n_estimators": 100,
        "max_depth": 10,
    },
    sample_input_data=train_class.select(CLASS_FEATURE_COLS).limit(10),
)

print(f"  Registered: TICKET_PRIORITY_CLASSIFIER v1")
print(f"  Metrics: {class_model_version.show_metrics()}")

# =============================================================================
# 5. RETRAIN REGRESSION MODEL (for fresh object to register)
# =============================================================================

print("\n=== Retraining Regression Model for Registry ===")

reg_features = session.table("ML.REVENUE_REGRESSION_FEATURES")

REG_FEATURE_COLS = [
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
REG_TARGET = "MONTHLY_REVENUE"

# Chronological split
month_list = reg_features.select("ORDER_MONTH").distinct().sort("ORDER_MONTH").collect()
cutoff_idx = int(len(month_list) * 0.8)
cutoff_date = month_list[cutoff_idx]["ORDER_MONTH"]

train_reg = reg_features.filter(F.col("ORDER_MONTH") < cutoff_date)
test_reg = reg_features.filter(F.col("ORDER_MONTH") >= cutoff_date)

xgb_model = XGBRegressor(
    input_cols=REG_FEATURE_COLS,
    label_cols=[REG_TARGET],
    output_cols=["PREDICTED_REVENUE"],
    n_estimators=200,
    max_depth=6,
    learning_rate=0.1,
    subsample=0.8,
    colsample_bytree=0.8,
    reg_alpha=0.1,
    reg_lambda=1.0,
    random_state=42,
)
xgb_model.fit(train_reg)

# Evaluate
xgb_preds = xgb_model.predict(test_reg)
xgb_mae = mean_absolute_error(df=xgb_preds, y_true_col_names=[REG_TARGET], y_pred_col_names=["PREDICTED_REVENUE"])
xgb_r2 = r2_score(df=xgb_preds, y_true_col_names=[REG_TARGET], y_pred_col_names=["PREDICTED_REVENUE"])

print(f"  MAE: ${xgb_mae:,.2f}, R²: {xgb_r2:.4f}")

# =============================================================================
# 6. REGISTER REGRESSION MODEL
# =============================================================================

print("\n=== Registering Regression Model ===")

reg_model_version = registry.log_model(
    model=xgb_model,
    model_name="REVENUE_PREDICTOR",
    version_name="v1",
    comment="XGBRegressor for monthly revenue prediction by product category. "
            "Uses lagged revenue, order metrics, and seasonal features.",
    metrics={
        "mae": xgb_mae,
        "r2": xgb_r2,
        "training_rows": train_reg.count(),
        "test_rows": test_reg.count(),
        "n_features": len(REG_FEATURE_COLS),
        "algorithm": "XGBRegressor",
        "n_estimators": 200,
        "max_depth": 6,
        "learning_rate": 0.1,
    },
    sample_input_data=train_reg.select(REG_FEATURE_COLS).limit(10),
)

print(f"  Registered: REVENUE_PREDICTOR v1")
print(f"  Metrics: {reg_model_version.show_metrics()}")

# =============================================================================
# 7. MODEL INFERENCE VIA REGISTRY
# =============================================================================
# Once registered, models can be called directly for inference.

print("\n=== Inference via Registry ===")

# Load models back from registry
loaded_class_model = registry.get_model("TICKET_PRIORITY_CLASSIFIER").version("v1")
loaded_reg_model = registry.get_model("REVENUE_PREDICTOR").version("v1")

# Classification inference on new data
print("\n--- Classification Inference ---")
sample_tickets = test_class.select(CLASS_FEATURE_COLS).limit(5)
class_results = loaded_class_model.run(sample_tickets, function_name="predict")
class_results.show()

# Regression inference on new data
print("\n--- Regression Inference ---")
sample_months = test_reg.select(REG_FEATURE_COLS).limit(5)
reg_results = loaded_reg_model.run(sample_months, function_name="predict")
reg_results.show()

# =============================================================================
# 8. SET DEFAULT MODEL VERSIONS
# =============================================================================

print("\n=== Setting Default Versions ===")

class_model_ref = registry.get_model("TICKET_PRIORITY_CLASSIFIER")
class_model_ref.default = "v1"
print(f"  TICKET_PRIORITY_CLASSIFIER default → v1")

reg_model_ref = registry.get_model("REVENUE_PREDICTOR")
reg_model_ref.default = "v1"
print(f"  REVENUE_PREDICTOR default → v1")

# =============================================================================
# 9. REGISTRY OVERVIEW
# =============================================================================

print("\n=== Model Registry Contents ===")
models = registry.show_models()
print(f"Total models: {len(models)}")
for _, row in models.iterrows():
    print(f"\n  Model: {row['name']}")
    print(f"    Created: {row['created_on']}")
    print(f"    Default Version: {row['default_version_name']}")
    print(f"    Comment: {row['comment'][:80]}...")

# Show all versions
print("\n=== Model Versions ===")
for model_name in ["TICKET_PRIORITY_CLASSIFIER", "REVENUE_PREDICTOR"]:
    model_ref = registry.get_model(model_name)
    versions = model_ref.show_versions()
    for _, v in versions.iterrows():
        print(f"  {model_name}/{v['name']}:")
        print(f"    Created: {v['created_on']}")

# =============================================================================
# 10. SQL-BASED INFERENCE EXAMPLE
# =============================================================================
# After registration, models can also be invoked from SQL:

print("\n=== SQL Inference Examples ===")
print("""
-- Classify new support tickets from SQL:
SELECT
    t.TICKET_ID,
    t.PRIORITY AS ACTUAL_PRIORITY,
    ML.TICKET_PRIORITY_CLASSIFIER!PREDICT(
        t.CATEGORY_ENCODED, t.SEGMENT_ENCODED, t.STATUS_ENCODED,
        t.HAS_PRODUCT, t.TICKET_DESC_LENGTH_SCALED,
        t.TICKET_SUBJECT_LENGTH_SCALED, t.TICKET_WORD_COUNT_SCALED,
        t.DAYS_AS_CUSTOMER_SCALED, t.TICKET_HOUR_SCALED,
        t.TICKET_DAY_OF_WEEK_SCALED, t.UNIT_PRICE_SCALED,
        t.CUSTOMER_ORDER_COUNT_SCALED, t.CUSTOMER_TOTAL_SPEND_SCALED,
        t.CUSTOMER_AVG_ORDER_SCALED
    ) AS PREDICTED_PRIORITY
FROM ML.TICKET_CLASSIFICATION_FEATURES t
LIMIT 10;

-- Predict monthly revenue from SQL:
SELECT
    r.ORDER_MONTH,
    r.CATEGORY,
    r.MONTHLY_REVENUE AS ACTUAL_REVENUE,
    ML.REVENUE_PREDICTOR!PREDICT(
        r.CATEGORY_ENCODED, r.MONTH_NUM, r.QUARTER, r.YEAR,
        r.ORDER_COUNT_SCALED, r.UNIQUE_CUSTOMERS_SCALED,
        r.TOTAL_UNITS_SCALED, r.AVG_DISCOUNT_SCALED,
        r.REVENUE_LAG_1_SCALED, r.REVENUE_LAG_2_SCALED,
        r.REVENUE_LAG_3_SCALED, r.REVENUE_MA_3_SCALED,
        r.ORDER_COUNT_LAG_1_SCALED
    ) AS PREDICTED_REVENUE
FROM ML.REVENUE_REGRESSION_FEATURES r
ORDER BY r.ORDER_MONTH DESC
LIMIT 10;
""")

print("Model registry setup complete.")
print("Both models registered, versioned, and ready for inference via Python or SQL.")
