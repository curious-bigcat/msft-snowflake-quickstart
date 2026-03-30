# =============================================================================
# MSFT-SNOWFLAKE QUICKSTART LAB: Classification Model
# =============================================================================
# Trains two classification models to predict support ticket priority:
#   1. RandomForestClassifier
#   2. LogisticRegression
#
# Uses snowflake.ml.modeling which wraps scikit-learn and runs training
# directly on Snowflake's compute (Snowpark-optimized warehouse).
#
# Run in a Snowflake Notebook or locally with snowflake-ml-python installed.
# Prerequisites: Run 05_ml_models/01_feature_engineering.py first.
# =============================================================================

from snowflake.snowpark import Session
from snowflake.snowpark import functions as F

from snowflake.ml.modeling.ensemble import RandomForestClassifier
from snowflake.ml.modeling.linear_model import LogisticRegression
from snowflake.ml.modeling.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix,
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

print("Using Snowpark-optimized warehouse for ML training.")

# =============================================================================
# 2. LOAD FEATURES
# =============================================================================

features_df = session.table("ML.TICKET_CLASSIFICATION_FEATURES")
print(f"Feature table rows: {features_df.count():,}")

# Define feature and target columns
FEATURE_COLS = [
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
TARGET_COL = "PRIORITY_ENCODED"
LABEL_COL = "PRIORITY"

# =============================================================================
# 3. TRAIN/TEST SPLIT
# =============================================================================

# Use Snowpark random split (80/20)
train_df, test_df = features_df.random_split([0.8, 0.2], seed=42)

print(f"Training set: {train_df.count():,} rows")
print(f"Test set:     {test_df.count():,} rows")

# Check class distribution in training set
print("\nTraining set class distribution:")
train_df.group_by(LABEL_COL).agg(F.count("*").alias("COUNT")).sort(LABEL_COL).show()

# =============================================================================
# 4. MODEL 1: RANDOM FOREST CLASSIFIER
# =============================================================================

print("\n=== Training RandomForestClassifier ===")

rf_model = RandomForestClassifier(
    input_cols=FEATURE_COLS,
    label_cols=[TARGET_COL],
    output_cols=["RF_PREDICTED"],
    n_estimators=100,
    max_depth=10,
    min_samples_split=5,
    random_state=42,
    n_jobs=-1,
)

rf_model.fit(train_df)
print("RandomForest training complete.")

# Predict on test set
rf_predictions = rf_model.predict(test_df)

# Evaluate
rf_accuracy = accuracy_score(
    df=rf_predictions,
    y_true_col_names=[TARGET_COL],
    y_pred_col_names=["RF_PREDICTED"],
)

rf_precision = precision_score(
    df=rf_predictions,
    y_true_col_names=[TARGET_COL],
    y_pred_col_names=["RF_PREDICTED"],
    average="weighted",
)

rf_recall = recall_score(
    df=rf_predictions,
    y_true_col_names=[TARGET_COL],
    y_pred_col_names=["RF_PREDICTED"],
    average="weighted",
)

rf_f1 = f1_score(
    df=rf_predictions,
    y_true_col_names=[TARGET_COL],
    y_pred_col_names=["RF_PREDICTED"],
    average="weighted",
)

print(f"\nRandomForest Results:")
print(f"  Accuracy:  {rf_accuracy:.4f}")
print(f"  Precision: {rf_precision:.4f}")
print(f"  Recall:    {rf_recall:.4f}")
print(f"  F1-Score:  {rf_f1:.4f}")

# Confusion matrix
rf_cm = confusion_matrix(
    df=rf_predictions,
    y_true_col_name=TARGET_COL,
    y_pred_col_name="RF_PREDICTED",
)
print(f"\nConfusion Matrix:\n{rf_cm}")

# =============================================================================
# 5. MODEL 2: LOGISTIC REGRESSION
# =============================================================================

print("\n=== Training LogisticRegression ===")

lr_model = LogisticRegression(
    input_cols=FEATURE_COLS,
    label_cols=[TARGET_COL],
    output_cols=["LR_PREDICTED"],
    max_iter=1000,
    multi_class="multinomial",
    solver="lbfgs",
    random_state=42,
)

lr_model.fit(train_df)
print("LogisticRegression training complete.")

# Predict on test set
lr_predictions = lr_model.predict(test_df)

# Evaluate
lr_accuracy = accuracy_score(
    df=lr_predictions,
    y_true_col_names=[TARGET_COL],
    y_pred_col_names=["LR_PREDICTED"],
)

lr_precision = precision_score(
    df=lr_predictions,
    y_true_col_names=[TARGET_COL],
    y_pred_col_names=["LR_PREDICTED"],
    average="weighted",
)

lr_recall = recall_score(
    df=lr_predictions,
    y_true_col_names=[TARGET_COL],
    y_pred_col_names=["LR_PREDICTED"],
    average="weighted",
)

lr_f1 = f1_score(
    df=lr_predictions,
    y_true_col_names=[TARGET_COL],
    y_pred_col_names=["LR_PREDICTED"],
    average="weighted",
)

print(f"\nLogisticRegression Results:")
print(f"  Accuracy:  {lr_accuracy:.4f}")
print(f"  Precision: {lr_precision:.4f}")
print(f"  Recall:    {lr_recall:.4f}")
print(f"  F1-Score:  {lr_f1:.4f}")

lr_cm = confusion_matrix(
    df=lr_predictions,
    y_true_col_name=TARGET_COL,
    y_pred_col_name="LR_PREDICTED",
)
print(f"\nConfusion Matrix:\n{lr_cm}")

# =============================================================================
# 6. MODEL COMPARISON
# =============================================================================

print("\n=== Model Comparison ===")
print(f"{'Metric':<12} {'RandomForest':>14} {'LogisticReg':>14}")
print("-" * 42)
print(f"{'Accuracy':<12} {rf_accuracy:>14.4f} {lr_accuracy:>14.4f}")
print(f"{'Precision':<12} {rf_precision:>14.4f} {lr_precision:>14.4f}")
print(f"{'Recall':<12} {rf_recall:>14.4f} {lr_recall:>14.4f}")
print(f"{'F1-Score':<12} {rf_f1:>14.4f} {lr_f1:>14.4f}")

# Select best model
best_model_name = "RandomForest" if rf_f1 >= lr_f1 else "LogisticRegression"
best_model = rf_model if rf_f1 >= lr_f1 else lr_model
best_f1 = max(rf_f1, lr_f1)
print(f"\nBest model: {best_model_name} (F1={best_f1:.4f})")

# =============================================================================
# 7. SAVE PREDICTIONS
# =============================================================================

# Save RF predictions with original labels for analysis
rf_output = (
    rf_predictions
    .with_column("MODEL_NAME", F.lit("RandomForestClassifier"))
    .with_column("MODEL_VERSION", F.lit("v1"))
    .select(
        "TICKET_ID", "PRIORITY", "PRIORITY_ENCODED",
        "RF_PREDICTED", "MODEL_NAME", "MODEL_VERSION"
    )
    .with_column_renamed("RF_PREDICTED", "PREDICTED_PRIORITY")
)

rf_output.write.save_as_table(
    "ML.CLASSIFICATION_PREDICTIONS",
    mode="overwrite",
)

print(f"\nPredictions saved to ML.CLASSIFICATION_PREDICTIONS")
print(f"  Rows: {rf_output.count():,}")

# =============================================================================
# 8. FEATURE IMPORTANCE (RandomForest)
# =============================================================================

# Extract feature importance from the sklearn model inside the wrapper
try:
    sklearn_model = rf_model.to_sklearn()
    importances = sklearn_model.feature_importances_

    print("\n=== Feature Importance (RandomForest) ===")
    feature_imp = sorted(
        zip(FEATURE_COLS, importances),
        key=lambda x: x[1],
        reverse=True,
    )
    for feat, imp in feature_imp:
        bar = "█" * int(imp * 50)
        print(f"  {feat:<40} {imp:.4f} {bar}")
except Exception as e:
    print(f"Could not extract feature importance: {e}")

print("\nClassification model training complete.")
print(f"Best model ({best_model_name}) ready for registry in 04_model_registry.py.")
