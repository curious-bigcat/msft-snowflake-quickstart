# =============================================================================
# MSFT-SNOWFLAKE QUICKSTART LAB: Feature Engineering for ML
# =============================================================================
# Prepares feature tables for two ML use cases:
#   1. Classification — Support ticket priority prediction
#   2. Regression — Monthly revenue prediction
#
# Uses snowflake.ml.modeling.preprocessing for scalable feature transforms
# that execute entirely within Snowflake (no data movement).
#
# Run in a Snowflake Notebook or locally with snowflake-ml-python installed.
# Prerequisites: Run phases 01-04 (setup through processing).
# =============================================================================

from snowflake.snowpark import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark import types as T
from snowflake.snowpark.window import Window

from snowflake.ml.modeling.preprocessing import (
    OrdinalEncoder,
    StandardScaler,
    MinMaxScaler,
    OneHotEncoder,
)

# =============================================================================
# 1. SESSION SETUP
# =============================================================================
# For Snowflake Notebooks, session is already available:
# session = get_active_session()

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

print(session.sql("SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()").collect())

# =============================================================================
# 2. CLASSIFICATION FEATURES — Support Ticket Priority
# =============================================================================
# Goal: Predict PRIORITY (Critical, High, Medium, Low) from ticket attributes
# Features: ticket category, customer segment, product info, ticket text length,
#           customer history, time-based features

print("\n=== Building Classification Feature Table ===")

# Base ticket data
tickets_df = session.table("RAW.SUPPORT_TICKETS")
customers_df = session.table("RAW.CUSTOMERS")
products_df = session.table("RAW.PRODUCTS")
orders_df = session.table("RAW.ORDERS")

# Customer order history aggregation
customer_history = (
    orders_df
    .group_by("CUSTOMER_ID")
    .agg(
        F.count("ORDER_ID").alias("CUSTOMER_ORDER_COUNT"),
        F.sum("TOTAL_AMOUNT").alias("CUSTOMER_TOTAL_SPEND"),
        F.avg("TOTAL_AMOUNT").alias("CUSTOMER_AVG_ORDER"),
        F.max("ORDER_DATE").alias("CUSTOMER_LAST_ORDER"),
    )
)

# Build feature set by joining ticket data with customer and product info
classification_features = (
    tickets_df
    .join(customers_df.select(
        "CUSTOMER_ID", "CUSTOMER_SEGMENT", "STATE", "REGISTRATION_DATE"
    ), on="CUSTOMER_ID", how="left")
    .join(products_df.select(
        "PRODUCT_ID", "CATEGORY", "UNIT_PRICE"
    ), on="PRODUCT_ID", how="left")
    .join(customer_history, on="CUSTOMER_ID", how="left")
    # Engineered features
    .with_column("TICKET_DESC_LENGTH",
        F.length(F.col("TICKET_DESCRIPTION")))
    .with_column("TICKET_SUBJECT_LENGTH",
        F.length(F.col("TICKET_SUBJECT")))
    .with_column("TICKET_WORD_COUNT",
        F.array_size(F.split(F.col("TICKET_DESCRIPTION"), F.lit(" "))))
    .with_column("DAYS_AS_CUSTOMER",
        F.datediff("day", F.col("REGISTRATION_DATE"), F.col("CREATED_AT")))
    .with_column("TICKET_HOUR",
        F.hour(F.col("CREATED_AT")))
    .with_column("TICKET_DAY_OF_WEEK",
        F.dayofweek(F.col("CREATED_AT")))
    .with_column("HAS_PRODUCT",
        F.when(F.col("PRODUCT_ID").is_not_null(), 1).otherwise(0))
    # Fill nulls for customers without order history
    .na.fill({
        "CUSTOMER_ORDER_COUNT": 0,
        "CUSTOMER_TOTAL_SPEND": 0.0,
        "CUSTOMER_AVG_ORDER": 0.0,
        "UNIT_PRICE": 0.0,
        "DAYS_AS_CUSTOMER": 0,
    })
    .select(
        "TICKET_ID",
        # Target
        "PRIORITY",
        # Categorical features (to be encoded)
        "CATEGORY",
        "CUSTOMER_SEGMENT",
        "STATUS",
        # Numeric features
        "TICKET_DESC_LENGTH",
        "TICKET_SUBJECT_LENGTH",
        "TICKET_WORD_COUNT",
        "DAYS_AS_CUSTOMER",
        "TICKET_HOUR",
        "TICKET_DAY_OF_WEEK",
        "HAS_PRODUCT",
        "UNIT_PRICE",
        "CUSTOMER_ORDER_COUNT",
        "CUSTOMER_TOTAL_SPEND",
        "CUSTOMER_AVG_ORDER",
    )
)

print(f"Classification features: {classification_features.count():,} rows")
classification_features.show(5)

# --- Encode categorical features ---

# Ordinal encode the target variable (PRIORITY)
priority_encoder = OrdinalEncoder(
    input_cols=["PRIORITY"],
    output_cols=["PRIORITY_ENCODED"],
    categories={"PRIORITY": ["Low", "Medium", "High", "Critical"]},
)
classification_features = priority_encoder.fit(classification_features).transform(classification_features)

# Ordinal encode categorical features
cat_encoder = OrdinalEncoder(
    input_cols=["CATEGORY", "CUSTOMER_SEGMENT", "STATUS"],
    output_cols=["CATEGORY_ENCODED", "SEGMENT_ENCODED", "STATUS_ENCODED"],
)
classification_features = cat_encoder.fit(classification_features).transform(classification_features)

# --- Scale numeric features ---

numeric_cols = [
    "TICKET_DESC_LENGTH", "TICKET_SUBJECT_LENGTH", "TICKET_WORD_COUNT",
    "DAYS_AS_CUSTOMER", "TICKET_HOUR", "TICKET_DAY_OF_WEEK",
    "UNIT_PRICE", "CUSTOMER_ORDER_COUNT", "CUSTOMER_TOTAL_SPEND",
    "CUSTOMER_AVG_ORDER",
]
scaled_cols = [f"{c}_SCALED" for c in numeric_cols]

scaler = StandardScaler(
    input_cols=numeric_cols,
    output_cols=scaled_cols,
)
classification_features = scaler.fit(classification_features).transform(classification_features)

# Select final feature columns
final_class_cols = (
    ["TICKET_ID", "PRIORITY", "PRIORITY_ENCODED"]
    + ["CATEGORY_ENCODED", "SEGMENT_ENCODED", "STATUS_ENCODED", "HAS_PRODUCT"]
    + scaled_cols
)

classification_features.select(final_class_cols).write.save_as_table(
    "ML.TICKET_CLASSIFICATION_FEATURES",
    mode="overwrite",
)

print(f"\nML.TICKET_CLASSIFICATION_FEATURES saved.")

# =============================================================================
# 3. REGRESSION FEATURES — Monthly Revenue Prediction
# =============================================================================
# Goal: Predict next month's revenue given historical patterns
# Features: lagged revenue, order counts, customer metrics, seasonal indicators

print("\n=== Building Regression Feature Table ===")

order_items_df = session.table("RAW.ORDER_ITEMS")

# Monthly aggregates by category
monthly_agg = (
    orders_df
    .join(order_items_df, on="ORDER_ID", how="inner")
    .join(products_df.select("PRODUCT_ID", "CATEGORY"), on="PRODUCT_ID", how="left")
    .with_column("ORDER_MONTH", F.date_trunc("month", F.col("ORDER_DATE")))
    .group_by("ORDER_MONTH", "CATEGORY")
    .agg(
        F.sum("LINE_TOTAL").alias("MONTHLY_REVENUE"),
        F.count_distinct("ORDERS.ORDER_ID").alias("ORDER_COUNT"),
        F.count_distinct("CUSTOMER_ID").alias("UNIQUE_CUSTOMERS"),
        F.sum("QUANTITY").alias("TOTAL_UNITS"),
        F.avg("DISCOUNT_PCT").alias("AVG_DISCOUNT"),
    )
)

# Add time-based and lag features
month_window = Window.partition_by("CATEGORY").order_by("ORDER_MONTH")

regression_features = (
    monthly_agg
    .with_column("MONTH_NUM", F.month(F.col("ORDER_MONTH")))
    .with_column("QUARTER", F.quarter(F.col("ORDER_MONTH")))
    .with_column("YEAR", F.year(F.col("ORDER_MONTH")))
    # Lag features (previous months' revenue)
    .with_column("REVENUE_LAG_1", F.lag("MONTHLY_REVENUE", 1).over(month_window))
    .with_column("REVENUE_LAG_2", F.lag("MONTHLY_REVENUE", 2).over(month_window))
    .with_column("REVENUE_LAG_3", F.lag("MONTHLY_REVENUE", 3).over(month_window))
    # Rolling averages
    .with_column("REVENUE_MA_3",
        F.avg("MONTHLY_REVENUE").over(
            month_window.rows_between(-3, -1)))
    # Order count lags
    .with_column("ORDER_COUNT_LAG_1", F.lag("ORDER_COUNT", 1).over(month_window))
    # Growth rate
    .with_column("REVENUE_GROWTH_PCT",
        F.when(F.col("REVENUE_LAG_1").is_not_null() & (F.col("REVENUE_LAG_1") > 0),
            F.round(
                (F.col("MONTHLY_REVENUE") - F.col("REVENUE_LAG_1"))
                / F.col("REVENUE_LAG_1") * 100, 2
            ))
        .otherwise(None))
    # Drop rows where we don't have enough lag data
    .filter(F.col("REVENUE_LAG_3").is_not_null())
)

# Encode CATEGORY
cat_enc_reg = OrdinalEncoder(
    input_cols=["CATEGORY"],
    output_cols=["CATEGORY_ENCODED"],
)
regression_features = cat_enc_reg.fit(regression_features).transform(regression_features)

# Scale numeric features for regression
reg_numeric = [
    "ORDER_COUNT", "UNIQUE_CUSTOMERS", "TOTAL_UNITS", "AVG_DISCOUNT",
    "REVENUE_LAG_1", "REVENUE_LAG_2", "REVENUE_LAG_3", "REVENUE_MA_3",
    "ORDER_COUNT_LAG_1",
]
reg_scaled = [f"{c}_SCALED" for c in reg_numeric]

reg_scaler = MinMaxScaler(
    input_cols=reg_numeric,
    output_cols=reg_scaled,
)
regression_features = reg_scaler.fit(regression_features).transform(regression_features)

# Final feature set
final_reg_cols = (
    ["ORDER_MONTH", "CATEGORY", "MONTHLY_REVENUE"]
    + ["CATEGORY_ENCODED", "MONTH_NUM", "QUARTER", "YEAR"]
    + reg_scaled
    + ["REVENUE_GROWTH_PCT"]
)

regression_features.select(final_reg_cols).write.save_as_table(
    "ML.REVENUE_REGRESSION_FEATURES",
    mode="overwrite",
)

print(f"ML.REVENUE_REGRESSION_FEATURES saved.")

# =============================================================================
# 4. VERIFICATION
# =============================================================================

print("\n=== Feature Tables Summary ===")
for tbl in ["ML.TICKET_CLASSIFICATION_FEATURES", "ML.REVENUE_REGRESSION_FEATURES"]:
    df = session.table(tbl)
    print(f"\n{tbl}:")
    print(f"  Rows: {df.count():,}")
    print(f"  Columns: {len(df.columns)}")
    df.describe().show()

print("\nFeature engineering complete.")
