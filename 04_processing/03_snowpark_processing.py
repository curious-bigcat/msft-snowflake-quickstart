# =============================================================================
# MSFT-SNOWFLAKE QUICKSTART LAB: Snowpark Python Processing
# =============================================================================
# Demonstrates Snowpark DataFrame API for complex transformations:
#   - Window functions for customer analytics
#   - Python UDFs for sentiment scoring
#   - Writing processed results to analytics tables
#
# Run this in a Snowflake Notebook or locally with snowflake-snowpark-python.
# Prerequisites: Run phases 01-02 (setup + synthetic data).
# =============================================================================

from snowflake.snowpark import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark import types as T
from snowflake.snowpark.window import Window
import json

# =============================================================================
# 1. SESSION SETUP
# =============================================================================
# Option A: Running in a Snowflake Notebook (session is pre-configured)
# Option B: Running locally — configure connection parameters below

# Uncomment for local execution:
# connection_params = {
#     "account": "<org>-<account>",
#     "user": "<your_username>",
#     "password": "<your_password>",
#     "role": "DEMO_ADMIN",
#     "warehouse": "DEMO_WH",
#     "database": "MSFT_SNOWFLAKE_DEMO",
#     "schema": "RAW"
# }
# session = Session.builder.configs(connection_params).create()

# For Snowflake Notebooks, session is already available:
# session = get_active_session()

print(session.sql("SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()").collect())

# =============================================================================
# 2. READ SOURCE DATA
# =============================================================================

orders_df = session.table("RAW.ORDERS")
customers_df = session.table("RAW.CUSTOMERS")
products_df = session.table("RAW.PRODUCTS")
reviews_df = session.table("RAW.PRODUCT_REVIEWS")
order_items_df = session.table("RAW.ORDER_ITEMS")

print(f"Orders: {orders_df.count():,} rows")
print(f"Customers: {customers_df.count():,} rows")
print(f"Reviews: {reviews_df.count():,} rows")

# =============================================================================
# 3. WINDOW FUNCTIONS — Customer Purchase Patterns
# =============================================================================

# Calculate running totals and order sequence per customer
customer_window = Window.partition_by("CUSTOMER_ID").order_by("ORDER_DATE")
customer_all = Window.partition_by("CUSTOMER_ID")

customer_orders = (
    orders_df
    .select(
        "ORDER_ID",
        "CUSTOMER_ID",
        "ORDER_DATE",
        "TOTAL_AMOUNT",
        "REGION",
        "CHANNEL"
    )
    .with_column("ORDER_SEQUENCE",
        F.row_number().over(customer_window))
    .with_column("RUNNING_TOTAL",
        F.sum("TOTAL_AMOUNT").over(customer_window.rows_between(
            Window.UNBOUNDED_PRECEDING, Window.CURRENT_ROW)))
    .with_column("AVG_ORDER_VALUE",
        F.avg("TOTAL_AMOUNT").over(customer_all))
    .with_column("DAYS_SINCE_PREV_ORDER",
        F.datediff("day",
            F.lag("ORDER_DATE").over(customer_window),
            F.col("ORDER_DATE")))
    .with_column("TOTAL_CUSTOMER_ORDERS",
        F.count("ORDER_ID").over(customer_all))
)

print("Customer order patterns sample:")
customer_orders.filter(F.col("CUSTOMER_ID") == 1).sort("ORDER_SEQUENCE").show(10)

# =============================================================================
# 4. PYTHON UDF — Sentiment Scoring
# =============================================================================

# Register a Python UDF for simple keyword-based sentiment analysis
@F.udf(
    name="RAW.UDF_SENTIMENT_SCORE",
    is_permanent=True,
    stage_location="@ML.ML_MODELS",
    replace=True,
    packages=["snowflake-snowpark-python"],
    input_types=[T.StringType()],
    return_type=T.VariantType()
)
def sentiment_score(review_text: str) -> dict:
    """Simple keyword-based sentiment scoring for product reviews."""
    if not review_text:
        return {"score": 0.0, "label": "neutral", "confidence": 0.0}

    text = review_text.lower()

    positive_words = [
        "excellent", "amazing", "love", "great", "best", "outstanding",
        "fantastic", "perfect", "recommend", "reliable", "impressed",
        "seamless", "intuitive", "efficient", "superb", "wonderful"
    ]
    negative_words = [
        "terrible", "worst", "hate", "awful", "poor", "disappointing",
        "broken", "useless", "waste", "crash", "bug", "slow",
        "unacceptable", "frustrating", "defective", "unreliable"
    ]

    pos_count = sum(1 for w in positive_words if w in text)
    neg_count = sum(1 for w in negative_words if w in text)
    total = pos_count + neg_count

    if total == 0:
        return {"score": 0.0, "label": "neutral", "confidence": 0.3}

    score = round((pos_count - neg_count) / total, 2)
    confidence = round(min(total / 5.0, 1.0), 2)

    if score > 0.2:
        label = "positive"
    elif score < -0.2:
        label = "negative"
    else:
        label = "neutral"

    return {"score": score, "label": label, "confidence": confidence}

print("Sentiment UDF registered successfully.")

# =============================================================================
# 5. APPLY SENTIMENT ANALYSIS TO REVIEWS
# =============================================================================

# Apply the UDF to all reviews
reviews_scored = (
    reviews_df
    .with_column("SENTIMENT_RESULT",
        F.call_udf("RAW.UDF_SENTIMENT_SCORE", F.col("REVIEW_TEXT")))
    .with_column("SENTIMENT_SCORE",
        F.col("SENTIMENT_RESULT")["score"].cast(T.FloatType()))
    .with_column("SENTIMENT_LABEL",
        F.col("SENTIMENT_RESULT")["label"].cast(T.StringType()))
    .with_column("SENTIMENT_CONFIDENCE",
        F.col("SENTIMENT_RESULT")["confidence"].cast(T.FloatType()))
    .select(
        "REVIEW_ID", "PRODUCT_ID", "CUSTOMER_ID", "REVIEW_TEXT",
        "RATING", "REVIEW_DATE", "HELPFUL_VOTES",
        "SENTIMENT_SCORE", "SENTIMENT_LABEL", "SENTIMENT_CONFIDENCE"
    )
)

# Write to analytics
reviews_scored.write.save_as_table(
    "ANALYTICS.PROCESSED_REVIEWS",
    mode="overwrite"
)

print(f"Processed {reviews_scored.count():,} reviews with sentiment analysis.")

# Show sentiment distribution
session.table("ANALYTICS.PROCESSED_REVIEWS").group_by("SENTIMENT_LABEL").agg(
    F.count("*").alias("COUNT"),
    F.avg("RATING").alias("AVG_RATING"),
    F.avg("SENTIMENT_SCORE").alias("AVG_SENTIMENT_SCORE")
).sort("COUNT", ascending=False).show()

# =============================================================================
# 6. PRODUCT SENTIMENT SUMMARY
# =============================================================================

product_sentiment = (
    session.table("ANALYTICS.PROCESSED_REVIEWS")
    .join(products_df.select("PRODUCT_ID", "PRODUCT_NAME", "CATEGORY", "BRAND"),
          on="PRODUCT_ID", how="left")
    .group_by("PRODUCT_ID", "PRODUCT_NAME", "CATEGORY", "BRAND")
    .agg(
        F.count("*").alias("REVIEW_COUNT"),
        F.avg("RATING").alias("AVG_RATING"),
        F.avg("SENTIMENT_SCORE").alias("AVG_SENTIMENT"),
        F.sum(F.when(F.col("SENTIMENT_LABEL") == "positive", 1).otherwise(0))
            .alias("POSITIVE_REVIEWS"),
        F.sum(F.when(F.col("SENTIMENT_LABEL") == "negative", 1).otherwise(0))
            .alias("NEGATIVE_REVIEWS"),
        F.sum("HELPFUL_VOTES").alias("TOTAL_HELPFUL_VOTES")
    )
)

product_sentiment.write.save_as_table(
    "ANALYTICS.PRODUCT_SENTIMENT_SUMMARY",
    mode="overwrite"
)

print("Product sentiment summary created.")
product_sentiment.sort(F.col("REVIEW_COUNT").desc()).show(10)

# =============================================================================
# 7. REVENUE ANALYTICS WITH COMPLEX JOINS
# =============================================================================

# Monthly revenue by category with month-over-month growth
monthly_revenue = (
    orders_df
    .join(order_items_df, on="ORDER_ID", how="inner")
    .join(products_df.select("PRODUCT_ID", "CATEGORY", "BRAND"),
          on="PRODUCT_ID", how="left")
    .with_column("ORDER_MONTH",
        F.date_trunc("month", F.col("ORDER_DATE")))
    .group_by("ORDER_MONTH", "CATEGORY")
    .agg(
        F.sum("LINE_TOTAL").alias("MONTHLY_REVENUE"),
        F.count(F.col("ORDER_ITEMS.ORDER_ID")).alias("ITEM_COUNT"),
        F.count_distinct("ORDERS.ORDER_ID").alias("ORDER_COUNT")
    )
)

# Add month-over-month growth
month_window = Window.partition_by("CATEGORY").order_by("ORDER_MONTH")
revenue_with_growth = (
    monthly_revenue
    .with_column("PREV_MONTH_REVENUE",
        F.lag("MONTHLY_REVENUE").over(month_window))
    .with_column("MOM_GROWTH_PCT",
        F.when(F.col("PREV_MONTH_REVENUE").is_not_null(),
            F.round(
                (F.col("MONTHLY_REVENUE") - F.col("PREV_MONTH_REVENUE"))
                / F.col("PREV_MONTH_REVENUE") * 100, 2
            ))
        .otherwise(None))
)

revenue_with_growth.write.save_as_table(
    "ANALYTICS.MONTHLY_REVENUE_BY_CATEGORY",
    mode="overwrite"
)

print("Monthly revenue analytics created.")

# =============================================================================
# 8. VERIFICATION
# =============================================================================

print("\n=== Analytics Tables Created ===")
for table_name in [
    "ANALYTICS.PROCESSED_REVIEWS",
    "ANALYTICS.PRODUCT_SENTIMENT_SUMMARY",
    "ANALYTICS.MONTHLY_REVENUE_BY_CATEGORY"
]:
    count = session.table(table_name).count()
    print(f"  {table_name}: {count:,} rows")

print("\nSnowpark processing complete.")
