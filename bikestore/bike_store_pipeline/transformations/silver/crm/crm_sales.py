# ============================================================================
# CRM Sales - Fact Table (Standardization and Cleanup)
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, to_date


@dp.materialized_view(
    name="bike_store.silver.crm_sales",
    comment="Cleaned and standardized CRM Sales",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def crm_sales_silver():
    df_bronze = spark.read.table("bike_store.bronze.crm_sales")
    
    df_silver = df_bronze.select(
        col("sls_ord_num").alias("order_number"),
        col("sls_prd_key").alias("product_key"),
        col("sls_cust_id").alias("customer_id"),
        to_date(col("sls_order_dt").cast("string"), "yyyyMMdd").alias("order_date"),
        to_date(col("sls_ship_dt").cast("string"), "yyyyMMdd").alias("ship_date"),
        to_date(col("sls_due_dt").cast("string"), "yyyyMMdd").alias("due_date"),
        col("sls_sales").alias("sales_amount"),
        col("sls_quantity").alias("quantity"),
        col("sls_price").alias("price")
    )
    
    # Data quality: Remove records with null critical fields
    df_silver = df_silver.filter(
        col("order_number").isNotNull() &
        col("product_key").isNotNull() &
        col("customer_id").isNotNull() &
        col("order_date").isNotNull()
    )
    
    # Data quality: Remove records with invalid quantities or amounts
    df_silver = df_silver.filter(
        (col("quantity") > 0) &
        (col("sales_amount") >= 0) &
        (col("price") >= 0)
    )
    
    # Add Silver metadata
    df_silver = df_silver.withColumn("ingest_timestamp", current_timestamp())
    
    return df_silver
