# ============================================================================
# CRM Sales - Fact Table (Standardization and Cleanup)
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, to_date, when, lit, trim


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
@dp.expect_or_drop("valid_order_number", "order_number IS NOT NULL")
@dp.expect("valid_quantity", "quantity >= 0")
@dp.expect("valid_sales_amount", "sales_amount >= 0")
@dp.expect("valid_price", "price >= 0")
@dp.expect("valid_order_date", "order_date IS NULL OR (order_date >= '1990-01-01' AND order_date <= CURRENT_DATE())")
@dp.expect("valid_ship_sequence", "ship_date IS NULL OR order_date IS NULL OR ship_date >= order_date")
@dp.expect("valid_due_sequence", "due_date IS NULL OR order_date IS NULL OR due_date >= order_date")
def crm_sales_silver():
    df_bronze = spark.read.table("bike_store.bronze.crm_sales")
    
    # Select and rename columns
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
    
    # Treat numeric columns: if null or <= 0, set to 0
    df_silver = df_silver.withColumn(
        "quantity",
        when((col("quantity").isNull()) | (col("quantity") <= 0), lit(0))
        .otherwise(col("quantity"))
    )
    
    df_silver = df_silver.withColumn(
        "sales_amount",
        when((col("sales_amount").isNull()) | (col("sales_amount") < 0), lit(0))
        .otherwise(col("sales_amount"))
    )
    
    df_silver = df_silver.withColumn(
        "price",
        when((col("price").isNull()) | (col("price") < 0), lit(0))
        .otherwise(col("price"))
    )
    
    # Treat date columns: must be valid (>= 1990-01-01 and <= today), otherwise NULL
    df_silver = df_silver.withColumn(
        "order_date",
        when(
            (col("order_date").isNull()) | 
            (col("order_date") < lit("1990-01-01")) | 
            (col("order_date") > current_timestamp()),
            lit(None)
        ).otherwise(col("order_date"))
    )
    
    df_silver = df_silver.withColumn(
        "ship_date",
        when(
            (col("ship_date").isNull()) | 
            (col("ship_date") < lit("1990-01-01")) | 
            (col("ship_date") > current_timestamp()),
            lit(None)
        ).otherwise(col("ship_date"))
    )
    
    df_silver = df_silver.withColumn(
        "due_date",
        when(
            (col("due_date").isNull()) | 
            (col("due_date") < lit("1990-01-01")) | 
            (col("due_date") > current_timestamp()),
            lit(None)
        ).otherwise(col("due_date"))
    )
    
    # Enforce date sequence: order_date <= ship_date <= due_date
    # If ship_date < order_date, set ship_date to NULL
    df_silver = df_silver.withColumn(
        "ship_date",
        when(
            (col("ship_date").isNotNull()) & 
            (col("order_date").isNotNull()) & 
            (col("ship_date") < col("order_date")),
            lit(None)
        ).otherwise(col("ship_date"))
    )
    
    # If due_date < order_date, set due_date to NULL
    df_silver = df_silver.withColumn(
        "due_date",
        when(
            (col("due_date").isNotNull()) & 
            (col("order_date").isNotNull()) & 
            (col("due_date") < col("order_date")),
            lit(None)
        ).otherwise(col("due_date"))
    )
    
    # If due_date < ship_date (and both not NULL), set due_date to NULL
    df_silver = df_silver.withColumn(
        "due_date",
        when(
            (col("due_date").isNotNull()) & 
            (col("ship_date").isNotNull()) & 
            (col("due_date") < col("ship_date")),
            lit(None)
        ).otherwise(col("due_date"))
    )
    
    # Treat string columns: if empty string, set to NULL
    df_silver = df_silver.withColumn(
        "product_key",
        when((col("product_key").isNull()) | (trim(col("product_key")) == ""), lit(None))
        .otherwise(col("product_key"))
    )
    
    df_silver = df_silver.withColumn(
        "customer_id",
        when((col("customer_id").isNull()) | (trim(col("customer_id")) == ""), lit(None))
        .otherwise(col("customer_id"))
    )
    
    # Add Silver metadata
    df_silver = df_silver.withColumn("ingest_timestamp", current_timestamp())
    
    return df_silver
