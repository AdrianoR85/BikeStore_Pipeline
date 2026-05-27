# ============================================================================
# CRM Customer - Dimension Table (already standardized, minor cleanup)
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, current_timestamp, trim, regexp_replace, initcap, 
    when, coalesce, lit, row_number
)
from pyspark.sql import Window


# Already standardized naming, just cleanup and select
@dp.materialized_view(
    name="bike_store.silver.crm_customer",
    comment="Cleaned and standardized CRM Customer with deduplication",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect("valid_gender", "gender IN ('Female', 'Male', 'N/A')")
@dp.expect("valid_marital_status", "marital_status IN ('Married', 'Single', 'N/A')")
@dp.expect("valid_created_date", "created_date IS NOT NULL AND created_date <= CURRENT_DATE()")
@dp.expect("complete_names", "first_name != 'N/A' AND last_name != 'N/A'")
def crm_customer_silver():
    df_bronze = spark.read.table("bike_store.bronze.crm_customer")
    
    df_silver = df_bronze.select(
        col("cst_id").alias("customer_id"),
        col("cst_key").alias("customer_key"),
        col("cst_firstname").alias("first_name"),
        col("cst_lastname").alias("last_name"),
        col("cst_marital_status").alias("marital_status"),
        col("cst_gndr").alias("gender"),
        col("cst_create_date").alias("created_date")
    )
    
    # Standardize text columns: trim, remove double spaces, title case, replace nulls
    def standardize_text(column_name):
        return initcap(
            regexp_replace(
                trim(coalesce(col(column_name), lit(""))),
                "\\s+",  # Replace multiple spaces with single space
                " "
            )
        )
    
    # Apply text standardization to string columns
    df_silver = df_silver.withColumn("first_name", 
        when(standardize_text("first_name") == "", lit("N/A"))
        .otherwise(standardize_text("first_name"))
    )
    
    df_silver = df_silver.withColumn("last_name",
        when(standardize_text("last_name") == "", lit("N/A"))
        .otherwise(standardize_text("last_name"))
    )
    
    # Standardize marital_status: M -> Married, S -> Single, null/empty -> N/A
    df_silver = df_silver.withColumn("marital_status",
        when(trim(coalesce(col("marital_status"), lit(""))) == "M", lit("Married"))
        .when(trim(coalesce(col("marital_status"), lit(""))) == "S", lit("Single"))
        .when((col("marital_status").isNull()) | (trim(col("marital_status")) == ""), lit("N/A"))
        .otherwise(standardize_text("marital_status"))
    )
    
    # Standardize gender: F -> Female, M -> Male, null/empty -> N/A
    df_silver = df_silver.withColumn("gender",
        when(trim(coalesce(col("gender"), lit(""))) == "F", lit("Female"))
        .when(trim(coalesce(col("gender"), lit(""))) == "M", lit("Male"))
        .when((col("gender").isNull()) | (trim(col("gender")) == ""), lit("N/A"))
        .otherwise(standardize_text("gender"))
    )
    
    # Deduplication: Calculate completeness score (count non-N/A values)
    completeness_cols = ["first_name", "last_name", "marital_status", "gender"]
    completeness_expr = sum(
        when(col(c) != "N/A", 1).otherwise(0) for c in completeness_cols
    )
    
    df_silver = df_silver.withColumn("completeness_score", completeness_expr)
    
    # Window: Partition by customer_id, order by completeness and created_date
    window_spec = Window.partitionBy("customer_id").orderBy(
        col("completeness_score").desc(),
        col("created_date").desc()
    )
    
    # Rank and keep only the most complete record per customer_id
    df_silver = df_silver.withColumn("rank", row_number().over(window_spec))
    df_silver = df_silver.filter(col("rank") == 1).drop("rank", "completeness_score")

    # Add Silver metadata
    df_silver = df_silver.withColumn("ingest_timestamp", current_timestamp())

    return df_silver
