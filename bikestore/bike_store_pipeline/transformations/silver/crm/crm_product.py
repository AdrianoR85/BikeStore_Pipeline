# ============================================================================
# CRM Product - Dimension Table (already standardized, minor cleanup)
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, trim, regexp_replace, initcap, when, coalesce, lit, lead, date_sub, substring
from pyspark.sql.window import Window


# Already standardized naming, just cleanup and select
@dp.materialized_view(
    name="bike_store.silver.crm_product",
    comment="Cleaned and standardized CRM Product",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def crm_product_silver():
    df_bronze = spark.read.table("bike_store.bronze.crm_product")
    
    # Standardize column names and select
    df_silver = df_bronze.select(
        col("prd_id").alias("product_id"),
        col("prd_key").alias("product_key"),
        col("prd_nm").alias("product_name"),
        col("prd_cost").alias("product_cost"),
        col("prd_line").alias("product_line"),
        col("prd_start_dt").alias("product_start_date"),
        col("prd_end_dt").alias("product_end_date")
    )
    
    # Standardize text columns: trim, remove double spaces, replace nulls
    def standardize_text(column_name):
        return regexp_replace(
            trim(coalesce(col(column_name), lit(""))),
            "\\s+",  # Replace multiple spaces with single space
            " "
        )
    
    # Fix date columns using window function (do this BEFORE splitting product_key)
    # Get the next start_date for the same product_key
    window_spec = Window.partitionBy("product_key").orderBy("product_start_date")
    
    df_silver = df_silver.withColumn(
        "next_start_date",
        lead("product_start_date", 1).over(window_spec)
    )
    
    # Calculate corrected end_date: next_start_date - 1 day
    # If next_start_date is NULL (last record), keep end_date as NULL
    df_silver = df_silver.withColumn(
        "product_end_date",
        when(col("next_start_date").isNotNull(), 
             date_sub(col("next_start_date"), 1))
        .otherwise(lit(None))
    )
    
    # Drop the temporary column
    df_silver = df_silver.drop("next_start_date")
    
    # Extract first 4 characters from product_key as product_category
    df_silver = df_silver.withColumn("product_category",
        substring(col("product_key"), 1, 5)
    )
    
    # Replace dashes with underscores in product_category
    df_silver = df_silver.withColumn("product_category",
        regexp_replace(col("product_category"), "-", "_")
    )
    
    # Standardize product_category
    df_silver = df_silver.withColumn("product_category",
        when(trim(col("product_category")) == "", lit("N/A"))
        .otherwise(trim(col("product_category")))
    )
    
    # Update product_key to remove the first 4 characters (keep the rest)
    df_silver = df_silver.withColumn("product_key",
        substring(col("product_key"), 7, 100)
    )
    
    # Apply text standardization to product_key
    df_silver = df_silver.withColumn("product_key", 
        when(standardize_text("product_key") == "", lit("N/A"))
        .otherwise(standardize_text("product_key"))
    )
    
    # Product name: remove dashes and extra spaces, then standardize
    df_silver = df_silver.withColumn("product_name",
        when(standardize_text("product_name") == "", lit("N/A"))
        .otherwise(
            regexp_replace(
                regexp_replace(
                    standardize_text("product_name"),
                    "-",  # Remove dashes
                    " "
                ),
                "\\s+",  # Remove double spaces again after dash removal
                " "
            )
        )
    )
    
    # Standardize product_line: M -> Mountain, R -> Road, T -> Touring, S -> Other Sales, null/empty -> N/A
    df_silver = df_silver.withColumn("product_line",
        when(trim(coalesce(col("product_line"), lit(""))) == "M", lit("Mountain"))
        .when(trim(coalesce(col("product_line"), lit(""))) == "R", lit("Road"))
        .when(trim(coalesce(col("product_line"), lit(""))) == "T", lit("Touring"))
        .when(trim(coalesce(col("product_line"), lit(""))) == "S", lit("Other Sales"))
        .when((col("product_line").isNull()) | (trim(col("product_line")) == ""), lit("N/A"))
        .otherwise(standardize_text("product_line"))
    )
    
    # Replace null costs with 0 or handle appropriately
    df_silver = df_silver.withColumn("product_cost",
        coalesce(col("product_cost"), lit(0))
    )
    
    # Reorder columns to place product_category before product_key
    df_silver = df_silver.select(
        "product_id",
        "product_category",
        "product_key",
        "product_name",
        "product_cost",
        "product_line",
        "product_start_date",
        "product_end_date"
    )

    # Add Silver metadata
    df_silver = df_silver.withColumn("ingest_timestamp", current_timestamp())

    return df_silver
