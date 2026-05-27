# ============================================================================
# ERP Category - Dimension Table (Cleaned and Standardized)
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, current_timestamp, trim, regexp_replace, 
    initcap, when, coalesce, lit, upper
)


@dp.materialized_view(
    name="bike_store.silver.erp_category",
    comment="Cleaned and standardized ERP Category dimension with product classification hierarchy",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dp.expect_or_drop("valid_category_id", "category_id IS NOT NULL")
@dp.expect("valid_category", "category IS NOT NULL")
@dp.expect("valid_subcategory", "subcategory IS NOT NULL")
@dp.expect("valid_maintenance_flag", "requires_maintenance IN (true, false)")
def erp_category_silver():
    df_bronze = spark.read.table("bike_store.bronze.erp_category")
    
    # Reusable function to standardize text columns
    def standardize_text(column_name):
        return initcap(
            regexp_replace(
                trim(coalesce(col(column_name), lit(""))),
                "\\s+",  # Replace multiple spaces with single space
                " "
            )
        )
    
    # Select and rename columns to snake_case
    df_silver = df_bronze.select(
        col("ID").alias("category_id"),
        col("CAT").alias("category"),
        col("SUBCAT").alias("subcategory"),
        col("MAINTENANCE").alias("requires_maintenance")
    )
    
    # Standardize category_id: trim and uppercase
    df_silver = df_silver.withColumn(
        "category_id",
        upper(trim(col("category_id")))
    )
    
    # Standardize category: trim, title case, handle nulls
    df_silver = df_silver.withColumn(
        "category",
        when(standardize_text("category") == "", lit("N/A"))
        .otherwise(standardize_text("category"))
    )
    
    # Standardize subcategory: trim, title case, handle nulls
    df_silver = df_silver.withColumn(
        "subcategory",
        when(standardize_text("subcategory") == "", lit("N/A"))
        .otherwise(standardize_text("subcategory"))
    )
    
    # Standardize requires_maintenance: Convert to boolean
    # Yes -> true, No -> false, null/empty -> false
    df_silver = df_silver.withColumn(
        "requires_maintenance",
        when(upper(trim(coalesce(col("requires_maintenance"), lit("")))) == "YES", lit(True))
        .otherwise(lit(False))
    )
    
    # Add Silver metadata
    df_silver = df_silver.withColumn("ingest_timestamp", current_timestamp())
    
    return df_silver
