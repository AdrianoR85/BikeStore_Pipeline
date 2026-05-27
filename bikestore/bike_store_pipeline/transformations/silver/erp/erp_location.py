# ============================================================================
# ERP Location - Dimension Table (Cleaned and Standardized)
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, current_timestamp, trim, upper, 
    initcap, regexp_replace, when, coalesce, lit
)


@dp.materialized_view(
    name="bike_store.silver.erp_location",
    comment="Cleaned and standardized ERP Location dimension with geographic information",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect("valid_country", "country IS NOT NULL")
@dp.expect("valid_region", "region IN ('Oceania', 'North America', 'Europe', 'Unknown', 'Other')")
def erp_location_silver():
    df_bronze = spark.read.table("bike_store.bronze.erp_location")
    
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
        col("CID").alias("customer_id"),
        col("CNTRY").alias("country")
    )
    
    # Standardize customer_id: remove dash, trim and uppercase
    df_silver = df_silver.withColumn(
        "customer_id",
        upper(trim(regexp_replace(col("customer_id"), "-", "")))
    )
    
    # Standardize country: trim, title case, handle nulls
    df_silver = df_silver.withColumn(
        "country",
        when(standardize_text("country") == "", lit("N/A"))
        .when(standardize_text("country") == "Us", lit("United States"))
        .otherwise(standardize_text("country"))
    )
    
    # Add derived region column based on country for analytics
    df_silver = df_silver.withColumn(
        "region",
        when(col("country").isin(["Australia", "New Zealand"]), lit("Oceania"))
        .when(col("country").isin(["United States", "Canada", "Mexico"]), lit("North America"))
        .when(col("country").isin(["United Kingdom", "Germany", "France", "Italy", "Spain", 
                                   "Netherlands", "Belgium", "Austria", "Switzerland", "Ireland", 
                                   "Poland", "Sweden", "Norway", "Denmark", "Finland"]), lit("Europe"))
        .when(col("country") == "N/A", lit("Unknown"))
        .otherwise(lit("Other"))
    )
    
    # Add Silver metadata
    df_silver = df_silver.withColumn("ingest_timestamp", current_timestamp())
    
    return df_silver
