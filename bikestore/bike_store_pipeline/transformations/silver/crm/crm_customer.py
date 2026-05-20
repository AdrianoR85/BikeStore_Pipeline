# ============================================================================
# CRM Customer - Dimension Table (already standardized, minor cleanup)
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, trim, regexp_replace, initcap, when, coalesce, lit


# Already standardized naming, just cleanup and select
@dp.materialized_view(
    name="bike_store.silver.crm_customer",
    comment="Cleaned and standardized CRM Customer",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
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

    # Data quality: Remove records with null customer_id
    df_silver = df_silver.filter(col("customer_id").isNotNull())
    
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

    # Add Silver metadata
    df_silver = df_silver.withColumn("ingest_timestamp", current_timestamp())

    return df_silver
