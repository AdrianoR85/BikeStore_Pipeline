# ============================================================================
# ERP Customer - Dimension Table (Cleaned and Standardized)
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, current_timestamp, trim, upper, 
    when, coalesce, lit, months_between, regexp_replace
)


@dp.materialized_view(
    name="bike_store.silver.erp_customer",
    comment="Cleaned and standardized ERP Customer dimension with demographic information",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def erp_customer_silver():
    df_bronze = spark.read.table("bike_store.bronze.erp_customer")
    
    # Select and rename columns to snake_case
    df_silver = df_bronze.select(
        col("CID").alias("customer_id"),
        col("BDATE").alias("birth_date"),
        col("GEN").alias("gender")
    )
    
    # Data quality: Remove records with null customer_id
    df_silver = df_silver.filter(col("customer_id").isNotNull())
    
    # Standardize customer_id: remove NAS prefix, trim and uppercase
    df_silver = df_silver.withColumn(
        "customer_id",
        upper(trim(regexp_replace(col("customer_id"), "^NAS", "")))
    )
    
    # Standardize gender: Male -> Male, Female -> Female, null/empty -> N/A
    df_silver = df_silver.withColumn(
        "gender",
        when(upper(trim(coalesce(col("gender"), lit("")))) == "MALE", lit("Male"))
        .when(upper(trim(coalesce(col("gender"), lit("")))) == "FEMALE", lit("Female"))
        .when(upper(trim(coalesce(col("gender"), lit("")))) == "M", lit("Male"))
        .when(upper(trim(coalesce(col("gender"), lit("")))) == "F", lit("Female"))
        .when((col("gender").isNull()) | (trim(col("gender")) == ""), lit("N/A"))
        .otherwise(lit("N/A"))
    )
    
    df_silver = df_silver.withColumn(
        "birth_date",
        when(col("birth_date") > current_timestamp(), lit(None)
        ).otherwise(col("birth_date"))
    )

    # Handle birth_date nulls - keep as null for proper date handling
    # Add derived age column based on birth_date
    df_silver = df_silver.withColumn(
        "age",
        when(col("birth_date").isNotNull(),
             (months_between(current_timestamp(), col("birth_date")) / 12).cast("int")
        ).otherwise(lit(None))
    )
    
    # Add age_group derived column for analytics
    df_silver = df_silver.withColumn(
        "age_group",
        when(col("age").isNull(), lit("Unknown"))
        .when(col("age") < 25, lit("Under 25"))
        .when((col("age") >= 25) & (col("age") < 35), lit("25-34"))
        .when((col("age") >= 35) & (col("age") < 45), lit("35-44"))
        .when((col("age") >= 45) & (col("age") < 55), lit("45-54"))
        .when((col("age") >= 55) & (col("age") < 65), lit("55-64"))
        .otherwise(lit("65+"))
    )
    
    # Add Silver metadata
    df_silver = df_silver.withColumn("ingest_timestamp", current_timestamp())
    
    return df_silver
