# ============================================================================
# CRM Product - Dimension Table (Silver Layer)
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, current_timestamp, trim, regexp_replace, coalesce,
    lit, lead, date_sub, substring, when
)
from pyspark.sql.window import Window


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
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dp.expect("valid_product_key", "product_key IS NOT NULL")
@dp.expect("valid_product_name", "product_name IS NOT NULL OR product_name = 'N/A'")
@dp.expect("valid_product_cost", "product_cost >= 0")
@dp.expect("valid_product_dates", "product_start_date IS NOT NULL AND (product_end_date IS NULL OR product_start_date <= product_end_date)")
@dp.expect("valid_product_line", "product_line IN ('Mountain', 'Road', 'Touring', 'Other Sales', 'N/A')")
def crm_product_silver():
    df_bronze = spark.read.table("bike_store.bronze.crm_product")

    # -- Helper: trim + collapse whitespace, coalescing nulls to empty string --
    def clean_text(c):
        return regexp_replace(trim(coalesce(c, lit(""))), r"\s+", " ")

    # -- 1. Rename columns from bronze naming convention --
    df = df_bronze.select(
        col("prd_id").alias("product_id"),
        col("prd_key").alias("product_key"),
        col("prd_nm").alias("product_name"),
        col("prd_cost").alias("product_cost"),
        col("prd_line").alias("product_line"),
        col("prd_start_dt").alias("product_start_date"),
        col("prd_end_dt").alias("product_end_date"),
    )

    # -- 2. Correct end_date using a window over product_key --
    # Use the next record's start_date minus 1 day; NULL for the latest record.
    window_spec = Window.partitionBy("product_key").orderBy("product_start_date")

    df = df.withColumn(
        "product_end_date",
        when(
            lead("product_start_date", 1).over(window_spec).isNotNull(),
            date_sub(lead("product_start_date", 1).over(window_spec), 1)
        ).otherwise(lit(None))
    )

    # -- 3. Derive product_category and strip the prefix from product_key --
    # product_key format: "XXXXX-<actual_key>" (5-char category + dash + key)
    # e.g. "MT-100-BK-42" → category="MT_10", key="BK-42"
    # NOTE: adjust offsets below if the source key format changes.
    raw_category = substring(col("product_key"), 1, 5)
    raw_key      = substring(col("product_key"), 7, 100)  # skip first 6 chars (prefix + dash)

    df = df.withColumn(
        "product_category",
        when(trim(raw_category) == "", lit("N/A"))
        .otherwise(regexp_replace(trim(raw_category), "-", "_"))
    ).withColumn(
        "product_key",
        when(clean_text(raw_key) == "", lit("N/A"))
        .otherwise(clean_text(raw_key))
    )

    # -- 4. Clean product_name: normalize whitespace, replace dashes with spaces --
    df = df.withColumn(
        "product_name",
        when(clean_text(col("product_name")) == "", lit("N/A"))
        .otherwise(
            # Single pass: collapse whitespace after replacing dashes
            regexp_replace(
                regexp_replace(trim(coalesce(col("product_name"), lit(""))), r"[-]+", " "),
                r"\s+", " "
            )
        )
    )

    # -- 5. Expand product_line abbreviations --
    product_line_map = {
        "M": "Mountain",
        "R": "Road",
        "T": "Touring",
        "S": "Other Sales",
    }
    line_col = trim(coalesce(col("product_line"), lit("")))
    line_expr = (
        when(line_col == "M", lit("Mountain"))
        .when(line_col == "R", lit("Road"))
        .when(line_col == "T", lit("Touring"))
        .when(line_col == "S", lit("Other Sales"))
        .when(line_col == "",  lit("N/A"))
        .otherwise(clean_text(col("product_line")))
    )
    df = df.withColumn("product_line", line_expr)

    # -- 6. Replace null costs with 0 --
    df = df.withColumn("product_cost", coalesce(col("product_cost"), lit(0)))

    # -- 7. Final column selection with metadata --
    df = df.select(
        "product_id",
        "product_category",
        "product_key",
        "product_name",
        "product_cost",
        "product_line",
        "product_start_date",
        "product_end_date",
        current_timestamp().alias("ingest_timestamp"),
    )

    return df