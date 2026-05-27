# Silver Layer - All Source Code

## CRM Files

### crm_customer.py
```python
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
```

### crm_product.py
```python
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
```

### crm_sales.py
```python
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
```

## ERP Files

### erp_customer.py
```python
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
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect("valid_gender", "gender IN ('Male', 'Female', 'N/A')")
@dp.expect("valid_birth_date", "birth_date IS NULL OR birth_date <= CURRENT_DATE()")
@dp.expect("valid_age", "age IS NULL OR (age >= 0 AND age <= 120)")
@dp.expect("valid_age_group", "age_group IN ('Unknown', 'Under 25', '25-34', '35-44', '45-54', '55-64', '65+')")
def erp_customer_silver():
    df_bronze = spark.read.table("bike_store.bronze.erp_customer")
    
    # Select and rename columns to snake_case
    df_silver = df_bronze.select(
        col("CID").alias("customer_id"),
        col("BDATE").alias("birth_date"),
        col("GEN").alias("gender")
    )
    
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
```

### erp_location.py
```python
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
```

### erp_category.py
```python
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
```
