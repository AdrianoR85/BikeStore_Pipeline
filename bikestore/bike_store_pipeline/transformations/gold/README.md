# Gold Layer - All Source Code

## Dimension Tables

### dim_customer.sql
```sql
CREATE OR REFRESH MATERIALIZED VIEW bike_store.gold.dim_customer
COMMENT "Gold customer dimension consolidating CRM customer data with ERP demographics and location"
TBLPROPERTIES (
    "quality" = "gold",
    "layer" = "gold"
)
AS
SELECT 
    -- Customer identifiers
    crm_cst.customer_id,
    crm_cst.customer_key,
    
    -- Customer personal info (from CRM)
    crm_cst.first_name,
    crm_cst.last_name,
    crm_cst.marital_status,
    crm_cst.created_date,
    
    -- Customer demographics (from ERP, prioritize ERP gender over CRM)
    COALESCE(erp_cst.gender, crm_cst.gender) AS gender,
    erp_cst.birth_date,
    erp_cst.age,
    erp_cst.age_group,
    
    -- Customer location (from ERP)
    erp_loc.country,
    erp_loc.region,
    
    -- Metadata
    current_timestamp() AS gold_processed_timestamp
    
FROM bike_store.silver.crm_customer AS crm_cst
LEFT JOIN bike_store.silver.erp_customer AS erp_cst
    ON crm_cst.customer_key = erp_cst.customer_id
LEFT JOIN bike_store.silver.erp_location AS erp_loc
    ON crm_cst.customer_key = erp_loc.customer_id
```

### dim_product.sql
```sql
CREATE OR REFRESH MATERIALIZED VIEW bike_store.gold.dim_product
COMMENT "Gold product dimension consolidating CRM product data with ERP category hierarchy"
TBLPROPERTIES (
    "quality" = "gold",
    "layer" = "gold"
)
AS
SELECT 
    -- Product identifiers
    crm_prd.product_id,
    crm_prd.product_key,
    crm_prd.product_category,
    
    -- Product attributes
    crm_prd.product_name,
    crm_prd.product_cost,
    crm_prd.product_line,
    crm_prd.product_start_date,
    crm_prd.product_end_date,
    
    -- Product category hierarchy (from ERP)
    erp_cat.category,
    erp_cat.subcategory,
    erp_cat.requires_maintenance,
    
    -- Metadata
    current_timestamp() AS gold_processed_timestamp
    
FROM bike_store.silver.crm_product AS crm_prd
LEFT JOIN bike_store.silver.erp_category AS erp_cat
    ON crm_prd.product_category = erp_cat.category_id
```

### dim_calendar.py
```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F

start_date = spark.conf.get("start_date")
end_date = spark.conf.get("end_date")


@dp.materialized_view(
    name="bike_store.gold.dim_calendar",
    comment="Calendar dimension with comprehensive date attributes",
    table_properties={
        "quality": "bike_store.gold.dim_calendar",
        "layer": "gold",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def calendar():
    df = spark.sql(
        f"""
        SELECT explode(sequence(
            to_date('{start_date}'),
            to_date('{end_date}'),
            interval 1 day
        )) as date
    """
    )

    df = df.withColumn(
        "date_key", F.date_format(F.col("date"), "yyyyMMdd").cast("int")
    )

    df = (
        df.withColumn("year", F.year(F.col("date")))
        .withColumn("month", F.month(F.col("date")))
        .withColumn("quarter", F.quarter(F.col("date")))
    )

    df = (
        df.withColumn("day_of_month", F.dayofmonth(F.col("date")))
        .withColumn("day_of_week", F.date_format(F.col("date"), "EEEE"))
        .withColumn("day_of_week_abbr", F.date_format(F.col("date"), "EEE"))
        .withColumn("day_of_week_num", F.dayofweek(F.col("date")))
    )

    df = (
        df.withColumn("month_name", F.date_format(F.col("date"), "MMMM"))
        .withColumn(
            "month_year",
            F.concat(F.date_format(F.col("date"), "MMMM"), F.lit(" "), F.col("year")),
        )
        .withColumn(
            "quarter_year",
            F.concat(F.lit("Q"), F.col("quarter"), F.lit(" "), F.col("year")),
        )
    )

    df = df.withColumn(
        "week_of_year", F.weekofyear(F.col("date"))
    ).withColumn("day_of_year", F.dayofyear(F.col("date")))

    df = df.withColumn(
        "is_weekend",
        F.when(F.col("day_of_week_num").isin([1, 7]), True).otherwise(False),
    ).withColumn(
        "is_weekday",
        F.when(F.col("day_of_week_num").isin([1, 7]), False).otherwise(True),
    )


    df = df.withColumn(
        "silver_processed_timestamp", F.current_timestamp()
    )

    df_silver = df.select(
        "date",
        "date_key",
        "year",
        "month",
        "day_of_month",
        "day_of_week",
        "day_of_week_abbr",
        "month_name",
        "month_year",
        "quarter",
        "quarter_year",
        "week_of_year",
        "day_of_year",
        "is_weekday",
        "is_weekend",
        "silver_processed_timestamp"
    )

    return df_silver
```

## Fact Table

### fact_sales.sql
```sql
CREATE OR REFRESH MATERIALIZED VIEW bike_store.gold.fact_sales
COMMENT "Gold fact table combining sales with customer, product and calendar dimensions"
TBLPROPERTIES (
    "quality" = "gold",
    "layer" = "gold"
)
AS
SELECT 
    -- Sales fact keys
    sl.order_number,
    sl.customer_id,
    sl.product_key,
    
    -- Sales dates
    sl.order_date,
    sl.ship_date,
    sl.due_date,
    
    -- Sales measures
    sl.sales_amount,
    sl.quantity,
    sl.price,
    
    -- Customer dimension attributes
    cst.customer_key,
    cst.first_name,
    cst.last_name,
    cst.marital_status,
    cst.gender AS customer_gender,
    cst.birth_date AS customer_birth_date,
    cst.age AS customer_age,
    cst.age_group AS customer_age_group,
    cst.country AS customer_country,
    cst.region AS customer_region,
    
    -- Product dimension attributes
    prd.product_id,
    prd.product_category,
    prd.product_name,
    prd.product_cost,
    prd.product_line,
    prd.product_start_date,
    prd.product_end_date,
    prd.category AS product_category_name,
    prd.subcategory AS product_subcategory,
    prd.requires_maintenance AS product_requires_maintenance,
    
    -- Calendar attributes
    cal.year AS order_year,
    cal.month AS order_month,
    cal.day_of_month AS order_day,
    cal.day_of_week AS order_day_of_week,
    cal.month_name AS order_month_name,
    cal.quarter AS order_quarter,
    cal.is_weekend AS order_is_weekend,
    
    -- Metadata
    current_timestamp() AS gold_processed_timestamp
    
FROM bike_store.silver.crm_sales AS sl
LEFT JOIN bike_store.gold.dim_customer AS cst 
    ON sl.customer_id = cst.customer_id
LEFT JOIN bike_store.gold.dim_product AS prd
    ON sl.product_key = prd.product_key
LEFT JOIN bike_store.gold.dim_calendar AS cal
    ON sl.order_date = cal.date
```
