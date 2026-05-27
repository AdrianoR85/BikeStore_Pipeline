# ============================================================================
# SILVER LAYER TRANSFORMATIONS - Column Standardization
# ============================================================================
# Purpose: Transform Bronze tables to Silver with standardized naming
# Conventions:
#   - All columns: lowercase_with_underscores
#   - Dimensions: dim_ prefix
#   - Facts: fct_ prefix
#   - Type corrections: INT → DATE, INT → DECIMAL
#   - Data quality: Remove invalid/negative values
# ============================================================================

from pyspark.sql import functions as F
from datetime import datetime

print(f"Silver transformation started at: {datetime.now()}")

# ============================================================================
# 1. ERP Category - Dimension Table
# ============================================================================
print("\nProcessing: bike_store.silver.dim_erp_category")

df_bronze = spark.table("bike_store.bronze.erp_category")

# Standardize column names
df_silver = df_bronze.select(
    F.col("ID").alias("category_id"),
    F.col("CAT").alias("category"),
    F.col("SUBCAT").alias("subcategory"),
    F.col("MAINTENANCE").alias("maintenance_flag"),
    F.col("file_name").alias("source_file"),
    F.col("ingest_datetime").alias("bronze_load_timestamp")
)

# Add Silver metadata
df_silver = df_silver.withColumn("silver_load_timestamp", F.current_timestamp())

# Write to Silver table
df_silver.write.mode("overwrite").saveAsTable("bike_store.silver.dim_erp_category")
print(f"  ✅ Created: bike_store.silver.dim_erp_category ({df_silver.count():,} records)")

# ============================================================================
# 2. ERP Customer - Dimension Table
# ============================================================================
print("\nProcessing: bike_store.silver.dim_erp_customer")

df_bronze = spark.table("bike_store.bronze.erp_customer")

# Standardize column names
df_silver = df_bronze.select(
    F.col("CID").alias("customer_id"),
    F.col("BDATE").alias("birth_date"),
    F.col("GEN").alias("gender"),
    F.col("file_name").alias("source_file"),
    F.col("ingest_datetime").alias("bronze_load_timestamp")
)

# Data quality: Standardize gender values
df_silver = df_silver.withColumn(
    "gender",
    F.when(F.col("gender").isin(["M", "Male"]), "M")
     .when(F.col("gender").isin(["F", "Female"]), "F")
     .otherwise(None)
)

# Data quality: Remove future birth dates
df_silver = df_silver.filter(
    (F.col("birth_date").isNull()) | 
    (F.col("birth_date") <= F.current_date())
)

# Add Silver metadata
df_silver = df_silver.withColumn("silver_load_timestamp", F.current_timestamp())

df_silver.write.mode("overwrite").saveAsTable("bike_store.silver.dim_erp_customer")
print(f"  ✅ Created: bike_store.silver.dim_erp_customer ({df_silver.count():,} records)")

# ============================================================================
# 3. ERP Location - Dimension Table
# ============================================================================
print("\nProcessing: bike_store.silver.dim_erp_location")

df_bronze = spark.table("bike_store.bronze.erp_location")

# Standardize column names
df_silver = df_bronze.select(
    F.col("CID").alias("customer_id"),
    F.col("CNTRY").alias("country_code"),
    F.col("file_name").alias("source_file"),
    F.col("ingest_datetime").alias("bronze_load_timestamp")
)

# Data quality: Clean country codes (remove nulls and invalid formats)
df_silver = df_silver.filter(
    F.col("country_code").isNotNull() & 
    (F.length(F.col("country_code")).between(2, 3))
)

# Add Silver metadata
df_silver = df_silver.withColumn("silver_load_timestamp", F.current_timestamp())

df_silver.write.mode("overwrite").saveAsTable("bike_store.silver.dim_erp_location")
print(f"  ✅ Created: bike_store.silver.dim_erp_location ({df_silver.count():,} records)")

# ============================================================================
# 4. CRM Customer - Dimension Table (already standardized, minor cleanup)
# ============================================================================
print("\nProcessing: bike_store.silver.dim_crm_customer")

df_bronze = spark.table("bike_store.bronze.crm_customer")

# Already standardized naming, just cleanup and select
df_silver = df_bronze.select(
    F.col("cst_id").alias("customer_id"),
    F.col("cst_key").alias("customer_key"),
    F.col("cst_firstname").alias("first_name"),
    F.col("cst_lastname").alias("last_name"),
    F.col("cst_marital_status").alias("marital_status"),
    F.col("cst_gndr").alias("gender"),
    F.col("cst_create_date").alias("customer_create_date"),
    F.col("file_name").alias("source_file"),
    F.col("ingest_datetime").alias("bronze_load_timestamp")
)

# Data quality: Remove records with null customer_id
df_silver = df_silver.filter(F.col("customer_id").isNotNull())

# Add Silver metadata
df_silver = df_silver.withColumn("silver_load_timestamp", F.current_timestamp())

df_silver.write.mode("overwrite").saveAsTable("bike_store.silver.dim_crm_customer")
print(f"  ✅ Created: bike_store.silver.dim_crm_customer ({df_silver.count():,} records)")

# ============================================================================
# 5. CRM Product - Dimension Table
# ============================================================================
print("\nProcessing: bike_store.silver.dim_crm_product")

df_bronze = spark.table("bike_store.bronze.crm_product")

df_silver = df_bronze.select(
    F.col("prd_id").alias("product_id"),
    F.col("prd_key").alias("product_key"),
    F.col("prd_nm").alias("product_name"),
    (F.col("prd_cost") / 100.0).cast("decimal(10,2)").alias("product_cost"),  # INT to DECIMAL
    F.col("prd_line").alias("product_line"),
    F.col("prd_start_dt").alias("product_start_date"),
    F.col("prd_end_dt").alias("product_end_date"),
    F.col("file_name").alias("source_file"),
    F.col("ingest_datetime").alias("bronze_load_timestamp")
)

# Data quality: Remove negative costs
df_silver = df_silver.filter(
    (F.col("product_cost").isNull()) | 
    (F.col("product_cost") >= 0)
)

# Add Silver metadata
df_silver = df_silver.withColumn("silver_load_timestamp", F.current_timestamp())

df_silver.write.mode("overwrite").saveAsTable("bike_store.silver.dim_crm_product")
print(f"  ✅ Created: bike_store.silver.dim_crm_product ({df_silver.count():,} records)")

# ============================================================================
# 6. CRM Sales - Fact Table
# ============================================================================
print("\nProcessing: bike_store.silver.fct_sales")

df_bronze = spark.table("bike_store.bronze.crm_sales")

df_silver = df_bronze.select(
    F.col("sls_ord_num").alias("sales_order_number"),
    F.col("sls_prd_key").alias("product_key"),
    F.col("sls_cust_id").alias("customer_id"),
    
    # Convert INT dates to proper DATE type (format: YYYYMMDD)
    F.to_date(F.col("sls_order_dt").cast("string"), "yyyyMMdd").alias("order_date"),
    F.to_date(F.col("sls_ship_dt").cast("string"), "yyyyMMdd").alias("ship_date"),
    F.to_date(F.col("sls_due_dt").cast("string"), "yyyyMMdd").alias("due_date"),
    
    # Convert INT to DECIMAL for monetary values (assuming cents)
    (F.col("sls_sales") / 100.0).cast("decimal(10,2)").alias("sales_amount"),
    F.col("sls_quantity").alias("quantity"),
    (F.col("sls_price") / 100.0).cast("decimal(10,2)").alias("unit_price"),
    
    F.col("file_name").alias("source_file"),
    F.col("ingest_datetime").alias("bronze_load_timestamp")
)

# Data quality filters
df_silver = df_silver.filter(
    (F.col("sales_amount").isNull() | (F.col("sales_amount") >= 0)) &
    (F.col("quantity") > 0) &
    (F.col("unit_price").isNull() | (F.col("unit_price") >= 0))
)

# Add Silver metadata
df_silver = df_silver.withColumn("silver_load_timestamp", F.current_timestamp())

df_silver.write.mode("overwrite").saveAsTable("bike_store.silver.fct_sales")
print(f"  ✅ Created: bike_store.silver.fct_sales ({df_silver.count():,} records)")

# ============================================================================
# Summary
# ============================================================================
print("\n" + "="*100)
print("✅ SILVER LAYER TRANSFORMATION COMPLETED")
print("="*100)
print(f"\nCompleted at: {datetime.now()}")
print("\nTables created:")
print("  - bike_store.silver.dim_erp_category")
print("  - bike_store.silver.dim_erp_customer")
print("  - bike_store.silver.dim_erp_location")
print("  - bike_store.silver.dim_crm_customer")
print("  - bike_store.silver.dim_crm_product")
print("  - bike_store.silver.fct_sales")
print("\nKey improvements:")
print("  ✅ Standardized naming: lowercase_with_underscores")
print("  ✅ Type corrections: INT → DATE, INT → DECIMAL")
print("  ✅ Data quality: Removed invalid/negative values")
print("  ✅ Metadata: Added silver_load_timestamp")
print("="*100)
