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
