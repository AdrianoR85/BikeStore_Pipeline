-- ML-Ready Sales Fact Table
-- This table filters out data quality issues and adds ML features for various use cases:
-- - Sales forecasting, customer segmentation, demand prediction, churn analysis

CREATE OR REFRESH MATERIALIZED VIEW bike_store.gold.fact_sales_ml
COMMENT "ML-ready sales fact table with data quality filters and engineered features"
TBLPROPERTIES (
    "quality" = "gold",
    "layer" = "ml_features",
    "filters_applied" = "price > 0 AND sales_amount > 0",
    "use_case" = "machine_learning"
)
CLUSTER BY (customer_id, order_year, order_month)
AS
SELECT 
    -- Primary Keys
    order_number,
    customer_id,
    product_key,
    
    -- Date Features
    order_date,
    ship_date,
    due_date,
    order_year,
    order_month,
    order_day,
    order_quarter,
    order_month_name,
    order_day_of_week,
    order_is_weekend,
    
    -- Original Sales Measures
    sales_amount,
    quantity,
    price,
    
    -- Engineered Features: Delivery Metrics
    DATEDIFF(ship_date, order_date) AS days_to_ship,
    DATEDIFF(due_date, order_date) AS days_until_due,
    CASE 
        WHEN ship_date IS NULL THEN 1
        WHEN ship_date <= due_date THEN 0
        ELSE 1 
    END AS is_late_shipment,
    
    -- Engineered Features: Price & Profit
    ROUND(sales_amount / quantity, 2) AS price_per_unit,
    CASE 
        WHEN product_cost > 0 THEN ROUND((price - product_cost) / price * 100, 2)
        ELSE NULL 
    END AS profit_margin_pct,
    CASE 
        WHEN product_cost > 0 THEN ROUND((price - product_cost) * quantity, 2)
        ELSE NULL 
    END AS estimated_profit,
    
    -- Customer Demographics
    customer_key,
    first_name,
    last_name,
    customer_gender,
    customer_age,
    customer_age_group,
    customer_birth_date,
    marital_status,
    customer_country,
    customer_region,
    
    -- Product Attributes
    product_id,
    product_name,
    product_category,
    product_subcategory,
    product_line,
    product_cost,
    product_category_name,
    product_requires_maintenance,
    product_start_date,
    product_end_date,
    
    -- Engineered Features: Product Lifecycle
    DATEDIFF(order_date, product_start_date) AS product_age_days,
    CASE 
        WHEN product_end_date IS NULL THEN 1
        WHEN order_date <= product_end_date THEN 1
        ELSE 0
    END AS is_product_active,
    
    -- Null Indicators (useful for some ML models)
    CASE WHEN ship_date IS NULL THEN 1 ELSE 0 END AS missing_ship_date,
    CASE WHEN customer_age IS NULL THEN 1 ELSE 0 END AS missing_customer_age,
    CASE WHEN product_cost IS NULL OR product_cost = 0 THEN 1 ELSE 0 END AS missing_product_cost,
    
    -- Metadata
    current_timestamp() AS ml_processed_timestamp
    
FROM bike_store.gold.fact_sales

-- Data Quality Filters: Remove invalid transactions
WHERE 
    price > 0                    -- Filter out zero prices (data quality issue)
    AND sales_amount > 0         -- Filter out zero or negative sales
    AND quantity > 0             -- Filter out zero quantities
    AND order_date IS NOT NULL   -- Must have order date
