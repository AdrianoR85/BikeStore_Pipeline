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
