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
