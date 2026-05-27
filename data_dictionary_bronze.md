
# DATA DICTIONARY - BIKE STORE
## Schema: bike_store.bronze
Generated on: 2026-05-18 17:15:12

[Back Home](#./Project_bikestore)

---

## 📋 SCHEMA SUMMARY

**Catalog:** bike_store
**Schema:** bronze  
**Total Tables:** 6
**Type:** Materialized Views (Bronze Layer)
**CRM (3 tables)**: Customer, Product, Sales
**ERP (3 tables)**: Customer, Category, Location

---

## 📊 TABLES

### 1. bike_store.bronze.crm_customer
**Description:** Customer Raw data from the CRM sales system  
**Type:** MATERIALIZED_VIEW  
**Source System:** CRM

| # | Column | Type | Nullable | Description |
|---|--------|------|----------|-------------|
| 1 | cst_id | INT | Yes | Customer unique ID |
| 2 | cst_key | STRING | Yes | Customer identification key |
| 3 | cst_firstname | STRING | Yes | Customer first name |
| 4 | cst_lastname | STRING | Yes | Customer last name |
| 5 | cst_marital_status | STRING | Yes | Customer marital status |
| 6 | cst_gndr | STRING | Yes | Customer gender |
| 7 | cst_create_date | DATE | Yes | Record creation date |
| 8 | file_name | STRING | Yes | Source file name |
| 9 | ingest_datetime | TIMESTAMP | Yes | Data ingestion date/time |

---

### 2. bike_store.bronze.crm_product
**Description:** Product Raw data from the CRM sales system  
**Type:** MATERIALIZED_VIEW  
**Source System:** CRM

| # | Column | Type | Nullable | Description |
|---|--------|------|----------|-------------|
| 1 | prd_id | INT | Yes | Product unique ID |
| 2 | prd_key | STRING | Yes | Product identification key |
| 3 | prd_nm | STRING | Yes | Product name |
| 4 | prd_cost | INT | Yes | Product cost |
| 5 | prd_line | STRING | Yes | Product line |
| 6 | prd_start_dt | DATE | Yes | Product start date |
| 7 | prd_end_dt | DATE | Yes | Product end date |
| 8 | file_name | STRING | Yes | Source file name |
| 9 | ingest_datetime | TIMESTAMP | Yes | Data ingestion date/time |

---

### 3. bike_store.bronze.crm_sales
**Description:** Sales Raw data from the CRM sales system  
**Type:** MATERIALIZED_VIEW  
**Source System:** CRM

| # | Column | Type | Nullable | Description |
|---|--------|------|----------|-------------|
| 1 | sls_ord_num | STRING | Yes | Sales order number |
| 2 | sls_prd_key | STRING | Yes | Product key (sold product) |
| 3 | sls_cust_id | INT | Yes | Customer ID |
| 4 | sls_order_dt | INT | Yes | Order date (numeric format) |
| 5 | sls_ship_dt | INT | Yes | Ship date (numeric format) |
| 6 | sls_due_dt | INT | Yes | Due date (numeric format) |
| 7 | sls_sales | INT | Yes | Sales amount |
| 8 | sls_quantity | INT | Yes | Quantity sold |
| 9 | sls_price | INT | Yes | Unit price |
| 10 | file_name | STRING | Yes | Source file name |
| 11 | ingest_datetime | TIMESTAMP | Yes | Data ingestion date/time |

---

### 4. bike_store.bronze.erp_category
**Description:** Category Raw data from the ERP sales system  
**Type:** MATERIALIZED_VIEW  
**Source System:** ERP

| # | Column | Type | Nullable | Description |
|---|--------|------|----------|-------------|
| 1 | ID | STRING | Yes | Category identifier |
| 2 | CAT | STRING | Yes | Main category |
| 3 | SUBCAT | STRING | Yes | Subcategory |
| 4 | MAINTENANCE | STRING | Yes | Maintenance indicator |
| 5 | file_name | STRING | Yes | Source file name |
| 6 | ingest_datetime | TIMESTAMP | Yes | Data ingestion date/time |

---

### 5. bike_store.bronze.erp_customer
**Description:** Customer Raw data from the ERP sales system  
**Type:** MATERIALIZED_VIEW  
**Source System:** ERP

| # | Column | Type | Nullable | Description |
|---|--------|------|----------|-------------|
| 1 | CID | STRING | Yes | Customer ID from ERP system |
| 2 | BDATE | DATE | Yes | Customer birth date |
| 3 | GEN | STRING | Yes | Customer gender |
| 4 | file_name | STRING | Yes | Source file name |
| 5 | ingest_datetime | TIMESTAMP | Yes | Data ingestion date/time |

---

### 6. bike_store.bronze.erp_location
**Description:** Location Raw data from the ERP sales system  
**Type:** MATERIALIZED_VIEW  
**Source System:** ERP

| # | Column | Type | Nullable | Description |
|---|--------|------|----------|-------------|
| 1 | CID | STRING | Yes | Customer ID |
| 2 | CNTRY | STRING | Yes | Customer country |
| 3 | file_name | STRING | Yes | Source file name |
| 4 | ingest_datetime | TIMESTAMP | Yes | Data ingestion date/time |

---

## 🔗 IDENTIFIED RELATIONSHIPS

### CRM System
- **crm_customer.cst_id** → **crm_sales.sls_cust_id** (Customer → Sales)
- **crm_product.prd_key** → **crm_sales.sls_prd_key** (Product → Sales)

### ERP System
- **erp_customer.CID** → **erp_location.CID** (Customer → Location)

### Cross-System (Potential)
- **crm_customer.cst_key** ↔ **erp_customer.CID** (CRM-ERP Integration)

---

## 📈 SCHEMA METRICS

- **crm_customer**: 18,494 records
- **crm_product**: 397 records
- **crm_sales**: 60,398 records
- **erp_category**: 37 records
- **erp_customer**: 18,484 records
- **erp_location**: 18,484 records
