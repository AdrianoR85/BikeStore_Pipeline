# Bronze Layer - All Source Code

## CRM Files

### source_paths_crm.py
```python
import sys
import os
from pathlib import Path

# Get the current user home path
user_home = os.path.expanduser("~")
sys.path.append(f"{user_home}/Project_bikestore")

from config import CRM

PATHS = {
    "crm_customer": f"{CRM['cst_info_path']}{CRM['cst_info_file']}",
    "crm_product": f"{CRM['prd_info_path']}{CRM['prd_info_file']}",
    "crm_sales": f"{CRM['sales_details_path']}{CRM['sales_details_file']}"
}
```

### crm_customer.py
```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from source_paths_crm import PATHS


@dp.materialized_view(
    name="bike_store.bronze.crm_customer",
    comment="Customer Raw data from the CRM sales system",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }

)
def crm_customer_bronze():
    df = spark.read.format("csv")\
        .option("header", True)\
        .option("inferSchema", "true")\
        .option("mode", "PERMISSIVE")\
        .option("mergeSchema", "true")\
        .option("columnNameOfCorruptRecord", "corrupt_record")\
        .load(PATHS["crm_customer"])
    
    df = (
        df
        .withColumn("file_name", col("_metadata.file_path"))
        .withColumn("ingest_datetime", current_timestamp())
    )

    return df
```

### crm_product.py
```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from source_paths_crm import PATHS

@dp.materialized_view(
    name="bike_store.bronze.crm_product",
    comment="Product Raw data from the CRM sales system",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
 )
def crm_product_bronze():
    df = spark.read.format("csv")\
        .option("header", True)\
        .option("inferSchema", "true")\
        .option("mode", "PERMISSIVE")\
        .option("mergeSchema", "true")\
        .option("columnNameOfCorruptRecord", "corrupt_record")\
        .load(PATHS["crm_product"])
    
    df = (
        df
        .withColumn("file_name", col("_metadata.file_path"))
        .withColumn("ingest_datetime", current_timestamp())
    )
    
    return df
```

### crm_sales.py
```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, when
from source_paths_crm import PATHS

@dp.materialized_view(
    name="bike_store.bronze.crm_sales",
    comment="Sales Raw data from the CRM sales system",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def crm_sales_bronze():
    df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .option("mode", "PERMISSIVE")\
    .option("mergeSchema", "true")\
    .option("columnNameOfCorruptRecord", "corrupt_record")\
    .load(PATHS["crm_sales"])

    df = (
        df
        .withColumn("file_name", col("_metadata.file_path"))
        .withColumn("ingest_datetime", current_timestamp())
    )

    return df
```

## ERP Files

### source_paths_erp.py
```python
import sys
import os
from pathlib import Path

# Get the current user home path
user_home = os.path.expanduser("~")
sys.path.append(f"{user_home}/Project_bikestore")

from config import ERP

PATHS = {
    "erp_customer": f"{ERP['cust_az12_path']}{ERP['cust_az12_file']}",
    "erp_location": f"{ERP['loc_a101_path']}{ERP['loc_a101_file']}",
    "erp_category": f"{ERP['px_cat_g1v2_path']}{ERP['px_cat_g1v2_file']}"
}
```

### erp_customer.py
```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from source_paths_erp import PATHS

@dp.materialized_view(
    name="bike_store.bronze.erp_customer",
    comment="Customer Raw data from the ERP sales system",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
 )
def erp_customer_bronze():
    df = spark.read.format("csv")\
        .option("header", True)\
        .option("inferSchema", "true")\
        .option("mode", "PERMISSIVE")\
        .option("mergeSchema", "true")\
        .option("columnNameOfCorruptRecord", "corrupt_record")\
        .load(PATHS["erp_customer"])
    
    df = (
        df
        .withColumn("file_name", col("_metadata.file_path"))
        .withColumn("ingest_datetime", current_timestamp())
    )
    
    return df
```

### erp_location.py
```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from source_paths_erp import PATHS

@dp.materialized_view(
    name="bike_store.bronze.erp_location",
    comment="Location Raw data from the ERP sales system",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
 )
def erp_location_bronze():
    df = spark.read.format("csv")\
        .option("header", True)\
        .option("inferSchema", "true")\
        .option("mode", "PERMISSIVE")\
        .option("mergeSchema", "true")\
        .option("columnNameOfCorruptRecord", "corrupt_record")\
        .load(PATHS["erp_location"])
    
    df = (
        df
        .withColumn("file_name", col("_metadata.file_path"))
        .withColumn("ingest_datetime", current_timestamp())
    )
    
    return df
```

### erp_category.py
```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from source_paths_erp import PATHS

@dp.materialized_view(
    name="bike_store.bronze.erp_category",
    comment="Category Raw data from the ERP sales system",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
 )
def erp_category_bronze():
    df = spark.read.format("csv")\
        .option("header", True)\
        .option("inferSchema", "true")\
        .option("mode", "PERMISSIVE")\
        .option("mergeSchema", "true")\
        .option("columnNameOfCorruptRecord", "corrupt_record")\
        .load(PATHS["erp_category"])
    
    df = (
        df
        .withColumn("file_name", col("_metadata.file_path"))
        .withColumn("ingest_datetime", current_timestamp())
    )
    
    return df
```

[Back Home](../../../../../../../BikeStore_Pipeline)
