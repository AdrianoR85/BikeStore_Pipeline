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
def crm_product_bronze():
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