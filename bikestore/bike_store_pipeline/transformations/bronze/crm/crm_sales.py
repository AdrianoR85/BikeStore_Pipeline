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