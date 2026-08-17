from pyspark import pipelines as dp
from pyspark.sql.functions import *

# Replace with the catalog and schema name that
# you are using:
path = spark.conf.get("source_path")
schema = spark.conf.get("bronze_schema")


dp.create_streaming_table(name= f"{schema}.employees", comment = "new employee data incrementally ingested from cloud object storage landing zone")

@dp.append_flow(
    target = f"{schema}.employees",
    name = "employees_bronze_ingest_flow"
)
def employees_bronze_ingest_flow():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")        
        .option("cloudFiles.schemaLocation", path)
        .option("cloudFiles.includeExistingFiles", "true")
        # .option("cloudFiles.partitionColumns", "")  # Disables the generation of year, month, day columns
        .load(path)
        .withColumn("ingestion_ts", current_timestamp())
    )