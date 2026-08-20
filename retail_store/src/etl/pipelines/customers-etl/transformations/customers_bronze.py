from pyspark import pipelines as dp
from pyspark.sql.functions import *

table_name = "customers"
path = f"{spark.conf.get("source_path")}/{table_name}/"
schema_path = f"{spark.conf.get("source_path")}/schemas/{table_name}/"
schema = spark.conf.get("bronze_schema")

dp.create_streaming_table(
  name = f"{schema}.{table_name}",
  comment = f"new {table_name} data incrementally ingested from cloud object storage landing zone"
)

@dp.append_flow(
  target = f"{schema}.{table_name}",
  name = f"{table_name}_bronze_ingestion_flow"
)
def customers_bronze_ingestion_flow():
  return (
    spark.readStream.format("cloudFiles")
         .option("cloudFiles.format", "json")   
         .option("multiLine", "true")              
         .option("cloudFiles.inferColumnTypes", "true")
         .option("cloudFiles.schemaLocation", schema_path)
         .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
         .option("cloudFiles.includeExistingFiles", "true")
         .load(path)
         .withColumn("ingestion_ts", current_timestamp())
         .withColumn("file_name", col("_metadata.file_path"))
         
  )