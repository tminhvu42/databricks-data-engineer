from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

table_name = "products"
bronze_schema = spark.conf.get("bronze_schema")
silver_schema = spark.conf.get("silver_schema")
gold_schema = spark.conf.get("gold_schema")

dp.create_streaming_table(
      name= f"{gold_schema}.dim_{table_name}", 
      comment = f"{table_name} data scd type 2 data in gold layer",
      table_properties = {
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)

@dp.temporary_view(name = f"vw_{table_name}_silver_stream")
def product_silver_stream():
  return (
      spark.readStream.table(f"{bronze_schema}.{table_name}")
        .withColumn("created_date", col("created_date").cast(DateType()))
        .withColumn("modified_date", col("modified_date").cast(DateType()))
        .withColumn("dim_product_key", sha2(concat_ws("||", col("product_id"), col("modified_date")), 256))
        .withColumn("gold_processed_ts", current_timestamp())
  )

dp.create_auto_cdc_flow(
    target = f"{gold_schema}.dim_{table_name}",
    source = f"vw_{table_name}_silver_stream",
    keys = ["product_id"],
    sequence_by = col("modified_date"),
    except_column_list = ["_rescued_data", "ingestion_ts"],
    stored_as_scd_type = 2
)