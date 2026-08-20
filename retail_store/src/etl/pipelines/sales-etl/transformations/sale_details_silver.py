from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os, sys

table_name = "sale_details"
bronze_schema = spark.conf.get("bronze_schema")
silver_schema = spark.conf.get("silver_schema")

# Dynamically add the shared directory to the Python path
shared_dir_path = spark.conf.get("shared_dir_path")
if shared_dir_path not in sys.path:
    sys.path.append(shared_dir_path)

# Import your shared rules module
import rules_fetcher

dp.create_streaming_table(
      name= f"{silver_schema}.{table_name}", 
      comment = f"cleaned {table_name} data loaded in silver layer",
      table_properties = {
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
      },
      expect_all_or_drop = rules_fetcher.get_rules("sales-etl", "silver_sale_details")
)


@dp.temporary_view(name = f"vw_{table_name}_bronze_stream")
def bronze_sale_details():
    return (
        spark.readStream.table(f"{bronze_schema}.sales")
       .withColumn("line_item", explode(col("line_items")))
       .selectExpr("transaction_id", "location_id", "created_date", "modified_date", "line_item.line_item_id", "line_item.product_id", "line_item.quantity", "line_item.unit_price", "line_item.discount_pct", "line_item.line_total", "current_timestamp() as processed_ts")
    )


dp.create_auto_cdc_flow(
    target = f"{silver_schema}.{table_name}",
    source = f"vw_{table_name}_bronze_stream",
    keys = ["transaction_id", "line_item_id"],
    sequence_by = col("modified_date"),
    # except_column_list = ["ingestion_ts"],
    stored_as_scd_type = 1
)

