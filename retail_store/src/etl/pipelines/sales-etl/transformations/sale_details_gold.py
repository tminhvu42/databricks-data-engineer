from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

bronze_schema = spark.conf.get("bronze_schema")
silver_schema = spark.conf.get("silver_schema")
gold_schema = spark.conf.get("gold_schema")
source_table_name = "sales"
target_table_name = "sale_details"

dp.create_streaming_table(
      name= f"{gold_schema}.fact_{target_table_name}", 
      comment = f"{target_table_name} data scd type 2 data in gold layer",
      table_properties = {
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)

@dp.temporary_view(name = f"vw_{target_table_name}_silver_stream")
def sale_details_silver_stream():
    emp_df = spark.read.table("retail_store.gold.dim_employees").alias("e")
    customer_df = spark.read.table("retail_store.gold.dim_customers").alias("c")
    location_df = spark.read.table("retail_store.gold.dim_location").alias("l")
    product_df = spark.read.table(f"{gold_schema}.dim_products").alias("p")
    sales_df = (
      spark.readStream.table(f"{silver_schema}.{source_table_name}")
        .withColumn("transaction_date", col("transaction_date").cast(DateType()))
        .alias("s")
    )
    details_df = (
        spark.readStream.table(f"{silver_schema}.{target_table_name}")
        .withColumn("fact_sale_details_key", sha2(concat_ws("||", col("transaction_id"), col("line_item_id"), col("modified_date")), 256))
        .withColumn("gold_processed_ts", current_timestamp())
    )

    return (
        details_df.alias("d")
        .join(sales_df, on=[col("d.transaction_id") == col("s.transaction_id")], how="inner")
        .join(emp_df, on=[col("s.employee_id") == col("e.employee_id"), col("s.transaction_date") >= col("e.__START_AT"), col("s.transaction_date") < coalesce(col("e.__END_AT"), lit("9999-12-31"))], how="left")
        .join(customer_df, on=[col("s.customer_id") == col("c.customer_id"), col("s.transaction_date") >= col("c.__START_AT"), col("s.transaction_date") < coalesce(col("c.__END_AT"), lit("9999-12-31"))], how="left")
        .join(location_df, on=[col("s.location_id") == col("l.location_id"), col("s.transaction_date") >= col("l.__START_AT"), col("s.transaction_date") < coalesce(col("l.__END_AT"), lit("9999-12-31"))], how="left")
        .join(product_df, on=[col("d.product_id") == col("p.product_id"), col("s.transaction_date") >= col("p.__START_AT"), col("s.transaction_date") < coalesce(col("p.__END_AT"), lit("9999-12-31"))], how="left")
        .select(
            col("d.fact_sale_details_key"),
            col("d.transaction_id"),
            col("s.transaction_date"),
            coalesce(col("e.dim_employee_key"), lit(-1)).alias("dim_employee_key"),
            coalesce(col("c.dim_customer_key"), lit(-1)).alias("dim_customer_key"),
            coalesce(col("l.dim_location_key"), lit(-1)).alias("dim_location_key"),
            col("d.line_item_id"),
            col("p.dim_product_key"),
            col("d.quantity"),
            col("d.unit_price"),
            col("d.discount_pct"),
            col("d.line_total"),
            col("s.total_amount"),
            col("s.payment_type"),
            col("s.created_date"),
            col("s.modified_date"),
            col("d.gold_processed_ts")
        )
    )

dp.create_auto_cdc_flow(
    target = f"{gold_schema}.fact_{target_table_name}",
    source = f"vw_{target_table_name}_silver_stream",
    keys = ["transaction_id", "line_item_id"],
    sequence_by = col("modified_date"),
    # except_column_list = ["ingestion_ts"],
    stored_as_scd_type = 1
)