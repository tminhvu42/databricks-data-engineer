from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

bronze_schema = spark.conf.get("bronze_schema")
silver_schema = spark.conf.get("silver_schema")
gold_schema = spark.conf.get("gold_schema")
table_name = "v_sales_summary"


@dp.materialized_view(
  name= f"{gold_schema}.{table_name}",
  comment="Precomputed sales aggregations"
)
def sales_summary():
    return (
        spark.read.table(f"{gold_schema}.fact_sale_details")
        .groupBy(["transaction_id", "transaction_date", "dim_customer_key", "dim_location_key", "dim_product_key", "payment_type"])
        .agg(
            sum("line_total").alias("total_amount"),
            sum("quantity").alias("total_quantity"),
            count("*").alias("line_item_count")
            )
    )
