from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

table_name = "customers"
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
def customers_silver_stream():
  return (
      spark.readStream.table(f"{bronze_schema}.{table_name}")
        .withColumn("created_date", col("created_date").cast(DateType()))
        .withColumn("modified_date", col("modified_date").cast(DateType()))
        .withColumn("date_of_birth", col("date_of_birth").cast(DateType()))
        .withColumn("operation_date", col("operation_date").cast(DateType()))
        .withColumn("dim_customer_key", sha2(concat_ws("||", col("customer_id"), col("modified_date")), 256))
        .withColumn("gold_processed_ts", current_timestamp())
        .select("dim_customer_key", "customer_id", "first_name", "last_name", "email", "phone_number", "gender", "date_of_birth", "ssn", "preferred_contact_method", "status", "created_date", "modified_date", "address.address_line1", "address.address_line2", "address.city", "address.postal_code", "address.state", "address.country", "file_name", "operation", "operation_date", "gold_processed_ts")
  )

dp.create_auto_cdc_flow(
    target = f"{gold_schema}.dim_{table_name}",
    source = f"vw_{table_name}_silver_stream",
    keys = ["customer_id"],
    sequence_by = col("modified_date"),
    except_column_list = [ "operation", "operation_date"],
    stored_as_scd_type = 2
)