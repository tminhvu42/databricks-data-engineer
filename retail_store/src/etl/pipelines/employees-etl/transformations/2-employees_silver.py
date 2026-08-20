from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *
from utilities.rules_module import *

# def get_rules(tag):
#   """
#     loads data quality rules from a table
#     :param tag: tag to match
#     :return: dictionary of rules that matched the tag
#   """
#   return {
#     row['name']: row['constraint']
#     for row in get_rules_as_list_of_dict()
#     if row['tag'] == tag
#   }

table_name = "employees"
bronze_schema = spark.conf.get("bronze_schema")
silver_schema = spark.conf.get("silver_schema")

dp.create_streaming_table(
    name = f"{silver_schema}.{table_name}",
    comment = f"{table_name} table in silver layer. SCD Type 1",
    table_properties = {
      "delta.autoOptimize.optimizeWrite": "true",
      "delta.autoOptimize.autoCompact": "true"
    },
    expect_all_or_drop = rules_fetcher.get_rules("employees-etl", "silver_employees")
)

@dp.temporary_view(name = f"vw_{table_name}_bronze_stream")
def bronze_employees_stream():
    return (
        spark.readStream.option("skipChangeCommits", "true").table(f"{bronze_schema}.{table_name}")
        .withColumn("employee_id", col("employee_id").cast(IntegerType()))
        .withColumn("hire_date", col("hire_date").cast(DateType()))
        .withColumn("termination_date", col("termination_date").cast(DateType()))
        .withColumn("pay", col("pay").cast(DoubleType()))
        .withColumn("created_date", col("created_date").cast(DateType()))
        .withColumn("modified_date", col("modified_date").cast(DateType()))
        .withColumn("processed_ts", current_timestamp())
    )


dp.create_auto_cdc_flow(
    target = f"{silver_schema}.{table_name}",
    source = f"vw_{table_name}_bronze_stream",
    keys = ["employee_id"],
    sequence_by = col("modified_date"),
    except_column_list = ["_rescued_data", "year", "month", "day", "ingestion_ts"],
    stored_as_scd_type = 1
)



