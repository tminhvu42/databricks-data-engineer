from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *


table_name = "employees"
bronze_schema = spark.conf.get("bronze_schema")
gold_schema = spark.conf.get("gold_schema")

# Dynamically add the shared directory to the Python path
shared_dir_path = spark.conf.get("shared_dir_path")
if shared_dir_path not in sys.path:
    sys.path.append(shared_dir_path)

# Import your shared rules module
import rules_fetcher

@dp.temporary_view(name=f"vw_{table_name}_silver_stream")
def silver_stream():
    return (
        spark.readStream.option("skipChangeCommits", "true").table(f"{bronze_schema}.{table_name}")
        .withColumn("employee_id", col("employee_id").cast(IntegerType()))
        .withColumn("hire_date", col("hire_date").cast(DateType()))
        .withColumn("termination_date", col("termination_date").cast(DateType()))
        .withColumn("pay", col("pay").cast(DoubleType()))
        .withColumn("created_date", col("created_date").cast(DateType()))
        .withColumn("modified_date", col("modified_date").cast(DateType()))
        .withColumn("dim_employee_key", sha2(concat_ws("||", col("employee_id"), col("modified_date")), 256))
        .withColumn("gold_processed_ts", current_timestamp())
    )


dp.create_streaming_table(
    name = f"{gold_schema}.dim_{table_name}",
    comment = "employees table gold layer. SCD Type 2",
    table_properties = {
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)

dp.create_auto_cdc_flow(
    target = f"{gold_schema}.dim_{table_name}",
    source = f"vw_{table_name}_silver_stream",
    keys = ["employee_id"],
    sequence_by = col("modified_date"),
    except_column_list = ["_rescued_data", "year", "month", "day", "ingestion_ts"],
    stored_as_scd_type = 2
)


