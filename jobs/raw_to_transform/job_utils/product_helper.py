
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    product_helper_df = spark.sql("SELECT cast(row_id as bigint) as row_id, source_system_name, source_system_field_name, source_system_code_value, cast(specification_id as bigint) as specification_id, specification_nm, creation_ts, cast(last_modified_ts as date) as last_modified_ts, created_timestamp, created_by, source_file_name, as_of_date as source_cycle_date, process_control_id from pdab_product_helper_csv")

    transform_df = product_helper_df

    return transform_df