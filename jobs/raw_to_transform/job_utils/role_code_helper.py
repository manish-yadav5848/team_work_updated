
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    role_code_helper_df = spark.sql("SELECT row_id, source_system_nm, source_system_field_nm, source_system_field_value, source_system_field_value_txt, creation_ts, cast(last_modified_ts as date) as last_modified_ts, created_timestamp, created_by, source_file_name, as_of_date as source_cycle_date, process_control_id from pdab_role_code_helper_csv")

    transform_df = role_code_helper_df

    return transform_df