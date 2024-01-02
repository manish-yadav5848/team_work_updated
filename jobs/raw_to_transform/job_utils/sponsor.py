
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    sponsor_df = spark.sql("SELECT row_id, created_timestamp,source_system_nm as source_system,source_system_field_nm,source_system_field_value,source_system_field_value_txt,creation_ts,last_modified_ts,created_by,source_file_name,as_of_date as source_cycle_date,process_control_id from pdab_sponsor_csv")

    transform_df = sponsor_df

    return transform_df