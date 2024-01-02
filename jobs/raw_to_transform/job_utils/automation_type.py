
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    automation_type_df = spark.sql("Select automation_code, automation_type_description,CAST( automation_code_id as DECIMAL(19,0)), CAST(automation_type_id as DECIMAL(19,0)), created_by,cast(as_of_date as DATE) as source_cycle_date, source_file_name,process_control_id,'IMPALA' as source_system,cast(created_timestamp as date) as created_timestamp  from pdab_automation_type_csv")

    transform_df = automation_type_df

    return transform_df