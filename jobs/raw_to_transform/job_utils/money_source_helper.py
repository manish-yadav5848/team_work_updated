
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    money_source_helper_df = spark.sql("SELECT  row_id, comp_cd, asset_acct, user_area, code_name, pwe_money_source_code, pwe_money_source_name, source_cd, money_type_code, source_system, to_date(substring(created_timestamp,1,10)) as created_timestamp,created_by, source_file_name, cast (null as  date) as source_cycle_date, process_control_id from pdab_money_source_helper_csv")

    transform_df = money_source_helper_df

    return transform_df