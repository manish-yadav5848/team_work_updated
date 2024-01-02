
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    plan_vnq_wc_txt_df = spark.sql("SELECT client_id, plan_number, name, type_code, type_name, status_code, status_name, start_date, end_date, date_format(to_date(batch_date, 'yyyyMMdd'), 'yyyy-MM-dd') as source_cycle_date,'NQ' as source_system  FROM plan_vnq_wc_txt")

    transform_df = plan_vnq_wc_txt_df

    return transform_df