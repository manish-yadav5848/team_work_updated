
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    market_segment_df = spark.sql("SELECT company_code, market_indicator, market_code, market_segment_code, market_segment_category_desc, created_timestamp, created_by, source_file_name, source_cycle_date,process_control_id,'IMPALA' as source_system FROM pdab_market_segment_csv")

    transform_df = market_segment_df

    return transform_df