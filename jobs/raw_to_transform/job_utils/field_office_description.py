
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    field_office_description_df = spark.sql("select agency_code,market_segment_code,field_office_description from fieldoffice_table_rs_raw_csv")

    transform_df = field_office_description_df

    return transform_df