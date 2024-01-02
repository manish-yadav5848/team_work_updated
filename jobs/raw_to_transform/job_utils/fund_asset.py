
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    fo_asset_txt_df = spark.sql("select coalesce(asset_code,-9999) as asset_code,asset_name,risk_rtn,asset_class_desc,asset_class_type from fo_asset_txt")

    transform_df = fo_asset_txt_df

    return transform_df