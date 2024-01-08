from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    cust_adv_helper_df = spark.sql("SELECT cust_adv_program,cust_adv_program_name,'IMPALA' as source_system from cust_adv_helper")

    transform_df = cust_adv_helper_df

    return transform_df