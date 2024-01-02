
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    stars_daily_contact_dat_df = spark.sql("SELECT plan_id AS plan_number,source_system,crm_name,crm_email,crm_phone,trustee_name,trustee_email,trustee_phone FROM stars_daily_contact_dat")

    transform_df = stars_daily_contact_dat_df

    return transform_df