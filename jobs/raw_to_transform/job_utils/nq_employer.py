
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    employer_vnq_wc_txt_df = spark.sql("SELECT employer_ein, employer_id, client_id, name, street_address_1, street_address_2, city, state, foreign_state_address, zip, country,date_format(to_date(batch_date, 'yyyyMMdd'), 'yyyy-MM-dd') as source_cycle_date,'NQ' as source_system FROM employer_vnq_wc_txt ;")

    transform_df = employer_vnq_wc_txt_df

    return transform_df