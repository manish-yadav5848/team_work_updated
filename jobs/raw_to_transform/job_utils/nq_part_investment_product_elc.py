
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    participant_investment_product_election_vnq_wc_txt_df = spark.sql("SELECT plan_number, participant_id, investment_product_id, CAST(source_id as INTEGER), source_name,  investment_product_name, cast(allocation_percent as DECIMAL(5,2)), cast(effective_date as DATE), date_format(to_date(batch_date, 'yyyyMMdd'), 'yyyy-MM-dd') as source_cycle_date,'NQ' as source_system  FROM participant_investment_product_election_vnq_wc_txt")

    transform_df = participant_investment_product_election_vnq_wc_txt_df

    return transform_df