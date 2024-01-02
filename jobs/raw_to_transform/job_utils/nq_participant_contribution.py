
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    participant_contribution_vnq_wc_txt_df = spark.sql("SELECT plan_number, participant_id, source_id, fund_id, source_name,  fund_ticker, fund_cusip, fund_name, amount, date_format(to_date(batch_date, 'yyyyMMdd'), 'yyyy-MM-dd') as source_cycle_date,'NQ' as source_system FROM participant_contribution_vnq_wc_txt ;")

    transform_df = participant_contribution_vnq_wc_txt_df

    return transform_df