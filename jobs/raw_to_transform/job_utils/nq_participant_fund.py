
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    participant_fund_vnq_wc_txt_df = spark.sql("SELECT plan_number, participant_id, cast(source_id as DECIMAL(38,0)) as source_id, fund_id, source_name,  fund_ticker, fund_cusip, fund_name, cast(total_shares as DECIMAL(18,6)) as total_shares, cast(total_balance as DECIMAL(18,2)) as total_balance,date_format(to_date(batch_date, 'yyyyMMdd'), 'yyyy-MM-dd') as source_cycle_date,'NQ' as source_system  FROM participant_fund_vnq_wc_txt")

    transform_df = participant_fund_vnq_wc_txt_df

    return transform_df