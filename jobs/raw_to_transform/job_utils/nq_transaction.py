
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    transaction_vnq_wc_txt_df = spark.sql("SELECT plan_number, participant_id, CAST(source_id as INTEGER),  fund_id, transaction_type_code, CAST(transaction_date as DATE), concat('D', newr_nqtxn_key) as newr_nqtxn_key, fund_name, fund_ticker, fund_cusip, investment_product_id, investment_product_name,  transaction_type_name, transaction_sub_type_code, transaction_sub_type_name,  CAST(number_units as DECIMAL(15,6)), CAST(unit_value as DECIMAL(13,6)),CAST( total_dollar_amount AS DECIMAL(11,2)), status_code,source_name, status_name, order_settled_code, order_settled_name, CAST(order_settled_date as DATE), buy_sell_code, buy_sell_name, CAST(trade_fee_amount as DECIMAL(11,2)),CAST(post_date as DATE),date_format(to_date(batch_date, 'yyyyMMdd'), 'yyyy-MM-dd') as source_cycle_date,'NQ' as source_system  FROM transaction_vnq_wc_txt where batch_date in (select distinct batch_date from transaction_vnq_wc_txt order by batch_date desc limit 5 )")

    transform_df = transaction_vnq_wc_txt_df

    return transform_df