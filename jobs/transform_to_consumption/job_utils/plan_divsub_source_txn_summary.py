
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, batch_date: str, primary_key: list):

    consumption_df = spark.sql("SELECT coalesce(T2.plan_number,'-9999') as plan_number,case when div_sub_id='' then '-9999' else div_sub_id end as div_sub_id,case when source='' then '-9999' else source end as source,coalesce(T2.base_transaction_code,'-9999') as base_transaction_code,coalesce(T2.run_date,current_date()-1) as run_date,coalesce(T2.trade_date,current_date()-1) as trade_date,coalesce(T2.total_cash_amount,0.00) as total_cash_amount,coalesce(T2.total_unit_shares,0.00) as total_unit_shares,coalesce(T2.total_share_price,0.00) as total_share_price from (SELECT T1.plan_number,T1.div_sub_id,T1.money_source as source,T1.base_transaction_code,T1.run_date,T1.trade_date,cast(T1.total_cash_amount as DECIMAL(17,2)) as total_cash_amount,cast(T1.total_unit_shares as DECIMAL(17,2)) as total_unit_shares,cast(T1.total_share_price as DECIMAL(17,2)) as total_share_price from (SELECT plan_number,div_sub_id,money_source,base_transaction_code,run_date,trade_date,sum(cash) as total_cash_amount,sum(shares) as total_unit_shares,sum(share_price) as total_share_price from (SELECT plan_number,div_sub_id,money_source,base_transaction_code,run_date,trade_date,coalesce(cash,0.0) as cash,coalesce(shares,0.0) as shares,coalesce(share_price,0.0) as share_price from txn) group by plan_number,div_sub_id,money_source,base_transaction_code,run_date,trade_date) T1 left outer join    calendar_helper as ch   on coalesce(t1.run_date,current_date()-1) = coalesce(ch.mo_last_business_day,current_date()-1)   where mo_last_business_day is null) T2")

    return consumption_df