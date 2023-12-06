
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, batch_date: str, primary_key: list):

    consumption_df = spark.sql("select t1.plan_number,t1.participant_id,t1.client_id,t1.run_date,t1.trade_date,coalesce(t2.activity_type,'-9999') as activity_type,coalesce(t2.base_transaction_code,'-9999') as base_transaction_code,t1.total_cash_amount,t1.total_share_price,t1.total_unit_shares,coalesce(t2.rev_code,'-9999') as rev_code from (select coalesce(plan_number,'-9999') as plan_number,coalesce(participant_id,'-9999') as participant_id,coalesce(client_id,'-9999') as client_id,coalesce(cast(total_cash_amount as decimal(17,2)),0.00) as total_cash_amount,coalesce(run_date,current_date()) as run_date,coalesce(trade_date,current_date()) as trade_date,coalesce(cast(total_share_price as decimal(17,2)),0.00) as total_share_price,coalesce(cast(total_unit_shares as decimal(17,2)),0.00) as total_unit_shares  from (select p.plan_number,p.client_id,p.participant_id,tx.total_cash_amount,tx.total_unit_shares,tx.total_share_price,tx.run_date,tx.trade_date from participant p left outer join (select t.plan_number,t.participant_id,t.run_date,t.trade_date,sum(t.cash) as total_cash_amount,sum(t.shares) as total_unit_shares,sum(t.share_price) as total_share_price from txn t group by t.plan_number,t.participant_id,t.run_date,t.trade_date ) tx on coalesce(p.plan_number,'-9999') = coalesce(tx.plan_number,'-9999') and coalesce(p.participant_id,'-9999') = coalesce(tx.participant_id,'-9999'))) t1 left outer join txn t2 on coalesce(t1.plan_number,'-9999') = coalesce(t2.plan_number,'-9999') and coalesce(t1.participant_id,'-9999') = coalesce(t2.participant_id,'-9999') left outer join (select mo_last_business_day from calendar_helper) calh on t2.run_date = calh.mo_last_business_day  where calh.mo_last_business_day is null")

    return consumption_df