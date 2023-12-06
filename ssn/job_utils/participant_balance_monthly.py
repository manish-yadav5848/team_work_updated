from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def transofrm(spark: SparkSession):
    df = spark.sql("select  client_id , plan_number , participant_id  , source_cycle_date , cast(sum(core_cash_value_amount)  as  decimal(17,2)) as core_cash_value_amount, cast(sum(ee_cash_value_amount)  as  decimal(17,2)) as er_cash_value_amount, cast(sum(life_cash_value_amount_monthly)  as  decimal(13,2)) as life_cash_value_amount_monthly, max(life_valuation_date) life_valuation_date, cast(sum(sdba_cash_value_amount)  as  decimal(17,2)) as sdba_cash_value_amount, cast(sum(loan_cash_value_amount)  as  decimal(17,2)) as loan_cash_value_amount, cast(sum(noncore_cash_value_amount)  as  decimal(13,2)) as noncore_cash_value_amount, cast(sum(ytd_contributions)  as  decimal(17,2)) as ytd_contributions from (select client_id , plan_number ,case when ssn.new_participant_id is null then pbm.participant_id else ssn.new_participant_id end as participant_id , source_cycle_date , core_cash_value_amount  ,  ee_cash_value_amount , er_cash_value_amount  , life_cash_value_amount_monthly  , life_valuation_date , sdba_cash_value_amount   , loan_cash_value_amount  , noncore_cash_value_amount , ytd_contributions  from newr_consumption.participant_balance_monthly pbm left outer join  (select old_participant_id,new_participant_id from  newr_transform.ssn_change_merge_helper where admin_tran_code in ('850','851') and concat(tran_year,tran_month,tran_day)=(select max(concat(tran_year,tran_month,tran_day)) from newr_transform.ssn_change_merge_helper)) ssn on pbm.participant_id=ssn.new_participant_id) group by client_id , plan_number , participant_id  , source_cycle_date ")
    return df