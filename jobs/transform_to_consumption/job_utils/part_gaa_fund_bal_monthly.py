
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, batch_date: str, primary_key: list):

    consumption_df = spark.sql("SELECT t2.client_id, t2.fund_number, t2.participant_id, t2.plan_number, t2.source_cycle_date, t2.all_curr_gaa_adjusted_amount, t2.all_curr_gaa_adj_surrender_amt, t2.all_current_gaa_mva_amount, t2.current_gaa_contract_amount, t2.surrender_penalty_amt, pb.plan_end_year, pb.participant_key, pb.plan_fund_key, pb.pm_oltpln_issue_state FROM ( select t1.client_id, t1.fund_number, t1.participant_id, t1.plan_number, cast( SUM(t1.all_curr_gaa_adjusted_amount) as DECIMAL(17, 2)) as all_curr_gaa_adjusted_amount, cast(SUM(t1.all_curr_gaa_adj_surrender_amt) as DECIMAL(17, 2)) as all_curr_gaa_adj_surrender_amt, cast( SUM(t1.all_current_gaa_mva_amount) as DECIMAL(17, 2)) as all_current_gaa_mva_amount, cast( SUM(t1.current_gaa_contract_amount) as DECIMAL(17, 2)) as current_gaa_contract_amount, cast(SUM(t1.surrender_penalty_amt) as DECIMAL(17, 2)) as surrender_penalty_amt, coalesce(t1.source_cycle_date,current_date()) as source_cycle_date FROM ( SELECT coalesce(pgfb.client_id, '-9999') as client_id, coalesce(pgfb.fund_number, '-9999') as fund_number, coalesce(pgfb.participant_id, '-9999') as participant_id, coalesce(pgfb.plan_number, '-9999') as plan_number, coalesce(pgfb.source_cycle_date, current_date() -1) as source_cycle_date, coalesce(pgfb.current_gaa_contract_amount, 0) as current_gaa_contract_amount, coalesce(pgfb.all_current_gaa_mva_amount, 0) as all_current_gaa_mva_amount, coalesce(pgfb.all_curr_gaa_adj_surrender_amt, 0) as all_curr_gaa_adj_surrender_amt, coalesce(pgfb.all_curr_gaa_adjusted_amount, 0) as all_curr_gaa_adjusted_amount, ch.mo_last_business_day, coalesce(pgfb.surrender_penalty_amt, 0) as surrender_penalty_amt FROM participant_gaa_fund_balance as pgfb inner join calendar_helper as ch ON coalesce(pgfb.source_cycle_date, current_date() -1) = coalesce(ch.mo_last_business_day, current_date() -1)) as t1 group by t1.client_id, t1.fund_number, t1.participant_id, t1.plan_number, t1.source_cycle_date) as t2 left outer join participant_gaa_fund_balance as pb ON coalesce(t2.client_id, '-9999') = coalesce(pb.client_id, '-9999') AND coalesce(t2.plan_number, '-9999') = coalesce(pb.plan_number, '-9999') AND coalesce(t2.participant_id, '-9999') = coalesce(pb.participant_id, '-9999') AND coalesce(t2.fund_number, '-9999') = coalesce(pb.fund_number, '-9999')")

    return consumption_df