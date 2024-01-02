from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    super_omni_newr_partgaabal_monthly_ng_dat_df = spark.sql("select client_id,participant_id, plan_number, 'VRP-PB' as source_system, source_cycle_date,fund_number, cast(all_current_gaa_adjusted_amount as DECIMAL(17,2)) as all_curr_gaa_adjusted_amount, cast(all_current_gaa_adjusted_surrender_amount as DECIMAL(17,2)) as all_curr_gaa_adj_surrender_amt,  cast(all_current_gaa_mva_amount as DECIMAL(17,2)), cast(current_gaa_contract_amount as DECIMAL(17,2)), plan_end_year, pm_oltpln_issue_state, cast(surrender_penalty_amt as DECIMAL(17,2)) as surrender_penalty_amt from super_omni_newr_partgaabal_monthly_ng_dat")

    transform_df = super_omni_newr_partgaabal_monthly_ng_dat_df

    return transform_df