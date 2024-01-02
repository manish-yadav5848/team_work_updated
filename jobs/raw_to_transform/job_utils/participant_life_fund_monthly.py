
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    super_omni_newr_lifeval_monthly_ng_dat_df = spark.sql("SELECT  fund_number, plan_id as plan_number, participant_account_number as participant_id ,cast(source_cycle_date as DATE), cast(valuation_date as DATE), action_code, plan_system_key, 'VRP-PB' as source_system, participant_agreement_system_key, participant_agreement_source_system_key, div_sub_id,life_insurance_policy_system_key as life_insur_policy_system_key, life_insurance_policy_number,  carrier_name, fund_name, CAST(cash_value_amt as DECIMAL(11,2)) as cash_value_amt, CAST(life_surr_val_amt as DECIMAL(11,2)) as life_insurance_surrender_value, CAST(death_ben_val_amt as DECIMAL(11,2)) as life_insur_death_benefit_val, error_code FROM super_omni_newr_lifeval_monthly_ng_dat where cash_value_amt>0")

    transform_df = super_omni_newr_lifeval_monthly_ng_dat_df

    return transform_df