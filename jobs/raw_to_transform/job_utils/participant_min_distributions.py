
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    super_omni_newr_plan_mindist_daily_ng_dat_df = spark.sql("SELECT client_id, plan_number, participant_id, CAST(begin_date as DATE) as rmd_begin_date, beneficiary_ssn, CAST(current_life_expectancy as DECIMAL(5,1)) as current_life_expectancy,CAST(distribution_due_date as DATE) as rmd_due_date,life_expectancy_calculation_method as life_expectancy_calc_method, CAST(non_applicable_current_year_distributions as DECIMAL(15,2)) as non_appl_curr_year_distrib, CAST(original_life_expectancy as decimal(4,1)) as original_life_expectancy,CAST(override_amount as DECIMAL(15,2)) as rmd_override_amount,CAST(rmd_amount as DECIMAL(15,2)) as rmd_amount,CAST(rmd_amount_ytd as DECIMAL(15,2)) as rmd_amount_ytd,CAST(shortfall_amount as DECIMAL(15,2)) as rmd_shortfall_amount,source_group, 'VRP-PB' as source_system ,cast(source_cycle_date as DATE) as source_cycle_date, cast(null as DATE) as source_processing_date FROM super_omni_newr_plan_mindist_daily_ng_dat") 

    exn_xxewre01_xxewrpmd_jb_df = spark.sql("select client_id, plan_number, participant_id, cast(begin_date as DATE) as rmd_begin_date, beneficiary_ssn, cast(current_life_expectancy as DECIMAL(5,1)) as current_life_expectancy, cast(distribution_due_date as DATE) as rmd_due_date, life_expectancy_calculation_method as life_expectancy_calc_method, cast(non_applicable_current_year_distributions as DECIMAL(15,2)) as non_appl_curr_year_distrib, cast(original_life_expectancy as DECIMAL(4,1)), cast(override_amount as DECIMAL(15,2)) as rmd_override_amount, cast(req_min_distribution_amount as DECIMAL(15,2)) as rmd_amount, cast(rmd_amount_ytd as DECIMAL(15,2)), cast(shortfall_amount as DECIMAL(15,2)) as rmd_shortfall_amount, source_group, 'VRP-SP' as source_system, cast(source_cycle_date as DATE), cast(null as DATE) as source_processing_date from exn_xxewre01_xxewrpmd_jb")

    transform_df = super_omni_newr_plan_mindist_daily_ng_dat_df.unionByName(exn_xxewre01_xxewrpmd_jb_df)

    return transform_df