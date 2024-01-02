
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    dcs_dtcc_pov_far_agent_dat_df = spark.sql("select  cyclde_Date, action_Code, producer_role_type, producer_role_code_description,, cast(role_split_percent as DECUNAK(5,2)), pais_entity_key, agency_agent_code, producer_tax_id, producer_last_name, producer_first_name, producer_middle_ini, producer_suffix, full_name_field, full_name_field,plan_id as plan_number, plan_source, participant_key, participant_money_source, participant_acct_number as participant_id, participant_level_code, hierarchy_pais_entity_key, hierarchy_tax_id, hierarchy_last_name, hierarchy_first_name, participant_money_source, participant_acct_number as participant_id, participant_level_code, hierarchy_pais_entity_key, hierarchy_tax_id, hierarchy_last_name, hierarchy_first_name, hierarchy_middle_init, hierarchy_suffix, hierarchy_full_name_field, npn_number, crd_number, hierarchy_middle_init, hierarchy_suffix, hierarchy_full_name_field, npn_number, crd_number from  dcs_dtcc_pov_far_agent_dat")

    transform_df = dcs_dtcc_pov_far_agent_dat_df

    return transform_df