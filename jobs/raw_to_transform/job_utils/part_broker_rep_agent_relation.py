
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    f912pb_j912dol_p912dol5_prdcrext_lseq_df = spark.sql("SELECT substr(cycle_date,0,10) as source_cycle_date, role_key, producer_role_type, producer_role_code_description, cast(role_split_percent as DECIMAL(5,2)), pais_entity_key, agency_agent_code, producer_tax_id, producer_last_name, producer_first_name, producer_middle_init, producer_suffix, full_name_field, plan_number, plan_source, participant_key, participant_money_source, participant_level_code, participant_id, hierarchy_pais_entity_key, hierarchy_tax_id, hierarchy_last_name, hierarchy_first_name, hierarchy_middle_init, hierarchy_suffix, hierarchy_full_name_field, npn_number, crd_number, broker_id from f912pb_j912dol_p912dol5_prdcrext_lseq")

    transform_df = f912pb_j912dol_p912dol5_prdcrext_lseq_df

    return transform_df