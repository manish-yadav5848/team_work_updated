from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    nw00paap_nlarc_prdps_pdab_sffa_data_dat01_df = spark.sql("SELECT fund_number,plan_number,money_source as source,policy_number as participant_id,soc_sec_no as social_security_number,allocation_pct,system_code,company_code,process_date,'PREMIER' as source_system  FROM nw00paap_nlarc_prdps_pdab_sffa_data")

    transform_df = nw00paap_nlarc_prdps_pdab_sffa_data_dat01_df

    return transform_df