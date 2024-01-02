
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    super_omni_newr_planwdrl_daily_ng_dat_df = spark.sql("select client_id, plan_number,withdrawal_type,withdrawal_code, 'VRP-PB' as source_system, cast(source_cycle_date as date), cast(distribution_fee as DECIMAL(11,4)),cast(null as DATE) as source_processing_date,  withdrawal_description from super_omni_newr_planwdrl_daily_ng_dat")

    super_omni_newr_planwdrl_daily_jb_dat_df = spark.sql("select client_id, plan_number,withdrawal_type,withdrawal_code, 'VRP-SP' as source_system, cast(source_cycle_date as date), cast(distribution_fee as DECIMAL(11,4)), cast(null as DATE) as source_processing_date,  withdrawal_description from super_omni_newr_planwdrl_daily_jb_dat")

    transform_df = super_omni_newr_planwdrl_daily_ng_dat_df.unionByName(super_omni_newr_planwdrl_daily_jb_dat_df)

    return transform_df