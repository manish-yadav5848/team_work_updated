
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    exn_xxadsumt_xxadhsbx_jb_df = spark.sql("SELECT client_id,plan_number,div_sub_id,fund_id,cast(source_cycle_date as DATE) as source_cycle_date,'VRP-SP' as source_system,cast(contribution_balance as DECIMAL(17,2)) as contribution_balance,cast(termination_balance as DECIMAL(17,2)) as termination_balance,cast(withdrawal_balance as DECIMAL(17,2)) as withdrawal_balance FROM exn_xxadsumt_xxadhsbx_jb")

    exn_xxadsumt_xxadhsbx_ng_df = spark.sql("SELECT client_id,plan_number,div_sub_id,fund_id,cast(source_cycle_date as DATE) as source_cycle_date,'VRP-PB' as source_system,cast(contribution_balance as DECIMAL(17,2)) as contribution_balance,cast(termination_balance as DECIMAL(17,2)) as termination_balance,cast(withdrawal_balance as DECIMAL(17,2)) as withdrawal_balance FROM exn_xxadsumt_xxadhsbx_ng")

    transform_df = exn_xxadsumt_xxadhsbx_jb_df.unionByName(exn_xxadsumt_xxadhsbx_ng_df)

    return transform_df