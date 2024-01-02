
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    exn_xxadsumt_xxadhsax_jb_df = spark.sql("select coalesce(client_id,'-9999') as client_id,coalesce(plan_number,'-9999') as plan_number,coalesce(div_sub_id,'-9999') as div_sub_id,cast(source_cycle_date as date) as source_cycle_date,coalesce(tran_type,'-9999') as tran_type,cast(age_less_than_20_count as integer),cast(age_20_to_25_count as integer),cast(age_26_to_30_count as integer),cast(age_31_to_35_count as integer),cast(age_36_to_40_count as integer),cast(age_41_to_45_count as integer),cast(age_46_to_50_count as integer),cast(age_51_to_55_count as integer),cast(age_56_plus_count as integer),'VRP-SP' as source_system from exn_xxadsumt_xxadhsax_jb")

    exn_xxadsumt_xxadhsax_ng_df = spark.sql("select coalesce(client_id,'-9999') as client_id,coalesce(plan_number,'-9999') as plan_number,coalesce(div_sub_id,'-9999') as div_sub_id,cast(source_cycle_date as date) as source_cycle_date,coalesce(tran_type,'-9999') as tran_type,cast(age_less_than_20_count as integer),cast(age_20_to_25_count as integer),cast(age_26_to_30_count as integer),cast(age_31_to_35_count as integer),cast(age_36_to_40_count as integer),cast(age_41_to_45_count as integer),cast(age_46_to_50_count as integer),cast(age_51_to_55_count as integer),cast(age_56_plus_count as integer),'VRP-PB' as source_system  from exn_xxadsumt_xxadhsax_ng")

    transform_df = exn_xxadsumt_xxadhsax_jb_df.unionByName(exn_xxadsumt_xxadhsax_ng_df)

    return transform_df