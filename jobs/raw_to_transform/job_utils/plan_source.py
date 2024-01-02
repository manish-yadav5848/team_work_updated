
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from wealthcentral.jobs.raw_to_transform.helper_utils.data_utils import merge_dataframes
def transform(spark: SparkSession, primary_key: list):

    exn_xxewre01_xxewrpsd_jb_df = spark.sql("select PS.client_id,PS.money_source as money_source,PS.plan_number,cast(null as date) as source_processing_date,PS.money_source_name_long_version AS money_source_name_long,PS.roth_deferral_source_indicator,CAST(PS.source_cycle_date AS DATE),'VRP-SP' as source_system,CAST(NULL AS VARCHAR(32)) AS vesting_method,CAST(NULL AS VARCHAR(2)) AS vesting_schedule,CAST(NULL AS VARCHAR(35)) AS vesting_schedule_label,CAST(NULL AS VARCHAR(20)) AS vesting_schedule_percent,CAST(NULL AS VARCHAR(2)) AS vesting_schedule_period, MS.money_type_name, MS.money_type_code FROM exn_xxewre01_xxewrpsd_jb AS PS LEFT OUTER JOIN (select source_cd, CASE WHEN money_type_code = 'EE' THEN 'EMPLOYEE' WHEN money_type_code = 'ER' THEN 'EMPLOYER' ELSE NULL END AS money_type_name, CASE WHEN source_system='SUPEROMNI' THEN money_type_code WHEN source_system='PREMIER' THEN user_area WHEN source_system='RPS' THEN user_area ELSE NULL END AS money_type_code from pdab_money_source_helper_csv where source_cd is not null) AS MS ON coalesce(PS.money_source,' -9999') = coalesce(MS.source_cd, '-9999')")

    exn_xxewre01_xxewrpsd_ng_df = spark.sql("select PS.client_id,PS.money_source as money_source,PS.plan_number,cast(null as date) as source_processing_date,PS.money_source_name_long_version AS money_source_name_long,PS.roth_deferral_source_indicator,CAST(PS.source_cycle_date AS DATE),'VRP-PB' as source_system,CAST(NULL AS VARCHAR(32)) AS vesting_method,CAST(NULL AS VARCHAR(2)) AS vesting_schedule,CAST(NULL AS VARCHAR(35)) AS vesting_schedule_label,CAST(NULL AS VARCHAR(20)) AS vesting_schedule_percent,CAST(NULL AS VARCHAR(2)) AS vesting_schedule_period, MS.money_type_name, MS.money_type_code FROM exn_xxewre01_xxewrpsd_ng AS PS LEFT OUTER JOIN (select source_cd, CASE WHEN money_type_code = 'EE' THEN 'EMPLOYEE' WHEN money_type_code = 'ER' THEN 'EMPLOYER' ELSE NULL END AS money_type_name, CASE WHEN source_system='SUPEROMNI' THEN money_type_code WHEN source_system='PREMIER' THEN user_area WHEN source_system='RPS' THEN user_area ELSE NULL END AS money_type_code from pdab_money_source_helper_csv where source_cd is not null) AS MS ON coalesce(PS.money_source,' -9999') = coalesce(MS.source_cd, '-9999')")

    super_omni_newr_planvesting_daily_jb_dat_df = spark.sql("select SP.client_id, SP.money_source_code as money_source,SP.plan_number,cast(null as date) as source_processing_date,CAST(NULL AS VARCHAR(255)) AS money_source_name_long,CAST(NULL AS VARCHAR(1)) AS roth_deferral_source_indicator,CAST(SP.source_cycle_date AS DATE),'VRP-SP' as source_system, SP.vesting_method, SP.vesting_schedule, SP.vesting_schedule_label, SP.vesting_schedule_percent, SP.vesting_schedule_period,  MS.money_type_code, MS.money_type_name FROM super_omni_newr_planvesting_daily_jb_dat AS SP LEFT OUTER JOIN (select source_cd, CASE WHEN money_type_code = 'EE' THEN 'EMPLOYEE' WHEN money_type_code = 'ER' THEN 'EMPLOYER' ELSE NULL END AS money_type_name, CASE WHEN source_system='SUPEROMNI' THEN money_type_code WHEN source_system='PREMIER' THEN user_area WHEN source_system='RPS' THEN user_area ELSE NULL END AS money_type_code from pdab_money_source_helper_csv where source_cd is not null) as MS on coalesce(SP.money_source_code, '-9999') = coalesce(MS.source_cd, '-9999')")

    super_omni_newr_planvesting_daily_ng_dat_df = spark.sql("select SP.client_id, SP.money_source_code as money_source,SP.plan_number,cast(null as date) as source_processing_date,CAST(NULL AS VARCHAR(255)) AS money_source_name_long,CAST(NULL AS VARCHAR(1)) AS roth_deferral_source_indicator,CAST(SP.source_cycle_date AS DATE),'VRP-PB' as source_system, SP.vesting_method, SP.vesting_schedule, SP.vesting_schedule_label, SP.vesting_schedule_percent, SP.vesting_schedule_period,  MS.money_type_code, MS.money_type_name FROM super_omni_newr_planvesting_daily_ng_dat AS SP LEFT OUTER JOIN (select source_cd, CASE WHEN money_type_code = 'EE' THEN 'EMPLOYEE' WHEN money_type_code = 'ER' THEN 'EMPLOYER' ELSE NULL END AS money_type_name, CASE WHEN source_system='SUPEROMNI' THEN money_type_code WHEN source_system='PREMIER' THEN user_area WHEN source_system='RPS' THEN user_area ELSE NULL END AS money_type_code from pdab_money_source_helper_csv where source_cd is not null) as MS on coalesce(SP.money_source_code, '-9999') = coalesce(MS.source_cd, '-9999')")

    ng_merged_df = merge_dataframes(
        spark=spark,
        df1=exn_xxewre01_xxewrpsd_jb_df,
        df2=super_omni_newr_planvesting_daily_jb_dat_df,
        primary_key=primary_key
    )

    jb_merged_df = merge_dataframes(
        spark=spark,
        df1=exn_xxewre01_xxewrpsd_ng_df,
        df2=super_omni_newr_planvesting_daily_ng_dat_df,
        primary_key=primary_key
    )
    transform_df = ng_merged_df.unionByName(jb_merged_df)

    return transform_df
