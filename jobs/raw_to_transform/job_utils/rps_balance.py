
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    f950_rps_parsdba_bal_ewr_dat_df = spark.sql("select coalesce(q1.plan_client_id,'-9999') as client_id,CAST(ytd_contributions AS DECIMAL(17,2)) AS ytd_contributions, CAST(vested_per AS DECIMAL(15,2)) AS vested_percent, coalesce(CAST(vested_bal AS DECIMAL(15,2)), 0) AS vested_balance, CAST(valm_date AS DATE) AS valuation_date, CAST(unit_priceo AS DECIMAL(19,6)) AS unit_price, part_agg_sys_nam AS source_system, CAST(cycle_date AS DATE) as source_cycle_date, case when money_sor_code = '' then '-9999' else coalesce(money_sor_code, '-9999') end AS money_source, coalesce(part_agg_sys_key, '-9999') AS retirement_account_id, m1.plan_sys_key AS plan_system_key, plan_sys_name AS plan_sys_name,coalesce(nullif(plan_id,''), '-9999') AS plan_number, coalesce(part_acc_num, '-9999') AS participant_id, CAST(num_of_units AS DECIMAL(17,4)) AS number_of_units, money_sor_sys_name AS money_source_system_name, money_sor_name AS money_source_name,fund_sor_sys_name AS fund_source_system_name, fund_sep_acc AS fund_separate_account, coalesce(fund_num,'-9999') as fund_number, fund_name AS fund_name, fund_mar_code AS fund_margin_code,coalesce(sub_plan_id, '-9999') AS div_sub_id,coalesce(CAST(cash_value AS DECIMAL(17,2)), 0) AS cash_value,act_code AS act_code, error_code from f950_rps_parsdba_bal_ewr_dat m1 left outer join f950_rps_plan_cef_data_ewr_dat q1 on m1.plan_id=q1.plan_number;")

    transform_df = f950_rps_parsdba_bal_ewr_dat_df

    return transform_df