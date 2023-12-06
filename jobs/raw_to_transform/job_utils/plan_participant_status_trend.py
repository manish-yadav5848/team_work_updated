
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    exn_xxadsumt_xxadhsex_ng_df = spark.sql("SELECT client_id, plan_number,'PLAN PARTICIPANT STATUS TREND' AS entity_name, cast(source_cycle_date AS date) AS source_cycle_date, cast(active_not_contributing_count AS INTEGER), cast( active_not_contributing_total_balance AS DECIMAL(14, 2) ) AS active_not_contrib_tot_bal, cast(active_contributing_count AS INTEGER), cast( active_contributing_total_balance AS DECIMAL(14, 2) ) AS active_contributing_total_bal, cast( eligible_not_participating_with_balance_count AS INTEGER ) AS elig_not_part_with_bal_count, cast( eligible_not_participating_with_balance_total_balance AS DECIMAL(14, 2) ) AS elig_not_part_bal_tot_bal, cast( eligible_not_participating_no_balance_count AS INTEGER ) AS elig_not_part_no_bal_count, cast(suspended_with_balance_count AS INTEGER) AS suspended_with_bal_count, cast( suspended_with_balance_total_balance AS DECIMAL(14, 2) ) AS suspended_with_bal_total_bal, cast(suspended_no_balance_count AS INTEGER) AS suspended_no_bal_count, cast(ineligible_with_balance_count AS INTEGER) AS ineligible_with_bal_count, cast( ineligible_with_balance_total_balance AS DECIMAL(14, 2) ) AS ineligible_with_bal_total_bal, cast(omni_status_code_01_count AS INTEGER), cast( omni_status_code_01_total_balance AS DECIMAL(14, 2) ) AS omni_status_code_01_total_bal, cast(omni_status_code_05_count AS INTEGER), cast( omni_status_code_05_total_balance AS DECIMAL(14, 2) ) AS omni_status_code_05_total_bal, cast( terminated_receiving_installments_count AS INTEGER ) AS term_receiv_install_cnt, cast( terminated_receiving_installments_total_balance AS DECIMAL(14, 2) ) AS term_receiv_instal_tot_bal, cast(terminated_with_balance_count AS INTEGER) AS terminated_with_bal_count, cast( terminated_with_balance_total_balance AS DECIMAL(14, 2) ) AS terminated_with_bal_total_bal, cast(terminated_no_balance_count AS INTEGER) AS terminated_no_bal_count, cast(participants_with_balance_count as INTEGER) AS participants_with_bal_count, cast( participants_with_balance_total_balance AS DECIMAL(14, 2) ) AS participants_with_bal_tot_bal, cast( participants_with_balance_average_balance AS DECIMAL(14, 2) ) AS part_w_bal_average_bal, cast(catchup_eligible_count AS INTEGER), cast(catchup_eligible_total_balance AS DECIMAL(14, 2)), catchup_participating_count, cast( catchup_participating_total_balance AS DECIMAL(14, 2) ) FROM exn_xxadsumt_xxadhsex_ng")


    exn_xxadsumt_xxadhsex_jb_df = spark.sql("SELECT client_id, plan_number,'PLAN PARTICIPANT STATUS TREND' AS entity_name, cast(source_cycle_date AS date) AS source_cycle_date, cast(active_not_contributing_count AS INTEGER), cast( active_not_contributing_total_balance AS DECIMAL(14, 2) ) AS active_not_contrib_tot_bal, cast(active_contributing_count AS INTEGER), cast( active_contributing_total_balance AS DECIMAL(14, 2) ) AS active_contributing_total_bal, cast( eligible_not_participating_with_balance_count AS INTEGER ) AS elig_not_part_with_bal_count, cast( eligible_not_participating_with_balance_total_balance AS DECIMAL(14, 2) ) AS elig_not_part_bal_tot_bal, cast( eligible_not_participating_no_balance_count AS INTEGER ) AS elig_not_part_no_bal_count, cast(suspended_with_balance_count AS INTEGER) AS suspended_with_bal_count, cast( suspended_with_balance_total_balance AS DECIMAL(14, 2) ) AS suspended_with_bal_total_bal, cast(suspended_no_balance_count AS INTEGER) AS suspended_no_bal_count, cast(ineligible_with_balance_count AS INTEGER) AS ineligible_with_bal_count, cast( ineligible_with_balance_total_balance AS DECIMAL(14, 2) ) AS ineligible_with_bal_total_bal, cast(omni_status_code_01_count AS INTEGER), cast( omni_status_code_01_total_balance AS DECIMAL(14, 2) ) AS omni_status_code_01_total_bal, cast(omni_status_code_05_count AS INTEGER), cast( omni_status_code_05_total_balance AS DECIMAL(14, 2) ) AS omni_status_code_05_total_bal, cast( terminated_receiving_installments_count AS INTEGER ) AS term_receiv_install_cnt, cast( terminated_receiving_installments_total_balance AS DECIMAL(14, 2) ) AS term_receiv_instal_tot_bal, cast(terminated_with_balance_count AS INTEGER) AS terminated_with_bal_count, cast( terminated_with_balance_total_balance AS DECIMAL(14, 2) ) AS terminated_with_bal_total_bal, cast(terminated_no_balance_count AS INTEGER) AS terminated_no_bal_count, cast(participants_with_balance_count as INTEGER) AS participants_with_bal_count, cast( participants_with_balance_total_balance AS DECIMAL(14, 2) ) AS participants_with_bal_tot_bal, cast( participants_with_balance_average_balance AS DECIMAL(14, 2) ) AS part_w_bal_average_bal, cast(catchup_eligible_count AS INTEGER), cast(catchup_eligible_total_balance AS DECIMAL(14, 2)), catchup_participating_count, cast( catchup_participating_total_balance AS DECIMAL(14, 2) ) FROM exn_xxadsumt_xxadhsex_jb")


    transform_df = exn_xxadsumt_xxadhsex_ng_df.unionByName(exn_xxadsumt_xxadhsex_jb_df)

    return transform_df