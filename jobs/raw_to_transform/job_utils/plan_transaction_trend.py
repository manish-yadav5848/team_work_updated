
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    exn_xxadsumt_xxadhsfx_jb_df = spark.sql("SELECT client_id, plan_number, coalesce(source_cycle_date, current_date()) AS source_cycle_date, cast(total_enrollments AS INTEGER),cast(total_hardship_withdrawals_amount as decimal(14,2)) as TOT_HARDSHIP_WITHDRAWAL_AMOUNT,cast(total_deminimus_distributions_amount as decimal(14,2)) as TOT_DEMINIMUS_DISTRIB_AMOUNT, cast(total_employee_before_tax_contributions_count AS INTEGER) AS tot_ee_beforetax_contrib_count, cast(total_employee_after_tax_contributions_count AS INTEGER) AS tot_ee_aftertax_contrib_count, cast(total_employer_contributions_count AS INTEGER) AS tot_employer_contrib_count, cast(total_rollover_contributions_count AS INTEGER) AS TOT_ROLLOVER_CONTRIB_COUNT, cast(total_termination_distributions_with_rollover_count AS INTEGER) AS tot_term_distrib_roll_count, cast(total_termination_distributions_with_rollover_rolled_amount AS DECIMAL(14, 2)) AS tot_term_distrib_roll_rolled, cast(total_termination_distributions_with_rollover_not_rolled_amount AS DECIMAL(14, 2)) AS tot_term_distrib_roll_not_roll, cast(total_termination_distributions_with_no_rollover_count AS INTEGER) AS TOT_TERM_DISTRIB_NO_ROLL_COUNT, cast(total_termination_distributions_with_no_rollover_amount AS DECIMAL(14, 2)) AS tot_term_distrib_no_roll_amt, cast(total_in_service_withdrawals_with_rollover_count AS INTEGER) AS tot_insrvc_withd_with_roll_cnt, cast(total_in_service_withdrawals_with_rollover_rolled_amount AS DECIMAL(14, 2)) AS tot_withd_roll_rolled_amt, cast(total_in_service_withdrawals_with_rollover_not_rolled_amount AS DECIMAL(14, 2)) AS tot_withd_roll_not_rolled_amt, cast(total_in_service_withdrawals_with_no_rollover_count AS INTEGER) AS tot_insrvc_withd_no_roll_count, cast(total_in_service_withdrawals_with_no_rollover_amount AS DECIMAL(14, 2)) AS tot_insrvc_withd_no_roll_amt, cast(total_hardship_withdrawals_count AS INTEGER) AS tot_hardship_withdrawal_count, total_installments_setup_count, cast(total_installment_distributions_by_check_count AS INTEGER ) AS tot_install_distrib_check_cnt, cast(total_installment_distributions_by_check_amount AS DECIMAL(14, 2)) AS tot_install_distrib_check_amt, cast(total_installment_distributions_by_ach_count AS INTEGER) AS tot_install_distrib_ach_count, cast(total_installment_distributions_by_ach_amount AS DECIMAL(14, 2)) AS tot_install_distrib_ach_amt, cast(total_default_rollover_distributions_count AS INTEGER) AS tot_def_roll_distrib_count, cast(total_default_rollover_distributions_amount AS DECIMAL(14, 2)) AS tot_def_roll_distrib_amt, cast(total_deminimus_distributions_count AS INTEGER) AS tot_deminimus_distrib_count, cast(total_loan_issue_count AS INTEGER), cast(total_loan_reamortized_count AS INTEGER), total_loan_defaults_deemed_distributions_count AS tot_loan_def_deemd_distrib_cnt, cast(total_loan_repayments_count AS INTEGER), cast(total_loan_payoff_count AS INTEGER), cast(total_dividend_pass_thru_count AS INTEGER) AS tot_dividend_pass_thru_count, cast(total_dividend_pass_thru_amount AS DECIMAL(14, 2)) AS tot_dividend_pass_thru_amount, cast(total_dividend_reinvestments_count AS INTEGER) AS tot_dividend_reinvest_count, cast(total_dividend_reinvestments_amount AS DECIMAL(14, 2)) AS tot_dividend_reinvest_amount, cast(total_rebalance_transfers_count AS INTEGER) AS tot_rebal_xfer_count, cast(total_fee_deductions_count AS INTEGER), cast(total_qdro_splits_count AS INTEGER), total_beneficiary_splits_count, cast(total_rmd_count AS INTEGER), cast(total_rmd_amount AS DECIMAL(14, 2)), cast(total_loan_issue_amount AS DECIMAL(14, 2)) FROM exn_xxadsumt_xxadhsfx_jb")
    exn_xxadsumt_xxadhsfx_jb_df = exn_xxadsumt_xxadhsfx_jb_df.withColumn("source_system", lit('VRP-SP'))

    exn_xxadsumt_xxadhsfx_ng_df = spark.sql("SELECT client_id, plan_number, coalesce(source_cycle_date, current_date()) AS source_cycle_date, cast(total_enrollments AS INTEGER), cast(total_employee_before_tax_contributions_count AS INTEGER) AS tot_ee_beforetax_contrib_count,cast(total_hardship_withdrawals_amount as decimal(14,2)) as TOT_HARDSHIP_WITHDRAWAL_AMOUNT,cast(total_deminimus_distributions_amount as decimal(14,2)) as TOT_DEMINIMUS_DISTRIB_AMOUNT, cast(total_employee_after_tax_contributions_count AS INTEGER) AS tot_ee_aftertax_contrib_count, cast(total_employer_contributions_count AS INTEGER) AS tot_employer_contrib_count, cast(total_rollover_contributions_count AS INTEGER) AS TOT_ROLLOVER_CONTRIB_COUNT, cast(total_termination_distributions_with_rollover_count AS INTEGER) AS tot_term_distrib_roll_count, cast(total_termination_distributions_with_rollover_rolled_amount AS DECIMAL(14, 2)) AS tot_term_distrib_roll_rolled, cast(total_termination_distributions_with_rollover_not_rolled_amount AS DECIMAL(14, 2)) AS tot_term_distrib_roll_not_roll, cast(total_termination_distributions_with_no_rollover_count AS INTEGER) AS TOT_TERM_DISTRIB_NO_ROLL_COUNT, cast(total_termination_distributions_with_no_rollover_amount AS DECIMAL(14, 2)) AS tot_term_distrib_no_roll_amt, cast(total_in_service_withdrawals_with_rollover_count AS INTEGER) AS tot_insrvc_withd_with_roll_cnt, cast(total_in_service_withdrawals_with_rollover_rolled_amount AS DECIMAL(14, 2)) AS tot_withd_roll_rolled_amt, cast(total_in_service_withdrawals_with_rollover_not_rolled_amount AS DECIMAL(14, 2)) AS tot_withd_roll_not_rolled_amt, cast(total_in_service_withdrawals_with_no_rollover_count AS INTEGER) AS tot_insrvc_withd_no_roll_count, cast(total_in_service_withdrawals_with_no_rollover_amount AS DECIMAL(14, 2)) AS tot_insrvc_withd_no_roll_amt, cast(total_hardship_withdrawals_count AS INTEGER) AS tot_hardship_withdrawal_count, total_installments_setup_count, cast(total_installment_distributions_by_check_count AS INTEGER ) AS tot_install_distrib_check_cnt, cast(total_installment_distributions_by_check_amount AS DECIMAL(14, 2)) AS tot_install_distrib_check_amt, cast(total_installment_distributions_by_ach_count AS INTEGER) AS tot_install_distrib_ach_count, cast(total_installment_distributions_by_ach_amount AS DECIMAL(14, 2)) AS tot_install_distrib_ach_amt, cast(total_default_rollover_distributions_count AS INTEGER) AS tot_def_roll_distrib_count, cast(total_default_rollover_distributions_amount AS DECIMAL(14, 2)) AS tot_def_roll_distrib_amt, cast(total_deminimus_distributions_count AS INTEGER) AS tot_deminimus_distrib_count, cast(total_loan_issue_count AS INTEGER), cast(total_loan_reamortized_count AS INTEGER), total_loan_defaults_deemed_distributions_count AS tot_loan_def_deemd_distrib_cnt, cast(total_loan_repayments_count AS INTEGER), cast(total_loan_payoff_count AS INTEGER), cast(total_dividend_pass_thru_count AS INTEGER) AS tot_dividend_pass_thru_count, cast(total_dividend_pass_thru_amount AS DECIMAL(14, 2)) AS tot_dividend_pass_thru_amount, cast(total_dividend_reinvestments_count AS INTEGER) AS tot_dividend_reinvest_count, cast(total_dividend_reinvestments_amount AS DECIMAL(14, 2)) AS tot_dividend_reinvest_amount, cast(total_rebalance_transfers_count AS INTEGER) AS tot_rebal_xfer_count, cast(total_fee_deductions_count AS INTEGER), cast(total_qdro_splits_count AS INTEGER), total_beneficiary_splits_count, cast(total_rmd_count AS INTEGER), cast(total_rmd_amount AS DECIMAL(14, 2)), cast(total_loan_issue_amount AS DECIMAL(14, 2)) FROM exn_xxadsumt_xxadhsfx_ng")
    exn_xxadsumt_xxadhsfx_ng_df = exn_xxadsumt_xxadhsfx_ng_df.withColumn("source_system", lit('VRP-PB'))

    transform_df = exn_xxadsumt_xxadhsfx_jb_df.unionByName(exn_xxadsumt_xxadhsfx_ng_df)

    return transform_df