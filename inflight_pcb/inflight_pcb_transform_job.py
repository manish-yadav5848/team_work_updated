from wealthcentral.jobs.inflight_pcb.helper_utils.data_utils import (generate_merge_condition_for_delta_in_raw,
                                                                     generate_insert_condition_for_delta_in_transform)
from wealthcentral.utils.config_utils import load_json_file
from wealthcentral.jobs.raw_to_transform.helper_utils.config_utils import read_transform_parameters
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from wealthcentral.utils.object_utils import InFlightTransformJobArguments
import logging
import os
from wealthcentral.utils.spark_utils import overwrite_delta_load


def inflight_pcb_transform(spark: SparkSession, job_arguments: InFlightTransformJobArguments):
    logger = logging.getLogger('pyspark')

    transform_config_path = os.getenv('TRANSFORM_CONFIG_PATH')
    transform_entity_name = job_arguments.transform_entity_name
    transform_database_name = os.getenv('TRANSFORM_DATABASE_NAME')
    raw_database_name = os.getenv('RAW_DATABASE_NAME')
    transform_output_path = os.getenv("TRANSFORM_OUTPUT_PATH")

    transform_config = load_json_file(
        file_path= transform_config_path
    )

    inflight_transform_parameters = read_transform_parameters(
        config_file=transform_config,
        entity_name=transform_entity_name
    )

    # (write sql query)
    spark.sql(f"use {raw_database_name}")
    df_participant_core_balance_monthly_12_31 = spark.sql("SELECT concat(SOPB.plan_number,'~',SOPB.participant_id) as retirement_account_id, coalesce(SOPB.client_id, '-9999') AS client_id, coalesce(SOPB.participant_id, '-9999') AS participant_id, coalesce(nullif(SOPB.plan_number, ''), '-9999') AS plan_number, SOPB.fund_iv AS fund_iv, CASE WHEN SOPB.money_source = '' THEN '-9999' ELSE coalesce(SOPB.money_source, '-9999') END AS money_source, CASE WHEN SOPB.fund_iv = '90' THEN 'LN90' WHEN SOPB.fund_iv = '91' THEN 'LN91' ELSE coalesce(nullif(pf.price_id, ''), '-9999') END AS fund_number, CAST( SOPB.contribution_current_cal_year AS DECIMAL(17, 2) ) AS ytd_contributions, CAST(SOPB.withdrawal_disbursements AS DECIMAL(15, 2)) AS disbursements_withdrawal, CAST(SOPB.vested_percent AS DECIMAL(15, 2)) AS vested_percent, coalesce(CAST(SOPB.vested_balance AS DECIMAL(15, 2)), 0) AS vested_balance, CAST(null AS DATE) AS valuation_date, CAST(SOPB.unit_price AS DECIMAL(19, 6)) AS share_price, coalesce(CAST(SOPB.uninvested_balance AS DECIMAL(15, 2)), 0) AS uninvested_balance, CAST(SOPB.transfers_out AS DECIMAL(15, 2)) AS transfers_out, CAST(SOPB.transfers_in AS DECIMAL(15, 2)) AS transfers_in, CAST(SOPB.termination_disbursements AS DECIMAL(15, 2)) AS disbursements_termination, 'VRP-SP' AS source_system, CAST(SOPB.source_cycle_date AS DATE) AS source_cycle_date, CAST(SOPB.shares_units_sold AS DECIMAL(13, 4)) AS shares_sold, CAST(SOPB.shares_units_receipted AS DECIMAL(13, 4)) AS shares_receipted, CAST(SOPB.shares_units_purchased AS DECIMAL(13, 4)) AS shares_purchased, CAST(SOPB.shares_units_forfeited AS DECIMAL(15, 2)) AS shares_forfeited, CAST(SOPB.shares_units_distributed AS DECIMAL(15, 2)) AS shares_distributed, CAST(SOPB.pending_debit_shares AS DECIMAL(15, 2)) AS shares_debit_pending, CAST(SOPB.pending_debit_cash AS DECIMAL(15, 2)) AS cash_debit_pending, CAST(SOPB.pending_credit_shares AS DECIMAL(15, 2)) AS shares_credit_pending, CAST(SOPB.pending_credit_cash AS DECIMAL(15, 2)) AS cash_credit_pending, CAST( SOPB.payment_attributable_contributions AS DECIMAL(15, 2) ) AS contrib_payment_attributable, CAST(SOPB.other_debits AS DECIMAL(15, 2)) AS debits_other, CAST(SOPB.number_of_units AS DECIMAL(17, 4)) AS total_shares, CAST(SOPB.net_contribution_current AS DECIMAL(15, 2)) AS current_net_contribution, CASE WHEN MS.source_system = 'SUPEROMNI' THEN MS.pwe_money_source_name ELSE NULL END AS money_source_name, CASE WHEN MS.source_system = 'SUPEROMNI' THEN MS.money_type_code ELSE NULL END AS money_type_description, CAST(SOPB.miscellanous_receipts AS DECIMAL(15, 2)) AS receipts_miscellanous, CAST(SOPB.miscellanous_debits AS DECIMAL(15, 2)) AS debits_miscellanous, CAST(SOPB.loan_repayment_principal AS DECIMAL(15, 2)) AS loan_repayment_principal, CAST(SOPB.loan_repayment_interest AS DECIMAL(15, 2)) AS loan_repayment_interest, CAST(SOPB.loan_issues AS DECIMAL(15, 2)) AS loan_issues, CAST(SOPB.insurance_premium_paid AS DECIMAL(15, 2)) AS insurance_premium_paid, CAST(SOPB.installment_disbursements AS DECIMAL(15, 2)) AS disbursements_installment, pf.fund_name AS fund_name, CAST(SOPB.forfeitures_debited AS DECIMAL(15, 2)) AS debited_forfeitures, CAST(SOPB.forfeitures_credited AS DECIMAL(15, 2)) AS credited_forfeitures, CAST(SOPB.fee_disbursements AS DECIMAL(15, 2)) AS disbursements_fee, CAST( SOPB.earnings_gain_loss_shares_unit AS DECIMAL(13, 4) ) AS shares_gain_loss_earnings, CAST(SOPB.earnings_dividend AS DECIMAL(15, 2)) AS dividend_earnings, CAST(SOPB.earnings_cash AS DECIMAL(15, 2)) AS cash_earnings, CAST(SOPB.conversions_out AS DECIMAL(15, 2)) AS conversions_out, CAST(SOPB.conversions_in AS DECIMAL(15, 2)) AS conversions_in, CAST(SOPB.contribution_gross AS DECIMAL(15, 2)) AS contribution_gross, CASE WHEN SOPB.contribution_current_fis_year = '0.00' THEN '0000' ELSE SOPB.contribution_current_fis_year END AS contribution_current_fis_year, CAST( SOPB.contribution_allocation_percent AS DECIMAL(13, 4) ) AS contribution_alloc_pct, coalesce(CAST(cash_value AS DECIMAL(17, 2)), 0) AS cash_value_amount, CAST(SOPB.brokerage_account_trade_date AS DATE) AS sdba_trade_date, case when SOPB.fund_iv='70' then COALESCE( CAST( SOPB.cash_value AS DECIMAL(15, 2) ), 0 ) else cast('0' as DECIMAL(15, 2)) end AS sdba_cash_value_amount, CAST(SOPB.annual_dividend_amount AS DECIMAL(15, 4)) AS annual_dividend_amount, CAST(null AS VARCHAR(1)) AS action_code, CAST(SOPB.accrued_dividend_amount AS DECIMAL(15, 4)) AS accrued_dividend_amount, CAST(SOPB.trade_date AS DATE) AS trade_date, CAST(SOPB.net_dollars AS DECIMAL(15, 2)) AS net_dollars, cast(null as varchar(10)) as fund_margin_code, cast(null as varchar(10)) as account_fund_separate FROM exn_xxewre01_xxewrpcb_delete_jb SOPB LEFT OUTER JOIN exn_xxewre01_xxewrpfd_jb AS pf ON SOPB.plan_number = pf.plan_number AND SOPB.fund_iv = pf.fund_iv LEFT OUTER JOIN pdab_money_source_helper_csv AS MS ON coalesce(SOPB.money_source, '-9999') = coalesce(MS.source_cd, '-9999') where not (SOPB.client_id ='AB' and (pf.price_id  in ('','-9999') or pf.price_id is null) and cash_value=0)")
    df_participant_core_balance_monthly_updated_12_31 = spark.sql("SELECT concat(SOPB.plan_number,'~',SOPB.participant_id) as retirement_account_id, coalesce(SOPB.client_id, '-9999') AS client_id, coalesce(SOPB.participant_id, '-9999') AS participant_id, coalesce(nullif(SOPB.plan_number, ''), '-9999') AS plan_number, SOPB.fund_iv AS fund_iv, CASE WHEN SOPB.money_source = '' THEN '-9999' ELSE coalesce(SOPB.money_source, '-9999') END AS money_source, CASE WHEN SOPB.fund_iv = '90' THEN 'LN90' WHEN SOPB.fund_iv = '91' THEN 'LN91' ELSE coalesce(nullif(pf.price_id, ''), '-9999') END AS fund_number, CAST( SOPB.contribution_current_cal_year AS DECIMAL(17, 2) ) AS ytd_contributions, CAST(SOPB.withdrawal_disbursements AS DECIMAL(15, 2)) AS disbursements_withdrawal, CAST(SOPB.vested_percent AS DECIMAL(15, 2)) AS vested_percent, coalesce(CAST(SOPB.vested_balance AS DECIMAL(15, 2)), 0) AS vested_balance, CAST(null AS DATE) AS valuation_date, CAST(SOPB.unit_price AS DECIMAL(19, 6)) AS share_price, coalesce(CAST(SOPB.uninvested_balance AS DECIMAL(15, 2)), 0) AS uninvested_balance, CAST(SOPB.transfers_out AS DECIMAL(15, 2)) AS transfers_out, CAST(SOPB.transfers_in AS DECIMAL(15, 2)) AS transfers_in, CAST(SOPB.termination_disbursements AS DECIMAL(15, 2)) AS disbursements_termination, 'VRP-SP' AS source_system, CAST(SOPB.source_cycle_date AS DATE) AS source_cycle_date, CAST(SOPB.shares_units_sold AS DECIMAL(13, 4)) AS shares_sold, CAST(SOPB.shares_units_receipted AS DECIMAL(13, 4)) AS shares_receipted, CAST(SOPB.shares_units_purchased AS DECIMAL(13, 4)) AS shares_purchased, CAST(SOPB.shares_units_forfeited AS DECIMAL(15, 2)) AS shares_forfeited, CAST(SOPB.shares_units_distributed AS DECIMAL(15, 2)) AS shares_distributed, CAST(SOPB.pending_debit_shares AS DECIMAL(15, 2)) AS shares_debit_pending, CAST(SOPB.pending_debit_cash AS DECIMAL(15, 2)) AS cash_debit_pending, CAST(SOPB.pending_credit_shares AS DECIMAL(15, 2)) AS shares_credit_pending, CAST(SOPB.pending_credit_cash AS DECIMAL(15, 2)) AS cash_credit_pending, CAST( SOPB.payment_attributable_contributions AS DECIMAL(15, 2) ) AS contrib_payment_attributable, CAST(SOPB.other_debits AS DECIMAL(15, 2)) AS debits_other, CAST(SOPB.number_of_units AS DECIMAL(17, 4)) AS total_shares, CAST(SOPB.net_contribution_current AS DECIMAL(15, 2)) AS current_net_contribution, CASE WHEN MS.source_system = 'SUPEROMNI' THEN MS.pwe_money_source_name ELSE NULL END AS money_source_name, CASE WHEN MS.source_system = 'SUPEROMNI' THEN MS.money_type_code ELSE NULL END AS money_type_description, CAST(SOPB.miscellanous_receipts AS DECIMAL(15, 2)) AS receipts_miscellanous, CAST(SOPB.miscellanous_debits AS DECIMAL(15, 2)) AS debits_miscellanous, CAST(SOPB.loan_repayment_principal AS DECIMAL(15, 2)) AS loan_repayment_principal, CAST(SOPB.loan_repayment_interest AS DECIMAL(15, 2)) AS loan_repayment_interest, CAST(SOPB.loan_issues AS DECIMAL(15, 2)) AS loan_issues, CAST(SOPB.insurance_premium_paid AS DECIMAL(15, 2)) AS insurance_premium_paid, CAST(SOPB.installment_disbursements AS DECIMAL(15, 2)) AS disbursements_installment, pf.fund_name AS fund_name, CAST(SOPB.forfeitures_debited AS DECIMAL(15, 2)) AS debited_forfeitures, CAST(SOPB.forfeitures_credited AS DECIMAL(15, 2)) AS credited_forfeitures, CAST(SOPB.fee_disbursements AS DECIMAL(15, 2)) AS disbursements_fee, CAST( SOPB.earnings_gain_loss_shares_unit AS DECIMAL(13, 4) ) AS shares_gain_loss_earnings, CAST(SOPB.earnings_dividend AS DECIMAL(15, 2)) AS dividend_earnings, CAST(SOPB.earnings_cash AS DECIMAL(15, 2)) AS cash_earnings, CAST(SOPB.conversions_out AS DECIMAL(15, 2)) AS conversions_out, CAST(SOPB.conversions_in AS DECIMAL(15, 2)) AS conversions_in, CAST(SOPB.contribution_gross AS DECIMAL(15, 2)) AS contribution_gross, CASE WHEN SOPB.contribution_current_fis_year = '0.00' THEN '0000' ELSE SOPB.contribution_current_fis_year END AS contribution_current_fis_year, CAST( SOPB.contribution_allocation_percent AS DECIMAL(13, 4) ) AS contribution_alloc_pct, coalesce(CAST(cash_value AS DECIMAL(17, 2)), 0) AS cash_value_amount, CAST(SOPB.brokerage_account_trade_date AS DATE) AS sdba_trade_date, case when SOPB.fund_iv='70' then COALESCE( CAST( SOPB.cash_value AS DECIMAL(15, 2) ), 0 ) else cast('0' as DECIMAL(15, 2)) end AS sdba_cash_value_amount, CAST(SOPB.annual_dividend_amount AS DECIMAL(15, 4)) AS annual_dividend_amount, CAST(null AS VARCHAR(1)) AS action_code, CAST(SOPB.accrued_dividend_amount AS DECIMAL(15, 4)) AS accrued_dividend_amount, CAST(SOPB.trade_date AS DATE) AS trade_date, CAST(SOPB.net_dollars AS DECIMAL(15, 2)) AS net_dollars, cast(null as varchar(10)) as fund_margin_code, cast(null as varchar(10)) as account_fund_separate FROM super_omni_newr_histbal_jb_dat_rerun_dat SOPB LEFT OUTER JOIN exn_xxewre01_xxewrpfd_jb AS pf ON SOPB.plan_number = pf.plan_number AND SOPB.fund_iv = pf.fund_iv LEFT OUTER JOIN pdab_money_source_helper_csv AS MS ON coalesce(SOPB.money_source, '-9999') = coalesce(MS.source_cd, '-9999') where not (SOPB.client_id ='AB' and (pf.price_id  in ('','-9999') or pf.price_id is null) and cash_value=0)")

    primary_keys = inflight_transform_parameters.primary_keys
    column_names = df_participant_core_balance_monthly_12_31.columns
    merge_condition = generate_merge_condition_for_delta_in_raw(primary_keys)
    insert_condition = generate_insert_condition_for_delta_in_transform(column_names)

    # delete from existing delta table (target) where primary keys of source (participant_core_balance_monthly_12_31) matches
    delta_table = DeltaTable.forName(spark, "{}.{}".format(transform_database_name, transform_entity_name))
    writer = delta_table.alias("target")
    writer = writer.merge(df_participant_core_balance_monthly_12_31.alias("source"), merge_condition)
    writer.whenMatchedDelete().execute()

    # insert into existing delta table (target) all the data from source (participant_core_balance_monthly_12_31)
    delta_table = DeltaTable.forName(spark, "{}.{}".format(transform_database_name, transform_entity_name))
    writer = delta_table.alias("target")
    writer = writer.merge(df_participant_core_balance_monthly_updated_12_31.alias("source"), merge_condition)
    writer.whenMatchedUpdate(set=insert_condition).whenNotMatchedInsert(values=insert_condition).execute()

    # write df_participant_core_balance_monthly_12_31 and df_participant_core_balance_monthly_updated_12_31_tmp into a table
    spark.sql(f"use {transform_entity_name}")
    table_name1 = "participant_core_balance_monthly_12_31_tmp"
    table_name2 = "participant_core_balance_monthly_updated_12_31_tmp"

    overwrite_delta_load(spark=spark,
                         df=df_participant_core_balance_monthly_12_31,
                         database_name=transform_database_name,
                         table_name=table_name1,
                         partition_cols=["source_cycle_date"],
                         location=transform_output_path + "/" + table_name1)

    overwrite_delta_load(spark=spark,
                         df=df_participant_core_balance_monthly_updated_12_31,
                         database_name=transform_database_name,
                         table_name=table_name2,
                         partition_cols=["source_cycle_date"],
                         location=transform_output_path + "/" + table_name2)