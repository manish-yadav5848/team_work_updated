
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    exn_xxewre01_xxewrpin_jb_df = spark.sql("SELECT coalesce(client_id, '-9999') as client_id,coalesce(plan_number, '-9999') plan_number, coalesce(participant_id, '-9999') as participant_id, coalesce(cast(create_date as DATE), current_date()) as create_date, coalesce(cast(end_date as DATE), current_date()) as end_date, cast(installment_hold_date as DATE), CAST(installment_sequence as INTEGER),cast(next_pay_date as DATE), CAST(number_of_payments_processed as INTEGER) as NUM_OF_PAYMENTS_PROCED_TO_DATE, recalculation_option,cast(start_date as DATE), vtd_file_id,vtd_fund_id,CAST(vtd_transaction_sequence as INTEGER), 'VRP-SP' as source_system, CAST(source_cycle_date AS DATE) AS source_cycle_date, termination_payment_method, installment_frequency, cast(installment_payment_amount as DECIMAL(14,2)) as installment_payment_amount,CAST(number_installment_payments as INTEGER), cast(null as DATE) as source_processing_date FROM exn_xxewre01_xxewrpin_jb")

    super_omni_newr_plan_ins_daily_ng_dat_df = spark.sql("SELECT coalesce(client_id, '-9999') as client_id, coalesce(plan_number, '-9999') as plan_number, coalesce(participant_id, '-9999') as participant_id, coalesce(cast(create_date as DATE), current_date()) as create_date, coalesce(cast(end_date as DATE), current_date()) as end_date, cast(installment_hold_date as DATE), CAST(installment_sequence as INTEGER),cast(next_pay_date as DATE), CAST(number_of_payments_processed as INTEGER) as NUM_OF_PAYMENTS_PROCED_TO_DATE, recalculation_option, cast(start_date as DATE), vtd_file_id,vtd_fund_id,CAST(vtd_transaction_sequence as INTEGER),cast(termination_payment_method as varchar(1)) as termination_payment_method,cast(installment_frequency as varchar(1)) as installment_frequency,cast(installment_payment_amount as decimal(14,2)) as installment_payment_amount,cast(number_installment_payments as integer) as number_installment_payments,'VRP-PB' as source_system,CAST(source_cycle_date AS DATE) AS source_cycle_date, cast(null as DATE) as source_processing_date FROM super_omni_newr_plan_ins_daily_ng_dat")

    transform_df = exn_xxewre01_xxewrpin_jb_df.unionByName(super_omni_newr_plan_ins_daily_ng_dat_df)

    return transform_df