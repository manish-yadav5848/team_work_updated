
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    exn_xxewre01_xxewrpap_jb_df = spark.sql("SELECT client_id,participant_id,plan_number,account_type,address_flag,address_line_1,address_line_2,address_line_3,address_line_4,address_type,bank_or_financial_institute_account_number as bank_account_num,city,cast(effective_date_lo as date) as effective_date_lo,ein,cast(last_change_date as date) as last_change_date,name,payment_frequency,plan_sequence as alt_payee_sequence,record_status,cast(recurring_payment_basis_amount as DECIMAL(19,4)),cast(recurring_payment_flat_dollar_amount as DECIMAL(15,2)) as recurr_payment_flat_dollar_amt,cast(sequence_number as INTEGER),cast(source_cycle_date as date) as source_cycle_date,'VRP-SP' as source_system,state,transit_or_routing_number as bank_routing_num,zip from exn_xxewre01_xxewrpap_jb")

    exn_xxewre01_xxewrpap_ng_df = spark.sql("SELECT client_id,participant_id,plan_number,account_type,address_flag,address_line_1,address_line_2,address_line_3,address_line_4,address_type,bank_or_financial_institute_account_number as bank_account_num,city,cast(effective_date_lo as date) as effective_date_lo,ein,cast(last_change_date as date) as last_change_date,name,payment_frequency,plan_sequence as alt_payee_sequence,record_status,cast(recurring_payment_basis_amount as DECIMAL(19,4)),cast(recurring_payment_flat_dollar_amount as DECIMAL(15,2)) as recurr_payment_flat_dollar_amt,cast(sequence_number as INTEGER),cast(source_cycle_date as date) as source_cycle_date,'VRP-PB' as source_system,state,transit_or_routing_number as bank_routing_num,zip from exn_xxewre01_xxewrpap_ng")

    transform_df = exn_xxewre01_xxewrpap_jb_df.unionByName(exn_xxewre01_xxewrpap_ng_df)

    return transform_df