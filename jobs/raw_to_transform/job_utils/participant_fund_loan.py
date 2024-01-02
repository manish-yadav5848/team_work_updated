
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    exn_xxewre01_xxewrpfl_jb_df = spark.sql("select client_id, plan_number, participant_id, cast(loan_number as SMALLINT), fund_id as fund_number, cast(accumulated_inactive_interest as DECIMAL(11,2)), cast(loan_calendar_year_interest as DECIMAL(11,2)), cast(loan_interest_repaid as DECIMAL(11,2)), cast(loan_payment_percent as DECIMAL(13,4)), cast(loan_principal_repaid as DECIMAL(11,2)), cast(original_loan_amount as DECIMAL(11,2)), 'VRP-SP' as source_system, cast(source_cycle_date as DATE) from exn_xxewre01_xxewrpfl_jb")

    transform_df = exn_xxewre01_xxewrpfl_jb_df

    return transform_df