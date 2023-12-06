
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, batch_date: str, primary_key: list):

    consumption_df = spark.sql("SELECT m.CLIENT_ID, m.FUND_NUMBER, m.PARTICIPANT_ID, m.LOAN_NUMBER, m.PLAN_NUMBER, m.source_cycle_date, m.LOAN_PRINCIPAL_REPAID, m.ORIGINAL_LOAN_AMOUNT, m.LOAN_CALENDAR_YEAR_INTEREST, m.LOAN_INTEREST_REPAID, m.loan_balance, m.LOAN_PAYMENT_PERCENT, m.div_sub_id from ( SELECT t1.CLIENT_ID, t1.FUND_NUMBER, t1.PARTICIPANT_ID, t1.LOAN_NUMBER, t1.PLAN_NUMBER, t1.source_cycle_date, t1.LOAN_PRINCIPAL_REPAID, t1.ORIGINAL_LOAN_AMOUNT, t1.LOAN_CALENDAR_YEAR_INTEREST, t1.LOAN_INTEREST_REPAID, t1.loan_balance, pfl2.LOAN_PAYMENT_PERCENT, cast(null as varchar(20)) as div_sub_id FROM ( SELECT coalesce(pfl.CLIENT_ID, '-9999') as CLIENT_ID, coalesce(pfl.FUND_NUMBER, '-9999') as FUND_NUMBER, coalesce(cast(pfl.LOAN_NUMBER as INTEGER), -9999) as LOAN_NUMBER, coalesce(pfl.PARTICIPANT_ID, '-9999') as PARTICIPANT_ID, coalesce(pfl.PLAN_NUMBER, '-9999') as PLAN_NUMBER, pfl.SOURCE_CYCLE_DATE, cast( sum(coalesce(pfl.LOAN_PRINCIPAL_REPAID, 0)) as DECIMAL(11, 2) ) as LOAN_PRINCIPAL_REPAID, cast( sum(coalesce(pfl.ORIGINAL_LOAN_AMOUNT, 0)) as DECIMAL(11, 2) ) as ORIGINAL_LOAN_AMOUNT, cast( sum(coalesce(pfl.LOAN_CALENDAR_YEAR_INTEREST, 0)) as DECIMAL(11, 2) ) as LOAN_CALENDAR_YEAR_INTEREST, cast( sum(coalesce(pfl.LOAN_INTEREST_REPAID, 0)) as DECIMAL(11, 2) ) as LOAN_INTEREST_REPAID, cast( sum(coalesce(pl.loan_balance, 0)) as DECIMAL(12, 2) ) as loan_balance from participant_fund_loan as pfl left outer join participant_loan as pl on coalesce(pfl.client_id, '-9999') = coalesce(pl.client_id, '-9999') and coalesce(pfl.source_cycle_date, current_date() -1) = coalesce(pl.client_id, current_date() -1) AND coalesce(pfl.plan_number, '-9999') = coalesce(pl.plan_number, '-9999') and coalesce(pfl.participant_id, '-9999') = coalesce(pl.participant_id, '-9999') AND coalesce(cast(pfl.loan_number as integer), -9999) = coalesce(cast(pl.loan_number as integer), -9999) group by pfl.client_id, pfl.fund_number, pfl.loan_number, pfl.participant_id, pfl.plan_number, pfl.source_cycle_date ) as t1 left outer join participant_fund_loan as pfl2 on coalesce(t1.client_id, '-9999') = coalesce(pfl2.client_id, '-9999') AND coalesce(t1.plan_number, '-9999') = coalesce(pfl2.plan_number, '-9999') and coalesce(t1.participant_id, '-9999') = coalesce(pfl2.participant_id, '-9999') and coalesce(t1.FUND_NUMBER, '-9999') = coalesce(pfl2.FUND_NUMBER, '-9999') AND coalesce(t1.loan_number, '-9999') = coalesce(cast(pfl2.loan_number as INTEGER), -9999) left outer join participant_loan as pl2 ON coalesce(pl2.client_id, '-9999') = coalesce(pfl2.client_id, '-9999') AND coalesce(pl2.plan_number, '-9999') = coalesce(pfl2.plan_number, '-9999') and coalesce(pl2.participant_id, '-9999') = coalesce(pfl2.participant_id, '-9999') AND coalesce(cast(pfl2.loan_number as integer), -9999) = coalesce(cast(pl2.loan_number as integer), -9999) ) m")

    return consumption_df