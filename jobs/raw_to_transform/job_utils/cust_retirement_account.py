
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    ods_retirement_account_df = spark.sql("SELECT D1.plan_number AS plan_number, D1.participant_id AS participant_id, D1.cust_ods_agreement_id AS cust_ods_agreement_id, date_format( D1.source_cycle_date, 'yyyy-MM-dd' ) AS source_cycle_date FROM ( SELECT plan_number, participant_id, date_format(source_cycle_timestamp, 'yyyy-MM-dd') AS source_cycle_date, cust_ods_agreement_id FROM ods_retirement_account ) AS D1 LEFT OUTER JOIN ( SELECT plan_number, participant_id, date_format(MAX(source_cycle_timestamp), 'yyyy-MM-dd') AS source_cycle_date FROM ods_retirement_account GROUP BY plan_number, participant_id ) AS D2 ON coalesce(D1.plan_number, '-9999') = coalesce(D2.plan_number, '-9999') AND coalesce(D1.participant_id, '-9999') = coalesce(D2.participant_id, '-9999') AND coalesce(D1.source_cycle_date, '-9999') = coalesce(D2.source_cycle_date, '-9999')")

    transform_df = ods_retirement_account_df

    return transform_df