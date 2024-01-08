
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, batch_date: str, primary_key: list):

    consumption_df = spark.sql("select   pb.client_id,   pb.source_cycle_date,   pb.participant_id,   pb.plan_number,   pb.fund_units,   pb.fund_amount from (         SELECT           coalesce(npart.client_id, '-9999') as client_id,           coalesce(npf.plan_number, '-9999') as plan_number,           coalesce(npf.participant_id, '-9999') as participant_id,           CAST(SUM(npf.total_shares) AS DECIMAL (17, 4)) as fund_units,           CAST(SUM(npf.total_balance) AS DECIMAL(18, 2)) as fund_amount,           coalesce(             CAST(npf.source_cycle_date AS DATE),             current_date() -1           ) as source_cycle_date         FROM           nq_participant as npart           RIGHT OUTER JOIN nq_participant_fund as npf ON coalesce(npf.plan_number, '-9999') = coalesce(npart.plan_number, '-9999')           AND coalesce(npf.participant_id, '-9999') = coalesce(npart.participant_id, '-9999')           AND coalesce(npf.source_cycle_date, current_date() -1) = coalesce(npart.source_cycle_date, current_date() -1)         GROUP BY           npart.client_id,           npf.plan_number,           npf.participant_id,           npf.source_cycle_date       ) as pb ")

    return consumption_df