
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, batch_date: str, primary_key: list):

    consumption_df = spark.sql("select   coalesce(q1.client_id, '-9999') as client_id,   coalesce(q1.plan_number, '-9999') as plan_number,   coalesce(q1.participant_id, '-9999') as participant_id,   q1.source_cycle_date as source_cycle_date,   fund_units,   fund_amount from   (     select       coalesce(t1.client_id, '-9999') as client_id,       coalesce(t1.plan_number, '-9999') as plan_number,       coalesce(t1.participant_id, '-9999') as participant_id,       t1.source_cycle_date as source_cycle_date,       CAST(SUM(t1.total_shares) AS DECIMAL(17, 4)) as fund_units,       CAST(SUM(t1.total_balance) AS DECIMAL(18, 2)) as fund_amount     from       (         SELECT           coalesce(np.client_id, '-9999') as client_id,           coalesce(npf.plan_number, '-9999') as plan_number,           coalesce(npf.participant_id, '-9999') as participant_id,           coalesce(npf.total_shares, 0.0000) as total_shares,           coalesce(npf.total_balance, 0.00) as total_balance,           CAST(npf.source_cycle_date AS DATE) as source_cycle_date         from           nq_participant as np           RIGHT OUTER JOIN nq_participant_fund_monthly as npf ON coalesce(npf.plan_number, '-9999') = coalesce(np.plan_number, '-9999')           AND coalesce(npf.participant_id, '-9999') = coalesce(np.participant_id, '-9999')       ) as t1     group by       t1.client_id,       t1.plan_number,       t1.participant_id,       source_cycle_date   ) q1 where month(q1.source_cycle_date) in (3,9,6,12)")

    return consumption_df
