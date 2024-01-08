
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, batch_date: str, primary_key: list):

    consumption_df = spark.sql("select   cb.client_id,   cb.source_cycle_date,   cb.fund_units,   cb.fund_amount from       (         SELECT           coalesce(np.client_id, '-9999') as client_id,           CAST(sum(npf.total_shares) AS DECIMAL (17, 4)) as fund_units,           CAST(sum(npf.total_balance) AS DECIMAL(18, 2)) as fund_amount,           coalesce(             CAST(npf.source_cycle_date AS DATE),             current_date() -1           ) as source_cycle_date         FROM           nq_participant_fund as npf           RIGHT OUTER JOIN nq_plan as np ON coalesce(npf.plan_number, '-9999') = coalesce(np.plan_number, '-9999')           AND coalesce(npf.source_cycle_date, current_date() -1) = coalesce(np.source_cycle_date, current_date() -1)         GROUP BY           np.client_id,           npf.source_cycle_date       ) as cb")

    return consumption_df