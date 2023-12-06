from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    consumption_df = spark.sql("select coalesce(m1.client_id, -9999) as client_id, coalesce(nullif(m1.plan_number, ''), -9999) as plan_number, m1.source_cycle_date as source_cycle_date, coalesce(nullif(m1.money_source , ''), '-9999') as money_source, m1.money_source_type_code, cast(m1.ytd_contributions as decimal(17,2)) as ytd_contributions, cast(m1.cash_value_amount as decimal(13,2)) as cash_value_amount, cast(m1.vested_balance as decimal(13,2)) as vested_balance, cast(null as varchar(36)) as plan_key, cast(null as varchar(36)) as plan_source_key from ( select trim(p1.plan_number) as plan_number, p1.source_cycle_date, trim(p1.client_id) as client_id, trim(p1.money_source) as money_source, p1.money_type_description as money_source_type_code, sum(coalesce(p1.cash_value_amount,0)) as cash_value_amount, sum(coalesce(p1.vested_balance,0)) as vested_balance, sum(coalesce(p1.ytd_contributions,0)) as ytd_contributions from participant_core_balance_monthly_updated_12_31_tmp p1 group by p1.client_id, p1.plan_number, p1.source_cycle_date, p1.money_source, p1.money_type_description ) m1")

    return consumption_df