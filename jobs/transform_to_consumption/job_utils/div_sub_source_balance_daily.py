
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, batch_date: str, primary_key: list):

    consumption_df = spark.sql("select coalesce(CLIENT_ID,'-9999') as client_id, coalesce(PLAN_NUMBER,'-9999') as PLAN_NUMBER, coalesce(DIV_SUB_ID,'-9999') as DIV_SUB_ID, coalesce(MONEY_SOURCE,'-9999') as MONEY_SOURCE, coalesce(SOURCE_CYCLE_DATE,'-9999') as SOURCE_CYCLE_DATE, cast(TOTAL_UNITS as decimal(17,4)) as TOTAL_UNITS, cast(CASH_VALUE_AMOUNT as decimal(17,2)) as CASH_VALUE_AMOUNT, cast(vested_balance as decimal(17,2)) as vested_balance from (select pcb.client_id,pcb.plan_number,pt.div_sub_id,pcb.money_source,pcb.source_cycle_date, sum(pcb.vested_balance) as vested_balance , sum(pcb.cash_value_amount) as cash_value_amount ,sum(pcb.total_shares) as total_units from participant_core_balance pcb left outer join participant pt on pt.plan_number=pcb.plan_number and pt.participant_id=pcb.participant_id where pcb.source_system in ('VRP-PB','VRP-SP') and div_sub_id is not null group by pcb.client_id,pcb.plan_number,pt.div_sub_id,pcb.money_source,pcb.source_cycle_date)")
    # consumption_df_2=spark.sql("select coalesce(CLIENT_ID,'-9999') as client_id, coalesce(plan_id,'-9999') as PLAN_NUMBER, coalesce(DIV_SUB_ID,'-9999') as DIV_SUB_ID, coalesce(money_src,'-9999') as MONEY_SOURCE, cast(as_of_date as date) as SOURCE_CYCLE_DATE, cast(unit_share_bal as decimal(17,4)) as TOTAL_UNITS, cast(source_bal_amt as decimal(17,2)) as CASH_VALUE_AMOUNT, cast(source_vested_bal as decimal(17,2)) as vested_balance from newr_raw.div_sub_src_bal_hist where div_sub_id <>'PLAN'")

    return consumption_df