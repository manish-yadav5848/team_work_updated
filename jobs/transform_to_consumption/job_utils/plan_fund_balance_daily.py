
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, batch_date: str, primary_key: list):

    consumption_df = spark.sql("select coalesce(CLIENT_ID, '-9999') as client_id, coalesce(PLAN_NUMBER, '-9999') as plan_number, coalesce(fund_iv, '-9999') as fund_iv, coalesce(SOURCE_CYCLE_DATE, current_date() -1) as source_cycle_date, cast(TOTAL_UNITS as decimal(17, 4)) as total_units, case when fund_iv is null or fund_iv in ('-9999', '') then 0 else cast(share_price [0] as DECIMAL(17, 6)) end as share_price, cast(CASH_VALUE_AMOUNT as decimal(17, 2)) as cash_value_amount, cast(vested_balance as decimal(17, 2)) as vested_balance from ( select pcb.client_id, pcb.plan_number, pcb.fund_iv, pcb.source_cycle_date, collect_set(share_price) as share_price, sum(pcb.vested_balance) as vested_balance, sum(pcb.cash_value_amount) as cash_value_amount, sum(pcb.total_shares) as total_units from participant_core_balance pcb where pcb.source_system in ('VRP-PB', 'VRP-SP') group by pcb.client_id, pcb.plan_number, pcb.fund_iv, pcb.source_cycle_date )")
    # consumption_df_1=spark.sql("select client_id,plan_id as plan_number, fund_id as fund_iv,cast(as_of_date as date) as source_cycle_date,cast(unit_price as decimal(17,6)) as share_price,cast(unit_share_bal as decimal(17,4)) as total_units,cast(fund_bal_amt as decimal(17, 2)) as CASH_VALUE_AMOUNT,cast(fund_vested_bal as decimal(17, 2)) as vested_balance from newr_raw.div_sub_fund_bal_hist where div_sub_id='PLAN'")

    return consumption_df