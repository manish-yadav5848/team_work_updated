
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, batch_date: str, primary_key: list):

    consumption_df = spark.sql("select coalesce(nullif(m1.client_id, ''), '-9999') as client_id, coalesce(nullif(m1.plan_number, ''), '-9999') as plan_number, coalesce(nullif(m1.fund_number, ''), '-9999') as fund_number, coalesce(m1.source_cycle_date, current_date() -1) as source_cycle_date, pcb1.fund_iv, pf1.fund_name, cast(m1.cash_value_amount as decimal(17, 2)) as cash_value_amount, cast(m1.total_units as decimal(17, 4)) as total_units, cast(m1.ee_cash_value_amount as decimal(17, 2)) as ee_cash_value_amount, cast(m1.vested_balance as decimal(17, 2)) as vested_balance, cast(m1.ytd_contributions as decimal(17, 2)) as ytd_contributions, cast(m1.er_cash_value_amount as decimal(17, 2)) as er_cash_value_amount, case when m1.fund_number is null or m1.fund_number in ('-9999','') then 0 else cast(m1.share_price[0] as DECIMAL(17,6)) end as share_price, cast(null as varchar(36)) as client_key, cast(null as varchar(36)) as plan_fund_key, cast(null as varchar(36)) as plan_key from ( select p1.client_id, p1.plan_number, p1.source_cycle_date, p1.fund_number, sum(p1.core_cash_value_amount) as cash_value_amount, sum(total_shares) as total_units, sum(coalesce(ee_cash_value_amount, 0.00)) as ee_cash_value_amount, sum(coalesce(vested_balance, 0.00)) as vested_balance, sum(coalesce(ytd_contributions, 0.00)) as ytd_contributions, sum(coalesce(er_cash_value_amount, 0.00)) as er_cash_value_amount, collect_set(share_price) as share_price from ( select pcb.client_id, trim(pcb.plan_number) as plan_number, trim(pcb.fund_number) as fund_number, pcb.source_cycle_date, pcb.total_shares, pcb.vested_balance, pcb.share_price, coalesce(pcb.cash_value_amount, 0.00) as core_cash_value_amount, coalesce(pcb.ytd_contributions, 0.00) as ytd_contributions, case when money_type_description = 'EE' then cash_value_amount end as ee_cash_value_amount, case when money_type_description = 'ER' then cash_value_amount end as er_cash_value_amount from participant_core_balance pcb ) as p1 group by p1.client_id, p1.plan_number, p1.fund_number, p1.source_cycle_date ) m1 left outer join ( select client_id, plan_number, fund_iv, fund_number from participant_core_balance group by client_id, plan_number, fund_iv, fund_number ) as pcb1 on coalesce(m1.client_id, '-9999') = coalesce(pcb1.client_id, '-9999') and coalesce(m1.plan_number, '-9999') = coalesce(pcb1.plan_number, '-9999') and coalesce(m1.fund_number, '-9999') = coalesce(pcb1.fund_number, '-9999') left outer join ( select client_id, plan_number, fund_number, fund_name from participant_core_balance where source_system = 'PREMIER' or source_system = 'RPS' group by client_id, plan_number, fund_number, fund_name union select client_id, plan_number, fund_number, fund_name from plan_fund group by client_id, plan_number, fund_number, fund_name ) as pf1 on coalesce(m1.client_id, '-9999') = coalesce(pf1.client_id, '-9999') and coalesce(m1.plan_number, '-9999') = coalesce(pf1.plan_number, '-9999') and coalesce(m1.fund_number, '-9999') = coalesce(pf1.fund_number, '-9999') order by client_id desc, plan_number desc, fund_number desc")

    return consumption_df
