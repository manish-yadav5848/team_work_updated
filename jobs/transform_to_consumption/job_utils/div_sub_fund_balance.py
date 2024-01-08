
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, batch_date: str, primary_key: list):

    consumption_df = spark.sql("select coalesce(m1.client_id, '-9999') as client_id, coalesce(m1.plan_number, '-9999') as plan_number, coalesce(nullif(m1.div_sub_id, ''), '-9999') as div_sub_id, coalesce(nullif(m1.fund_number, ''), '-9999') as fund_number, pcb1.fund_iv, pcb1.fund_name, case when m1.fund_number='-9999' or m1.fund_number is null or m1.fund_number='' then 0 else cast(pcb1.share_price[0] as decimal (17,6)) end as share_price , cast(m1.cash_value_amount as decimal(17, 2)) as cash_value_amount, cast(m1.total_units as decimal(17, 4)) as total_units, cast(m1.ee_cash_value_amount as decimal(17, 2)) as ee_cash_value_amount, cast(m1.vested_balance as decimal(17, 2)) as vested_balance, cast(m1.ytd_contributions as decimal(17, 2)) as ytd_contributions, cast(m1.er_cash_value_amount as decimal(17, 2)) as er_cash_value_amount, m1.source_cycle_date, cast(null as varchar(36)) as client_key, cast(null as varchar(36)) as consumption_hash_key, cast(null as varchar(36)) as plan_fund_key, cast(null as varchar(36)) as plan_key from ( select p1.client_id, p1.plan_number, p1.source_cycle_date, p1.div_sub_id, p1.fund_number, sum(p1.core_cash_value_amount) as cash_value_amount, sum(total_shares) as total_units, sum(coalesce(vested_balance, 0.00)) as vested_balance, sum(coalesce(ytd_contributions, 0.00)) as ytd_contributions, sum(coalesce(ee_cash_value_amount, 0.00)) as ee_cash_value_amount, sum(coalesce(er_cash_value_amount, 0.00)) as er_cash_value_amount from ( select pcb.client_id, pcb.plan_number, trim(p.div_sub_id) as div_sub_id, trim(pcb.fund_number) as fund_number, pcb.source_cycle_date, pcb.total_shares, pcb.vested_balance, pcb.ytd_contributions, coalesce(pcb.cash_value_amount, 0.00) as core_cash_value_amount, case when money_type_description = 'EE' then cash_value_amount end as ee_cash_value_amount, case when money_type_description = 'ER' then cash_value_amount end as er_cash_value_amount from participant_core_balance pcb left outer join participant p on pcb.client_id = p.client_id and pcb.plan_number = p.plan_number and pcb.participant_id = p.participant_id ) as p1 group by p1.client_id, p1.plan_number, p1.div_sub_id, p1.fund_number, p1.source_cycle_date ) m1 left outer join( select client_id, plan_number, fund_iv, fund_number, fund_name, collect_set(share_price) as share_price from participant_core_balance group by client_id, plan_number, fund_iv, fund_number, fund_name ) as pcb1 on coalesce(m1.client_id, '-9999') = coalesce(pcb1.client_id, '-9999') and coalesce(m1.plan_number, '-9999') = coalesce(pcb1.plan_number, '-9999') and coalesce(m1.fund_number, '-9999') = coalesce(pcb1.fund_number, '-9999') where div_sub_id <> '' and div_sub_id <> '-9999' order by plan_number desc, div_sub_id desc")

    return consumption_df
