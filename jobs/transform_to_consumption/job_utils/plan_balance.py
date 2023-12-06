
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, batch_date: str, primary_key: list):

    participant_core_balance_df = spark.sql("select coalesce(m1.client_id, '-9999') as client_id, coalesce(nullif(m1.plan_number, ''), '-9999') as plan_number, coalesce(m1.core_cash_value_amount, 0.00) as core_cash_value_amount, coalesce(m1.noncore_cash_value_amount, 0.00) as noncore_cash_value_amount, coalesce(m1.sdba_cash_value_amount, 0.00) as sdba_cash_value_amount, coalesce(m1.life_cash_value_amount, 0.00) as life_cash_value_amount_monthly, m1.life_valuation_date, coalesce(m1.loan_cash_value_amt, 0.00) as loan_cash_value_amount, coalesce(m1.ee_cash_value_amount, 0.00) as ee_cash_value_amount, coalesce(m1.er_cash_value_amount, 0.00) as er_cash_value_amount, coalesce(m1.participant_count, 0) as participant_count, coalesce(m1.active_participant_count, 0) as active_participant_count, coalesce(m1.is_participant_no_count, 0) as is_participant_no_count, case when ( m1.allocated_participant_count - m1.is_participant_no_count ) > 0 then coalesce( m1.allocated_participant_count - m1.is_participant_no_count, 0 ) else 0 end as allocated_participant_count, m1.ytd_contributions, coalesce(m1.source_cycle_date, current_date() -1) as source_cycle_date, cast(null as VARCHAR(36)) as plan_key from ( select T.source_cycle_date, T.client_id, T.plan_number, t3.valuation_date as life_valuation_date, cast(T.core_cash_value_amount as DECIMAL(17, 2)) as core_cash_value_amount, cast(T.ee_cash_value_amount as DECIMAL(17, 2)) as ee_cash_value_amount, cast(T.er_cash_value_amount as DECIMAL(17, 2)) as er_cash_value_amount, cast(t3.life_cash_value_amount as DECIMAL(13, 2)) as life_cash_value_amount, cast(T.sdba_cash_value_amount as DECIMAL(17, 2)) as sdba_cash_value_amount, cast(T.loan_cash_value_amt as DECIMAL(17, 2)) as loan_cash_value_amt, cast( ( coalesce(T.sdba_cash_value_amount, 0.00) + coalesce(T.loan_cash_value_amt, 0.00) + coalesce(t3.life_cash_value_amount, 0.00) ) as DECIMAL(13, 2) ) as noncore_cash_value_amount, cast(c1.allocated_participant_count as INTEGER) as allocated_participant_count, cast(c1.active_participant_count as INTEGER) as active_participant_count, cast(c1.participant_count as INTEGER) as participant_count, cast(c1.is_participant_no_count as INTEGER) as is_participant_no_count, cast(T.ytd_contributions as DECIMAL(17, 2)) as ytd_contributions from ( select coalesce(t1.client_id, t4.client_id) as client_id, coalesce(t1.plan_number, t4.plan_number) as plan_number, t1.core_cash_value_amount, t1.ee_cash_value_amount, t1.er_cash_value_amount, t1.sdba_cash_value_amount, t1.ytd_contributions, t4.loan_cash_value_amt, t1.source_cycle_date from ( select p1.client_id, p1.plan_number, p1.source_cycle_date, sum(p1.core_cash_value_amount) as core_cash_value_amount, sum(coalesce(ee_cash_value_amount, 0.00)) as ee_cash_value_amount, sum(coalesce(er_cash_value_amount, 0.00)) as er_cash_value_amount, sum(sdba_cash_value_amount) as sdba_cash_value_amount, sum(p1.ytd_contributions) as ytd_contributions from ( select pcb.client_id, trim(pcb.plan_number) as plan_number, pcb.source_cycle_date, case when fund_iv in ('70', '90', '91') and pcb.client_id <> 'NG' then 0.0 else coalesce(pcb.cash_value_amount, 0.00) end as core_cash_value_amount, case when money_type_description = 'EE' then cash_value_amount end as ee_cash_value_amount, case when money_type_description = 'ER' then cash_value_amount end as er_cash_value_amount, coalesce(pcb.sdba_cash_value_amount, 0.00) as sdba_cash_value_amount, coalesce(pcb.ytd_contributions, 0.00) as ytd_contributions from participant_core_balance pcb ) p1 group by p1.client_id, p1.plan_number, p1.source_cycle_date ) t1 full outer join ( select k.client_id, k.plan_number, sum(coalesce(k.loan_cash_value_amt, 0.00)) as loan_cash_value_amt from ( select pl.client_id, pl.plan_number, case when pl.source_system = 'PREMIER' THEN outstanding_principal_balance ELSE loan_balance end as loan_cash_value_amt from participant_loan pl ) k where k.loan_cash_value_amt > 0 group by k.plan_number, k.client_id ) t4 on coalesce(t1.client_id, -9999) = coalesce(t4.client_id, -9999) and coalesce(t1.plan_number, -9999) = coalesce(t4.plan_number, -9999) ) T left outer join ( select sum(coalesce(cash_value_amt, 0)) as life_cash_value_amount, plan_number, valuation_date from participant_life_fund_monthly group by plan_number, valuation_date ) as t3 on coalesce(T.plan_number, -9999) = coalesce(t3.plan_number, -9999) left outer join ( select plan_number, count(distinct(allocated_participant_count)) as allocated_participant_count, count(distinct(Active_participant_count)) as Active_participant_count,count(distinct(participant_count)) as participant_count, count(distinct(is_participant_no_count)) as is_participant_no_count from (select pb.plan_number, case when pt.source_system in ('VRP-PB','VRP-SP') and  ACCOUNT_TYPE_CODE = 'ALLOC' then pb.participant_id when pt.source_system = 'PREMIER' then pb.participant_id end as  allocated_participant_count, case when pt.source_system in ('VRP-PB','VRP-SP') and  pt.PARTICIPANT_STATUS_code <= 16 then pb.participant_id when pt.source_system = 'PREMIER' and  pt.PARTICIPANT_STATUS_code = 'A' then pb.participant_id end as Active_participant_count, pb.participant_id as participant_count, case when pt.is_participant = 'N' then pb.participant_id end as is_participant_no_count from (select    distinct coalesce(pcb.plan_number,pl.plan_number) as plan_number, coalesce(pcb.participant_id,pl.participant_id) as participant_id from  participant_core_balance pcb full outer join participant_loan as pl on pcb.plan_number=pl.plan_number and pcb.participant_id=pl.participant_id ) as pb inner join   participant pt on pb.plan_number=pt.plan_number and pb.participant_id=pt.participant_id ) group by plan_number ) as c1 on coalesce(T.plan_number, -9999) = coalesce(c1.plan_number, -9999) ) m1")

    rps_balance_df = spark.sql("select   coalesce(t1.client_id, '-9999') as client_id,   coalesce(nullif(t1.plan_number, ''), '-9999') as plan_number,   t1.source_cycle_date as source_cycle_date,   cast(t1.core_cash_value_amount as DECIMAL(17, 2)) as core_cash_value_amount,   cast(0.00 as DECIMAL(17, 2)) as ee_cash_value_amount,   cast(t1.core_cash_value_amount as DECIMAL(17, 2)) as er_cash_value_amount,   cast(0.00 as DECIMAL(13, 2)) as life_cash_value_amount_monthly,   cast(t1.core_cash_value_amount as DECIMAL(17, 2)) as sdba_cash_value_amount,   cast(0.00 as DECIMAL(17, 2)) as loan_cash_value_amount,   cast(t1.core_cash_value_amount as DECIMAL(13, 2)) as noncore_cash_value_amount,   cast(t1.ytd_contributions as DECIMAL(17, 2)) as ytd_contributions,   c1.participant_count,   cast(0 as integer) as active_participant_count,   cast(0 as integer) as allocated_participant_count,  cast(0 as integer) as is_participant_no_count,  cast(null as date) as life_valuation_date,   cast(null as VARCHAR(36)) as plan_key from   (     select       p1.client_id,       p1.plan_number,       p1.source_cycle_date,       sum(p1.core_cash_value_amount) as core_cash_value_amount,       sum(p1.ytd_contributions) as ytd_contributions     from       (         select           pcb.client_id,           pcb.plan_number,           pcb.source_cycle_date,           coalesce(pcb.cash_value, 0.00) as core_cash_value_amount,     coalesce(pcb.ytd_contributions, 0.00) as ytd_contributions         from           rps_balance pcb       ) p1     group by       p1.client_id,       p1.plan_number,       p1.source_cycle_date   ) t1   left outer join ( select       plan_number,       count(distinct(participant_id)) as participant_count     from       participant     group by       plan_number   ) as c1 on coalesce(t1.plan_number, -9999) = coalesce(c1.plan_number, -9999)")

    consumption_df = participant_core_balance_df.unionByName(rps_balance_df)

    return consumption_df
