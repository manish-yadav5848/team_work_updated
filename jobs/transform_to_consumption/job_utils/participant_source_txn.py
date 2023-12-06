
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, batch_date: str, primary_key: list):

    consumption_df = spark.sql("SELECT   z.plan_number,   z.participant_id,   z.client_id,   z.source,   z.trade_date,   CASE     WHEN pf.fund_number = '' then '-9999'     when pf.fund_number is null then '-9999'     else pf.fund_number   end as fund_number,   z.rev_code,   cast(z.total_unit_shares as DECIMAL(23, 2)) as total_unit_shares,   z.source_cycle_date,   cast(null as VARCHAR(36)) as client_key,   cast(null as VARCHAR(36)) as plan_key,   cast(null as VARCHAR(36)) as participant_key,   cast(null as VARCHAR(36)) as loan_key from   (     SELECT       T4.plan_number,       T4.participant_id,       T4.client_id,       T4.source,       T4.trade_date,       T4.fund_number,       T4.rev_code,       T4.total_unit_shares,       T4.source_cycle_date     from       (         select           coalesce(t3.source, '-9999') as source,           coalesce(t3.participant_id, '-9999') as participant_id,           coalesce(t3.client_id, '-9999') as client_id,           coalesce(t3.plan_number, '-9999') as plan_number,           coalesce(t3.fund_number, '-9999') as fund_number,           coalesce(t3.rev_code, '-9999') as rev_code,           coalesce(t3.total_unit_shares, 0.00) as total_unit_shares,           coalesce(t3.source_cycle_date, current_date() -1) as source_cycle_date,           coalesce(t3.trade_date, current_date() -1) as trade_date,           ch.mo_last_business_day         from           (             SELECT               ps.plan_number,               ps.participant_id,               ps.money_source as source,               ps.client_id,               ps.source_cycle_date,               T2.fund_number,               T2.rev_code,               T2.trade_date,               T2.total_unit_shares             from               participant_source ps               left outer join (                 SELECT                   T1.plan_number as plan_number,                   T1.trade_date as trade_date,                   T1.participant_id as participant_id,                   T1.fund_number as fund_number,                   T1.rev_code as rev_code,                   sum(T1.shares) as total_unit_shares                 from                   (                     select                       fund_number,                       rev_code,                       trade_date,                       shares,                       plan_number,                       participant_id                     from                       txn                   ) T1                 group by                   plan_number,                   trade_date,                   participant_id,                   fund_number,                   rev_code               ) T2 ON coalesce(ps.plan_number, '-9999') = coalesce(T2.plan_number)               AND coalesce(ps.participant_id, '-9999') = coalesce(T2.participant_id, '-9999')           ) T3           left outer join calendar_helper as ch on coalesce(t3.source_cycle_date, current_date() -1) = coalesce(ch.mo_last_business_day, current_date() -1)         where           mo_last_business_day is null       ) T4   ) z   left outer join(select plan_number,fund_number,count(1) from plan_fund where plan_number<>'-9999' or fund_number<>'' or fund_number<>'-9999' group by plan_number,fund_number  )pf on coalesce(z.plan_number, '-9999') = coalesce(pf.plan_number, '-9999');")

    return consumption_df