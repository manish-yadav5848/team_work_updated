
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, batch_date: str, primary_key: list):

    consumption_df = spark.sql("select coalesce(M1.rep_tax_id_ssn, '-9999') as rep_tax_id_ssn, coalesce(M1.plan_number, '-9999') as plan_number, M1.producer_role_code, M1.producer_role_type, cast( coalesce(M1.core_cash_value_amount, 0) as DECIMAL(17, 2) ) as core_cash_value_amount, cast( ( coalesce(M1.sdba_cash_value_amount, 0) + coalesce(M1.total_loan_balance, 0) + coalesce(M1.life_cash_value_amount_monthly, 0) ) as DecimAL(17, 2) ) as noncore_cash_value_amount, cast( coalesce(M1.ee_cash_value_amount, 0) as DECIMAL(17, 2) ) as ee_cash_value_amount, cast( coalesce(M1.er_cash_value_amount, 0) as DECIMAL(17, 2) ) as er_cash_value_amount, cast( coalesce(M1.sdba_cash_value_amount, 0) as DECIMAL(17, 2) ) as sdba_cash_value_amount, cast( coalesce(M1.total_loan_balance, 0) as DECIMAL(17, 2) ) as loan_cash_value_amount, cast( coalesce(M1.life_cash_value_amount_monthly, 0) as DECIMAL(17, 2) ) as life_cash_value_amount_monthly, M2.valuation_date as life_valuation_date, cast( coalesce(M1.ytd_contributions, 0) as DECIMAL(17, 2) ) as ytd_contributions, case when ( M1.allocated_participant_count - M1.is_participant_no_count ) > 0 then coalesce( cast( ( M1.allocated_participant_count - M1.is_participant_no_count ) as INTEGER ), 0 ) else 0 end as allocated_participant_count, cast(M1.active_participant_count as integer) as active_participant_count, cast(M1.participant_count as integer) as participant_count, cast(M1.is_participant_no_count as integer) as is_participant_no_count, coalesce(M1.source_cycle_date, current_date() -1) as source_cycle_date from ( ( select P.rep_tax_id_ssn, P.plan_number, coalesce(B.source_cycle_date, current_date() -1) as source_cycle_date, B.core_cash_value_amount, B.ee_cash_value_amount, B.er_cash_value_amount, B.ytd_contributions, B.sdba_cash_value_amount, B.total_loan_balance, B.life_cash_value_amount_monthly, P.producer_role_code, P.producer_role_type, C.active_participant_count, C.participant_count, C.allocated_participant_count, C.is_participant_no_count from ( select rep_tax_id_ssn, concat_ws ( ';', collect_set (distinct (nullif(producer_role_code_desc, ''))) ) as producer_role_code, concat_ws ( ';', collect_set ( distinct ( nullif( case when producer_role_type = 'RIA/IAR' then producer_role_type else substring(producer_role_type, 0, 3) end, '' ) ) ) ) as producer_role_type, plan_number from producer where AGREEMENT_LEVEL_CODE = 'PLAN' group by rep_tax_id_ssn, plan_number ) P inner join ( select W.plan_number, W.source_cycle_date, W.core_cash_value_amount, W.ee_cash_value_amount, W.er_cash_value_amount, W.ytd_contributions, W.sdba_cash_value_amount, W.total_loan_balance, B3.life_cash_value_amount_monthly from ( select coalesce(B1.plan_number, B2.plan_number) as plan_number, coalesce(B1.source_cycle_date, current_date() -1) as source_cycle_date, B1.core_cash_value_amount, B1.ee_cash_value_amount, B1.er_cash_value_amount, B1.ytd_contributions, B1.sdba_cash_value_amount, B2.total_loan_balance from ( SELECT coalesce(t1.plan_number, '-9999') as plan_number, coalesce(t1.source_cycle_date, current_date() -1) as source_cycle_date, sum(coalesce(cash_value, 0)) as core_cash_value_amount, sum(coalesce(ee_cash_value, 0)) as ee_cash_value_amount, sum(coalesce(er_cash_value, 0)) as er_cash_value_amount, sum(coalesce(ytd_contributions, 0)) as ytd_contributions, sum(coalesce(sdba_cash_value_amount, 0)) as sdba_cash_value_amount from ( SELECT pc.plan_number, pc.source_cycle_date, coalesce(pc.cash_value_amount, 0) as cash_value, coalesce(pc.sdba_cash_value_amount, 0) as sdba_cash_value_amount, coalesce(pc.ytd_contributions, 0) as ytd_contributions, case when pc.cash_value_amount = null then 0 when pc.money_type_description = 'EE' then cash_value_amount end as ee_cash_value, case when pc.cash_value_amount = null then 0 when pc.money_type_description = 'ER' then cash_value_amount end as er_cash_value from participant_core_balance pc ) as T1 GROUP BY plan_number, source_cycle_date ) B1 full outer join ( select t3.plan_number as plan_number, sum(coalesce(t3.total_loan_balance, 0)) as total_loan_balance from ( SELECT coalesce(ppl.plan_number, '-9999') as plan_number, coalesce(ppl.source_cycle_date, current_date() -1) as source_cycle_date, case when ppl.source_system = 'PREMIER' then ppl.outstanding_principal_balance else loan_balance end as total_loan_balance from participant_loan as ppl ) as t3 group by plan_number ) as B2 On coalesce(B1.plan_number, '-9999') = coalesce(B2.plan_number, '-9999') ) W left outer join ( select plan_number, sum(coalesce(cash_value_amt, 0)) as life_cash_value_amount_monthly, valuation_date as life_valuation_date from participant_life_fund_monthly group by plan_number, valuation_date ) as B3 on coalesce(W.plan_number, '-9999') = coalesce(B3.plan_number, '-9999') ) as B On coalesce(P.plan_number, '-9999') = coalesce(B.plan_number, '-9999') left outer join ( select plan_number, count(distinct(allocated_participant_count)) as allocated_participant_count, count(distinct(Active_participant_count)) as Active_participant_count, count(distinct(participant_count)) as participant_count, count(distinct(is_participant_no_count)) as is_participant_no_count from ( select pb.plan_number, case when pt.source_system in ('VRP-PB', 'VRP-SP') and ACCOUNT_TYPE_CODE = 'ALLOC' then pb.participant_id when pt.source_system = 'PREMIER' then pb.participant_id end as allocated_participant_count, case when pt.source_system in ('VRP-PB', 'VRP-SP') and pt.PARTICIPANT_STATUS_code <= 16 then pb.participant_id when pt.source_system = 'PREMIER' and pt.PARTICIPANT_STATUS_code = 'A' then pb.participant_id end as Active_participant_count, pb.participant_id as participant_count, case when pt.is_participant = 'N' then pb.participant_id end as is_participant_no_count from ( select distinct coalesce(pcb.plan_number, pl.plan_number) as plan_number, coalesce(pcb.participant_id, pl.participant_id) as participant_id from participant_core_balance pcb full outer join participant_loan as pl on pcb.plan_number = pl.plan_number and pcb.participant_id = pl.participant_id ) as pb inner join participant pt on pb.plan_number = pt.plan_number and pb.participant_id = pt.participant_id ) group by plan_number ) as C on coalesce(B.plan_number, '-9999') = coalesce(C.plan_number, '-9999') ) union ( select m1.rep_tax_id_ssn, m1.plan_number, coalesce(m1.source_cycle_date, current_date() -1) as source_cycle_date, sum(coalesce(m1.cash_value, 0)) as core_cash_value_amount, sum(coalesce(m1.ee_cash_value, 0)) as ee_cash_value_amount, sum(coalesce(m1.er_cash_value, 0)) as er_cash_value_amount, sum(coalesce(m1.ytd_contributions, 0)) as ytd_contributions, sum(coalesce(m1.brokerage_account_cash_value, 0)) as sdba_cash_value_amount, sum(coalesce(m1.total_loan_balance, 0)) as total_loan_balance, sum(coalesce(m1.cash_value_amt, 0)) as life_cash_value_amount_monthly, concat_ws ( ';', array_distinct ( flatten (collect_set (distinct (producer_role_code))) ) ) as producer_role_code, concat_ws ( ';', array_distinct ( flatten (collect_set (distinct (producer_role_type))) ) ) as producer_role_type, count(distinct (m1.active_participant_count)) as active_participant_count, count(distinct (m1.participant_count)) as participant_count, count(distinct (m1.allocated_participant_count)) as allocated_participant_count, count(distinct (m1.is_participant_no_count)) as is_participant_no_count from ( SELECT pr.rep_tax_id_ssn, pr.producer_role_code, pr.producer_role_type, W.participant_id, pr.plan_number, W.source_cycle_date, W.cash_value, W.brokerage_account_cash_value, W.ytd_contributions, W.er_cash_value, W.ee_cash_value, W.total_loan_balance, plf.cash_value_amt, pt.participant_id as participant_count, pt.active_participant_count, pt.allocated_participant_count, pt.is_participant_no_count from ( select rep_tax_id_ssn, participant_id, retirement_account_id, collect_set (distinct (nullif(producer_role_code_desc, ''))) as producer_role_code, collect_set ( distinct ( nullif( case when producer_role_type = 'RIA/IAR' then producer_role_type else substring(producer_role_type, 0, 3) end, '' ) ) ) as producer_role_type, plan_number from producer where AGREEMENT_LEVEL_CODE = 'MSRC' OR AGREEMENT_LEVEL_CODE = 'PART' group by rep_tax_id_ssn, participant_id, plan_number, retirement_account_id ) pr inner join ( select coalesce(pc.participant_id, pl.participant_id) as participant_id, coalesce(pc.plan_number, pl.plan_number) as plan_number, coalesce(pc.source_cycle_date, current_date() -1) as source_cycle_date, coalesce(pc.cash_value, 0) as cash_value, coalesce(pc.brokerage_account_cash_value, 0) as brokerage_account_cash_value, coalesce(pc.ytd_contributions, 0.00) as ytd_contributions, coalesce(pc.er_cash_value, 0) as er_cash_value, coalesce(pc.ee_cash_value, 0) as ee_cash_value, coalesce( pc.retirement_account_id, pl.retirement_account_id ) as retirement_account_id, coalesce(pl.total_loan_balance, 0) as total_loan_balance from ( select participant_id, plan_number, source_cycle_date, retirement_account_id, sum(cash_value_amount) as cash_value, sum(er_cash_value) as er_cash_value, sum(ee_cash_value) as ee_cash_value, sum(sdba_cash_value_amount) as brokerage_account_cash_value, sum(ytd_contributions) as ytd_contributions from ( select source_cycle_date, participant_id, plan_number, retirement_account_id, sdba_cash_value_amount, ytd_contributions, cash_value_amount, case when money_type_description = 'EE' then cash_value_amount end as ee_cash_value, case when money_type_description = 'ER' then cash_value_amount end as er_cash_value from participant_core_balance ) group by participant_id, plan_number, source_cycle_date, retirement_account_id ) pc full outer join ( select plan_number, participant_id, retirement_account_id, sum(total_loan_balance) as total_loan_balance from ( SELECT coalesce(plan_number, '-9999') as plan_number, coalesce(participant_id, '-9999') as participant_id, RETIREMENT_ACCOUNT_ID, case when source_system = 'PREMIER' then outstanding_principal_balance else loan_balance end as total_loan_balance from participant_loan ) group by plan_number, participant_id, retirement_account_id ) as pl on coalesce(pc.plan_number, '-9999') = coalesce(pl.plan_number, '-9999') and coalesce(pc.participant_id, '-9999') = coalesce(pl.participant_id, '-9999') and coalesce(pc.retirement_account_id, '-9999') = coalesce(pl.retirement_account_id, '-9999') ) W on coalesce(W.plan_number, '-9999') = coalesce(pr.plan_number, '-9999') and coalesce(substring(W.participant_id, 1, 13), '-9999') = coalesce(substring(pr.participant_id, 1, 13), '-9999') and coalesce(W.retirement_account_id, '-9999') = coalesce(pr.retirement_account_id, '-9999') left outer join ( select case when source_system = 'VRP-PB' or source_system = 'VRP-SP' then case when PARTICIPANT_STATUS_CODE <= 16 then participant_id end when source_system = 'PREMIER' then case when PARTICIPANT_STATUS_CODE = 'A' then participant_id end end as Active_participant_count, case when source_system = 'VRP-PB' OR source_system = 'VRP-SP' then case when ACCOUNT_TYPE_CODE = 'ALLOC' then p.participant_id end when source_system = 'PREMIER' then participant_id end as allocated_participant_count, case when is_participant = 'N' then participant_id end as is_participant_no_count, p.participant_id, p.plan_number from participant p ) as pt on coalesce(W.plan_number, '-9999') = coalesce(pt.plan_number, '-9999') and coalesce(W.participant_id, '-9999') = coalesce(pt.participant_id, '-9999') left outer join ( select plan_number, participant_id, sum(cash_value_amt) as cash_value_amt from participant_life_fund_monthly group by plan_number, participant_id ) as plf on coalesce(W.plan_number, '-9999') = coalesce(plf.plan_number, '-9999') and coalesce(W.participant_id, '-9999') = coalesce(plf.participant_id, '-9999') ) m1 group by m1.rep_tax_id_ssn, m1.plan_number, source_cycle_date ) ) as M1 left outer join ( select plan_number, valuation_date from participant_life_fund_monthly group by plan_number, valuation_date ) as M2 on M1.plan_number = M2.plan_number")

    consumption_df.createOrReplaceTempView("pr_plan_bal_temp")

    comsumption_final_df = spark.sql("select * from pr_plan_bal_temp where not ((core_cash_value_amount+noncore_cash_value_amount)<=0 and (participant_count-is_participant_no_count)<=0)")
    return comsumption_final_df
