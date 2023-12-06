
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    exn_xxewre01_xxewrdpg_jb_df = spark.sql("SELECT client_id,dpg.plan_number,dpg.div_sub_id,cast(null as varchar(36)) as div_sub_key,address_city,address_line_1,address_line_2,address_line_3,address_state,address_zip_code,primary_name,secondary_name,'VRP-SP' as source_system,source_cycle_date,case when cardinality(account_type_code)=1 and account_type_code[0]='ALLOC' then 'N' when cardinality(account_type_code)=-1 then cast(null as varchar(1)) else 'Y' end as PLAN_UNALLOCATED_FLAG FROM exn_xxewre01_xxewrdpg_jb dpg left outer join (select collect_set(account_type_code) as account_type_code,div_sub_id,plan_number from (select  case when upper(trim(sp.participant_account_type_code)) in ('ALLOC','ALOC') then 'ALLOC' else sp.participant_account_type_code end  as account_type_code, op.div_sub_id,op.plan_number from super_omni_newr_particpantaccount_daily_jb_dat sp right join exn_xxewre01_xxewrptd_jb op on op.part_number=sp.participant_id and op.plan_number=sp.plan_number where op.div_sub_id is not null or op.div_sub_id<> '' ) group by div_sub_id,plan_number ) as ptd on dpg.plan_number=ptd.plan_number and dpg.div_sub_id=ptd.div_sub_id ")

    super_omni_newr_plan_divsub_daily_ng_dat_df = spark.sql("SELECT client_id,dpg.plan_number,dpg.div_sub_id,cast(null as varchar(36)) as div_sub_key,address_city,address_line_1,address_line_2,address_line_3,address_state,address_zip_code,primary_name,secondary_name,'VRP-PB' as source_system,source_cycle_date,case when cardinality(account_type_code)=1 and account_type_code[0]='ALLOC' then 'N'  when cardinality(account_type_code)=-1 then cast(null as varchar(1)) else 'Y' end as PLAN_UNALLOCATED_FLAG FROM super_omni_newr_plan_divsub_daily_ng_dat dpg left outer join (select collect_set(account_type_code) as account_type_code,div_sub_id,plan_number from (select  case when upper(trim(sp.participant_account_type_code)) in ('ALLOC','ALOC') then 'ALLOC' else sp.participant_account_type_code end  as account_type_code, op.div_sub_id,op.plan_number from super_omni_newr_particpantaccount_daily_ng_dat sp right join exn_xxewre01_xxewrptd_ng op on op.part_number=sp.participant_id and op.plan_number=sp.plan_number where op.div_sub_id is not null or op.div_sub_id<> '' ) group by div_sub_id,plan_number ) as ptd on dpg.plan_number=ptd.plan_number and ptd.div_sub_id=dpg.div_sub_id ")

    transform_df = exn_xxewre01_xxewrdpg_jb_df.unionByName(super_omni_newr_plan_divsub_daily_ng_dat_df)

    return transform_df