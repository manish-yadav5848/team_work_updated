
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, batch_date: str, primary_key: list):

    consumption_df = spark.sql("select   coalesce(client_id, '-9999') as client_id,   coalesce(plan_number, '-9999') as plan_number,   coalesce(money_source_code, '-9999') as money_source_code,   source_cycle_date,   money_src_name,   money_type_code as money_type_code,   CASE     when money_type_code = 'EE' then 'EMPLOYEE'     else 'EMPLOYER'   end as money_typ_name,    cast(null as varchar(36)) as plan_key,   plan_source_key from(     select       t1.source_cycle_date,       t1.money_source_code,       t1.plan_source_key,       t1.client_id,       t1.plan_number,       t1.money_source_name_long as money_src_name,       CASE         when ms.source_system = 'SUPEROMNI' then ms.money_type_code         else ms.user_area       end as money_type_code     from       (         SELECT           ps.source_cycle_date,           ps.money_source as money_source_code,           ps.client_id,           ps.plan_number,           cast(null as varchar(36) ) as plan_source_key,           ps.money_source_name_long         from           plan_source as ps       ) as t1       left outer join money_source_helper as ms on coalesce(t1.money_source_code, '-9999') = coalesce(ms.source_cd, '-9999')   )")

    return consumption_df