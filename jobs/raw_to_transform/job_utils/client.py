
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    exn_xxewre01_xxewrcld_ng_df = spark.sql("select client_id, case when client_id='NG' then 'Voya Recordkeeping Platform' else client_name end as client_name, 'VRP-PB' as source_system, cast(source_cycle_date as date) as source_cycle_date from exn_xxewre01_xxewrcld_ng")

    exn_xxewre01_xxewrcld_jb_df = spark.sql("select client_id, client_name, 'VRP-SP' as source_system, cast(source_cycle_date as date) as source_cycle_date from exn_xxewre01_xxewrcld_jb")

    transform_df = exn_xxewre01_xxewrcld_ng_df.unionByName(exn_xxewre01_xxewrcld_jb_df)

    return transform_df