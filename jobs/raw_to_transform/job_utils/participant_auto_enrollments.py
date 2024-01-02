from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    exn_xxewre01_xxewrpae_ng_df = spark.sql("select client_id,plan_number, participant_id,cast(null as date) as source_processing_date, cast(auto_enroll_date as date) as auto_enroll_date, 'VRP-PB' as source_system, cast(source_cycle_date as date) as source_cycle_date from exn_xxewre01_xxewrpae_ng")

    exn_xxewre01_xxewrpae_jb_df = spark.sql("select client_id,plan_number, participant_id,cast(null as date) as source_processing_date, cast(auto_enroll_date as date) as auto_enroll_date, 'VRP-SP' as source_system, cast(source_cycle_date as date) as source_cycle_date from exn_xxewre01_xxewrpae_jb")

    transform_df = exn_xxewre01_xxewrpae_ng_df.unionByName(exn_xxewre01_xxewrpae_jb_df)

    return transform_df