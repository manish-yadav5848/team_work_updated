
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    ssn_output_jb_df = spark.sql("select admin_tran_code,plan_number,client_id,old_participant_id,new_participant_id,tran_year,sequence_number,tran_month,tran_day,date_format(to_date(batch_date, 'yyyyMMdd'), 'yyyy-MM-dd') as transform_run_date from ssn_output_jb")

    ssn_output_ng_df = spark.sql("select admin_tran_code,plan_number,client_id,old_participant_id,new_participant_id,tran_year,sequence_number,tran_month,tran_day,date_format(to_date(batch_date, 'yyyyMMdd'), 'yyyy-MM-dd') as transform_run_date from ssn_output_ng")

    transform_df = ssn_output_jb_df.unionByName(ssn_output_ng_df)

    return transform_df