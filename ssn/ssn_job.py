import os
import logging
from wealthcentral.utils.config_utils import load_json_file
from pyspark.sql.functions import  lit
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from wealthcentral.utils.spark_utils import overwrite_parquet_load
from wealthcentral.utils.object_utils import SSNChangeMergeParameters
from wealthcentral.jobs.ssn.helper_utils.config_utils import read_file_parameters
from wealthcentral.jobs.ssn.helper_utils.data_utils import  (generate_merge_condition_for_ssn_handling,
                                                             generate_insert_condition_for_ssn_handling,
                                                             generate_delete_condition_for_ssn_handling)
from wealthcentral.jobs.ssn.helper_utils.objects_utils import JobNameToJobMapper


def ssn_change_merge(spark: SparkSession, job_arguments: SSNChangeMergeParameters):
    logger = logging.getLogger("pyspark")
    ssn_config_path = os.getenv('SSN_CONFIG_PATH')
    transform_database_name = os.getenv('TRANSFORM_DATABASE_NAME')
    ssn_output_path = os.getenv('SSN_OUTPUT_PATH')
    entity_name = job_arguments.entity_name

    ssn_config = load_json_file(
        ssn_config_path
        )

    file_parameters = read_file_parameters(
        config_file = ssn_config,
        entity_name = entity_name
    )

    ssn_df = read_ssn(spark, file_parameters)
    monthly_quarterly_df = None

    if entity_name in JobNameToJobMapper:
        # get the function associated with the name and invoke it with the variable
        monthly_quarterly_df = JobNameToJobMapper[entity_name](spark)
    else:
        logger.info('SSN Entity Function Not Found')

    if monthly_quarterly_df:
        old_df = spark.sql(f"select * {file_parameters.database_name}.{entity_name}")

        ssn_df = ssn_df.alias("source")
        old_df = old_df.alias("target")

        delete_df = ssn_df.join(old_df, ssn_df.old_participant_id == old_df.participant_id, "inner").select("target.*").withColumn("delta_op_ind", lit("D"))

        monthly_quarterly_df = monthly_quarterly_df.alias("target")
        append_df = ssn_df.join(monthly_quarterly_df, ssn_df.new_participant_id == monthly_quarterly_df.participant_id, "inner").select("target.*").withColumn("delta_op_ind", lit("A"))

        merge_condition = generate_merge_condition_for_ssn_handling(file_parameters.primary_keys)
        delete_condition = generate_delete_condition_for_ssn_handling("old_participant_id", "participant_id")
        insert_condition = generate_insert_condition_for_ssn_handling(append_df.columns)

        delta_table = DeltaTable.forName(spark, "{}.{}".format(file_parameters.database_name, entity_name))
        writer = delta_table.alias("target")
        writer = writer.merge(append_df.alias("source"), merge_condition)
        writer.whenMatchedUpdate(set=insert_condition).whenNoMatchedInsert(set=insert_condition)

        delta_table = DeltaTable.forName(spark, "{}.{}".format(file_parameters.database_name, entity_name))
        writer = delta_table.alias("target")
        writer = writer.merge(ssn_df.alias("source"), delete_condition)
        writer.whenMatchedDelete()

        upsert_df = append_df.unionByName(delete_df)
        overwrite_parquet_load(df=upsert_df,
                               location=ssn_output_path)


def read_ssn(spark: SparkSession, database_name: str, batch_date: str):
    ssn_df = spark.sql(f"select * from {database_name}.ssn_change_merge_helper where concat(tran_year, tran_month, tran_day) = '{batch_date}'")
    return ssn_df