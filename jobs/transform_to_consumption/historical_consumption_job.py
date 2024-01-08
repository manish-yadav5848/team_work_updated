from pyspark.sql import SparkSession
import os
from wealthcentral.jobs.transform_to_consumption.helper_utils.data_utils import substitute_null_primary_keys, \
    drop_duplicates, trim_string_columns, remove_junk_values_from_cols, add_delta_op_col
from wealthcentral.jobs.transform_to_consumption.helper_utils.objects_utils import JobNameToJobMapper
from wealthcentral.utils.config_utils import load_json_file, add_consumption_control_table_entry, create_control_file
from wealthcentral.utils.object_utils import ConsumptionJobArguments, ConsumptionControlTableParameters
from wealthcentral.jobs.transform_to_consumption.helper_utils.config_utils import read_consumption_parameters
import logging
from wealthcentral.jobs.staging_to_raw.helper_utils.data_utils import ( generate_merge_condition_for_delta_in_raw,
    generate_update_condition_for_delta_in_raw,
    generate_insert_condition_for_delta_in_raw)

from wealthcentral.utils.spark_utils import overwrite_delta_load, overwrite_parquet_load, get_current_datetime, \
    append_delta_load, get_dbutils,merge_and_write_delta_load


def historical_load_file_in_consumption(spark: SparkSession, job_arguments: ConsumptionJobArguments):
    logger = logging.getLogger('pyspark')

    # Get Current Datetime as Job Start Time
    etl_job_start_time = get_current_datetime()

    consumption_config_path = os.getenv('CONSUMPTION_CONFIG_PATH')
    logger.info('File Config Path: %s', consumption_config_path)

    # Read source file config
    consumption_config = load_json_file(
        file_path=consumption_config_path
    )

    # Read File Parameters
    consumption_parameters = read_consumption_parameters(
        config_file=consumption_config,
        entity_name=job_arguments.consumption_entity_name
    )


    # Set Spark Session to Use Transform Database
    database_name = os.getenv('TRANSFORM_DATABASE_NAME')
    spark.sql(f"use {database_name}")

    consumption_df = None

    # Basis transform entity name, trigger job
    if job_arguments.consumption_entity_name in JobNameToJobMapper:
        # get the function associated with the name and invoke it with the variable
        consumption_df = JobNameToJobMapper[job_arguments.consumption_entity_name](spark, job_arguments.batch_date, consumption_parameters.primary_keys, job_arguments.client_name)
    else:
        logger.info('Consumption Entity Function Not Found')


    if consumption_df:
        consumption_df = trim_string_columns(
            df=consumption_df
        )

        consumption_df = remove_junk_values_from_cols(
            df=consumption_df
        )

        if consumption_parameters.primary_keys:
            # Stub primary keys in the Dataframe using default values
            consumption_df = substitute_null_primary_keys(
                df=consumption_df,
                primary_keys=consumption_parameters.primary_keys
            )
            # Drop duplicate records
            consumption_df = drop_duplicates(
                df=consumption_df,
                partition_key=consumption_parameters.primary_keys
            )

        # Persist DF
        consumption_df.persist()


        # Write data in consumption environment
        database_name = os.getenv('CONSUMPTION_DATABASE_NAME')
        table_name = consumption_parameters.entity_name
        location = '/'.join([os.getenv('CONSUMPTION_OUTPUT_PATH'), table_name])


        if consumption_parameters.entity_load == 'full_load':
            overwrite_delta_load(
                spark=spark,
                df=consumption_df,
                database_name=database_name,
                table_name=table_name,
                location=location
            )

        if consumption_parameters.entity_load == 'incremental':
            overwrite_delta_load(
                spark=spark,
                df=consumption_df,
                database_name=database_name,
                table_name=table_name,
                partition_cols=["source_cycle_date"],
                location=location
            )

        # Write data in consumption stage environment. Only incremental chunk to be persisted in stage
        location = '/'.join([os.getenv('CONSUMPTION_STAGE_PATH'), table_name])
        overwrite_parquet_load(
                df=consumption_df,
                location=location
        )



    spark.catalog.dropTempView("*")
    spark.catalog.clearCache()

    # Get Current Datetime as Job Start Time
    etl_job_end_time = get_current_datetime()

    # Add Control Table Entry
    control_table_df = add_consumption_control_table_entry(
        spark=spark,
        control_table_parameters=ConsumptionControlTableParameters(
            batch_date=job_arguments.batch_date,
            table_name=consumption_parameters.table_name,
            etl_start_datetime=etl_job_start_time,
            etl_end_datetime=etl_job_end_time,
            status='SUCCESS',
            created_by='SYSTEM'
        )
    )

    database_name = os.getenv('CONSUMPTION_DATABASE_NAME')
    source_name = "system"
    table_name = "control_table"
    location = '/'.join([os.getenv('CONSUMPTION_OUTPUT_PATH'), source_name, table_name])

    # Write Control Table
    append_delta_load(
        spark=spark,
        df=control_table_df,
        database_name=database_name,
        table_name=table_name,
        location=location
    )

    # Create Control File
    source_name = "system"
    dir_name = "control_file"
    control_file_date = job_arguments.batch_date
    table_name = consumption_parameters.table_name
    location = '/'.join([os.getenv('CONSUMPTION_OUTPUT_PATH'), source_name, dir_name, control_file_date, table_name])
    dbutils = get_dbutils(spark)

    create_control_file(
        dbutils=dbutils,
        file_path=location
    )

    source_name = "system"
    dir_name = "control_file"
    control_file_date = job_arguments.batch_date
    table_name = consumption_parameters.table_name
    location = '/'.join([os.getenv('CONSUMPTION_STAGE_PATH'), source_name, dir_name, control_file_date, table_name])
    dbutils = get_dbutils(spark)
    create_control_file(
        dbutils=dbutils,
        file_path=location
    )
