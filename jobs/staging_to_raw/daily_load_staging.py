__author__ = "lumiq"

from wealthcentral.utils.object_utils import StagingJobArguments, StagingControlTableParameters
from wealthcentral.utils.config_utils import load_json_file, add_staging_control_table_entry, create_control_file
from wealthcentral.jobs.staging_to_raw.helper_utils.config_utils import read_file_parameters
from wealthcentral.jobs.staging_to_raw.helper_utils.data_utils import (
    read_data_from_file,
    add_col_batch_date,
    add_col_file_segment,
    get_record_count,
)
from wealthcentral.utils.spark_utils import overwrite_delta_load, get_dbutils, \
    append_delta_load, get_current_datetime
import os
from pyspark.sql import SparkSession
import logging


def load_file_in_staging(spark: SparkSession, job_arguments: StagingJobArguments):
    logger = logging.getLogger('pyspark')

    # Get Current Datetime as Job Start Time
    etl_job_start_time = get_current_datetime()

    file_config_path = os.getenv('FILE_CONFIG_PATH')
    logger.info('File Config Path: %s', file_config_path)

    # Read source file config
    file_config = load_json_file(
        file_path=file_config_path
    )

    # Read File Parameters
    file_parameters = read_file_parameters(
        env=job_arguments.env,
        config_file=file_config,
        source_name=job_arguments.source_name,
        file_name=job_arguments.file_name,
        file_segment_name=job_arguments.file_segment_name
    )
    logger.info(
        'Parsed File Parameters: source_name: %s, file_name: %s, file_type: %s, file_format: %s, file_segmented_flag: %s, file_load: %s, file_header_flag: %s, primary_keys: %s, table_name: %s, file_segment: %s',
        file_parameters.source_name,
        file_parameters.file_name,
        file_parameters.file_type,
        file_parameters.file_format,
        file_parameters.file_segmented_flag,
        file_parameters.file_load,
        file_parameters.file_header_flag,
        file_parameters.primary_keys,
        file_parameters.table_name,
        file_parameters.file_segment
        )

    # Read source load
    df = read_data_from_file(
        spark=spark,
        job_arguments=job_arguments,
        file_parameters=file_parameters
    )
    logger.info('Data Read from File into Spark Dataframe')

    # Transform df load by adding file_segment col required for partitioning in staging_to_raw. To be added only if file is segmented
    if file_parameters.file_segmented_flag:
        df = add_col_file_segment(
            df=df,
            value=file_parameters.file_segment
        )
        logger.info('File Segment Column added to Spark Dataframe')

    # Transform source df by adding batch_date col required for partitioning and maintaining history
    df = add_col_batch_date(
        df=df,
        batch_date=job_arguments.batch_date
    )
    logger.info('Batch Date Column added to Spark Dataframe')

    # Persist DF
    df.persist()

    # Get Spark Dataframe record count
    record_count = get_record_count(
        df=df
    )
    logger.info('Spark Dataframe count of records: %s', record_count)


    if record_count > 0:

        # Staging environment to always be full_load basis partition on batch_date and file_segment (if present)
        database_name = os.getenv('STAGING_DATABASE_NAME')
        source_name = file_parameters.source_name
        table_name = file_parameters.table_name
        if file_parameters.file_segmented_flag:
            partition_cols = ["batch_date", "file_segment"]
        else:
            partition_cols = ["batch_date"]
        location = '/'.join([os.getenv('STAGING_OUTPUT_PATH'), source_name, table_name])
        logger.info(
            'Writing Spark Dataframe to Staging with parameters: database_name: %s, table_name: %s, partition_columns: %s, location: %s',
            database_name,
            table_name,
            partition_cols,
            location
        )

        # Write source load in staging environment
        overwrite_delta_load(
            spark=spark,
            df=df,
            database_name=database_name,
            table_name=table_name,
            partition_cols=partition_cols,
            location=location
        )
        logger.info('Spark Dataframe written to Staging')

        # Extract all distinct batch_date from df to list
        # batch_date = get_distinct_batch_date(
        #     df=df
        # )
        # logger.info('Distinct Batch Dates: %s', batch_date)


    # if not batch_date:
    #     batch_date = create_distinct_batch_date(
    #         job_arguments=job_arguments
    #     )

    # Get Current Datetime as Job End Time
    etl_job_end_time = get_current_datetime()

    # Un-persist DF
    df.unpersist()

    # Add Control Table Entry
    control_table_df = add_staging_control_table_entry(
        spark=spark,
        control_table_parameters=StagingControlTableParameters(
            source_name=file_parameters.source_name,
            file_name=file_parameters.file_name,
            file_segment=file_parameters.file_segment,
            batch_date=job_arguments.batch_date,
            table_name=file_parameters.table_name,
            etl_start_datetime=etl_job_start_time,
            etl_end_datetime=etl_job_end_time,
            record_count=record_count,
            status='SUCCESS',
            created_by='SYSTEM'
        )
    )

    database_name = os.getenv('STAGING_DATABASE_NAME')
    source_name = "system"
    table_name = "control_table"
    location = '/'.join([os.getenv('STAGING_OUTPUT_PATH'), source_name, table_name])

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
    file_segment_name = job_arguments.file_segment_name
    location = '/'.join([os.getenv('STAGING_OUTPUT_PATH'), source_name, dir_name, control_file_date, file_segment_name])
    dbutils = get_dbutils(spark)

    create_control_file(
        dbutils=dbutils,
        file_path=location
    )

    return job_arguments.batch_date, file_parameters
