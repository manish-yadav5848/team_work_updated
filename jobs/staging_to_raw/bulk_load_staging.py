__author__ = "lumiq"


from wealthcentral.utils.object_utils import StagingJobArguments
from wealthcentral.utils.config_utils import load_json_file
from wealthcentral.jobs.staging_to_raw.helper_utils.config_utils import read_file_parameters
from wealthcentral.jobs.staging_to_raw.helper_utils.data_utils import (
    read_data_from_file,
    add_col_batch_date,
    add_col_file_segment,
    get_file_segments_in_dir
)
from wealthcentral.utils.spark_utils import overwrite_delta_load, get_dbutils
import os
from pyspark.sql import SparkSession
import logging
from pyspark.sql.types import StructType


def load_files_in_staging(spark: SparkSession, job_arguments: StagingJobArguments):
    logger = logging.getLogger('pyspark')

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

    # Get all file_segments from dir
    dbutils = get_dbutils(spark)
    file_segments = get_file_segments_in_dir(
        dbutil=dbutils,
        job_arguments=job_arguments
    )
    logger.info('file segments: %s', file_segments)

    # Read source load
    combined_df = spark.createDataFrame([],StructType([]))

    for file_segment_name in file_segments:
        file_segment_name_wo_dt = file_segment_name.upper().rpartition('_')[0].rpartition('_')[0]  # Remove datetime from file_segment_name
        logger.info('File Segment Name without Datetime: %s', file_segment_name_wo_dt)

        job_arguments.file_segment_name = file_segment_name_wo_dt
        df = read_data_from_file(
            spark=spark,
            job_arguments=job_arguments,
            file_parameters=file_parameters
        )
        logger.info('Data Read from Individual File into Spark Dataframe')

        if file_parameters.file_segmented_flag:
            file_segment = file_segment_name_wo_dt.upper().rpartition('.')[2]
            df = add_col_file_segment(
                df=df,
                value=file_segment
            )
            logger.info('File Segment Column added to Spark Dataframe')

        # file_recv_datetime = file_segment_name.upper().rpartition('_')[2]
        # file_recv_date = (datetime.strptime(file_recv_datetime.rpartition('T')[0], "%Y-%m-%d")).strftime("%Y%m%d")
        # file_recv_ts = file_recv_datetime.rpartition('T')[2][:2] + ":" + file_recv_datetime.rpartition('T')[2][2:4]
        # logger.info('File Received Parameters: file_recv_datetime: %s, file_recv_date: %s, file_recv_ts: %s',
        #             file_recv_datetime,
        #             file_recv_date,
        #             file_recv_ts
        #             )

        df = add_col_batch_date(
            df=df,
            batch_date=job_arguments.batch_date
        )
        logger.info('Batch Date Column added to Spark Dataframe')

        combined_df = combined_df.unionByName(df, allowMissingColumns=True)
        logger.info('Combined Spark Dataframe to the Source Dataframe')

        job_arguments.file_segment_name = None


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
        df=combined_df,
        database_name=database_name,
        table_name=table_name,
        partition_cols=partition_cols,
        location=location
    )
    logger.info('Spark Dataframe written to Staging')



