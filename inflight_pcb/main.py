__author__ = "lumiq"

from wealthcentral.jobs.raw_to_transform.transform_job import load_file_in_transform
from wealthcentral.jobs.staging_to_raw.bulk_load_staging import load_files_in_staging
from wealthcentral.jobs.transform_to_consumption.consumption_job import load_file_in_consumption
from wealthcentral.utils.config_utils import (
    parse_job_arguments,
    load_env_variables,
    parse_staging_job_arguments,
    parse_raw_job_arguments, parse_transform_job_arguments,
    parse_consumption_job_arguments,
    parse_inflight_pcb_transform_job_arguments,
    parse_inflight_pcb_consumption_job_arguments
)
from wealthcentral.jobs.inflight_pcb.inflight_pcb_transform_job import inflight_pcb_transform
from wealthcentral.jobs.inflight_pcb.inflight_pcb_consumption_job import inflight_pcb_consumption
import os
import logging
from wealthcentral.utils.logging_utils import init_logger
from wealthcentral.jobs.staging_to_raw.daily_load_staging import load_file_in_staging
from wealthcentral.jobs.staging_to_raw.daily_load_raw import load_file_in_raw
from wealthcentral.utils.spark_utils import init_spark_session


if __name__ == "__main__":
    print('Starting PySpark Application')


    # get the current working directory
    current_working_directory = os.getcwd()

    # print output to the console
    print(current_working_directory)


    # Parse Job Arguments
    print('Parsing Job Arguments')

    job_arguments = parse_job_arguments()
    print('Parsed Job Arguments: env: %s, job_name: %s',
          job_arguments.env,
          job_arguments.job_name
          )

    # Load Environment Variables
    # TODO: Fix path to environment variables file and add try-catch
    print('Loading Environment Variables')
    load_env_variables(job_arguments.env)
    print('Environment Variables Loaded')

    # Load Logger
    logging_config_path = os.getenv('LOGGING_CONFIG_PATH')

    init_logger(
        config_path=logging_config_path
    )
    logger = logging.getLogger('pyspark')

    logger.info('Logger Initialized')

    # Initialise Spark Session
    spark = init_spark_session()

    logger.info('Spark Initialized')

    # Trigger Job Basis 'job_name' variable
    if job_arguments.job_name == 'daily_load_staging':
        # Load Staging Job Arguments
        staging_job_arguments = parse_staging_job_arguments()
        logger.info('Parsed Staging Job Arguments: batch_date: %s, source_name: %s, file_name: %s, file_segment_name: %s',
                    staging_job_arguments.batch_date,
                    staging_job_arguments.source_name,
                    staging_job_arguments.file_name,
                    staging_job_arguments.file_segment_name
                    )
        load_file_in_staging(
            spark=spark,
            job_arguments=staging_job_arguments,
        )

    elif job_arguments.job_name == 'bulk_load_staging':
        # Load Staging Job Arguments
        staging_job_arguments = parse_staging_job_arguments()
        logger.info('Parsed Staging Job Arguments: batch_date: %s, source_name: %s, file_name: %s',
                    staging_job_arguments.batch_date,
                    staging_job_arguments.source_name,
                    staging_job_arguments.file_name,
                    )
        load_files_in_staging(
            spark=spark,
            job_arguments=staging_job_arguments,
        )

    elif job_arguments.job_name == 'daily_load_raw':
        # Load Raw Job Arguments
        raw_job_arguments = parse_raw_job_arguments()
        logger.info(
            'Parsed Raw Job Arguments: batch_date: %s, source_name: %s, file_name: %s',
            raw_job_arguments.batch_date,
            raw_job_arguments.source_name,
            raw_job_arguments.file_name
            )
        load_file_in_raw(
            spark=spark,
            job_arguments=raw_job_arguments,
        )

    elif job_arguments.job_name == 'daily_load_transform':
        # Load Staging Job Arguments
        transform_job_arguments = parse_transform_job_arguments()
        logger.info('Parsed Transform Job Arguments: batch_date: %s, transform_entity_name: %s',
                    transform_job_arguments.batch_date,
                    transform_job_arguments.transform_entity_name
                    )

        load_file_in_transform(
            spark=spark,
            job_arguments=transform_job_arguments,
        )

    elif job_arguments.job_name == 'daily_load_consumption':
        # Load Staging Job Arguments
        consumption_job_arguments = parse_consumption_job_arguments()
        logger.info('Parsed Consumption Job Arguments: batch_date: %s, consumption_entity_name: %s',
                    consumption_job_arguments.batch_date,
                    consumption_job_arguments.consumption_entity_name
                    )

        load_file_in_consumption(
            spark=spark,
            job_arguments=consumption_job_arguments,
        )
    elif job_arguments.job_name == 'inflight_pcb_transform':
        # Load Staging Job Arguments
        inflight_pcb_transform_job_arguments = parse_inflight_pcb_transform_job_arguments()
        logger.info('Parsed Inflight Transform Job Arguments: batch_date: %s, inflight_entity_name: %s',
                    inflight_pcb_transform_job_arguments.batch_date,
                    inflight_pcb_transform_job_arguments.transform_entity_name
                    )
        inflight_pcb_transform(
            spark=spark,
            job_arguments=inflight_pcb_transform_job_arguments,
        )

    elif job_arguments.job_name == 'inflight_pcb_consumption':
        # Load Staging Job Arguments
        inflight_pcb_consumption_job_arguments = parse_inflight_pcb_consumption_job_arguments()
        logger.info('inflight_pcb Consumption Job Arguments: batch_date: %s, inflight_entity_name: %s',
                    inflight_pcb_consumption_job_arguments.batch_date,
                    inflight_pcb_consumption_job_arguments.consumption_entity_name
                    )

        inflight_pcb_consumption(
            spark=spark,
            job_arguments=inflight_pcb_consumption_job_arguments,
        )

