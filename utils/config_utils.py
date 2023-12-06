from dotenv import load_dotenv
import simplejson
import argparse
from pathlib import Path
from wealthcentral.utils.object_utils import (
    JobArguments,
    StagingJobArguments,
    RawJobArguments,
    TransformJobArguments,
    ConsumptionJobArguments,
    StagingControlTableParameters,
    RawControlTableParameters,
    TransformControlTableParameters,
    ConsumptionControlTableParameters,
)
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
import os


def parse_job_arguments():
    """
    Parse Cmd line args
    Return Example: Namespace(config_file_name=file_config.json)
    :return: ArgumentParser Namespace
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", help="specify env: dev, unit, intg, accp, prod")
    parser.add_argument("--job_name", help="specify job name")
    parser.add_argument("--batch_date", help="specify batch date (yyyyMMdd) for which process needs to be executed",
                        required=False)
    parser.add_argument("--source_name", help="specify source system name", required=False)
    parser.add_argument("--file_name", help="specify file name", required=False)
    parser.add_argument("--file_segment_name", help="specify file segment name", required=False)
    parser.add_argument("--transform_entity_name", help="specify transform entity name", required=False)
    parser.add_argument("--consumption_entity_name", help="specify consumption entity name", required=False)
    parser.add_argument("--client_name", help="specify client (JB/NG))", required=False)
    args = parser.parse_args()
    parsed_args = JobArguments(
        env=args.env,
        job_name=args.job_name,
    )
    return parsed_args


def load_env_variables(env):
    file_path = f"wealthcentral/conf/.env.{env}"
    if env != 'dev':
        file_path = '/local_disk0/.ephemeral_nfs/cluster_libraries/python/lib/python3.9/site-packages/' + file_path

    env_file = Path(file_path)
    if env_file.is_file():
        load_dotenv(env_file)
        return True
    else:
        return False


def parse_staging_job_arguments():
    """
    Parse Cmd line args
    Return Example: Namespace(config_file_name=file_config.json)
    :return: ArgumentParser Namespace
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", help="specify env: dev, unit, intg, accp, prod")
    parser.add_argument("--job_name", help="specify job name", required=False)
    parser.add_argument("--batch_date", help="specify batch date (yyyyMMdd) for which process needs to be executed")
    parser.add_argument("--source_name", help="specify source system name")
    parser.add_argument("--file_name", help="specify file name")
    parser.add_argument("--file_segment_name", help="specify file segment name", required=False)  # OPTIONAL: Not required for bulk job
    parser.add_argument("--transform_entity_name", help="specify transform entity name", required=False)
    parser.add_argument("--consumption_entity_name", help="specify consumption entity name", required=False)
    parser.add_argument("--client_name", help="specify client (JB/NG))", required=False)
    args = parser.parse_args()

    if args.file_segment_name:
        file_segment_name = args.file_segment_name
    else:
        file_segment_name = None

    parsed_args = StagingJobArguments(
        env=args.env,
        batch_date=args.batch_date,
        source_name=args.source_name,
        file_name=args.file_name,
        file_segment_name=file_segment_name,
    )
    return parsed_args


def parse_raw_job_arguments():
    """
    Parse Cmd line args
    Return Example: Namespace(config_file_name=file_config.json)
    :return: ArgumentParser Namespace
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", help="specify env: dev, unit, intg, accp, prod")
    parser.add_argument("--job_name", help="specify job name", required=False)
    parser.add_argument("--batch_date", help="specify batch date (yyyyMMdd) for which process needs to be executed", required=False)  # OPTIONAL: Not required for (first_day_raw_job) raw job ran on first day run job
    parser.add_argument("--source_name", help="specify source system name")
    parser.add_argument("--file_name", help="specify file name")
    parser.add_argument("--file_segment_name", help="specify file segment name", required=False)
    parser.add_argument("--transform_entity_name", help="specify transform entity name", required=False)
    parser.add_argument("--consumption_entity_name", help="specify consumption entity name", required=False)
    parser.add_argument("--client_name", help="specify client (JB/NG))", required=False)
    args = parser.parse_args()

    if args.batch_date:
        batch_date = args.batch_date
    else:
        batch_date = None

    parsed_args = RawJobArguments(
        env=args.env,
        batch_date=batch_date,
        source_name=args.source_name,
        file_name=args.file_name,
    )
    return parsed_args


def parse_transform_job_arguments():
    """
    Parse Cmd line args
    Return Example: Namespace(config_file_name=file_config.json)
    :return: ArgumentParser Namespace
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", help="specify env: dev, unit, intg, accp, prod", required=False)
    parser.add_argument("--job_name", help="specify job name", required=False)
    parser.add_argument("--batch_date", help="specify batch date (yyyyMMdd) for which process needs to be executed")
    parser.add_argument("--source_name", help="specify source system name", required=False)
    parser.add_argument("--file_name", help="specify file name", required=False)
    parser.add_argument("--file_segment_name", help="specify file segment name", required=False)
    parser.add_argument("--transform_entity_name", help="specify transform entity name")
    parser.add_argument("--consumption_entity_name", help="specify consumption entity name", required=False)
    parser.add_argument("--client_name", help="specify client (JB/NG))", required=False)
    args = parser.parse_args()
    parsed_args = TransformJobArguments(
        batch_date=args.batch_date,
        transform_entity_name=args.transform_entity_name
    )
    return parsed_args


def parse_consumption_job_arguments():
    """
    Parse Cmd line args
    Return Example: Namespace(config_file_name=file_config.json)
    :return: ArgumentParser Namespace
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", help="specify env: dev, unit, intg, accp, prod", required=False)
    parser.add_argument("--job_name", help="specify job name", required=False)
    parser.add_argument("--batch_date", help="specify batch date (yyyyMMdd) for which process needs to be executed")
    parser.add_argument("--source_name", help="specify source system name", required=False)
    parser.add_argument("--file_name", help="specify file name", required=False)
    parser.add_argument("--file_segment_name", help="specify file segment name", required=False)
    parser.add_argument("--transform_entity_name", help="specify transform entity name", required=False)
    parser.add_argument("--consumption_entity_name", help="specify consumption entity name")
    parser.add_argument("--client_name", help="specify client (JB/NG))", required=False)  # OPTIONAL: Not required for daily_consumption. Necessary only in case of historical loads
    args = parser.parse_args()

    if args.client_name:
        client_name = args.client_name
    else:
        client_name = None
    parsed_args = ConsumptionJobArguments(
        batch_date=args.batch_date,
        consumption_entity_name=args.consumption_entity_name,
        client_name=client_name
    )
    return parsed_args


def load_json_file(file_path):
    """
    Given the config file name, it reads the config file and returns it in the form of JSON dict
    :return: config in the form of dictionary
    """
    with open(file_path) as config_file:
        try:
            return simplejson.load(config_file)
        except simplejson.errors.JSONDecodeError as error:
            raise simplejson.errors.JSONDecodeError(
                f"Issue with the Job Config: {file_path} {error}"
            )


def add_staging_control_table_entry(spark: SparkSession, control_table_parameters: StagingControlTableParameters):
    # Define the schema of the DataFrame
    schema = StructType([
        StructField("source_name", StringType(), True),
        StructField("file_name", StringType(), True),
        StructField("file_segment", StringType(), True),
        StructField("batch_date", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("etl_start_datetime", StringType(), True),
        StructField("etl_end_datetime", StringType(), True),
        StructField("record_count", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("created_by", StringType(), True)
    ])

    # Create a PySpark DataFrame with the schema
    data = [(control_table_parameters.source_name,
             control_table_parameters.file_name,
             control_table_parameters.file_segment,
             control_table_parameters.batch_date,
             control_table_parameters.table_name,
             control_table_parameters.etl_start_datetime,
             control_table_parameters.etl_end_datetime,
             control_table_parameters.record_count,
             control_table_parameters.status,
             control_table_parameters.created_by)]

    df = spark.createDataFrame(data, schema=schema)

    return df


def add_raw_control_table_entry(spark: SparkSession, control_table_parameters: RawControlTableParameters):
    # Define the schema of the DataFrame
    schema = StructType([
        StructField("source_name", StringType(), True),
        StructField("file_name", StringType(), True),
        StructField("batch_date", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("etl_start_datetime", StringType(), True),
        StructField("etl_end_datetime", StringType(), True),
        StructField("status", StringType(), True),
        StructField("created_by", StringType(), True)
    ])

    # Create a PySpark DataFrame with the schema
    data = [(control_table_parameters.source_name,
             control_table_parameters.file_name,
             control_table_parameters.batch_date,
             control_table_parameters.table_name,
             control_table_parameters.etl_start_datetime,
             control_table_parameters.etl_end_datetime,
             control_table_parameters.status,
             control_table_parameters.created_by)]

    df = spark.createDataFrame(data, schema=schema)

    return df


def add_transform_control_table_entry(spark: SparkSession, control_table_parameters: TransformControlTableParameters):
    # Define the schema of the DataFrame
    schema = StructType([
        StructField("batch_date", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("etl_start_datetime", StringType(), True),
        StructField("etl_end_datetime", StringType(), True),
        StructField("status", StringType(), True),
        StructField("created_by", StringType(), True)
    ])

    # Create a PySpark DataFrame with the schema
    data = [(control_table_parameters.batch_date,
             control_table_parameters.table_name,
             control_table_parameters.etl_start_datetime,
             control_table_parameters.etl_end_datetime,
             control_table_parameters.status,
             control_table_parameters.created_by)]

    df = spark.createDataFrame(data, schema=schema)

    return df


def add_consumption_control_table_entry(spark: SparkSession, control_table_parameters: ConsumptionControlTableParameters):
    # Define the schema of the DataFrame
    schema = StructType([
        StructField("batch_date", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("etl_start_datetime", StringType(), True),
        StructField("etl_end_datetime", StringType(), True),
        StructField("status", StringType(), True),
        StructField("created_by", StringType(), True)
    ])

    # Create a PySpark DataFrame with the schema
    data = [(control_table_parameters.batch_date,
             control_table_parameters.table_name,
             control_table_parameters.etl_start_datetime,
             control_table_parameters.etl_end_datetime,
             control_table_parameters.status,
             control_table_parameters.created_by)]

    df = spark.createDataFrame(data, schema=schema)

    return df


def create_control_file(dbutils, file_path):
    # Create an empty file at the specified path
    directory = os.path.dirname(file_path)

    if not directory:
        dbutils.fs.mkdirs(directory)

    dbutils.fs.put(file_path, 'CONTROL FILE SUCCESSFULLY CREATED', True)
