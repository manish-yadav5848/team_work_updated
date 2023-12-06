import os
import logging
from delta.tables import DeltaTable
from wealthcentral.utils.config_utils import load_json_file
from pyspark.sql import SparkSession
from wealthcentral.jobs.transform_to_consumption.helper_utils.config_utils import read_consumption_parameters
from wealthcentral.utils.object_utils import InFlightConsumptionJobArguments
from wealthcentral.jobs.inflight_pcb.helper_utils.object_utils import JobNameToJobMapper
from wealthcentral.jobs.inflight_pcb.helper_utils.data_utils import (generate_merge_condition_for_delta_in_raw,
                                                                     generate_insert_condition_for_delta_in_transform)

def inflight_pcb_consumption(spark: SparkSession, job_arguments: InFlightConsumptionJobArguments):
    logger = logging.getLogger('pyspark')

    consumption_config_path = os.getenv('CONSUMPTION_CONFIG_PATH')

    logger.info('File Config Path: %s', consumption_config_path)
    consumption_database_name = os.getenv('CONSUMPTION_DATABASE_NAME')
    consumption_entity_name = job_arguments.consumption_entity_name

    consumption_config = load_json_file(
        file_path=consumption_config_path
    )

    # Read File Parameters
    consumption_parameters = read_consumption_parameters(
        config_file=consumption_config,
        entity_name=job_arguments.consumption_entity_name
    )
    primary_keys = consumption_parameters.primary_keys
    df_12_31, df_updated_12_31 = None, None

    entity_name1 = consumption_entity_name + "_12_31"
    entity_name2 = consumption_entity_name + "_updated_12_31"

    if entity_name1 in JobNameToJobMapper:
        # get the function associated with the name and invoke it with the variable
        df_12_31 = JobNameToJobMapper[entity_name1](spark, primary_keys)
    else:
        logger.info('Consumption Entity Function Not Found')

    if entity_name2 in JobNameToJobMapper:
        # get the function associated with the name and invoke it with the variable
        df_updated_12_31 = JobNameToJobMapper[entity_name2](spark, primary_keys)
    else:
        logger.info('Consumption Entity Function Not Found')

    if df_12_31 and df_updated_12_31:
        column_names = df_12_31.columns
        merge_condition = generate_merge_condition_for_delta_in_raw(primary_keys)
        insert_condition = generate_insert_condition_for_delta_in_transform(column_names)

        # delete from existing delta table (target) where primary keys of source (df_12_31) matches
        delta_table = DeltaTable.forName(spark, "{}.{}".format(consumption_database_name, consumption_entity_name))
        writer = delta_table.alias("target")
        writer = writer.merge(df_12_31.alias("source"), merge_condition)
        writer.whenMatchedDelete().execute()

        # insert into existing delta table (target) all the data from source (df_12_31_updated)
        delta_table = DeltaTable.forName(spark, "{}.{}".format(consumption_database_name, consumption_entity_name))
        writer = delta_table.alias("target")
        writer = writer.merge(df_updated_12_31.alias("source"), merge_condition)
        writer.whenMatchedUpdate(set=insert_condition).whenNotMatchedInsert(values=insert_condition).execute()
