from wealthcentral.jobs.transform_to_consumption.helper_utils.objects_utils import ConsumptionParameters


def read_consumption_parameters(config_file, entity_name: str):
    """
    Loop through each source in the source list and create a dictionary of
    dataset name and its dataframe.
    :param sources: List of sources
    :param spark: spark session
    :return: dictionary of key:dataset_name and value:dataframe
    """
    for sink in config_file['sink']:
        if sink['entityName'] == entity_name:
            if sink['primaryKeys']:
                primary_keys = sink['primaryKeys']
            else:
                primary_keys = None
            return ConsumptionParameters(
                entity_name=entity_name,
                entity_load=sink['entityLoad'],
                primary_keys=primary_keys
            )
    return None
