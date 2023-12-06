__author__ = "lumiq"
# config_utils.py: Contains utility functions for managing PySpark configuration settings
# such as loading configuration files and setting up environment variables.


from wealthcentral.jobs.ssn.helper_utils.objects_utils import FileParameters


def read_file_parameters(config_file, entity_name: str):
    for sink in config_file['sink']:
        entityName = sink['entityName']

        if entityName == entity_name:
            primaryKeys = sink['primaryKeys']
            databaseName = sink['databaseName']
            return FileParameters(
                entity_name = entityName,
                primary_keys = primaryKeys,
                database_name = databaseName
            )
    return None
