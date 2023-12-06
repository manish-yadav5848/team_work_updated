__author__ = "lumiq"
# config_utils.py: Contains utility functions for managing PySpark configuration settings
# such as loading configuration files and setting up environment variables.


from wealthcentral.jobs.staging_to_raw.helper_utils.objects_utils import FileParameters


def read_file_parameters(config_file, env: str, source_name: str, file_name: str, file_segment_name: str = None):
    for source in config_file['source']:
        sourceName = source['sourceName']
        if isinstance(source['fileName'], str):
            fileName = source['fileName']
        else:
            fileName = source['fileName'][env]

        if sourceName == source_name and fileName == file_name:
            if source['primaryKeys']:
                primary_keys = source['primaryKeys']
            else:
                primary_keys = None
            return FileParameters(
                source_name=source_name,
                file_name=file_name,
                file_type=source['fileType'],
                file_format=source['fileFormat'],
                file_segmented_flag=source['fileSegmentedFlag'],
                file_load=source['fileLoad'],
                file_header_flag=source['headerPresentFlag'],
                primary_keys=primary_keys,
                table_name=source['tableName'],
                file_segment_name=file_segment_name
            )
    return None
