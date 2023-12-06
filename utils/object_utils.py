from datetime import time


class JobArguments:
    def __init__(self, env: str, job_name: str):
        self.env = env.lower()
        self.job_name = job_name.lower()


class StagingJobArguments:
    def __init__(self, env: str, batch_date: str, source_name: str, file_name: str, file_segment_name: str = None):
        self.env = env.lower()
        self.batch_date = batch_date
        self.source_name = source_name.lower()
        self.file_name = file_name.upper()
        if file_segment_name:
            self.file_segment_name = file_segment_name.upper()
        else:
            self.file_segment_name = file_segment_name


class RawJobArguments:
    def __init__(self, env: str, source_name: str, file_name: str, batch_date: str = None):
        self.env = env.lower()
        self.batch_date = batch_date
        self.source_name = source_name.lower()
        self.file_name = file_name.upper()


class TransformJobArguments:
    def __init__(self, batch_date: str, transform_entity_name: str):
        self.batch_date = batch_date
        self.transform_entity_name = transform_entity_name


class ConsumptionJobArguments:
    def __init__(self, batch_date: str, consumption_entity_name: str, client_name: str):
        self.batch_date = batch_date
        self.consumption_entity_name = consumption_entity_name
        self.client_name = client_name


class StagingControlTableParameters:
    def __init__(self, source_name: str, file_name: str, file_segment: str, batch_date: str, table_name: str, etl_start_datetime: str, etl_end_datetime: str, record_count: int, status: str, created_by: str):
        self.source_name = source_name.lower()
        self.file_name = file_name.upper()
        self.file_segment = file_segment
        self.batch_date = batch_date
        self.table_name = table_name
        self.etl_start_datetime = etl_start_datetime
        self.etl_end_datetime = etl_end_datetime
        self.record_count = record_count
        self.status = status.upper()
        self.created_by = created_by.upper()


class RawControlTableParameters:
    def __init__(self, source_name: str, file_name: str, batch_date: str, table_name: str, etl_start_datetime: str, etl_end_datetime: str, status: str, created_by: str):
        self.source_name = source_name.lower()
        self.file_name = file_name.upper()
        self.batch_date = batch_date
        self.table_name = table_name
        self.etl_start_datetime = etl_start_datetime
        self.etl_end_datetime = etl_end_datetime
        self.status = status.upper()
        self.created_by = created_by.upper()


class TransformControlTableParameters:
    def __init__(self, batch_date: str, table_name: str, etl_start_datetime: str, etl_end_datetime: str, status: str, created_by: str):
        self.batch_date = batch_date
        self.table_name = table_name
        self.etl_start_datetime = etl_start_datetime
        self.etl_end_datetime = etl_end_datetime
        self.status = status.upper()
        self.created_by = created_by.upper()


class ConsumptionControlTableParameters:
    def __init__(self, batch_date: str, table_name: str, etl_start_datetime: str, etl_end_datetime: str, status: str, created_by: str):
        self.batch_date = batch_date
        self.table_name = table_name
        self.etl_start_datetime = etl_start_datetime
        self.etl_end_datetime = etl_end_datetime
        self.status = status.upper()
        self.created_by = created_by.upper()