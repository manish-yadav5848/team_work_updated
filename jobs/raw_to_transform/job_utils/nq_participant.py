
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    participant_vnq_wc_txt_df = spark.sql("SELECT client_id, plan_number, participant_id, party_id, employee_id,  sso_pin, first_name, middle_name, last_name, marital_status_code, marital_status_name, gender, street_address_1, street_address_2, city, state, foreign_state_address, zip, country, work_email_address, personal_email_address, home_phone_number, office_phone_number, cast(date_of_birth as DATE) as date_of_birth, cast(date_of_original_hire as DATE),  cast(date_of_termination as DATE), job_status_code, job_status_name, cast(years_service as INTEGER), hc_employee, key_employee, officer_16b, cast(create_date as DATE), cast(last_modified_date as DATE), cast(plan_entry_date as DATE), date_format(to_date(batch_date, 'yyyyMMdd'), 'yyyy-MM-dd') as source_cycle_date,'NQ' as source_system  FROM participant_vnq_wc_txt")

    transform_df = participant_vnq_wc_txt_df

    return transform_df