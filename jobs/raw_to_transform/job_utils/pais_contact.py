
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def transform(spark: SparkSession, primary_key: list):

    f986pb_mis_j986ds_detail_file_dat_df = spark.sql("SELECT agency_cd AS agency_code,agent_cd AS agent_code,business_address_city,business_address_line_1,business_address_line_2,business_address_line_3,business_address_state,business_address_status,business_address_zipcode,business_phone_number,career_rep_status_indicator ,email_address,firm_name,first_name,ifp_rag_rep_status_indicator AS ifp_rag_rep_status_indicator ,last_name,mail_name as full_name,middle_name,pais_entity_key,pais_entity_type,rep_tax_id_ssn ,rep_tax_id_ssn_indicator ,reps_last_known_bd_name AS reps_last_known_bd_name ,reps_status_with_last_known_bd AS reps_status_with_last_known_bd ,resident_address_city,resident_address_line_1,resident_address_line_2,resident_address_line_3,resident_address_state,resident_address_status,resident_address_zipcode,entstat as entity_status,status_of_business_phone_number AS status_of_business_phone_num ,cast(null as VARCHAR(36)) as pais_producer_key,status_of_email_address,suffix,'MIS' as source_system  FROM f986pb_mis_j986ds_detail_file")

    transform_df = f986pb_mis_j986ds_detail_file_dat_df

    return transform_df