
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from wealthcentral.jobs.raw_to_transform.helper_utils.data_utils import merge_dataframes


def transform(spark: SparkSession, primary_key: list):
    
    super_omni_newr_bene_daily_jb_dat_df = spark.sql("select beneficiary_tin ,concat(plan_number,'~',participant_id) as retirement_account_id,beneficiary_type,coalesce(client_id,'-9999') as client_id,coalesce(participant_id,'-9999') as participant_id ,coalesce(plan_number,'-9999') as plan_number,cast( NULL as VARCHAR(5)) as relationship_id,relationship_name,CAST(signed_date AS DATE) AS signed_date,'VRP-SP' as source_system,cast( null as VARCHAR(40)) as address_line_1,cast(null as DECIMAL(5,2)) as beneficiary_allocation_percent,cast( null as VARCHAR(01)) as beneficiary_category,cast( null as DATE) as beneficiary_last_change_date,coalesce( nullif(primary_contingent_indicator,''),'-9999') as beneficiary_level,cast( null as VARCHAR(30)) as beneficiary_name,cast( null as VARCHAR(1)) as beneficiary_percent_type,cast( beneficiary_sequence_number as INTEGER) as beneficiary_sequence_number,cast(null as VARCHAR(1)) as beneficiary_status,cast( null as DATE) as birth_date,cast( null as VARCHAR(28)) as city,cast( null as VARCHAR(01)) as foreign_address_indicator,cast( null as VARCHAR(1)) as gender,cast( null as VARCHAR(10)) as origin_changed,cast(source_cycle_date as date),cast( null as VARCHAR(3)) as state,cast( null as VARCHAR(9)) as zip,cast( null as VARCHAR(1)) as participant_beneficiary, cast(null as DATE) as source_processing_date FROM super_omni_newr_bene_daily_jb_dat")

    exn_xxewre01_xxewrpbe_jb_df = spark.sql("SELECT  cast(NULL as varchar(17)) as beneficiary_tin,concat(plan_number,'~',participant_id) as retirement_account_id,coalesce(client_id,'-9999') as client_id,coalesce(participant_id,'-9999') as participant_id ,coalesce(plan_number,'-9999') as plan_number,relationship_id,address_line_1,CAST(beneficiary_allocation_percent AS DECIMAL(5,2)) ,beneficiary_category,CAST(beneficiary_last_change_date AS DATE) AS beneficiary_last_change_date, coalesce( nullif(primary_contingent_indicator,''),'-9999') as beneficiary_level,beneficiary_name,beneficiary_percent_type,cast(beneficiary_sequence_number as INTEGER),beneficiary_status,CAST(birth_date AS DATE) as birth_date,city,foreign_address_indicator,gender,origin_changed, cast (null as varchar(1)) as beneficiary_type,state,zip,CASE WHEN relationship_id = 0 THEN 'Spouse' WHEN relationship_id = 1 THEN 'Aunt' WHEN relationship_id = 2 THEN 'Buisness Partner' WHEN relationship_id = 3 THEN 'Daughter' WHEN relationship_id = 4 THEN 'Father' WHEN relationship_id = 5 THEN 'Friend' WHEN relationship_id = 6 THEN 'Father-In-Law' WHEN relationship_id = 7 THEN 'Grandchild' WHEN relationship_id = 8 THEN 'Grandfather' WHEN relationship_id = 9 THEN 'Grandmother' WHEN relationship_id = 'A' THEN 'Mother' WHEN relationship_id = 'B' THEN 'Mother-In-Law' WHEN relationship_id = 'C' THEN 'Niece-Nephew' WHEN relationship_id = 'D' THEN 'Step-Daughter' WHEN relationship_id = 'E' THEN 'Step-Father'WHEN relationship_id = 'F' THEN 'Step-Mother' WHEN relationship_id = 'G' THEN 'Step-Son' WHEN relationship_id = 'H' THEN 'Sister-Brother'WHEN relationship_id = 'I' THEN 'Son' WHEN relationship_id = 'J' THEN 'Uncle' WHEN relationship_id = 'K' THEN 'Charity' WHEN relationship_id = 'L' THEN 'Church' WHEN relationship_id = 'M' THEN 'Creditor' WHEN relationship_id = 'N' THEN 'Employer' WHEN relationship_id = 'O' THEN 'Estate' WHEN relationship_id = 'P' THEN 'School' WHEN relationship_id = 'Q' THEN 'Trust' WHEN relationship_id = 'R' THEN 'Other' WHEN relationship_id = 'S' THEN 'Originator' WHEN relationship_id = 'T' THEN 'Domestic Partner' END AS participant_beneficiary,cast(null as varchar(30)) as relationship_name, CAST(null as DATE) AS signed_date,'VRP-SP' as source_system, cast(null as DATE) as source_processing_date, cast(source_cycle_date as DATE) FROM exn_xxewre01_xxewrpbe_jb")

    super_omni_newr_bene_daily_ng_dat_df = spark.sql("select beneficiary_tin,beneficiary_type,concat(plan_number,'~',participant_id) as retirement_account_id,coalesce(client_id,'-9999') as client_id,coalesce(participant_id,'-9999') as participant_id ,coalesce(plan_number,'-9999') as plan_number,cast( null as VARCHAR(01)) as relationship_id,relationship_name,CAST(signed_date AS DATE) AS signed_date,'VRP-PB' as source_system,cast( null as VARCHAR(40)) as address_line_1,cast(null as DECIMAL(5,2)) as beneficiary_allocation_percent,cast( null as VARCHAR(01)) as beneficiary_category,cast( null as DATE) as beneficiary_last_change_date,coalesce( nullif(primary_contingent_indicator,''),'-9999') as beneficiary_level,cast( null as VARCHAR(30)) as beneficiary_name,cast( null as VARCHAR(1)) as beneficiary_percent_type,cast( beneficiary_sequence_number as INTEGER) as beneficiary_sequence_number,cast(null as VARCHAR(1)) as beneficiary_status,cast( null as DATE) as birth_date,cast( null as VARCHAR(28)) as city,cast( null as VARCHAR(01)) as foreign_address_indicator,cast( null as VARCHAR(1)) as gender,cast( null as VARCHAR(10)) as origin_changed,cast(source_cycle_date as date),cast( null as VARCHAR(3)) as state,cast( null as VARCHAR(9)) as zip,cast( null as VARCHAR(1)) as participant_beneficiary, cast(null as DATE) as source_processing_date FROM super_omni_newr_bene_daily_ng_dat")

    exn_xxewre01_xxewrpbe_ng_df = spark.sql("SELECT cast(NULL as varchar(17)) as beneficiary_tin,concat(plan_number,'~',participant_id) as retirement_account_id,coalesce(client_id,'-9999') as client_id,coalesce(participant_id,'-9999') as participant_id ,coalesce(plan_number,'-9999') as plan_number,relationship_id,address_line_1,CAST(beneficiary_allocation_percent AS DECIMAL(5,2)) ,beneficiary_category,CAST(beneficiary_last_change_date AS DATE) AS beneficiary_last_change_date, coalesce( nullif(primary_contingent_indicator,''),'-9999') as beneficiary_level,beneficiary_name,beneficiary_percent_type,cast(beneficiary_sequence_number as INTEGER),beneficiary_status,CAST(birth_date AS DATE) as birth_date,city,foreign_address_indicator,gender,origin_changed, cast( null as VARCHAR(1))  as beneficiary_type,state,zip,CASE WHEN relationship_id = 0 THEN 'Spouse' WHEN relationship_id = 1 THEN 'Aunt' WHEN relationship_id = 2 THEN 'Buisness Partner' WHEN relationship_id = 3 THEN 'Daughter' WHEN relationship_id = 4 THEN 'Father' WHEN relationship_id = 5 THEN 'Friend' WHEN relationship_id = 6 THEN 'Father-In-Law' WHEN relationship_id = 7 THEN 'Grandchild' WHEN relationship_id = 8 THEN 'Grandfather' WHEN relationship_id = 9 THEN 'Grandmother' WHEN relationship_id = 'A' THEN 'Mother' WHEN relationship_id = 'B' THEN 'Mother-In-Law' WHEN relationship_id = 'C' THEN 'Niece-Nephew' WHEN relationship_id = 'D' THEN 'Step-Daughter' WHEN relationship_id = 'E' THEN 'Step-Father'WHEN relationship_id = 'F' THEN 'Step-Mother' WHEN relationship_id = 'G' THEN 'Step-Son' WHEN relationship_id = 'H' THEN 'Sister-Brother'WHEN relationship_id = 'I' THEN 'Son' WHEN relationship_id = 'J' THEN 'Uncle' WHEN relationship_id = 'K' THEN 'Charity' WHEN relationship_id = 'L' THEN 'Church' WHEN relationship_id = 'M' THEN 'Creditor' WHEN relationship_id = 'N' THEN 'Employer' WHEN relationship_id = 'O' THEN 'Estate' WHEN relationship_id = 'P' THEN 'School' WHEN relationship_id = 'Q' THEN 'Trust' WHEN relationship_id = 'R' THEN 'Other' WHEN relationship_id = 'S' THEN 'Originator' WHEN relationship_id = 'T' THEN 'Domestic Partner' END AS participant_beneficiary,cast(null as varchar(30)) as relationship_name,  CAST(null as DATE) AS signed_date,'VRP-PB' as source_system, cast(null as DATE) as source_processing_date, cast(source_cycle_date as DATE)  FROM exn_xxewre01_xxewrpbe_ng")

    ng_merged_df = merge_dataframes(
        spark=spark,
        df1=exn_xxewre01_xxewrpbe_ng_df,
        df2=super_omni_newr_bene_daily_ng_dat_df,
        primary_key=primary_key
    )

    jb_merged_df = merge_dataframes(
        spark=spark,
        df1=exn_xxewre01_xxewrpbe_jb_df,
        df2=super_omni_newr_bene_daily_jb_dat_df,
        primary_key=primary_key
    )

    participant_beneficiary_temp_df = jb_merged_df.unionByName(ng_merged_df)

    participant_beneficiary_temp_df.createOrReplaceTempView('participant_beneficiary_temp')

    transform_df = spark.sql("select coalesce(substring(pb.beneficiary_tin,1,9), '000000000') as beneficiary_tin,pb.retirement_account_id as retirement_account_id,pb.beneficiary_type as beneficiary_type,case when pb.beneficiary_type='1' then 'Individual' when pb.beneficiary_type='2' then 'Organization' when pb.beneficiary_type='3' then 'Trust' else null end as beneficiary_type_desc,  coalesce(pb.client_id, '-9999') as client_id,coalesce(pb.participant_id, '-9999') as participant_id,  coalesce(pb.plan_number, '-9999') as plan_number, coalesce(nullif(pb.beneficiary_sequence_number,''),'-9999') as beneficiary_sequence_number,pb.address_line_1, pb.beneficiary_allocation_percent, pb.beneficiary_category, pb.beneficiary_last_change_date,coalesce(pb.beneficiary_level,'-9999') as beneficiary_level , pb.beneficiary_name, pb.beneficiary_percent_type, pb.beneficiary_status,  pb.birth_date, pb.city, pb.foreign_address_indicator, pb.gender, pb.origin_changed, pb.participant_beneficiary,  pb.relationship_id  as relationship_id, pb.relationship_name, pb.signed_date, pb.source_cycle_date, pb.source_processing_date, pb.source_system, pb.state, pb.zip as zip_code from participant_beneficiary_temp as pb order by client_id,plan_number,participant_id,beneficiary_sequence_number,beneficiary_level")


    return transform_df