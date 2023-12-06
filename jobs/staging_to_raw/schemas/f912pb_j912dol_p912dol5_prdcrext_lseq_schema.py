from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

schema = StructType([
    StructField("cycle_date", StringType(), True),
    StructField("role_key", StringType(), True),
    StructField("producer_role_type", StringType(), True),
    StructField("producer_role_code_description", StringType(), True),
    StructField("role_split_percent", StringType(), True),
    StructField("pais_entity_key", StringType(), True),
    StructField("agency_agent_code", StringType(), True),
    StructField("producer_tax_id", StringType(), True),
    StructField("producer_last_name", StringType(), True),
    StructField("producer_first_name", StringType(), True),
    StructField("producer_middle_init", StringType(), True),
    StructField("producer_suffix", StringType(), True),
    StructField("full_name_field", StringType(), True),
    StructField("plan_number", StringType(), True),
    StructField("plan_source", StringType(), True),
    StructField("participant_key", StringType(), True),
    StructField("participant_money_source", StringType(), True),
    StructField("participant_level_code", StringType(), True),
    StructField("participant_id", StringType(), True),
    StructField("hierarchy_pais_entity_key", StringType(), True),
    StructField("hierarchy_tax_id", StringType(), True),
    StructField("hierarchy_last_name", StringType(), True),
    StructField("hierarchy_first_name", StringType(), True),
    StructField("hierarchy_middle_init", StringType(), True),
    StructField("hierarchy_suffix", StringType(), True),
    StructField("hierarchy_full_name_field", StringType(), True),
    StructField("npn_number", StringType(), True),
    StructField("crd_number", StringType(), True),
    StructField("broker_id", StringType(), True)])
