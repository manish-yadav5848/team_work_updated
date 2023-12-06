from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql import functions as F

def merge_dataframes(spark: SparkSession, df1, df2, primary_key):
    df1.persist()
    df2.persist()

    df1.createOrReplaceTempView('df1')
    df2.createOrReplaceTempView('df2')

    select_columns = df1.columns

    select_clause = "select\n"

    coalesce_clause = "coalesce("
    for column in select_columns:
        select_clause += coalesce_clause + "df1.{},".format(column) + "df2.{}) as {},\n".format(column, column)

    join_clause = "from df1 inner join df2 on\n"

    for pk in primary_key:
        join_clause += "df1.{} = df2.{} and\n".format(pk, pk)

    query_string = select_clause[:-2] + "\n" + join_clause[:-5]

    # print(query_string)

    # Pick Intersection of df1 and df2
    merged_df = spark.sql(query_string)

    # New requirement is to keep only common records
    # Unique record pickup logic to be commented

    # # Join Condition
    # join_condition = ' and '.join(
    #     '{}.{} = {}.{}'.format('df1', pkColumn, 'df2', pkColumn) for pkColumn in primary_key)
    #
    # # Pick from df1 where not in df2
    # anti_df1 = spark.sql(f'select df1.* from df1 left anti join df2 on {join_condition}')
    #
    # # Pick from df2 where not in df1
    # anti_df2 = spark.sql(f'select df2.* from df2 left anti join df1 on {join_condition}')
    #
    # # Union all Dataframes
    # final_df = merged_df.unionByName(anti_df1).unionByName(anti_df2)

    final_df = merged_df

    return final_df


def trim_string_columns(df: DataFrame) -> DataFrame:
    trimmed_df = df.select([(F.trim(c.name).alias(c.name) if isinstance(c.dataType, StringType) else c.name) for c in df.schema])
    return trimmed_df


def remove_junk_values_from_cols(df):
    # Remove '' and \r\n from columns
    for column_name in df.columns:
        df = df.withColumn(column_name, when(col(column_name) == '', lit(None)).otherwise(col(column_name))) \

    return df


def substitute_null_primary_keys(df, primary_keys):
    for key in primary_keys:
        if ("date" in key) or ("_dt" in key) or ("_dob" in key):
            default = '1900-01-01'
        else:
            default = '-9999'

        df = df.fillna(default, subset=[key])
        df = df.withColumn(key, coalesce(col(key), lit(default)))

        if ("date" in key) or ("_dt" in key) or ("_dob" in key):
            df = df.withColumn(key, to_date(col(key)))

    return df


def drop_duplicates(df, partition_key):
    """
    Drops duplicates over the specified partition key and keeps only the first occurrence of each duplicate.
    Parameters:
        df (DataFrame): The DataFrame to drop duplicates from.
        partition_key (list): A list of column names to partition by.
    Returns:
        DataFrame: The input DataFrame with duplicates over the partition key dropped.
    """
    # Define the window specification
    window_spec = Window.partitionBy(partition_key).orderBy(lit('NONE'))

    # Add a row number to each partition
    df = df.withColumn("row_number", row_number().over(window_spec))

    # Flag the first occurrence of each duplicate for dropping
    df = df \
        .filter(col("row_number") == 1)\
        .drop('row_number')

    return df


def add_delta_op_col(df):
    df = df.withColumn('delta_op_ind', lit('I'))
    return df
