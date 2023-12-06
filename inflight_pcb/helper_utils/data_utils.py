def generate_merge_condition_for_delta_in_raw(merge_keys, target_alias_keyword="target", source_alias_keyword="source"):
    # target is delta table and source is load DF
    merge_condition = ' and '.join(
        '{}.{} = {}.{}'.format(target_alias_keyword, pkColumn, source_alias_keyword, pkColumn) for pkColumn in
        merge_keys)
    return merge_condition


def generate_insert_condition_for_delta_in_transform(col_list, target_alias_keyword="target", source_alias_keyword="source"):
    """
    Creates a target dictionary with the specified structure.

    Parameters:
        col_list (list): A list of column names.

    Returns:
        dict: A dictionary with the specified structure.
    """
    # target is delta table and source is load DF
    dict = {}
    for col_name in col_list:
        dict[col_name] = f"{source_alias_keyword}.{col_name}"
    return dict

