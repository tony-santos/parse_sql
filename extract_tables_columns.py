from datetime import datetime
from itertools import chain
import re
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# import string

from loguru import logger

datetime_format = "%Y%m%d-%H%M"
logfile_datetime = datetime.now().strftime(datetime_format)

logger.add(f"extract_tables_columns-{logfile_datetime}.log", backtrace=True, diagnose=True)

# from extract_utilities import *
import extract_utilities as eu


if __name__ == "__main__":
    #print(datetime.now())
    with open(sys.argv[1], 'r') as myfile:
        query1 = myfile.read()
    
    select_count = 0
    select_froms = []
    columns = []
    logger.info(f"query read from file:")
    logger.info(f"{query1}")

    sql_str = eu.reformat_query(query1)
    logger.info(f"sql_str: {sql_str}")

    # while there are more SELECTs, get columns
    select_position = eu.get_select_position(sql_str, 0)
    while eu.more_selects(sql_str, select_position + 6):
        select_position = eu.get_select_position(sql_str, select_position + 6)
        from_position = eu.get_matching_from_position(sql_str, select_position)

        logger.info(f"select_position: {select_position},   from_postion: {from_position}")
        logger.info(f"sql_str area between SELECT and FROM (inclusive): +++{sql_str[select_position:from_position+5]}+++")
        logger.info(f"sql_str area between SELECT and FROM (eclusive): +++{sql_str[select_position+len('SELECT'):from_position].lstrip().rstrip()}+++")

        if from_position > -1:
            select_count = select_count + 1
            select_froms.append((select_position, from_position))
        else:
            break

    logger.info(f"all SELECTs have been parsed. getting column entries")
    field_entries = []
    parsed_fields = []
    tables2 = []
    logger.info(f"there are {select_count} SELECT-FROM pairs: {select_froms}")
    for select_pos, from_pos in select_froms:
        logger.info(f"sql_str area between SELECT and FROM (exclusive): +++{sql_str[select_pos+len('SELECT'):from_pos].lstrip().rstrip()}+++")
        # get fields
        fields = eu.split_fields(sql_str[select_pos+len('SELECT'):from_pos].lstrip().rstrip())
        logger.info(f"fields = {fields}")
# # remove section
#         for field in fields:
#             logger.info(f"field = {field}")
#             eu.parse_field(field)
#             logger.info(f"parsed_version = {eu.parse_field(field)}")
# # remove section
        parsed_fields = [eu.parse_field(field) for field in fields]
        logger.info(f"parsed_fields: {parsed_fields}")
        # check for table_alias
        logger.info(f"fields[0]: {fields[0]}")
        if fields[0].find('.') == -1: # no table alias found. get table name next to FROM
            table_entry = eu.get_table_next_to_from(sql_str, from_pos)
            logger.info(f"table_entry: {table_entry}")
            table_name = table_entry[0]
            logger.info(f"table_name: {table_name}")
            for field in parsed_fields:
                logger.info(f"field: {field}")
                tab_name, column_name, field_name    = field
                field_entry = (table_name, column_name, field_name)
                logger.info(f"adding {field_entry} to field_entries")
                field_entries.append(field_entry)
        else:
            field_entries.extend(parsed_fields) # use extend because parsed_fields returns a list
            logger.info(f"appending to field_entries: {parsed_fields}")
    logger.info(f"field_entries: {field_entries}")
    logger.info(f"there are {len(field_entries)} column entries")

    logger.info(f"\n\n")
    logger.info(f"getting tables")
    logger.info(f"select_froms{select_froms}")
    tables2.extend(eu.get_tables_after_froms(sql_str, select_froms))
    logger.info(f"tables: {tables2}")

    join_positions = []
    start_pos = 0
    while sql_str.find(' JOIN ', start_pos) > -1:
        new_join_position = sql_str.find(' JOIN ', start_pos)
        join_positions.append(new_join_position)
        start_pos = new_join_position + 1
    logger.info(f"join_positions: {join_positions}")

    for join_position in join_positions:
        tables2.append(eu.get_table_next_to_join(sql_str, join_position))

    tables2.append(('DERIVED', 'DERIVED'))

    logger.info(f"tables2: {tables2}")
    table_set = set(tables2)
    tables_list = list(table_set)
    print(type(table_set))
    print(type(tables_list))
    # logger.info(f"tables: {list(set(tables2))}")
    logger.info(f"tables: {tables_list}")

    table_dict = {}

    for index, table_entry in enumerate(tables2):
        logger.info(f"table {index}: {table_entry}")
        table_name, table_alias = table_entry
        table_dict[table_alias] = table_name
        if table_alias.find('.') > -1:
            logger.info(f"adding entry for {table_alias.split('.')[1]}")
            table_dict[table_alias.split(".")[1]] = table_name
    
    logger.info(f"table_dict: {table_dict}")
    for table_alias, table_name in table_dict.items():
        if table_name.find(".") > -1:
            table_dict[table_alias] = table_name.split(".")[1]
        elif table_name == "SUBQUERY":
            table_dict[table_alias] = table_name + " - " + table_alias
    logger.info(f"table_dict: {table_dict}")


    for index, field in enumerate(field_entries):
        # logger.info(f"field {index}: {field}")
        table_name, column_name, field_name = field
        if table_name.find(".") > -1:
            table_name = table_name.split(".")[1]
            # logger.info(f"new table_name value: {table_name}")
        else: # no schema name, might contain a table name or an alias. perform lookup in table_dict to get table name
            table_name = table_dict[table_name]
        logger.info(f"field_name: {field_name}, table_name: {table_name}  , column_name: {column_name}")
