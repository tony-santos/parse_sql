from datetime import datetime
from itertools import chain
import re
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import string

from loguru import logger

datetime_format = "%Y%m%d-%H%M"
logfile_datetime = datetime.now().strftime(datetime_format)
#print(datetime_format)
#print(logfile_datetime)
#exit(0)

logger.add(f"output-{sys.argv[0].split('.')[0]}-{logfile_datetime}.log", backtrace=True, diagnose=True)

from extract_utilities import *




def get_tables(sql_str, from_clause):
    """extract table names from sql query
    
    might need multiple passes to get all table names and may lead to additional columns being found

    Arguments:
        sql_str {str} -- string containing query
    Returns:
        list_of tuples - each tuple will contain the name of the table and its alias. if no alias provided, the alias will be the table name. table names will include schema names if
        they were part of the query
    """
    # start with text between FROM and WHERE or between FROM and ';' and return list of tables by splitting on ',' 
    from_clause = from_clause.upper().lstrip().rstrip()
    logger.info(f"entering get_tables - from_clause: +++{from_clause}+++")
    table_entries = []
    # rest_of_query = get_rest_after_main_select(sql_str)
    # logger.info(f"rest_of_query: {rest_of_query}")
    # if rest_of_query[-1] == ';':
    #     rest_of_query = rest_of_query[:-1] 

    # logger.info(f"rest_of_query2: {rest_of_query}")
    # from_clause = rest_of_query
    # logger.info(f"from_clause1: {from_clause}")
    # if from_clause.find('WHERE') > 0:
    #     from_clause = from_clause[:from_clause.find('WHERE')]
    #     logger.info(f"from_clause2: {from_clause}")
    
    if from_clause[0] == '(': # from clause is a  subquery
        logger.info(f"from_clause is a subquery. skip processing")
        subquery_alias = get_subquery_alias(sql_str, from_clause)
        table_entries.append(('SUBQUERY', subquery_alias))
        return table_entries
        # from_clause = from_clause[1:closing_paren_pos].lstrip().rstrip() # strip off opening and closing parens
        # logger.info(f"after stripping parens: +++{from_clause}+++")

    
    # left off here. need to figure out what kind of table list has been passed. if simple list separated by commas or combination of different JOIN keywords
    if from_clause.find('JOIN') == -1: # should be simple list of tables separated by comma
        logger.info(f"JOIN not found. should be simple list of tables separated by comma")
        table_lines = from_clause.split(',')
    else: # since the word JOIN was found, assume it  is a series of LEFT, RIGHT, INNER, or OUTER JOINS
        table_lines = process_other_join_types(from_clause)

    for table_line in table_lines:
        logger.info(table_line)
    table_lines = [table_line.rstrip().lstrip() for table_line in table_lines]

    table_entries = create_table_entries(table_lines)
    
    return table_entries

def process_other_join_types(from_clause):
    """generates a list of table lines by separating each splitting the list on the word JOIN and then cleaning up the LEFT/RIGHT/FULL/INNER/OUTER keywords
    
    Arguments:
        from_clause {str} -- text string containing tables, their alias', join types, and join condistions 
    """
    print_buffer()
    print_buffer()
    logger.info(f"entering process_other_join_types. from_clause: {from_clause}")
    table_lines_raw = from_clause.split(' JOIN') # after splitting on JOIN, all table lines except last should have extra keywords at the end to be removed
    table_lines_raw2 = [table_line.lstrip().rstrip() for table_line in table_lines_raw]
  
    table_lines = []
    for table_line_raw in table_lines_raw2:
        logger.info(f"table_line_raw1: {table_line_raw}")
#        logger.info(table_line_raw.find(' LEFT'))

        # look for LEFT & LEFT OUTER
        if table_line_raw.find(' LEFT') > -1:
            logger.info("found LEFT or LEFT OUTER JOIN")
            if table_line_raw.find(' LEFT') > 0: # there should be a table in front of LEFT XXX JOIN. 
                logger.info(f"table_line_raw2: {table_line_raw}")
                logger.info(f"table_line_raw before LEFT: {table_line_raw[:table_line_raw.find(' LEFT')].rstrip()}")
                #logger.info(f"adding1: {table_line_raw[:table_line_raw.find(' LEFT')].rstrip().upper()}")
                #table_lines.append(table_line_raw[:table_line_raw.find(' LEFT')].rstrip().upper()) # get line up to, but not including LEFT. should also remove LEFT OUTER
                table_line_raw = table_line_raw[:table_line_raw.find(' LEFT')].rstrip().upper() # get line up to, but not including LEFT. should also remove LEFT OUTER
        # look for LEFT & LEFT OUTER
        elif table_line_raw.find(' RIGHT') > -1:
            logger.info("found RIGHT or RIGHT OUTER JOIN")
            if table_line_raw.find(' RIGHT') > 0: # there should be a table in front of RIGHT XXX JOIN. 
                logger.info(f"table_line_raw3: {table_line_raw}")
                #logger.info(f"table_line_raw before RIGHT: {table_line_raw[:table_line_raw.find(' RIGHT')].rstrip()}")
                #logger.info(f"adding2: {table_line_raw[:table_line_raw.find(' RIGHT')].rstrip().upper()}")
                #table_lines.append(table_line_raw[:table_line_raw.find(' RIGHT')].rstrip().upper()) # get line up to, but not including LEFT. should also remove LEFT OUTER
                table_line_raw = table_line_raw[:table_line_raw.find(' RIGHT')].rstrip().upper() # get line up to, but not including LEFT. should also remove LEFT OUTER
    #        table_line2 = re.search('(LEFT JOIN (.+?) ON ', table_line2, re.DOTALL).group(1)
    # ON and join condition will be on next line. need to check separately at same level as check for LEFT
        if table_line_raw.find(' ON ') > -1:
            logger.info("found JOIN CONDITION")
            if table_line_raw.find(' ON ') > 0: # there should be a table in front of ON. 
                logger.info(f"table_line_raw4: {table_line_raw}")
                logger.info(f"table_line_raw before ON: {table_line_raw[:table_line_raw.find(' ON ')].rstrip()}")
                table_line_raw = table_line_raw[:table_line_raw.find(' ON ')].rstrip().upper()
        logger.info(f"adding3: {table_line_raw}")
        table_lines.append(table_line_raw) # get line up to, but not including LEFT. should also remove LEFT OUTER
    
    return table_lines

def create_table_entries(table_entries):
    """creates list of tuples with table name and alias.

    alias is needed to perform lookups for queries that utilize table alias
    
    Arguments:
        table_entries {list} -- [list of table records. each entry may include an alias (table_name AS table_alias)]
    """
    table_list = []
    table_name = ''
    table_alias = ''
    for table_entry in table_entries:
        logger.info(f"table_entry: {table_entry}")
        if table_entry.lstrip().startswith('('): # subquery, possibly a set of UNIONs
            subquery_table_list = process_subquery(table_entry)
            table_list.extend(subquery_table_list)
        elif table_entry.upper().find(' AS ') > -1: # table entry contains an alias (part after the AS)
            table_name, table_alias = table_entry.upper().split(' AS ')
        elif table_entry.upper().find(' ') > -1: # table entry contains a space. probably has a table alias after space
            logger.info(f"table_entry {table_entry.upper()} contains a space. look for alias")
            table_name, table_alias = table_entry.upper().split()
            if table_alias == ')':
                table_alias = table_name
        else:
            table_name = table_alias = table_entry.lstrip().rstrip()
 
        table_list.append((table_name.lstrip().rstrip(), table_alias.lstrip().rstrip()))
    
    return table_list


def process_subquery(table_entry):
    """extracts table names from subqueries

    will eventually handle simple subqueries and those with a bunch of unions. not sure how the subquery table alias should be handled when resolving columns from main query to tables
    
    Arguments:
        table_entry {str} -- text string containing a subquery table entry that starts with '('

    Returns: 
        list of table entry tuples
    """
    table_entries = [] # create empty list
    logger.info(f"entering process_subquery with table_entry: {table_entry}")
    # get text after last ) and check for alias
    logger.info(f"where is ')'? {table_entry.rfind(')')}")
    subquery_alias = ''
    if table_entry.rfind(')') > -1: # ) found and can be removed
        subquery_alias = table_entry[table_entry.rfind(')') + 1:]
        table_entry = table_entry[:table_entry.rfind(')')]
    logger.info(f"subquery_alias for table_entry: {subquery_alias}")
    logger.info(f"table_entry after removing ')': {table_entry}")
    logger.info(f"length of table_entry after removing ')': {len(table_entry)}")
    # get part after FROM
    logger.info(f"position of 'FROM': {table_entry.upper().find('FROM')}")
    table_entry = table_entry.upper().lstrip().rstrip()
    rest_of_query = get_rest_after_main_select(table_entry)
    logger.info(f"rest_of_query (after FROM): {rest_of_query}")
    if rest_of_query[-1] == ';':
        rest_of_query = rest_of_query[:-1] 

    logger.info(f"rest_of_query2: {rest_of_query}")
    from_clause = rest_of_query
    logger.info(f"from_clause1: {from_clause}")
    if table_entry.upper().find('UNION') > -1: # one or more unions found in subquery. split and process each one
        logger.info(f"UNION found at position {table_entry.upper().find('UNION')}")
        subqueries = table_entry.upper().split('UNION')
        for subquery in subqueries:
            logger.info(f"calling process_subquery with: {subquery}")
            table_entries.extend(process_subquery(subquery))
    else:
        if from_clause.find('WHERE') > 0:
            from_clause = from_clause[:from_clause.find('WHERE')]
            logger.info(f"from_clause2: {from_clause}")

        if from_clause.find('JOIN') == -1: # should be simple list of tables separated by comma
            table_lines = from_clause.split(',')
        else: # since the word JOIN was found, assume it  is a series of LEFT, RIGHT, INNER, or OUTER JOINS
            table_lines = process_other_join_types(from_clause)

        for table_line in table_lines:
            logger.info(f"table_line: {table_line}")
        table_lines = [table_line.rstrip().lstrip() for table_line in table_lines]

        table_entries = create_table_entries(table_lines)
    
    return table_entries
    #result.append()
#    return [('', '')]



def build_table_list(sql_str):
    """go through steps of building a list of tables used in a query
    
    Arguments:
        sql_str {str} -- text string containing the sql query
    """
    query_no_comments = remove_comments(sql_str)
    logger.info(f"sql_str: {sql_str}")
    logger.info(f"query_no_comments: {query_no_comments}")
    tables = get_tables(sql_str)
    return tables

def more_selects(sql_str, start_pos=0):
    return sql_str[start_pos:].upper().find('SELECT') > -1

def split_fields(fields_string, separator=','):
    """
    splits a string using separator unless the separator is within parentheses
    takes string of fields

    returns list of fields
    """
    logger.info(f"entering split_fields: fields_string = {fields_string} len(fields_string) = {len(fields_string)}")
    fields = []
    parenthesis_count = 0
    current_field = ''
    for char in fields_string.lstrip().rstrip():
        if char == ',' and parenthesis_count == 0: # split here by appending current field to fields and setting current_field to empty string
            fields.append(current_field)
            logger.info(f"appending: {current_field}")
            current_field = ''
        else:
            current_field = current_field + char
            if char == '(':
                parenthesis_count = parenthesis_count + 1
            elif char == ')':
                parenthesis_count = parenthesis_count - 1
    
    # add last field if it was not already added to fields
    if current_field != fields[-1]:
        logger.info(f"appending last field: {current_field}")
        fields.append(current_field.lstrip().rstrip())

    logger.info(f"fields: {fields}")
    return fields
            


def get_table_entries(sql_str, from_pos=0):
    """[summary]
    
    Arguments:
        sql_str {str} -- text string containing query
    
    Keyword Arguments:
        from_pos {int} -- [position of FROM keyword for which we are getting the table entries] (default: {0})
    
    Returns:
        table_entries_str {str} -- text string containing the portion of query that has the table entries
    """
    if sql_str[from_pos: from_pos + 4] == 'FROM': 
        start_pos = from_pos + 4 # move past 

if __name__ == "__main__":
    #print(datetime.now())
    with open(sys.argv[1], 'r') as myfile:
        query1 = myfile.read()
    
    select_count = 0
    select_from_wheres = []
    columns = []
    print_buffer()
    query_no_comments = remove_comments(query1)

    query_no_tabs_newlines = query_no_comments.replace("\t", " ").replace("\n", " ")
    query_single_spaces = query_no_tabs_newlines
    while query_single_spaces.find("  ") > -1:
        # logger.info(query_single_spaces.find("  "))
        query_single_spaces = query_single_spaces.replace("  ", " ")
    # logger.info(f"query_all_spaces: {query_single_spaces}")

    actual_select_position = 0
    actual_from_position = 0
    select_position = -6 # initialize to -6 so 1st pass will start at position 0
    from_position = 0

# does it make sense to replace all whitespace with spaces to make finding substring by joining tokens easier? going to try


    # logger.info(f"query1: {query1}")
    # sql_str = query1
    sql_str = query_single_spaces
    logger.info(f"sql_str: {sql_str}")
    # while there are more queries/subqueries, get columns
    while more_selects(sql_str, select_position + 6):
        select_position = get_select_position(sql_str, select_position + 6)
        # actual_select_position = actual_from_position + select_position
        from_position = get_matching_from_position(sql_str, select_position)
        where_position = get_matching_where_position(sql_str, from_position)
        # actual_from_position = actual_select_position + from_position
        logger.info(f"select_position: {select_position},   from_postion: {from_position},   where_postion: {where_position}")
        if from_position > -1:
            select_count = select_count + 1
            # select_from_pairs.append((actual_select_position, actual_from_position))
            select_from_wheres.append((select_position, from_position, where_position))
            # fields = split_fields(sql_str[select_position + 6:from_position])
            # logger.info(f"fields: {fields}")
            # new_columns = [parse_field(field) for field in fields]
            # logger.info(f"new_columns: {new_columns}")
            # columns.extend(new_columns)
            # sql_str = sql_str[select_position + 6:]
        else:
            break
    field_entries = []
    logger.info(f"select_count = {select_count}")
    logger.info(f"select_from_wheres: {select_from_wheres}")
    for select_from_where in select_from_wheres:
        select_pos, from_pos, where_pos = select_from_where
        logger.info(f"select_pos: {select_pos},   from_pos: {from_pos},   where_pos: {where_pos}")
        if from_pos - select_pos < 200:
            logger.info(f"sql_str part1a: +++{sql_str[select_pos: from_pos+4]}+++")
            logger.info(f"sql_str part1b: +++{sql_str[select_pos+6: from_pos]}+++")
        else:
            logger.info(f"sql_str part1: {sql_str[select_pos: select_pos + 80]} ... {sql_str[from_pos - 80: from_pos + 4 ]}")

        if where_pos - from_pos < 200:
            logger.info(f"sql_str part2: +++{sql_str[from_pos: where_pos+5]}+++")
            logger.info(f"sql_str part2a: +++{sql_str[from_pos + 4: where_pos]}+++")
        else:
            logger.info(f"sql_str part2: +++{sql_str[from_pos: from_pos + 80]} ... {sql_str[where_pos - 80: where_pos + 5 ]}+++")
    
        logger.info(f"calling get_tables:")
        tables = get_tables(sql_str, sql_str[from_pos + 4: where_pos])
        logger.info(f"tables: {tables}")
        logger.info(f"calling split_fields:")
        field_list = split_fields(sql_str[select_pos+6: from_pos])
        parsed_fields = [parse_field(field) for field in field_list]
        logger.info(f"fields: {field_list}")
        logger.info(f"parsed_fields: {parsed_fields}")
        entries = merge_tables_columns(tables, parsed_fields)
        logger.info(f"entries: {entries}")
    field_entries.append(entries)
        # columns.append()
    logger.info(f"field_entries: {field_entries}")
    exit(0)
    #logger.info(find_sub_queries(query_no_comments))

    # get tables
    tables = build_table_list(query_no_comments)
#    rest_of_query = get_rest_after_main_select(query_no_comments)
#    logger.info(rest_of_query)
#    print_buffer()
#    logger.info(split_unions(rest_of_query))

    print_buffer()
    logger.info(process_other_join_types('table1 LEFT JOIN table2 ON table1.column1 = table2.column2'))
    print_buffer()
    logger.info(process_other_join_types('\n table1 LEFT OUTER JOIN TABLE2 ON TABLE1.COLUMN1 = TABLE2.COLUMN2 \nLEFT OUTER JOIN table3 ON table2.column1 = table3.column2'))
    print_buffer()
    table_entries = ['TABLE1', 'TABLE2']
    logger.info(create_table_entries(table_entries))
    table_entries = ['TABLE1 AS T1', 'TABLE2']
    logger.info(create_table_entries(table_entries))

    print_buffer()
    sql_str = 'SELECT * FROM table1 AS t1, table2 AS t2 WHERE t1.column1 = t2.column2;'
    logger.info(build_table_list(sql_str))

    print_buffer()
    sql_str = "(SELECT column1 FROM table1, table2, table3) AS table4"
    logger.info(f"process_subquery results: {process_subquery(sql_str)}")

    print_buffer()
    sql_str = "(SELECT column2 FROM table2 AS t2, table3 AS t3, table4 AS t4 WHERE table2.column2 = table3.column2 AND table3.column2 = table4.column3) AS table5"
    logger.info(f"process_subquery results: {process_subquery(sql_str)}")

    print_buffer()
    sql_str = "(SELECT column1 FROM table1 UNION SELECT column2 FROM table2 ) AS table5"
    logger.info(f"process_subquery results: {process_subquery(sql_str)}")

    print_buffer()
    sql_str = "(SELECT column1 FROM table1 AS t1 UNION SELECT column2 FROM table2 AS t2 UNION SELECT column2 FROM table3 AS t3) AS table5"
    logger.info(f"process_subquery results: {process_subquery(sql_str)}")

    print_buffer()
    table_list = build_table_list(query1)
    print_buffer()
    logger.info(f"table_list: {table_list}")

    import pprint
    print_buffer()
    pprint.pprint(table_list)

#    flat_table_list = list(chain.from_iterable(table_list))
#    print_buffer()
#    pprint.plogger.info(flat_table_list)

    print_buffer()
    logger.info(table_list[0])
    logger.info(table_list[-1])

    print_buffer()
    list1 = [('item1.1', 'item1.2'), ('item2.1', 'item2.2')]
    list1.append(('item3.1','item3.2'))
    list1.append([('item4.1','item4.2')])
    logger.info(list1)

    list2 = [('item4.1', 'item4.2'), ('item5.1', 'item5.2')]
    list2.extend([('item6.1','item6.2')])
    logger.info(list2)

    print_buffer()
    print("...")
    print('\nabcdef\n'.lstrip().rstrip())
    print("...")

    print(datetime_format)
