import re
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from extract_utilities import *

def print_buffer(size = 3):
    print("\n" * size)

def remove_comments(sql_str):
    print("entering remove_comments")
    # remove the /* */ comments
    q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sql_str)

    # remove whole line -- and # comments
    lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]
#    print_buffer()

#    for line in lines:
#        print(line)
#    print_buffer()

    # remove trailing -- and # comments
    q = " ".join([re.split("--|#", line)[0] for line in lines])

    print("returning from remove_comments")
    return q

def get_main_select(sql_str):
    return re.search('SELECT (.+?) FROM', sql_str, re.DOTALL).group(0)

def get_rest_after_main_select(sql_str):
    return sql_str[sql_str.upper().find("FROM")+4:]
#    return re.search('FROM (.+?)', sql_str, re.DOTALL).group(1)

def find_sub_queries(sql_str):
    return re.findall('(SELECT (.+?) FROM', sql_str, re.DOTALL)

def find_queries(sql_str):
    return re.findall('SELECT (.+?) FROM', sql_str, re.DOTALL)

def field_split(fields_string, separator=','):
    """
    splits a string using separator unless the separator is within parentheses
    takes string of fields

    returns list of fields
    """
    print(f"entering field_split: fields_string = {fields_string}")
    fields = []
    parenthesis_count = 0
    current_field = ''
    for char in fields_string.lstrip().rstrip():
        if char == ',' and parenthesis_count == 0: # split here by appending current field to fields and setting current_field to empty string
            fields.append(current_field)
            current_field = ''
            print(f"appending: {current_field}")
            print(f"fields: {fields}")
        else:
            current_field = current_field + char
            if char == '(':
                parenthesis_count = parenthesis_count + 1
            elif char == ')':
                parenthesis_count = parenthesis_count - 1
    
    # add last field if it was not already added to fields
    if current_field != fields[-1]:
        fields.append(current_field.lstrip().rstrip())

    return fields
            
def parse_field(field):
    table_name, column_name, alias = '', '', ''
    field = field.upper().lstrip()
    if field.upper().find(' AS ') == -1: # AS not found in string
        if field.find('.') > 0: # no AS, but contains dot (.) to separate table name and column name
            table_name, column_name = field.split('.')
        else: # no AS and no dot (only column name)
            column_name = field
    else:
        source = re.search('(.+?) AS ', field, re.DOTALL)
        #print("source: ", source.group(1))
        #print("---", source.group(1).upper()[0])
        if source:
            # check for function names
            if source.group(1).startswith('CASE') or source.group(1).lstrip().startswith('ROUND') \
                or source.group(1).startswith("'") or source.group(1).startswith('"'):
                table_name = ''
                column_name = source.group(1)
            else: 
                if source.group(1).find('.') > -1:
                    table_name, column_name = source.group(1).split('.')
                else: # no dot means no table alias
                    table_name = ''
                    column_name = source.group(1)
            
            #alias  = re.search(' AS (.+?)', field.upper(), re.DOTALL).group(1)
            alias  = field[field.find(' AS ') + 4:]
    print(f"alias: {alias}, {len(alias)}")
    if len(alias) == 0:
        alias = column_name
    return (table_name, column_name, alias)

def process_select(sql_str):
    """[generate a list of columns. each column will include the source table (or alias), column name, and column alias]
    
    Arguments:
        sql_str {str} -- [text between SELECT AND FROM. may or may not include SELECT & FROM as endpoints]
    
    Returns:
        [list of tuples] -- [each tuple will include the source table (or alias), column name, and column alias. if an alias is not specified, 
        column_name will be used as the alias]
        
    """
    # remove SELECT from beginning of sql_str
    if sql_str.lstrip().upper().find('SELECT') == 0:
        sql_str = sql_str[sql_str.lstrip().upper().find('SELECT')+len('SELECT')+1:]
    # remove FROM from end of sql_str
    if sql_str.lstrip().rstrip().upper().find('FROM') == (len(sql_str.lstrip().rstrip().upper()) - 4):
        sql_str = sql_str[:sql_str.lstrip().upper().find('FROM', len(sql_str.lstrip().rstrip()) - 6)]

    fields = field_split(sql_str)
    columns = [parse_field(field) for field in fields]
    return columns

def build_column_list(sql_str):
    """go through steps of building a list of tables used in a query
    
    Arguments:
        sql_str {str} -- text string containing the sql query
    """
    query_no_comments = remove_comments(sql_str)
    print(f"sql_str: {sql_str}")
    print(f"query_no_comments: {query_no_comments}")
    columns = get_columns(sql_str)

    return columns


if __name__ == "__main__":
    with open(sys.argv[1], 'r') as myfile:
        query1 = myfile.read()


    print_buffer()
    query_no_comments = remove_comments(query1)

    #print(find_sub_queries(query_no_comments))
    # get columns
#    columns = get_columns(query_no_comments)
    main_select = get_main_select(query_no_comments)
    print(main_select)
    print_buffer()
    columns = process_select(main_select)
    for column in columns:
        print(column)

    print_buffer()
    sql_str = " table_name1.column_name1 AS column_alias1, table_name2.column_name2 AS column_alias2"
    print(field_split(sql_str))
