# remove comments
# parse query into tokens?
# scan until you reach a SELECT (or use find to get next SELECT)
# from there, find FROM
# from the FROM, locate WHERE or end of query (';', ')', or EOF)
# get all columns between SELECT and FROM: 
# get all tables between FROM and WHERE: separated by ', or some kind of join (INNER, LEFT/LEFT OUTER, RIGHT/RIGHT OUTER, OUTER/FULL OUTER)
# if SELECT found between SELECT and FROM: process correlated subquery
# if SELECT found between FROM and WHERE: process subquery
# merge or join list of column to list of tables by table alias
# repeat above steps on rest of the query 

# need checks for subqueries to keep everything at the correct level

# get_matching_paren - only needed for subqueries?
# get_matching_from - pass in location of SELECT (may have to look for correlated subqueries. remove in intial version )
# get_matching_where - pass in location of FROM (need to look for subqueries)
# get_field_list - pass locations of SELECT and FROM, use parse_fields to split on , but allow function calls
# get_table_list - pass locations of FROM and WHERE, table names follow FROM and JOIN. need to get alias following table name by space or AS
# process_subquery - called when ( found between FROM and WHERE. pass location of '('
# join_lists

from datetime import datetime
from loguru import logger
import sys
import string



datetime_format = "%Y%m%d-%H%M"
logfile_datetime = datetime.now().strftime(datetime_format)

logger.add(f"output-{sys.argv[0]}-{logfile_datetime}.log", backtrace=True, diagnose=True)

def get_select_position(sql_str, start_pos=0):
    select_position = sql_str.upper().find('SELECT', start_pos)
    if sql_str[select_position + 6] in string.whitespace: # we matched keyword and not a substring (unless string ends with select and is followed by whitespace)
        pass
    else:
        select_position = get_select_position(sql_str, select_position+6) # we matched a substring. keep looking
        
    logger.info(f"select_position = {select_position}")
    logger.info(f"character after 'SELECT':{sql_str[select_position + 6]}:")
    logger.info(f"whitespace after SELECT: {sql_str[select_position + 6] in string.whitespace}")
    if select_position > -1:
        logger.info(f"before SELECT: {sql_str[:select_position]}")

    return select_position

def get_matching_from_position(sql_str, start_pos=0):
    sql_str = sql_str.upper()
    select_level = 1
    matching_from_position = -1
    for token, index in enumerate(sql_str.split()):
        if token == 'SELECT':
            select_level = select_level + 1
            logger.info(f"another select found. select_level: {select_level}")
        elif token== 'FROM':
            select_level = select_level - 1
            logger.info(f"FROM found. select_level: {select_level}")

            if select_level == 0:
                matching_from_position = index
                break
    logger.info(f"paren_level: {select_level}")

    return matching_from_position

def junk():
    sql_str = sql_str.upper()
    from_postion.find('FROM', start_pos)
    logger.info(f"from_position: {from_position}")
    if from_position > -1:
        logger.info(f"before FROM: {from_postion}")
        select_position = sql_str.find('SELECT', 0, from_position)
        if select_position > -1:
            logger.info(f"SELECT found in field list at position: {sql_str}. probable subquery")
            logger.info(f"sql_str: {sql_str[:from_position]}")

def get_from_position(sql_str, start_pos=0):
    sql_str = sql_str.upper()
    from_position = sql_str.find('FROM', start_pos)
    logger.info(f"from_position: {from_position}")
    if from_position > -1:
        logger.info(f"before FROM: {from_position}")
        select_position = sql_str.find('SELECT', 0, from_position)
        if select_position > -1:
            logger.info(f"SELECT found in field list at position: {sql_str}. probable subquery")
            logger.info(f"sql_str: {sql_str[:from_position]}")

def get_matching_paren_position(sql_str, open_paren_position=0):
    paren_level = 1
    matching_paren_position = -1
    for char, index in enumerate(sql_str):
        if char == '(':
            paren_level = paren_level + 1
            logger.info(f"another opening paren found. paren_level: {paren_count}")
        elif char== ')':
            paren_level = paren_level - 1
            logger.info(f"closing paren found. paren_level: {paren_count}")
            if paren_level == 0:
                matching_paren_position = index
                break
    logger.info(f"paren_level: {paren_level}")

    return matching_paren_position


if __name__ == "__main__":
    query1 = "SELECT    col1, col2, col3 FROM tab1"
    logger.info(f"query: {query1}")
    logger.info(f"get_select_position(query1): {get_select_position(query1)}")
    logger.info(f"get_from_position(query1): {get_from_position(query1)}")
 
    query2 = "SELECT\ncol1, col2, col3 FROM (SELECT col1, col2, col3 FROM tab1)"
    logger.info(f"query: {query2}")
    logger.info(f"get_select_position(query2): {get_select_position(query2)}")
    logger.info(f"get_from_postion(query2): {get_from_position(query2)}")
    logger.info(f"get_matching_from_position(query2): {get_matching_from_position(query2)}")
    
    query3 = "SELECT col1, col2, col3 FROM (SELECT col1, col2, col3 FROM tab1) WHERE col1 < 10"
    logger.info(f"query: {query3}")
    logger.info(f"get_select_position(query3): {get_select_position(query3)}")
    logger.info(f"get_from_position(query3): {get_from_position(query3)}")
    logger.info(f"get_matching_from_position(query3): {get_matching_from_position(query3)}")
    
