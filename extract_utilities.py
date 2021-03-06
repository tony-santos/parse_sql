import re
import string

from loguru import logger

def print_buffer(size = 3):
    print("\n" * size)

def remove_comments(sql_str):

    # remove the /* */ comments
    q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sql_str)
    logger.info(f"after 1st pass:")
    logger.info(sql_str)

    # remove whole line -- and # comments
    lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]

    # remove trailing -- and # comments
    # q = " ".join([re.split("--|#", line)[0] for line in lines])
    # 2020-01-20: removed | from regex because it was splitting column containing concatenation (||)
    q = " ".join([re.split("--", line)[0] for line in lines])
    logger.info(f"q:+++{q}+++")

    return q

def reformat_query(sql_str):
    """removes comments and converts to all uppercase and all whitespace to single spaces in order to simplify substring matching
    
    Arguments:
        sql_str {str} -- query to be reformatted
    
    Returns:
        [str] -- string containing the query with no comments and all whitespace converted to single spaces
    """
    query_no_comments = remove_comments(sql_str.upper())
    logger.info(f"query after comments removed:")
    logger.info(f"{query_no_comments}")

    query_no_tabs_newlines = query_no_comments.replace("\t", " ").replace("\n", " ")
    logger.info(f"query after tabs and newlines removed:")
    logger.info(f"{query_no_tabs_newlines}")
    query_single_spaces = query_no_tabs_newlines
    while query_single_spaces.find("  ") > -1:
        query_single_spaces = query_single_spaces.replace("  ", " ")

    actual_select_position = 0
    actual_from_position = 0
    select_position = -6 # initialize to -6 so 1st pass will start at position 0
    from_position = 0

    reformatted_query = query_single_spaces
    logger.info(f"sql_str: {sql_str}")
# marker for code to be moved

    return reformatted_query


def get_select_position(sql_str, start_pos=0):
    logger.info(f"entering get_select_position: start_pos = {start_pos}   sql_str: {sql_str[start_pos: start_pos+200]}")
    select_position = sql_str.upper().find('SELECT', start_pos)
    if select_position == -1:
        logger.info(f"SELECT not found. should have hit more_selects = False")
    else:
        if sql_str[select_position + 6] in string.whitespace: # we matched keyword and not a substring (unless string ends with select and is followed by whitespace)
            logger.info(f"matched SELECT keyword")
            pass
        else:
            logger.info(f"matched SELECT but not keyword: {sql_str[select_position:select_position + 15]}")
            select_position = get_select_position(sql_str, select_position+6) # we matched a substring. keep looking
            
        logger.info(f"select_position = {select_position}")
        logger.info(f"character after 'SELECT':{sql_str[select_position + 6]}:")
        logger.info(f"whitespace after SELECT: {sql_str[select_position + 6] in string.whitespace}")
        if select_position > -1:
            logger.info(f"before SELECT: {sql_str[max(0,select_position - 100):select_position]}")
    
    logger.info(f"returning from get_select_position: {select_position}")
    return select_position

def get_matching_from_position(sql_str, start_pos=0):
    logger.info(f"entering get_matching_from_position: start_pos: {start_pos}")
    # make copy of sql_str
    orig_sql_str = sql_str
    sql_str = sql_str.upper()[start_pos:]
    select_level = 1
    matching_from_position = -1
    tokens = sql_str.split() # generate list of tokens in order to grab multiple tokens later
    for index, token in enumerate(tokens[1:]):  # skip 1st token (shuold be SELECT)
        # logger.info(f"index: {index},      token: {token}")
        if token == 'SELECT':
            select_level = select_level + 1
            logger.info(f"another select found. select_level: {select_level}")
            select_position = sql_str.find('SELECT')
        elif token== 'FROM':
            logger.info(f"FROM found in token: {index}")
            select_level = select_level - 1
            logger.info(f"FROM found. select_level: {select_level}")

            if select_level == 0:
                match_string = " ".join(tokens[(index - 2):(index + 2)]) # assume at least 3 tokens before closing FROM encountered. use +2 because we are starting with token 1 instead of token 0
                logger.info(f"match_string: {match_string}")
                matching_from_position = sql_str.find(match_string) + len(match_string) - 4 # subtract 4 for length of FROM
                logger.info(f"match_string location: {matching_from_position}   --{sql_str[matching_from_position - 5: matching_from_position + 5]}--")
                logger.info(f"match_string location: {matching_from_position}   ++{sql_str[matching_from_position:matching_from_position+5]}++")
                matching_where_position = start_pos + matching_from_position
                logger.info(f"orig_sql_str[matching_where_position] = +++{orig_sql_str[matching_where_position]}+++")
                break
    logger.info(f"select_level: {select_level}")
    matching_from_position = matching_from_position + start_pos
    logger.info(f"returning from get_matching_from_position.      matching_from_position: {matching_from_position}")
    return matching_from_position

def get_matching_where_position(sql_str, from_pos):
    # take sql_str after from_pos
    # break into tokens
    # get where or end of query at same level ('WHERE', ';', ')' if paren-level == -1)
    logger.info(f"entering get_matching_where_position: from_pos: {from_pos}")
    logger.info(f"sql_str: {sql_str[from_pos: from_pos + 130]}")

    orig_sql_str = sql_str
    sql_str = sql_str.upper()[from_pos:]
    select_level = 1
    from_level = 0
    paren_level = 0
    open_paren_found = False
    matching_where_position = -1
    tokens = sql_str.split() # generate list of tokens in order to grab multiple tokens later
    for index, token in enumerate(tokens[1:]):  # skip 1st token (shuold be FROM)
        # logger.info(f"index: {index},      token: {token}")
        if token == '(':
            open_paren_found = True
            paren_level = paren_level + 1
            logger.info(f"paren found in token {index}     paren_level = {paren_level}")
        elif token == ')':
            paren_level = paren_level - 1
            logger.info(f"paren found in token {index}     paren_level = {paren_level}")
            if paren_level == -1: # or (paren_level == 0 and open_paren_found): # either closing paren for subquery already open or end of subquery after FROM
                logger.info(f"ending paren found in token {index}   need to match correct string {tokens[index - 4:index + 1]}")
                logger.info(f"len(tokens): {len(tokens)}")
                logger.info(f"paren_level: {paren_level}     open_paren_found: {open_paren_found}")
                if index < 5:
                    match_string = " ".join(tokens[:index + 2])
                    logger.info(f"match_string1: {match_string}")
                else:
                    match_string = " ".join(tokens[index - 2:index + 1]) # assume at least 3 tokens before closing paren encountered
                    logger.info(f"match_string2: {match_string}")
                match_string_location = sql_str.find(match_string) + len(match_string) - 1
                logger.info(f"match_string location: {match_string_location}   --{sql_str[match_string_location - 2: match_string_location + 2]}--")
                logger.info(f"match_string location: {match_string_location}   ++{sql_str[match_string_location]}++")
                matching_where_position = from_pos + match_string_location + 1
                break
        elif token =='UNION': # should be matching where position if select_level == 1
            logger.info(f"UNION found in token {index}")
            logger.info(f"select_level: {select_level}")
            if select_level == 1 and paren_level == 0:
                match_string = " ".join(tokens[index - 1:index + 2]) # assume at least 3 tokens before UNION encountered. using +2 because we started on index 1
                logger.info(f"match_string: {match_string}")
                match_string_location = sql_str.find(match_string) + len(match_string) - 5
                logger.info(f"match_string location: {match_string_location}   --{sql_str[match_string_location - 4: match_string_location + 4]}--")
                logger.info(f"match_string location: {match_string_location}   ++{sql_str[match_string_location]}++")
                matching_where_position = from_pos + match_string_location
                break
        elif token == 'SELECT':
            select_level = select_level + 1
            logger.info(f"another select found. select_level: {select_level}")
            select_position = sql_str.find('SELECT')
        elif token== 'FROM':
            logger.info(f"FROM found in token: {index}")
            from_position = sql_str.find('FROM')
            select_level = select_level - 1
            logger.info(f"FROM found. select_level: {select_level}")

            # if select_level == 0:
            #     matching_from_position = start_pos + from_position
        elif token== 'WHERE':
            logger.info(f"WHERE found in token: {index}")
            where_position = sql_str.find('WHERE')
            from_level = from_level - 1
            logger.info(f"WHERE found. from_level: {from_level}")

            if from_level == 0:
                logger.info(f"found WHERE and from_level ==0")
                matching_where_position = from_pos + where_position
                break
        elif token == ';':
            logger.info(f"ending ';' found")
            where_position = sql_str.find(' ; ')
            matching_where_position = from_pos + where_position
            break

    logger.info(f"select_level: {select_level}")
    logger.info(f"returning from get_matching_where_position.      matching_where_position: {matching_where_position} end of from clause: {orig_sql_str[matching_where_position - 20:matching_where_position]}+++")
    return matching_where_position

def get_matching_paren_position(sql_str, open_paren_position=0):
    paren_level = 0
    matching_paren_position = -1
    for index, char in enumerate(sql_str):
        if char == '(':
            paren_level = paren_level + 1
            logger.info(f"another opening paren found. paren_level: {paren_level}")
        elif char== ')':
            paren_level = paren_level - 1
            logger.info(f"closing paren found. paren_level: {paren_level}")
            if paren_level == 0:
                matching_paren_position = index
                break
    logger.info(f"paren_level: {paren_level}")

    return matching_paren_position

def get_join_positions(sql_str):
    join_positions = []
    start_pos = 0
    while sql_str.find(' JOIN ', start_pos) > -1:
        new_join_position = sql_str.find(' JOIN ', start_pos)
        join_positions.append(new_join_position)
        start_pos = new_join_position + 1
    logger.info(f"join_positions: {join_positions}")
    return join_positions


def parse_field(field):
    logger.info(f"entering parse_field: field = {field}")
    table_name, column_name, alias = '', '', ''
    field = field.upper().lstrip()
    if field.upper().find(' AS ') == -1: # AS not found in string
        if field.find('.') > 0: # no AS, but contains dot (.) to separate table name and column name
            table_name, column_name = field.split('.')
        else: # no AS and no dot (only column name)
            column_name = field
    else:
        source = re.search('(.+?) AS ', field, re.DOTALL)
        logger.info(f"type(source.group(1)): {type(source.group(1))}")
        logger.info(f"source: {source.group(1)}")
        logger.info("---", source.group(1).upper()[0])
        if source:
            # check for function names
            if source.group(1).startswith('CASE') or source.group(1).lstrip().startswith('ROUND') \
                or source.group(1).startswith("'") or source.group(1).startswith('"'):
                table_name = 'DERIVED'
                column_name = source.group(1)
            else: 
                if source.group(1).find('.') > -1:
                    table_name, column_name = source.group(1).split('.')
                else: # no dot means no table alias
                    table_name = ''
                    column_name = source.group(1)
            
            #alias  = re.search(' AS (.+?)', field.upper(), re.DOTALL).group(1)
            alias  = field[field.find(' AS ') + 4:]
    logger.info(f"alias: {alias}, {len(alias)}")
    if len(alias) == 0:
        alias = column_name
    return (table_name, column_name, alias)

def merge_tables_columns(tables, columns):
    entries = []
    table_dict = {}
    logger.info(f"entering merge_tables-columns...")
    logger.info(f"tables: {tables}")
    logger.info(f"columns: {columns}")
    for table_name, table_table_alias in tables:
        table_dict[table_table_alias] = (table_name, table_table_alias)
        if table_name.find('.') > -1: # split scema name from table name
            logger.info(f"{table_name} contains schema name. splitting and adding second entry")
            schema_name, split_table_name = table_name.split('.')
            table_dict[split_table_name] = table_table_alias
    for key, value in table_dict.items():
        logger.info(f"key: +++{key}+++   value: +++{value}+++")
    for table_alias, column_name, field_name in columns:
        logger.info(f"table_alias = {table_alias},   column_name = {column_name},   field_name = {field_name}")
        if table_alias == '':
            if len(tables) == 1: # all columns belong to same table (expected)
                logger.info(f"no table alias. all columns belong to: {tables[0][0]}")
                table_name, table_alias = tables[0]
                entries.append((field_name, table_name, column_name))
            else: # no table alias but multiple tables. might leave table_name blank
                logger.info(f"no table alias and multiple tables. not sure how to resolve. leaving table name blank for {field_name}")
                entries.append((field_name, '', column_name))
        else: # lookup table by alias
            table_name, table_table_alias = table_dict[table_alias]
            entries.append((field_name, table_name, column_name))
    return entries

def get_subquery_alias(sql_str, subquery):
    # to get subquery_alias, find subquery in sql_str, take string from end of subquery to end of sql_str, split, take first token (unless ), then take second token
    sql_str = sql_str.upper()
    subquery = subquery.upper()
    logger.info(f"sql_str: +++{sql_str}+++")
    logger.info(f"subquery: +++{sql_str}+++")
    start_pos = sql_str.find(subquery) + len(subquery) + 1 # should be ) or space
    logger.info(f"start_pos: {start_pos}")
    rest_of_query = sql_str[start_pos:]
    rest_of_query_tokens = rest_of_query.split()
    logger.info(f"rest_of_query: {rest_of_query}")
    logger.info(f"first few tokens: {rest_of_query_tokens[0:3]}")
    for token in rest_of_query_tokens:
        if token == ')':
            continue
        else:
            subquery_alias = token
            break
    
    return subquery_alias

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
            

def get_tables_after_froms(sql_str, select_froms):
    sql_str = sql_str.upper()
    table_list = []

    new_from_location = prev_from_location = sql_str.find('FROM ')
    after_from = sql_str
    start_pos = end_pos = 0
    for new_select_location, new_from_location in select_froms:
        logger.info(f"new_from_location: {new_from_location}")
        after_from = sql_str[new_from_location+len('FROM '):].lstrip().rstrip()
        # logger.info(f"after_from = +++{after_from}+++")
        if after_from.startswith('('):
            # ( after FROM or JOIN is a subquery)
            logger.info(f"subquery found. getting subquery alias and adding to table_list")
            end_pos = get_matching_paren_position(after_from, 0)
            subquery = after_from[:end_pos+1]
            logger.info(f"subquery: +++{subquery}+++")
            subquery_alias = get_subquery_alias(sql_str, subquery)
            table_list.append(('SUBQUERY', subquery_alias))
            # get table names next to JOIN keywords
            start_pos = end_pos + 1
        else:  # should be a table
            logger.info(f"table found. getting table entry and adding to table_list")
            table_list.append(get_table_next_to_from(sql_str, new_from_location))

        join_pos = sql_str.find(' JOIN ', start_pos, end_pos)
        while join_pos > -1:
            logger.info(f"join_pos: {join_pos}")
            table_list.append(get_table_next_to_join(sql_str, join_pos))
            join_pos = sql_str.find(' JOIN ', join_pos + 6)

        # get location of next from
        prev_select_location = new_select_location
        prev_from_location = new_from_location
        # new_from_location = sql_str.find(' FROM ', prev_from_location+1) # need +1 to not match same substring
        logger.info(f"prev_select_location: {prev_select_location}")
        logger.info(f"prev_from_location: {prev_from_location}")
    logger.info(f"returning from get_tables_after_froms")
    logger.info(f"table_list: {table_list}")

    return table_list

def get_table_next_to_from(sql_str, from_pos): # does not handle subqueries
    logger.info(f"entering get_table_next_to_from: sql_str: +++{sql_str[from_pos:from_pos + 100]}+++")
    tokens = sql_str[from_pos:].split()[0:4]
    logger.info(f"tokens: {tokens}")
    print(f"tokens: {tokens}")
    if tokens[0] == 'FROM':
        tokens = tokens[1:] # drop 1st token

    table_name = tokens[0]

    #check for alias
    if tokens[1] in ['ON', 'UNION', 'LEFT', 'RIGHT', 'WHERE', ')', ',']: # no alias
        table_alias = table_name
    elif tokens[1] == 'AS': # next token is the alias
        table_alias = tokens[2]
    else:
        table_alias = tokens[1]

    return (table_name, table_alias)

def get_table_next_to_join(sql_str, join_pos): # does not handle subqueries
    logger.info(f"entering get_table_next_to_from: join_pos = {join_pos}")
    logger.info(f"entering get_table_next_to_from: sql_str: +++{sql_str[join_pos:join_pos + 100]}+++")
    tokens = sql_str[join_pos:].split()[0:4]
    logger.info(f"tokens: {tokens}")
    if tokens[0] == 'JOIN':
        tokens = tokens[1:] # drop 1st token

    table_name = tokens[0]

    #check for alias
    if tokens[1] in ['ON', 'UNION', 'LEFT', 'RIGHT', 'WHERE', ')']: # no alias
        table_alias = table_name
    elif tokens[1] == 'AS': # next token is the alias
        table_alias = tokens[2]
    else:
        table_alias = tokens[1]

    return (table_name, table_alias)
