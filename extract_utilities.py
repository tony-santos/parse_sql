import re
import string

from loguru import logger

def print_buffer(size = 3):
    print("\n" * size)

def remove_comments(sql_str):

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

    return q

def split_unions(sql_str):
    return re.split('UNION', sql_str)

def get_main_select(sql_str):
    return re.search('SELECT (.+?) FROM', sql_str, re.DOTALL).group(0)

def get_rest_after_main_select(sql_str):
    return sql_str[sql_str.upper().find("FROM")+4:]
#    return re.search('FROM (.+?)', sql_str, re.DOTALL).group(1)

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
        logger.info(f"index: {index},      token: {token}")
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
    # make copy of sql_str
    orig_sql_str = sql_str
    sql_str = sql_str.upper()[from_pos:]
    select_level = 1
    from_level = 0
    paren_level = 0
    open_paren_found = False
    matching_where_position = -1
    tokens = sql_str.split() # generate list of tokens in order to grab multiple tokens later
    for index, token in enumerate(tokens[1:]):  # skip 1st token (shuold be FROM)
        logger.info(f"index: {index},      token: {token}")
        if token == '(':
            open_paren_found = True
            paren_level = paren_level + 1
            logger.info(f"paren found in token {index}     paren_level = {paren_level}")
        elif token == ')':
            paren_level = paren_level - 1
            logger.info(f"paren found in token {index}     paren_level = {paren_level}")
            if paren_level == -1 or (paren_level == 0 and open_paren_found): # either closing paren for subquery already open or end of subquery after FROM
                logger.info(f"ending paren found in token {index}   need to match correct string {tokens[index - 4:index + 1]}")
                logger.info(f"len(tokens): {len(tokens)}")
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
            logger.info(f"FROM found. select_level: {select_level}")

            if from_level == 0:
                matching_where_position = from_pos + where_position
                break
    logger.info(f"select_level: {select_level}")
    logger.info(f"returning from get_matching_where_position.      matching_where_position: {matching_where_position} end of from clause: {orig_sql_str[matching_where_position - 20:matching_where_position]}+++")
    return matching_where_position

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

def merge_tables_columns(tables, columns):
    entries = []
    table_dict = {}
    logger.info(f"entering merge_tables-columns...")
    logger.info(f"tables: {tables}")
    logger.info(f"columns: {columns}")
    for table_name, table_table_alias in tables:
        table_dict[table_table_alias] = (table_name, table_table_alias)
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
    start_pos = sql_str.find(subquery) + len(subquery) + 1 # should be ) or space
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
        

if __name__ == "__main__":
    
    query1 = """
    CREATE OR REPLACE VIEW rlsc_master.v_webmtbs_unified_view
    AS
    /* should be removed */
    -- should be removed
    --- should be removed

    ---without the DISTINCT, the time takes to generate the mv_v_ will be cut a lot, and the total number of records will not differ much
    SELECT protocol_instance.md_batch_name AS experiment_run_info_metid,  -- should be removed
            protocol_instance.md_compound_name AS compound_name_metid,  /* should be removed */
            "substring"(protocol_instance.md_compound_name::text, 1, 11) AS parent_id_metid, protocol_instance.md_id AS protocol_instance_id_metid, clustered_compound.md_id AS clustered_compound_id_metid, compound_data.md_area_ms AS ms_signal_area_metid, compound_data.md_id AS compound_data_id_metid, compound_data.md_ion_formula AS metabolite_ion_formula_metid, compound_data.metabolite_mz_value_metid, compound_data.metabolite_mz_diff_ppm_metid, compound_data.metabolite_mz_diff_mda_metid, compound_data.md_name AS metabolite_name_metid, compound_data.metabolite_area_abs_metid, mim_calc.mim AS metabolite_mim_metid, mass_shift_calc.mass_shift AS mass_shift_metid, compound_data.retention_time_metid, compound_data.md_z AS metabolite_z_metid, 
            
    FROM (
    SELECT animal_txt, study, studyday
    FROM rlsc_master.mv_v_salar_clinical_signs
    UNION
    SELECT animal_txt, study, studyday
    FROM rlsc_master.mv_v_salar_invivo_results
    UNION
    SELECT animal_txt, study, studyday
    FROM rlsc_master.mv_v_salar_food_consumption
    UNION
    SELECT animal_txt, study, NULL as studyday
    FROM rlsc_master.mv_v_salar_necropsy
    UNION
    SELECT animal_txt, study, NULL as studyday
    FROM rlsc_master.mv_v_salar_histopathology
    ) mv_v_salar_animal_txt_study
    LEFT JOIN (
    SELECT animal_txt2, study2, studyday2
    FROM rlsc_master.mv_v_salar_clinical_signs
    UNION
    SELECT animal_txt, study, studyday
    FROM rlsc_master.mv_v_salar_invivo_results
    UNION
    SELECT animal_txt, study, studyday
    FROM rlsc_master.mv_v_salar_food_consumption
    UNION
    SELECT animal_txt, study, NULL as studyday
    FROM rlsc_master.mv_v_salar_necropsy
    UNION
    SELECT animal_txt, study, NULL as studyday
    FROM rlsc_master.mv_v_salar_histopathology
    ) mv_v_salar_animal_txt_study2
    LEFT JOIN rlsc_master.webmtbs_md_protocol_instance protocol_instance

    LEFT JOIN rlsc_master.mv_v_webmtbs_unified_view_compound_data compound_data ON protocol_instance.md_id = compound_data.md_protocol_instance_id
    LEFT JOIN rlsc_master.webmtbs_md_experiment experiment ON compound_data.md_experiment_id = experiment.md_id
    LEFT JOIN rlsc_master.mv_v_webmtbs_unified_view_clustered_compound
    
    clustered_compound ON clustered_compound.compound_md_id = compound_data.md_id
    ---LEFT JOIN rlsc_master.webmtbs_md_compound_data compound_sub ON compound_sub.md_id = experiment.md_substrate_id
    WHERE protocol_instance.md_approved = 1
    with no schema binding;
    """

    print_buffer()
    query_no_comments = remove_comments(query1)

    #print(find_sub_queries(query_no_comments))
    # get columns
#    columns = get_columns(query_no_comments)
