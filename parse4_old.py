import re

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
    fields = []
    parenthesis_count = 0
    current_field = ''
    for char in fields_string.lstrip().rstrip():
        if char == ',' and parenthesis_count == 0: # split here by appending current field to fields and setting current_field to empty string
            fields.append(current_field)
            current_field = ''
        else:
            current_field = current_field + char
            if char == '(':
                parenthesis_count = parenthesis_count + 1
            elif char == ')':
                parenthesis_count = parenthesis_count - 1
    
    # add last field if it was not already added to fields
    if current_field != fields[-1]:
        fields.extend(current_field.lstrip().rstrip())

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

def get_tables(sql_str):
    """extract table names from sql query
    
    might need multiple passes to get all table names and may lead to additional columns being found

    Arguments:
        sql_str {str} -- string containing query
    Returns:
        list_of tuples - each tuple will contain the name of the table and its alias. if no alias provided, the alias will be the table name. table names will include schema names if
        they were part of the query
    """
    # start with text between FROM and WHERE or between FROM and ';' and return list of tables by splitting on ',' 
    sql_str = sql_str.upper().lstrip().rstrip()
    rest_of_query = get_rest_after_main_select(sql_str)
    print(f"rest_of_query: {rest_of_query}")
    if rest_of_query[-1] == ';':
        rest_of_query = rest_of_query[:-1] 

    print(f"rest_of_query2: {rest_of_query}")
    from_clause = rest_of_query
    print(f"from_clause1: {from_clause}")
    if from_clause.find('WHERE') > 0:
        from_clause = from_clause[:from_clause.find('WHERE')]
        print(f"from_clause2: {from_clause}")
    
    # left off here. need to figure out what kind of table list has been passed. if simple list separated by commas or combination of different JOIN keywords
    if from_clause.find('JOIN') == -1: # should be simple list of tables separated by comma
        table_lines = from_clause.split(',')
    else: # since the word JOIN was found, assume it  is a series of LEFT, RIGHT, INNER, or OUTER JOINS
        table_lines = process_other_join_types(from_clause)

    for table_line in table_lines:
        print(table_line)
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
    print(from_clause)
    table_lines_raw = from_clause.split(' JOIN') # after splitting on JOIN, all table lines except last should have extra keywords at the end to be removed
    table_lines_raw2 = [table_line.lstrip().rstrip() for table_line in table_lines_raw]
  
    table_lines = []
    for table_line_raw in table_lines_raw2:
        print(f"table_line_raw1: {table_line_raw}")
#        print(table_line_raw.find(' LEFT'))

        # look for LEFT & LEFT OUTER
        if table_line_raw.find(' LEFT') > -1:
            print("found LEFT or LEFT OUTER JOIN")
            if table_line_raw.find(' LEFT') > 0: # there should be a table in front of LEFT XXX JOIN. 
                print(f"table_line_raw2: {table_line_raw}")
                print(f"table_line_raw before LEFT: {table_line_raw[:table_line_raw.find(' LEFT')].rstrip()}")
                print(f"adding1: {table_line_raw[:table_line_raw.find(' LEFT')].rstrip().upper()}")
                table_lines.append(table_line_raw[:table_line_raw.find(' LEFT')].rstrip().upper()) # get line up to, but not including LEFT. should also remove LEFT OUTER
        # look for LEFT & LEFT OUTER
        elif table_line_raw.find(' RIGHT') > -1:
            print("found RIGHT or RIGHT OUTER JOIN")
            if table_line_raw.find(' RIGHT') > 0: # there should be a table in front of RIGHT XXX JOIN. 
                print(f"table_line_raw3: {table_line_raw}")
                print(f"table_line_raw before RIGHT: {table_line_raw[:table_line_raw.find(' RIGHT')].rstrip()}")
                print(f"adding2: {table_line_raw[:table_line_raw.find(' RIGHT')].rstrip().upper()}")
                table_lines.append(table_line_raw[:table_line_raw.find(' RIGHT')].rstrip().upper()) # get line up to, but not including LEFT. should also remove LEFT OUTER
    #        table_line2 = re.search('(LEFT JOIN (.+?) ON ', table_line2, re.DOTALL).group(1)
    # ON and join condition will be on next line. need to check separately at same level as check for LEFT
        if table_line_raw.find(' ON ') > -1:
            print("found JOIN CONDITION")
            if table_line_raw.find(' ON ') > 0: # there should be a table in front of ON. 
                print(f"table_line_raw4: {table_line_raw}")
                print(f"table_line_raw before ON: {table_line_raw[:table_line_raw.find(' ON ')].rstrip()}")
                print(f"adding3: {table_line_raw[:table_line_raw.find(' ON ')].rstrip().upper()}")
                table_lines.append(table_line_raw[:table_line_raw.find(' ON ')].rstrip().upper()) # get line up to, but not including LEFT. should also remove LEFT OUTER
    
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
        print(f"table_entry: {table_entry}")
        if table_entry.lstrip().startswith('('): # subquery, possibly a set of UNIONs
            subquery_table_list = process_subquery(table_entry)
            table_list.append(subquery_table_list)
        elif table_entry.upper().find(' AS ') > -1: # table entry contains an alias (part after the AS)
            table_name, table_alias = table_entry.upper().split(' AS ')
        else:
            table_name = table_alias = table_entry.lstrip().rstrip()
 
        table_list.append((table_name, table_alias))
    
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
    print(f"entering process_subquery with table_entry: {table_entry}")
    # get text after last ) and check for alias
    print(f"where is ')'? {table_entry.rfind(')')}")
    subquery_alias = ''
    if table_entry.rfind(')') > -1: # ) found and can be removed
        subquery_alias = table_entry[table_entry.rfind(')') + 1:]
        table_entry = table_entry[:table_entry.rfind(')')]
    print(f"subquery_alias for table_entry: {subquery_alias}")
    print(f"table_entry after removing ')': {table_entry}")
    # get part after FROM
    table_entry = table_entry.upper().lstrip().rstrip()
    rest_of_query = get_rest_after_main_select(table_entry)
    print(f"rest_of_query: {rest_of_query}")
    if rest_of_query[-1] == ';':
        rest_of_query = rest_of_query[:-1] 

    print(f"rest_of_query2: {rest_of_query}")
    from_clause = rest_of_query
    print(f"from_clause1: {from_clause}")
    if table_entry.upper().find('UNION') > -1: # one or more unions found in subquery. split and process each one
        subqueries = table_entry.upper().split('UNION')
        for subquery in subqueries:
            table_entries.extend(process_subquery(subquery))
    else:
        if from_clause.find('WHERE') > 0:
            from_clause = from_clause[:from_clause.find('WHERE')]
            print(f"from_clause2: {from_clause}")

        if from_clause.find('JOIN') == -1: # should be simple list of tables separated by comma
            table_lines = from_clause.split(',')
        else: # since the word JOIN was found, assume it  is a series of LEFT, RIGHT, INNER, or OUTER JOINS
            table_lines = process_other_join_types(from_clause)

        for table_line in table_lines:
            print(table_line)
        table_lines = [table_line.rstrip().lstrip() for table_line in table_lines]

        table_entries = create_table_entries(table_lines)
    
    return table_entries
    #result.append()
#    return [('', '')]



def build_column_list(sql_str):
    pass

def build_table_list(sql_str):
    """go through steps of building a list of tables used in a query
    
    Arguments:
        sql_str {str} -- text string containing the sql query
    """
    query_no_comments = remove_comments(sql_str)
    print(f"sql_str: {sql_str}")
    print(f"query_no_comments: {query_no_comments}")
    tables = get_tables(sql_str)
    return tables


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
    main_select = get_main_select(query_no_comments)
    print(main_select)
    print_buffer()
    columns = process_select(main_select)
    for column in columns:
        print(column)

    # get tables
    tables = build_table_list(query_no_comments)
#    rest_of_query = get_rest_after_main_select(query_no_comments)
#    print(rest_of_query)
#    print_buffer()
#    print(split_unions(rest_of_query))

    print_buffer()
    print(process_other_join_types('table1 LEFT JOIN table2 ON table1.column1 = table2.column2'))
    print_buffer()
    print(process_other_join_types('\n table1 LEFT OUTER JOIN TABLE2 ON TABLE1.COLUMN1 = TABLE2.COLUMN2 \nLEFT OUTER JOIN table3 ON table2.column1 = table3.column2'))
    print_buffer()
    table_entries = ['TABLE1', 'TABLE2']
    print(create_table_entries(table_entries))
    table_entries = ['TABLE1 AS T1', 'TABLE2']
    print(create_table_entries(table_entries))

    print_buffer()
    sql_str = 'SELECT * FROM table1 AS t1, table2 AS t2 WHERE t1.column1 = t2.column2;'
    print(build_table_list(sql_str))

    print_buffer()
    sql_str = "(SELECT column1 FROM table1, table2, table3) AS table4"
    print(f"process_subquery results: {process_subquery(sql_str)}")

    print_buffer()
    sql_str = "(SELECT column2 FROM table2 AS t2, table3 AS t3, table4 AS t4 WHERE table2.column2 = table3.column2 AND table3.column2 = table4.column3) AS table5"
    print(f"process_subquery results: {process_subquery(sql_str)}")

    print_buffer()
    sql_str = "(SELECT column1 FROM table1 UNION SELECT column2 FROM table2 ) AS table5"
    print(f"process_subquery results: {process_subquery(sql_str)}")

    print_buffer()
    sql_str = "(SELECT column1 FROM table1 AS t1 UNION SELECT column2 FROM table2 AS t2 UNION SELECT column2 FROM table3 AS t3) AS table5"
    print(f"process_subquery results: {process_subquery(sql_str)}")

    print_buffer()
    table_list = build_table_list(query1)
    print_buffer()
    print(f"table_list: {table_list}")