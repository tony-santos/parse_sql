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

    from_clause = sql_str[sql_str.find('FROM')+5:]
    print(from_clause)
    if from_clause.find('WHERE') > 0:
        from_clause = from_clause[:from_clause.find('WHERE')]
    
    # left off here. need to figure out what kind of table list has been passed. if simple list separated by commas or combination of different JOIN keywords
    if from_clause.find('JOIN') == -1: # should be simple list of tables separated by comma
        table_lines = from_clause.split(',')
    else: # since the word JOIN was found, assume it  is a series of LEFT, RIGHT, INNER, or OUTER JOINS
        table_lines = process_other_join_types(from_clause)

    for table_line in table_lines:
        print(table_line)
    table_lines = [table_line.rstrip().lstrip() for table_line in table_lines]

    return table_lines

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
        print(table_line_raw)
        print(table_line_raw.find(' LEFT'))

        # look for LEFT & LEFT OUTER
        if table_line_raw.find(' LEFT') > -1:
            print("found LEFT or LEFT OUTER JOIN")
            if table_line_raw.find(' LEFT') > 0: # there should be a table in front of LEFT XXX JOIN. 
                table_lines.append(table_line_raw[:table_line_raw.find(' LEFT')].rstrip()).upper() # get line up to, but not including LEFT. should also remove LEFT OUTER
        # look for LEFT & LEFT OUTER
        elif table_line_raw.find(' RIGHT') > -1:
            print("found RIGHT or RIGHT OUTER JOIN")
            if table_line_raw.find(' RIGHT') > 0: # there should be a table in front of RIGHT XXX JOIN. 
                table_lines.append(table_line_raw[:table_line_raw.find(' RIGHT')].rstrip()).upper() # get line up to, but not including LEFT. should also remove LEFT OUTER
    #        table_line2 = re.search('(LEFT JOIN (.+?) ON ', table_line2, re.DOTALL).group(1)
    # ON and join condition will be on next line. need to check separately at same level as check for LEFT
        if table_line_raw.find(' ON ') > -1:
            print("found JOIN CONDITION")
            if table_line_raw.find(' ON ') > 0: # there should be a table in front of ON. 
                table_lines.append(table_line_raw[:table_line_raw.find(' ON ')].rstrip()).upper() # get line up to, but not including LEFT. should also remove LEFT OUTER
            print(f"table_line_raw: {table_line_raw}")
    
    return table_lines


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
    query_no_comments = remove_comments(query1)
    #print(type(query_no_comments))
    #print(query_no_comments)

    #print(find_sub_queries(query_no_comments))
    main_select = get_main_select(query_no_comments)
    print(main_select)
    print_buffer()
    rest_of_query = get_rest_after_main_select(query_no_comments)
    print(rest_of_query)
    print_buffer()
    print(split_unions(rest_of_query))

    columns = process_select(main_select)
    for column in columns:
        print(column)

    print_buffer()
    print(process_other_join_types('\n LEFT JOIN TABLE2 ON TABLE1.COLUMN1 = TABLE2.COLUMN2'))

    print(process_other_join_types('table1 LEFT JOIN table2 ON table1.column1 = table2.column2'))