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
    logger.info(f"entering get_select_position: start_pos = {start_pos}   sql_str: {sql_str}")
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
            logger.info(f"before SELECT: {sql_str[:select_position]}")

    return select_position

def get_matching_from_position(sql_str, start_pos=0):
    logger.info(f"entering get_matching_from_position: start_pos: {start_pos}")
    # make copy of sql_str
    sql_str = sql_str.upper()[start_pos:]
    select_level = 1
    matching_from_position = -1
    for index, token in enumerate(sql_str.split()):
        logger.info(f"index: {index},      token: {token}")
        if token == 'SELECT':
            select_level = select_level + 1
            logger.info(f"another select found. select_level: {select_level}")
            select_position = sql_str.find('SELECT')
        elif token== 'FROM':
            logger.info(f"FROM found in token: {index}")
            from_position = sql_str.find('FROM')
            select_level = select_level - 1
            logger.info(f"FROM found. select_level: {select_level}")

            if select_level == 0:
                matching_from_position = start_pos + from_position
                break
    logger.info(f"select_level: {select_level}")
    logger.info(f"returning from get_matching_from_position.      matching_from_position: {matching_from_position}")
    return matching_from_position

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
