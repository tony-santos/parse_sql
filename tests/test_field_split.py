import pytest
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import extract_columns


def test_field_split():
    sql_str = " table_name1.column_name1 AS column_alias1, table_name2.column_name2 AS column_alias2"

    print(extract_columns.field_split(sql_str))
#    assert(extract_columns.field_split(sql_str) == [' table_name1.column_name1 AS column_alias1', ' table_name2.column_name2 AS column_alias2', ' table_name3.column_name3 AS column_alias3'])
    assert(extract_columns.field_split(sql_str) == ['table_name1.column_name1 AS column_alias1', 'table_name2.column_name2 AS column_alias2'])
    
def test_field_split_derivation():
    sql_str = 'table_name1.column_name1 AS column_alias1, "substring"(protocol_instance.md_compound_name::text, 1, 11) AS parent_id_metid, table_name2.column_name2 AS column_alias2 '

    print(extract_columns.field_split(sql_str))
#    assert(extract_columns.field_split(sql_str) == [' table_name1.column_name1 AS column_alias1', ' table_name2.column_name2 AS column_alias2', ' table_name3.column_name3 AS column_alias3'])
    assert(extract_columns.field_split(sql_str) == ['table_name1.column_name1 AS column_alias1', ' "substring"(protocol_instance.md_compound_name::text, 1, 11) AS parent_id_metid', 'table_name2.column_name2 AS column_alias2'])
def test_field_split_multiline():
    sql_str = """table_name1.column_name1 AS column_alias1,
    table_name2.column_name2 AS column_alias2,
    table_name3.column_name3 AS column_alias3"""

    print(extract_columns.field_split(sql_str))
    # not clear why only the 2nd line has to have the \n with 4 trailing spaces while 3rd line gets properly stripped
    assert(extract_columns.field_split(sql_str) == ['table_name1.column_name1 AS column_alias1', '\n    table_name2.column_name2 AS column_alias2', 'table_name3.column_name3 AS column_alias3'])
    