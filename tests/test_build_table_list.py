import pytest
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import extract_tables

def test_build_table_list_2_tables_with_alias():
    sql_str = 'SELECT * FROM table1 AS t1, table2 AS t2 WHERE t1.column1 = t2.column2;'
    assert(extract_tables.build_table_list(sql_str)) == [('TABLE1', 'T1'), ('TABLE2', 'T2')]

