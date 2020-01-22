import pytest
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import extract_tables_columns

def test_process_other_join_types_left_join_2_tables():
    table_line = '\n table1 LEFT JOIN TABLE2 ON TABLE1.COLUMN1 = TABLE2.COLUMN2'
    assert(extract_tables.process_other_join_types(table_line)) == ['TABLE1','TABLE2']

def test_process_other_join_types_left_outer_join_2_tables():
    table_line = '\n table1 LEFT OUTER JOIN TABLE2 ON TABLE1.COLUMN1 = TABLE2.COLUMN2'
    assert(extract_tables.process_other_join_types(table_line)) == ['TABLE1','TABLE2']

def test_process_other_join_types_left_outer_join_3_tables():
    table_line = '\n table1 LEFT OUTER JOIN TABLE2 ON TABLE1.COLUMN1 = TABLE2.COLUMN2 \nLEFT OUTER JOIN table3 ON table2.column1 = table3.column2'
    assert(extract_tables.process_other_join_types(table_line)) == ['TABLE1','TABLE2', 'TABLE3']

def test_process_other_join_types_right_join_2_tables():
    table_line = '\n table1 RIGHT JOIN TABLE2 ON TABLE1.COLUMN1 = TABLE2.COLUMN2'
    assert(extract_tables.process_other_join_types(table_line)) == ['TABLE1','TABLE2']

def test_process_other_join_types_right_outer_join_2_tables():
    table_line = '\n table1 RIGHT OUTER JOIN TABLE2 ON TABLE1.COLUMN1 = TABLE2.COLUMN2'
    assert(extract_tables.process_other_join_types(table_line)) == ['TABLE1','TABLE2']

def test_process_other_join_types_left_join_2_tables_both_with_alias():
    table_line = '\n table1 AS t1 LEFT JOIN TABLE2 AS t2 ON t1.COLUMN1 = t2.COLUMN2'
    assert(extract_tables.process_other_join_types(table_line)) == ['TABLE1 AS T1','TABLE2 AS T2']

def test_process_other_join_types_left_join_2_tables_first_with_alias():
    table_line = '\n table1 AS t1 LEFT JOIN TABLE2 ON t1.COLUMN1 = table2.COLUMN2'
    assert(extract_tables.process_other_join_types(table_line)) == ['TABLE1 AS T1','TABLE2']

def test_process_other_join_types_left_join_2_tables_second_with_alias():
    table_line = '\n table1 LEFT JOIN TABLE2 AS t2 ON table1.COLUMN1 = t2.COLUMN2'
    assert(extract_tables.process_other_join_types(table_line)) == ['TABLE1','TABLE2 AS T2']

def test_process_other_join_types_left_outer_join_3_tables_all_with_alias():
    table_line = '\n table1 AS T1 LEFT OUTER JOIN TABLE2 AS t2 ON T1.COLUMN1 = T2.COLUMN2 \nLEFT OUTER JOIN table3 AS t3 ON t2.column1 = t3.column2'
    assert(extract_tables.process_other_join_types(table_line)) == ['TABLE1 AS T1','TABLE2 AS T2', 'TABLE3 AS T3']

