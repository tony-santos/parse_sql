import pytest
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import extract_columns


def test_get_main_select():
    sql_str = """SELECT  table_name1.column_name1 AS column_alias1,
            table_name2.column_name2 AS column_alias2,
            table_name3.column_name3 AS column_alias3 
            FROM table1
            LEFT JOIN table2 ON table1.column1 = table2.column1
            LEFT JOIN table3 ON table2.column2 = table3.column2
            """

    print(extract_columns.field_split(sql_str))
    assert(extract_columns.get_main_select(sql_str) == """SELECT  table_name1.column_name1 AS column_alias1,
            table_name2.column_name2 AS column_alias2,
            table_name3.column_name3 AS column_alias3 
            FROM""")

