import pytest
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import parse4

def test_get_tables():
    sql_str = "SELECT * FROM table1, table2 WHERE table1.column1 = table2.column1;"
    assert(parse4.get_tables(sql_str)) == ['TABLE1', 'TABLE2']

def test_get_tables_mutiline():
    sql_str = """
    SELECT * 
    FROM table1, table2 
    WHERE table1.column1 = table2.column1;
    """
    assert(parse4.get_tables(sql_str)) == ['TABLE1', 'TABLE2']

def test_get_tables_tables_on_muti_lines():
    sql_str = """
    SELECT * 
    FROM table1, table2,
         table3
    WHERE table1.column1 = table2.column1;
    """
    assert(parse4.get_tables(sql_str)) == ['TABLE1', 'TABLE2', 'TABLE3']

def test_get_tables_single_table():
    sql_str = """
    SELECT * 
    FROM table1
    WHERE table1.column1 = table2.column1;
    """
    assert(parse4.get_tables(sql_str)) == ['TABLE1']

def test_get_tables_left_join():
    sql_str = """
    SELECT * 
    FROM table1
    LEFT JOIN table2 ON table1.column1 = table2.column2
    WHERE table1.column1 < 10;
    """
    assert(parse4.get_tables(sql_str)) == ['TABLE1', 'TABLE2']