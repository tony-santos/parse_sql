import pytest
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import parse4


def test_process_subquery_simple():
    sql_str = "(SELECT column1 FROM table1) AS table2"
    assert(parse4.process_subquery(sql_str) == [('TABLE1', 'TABLE1')])

def test_process_subquery_tables_with_alias():
    sql_str = "(SELECT column2 FROM table2 AS t2, table3 AS t3, table4 AS t4 WHERE table2.column2 = table3.column2 AND table3.column2 = table4.column3) AS table5"
    assert(parse4.process_subquery(sql_str) == [('TABLE2', 'T2'), ('TABLE3', 'T3'), ('TABLE4', 'T4')])

def test_process_subquery_tables_union_simple():
    sql_str = "(SELECT column1 FROM table1 UNION SELECT column2 FROM table2 ) AS table5"
    assert(parse4.process_subquery(sql_str) == [('TABLE1', 'TABLE1'), ('TABLE2', 'TABLE2')])

def test_process_subquery_tables_union_with_alias():
    sql_str = "(SELECT column1 FROM table1 AS t1 UNION SELECT column2 FROM table2 AS t2 UNION SELECT column2 FROM table3 AS t3) AS table5"
    assert(parse4.process_subquery(sql_str) == [('TABLE1', 'T1'), ('TABLE2', 'T2'), ('TABLE3', 'T3')])
