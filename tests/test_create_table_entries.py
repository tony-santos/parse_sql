import pytest
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import parse4

def test_create_table_entries_no_alias():
    table_list = ['TABLE1', 'TABLE2']
    assert(parse4.create_table_entries(table_list)) == [('TABLE1', 'TABLE1'),('TABLE2', 'TABLE2')]

def test_create_table_entries_first_with_alias():
    table_list = ['TABLE1 AS T1', 'TABLE2']
    assert(parse4.create_table_entries(table_list)) == [('TABLE1', 'T1'),('TABLE2', 'TABLE2')]

def test_create_table_entries_both_with_alias():
    table_list = ['TABLE1 AS T1', 'TABLE2 AS T2']
    assert(parse4.create_table_entries(table_list)) == [('TABLE1', 'T1'),('TABLE2', 'T2')]

