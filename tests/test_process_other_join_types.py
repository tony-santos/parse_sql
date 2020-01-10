import pytest
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import parse4

def test_process_other_join_types():
    table_line = '\n LEFT JOIN TABLE2 ON TABLE1.COLUMN1 = TABLE2.COLUMN2'
    assert(parse4.process_other_join_types(table_line)) == ['TABLE2']

