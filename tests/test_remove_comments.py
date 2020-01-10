import pytest
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import parse4


def test_remove_comments_end_of_line():
    sql_str = "SELECT * FROM table1 -- this comment should be removed"

    assert(parse4.remove_comments(sql_str) == "SELECT * FROM table1 ")

def test_remove_comments_beginning_of_line():
    sql_str = "/* remove this comment */ SELECT * FROM table1"

    assert(parse4.remove_comments(sql_str) == " SELECT * FROM table1")

def test_remove_comments_beginning_of_line():
    sql_str = """/* remove this comment */
    SELECT * FROM table1"""

    assert(parse4.remove_comments(sql_str) == "     SELECT * FROM table1")
