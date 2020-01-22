import pytest
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import extract_utilities


def test_remove_comments_end_of_line():
    sql_str = "SELECT * FROM table1 -- this comment should be removed"

    assert(extract_utilities.remove_comments(sql_str) == "SELECT * FROM table1 ")

def test_remove_comments_beginning_of_line():
    sql_str = "/* remove this comment */ SELECT * FROM table1"

    assert(extract_utilities.remove_comments(sql_str) == " SELECT * FROM table1")

def test_remove_comments_beginning_of_line_multiline():
    sql_str = """/* remove this comment */
    SELECT * FROM table1"""

    assert(extract_utilities.remove_comments(sql_str) == "     SELECT * FROM table1")

def test_remove_comments_end_of_line_concatenation():
    # this test added because lines with concatenation were getting truncated due to presence of '|'
    sql_str = """/* remove this comment */
    SELECT column1||' some text to add' FROM table1 """

    assert(extract_utilities.remove_comments(sql_str) == "     SELECT column1||' some text to add' FROM table1 ")
