import pytest
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import extract_columns


def test_parse_field_base():
    field = "table.column AS column_alias"
    print(extract_columns.parse_field(field))
    assert(extract_columns.parse_field(field) == ('TABLE', 'COLUMN', 'COLUMN_ALIAS'))

def test_parse_field_no_column_alias():
    field = "table.column"
    print(extract_columns.parse_field(field))
    assert(extract_columns.parse_field(field) == ('TABLE', 'COLUMN', 'COLUMN'))

def test_parse_field_no_table_alias():
    field = "column AS column_alias"
    print(extract_columns.parse_field(field))
    assert(extract_columns.parse_field(field) == ('', 'COLUMN', 'COLUMN_ALIAS'))

def test_parse_field_no_table_alias_no_column_alias():
    field = "column"
    print(extract_columns.parse_field(field))
    assert(extract_columns.parse_field(field) == ('', 'COLUMN', 'COLUMN'))
