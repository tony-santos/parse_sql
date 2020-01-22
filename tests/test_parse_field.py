import pytest
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
print(f"{sys.path}")

import extract_utilities


def test_parse_field_base():
    field = "table.column AS column_alias"
    print(extract_utilities.parse_field(field))
    assert(extract_utilities.parse_field(field) == ('TABLE', 'COLUMN', 'COLUMN_ALIAS'))

def test_parse_field_no_column_alias():
    field = "table.column"
    print(extract_utilities.parse_field(field))
    assert(extract_utilities.parse_field(field) == ('TABLE', 'COLUMN', 'COLUMN'))

def test_parse_field_no_table_alias():
    field = "column AS column_alias"
    print(extract_utilities.parse_field(field))
    assert(extract_utilities.parse_field(field) == ('', 'COLUMN', 'COLUMN_ALIAS'))

def test_parse_field_no_table_alias_no_column_alias():
    field = "column"
    print(extract_utilities.parse_field(field))
    assert(extract_utilities.parse_field(field) == ('', 'COLUMN', 'COLUMN'))
