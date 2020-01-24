#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2009-2018 the sqlparse authors and contributors
# <see AUTHORS file>
#
# This example is part of python-sqlparse and is released under
# the BSD License: https://opensource.org/licenses/BSD-3-Clause
#
# This example illustrates how to extract table names from nested
# SELECT statements.
#
# See:
# https://groups.google.com/forum/#!forum/sqlparse/browse_thread/thread/b0bd9a022e9d4895

import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML

import sys
from datetime import datetime

from loguru import logger

datetime_format = "%Y%m%d-%H%M"
logfile_datetime = datetime.now().strftime(datetime_format)

# logger.add(f"extract_tables_columns-{logfile_datetime}.log", backtrace=True, diagnose=True)
logger.add(f"{__file__.split('.')[0]        }-{logfile_datetime}.log", backtrace=True, diagnose=True)

import extract_utilities as eu 


def is_subselect(parsed):
    # I think this needs to be made to see "( SELECT" as well as SELECT)
    if not parsed.is_group:
        return False
    for item in parsed.tokens:
        logger.info(f"item: +++{item}+++    item.ttype: +++{item.ttype}+++")
        logger.info(f"is_subselect: {item.ttype is DML and item.value.upper() == 'SELECT'}")
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False


def extract_from_part(parsed):
    from_seen = False
    for item in parsed.tokens:
        logger.info(f"item.value: +++{item.value}+++       -- item.ttype:{item.ttype}")
        if from_seen:
            if is_subselect(item):
                for x in extract_from_part(item):
                    logger.info(f"yielding x: {x}")
                    yield x
            elif item.ttype is Keyword:
                return
            else:
                logger.info(f"yielding item: {item}")
                yield item
        elif item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True
            logger.info(f"from_seen: {from_seen}")


def extract_table_identifiers(token_stream):
    for item in token_stream:
        logger.info(f"item: {item}")
        if isinstance(item, IdentifierList):
            logger.info(f"isIntance(item, IdentifierList: {IdentifierList}")
            for identifier in item.get_identifiers():
                yield identifier.get_name()
        elif isinstance(item, Identifier):
            logger.info(f"isIntance(item, Identifier: {Identifier}")
            yield item.get_name()
        # It's a bug to check for Keyword here, but in the example
        # above some tables names are identified as keywords...
        elif item.ttype is Keyword:
            logger.info(f"item.ttype is Keyword: {item.value}")
            yield item.value


def extract_tables(sql):
    stream = extract_from_part(sqlparse.parse(sql)[0])
    logger.info(f"stream: {stream}")
    return list(extract_table_identifiers(stream))


if __name__ == '__main__':
    with open(sys.argv[1], 'r') as myfile:
        query1 = myfile.read()
    
    select_count = 0
    select_froms = []
    columns = []
    logger.info(f"query read from file:")
    logger.info(f"{query1}")

    sql_str = eu.reformat_query(query1)
    logger.info(f"sql_str: {sql_str}")

    parsed = sqlparse.parse(sql_str)[0] # only parse the first sql statement 

    logger.info(f"sqlparse.parse(sql_str): +++{sqlparse.parse(sql_str)}+++")
    logger.info(f"type(parsed): {type(parsed)}")

    tables = ', '.join(extract_tables(sql_str))
    print('Tables: {0}'.format(tables))
