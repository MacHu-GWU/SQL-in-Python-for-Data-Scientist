#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy.dialects import sqlite
from sqlalchemy.orm import Query
from sqlalchemy_mate.pkg.prettytable import from_db_cursor


def pprint(query_or_sql, engine):
    if isinstance(query_or_sql, Query):
        sql = query_or_sql.statement.compile(dialect=sqlite.dialect())
    else:
        sql = query_or_sql
    result_proxy = engine.execute(sql)
    p_table = from_db_cursor(result_proxy.cursor)
    print(p_table)
