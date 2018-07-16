#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy.orm import Query, sessionmaker
from sqlalchemy_mate.pkg.prettytable import from_db_cursor


def convert_query_to_sql_statement(query):
    """

    :param query: :class:`sqlalchemy.orm.Query`

    :return: :class:`sqlalchemy.sql.selectable.Select`
    """
    context = query._compile_context()
    context.statement.use_labels = True
    return context.statement


def execute_query_return_result_proxy(query):
    """
    :param query: :class:`sqlalchemy.orm.Query`

    :return: :class:`sqlalchemy.engine.result.ResultProxy`
    """
    context = query._compile_context()
    context.statement.use_labels = True
    if query._autoflush and not query._populate_existing:
        query.session._autoflush()

    conn = query._get_bind_args(
        context,
        query._connection_from_session,
        close_with_result=True)

    return conn.execute(context.statement, query._params)


def pprint(query_or_sql, engine):
    if isinstance(query_or_sql, Query):
        query = query_or_sql
        result_proxy = execute_query_return_result_proxy(query)
    else:
        sql = query_or_sql
        result_proxy = engine.execute(sql)
    p_table = from_db_cursor(result_proxy.cursor)
    print(p_table)


def preview(obj, engine, limit=None):
    """
    Preview a table of an object.
    """
    Session = sessionmaker(bind=engine)
    ses = Session()
    query = ses.query(obj)
    if limit is not None:
        query = query.limit(limit)
    pprint(query, engine)
    ses.close()
