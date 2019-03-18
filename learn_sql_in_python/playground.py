# -*- coding: utf-8 -*-

from __future__ import print_function
import sqlalchemy
from sqlalchemy_mate.pt import from_resultproxy
from .db import reset_db


class Playground(object):
    def __init__(self, engine):
        self.engine = engine
        reset_db(self.engine)

    def add_table(self, metadata, table, data):
        metadata.create_all(self.engine)
        self.engine.execute(table.insert(), data)

    def print_result(self, sql):
        stmt = sqlalchemy.text(sql)
        result_proxy = self.engine.execute(stmt)
        pretty_table = from_resultproxy(result_proxy)
        print(pretty_table)
