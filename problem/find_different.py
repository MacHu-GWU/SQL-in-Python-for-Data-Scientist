#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Handling Duplicate
"""

from __future__ import print_function, division
import random
from sfm import rnd

import sqlalchemy
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from learn_sql_in_python.db import engine
from learn_sql_in_python.util import pprint, preview

Base = declarative_base()


class List1(Base):
    __tablename__ = "list1"

    id = Column(Integer, primary_key=True)
    value = Column(Integer)


class List2(Base):
    __tablename__ = "list2"

    id = Column(Integer, primary_key=True)
    value = Column(Integer)


Base.metadata.create_all(engine)

# Put some example data
Session = sessionmaker(bind=engine)
ses = Session()

n = 10
l1 = [random.randint(1, 5) for _ in range(n)]
l2 = random.sample(l1, n - 1)

ses.add_all([List1(id=id+1, value=v) for id, v in enumerate(l1)])
ses.add_all([List2(id=id+1, value=v) for id, v in enumerate(l2)])

ses.commit()


preview(List1, engine)
preview(List2, engine)

def solution1():
    query = sqlalchemy.text("""
    SELECT list1.value
    FROM list1
    EXCEPT
    SELECT list2.value
    FROM list2
    """)
    pprint(query, engine)

solution1()
