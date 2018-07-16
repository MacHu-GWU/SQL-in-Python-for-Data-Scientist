#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Mary is a teacher in a middle school and she has a table seat storing students' names
and their corresponding seat ids.

The column id is continuous increment.
Mary wants to change seats for the adjacent students.
Can you write a SQL query to output the result for Mary?

::

    +---------+---------+
    |    id   | student |
    +---------+---------+
    |    1    | Abbot   |
    |    2    | Doris   |
    |    3    | Emerson |
    |    4    | Green   |
    |    5    | Jeames  |
    +---------+---------+

For the sample input, the output is::

    +---------+---------+
    |    id   | student |
    +---------+---------+
    |    1    | Doris   |
    |    2    | Abbot   |
    |    3    | Green   |
    |    4    | Emerson |
    |    5    | Jeames  |
    +---------+---------+
"""

from __future__ import print_function, division
import rolex
from sfm import rnd

import sqlalchemy
from sqlalchemy import Column
from sqlalchemy import String, Integer, Date, ForeignKey
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy.ext.declarative import declarative_base
from util import pprint

engine = sqlalchemy.create_engine("sqlite:///:memory:")

Base = declarative_base()


class Seating(Base):
    __tablename__ = "seating"

    id = Column(Integer, primary_key=True)
    student = Column(String)


Base.metadata.create_all(engine)

# Put some example data
Session = sessionmaker(bind=engine)
ses = Session()

n_seat = 6
seating_list = [
    Seating(id=id, student=rnd.name())
    for id in range(1, 1 + n_seat)
]
ses.add_all(seating_list)
ses.commit()


def solution1():
    """
    Not perfect solution. If seat id ends with odds number, then it doesn't work.
    """
    query = ses.query(Seating)
    pprint(query, engine)

    query = text("""
    SELECT seating.id - 1 as id, seating.student
    FROM seating
    WHERE seating.id % 2 = 0
    UNION
    SELECT seating.id + 1 as id, seating.student
    FROM seating
    WHERE seating.id % 2 = 1
    ORDER BY id
    """)
    pprint(query, engine)

solution1()
