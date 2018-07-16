#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Write a SQL query to rank scores.
If there is a tie between two scores, both should have the same ranking.
Note that after a tie, the next ranking number should be the next consecutive integer value.
In other words, there should be no "holes" between ranks.

::

    +----+-------+
    | Id | Score |
    +----+-------+
    | 1  | 3.50  |
    | 2  | 3.65  |
    | 3  | 4.00  |
    | 4  | 3.85  |
    | 5  | 4.00  |
    | 6  | 3.65  |
    +----+-------+

For example, given the above ``Scores`` table, your query should generate
the following report (order by highest score)::

    +-------+------+
    | Score | Rank |
    +-------+------+
    | 4.00  | 1    |
    | 4.00  | 1    |
    | 3.85  | 2    |
    | 3.65  | 3    |
    | 3.65  | 3    |
    | 3.50  | 4    |
    +-------+------+
"""

from __future__ import print_function, division
import random

import sqlalchemy
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from learn_sql_in_python.db import engine
from learn_sql_in_python.util import pprint, preview

Base = declarative_base()


class Score(Base):
    __tablename__ = "score"

    id = Column(Integer, primary_key=True)
    value = Column(Integer)


Base.metadata.create_all(engine)

# Put some example data
Session = sessionmaker(bind=engine)
ses = Session()

n = 100
data = [Score(id=id, value=random.randint(60, 100)) for id in range(1, 1 + n)]
ses.add_all(data)

ses.commit()

preview(Score, engine, limit=10)


def solution1():
    query = sqlalchemy.text("""
    SELECT score.id, score.value, score2.rank
    FROM score
    JOIN (
        SELECT score1.unique_value, ROW_NUMBER() OVER (ORDER BY score1.unique_value DESC) as rank
        FROM (
            SELECT DISTINCT(score.value) as unique_value
            FROM score
        ) as score1
    ) as score2
    ON score.value = score2.unique_value
    ORDER BY score.value DESC, score.id DESC
    """)
    pprint(query, engine)


solution1()

ses.close()
