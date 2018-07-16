#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
- DISTINCT function
- SELECT in WHERE clause
"""

from __future__ import print_function, division
import random
import sqlalchemy
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import func, text

from util import pprint


engine = sqlalchemy.create_engine("sqlite:///:memory:")

Base = declarative_base()


class Number(Base):
    __tablename__ = "number"

    id = Column(Integer, primary_key=True)
    value = Column(Integer)

Base.metadata.create_all(engine)

# Put some example data
Session = sessionmaker(bind=engine)
ses = Session()

n_id = 100
n_value = 5

data = list()
for id in range(1, n_id + 1):
    number = Number(
        id=id,
        value=random.randint(1, n_value),
    )
    data.append(number)

ses.add_all(data)
ses.commit()

def problem1():
    """
    find all numbers that appear at least three times consecutively.
    """
    query = ses.query(Number.value).order_by(Number.id)
    number_list = [number.value for number in query]

    init = 0
    count = 0
    n_consecutive = 3
    target_numbers = list()
    for i in number_list:
        if i != init:
            init = i
            count = 1
        else:
            count += 1
        if count == n_consecutive:
            target_numbers.append(init)

    # print(number_list)
    # print(target_numbers)

    # pprint(query, engine)

    query = ses.query(Number.value, func.lag(Number.value, 1))
    pprint(query, engine)


problem1()
