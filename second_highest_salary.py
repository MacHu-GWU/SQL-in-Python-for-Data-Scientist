#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
- DISTINCT function
- SELECT in WHERE clause
"""

from __future__ import print_function, division
import rolex
import random
from sfm import rnd

import sqlalchemy
from sqlalchemy import Column
from sqlalchemy import String, Integer, Date, ForeignKey
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy.ext.declarative import declarative_base
from util import pprint

engine = sqlalchemy.create_engine("sqlite:///:memory:")

Base = declarative_base()


class Salary(Base):
    __tablename__ = "salary"

    id = Column(Integer, primary_key=True)
    salary = Column(Integer)


Base.metadata.create_all(engine)

# Put some example data
Session = sessionmaker(bind=engine)
ses = Session()

start_date = "2017-01-01"
end_date = "2017-01-07"
n_records = 1000
salary_list = [Salary(id=i+1, salary=random.randint(50000, 100000)) for i in range(n_records)]
ses.add_all(salary_list)
ses.commit()


def example():
    """
    join Team.id column twice.
    """
    query = ses.query(Salary.salary.label("top_five_highest_salary")) \
        .order_by(Salary.salary.desc()).limit(5)
    pprint(query, engine)

    query = ses.query(Salary.salary.label("second_highest_salary")) \
        .order_by(Salary.salary.desc()) \
        .offset(1).limit(1)
    pprint(query, engine)


example()
