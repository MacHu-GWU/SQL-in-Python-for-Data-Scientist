#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Write a SQL query to get the nth highest salary from the ``Employee``table.

::

    +----+--------+
    | Id | Salary |
    +----+--------+
    | 1  | 100    |
    | 2  | 200    |
    | 3  | 300    |
    +----+--------+

For example, given the above Employee table, the nth highest salary where n = 2 is ``200``.
If there is no nth highest salary, then the query should return ``null``.

::

    +------------------------+
    | getNthHighestSalary(2) |
    +------------------------+
    | 200                    |
    +------------------------+
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


class Employee(Base):
    __tablename__ = "employee"

    id = Column(Integer, primary_key=True)
    salary = Column(Integer)


Base.metadata.create_all(engine)

# Put some example data
Session = sessionmaker(bind=engine)
ses = Session()

n = 10
data = [
    Employee(id=id, salary=random.randint(60000, 100000))
    for id in range(1, 1 + n)
]
ses.add_all(data)

ses.commit()

preview(Employee, engine, limit=10)


def solution1():
    query = sqlalchemy.text("""
        SELECT employee.salary
        FROM employee
        ORDER BY employee.salary DESC
        LIMIT 5
        """)
    pprint(query, engine)

    # The third highest
    query = sqlalchemy.text("""
    SELECT employee.salary
    FROM employee
    ORDER BY employee.salary DESC
    OFFSET 2
    LIMIT 1
    """)
    pprint(query, engine)


solution1()

ses.close()
