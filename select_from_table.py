#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

"""

from __future__ import print_function, division
import rolex
import random
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


class Employee(Base):
    __tablename__ = "employee"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    salary = Column(Integer)
    department_id = Column(Integer, ForeignKey("department.id"))


class Department(Base):
    __tablename__ = "department"

    id = Column(Integer, primary_key=True)
    name = Column(String)


Base.metadata.create_all(engine)

# Put some example data
Session = sessionmaker(bind=engine)
ses = Session()

n_department = 3
department_list = [
    Department(id=id, name=rnd.simple_faker.fake.job())
    for id in range(1, 1 + n_department)
]

n_employee = 10
employee_list = [
    Employee(
        id=id,
        name=rnd.name(),
        salary=random.randint(50000, 80000),
        department_id=random.randint(1, n_department),
    )
    for id in range(1, 1 + n_employee)
]

ses.add_all(department_list)
ses.add_all(employee_list)
ses.commit()


def solution1():
    """
    Not perfect solution. If seat id ends with odds number, then it doesn't work.
    """
    query = ses.query(Employee)
    pprint(query, engine)
    query = ses.query(Department)
    pprint(query, engine)

    query = text("""
    SELECT employee.department_id, COUNT(*)
    FROM employee
    GROUP BY employee.department_id
    """)
    query = text("""
    SELECT 
        D.id as department_id, 
        E1.name as employee_name, 
        E1.salary as salary1,
        E2.salary as salary2
    FROM 
        department D, 
        employee E1, 
        employee E2
    """)
    """
    WHERE 
        D.id = E1.department_id 
        AND E1.department_id = E2.department_id 
        AND E1.salary <= E2.salary
    group by D.id, E1.name having count(distinct E2.salary) <= 3
    order by D.name, E1.salary desc
    """
    pprint(query, engine)

solution1()
