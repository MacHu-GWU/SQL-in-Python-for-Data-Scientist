#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
The ``Employee`` table holds all employees.
Every employee has an Id, and there is also a column for the department Id.

::

    +----+-------+--------+--------------+
    | Id | Name  | Salary | DepartmentId |
    +----+-------+--------+--------------+
    | 1  | Joe   | 70000  | 1            |
    | 2  | Henry | 80000  | 2            |
    | 3  | Sam   | 60000  | 2            |
    | 4  | Max   | 90000  | 1            |
    | 5  | Janet | 69000  | 1            |
    | 6  | Randy | 85000  | 1            |
    +----+-------+--------+--------------+

The ``Department`` table holds all departments of the company.

::

    +----+----------+
    | Id | Name     |
    +----+----------+
    | 1  | IT       |
    | 2  | Sales    |
    +----+----------+

Write a SQL query to find employees who earn the top three salaries in each of the department.
For the above tables, your SQL query should return the following rows.

::

    +------------+----------+--------+
    | Department | Employee | Salary |
    +------------+----------+--------+
    | IT         | Max      | 90000  |
    | IT         | Randy    | 85000  |
    | IT         | Joe      | 70000  |
    | Sales      | Henry    | 80000  |
    | Sales      | Sam      | 60000  |
    +------------+----------+--------+
"""

from __future__ import print_function, division

import random
from sfm import rnd

import sqlalchemy
from sqlalchemy import Column
from sqlalchemy import String, Integer, ForeignKey
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
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

n_employee = 20
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

query = ses.query(Employee)
pprint(query, engine)

query = ses.query(Department)
pprint(query, engine)


def permutation1():
    query = text("""
    SELECT 
        E.id as e_id, 
        E.name as e_name, 
        D.id as d_id, 
        D.name as d_name
    FROM 
        employee as E, 
        department as D
    """)
    pprint(query, engine)

# permutation1()


def permutation2():
    query = text("""
    SELECT 
        E.id as e_id, 
        E.name as e_name, 
        D1.id as d_id1, 
        D1.name as d_name1,
        D2.id as d_id2, 
        D2.name as d_name2
    FROM 
        employee as E, 
        department as D1,
        department as D2
    """)
    pprint(query, engine)

# permutation2()

def solution1():
    """
    This solution use trick of permutation.
    """
    query = text("""
    SELECT
        D.name as department,
        E1.name as employee,
        E1.salary as salary
    FROM
        department D,
        employee E1,
        employee E2
    WHERE
        D.id = E1.department_id
        AND E1.department_id = E2.department_id
        AND E1.salary <= E2.salary
    GROUP BY 
        D.id, 
        E1.name
        HAVING COUNT(DISTINCT E2.salary) <= 3
    ORDER BY D.name, E1.salary DESC
    """)
    pprint(query, engine)

solution1()
