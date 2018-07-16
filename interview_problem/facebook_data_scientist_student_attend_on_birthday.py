#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
An attendance log for every student in a school district

attendance_events : date | student_id | attendance

A summary table with demographics for each student in the district

all_students : student_id | school_id | grade_level | date_of_birth | hometown

Using this data, you could answer questions like the following:

- What percent of students attend school on their birthday?
- Which grade level had the largest drop in attendance between yesterday and today?
"""

from __future__ import print_function, division
import random
import rolex
from sfm import rnd
from datetime import date

import sqlalchemy
from sqlalchemy import Column
from sqlalchemy import String, Integer, Date, Boolean, ForeignKey
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import func

from learn_sql_in_python.db import sqlite_in_memory_engine as engine
# from learn_sql_in_python.db import postgres_engine as engine
from learn_sql_in_python.util import pprint, preview

Base = declarative_base()


class Student(Base):
    __tablename__ = "student"

    student_id = Column(String, primary_key=True)
    school_id = Column(String)
    grade_level = Column(Integer)
    dob = Column(Date)
    hometown = Column(String)


class Attendance(Base):
    __tablename__ = "attendance"

    date = Column(Date, primary_key=True)
    student_id = Column(
        String,
        ForeignKey("student.student_id"),
        primary_key=True,
    )
    attendance = Column(Boolean)


Base.metadata.create_all(engine)

# Put some example data
Session = sessionmaker(bind=engine)
ses = Session()

n_student = 100
n_school = 1
student_id_list = ["stu_%s" % str(i).zfill(6) for i in range(1, 1 + n_student)]
school_id_list = ["sch_%s" % str(i).zfill(6) for i in range(1, 1 + n_school)]

student_list = list()
for student_id in student_id_list:
    student = Student(
        student_id=student_id,
        school_id=random.choice(school_id_list),
        grade_level=random.randint(1, 6),
        dob=rolex.rnd_date("2000-01-01", "2000-01-31"),
        hometown=rnd.simple_faker.fake.city(),
    )
    student_list.append(student)

attendance_list = list()
attendance_pool = [True, False, False]
for att_date in rolex.time_series("2009-01-01", "2009-01-31"):
    for student_id in student_id_list:
        attendance = Attendance(
            date=att_date,
            student_id=student_id,
            attendance=random.choice(attendance_pool),
        )
        attendance_list.append(attendance)

ses.add_all(student_list)
ses.add_all(attendance_list)
ses.commit()

preview(Student, engine, limit=10)
preview(Attendance, engine, limit=10)


def q1_solution_in_sql():
    """
    Q:  What percent of students attend school on their birthday?

    - in sqlite, STRFTIME function is: ``to_char(format, datetime)``
    - in postgresql, STRFTIME function is: ``to_char(datetime, format)``
    """
    sqlite_sql = sqlalchemy.text("""
    SELECT 
        COUNT(DISTINCT(student.student_id)) as n_student_attend_school_on_birthday
    FROM attendance
    JOIN student
    ON attendance.student_id = student.student_id
    WHERE 
        STRFTIME('%m-%d', attendance.date) = STRFTIME('%m-%d', student.dob)
        AND attendance.attendance = 1
    """)
    pprint(sqlite_sql, engine)

    postgres_sql = sqlalchemy.text("""
    SELECT 
        COUNT(DISTINCT(student.student_id)) as n_student_attend_school_on_birthday
    FROM attendance
    JOIN student
    ON attendance.student_id = student.student_id
    WHERE 
        to_char(attendance.date, 'MM-DD') = to_char(student.dob, 'MM-DD')
        AND attendance.attendance = True
    """)
    # pprint(postgres_sql, engine)


# q1_solution_in_sql()


def q1_solution_in_query():
    """
    Q:  What percent of students attend school on their birthday?

    Construct the sql from ORM.
    """
    query = ses.query(
        # Attendance.student_id,
        # Attendance.date,
        # Student.dob,
        func.count(
            func.distinct(Attendance.student_id)
        ).label("n_student_attend_school_on_birthday")
    ) \
        .select_from(Attendance).join(Student, Attendance.student_id == Student.student_id) \
        .filter(func.strftime('%m-%d', Attendance.date) == func.strftime('%m-%d', Student.dob)) \
        .filter(Attendance.attendance == True)
    pprint(query, engine)


# q1_solution_in_query()


def q2_solution_in_sql():
    sqlite_sql = sqlalchemy.text("""
    """)
    pprint(sqlite_sql, engine)


q2_solution_in_sql()


def q2_solution():
    """
    Q: Which grade level had the largest drop in attendance between yesterday and today?
    """
    yesterday = date(2009, 1, 20)
    today = date(2009, 1, 21)

    query = ses.query(func.count(Student.student_id).label("n_student"), Student.grade_level) \
        .group_by(Student.grade_level)
    pprint(query, engine)

    def solution1():
        """
        make two query.
        """
        base_query = ses.query(func.count(Attendance.student_id).label("n_attend"), Student.grade_level) \
            .select_from(Attendance).join(Student, Attendance.student_id == Student.student_id) \
            .filter(Attendance.attendance == True)

        yesterday_query = base_query \
            .filter(Attendance.date == today) \
            .group_by(Student.grade_level)

        today_query = base_query \
            .filter(Attendance.date == yesterday) \
            .group_by(Student.grade_level)

        pprint(yesterday_query, engine)
        pprint(today_query, engine)

    # solution1()

    def solution2():
        query = ses.query(
            func.count(Attendance.student_id).label("n_attend"),
            Attendance.date,
            Student.grade_level,
        ) \
            .select_from(Attendance).join(Student, Attendance.student_id == Student.student_id) \
            .filter(Attendance.date >= yesterday) \
            .filter(Attendance.date <= today) \
            .group_by(Attendance.date, Student.grade_level)
        pprint(query, engine)

    solution2()


# q2_solution()

ses.close()
