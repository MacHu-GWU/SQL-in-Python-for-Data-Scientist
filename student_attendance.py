#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
- strftime function
- join
- group_by
"""

from __future__ import print_function, division
import random
import rolex
from sfm import rnd

import sqlalchemy
from sqlalchemy import Column
from sqlalchemy import String, Integer, Date, Boolean, ForeignKey
from sqlalchemy.orm import sessionmaker, Query
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_mate.pkg.prettytable import from_db_cursor
from sqlalchemy import func, text
from sqlalchemy.dialects import sqlite
from datetime import date

engine = sqlalchemy.create_engine("sqlite:///:memory:")

Base = declarative_base()


class Attendance(Base):
    __tablename__ = "attendance"

    date = Column(Date, primary_key=True)
    student_id = Column(
        String,
        ForeignKey("student.student_id"),
        primary_key=True,
    )
    attendance = Column(Boolean)


class Student(Base):
    __tablename__ = "student"

    student_id = Column(String, primary_key=True)
    school_id = Column(String)
    grade_level = Column(Integer)
    dob = Column(Date)
    hometown = Column(String)


Base.metadata.create_all(engine)

# Put some example data
Session = sessionmaker(bind=engine)
ses = Session()

n_student = 100
n_school = 1
student_id_list = ["stu_%s" % str(i).zfill(6) for i in range(n_student)]
school_id_list = ["sch_%s" % str(i).zfill(6) for i in range(n_school)]

student_list = list()
for student_id in student_id_list:
    student = Student(
        student_id=student_id,
        school_id=random.choice(school_id_list),
        grade_level=random.randint(1, 6),
        dob=rolex.rnd_date("2000-01-01", "2000-01-31"),
        hometown=rnd.rand_str(32),
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


def pprint(query_or_sql, engine):
    if isinstance(query_or_sql, Query):
        sql = query_or_sql.statement.compile(dialect=sqlite.dialect())
    else:
        sql = query_or_sql
    result_proxy = engine.execute(sql)
    p_table = from_db_cursor(result_proxy.cursor)
    print(p_table)


def example1():
    """
    func.strftime practice:

    seleft dob that month = day (in this case, dob = Jan 1th)
    """
    result = ses.query(Student.student_id, Student.dob) \
        .filter(func.strftime("%m", Student.dob) == func.strftime("%d", Student.dob)) \
        .all()

    for row in result:
        print(row)


# example1()


def example2():
    """
    What percent of students attend school on their birthday?
    """
    # This query get all attendance event that, attendance.date is birthday
    query = ses.query(Attendance.student_id, Attendance.date, Student.dob) \
        .select_from(Attendance).join(Student, Attendance.student_id == Student.student_id) \
        .filter(func.strftime('%m-%d', Attendance.date) == func.strftime('%m-%d', Student.dob)) \
        .filter(Attendance.attendance == True)
    # pprint(query, engine)

    # Final query
    n_student = ses.query(Student.student_id).count()
    query = ses.query(func.distinct(Attendance.student_id)) \
        .select_from(Attendance).join(Student, Attendance.student_id == Student.student_id) \
        .filter(func.strftime('%m-%d', Attendance.date) == func.strftime('%m-%d', Student.dob)) \
        .filter(Attendance.attendance == True)
    n_student_attend_school_on_birthday = query.count()
    print(n_student_attend_school_on_birthday / n_student)


# example2()


def example3():
    """
    Which grade level had the largest drop in attendance between yesterday and today?
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

example3()
