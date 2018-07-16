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
import string
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

engine = sqlalchemy.create_engine("sqlite:///:memory:")

Base = declarative_base()


class User(Base):
    __tablename__ = "user"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    email = Column(String)


class Post(Base):
    __tablename__ = "attendance"

    id = Column(Integer, primary_key=True)
    author_id = Column(Integer, ForeignKey("user.id"))
    title = Column(String)


Base.metadata.create_all(engine)

# Put some example data
Session = sessionmaker(bind=engine)
ses = Session()


def rnd_str(length):
    return "".join([random.choice(string.ascii_lowercase) for _ in range(length)]

n_user = 100
ses.add_all([
    User(id=id, name=)
    for id in range(1, 1+n_user)
])
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

ses.close()
