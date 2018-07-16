#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
- Join a table twice
-
"""

from __future__ import print_function, division
import random
from sfm import rnd
import rolex
import numpy as np

import sqlalchemy
from sqlalchemy import Column
from sqlalchemy import String, Integer, Date, Boolean, ForeignKey
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import func, text
from util import pprint

engine = sqlalchemy.create_engine("sqlite:///:memory:")

Base = declarative_base()


class Attendance(Base):
    __tablename__ = "trip"

    student_id = Column(Integer, primary_key=True)
    date = Column(Date, primary_key=True)
    is_attendance = Column(Boolean)


Base.metadata.create_all(engine)

# Put some example data
Session = sessionmaker(bind=engine)
ses = Session()

n_student = 100
start_date = "2017-01-01"
end_date = "2017-01-07"
attendance_pool = [True, True, False]

data = list()
for today in rolex.time_series(start_date, end_date, freq="1day", return_date=True):
    for id in range(1, 1 + n_student):
        att = Attendance(
            student_id=id,
            date=today,
            is_attendance=random.choice(attendance_pool),
        )
        data.append(att)
data.append(att)

ses.add_all(data)

ses.commit()


def solution():
    """
    find the cancellation rate of requests made by unbanned users
    between 2017-01-01 to 2017-01-03
    """
    subquery = ses.query(Attendance).filter(Attendance.date.between("2017-01-01", "2017-01-03"))

    query = ses.query(
            ses.query(subquery.filter(Attendance.is_attendance==True)),
        ) \
        .group_by(Attendance.date)

    pprint(query, engine)

solution()
