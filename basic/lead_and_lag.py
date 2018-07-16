#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
LEAD, LAG, USAGE
"""

from __future__ import print_function, division
import rolex

import sqlalchemy
from sqlalchemy import Column
from sqlalchemy import Integer, Date
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from learn_sql_in_python.db import engine
from learn_sql_in_python.util import pprint, preview

Base = declarative_base()


class Weather(Base):
    __tablename__ = "weather"
    date = Column(Date, primary_key=True)
    temp = Column(Integer)


Base.metadata.create_all(engine)

# Put some example data
Session = sessionmaker(bind=engine)
ses = Session()

data = [
    Weather(
        date=a_date,
        temp=10 + ind,
    )
    for ind, a_date in enumerate(
        rolex.time_series("2017-01-01", "2017-01-07", freq="1day", return_date=True)
    )
]
ses.add_all(data)

ses.commit()

preview(Weather, engine)


def lead_lag():
    query = sqlalchemy.text("""
    SELECT 
        weather.date,
        weather.temp,
        LEAD(weather.temp, 1) OVER (ORDER BY weather.date) as next1,
        LEAD(weather.temp, 2, 0) OVER (ORDER BY weather.date) as next2,
        LAG(weather.temp, 1) OVER (ORDER BY weather.date) as previous1,
        LAG(weather.temp, 2, 0) OVER (ORDER BY weather.date) as previous2,
        weather.temp - LAG(weather.temp, 1) OVER (ORDER BY weather.date) as increment
    FROM weather
    """)
    pprint(query, engine)


lead_lag()


def derivative():
    query = sqlalchemy.text("""
    SELECT 
        weather.date as start,
        LEAD(weather.date, 1) OVER (ORDER BY weather.date) as end,
        weather.temp as start_temp,
        LEAD(weather.temp, 1) OVER (ORDER BY weather.date) as end_temp,
        LEAD(weather.temp, 1) OVER (ORDER BY weather.date) - weather.temp as increament
    FROM weather
    """)
    pprint(query, engine)

derivative()


ses.close()
